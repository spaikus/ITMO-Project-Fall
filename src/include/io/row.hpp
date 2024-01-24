#pragma once

#include <cstddef>
#include <fstream>
#include <memory>
#include <tuple>

#include <parquet/stream_reader.h>
#include <parquet/stream_writer.h>

#include <io/binary_serializer.hpp>
#include <models/type_traits.hpp>

namespace io {

#define IOFIELD(ftype, name)                                                   \
  struct IOFieldN##name {                                                      \
    ftype name;                                                                \
                                                                               \
    operator ftype &() { return name; }                                        \
    operator const ftype &() const { return name; }                            \
                                                                               \
    using type = ftype;                                                        \
    static constexpr auto kName = #name;                                       \
    static constexpr type IOFieldN##name::*kField = &IOFieldN##name::name;     \
  }

template <class... Fields> struct Row : public Fields... {
  static_assert(sizeof...(Fields));

  static inline const std::shared_ptr<arrow::Schema> kSchema = arrow::schema(
      {arrow::field(Fields::kName,
                    models::TypeTraits<typename Fields::type>::field_type)...});

  size_t SerializedSize() const {
    return SerializedSizeOf<Fields::kField...>(*this);
  }

  void Serialize(char *dst) const {
    io::Serialize<Fields::kField...>(dst, *this);
  }

  void Deserialize(char *src) {
    io::Deserialize<Fields::kField...>(src, *this);
  }

  class BatchBuilder {
  public:
    BatchBuilder() = default;
    BatchBuilder(size_t size) : size_(size) { Init<sizeof...(Fields)>(); }

    void Append(const Row &row) {
      Append(row, std::make_index_sequence<sizeof...(Fields)>{});
    }

    size_t Size() const { return std::get<0>(builders_).length(); }
    size_t Capacity() const { return size_; }
    size_t Full() const { return Size() == Capacity(); }

    std::shared_ptr<arrow::RecordBatch> Finish() {
      const size_t size = Size();
      std::vector<std::shared_ptr<arrow::Array>> arrays(sizeof...(Fields));
      Finish<sizeof...(Fields)>(arrays);
      Init<sizeof...(Fields)>();
      return arrow::RecordBatch::Make(kSchema, size, std::move(arrays));
    }

  private:
    template <size_t I> void Init() {
      if constexpr (I) {
        PARQUET_THROW_NOT_OK(std::get<I - 1>(builders_).Reserve(size_));
        Init<I - 1>();
      }
    }

    template <size_t... Is>
    void Append(const Row &row, std::index_sequence<Is...>) {
      (..., [&] {
        PARQUET_THROW_NOT_OK(
            std::get<Is>(builders_).Append(row.*Fields::kField));
      }());
    }

    template <size_t I>
    void Finish(std::vector<std::shared_ptr<arrow::Array>> &arrays) {
      if constexpr (I) {
        PARQUET_ASSIGN_OR_THROW(arrays[I - 1],
                                std::get<I - 1>(builders_).Finish());
        Finish<I - 1>(arrays);
      }
    }

  private:
    size_t size_;
    std::tuple<models::BuilderT<typename Fields::type>...> builders_;
  };

  class BatchArray {
  public:
    BatchArray() = default;
    BatchArray(const arrow::RecordBatch &batch) {
      Unpack(batch, std::make_index_sequence<sizeof...(Fields)>{});
    }

    void Read(Row &row, size_t ind) {
      return Read(row, ind, std::make_index_sequence<sizeof...(Fields)>{});
    }

  private:
    template <size_t... Is>
    void Unpack(const arrow::RecordBatch &batch, std::index_sequence<Is...>) {
      (..., (std::get<Is>(array_ptrs_) =
                 reinterpret_cast<models::ArrayT<typename Fields::type> *>(
                     batch.column(Is).get())));
    }

    template <size_t... Is>
    void Read(Row &row, size_t ind, std::index_sequence<Is...>) {
      (..., (row.*Fields::kField = {typename Fields::type(
                 std::get<Is>(array_ptrs_)->Value(ind))}));
    }

  private:
    std::tuple<models::ArrayT<typename Fields::type> *...> array_ptrs_;
  };
};

template <class... Fields>
parquet::StreamReader &operator>>(parquet::StreamReader &input,
                                  Row<Fields...> &row) {
  return (..., (input >> (row.*Fields::kField)));
}

template <class... Fields>
parquet::StreamWriter &operator<<(parquet::StreamWriter &output,
                                  const Row<Fields...> &row) {
  return (..., (output << (row.*Fields::kField)));
}

template <class... Fields>
std::istream &operator>>(std::istream &input, Row<Fields...> &row) {
  return (..., (input >> (row.*Fields::kField)));
}

template <class... Fields>
std::ostream &operator<<(std::ostream &output, const Row<Fields...> &row) {
  return (..., (output << (row.*Fields::kField)));
}

} // namespace io
