#pragma once

#include <tuple>
#include <type_traits>
#include <vector>

#include <arrow/api.h>

#include <generators/array.hpp>
#include <generators/field.hpp>

namespace generators {

namespace details {

template <size_t I, class Fields, class Generator>
arrow::Status GenerateRecordBatchField(
    Fields &fields, size_t size, Generator &gen,
    std::vector<std::shared_ptr<arrow::Field>> &schema_fields,
    std::vector<std::shared_ptr<arrow::Array>> &batch_arrays) {
  using field_type = typename std::tuple_element<I, Fields>::type;
  using builder_type = typename field_type::builder_type;

  field_type &field = std::get<I>(fields);

  schema_fields[I] = arrow::field(field.name, field_type::field_type);

  builder_type builder;
  ARROW_ASSIGN_OR_RAISE(batch_arrays[I],
                        GenerateArray(builder, field.distribution, size, gen));

  if constexpr (I) {
    return GenerateRecordBatchField<I - 1>(fields, size, gen, schema_fields,
                                           batch_arrays);
  } else {
    return arrow::Status::OK();
  }
}

} // namespace details

template <Distribution... Distributions, class Generator>
arrow::Result<std::shared_ptr<arrow::RecordBatch>>
GenerateRecordBatch(std::tuple<FieldToGenerate<Distributions>...> &fields,
                    size_t size, Generator &gen) {
  static constexpr size_t kFieldsNum =
      std::tuple_size_v<std::decay_t<decltype(fields)>>;

  std::vector<std::shared_ptr<arrow::Field>> schema_fields(kFieldsNum);
  std::vector<std::shared_ptr<arrow::Array>> batch_arrays(kFieldsNum);

  if constexpr (kFieldsNum) {
    ARROW_RETURN_NOT_OK(details::GenerateRecordBatchField<kFieldsNum - 1>(
        fields, size, gen, schema_fields, batch_arrays));
  }

  std::shared_ptr<arrow::Schema> schema =
      arrow::schema(std::move(schema_fields));
  return arrow::RecordBatch::Make(std::move(schema), size,
                                  std::move(batch_arrays));
}

template <Distribution... Distributions, class Generator>
arrow::Result<std::shared_ptr<arrow::RecordBatch>>
GenerateRecordBatch(std::tuple<FieldToGenerate<Distributions>...> &&fields,
                    size_t size, Generator &gen) {
  return GenerateRecordBatch(fields, size, gen);
}

} // namespace generators
