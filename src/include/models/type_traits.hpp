#pragma once

#include <arrow/api.h>

namespace models {

template <class T> struct TypeTraits;

template <> struct TypeTraits<uint64_t> {
  using builder_type = arrow::UInt64Builder;
  using array_type = arrow::UInt64Array;
  static inline const std::shared_ptr<arrow::DataType> &field_type =
      arrow::uint64();
};

template <> struct TypeTraits<uint32_t> {
  using builder_type = arrow::UInt32Builder;
  using array_type = arrow::UInt32Array;
  static inline const std::shared_ptr<arrow::DataType> &field_type =
      arrow::uint32();
};

template <> struct TypeTraits<std::string> {
  using builder_type = arrow::StringBuilder;
  using array_type = arrow::StringArray;
  static inline const std::shared_ptr<arrow::DataType> &field_type =
      arrow::utf8();
};

template <class T> using BuilderT = typename TypeTraits<T>::builder_type;
template <class T> using ArrayT = typename TypeTraits<T>::array_type;

} // namespace models
