#pragma once

#include <generators/distributions.hpp>
#include <models/type_traits.hpp>

namespace generators {

template <Distribution Distribution> struct FieldToGenerate {
  using value_type = typename Distribution::result_type;
  using builder_type = models::BuilderT<value_type>;
  static inline const std::shared_ptr<arrow::DataType> &field_type =
      models::TypeTraits<value_type>::field_type;

  FieldToGenerate(std::string name, Distribution distribution)
      : name(std::move(name)), distribution(std::move(distribution)) {}

  std::string name;
  Distribution distribution;
};

} // namespace generators
