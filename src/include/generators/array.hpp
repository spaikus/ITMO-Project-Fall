#pragma once

#include <memory>

#include <arrow/api.h>

#include <generators/distributions.hpp>

namespace generators {

template <class Builder, Distribution Distribution, class Generator>
arrow::Result<std::shared_ptr<arrow::Array>>
GenerateArray(Builder &builder, Distribution &distribution, size_t size,
              Generator &gen) {
  using Value = typename Distribution::result_type;
  static const size_t batch_size = (1 << 12) / sizeof(Value);

  std::vector<Value> buffer(std::min(size, batch_size));

  while (size) {
    if (size >= batch_size) {
      size -= batch_size;
    } else {
      buffer.resize(size);
      size = 0;
    }

    for (auto &el : buffer) {
      el = distribution(gen);
    }
    ARROW_RETURN_NOT_OK(builder.AppendValues(buffer));
  }

  ARROW_ASSIGN_OR_RAISE(auto result, builder.Finish());
  return result;
}

} // namespace generators
