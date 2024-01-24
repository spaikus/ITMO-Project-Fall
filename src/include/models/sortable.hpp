#pragma once

#include <models/io_stream.hpp>

namespace models {

template <class T, class KeyF>
concept Sortable = models::Streamable<T> && requires(const T &a, KeyF key) {
  { std::invoke(key, a) } -> std::unsigned_integral;
};

template <class T, class KeyF>
using SortKey = std::decay_t<std::invoke_result_t<KeyF, const T &>>;

} // namespace models
