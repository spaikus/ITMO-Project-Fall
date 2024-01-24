#pragma once

#include <concepts>
#include <type_traits>

#include <arrow/api.h>

namespace models {

namespace details {

template <class T> struct IsResult { static constexpr bool value = false; };

template <class T> struct IsResult<arrow::Result<T>> {
  static constexpr bool value = true;
};

template <class T> static constexpr bool IsResultV = IsResult<T>::value;

} // namespace details

template <class T>
concept Result = details::IsResultV<T>;

template <class F, class... Args>
requires Result<std::invoke_result_t<F, Args...>> arrow::Status
GetStatus(F f, Args &&...args) {
  return std::invoke(f, std::forward<Args>(args)...).status();
}

template <class F, class... Args>
requires std::same_as<arrow::Status, std::invoke_result_t<F, Args...>>
    arrow::Status GetStatus(F f, Args &&...args) {
  return std::invoke(f, std::forward<Args>(args)...);
}

template <class F, class... Args>
requires std::is_void_v<std::invoke_result_t<F, Args...>> arrow::Status
GetStatus(F f, Args &&...args) {
  std::invoke(f, std::forward<Args>(args)...);
  return arrow::Status::OK();
}

} // namespace models
