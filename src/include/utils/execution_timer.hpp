#pragma once

#include <chrono>
#include <type_traits>

#include <models/status.hpp>

namespace utils {

namespace details {

template <models::Result R> struct TimedResult : public R {
  using R::R;
  using R::operator=;

public:
  std::chrono::duration<uint64_t, std::milli> ms;
};

} // namespace details

template <class F, class... Args>
requires requires(F f, Args &&...args) { models::GetStatus(f, args...); }
arrow::Result<std::chrono::duration<uint64_t, std::milli>>
TimeExecution(F f, Args &&...args) {
  const auto begin = std::chrono::high_resolution_clock::now();
  const auto status = models::GetStatus(f, std::forward<Args>(args)...);
  const auto end = std::chrono::high_resolution_clock::now();

  if (status.ok()) {
    return std::chrono::duration_cast<std::chrono::milliseconds>(end - begin);
  } else {
    return status;
  }
}

template <class F, class... Args>
details::TimedResult<std::invoke_result_t<F, Args...>>
ResultedTimeExecution(F f, Args &&...args) {
  details::TimedResult<std::invoke_result_t<F, Args...>> result;

  const auto begin = std::chrono::high_resolution_clock::now();
  result = std::invoke(f, std::forward<Args>(args)...);
  const auto end = std::chrono::high_resolution_clock::now();

  result.ms =
      std::chrono::duration_cast<std::chrono::milliseconds>(end - begin);
  return result;
}

} // namespace utils
