#pragma once

#include <concepts>
#include <random>
#include <utility>

namespace generators {

template <class T>
concept Distribution = requires(T a, std::mt19937 gen) {
  typename T::result_type;
  { a(gen) } -> std::same_as<typename T::result_type>;
};

template <std::integral Int> class AutoIncrement {
public:
  using result_type = Int;

  AutoIncrement(result_type last = 0) : last_(last) {}

  void reset(result_type last = 0) { last_ = last; }

  template <class Generator> result_type operator()(Generator &) {
    return last_++;
  }
  result_type operator()() { return last_++; }

private:
  result_type last_;
};

class RandomString {
public:
  using result_type = std::string;

  RandomString(size_t max = 16, size_t min = 0) : distribution_(min, max) {}

  void reset(size_t max = 16, size_t min = 0) {
    distribution_ = std::uniform_int_distribution<size_t>(min, max);
  }

  template <class Generator> result_type operator()(Generator &gen) {
    const size_t size = distribution_(gen);

    std::string result;
    result.resize(size);
    for (auto &c : result) {
      c = kAlphaNum[kCharDistribution(gen)];
    }

    return result;
  }

private:
  static const size_t kCharNum = 62;
  static const constexpr char kAlphaNum[] =
      "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
  static_assert(sizeof(kAlphaNum) == kCharNum + 1);

  static inline std::uniform_int_distribution<size_t> kCharDistribution =
      std::uniform_int_distribution<size_t>(0, RandomString::kCharNum - 1);

private:
  std::uniform_int_distribution<size_t> distribution_;
};

namespace details {

template <std::integral Int, class Distribution> class CastTo {
public:
  using result_type = Int;

  CastTo(Distribution distribution) : distribution_(std::move(distribution)) {}

  void reset(auto &&...args) { distribution_.reset(std::forward(args)...); }

  template <class Generator> result_type operator()(Generator &gen) {
    return static_cast<result_type>(distribution_(gen));
  }

private:
  Distribution distribution_;
};

} // namespace details

template <std::integral Int, class Distribution>
auto CastTo(Distribution distribution) {
  return details::CastTo<Int, Distribution>(distribution);
}

} // namespace generators
