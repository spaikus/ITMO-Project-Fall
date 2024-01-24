#pragma once

#include <gtest/gtest.h>

#include <algorithm>
#include <iostream>
#include <numeric>
#include <random>
#include <vector>

#include <io/literals.hpp>
#include <utils/execution_timer.hpp>

namespace system_check {

inline void RamCheck() {
  using T = int64_t;
  static const size_t kSize = 2_GiB;
  static const size_t kNum = kSize / sizeof(T);

  std::cout << "\nRam check\n";

  std::vector<T> buf1(kNum);
  std::vector<T> buf2(kNum);

  T val = rand();

  const auto seq_memset = [&] {
    std::memset(buf1.data(), 0, kNum * sizeof(T));
  };
  const auto seq_assign = [&] { std::fill_n(buf1.begin(), kNum, val); };
  const auto assert_seq_assign = [&] {
    for (T el : buf1) {
      ASSERT_EQ(el, val);
    }
  };

  const auto seq_copy = [&]() {
    std::copy_n(buf1.begin(), kNum, buf2.begin());
  };
  const auto assert_seq_copy = [&] {
    for (size_t ind = 0; ind != kNum; ++ind) {
      ASSERT_EQ(buf1[ind], buf2[ind]);
    }
  };

  const auto init_rand_inds = [&] {
    std::iota(buf2.begin(), buf2.end(), 0);
    std::mt19937_64 gen;
    std::shuffle(buf2.begin(), buf2.end(), gen);
  };
  const auto rand_assign = [&] {
    for (size_t ind = 0; ind != kNum; ++ind) {
      buf1[buf2[ind]] = ind;
    }
  };
  const auto assert_rand_assign = [&] {
    for (size_t ind = 0; ind != kNum; ++ind) {
      ASSERT_EQ(buf2[buf1[ind]], ind);
    }
  };

  // idle runs
  for (int i = 0; i != 1; ++i) {
    seq_assign();
    seq_copy();
    init_rand_inds();
    rand_assign();
  }

  static constexpr size_t kIters = 2;

  size_t time_ms = utils::TimeExecution(seq_memset)->count();
  val = 0;
  assert_seq_assign();
  std::cout << "RAM seq memset: "
            << 1000.0 * kSize / time_ms * kIters / (1ul << 30) << " GiB/s\n";

  time_ms = 0;
  for (size_t iter = 0; iter != kIters; ++iter) {
    val = rand();
    time_ms += utils::TimeExecution(seq_assign)->count();
    assert_seq_assign();
  }
  std::cout << "RAM seq assign: "
            << 1000.0 * kSize / time_ms * kIters / (1ul << 30) << " GiB/s\n";

  time_ms = 0;
  for (size_t iter = 0; iter != kIters; ++iter) {
    init_rand_inds();
    time_ms += utils::TimeExecution(seq_copy)->count();
    assert_seq_copy();
  }
  std::cout << "RAM seq copy: "
            << 1000.0 * kSize / time_ms * kIters / (1ul << 30) << " GiB/s\n";

  time_ms = 0;
  for (size_t iter = 0; iter != kIters; ++iter) {
    init_rand_inds();
    time_ms += utils::TimeExecution(rand_assign)->count();
    assert_rand_assign();
  }
  std::cout << "RAM rand assign: "
            << 1000.0 * kSize / time_ms * kIters / (1ul << 30) << " GiB/s\n";
}

} // namespace system_check
