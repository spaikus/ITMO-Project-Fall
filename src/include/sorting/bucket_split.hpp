#pragma once

#include <algorithm>
#include <bit>
#include <concepts>
#include <vector>

namespace sorting {

template <std::unsigned_integral Key> class UniformSplitter {
public:
  UniformSplitter(std::vector<Key> /*samples*/, Key min, Key max,
                  size_t batches_num)
      : min_(min), max_(max) {
    const Key div = 1 + (max_ - min_) / batches_num;
    pow_ =
        sizeof(Key) * 8 - 1 - std::countl_zero(div) + (std::popcount(div) > 1);

    buckets_ = 1 + (max_ - min_) / (static_cast<Key>(1u) << pow_);
  }

  Key Min(size_t ind) const {
    return ind < buckets_ ? min_ + (ind << pow_) : max_;
  }

  Key Max(size_t ind) const { return Min(ind + 1); }

  size_t operator()(Key key) const { return (key - min_) >> pow_; }

private:
  Key min_;
  Key max_;
  Key pow_;
  size_t buckets_;
};

template <std::unsigned_integral Key> class SampleSplitter {
public:
  SampleSplitter(std::vector<Key> samples, Key min, Key max, size_t batches_num)
      : min_(min) {
    const size_t samples_per_bin = samples.size() / batches_num;

    bins_ = std::move(samples);
    std::sort(bins_.begin(), bins_.end());

    for (size_t from_ind = samples_per_bin - 1, to_ind = 0;
         to_ind != batches_num; from_ind += samples_per_bin, ++to_ind) {
      bins_[to_ind] = bins_[from_ind];
    }
    bins_.resize(batches_num);
    bins_.back() = max;
  }

  Key Min(size_t ind) const {
    return ind == 0 ? min_ : ind < bins_.size() ? bins_[ind - 1] : bins_.back();
  }

  Key Max(size_t ind) const { return Min(ind + 1); }

  size_t operator()(Key key) const {
    return std::lower_bound(bins_.begin(), bins_.end(), key) - bins_.begin();
  }

private:
  size_t pow_;
  std::vector<Key> bins_;
  Key min_;
  size_t last_iters_;
};

} // namespace sorting
