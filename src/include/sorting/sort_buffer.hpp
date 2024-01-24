#pragma once

#include <algorithm>
#include <bit>
#include <chrono>
#include <concepts>
#include <functional>
#include <limits>
#include <numeric>
#include <type_traits>
#include <utility>
#include <vector>

namespace sorting {

namespace details {

template <size_t BitsPerIter, class T, class KeyF,
          std::unsigned_integral KeyT = std::invoke_result_t<KeyF, const T &>>
void LSDRadixSort(std::vector<T> &vec, std::vector<T> &extra, size_t size,
                  KeyT min, KeyT max, KeyF key) {
  static_assert(BitsPerIter != 0 && BitsPerIter < sizeof(KeyF) * 8);
  static constexpr size_t kBuckets = 1ull << BitsPerIter;
  static constexpr size_t kMask = kBuckets - 1;

  const size_t max_bit = sizeof(KeyT) * 8 - std::countl_zero(max - min);

  const auto get_bucket = [key, min](const T &el, size_t bit) {
    return ((std::invoke(key, el) - min) >> bit) & kMask;
  };

  // TODO: use ptr instead of index

  bool swapped = false;
  for (size_t bit = 0; bit < max_bit; bit += BitsPerIter) {
    size_t cntrs[kBuckets] = {0};

    for (size_t ind = 0; ind != size; ++ind) {
      ++cntrs[get_bucket(vec[ind], bit)];
    }

    bool skip_iter = false;
    size_t cumsum = 0;
    for (size_t bucket = 0; bucket != kBuckets; ++bucket) {
      const auto bucket_cntr = cntrs[bucket];
      if (bucket_cntr == size) {
        skip_iter = true;
        break;
      }
      cntrs[bucket] = cumsum;
      cumsum += bucket_cntr;
    }
    if (skip_iter) {
      continue;
    }

    for (size_t ind = 0; ind != size; ++ind) {
      auto &cntr = cntrs[get_bucket(vec[ind], bit)];
      extra[cntr] = std::move(vec[ind]);
      ++cntr;
    }

    vec.swap(extra);
    swapped ^= true;
  }

  if (swapped) {
    vec.swap(extra);
    std::move(extra.begin(), extra.begin() + size, vec.begin());
  }
}

template <size_t BitsPerIter, class T, class KeyF,
          std::unsigned_integral KeyT = std::invoke_result_t<KeyF, const T &>>
void MSDRadixSort(std::vector<T> &vec, std::vector<T> &extra, size_t size,
                  KeyT min, KeyT max, KeyF key) {
  static_assert(BitsPerIter != 0 && BitsPerIter < sizeof(KeyT) * 8);
  constexpr size_t kBuckets = 1ull << BitsPerIter;
  constexpr size_t kMask = kBuckets - 1;

  const size_t max_bit = sizeof(KeyT) * 8 - std::countl_zero(max - min);

  const auto get_bucket = [key, min](const T &el, size_t bit) {
    return ((std::invoke(key, el) - min) >> bit) & kMask;
  };

  struct {
    size_t cntrs[kBuckets];
    size_t last;
    size_t depth;
    bool swapped;
  } stack[(sizeof(KeyT) * 8 + BitsPerIter - 1) / BitsPerIter];

  size_t depth = 0;
  bool swapped = false;
  size_t from = 0;
  size_t to = size;
  size_t stack_ind = 0;

  for (;;) {
    auto &sorting_vec = swapped ? extra : vec;
    auto &sorted_vec = swapped ? vec : extra;

    size = to - from;

    if (size > kBuckets) {
      auto bit = max_bit - (depth + 1) * BitsPerIter;
      if (max_bit < bit) {
        bit = 0;
      }

      auto &cntrs = stack[stack_ind].cntrs;
      std::fill_n(cntrs, kBuckets, 0);

      for (size_t ind = from; ind != to; ++ind) {
        ++cntrs[get_bucket(sorting_vec[ind], bit)];
      }

      bool skip_iter = false;
      size_t cumsum = from;
      for (size_t bucket = 0; bucket != kBuckets; ++bucket) {
        const auto bucket_cntr = cntrs[bucket];
        if (bucket_cntr == size) {
          skip_iter = true;
          break;
        }
        cntrs[bucket] = cumsum;
        cumsum += bucket_cntr;
      }

      if (!skip_iter) {
        for (size_t ind = from; ind != to; ++ind) {
          auto &cntr = cntrs[get_bucket(sorting_vec[ind], bit)];
          sorted_vec[cntr++] = std::move(sorting_vec[ind]);
        }
        swapped ^= true;
      }

      if (bit) {
        if (!skip_iter) {
          auto &level = stack[stack_ind];
          level.last = 0;
          level.depth = depth;
          level.swapped = swapped;

          to = level.cntrs[0];
          ++stack_ind;
        }

        ++depth;
        continue;
      }
    } else if (size) {
      std::sort(sorting_vec.begin() + from, sorting_vec.begin() + to,
                [&](const T &lhs, const T &rhs) {
                  return std::invoke(key, lhs) < std::invoke(key, rhs);
                });
    }

    if (swapped) {
      std::move(extra.begin() + from, extra.begin() + to, vec.begin() + from);
    }

    do {
      --stack_ind;
    } while (stack_ind != -1ul && ++stack[stack_ind].last == kBuckets);

    if (stack_ind == -1ul) {
      break;
    }

    const auto &level = stack[stack_ind];
    from = level.cntrs[level.last - 1];
    to = level.cntrs[level.last];
    swapped = level.swapped;
    depth = level.depth + 1;
    ++stack_ind;
  }
}

template <class T, class I>
void SortByIndices(std::vector<T> &data, std::vector<T> &extra,
                   std::vector<I> &inds, size_t size) {
  for (size_t ind = 0; ind != size; ++ind) {
    if constexpr (std::is_same_v<I, size_t>) {
      extra[ind] = std::move(data[inds[ind]]);
    } else {
      extra[ind] = std::move(data[inds[ind].ind]);
    }
  }
  data.swap(extra);
}

template <class T, class I>
void SortByIndices(std::vector<T> &data, std::vector<I> &inds, size_t size) {
  const auto get_ind = [&](size_t ind) -> size_t & {
    if constexpr (std::is_same_v<I, size_t>) {
      return inds[ind];
    } else {
      return inds[ind].ind;
    }
  };

  T tmp;
  T *to = &tmp;

  for (size_t ind = 0; ind != size; ++ind) {
    if (ind == get_ind(ind)) {
      continue;
    }

    do {
      T *from = &data[ind];
      *to = std::move(*from);
      to = from;

      std::swap(ind, get_ind(ind));
    } while (ind != get_ind(ind));

    *to = std::move(tmp);
    to = &tmp;
  }
}

template <class KeyT> struct KeyedInd {
  KeyT key;
  size_t ind;
};

} // namespace details

struct {
  template <class T, class KeyF, class KeyT>
  void operator()(std::vector<T> &vec, std::vector<T> &extra, size_t size,
                  KeyT min, KeyT max, KeyF key) const {
    details::MSDRadixSort<8>(vec, extra, size, min, max, key);
  }
} constexpr RadixSort;

template <class T, class KeyF> class SortBuffer {
public:
  using KeyT = std::invoke_result_t<KeyF, const T &>;
  static_assert(std::unsigned_integral<KeyT>);

  virtual ~SortBuffer() = default;

  SortBuffer(size_t size, KeyF key) : key(key), data_(size) {}

  T &operator[](size_t ind) { return data_[ind]; }
  const T &operator[](size_t ind) const { return data_[ind]; }

  size_t Size() const { return data_.size(); }

  virtual void Clear() {
    data_.clear();
    data_.shrink_to_fit();
  }

  virtual void Sort(size_t size, KeyT = std::numeric_limits<KeyT>::min(),
                    KeyT = std::numeric_limits<KeyT>::max()) {
    std::sort(data_.begin(), data_.begin() + size,
              [&](const T &lhs, const T &rhs) {
                return std::invoke(key, lhs) < std::invoke(key, rhs);
              });
  }

public:
  KeyF key;

protected:
  std::vector<T> data_;
};

template <class T, class KeyF>
class RadixSortBuffer : public SortBuffer<T, KeyF> {
public:
  using typename SortBuffer<T, KeyF>::KeyT;

  RadixSortBuffer(size_t size, KeyF key)
      : SortBuffer<T, KeyF>(size, key), extra_(size) {}

  void Clear() override {
    SortBuffer<T, KeyF>::Clear();
    extra_.clear();
    extra_.shrink_to_fit();
  }

  void Sort(size_t size, KeyT min = std::numeric_limits<KeyT>::min(),
            KeyT max = std::numeric_limits<KeyT>::max()) override {
    RadixSort(data_, extra_, size, min, max, key);
  }

public:
  using SortBuffer<T, KeyF>::key;

private:
  using SortBuffer<T, KeyF>::data_;
  std::vector<T> extra_;
};

template <class T, class KeyF>
class IndicesSortBuffer : public SortBuffer<T, KeyF> {
public:
  using typename SortBuffer<T, KeyF>::KeyT;

  IndicesSortBuffer(size_t size, KeyF key)
      : SortBuffer<T, KeyF>(size, key), inds_(size) {
    std::iota(inds_.begin(), inds_.end(), 0);
  }

  void Clear() override {
    SortBuffer<T, KeyF>::Clear();
    inds_.clear();
    inds_.shrink_to_fit();
  }

  void Sort(size_t size, KeyT = std::numeric_limits<KeyT>::min(),
            KeyT = std::numeric_limits<KeyT>::max()) override {
    std::sort(inds_.begin(), inds_.begin() + size, [&](size_t lhs, size_t rhs) {
      return std::invoke(key, data_[lhs]) < std::invoke(key, data_[rhs]);
    });
    details::SortByIndices(data_, inds_, size);
  }

public:
  using SortBuffer<T, KeyF>::key;

protected:
  using SortBuffer<T, KeyF>::data_;
  std::vector<size_t> inds_;
};

template <class T, class KeyF>
class RadixIndicesSortBuffer : public SortBuffer<T, KeyF> {
public:
  using typename SortBuffer<T, KeyF>::KeyT;

  RadixIndicesSortBuffer(size_t size, KeyF key)
      : SortBuffer<T, KeyF>(size, key), inds_(size), extra_(size) {
    std::iota(inds_.begin(), inds_.end(), 0);
  }

  void Clear() override {
    SortBuffer<T, KeyF>::Clear();
    inds_.clear();
    inds_.shrink_to_fit();
    extra_.clear();
    extra_.shrink_to_fit();
  }

  void Sort(size_t size, KeyT min = std::numeric_limits<KeyT>::min(),
            KeyT max = std::numeric_limits<KeyT>::max()) override {
    RadixSort(inds_, extra_, size, min, max,
              [&](size_t ind) { return std::invoke(key, data_[ind]); });
    details::SortByIndices(data_, inds_, size);
  }

public:
  using SortBuffer<T, KeyF>::key;

protected:
  using SortBuffer<T, KeyF>::data_;
  std::vector<size_t> inds_;
  std::vector<size_t> extra_;
};

template <class T, class KeyF>
class RadixKeyedIndicesSortBuffer : public SortBuffer<T, KeyF> {
public:
  using typename SortBuffer<T, KeyF>::KeyT;

  RadixKeyedIndicesSortBuffer(size_t size, KeyF key)
      : SortBuffer<T, KeyF>(size, key), keyed_inds_(size), extra_(size) {}

  void Clear() override {
    SortBuffer<T, KeyF>::Clear();
    keyed_inds_.clear();
    keyed_inds_.shrink_to_fit();
    extra_.clear();
    extra_.shrink_to_fit();
  }

  void Sort(size_t size, KeyT min = std::numeric_limits<KeyT>::min(),
            KeyT max = std::numeric_limits<KeyT>::max()) override {
    for (size_t ind = 0; ind != size; ++ind) {
      keyed_inds_[ind].key = std::invoke(key, data_[ind]);
      keyed_inds_[ind].ind = ind;
    }
    RadixSort(keyed_inds_, extra_, size, min, max,
              [&](const details::KeyedInd<KeyT> &keyed_ind) {
                return keyed_ind.key;
              });
    details::SortByIndices(data_, keyed_inds_, size);
  }

public:
  using SortBuffer<T, KeyF>::key;

private:
  using SortBuffer<T, KeyF>::data_;
  std::vector<details::KeyedInd<KeyT>> keyed_inds_;
  std::vector<details::KeyedInd<KeyT>> extra_;
};

} // namespace sorting
