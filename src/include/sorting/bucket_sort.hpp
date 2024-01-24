#pragma once

#include <algorithm>
#include <concepts>
#include <filesystem>
#include <iostream>
#include <memory>
#include <string>
#include <type_traits>
#include <vector>

#include <arrow/api.h>
#include <arrow/io/file.h>
#include <parquet/arrow/reader.h>
#include <parquet/arrow/schema.h>
#include <parquet/stream_reader.h>
#include <parquet/stream_writer.h>

#include <io/settings.hpp>
#include <models/io_stream.hpp>
#include <models/sortable.hpp>
#include <sorting/bucket_split.hpp>
#include <sorting/settings.hpp>
#include <sorting/sort_buffer.hpp>

namespace sorting {

namespace details {

template <class T, class KeyF> struct BucketRange {
  size_t file_id;
  models::SortKey<T, KeyF> min, max;
};

template <models::OStream O, class T, class KeyF, models::IStream I>
arrow::Status SplitIntoBuckets(SortBuffer<T, KeyF> &buffer,
                               std::vector<BucketRange<T, KeyF>> &stack,
                               I input, const io::Settings &settings) {
  using BucketSortKey = models::SortKey<T, KeyF>;

  const BucketSortKey min = stack.back().min;
  const BucketSortKey max = stack.back().max;
  size_t file_id = stack.back().file_id;
  stack.pop_back();

  // TODO: gather stats for ~about equal size~ splitting, sample size
  const size_t samples_num = settings.batches_num * 4;
  std::vector<BucketSortKey> samples(samples_num);
  for (size_t ind = 0; ind != samples_num; ++ind) {
    samples[ind] = std::invoke(buffer.key, buffer[ind]);
  }
  SampleSplitter<BucketSortKey> splitter(std::move(samples), min, max,
                                         settings.batches_num);

  std::vector<O> outputs(settings.batches_num);
  for (size_t ind = settings.batches_num; ind != 0;) {
    --ind;
    ++file_id;
    const BucketSortKey l = splitter.Min(ind);
    const BucketSortKey r = splitter.Max(ind);

    stack.push_back({file_id, l, r});
    outputs[ind] = O(kTmpSortDir + std::to_string(file_id), settings);
  }

  for (size_t ind = 0; ind != buffer.Size(); ++ind) {
    const size_t batch_ind = splitter(std::invoke(buffer.key, buffer[ind]));
    outputs[batch_ind] << buffer[ind];
  }

  while (!input.Eof()) {
    input >> buffer[0];
    const size_t batch_ind = splitter(std::invoke(buffer.key, buffer[0]));
    outputs[batch_ind] << buffer[0];
  }

  return arrow::Status::OK();
}

} // namespace details

template <class T, models::IStream I, models::IOStreams M_IO, models::OStream O,
          class KeyF>
arrow::Status BucketSort(std::string file_input, const std::string &file_output,
                         size_t buckets_num, SortBuffer<T, KeyF> &buffer,
                         models::SortKey<T, KeyF> min,
                         models::SortKey<T, KeyF> max) {
  static_assert(models::Sortable<T, KeyF>);

  using M_I = typename M_IO::input;
  using M_O = typename M_IO::output;

  io::Settings settings(buffer.Size(), buckets_num, sizeof(T), file_input);
  O output(file_output, settings);

  std::vector<details::BucketRange<T, KeyF>> stack;

  const auto bucket_step = [&](auto input) {
    const size_t input_id = stack.back().file_id;
    const bool single_value = stack.back().min == stack.back().max;

    size_t last_ind = 0;
    if (single_value) {
      while (!input.Eof()) {
        input >> buffer[0];
        output << buffer[0];
        ++last_ind;
      }
    } else {
      while (!input.Eof() && last_ind != settings.total_rows) {
        input >> buffer[last_ind];
        ++last_ind;
      }
    }

    if (!input.Eof()) {
      ARROW_RETURN_NOT_OK(details::SplitIntoBuckets<M_O>(
          buffer, stack, std::move(input), settings));
    } else {
      if (!single_value) {
        buffer.Sort(last_ind, stack.back().min, stack.back().max);
        for (size_t ind = 0; ind != last_ind; ++ind) {
          output << buffer[ind];
        }
      }
      stack.pop_back();
    }

    input = decltype(input){};
    std::filesystem::remove(kTmpSortDir + std::to_string(input_id));

    if (!stack.empty()) {
      file_input = kTmpSortDir + std::to_string(stack.back().file_id);
    }

    return arrow::Status::OK();
  };

  std::filesystem::create_directory(kTmpSortDir);

  stack.push_back({0, min, max});
  ARROW_RETURN_NOT_OK(bucket_step(I(file_input, settings)));
  while (!stack.empty()) {
    ARROW_RETURN_NOT_OK(bucket_step(M_I(file_input, settings)));
  }

  std::filesystem::remove_all(kTmpSortDir);

  return arrow::Status::OK();
}

} // namespace sorting
