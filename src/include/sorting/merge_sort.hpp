#pragma once

#include <algorithm>
#include <concepts>
#include <filesystem>
#include <memory>
#include <queue>
#include <type_traits>
#include <utility>
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
#include <sorting/settings.hpp>
#include <sorting/sort_buffer.hpp>

namespace sorting {

namespace details {

template <class T> struct MergeHeapNode {
  MergeHeapNode() = default;
  MergeHeapNode(size_t ind, T val) : ind(ind), val(val) {}

  size_t ind;
  T val;
};

template <class T, models::IStream I, models::OStream O>
void Merge(const std::string &file_output, size_t cur_file,
           size_t file_merge_up_to, const io::Settings &settings, auto &heap) {
  O output(file_output, settings);

  std::vector<I> inputs;
  inputs.reserve(settings.batches_num);

  for (size_t ind = 0, file_id = cur_file; file_id != file_merge_up_to;
       ++ind, ++file_id) {
    const auto filename = kTmpSortDir + std::to_string(file_id);

    inputs.emplace_back(filename, settings);

    T val;
    inputs.back() >> val;
    heap.emplace(ind, val);
  };

  while (!heap.empty()) {
    const auto next_sorted = heap.top();
    heap.pop();
    output << next_sorted.val;

    if (!inputs[next_sorted.ind].Eof()) {
      T val;
      inputs[next_sorted.ind] >> val;
      heap.emplace(next_sorted.ind, val);
    }
  }

  heap.size();
}

template <class T, class KeyF, models::IOStreams M_IO, models::OStream O>
arrow::Status MergePass(const std::string &file_output, size_t last_file,
                        const io::Settings &settings, KeyF key) {
  using M_I = typename M_IO::input;
  using M_O = typename M_IO::output;
  using Node = details::MergeHeapNode<T>;

  const auto cmp = [&](const Node &lhs, const Node &rhs) {
    return std::invoke(key, rhs.val) < std::invoke(key, lhs.val);
  };
  std::priority_queue<Node, std::vector<Node>, decltype(cmp)> heap(cmp);
  size_t cur_file = 0;

  while (cur_file != last_file) {
    ++last_file;
    const size_t file_merge_up_to =
        std::min(cur_file + settings.batches_num, last_file);

    if (file_merge_up_to == last_file) {
      Merge<T, M_I, O>(file_output, cur_file, file_merge_up_to, settings, heap);
    } else {
      const auto filename = kTmpSortDir + std::to_string(last_file);
      Merge<T, M_I, M_O>(filename, cur_file, file_merge_up_to, settings, heap);
    }

    for (size_t file_id = cur_file; file_id != file_merge_up_to; ++file_id) {
      std::filesystem::remove(kTmpSortDir + std::to_string(file_id));
    }

    cur_file = file_merge_up_to;
  }

  return arrow::Status::OK();
}

} // namespace details

struct MergeStats {
  std::chrono::duration<uint64_t, std::milli> fp_read{0};
  std::chrono::duration<uint64_t, std::milli> fp_sort{0};
  std::chrono::duration<uint64_t, std::milli> fp_write{0};
};

template <class T, models::IStream I, models::IOStreams M_IO, models::OStream O,
          class KeyF>
arrow::Result<MergeStats>
MergeSort(const std::string &file_input, const std::string &file_output,
          size_t batches_num, SortBuffer<T, KeyF> &buffer) {
  static_assert(models::Sortable<T, KeyF>);

  using M_O = typename M_IO::output;

  MergeStats result;

  io::Settings settings(buffer.Size(), batches_num, sizeof(T), file_input);
  I input(file_input, settings);

  size_t last_ind;

  const auto read_block = [&] {
    const auto begin = std::chrono::high_resolution_clock::now();

    for (last_ind = 0; last_ind != settings.total_rows && !input.Eof();
         ++last_ind) {
      input >> buffer[last_ind];
    }

    result.fp_read += std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::high_resolution_clock::now() - begin);
  };

  const auto sort_block = [&] {
    const auto begin = std::chrono::high_resolution_clock::now();

    buffer.Sort(last_ind);

    result.fp_sort += std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::high_resolution_clock::now() - begin);
  };

  const auto write_block = [&](auto &sorted_output) {
    const auto begin = std::chrono::high_resolution_clock::now();

    for (size_t ind = 0; ind != last_ind; ++ind) {
      sorted_output << buffer[ind];
    }

    result.fp_write += std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::high_resolution_clock::now() - begin);
  };

  read_block();
  sort_block();

  if (input.Eof()) {
    auto output = O(file_output, settings);
    write_block(output);

    return result;
  }

  std::filesystem::create_directory(kTmpSortDir);
  size_t last_file = 0;

  for (;;) {
    M_O output(kTmpSortDir + std::to_string(last_file), settings);
    write_block(output);

    if (input.Eof()) {
      break;
    }

    ++last_file;
    read_block();
    sort_block();
  }

  const auto status = details::MergePass<T, KeyF, M_IO, O>(
      file_output, last_file, settings, buffer.key);
  ARROW_RETURN_NOT_OK(status);

  std::filesystem::remove_all(kTmpSortDir);

  return result;
}

} // namespace sorting
