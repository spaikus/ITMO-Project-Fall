#pragma once

#include <filesystem>
#include <iostream>

#include <arrow/api.h>
#include <arrow/io/file.h>
#include <parquet/arrow/reader.h>
#include <parquet/arrow/schema.h>
#include <parquet/stream_reader.h>
#include <parquet/stream_writer.h>

#include <io/settings.hpp>
#include <models/io_stream.hpp>
#include <utils/execution_timer.hpp>

namespace system_check {

template <class T, models::IOStreams IO>
void StreamsCheck(std::string filename) {
  using I = typename IO::input;
  using O = typename IO::output;

  static constexpr size_t kBufferSize = 1_GiB;
  static constexpr size_t kRows = kBufferSize / sizeof(T);
  static const std::string kOutputFile = ".tmp_output";

  const auto settings = [&] {
    if constexpr (std::is_same_v<
                      IO, models::BinaryStreams<typename IO::input::type>>) {
      return io::BufferSettings(kRows, /*batches=*/1, sizeof(T));
    } else {
      return io::Settings(kRows, /*batches=*/1, sizeof(T), filename);
    }
  }();

  std::vector<T> buffer(kRows);
  {
    I input(filename, settings);
    O output(kOutputFile, settings);

    while (!input.Eof()) {
      size_t last_ind;
      size_t time_ms =
          utils::TimeExecution([&] {
            for (last_ind = 0; last_ind != kRows && !input.Eof(); ++last_ind) {
              input >> buffer[last_ind];
            }
          })->count();

      if (time_ms == 0) {
        break;
      }
      std::cout << "stream batch read " << last_ind * sizeof(T) << " bytes in "
                << time_ms << "ms ("
                << (1000ul * last_ind * sizeof(T) / time_ms / 1_MiB)
                << " MiB/s)\n";

      time_ms = utils::TimeExecution([&] {
                  for (size_t ind = 0; ind != last_ind; ++ind) {
                    output << buffer[ind];
                  }
                })->count();
      if (time_ms == 0) {
        break;
      }

      std::cout << "stream batch write " << last_ind * sizeof(T) << " bytes in "
                << time_ms << "ms ("
                << (1000ul * last_ind * sizeof(T) / time_ms / 1_MiB)
                << " MiB/s)\n";
    }
  }

  std::filesystem::remove(kOutputFile);
}

} // namespace system_check
