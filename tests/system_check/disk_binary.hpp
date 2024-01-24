#pragma once

#include <stdio.h>
#include <sys/fcntl.h>
#include <unistd.h>

#include <filesystem>
#include <iostream>
#include <vector>

#include <io/literals.hpp>
#include <utils/execution_timer.hpp>

namespace system_check {

inline void BinaryCheck(const std::string &filename) {
  static constexpr size_t kBufferSize = 1_GiB;
  static constexpr std::string_view kOutputFile = ".tmp_output";

  std::cout << "\nDisk binary io check\n";

  int input_fd = open(filename.c_str(), O_RDONLY, S_IRUSR | S_IWUSR);
  int output_fd = open(kOutputFile.data(), O_WRONLY | O_SYNC | O_CREAT | O_TRUNC,
                       S_IRUSR | S_IWUSR);

  std::vector<char> buffer(kBufferSize);
  while (true) {
    size_t size = 0;
    size_t time_ms = utils::TimeExecution([&] {
                       size_t last_read;
                       do {
                         last_read = read(input_fd, buffer.data() + size,
                                          buffer.size() - size);
                         size += last_read;
                       } while (size != buffer.size() && last_read != 0);
                     })->count();

    if (time_ms == 0) {
      break;
    }
    std::cout << "binary batch read " << size << " bytes in " << time_ms
              << "ms (" << (1000ul * size / time_ms / 1_MiB) << " MiB/s)\n";

    size_t to_write = size;
    time_ms = utils::TimeExecution([&] {
                auto ptr = buffer.data();
                do {
                  size_t last_write = write(output_fd, ptr, to_write);
                  ptr += last_write;
                  to_write -= last_write;
                } while (to_write != 0);
              })->count();
    if (time_ms == 0) {
      break;
    }

    std::cout << "binary batch write " << size << " bytes in " << time_ms
              << "ms (" << (1000ul * size / time_ms / 1_MiB) << " MiB/s)\n";
  }

  close(input_fd);
  close(output_fd);

  std::filesystem::remove(kOutputFile);
}

} // namespace system_check
