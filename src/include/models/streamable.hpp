#pragma once

#include <parquet/stream_reader.h>
#include <parquet/stream_writer.h>

namespace models {

template <class T>
concept IStreamable = requires(T &a) {
  std::declval<parquet::StreamReader &>() >> a;
  std::declval<std::istream &>() >> a;
};

template <class T>
concept OStreamable = requires(const T &const_a) {
  std::declval<parquet::StreamWriter &>() << const_a;
  std::declval<std::ostream &>() << const_a;
};

template <class T>
concept Streamable = IStreamable<T> && OStreamable<T>;

} // namespace models
