#pragma once

#include <concepts>

#include <io/batch_stream.hpp>
#include <io/binary_stream.hpp>
#include <io/parquet_stream.hpp>

namespace models {

template <class T> struct ParquetStreams {
  using input = io::ParquetIStream<T>;
  using output = io::ParquetOStream<T>;
};

template <class T> struct BinaryStreams {
  using input = io::BinaryIStream<T>;
  using output = io::BinaryOStream<T>;
};

template <class T> struct BatchStreams {
  using input = io::BatchIStream<T>;
  using output = io::BatchOStream<T>;
};

template <class T>
concept IStream = requires(T a) {
  typename T::type;
  { a.operator>>(std::declval<typename T::type &>()) } -> std::same_as<T &>;
  { a.Eof() } -> std::same_as<bool>;
};

template <class T>
concept OStream = requires(T a, int b) {
  typename T::type;
  {
    a.operator<<(std::declval<const typename T::type &>())
    } -> std::same_as<T &>;
};

template <class T>
concept IOStreams = IStream<typename T::input> && OStream<typename T::output>;

} // namespace models
