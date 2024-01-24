#pragma once

#include <concepts>
#include <cstring>
#include <string>
#include <type_traits>

namespace io {

template <std::integral T> constexpr size_t SerializedValueSizeOf(T) {
  return sizeof(T);
}

inline size_t SerializedValueSizeOf(const std::string &str) {
  return sizeof(size_t) + str.size();
}

template <std::integral T> void SerializeValue(char *&dst, T val) {
  std::memcpy(dst, &val, sizeof(T));
  dst += sizeof(T);
}

inline void SerializeValue(char *&dst, const std::string &str) {
  SerializeValue(dst, str.size());
  std::memcpy(dst, str.c_str(), str.size());
  dst += str.size();
}

template <class T> T DeserializeValue(char *&src);

template <std::integral T> T DeserializeValue(char *&src) {
  T val;
  memcpy(&val, src, sizeof(T));
  src += sizeof(T);
  return val;
}

template <> inline std::string DeserializeValue<std::string>(char *&src) {
  const size_t size = DeserializeValue<size_t>(src);
  std::string str(src, src + size);
  src += size;
  return str;
}

template <auto... Fs, class T> size_t SerializedSizeOf(const T &obj) {
  static_assert((... && std::is_lvalue_reference_v<
                            std::invoke_result_t<decltype(Fs), const T &>>));
  return (0 + ... + SerializedValueSizeOf(std::invoke(Fs, obj)));
}

template <auto... Fs, class T> void Serialize(char *&dst, const T &obj) {
  static_assert((... && std::is_lvalue_reference_v<
                            std::invoke_result_t<decltype(Fs), const T &>>));
  (..., SerializeValue(dst, std::invoke(Fs, obj)));
}

template <auto... Fs, class T> void Deserialize(char *&src, T &obj) {
  static_assert(
      (... &&
       std::is_lvalue_reference_v<std::invoke_result_t<decltype(Fs), T &>>));
  (...,
   (std::invoke(Fs, obj) =
        DeserializeValue<std::decay_t<decltype(std::invoke(Fs, obj))>>(src)));
}

} // namespace io
