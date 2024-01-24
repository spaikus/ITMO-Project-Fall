#pragma once

#include <cstdlib>

constexpr size_t operator""_MiB(unsigned long long mbs) {
  return 1024ul * 1024ul * mbs;
}

constexpr size_t operator""_MiB(long double mbs) {
  return 1024ul * 1024ul * mbs;
}

constexpr size_t operator""_GiB(unsigned long long gbs) {
  return 1024ul * 1024ul * 1024ul * gbs;
}

constexpr size_t operator""_GiB(long double gbs) {
  return 1024ul * 1024ul * 1024ul * gbs;
}
