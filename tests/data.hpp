#pragma once

#include <gtest/gtest.h>

#include <generators/generate_data.hpp>
#include <io/literals.hpp>
#include <io/row.hpp>

IOFIELD(uint64_t, field);
using Row = io::Row<IOFieldNfield>;

const auto kGenSchema = std::make_tuple(generators::FieldToGenerate(
    "field", std::uniform_int_distribution<uint64_t>{}));

const std::string kDataFile = ".tmp_data";

class DataTest : public testing::Test {
public:
  static constexpr size_t kMemory = 12_GiB;
  static constexpr size_t kRows = kMemory / sizeof(Row);

protected:
  void SetUp() override {
    PARQUET_THROW_NOT_OK(
        generators::GenerateParquet(kDataFile, kGenSchema, kRows, 1ul << 25));
  }

  void TearDown() override { std::filesystem::remove(kDataFile); }
};

class SmallDataTest : public testing::Test {
public:
  static constexpr size_t kMemory = 2_GiB;
  static constexpr size_t kRows = kMemory / sizeof(Row);

protected:
  void SetUp() override {
    PARQUET_THROW_NOT_OK(
        generators::GenerateParquet(kDataFile, kGenSchema, kRows, 1ul << 25));
  }

  void TearDown() override { std::filesystem::remove(kDataFile); }
};

class BinaryDataTest : public testing::Test {
public:
  static constexpr size_t kMemory = 12_GiB;
  static constexpr size_t kRows = kMemory / sizeof(Row);

protected:
  void SetUp() override {
    generators::GenerateBinary<Row>(kDataFile, kGenSchema, kRows);
  }

  void TearDown() override { std::filesystem::remove(kDataFile); }
};
