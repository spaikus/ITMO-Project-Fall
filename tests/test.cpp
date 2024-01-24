#include <gtest/gtest.h>

#include <models/io_stream.hpp>
#include <sorting/arrow_sort.hpp>
#include <sorting/bucket_sort.hpp>
#include <sorting/merge_sort.hpp>

#include "data.hpp"
#include "system_check/disk_binary.hpp"
#include "system_check/disk_streams.hpp"
#include "system_check/ram.hpp"

const std::string kTmpOutputFile = ".tmp_sorted";

inline uint64_t RowKey(const Row &row) { return row.field; }

void AssertOrder() {
  static constexpr size_t kBufferSize = 512_MiB;
  static constexpr size_t kRows = kBufferSize / sizeof(Row);

  io::ParquetSettings settings(kBufferSize, kRows, kTmpOutputFile);
  io::ParquetIStream<Row> input(kTmpOutputFile, settings);

  Row row;
  uint64_t prev = 0;

  while (!input.Eof()) {
    input >> row;
    const uint64_t key = RowKey(row);
    ASSERT_LE(prev, key);
    prev = key;
  }

  std::filesystem::remove(kTmpOutputFile);
}

TEST_F(BinaryDataTest, SystemCheck) {
  system_check::RamCheck();
  system_check::BinaryCheck(kDataFile);

  std::cout << "\nBinary stream io check\n";
  system_check::StreamsCheck<Row, models::BinaryStreams<Row>>(kDataFile);
}

TEST_F(DataTest, SystemCheck) {
  std::cout << "\nRecord batch stream io check\n";
  system_check::StreamsCheck<Row, models::BatchStreams<Row>>(kDataFile);

  std::cout << "\nParquet stream io check\n";
  system_check::StreamsCheck<Row, models::ParquetStreams<Row>>(kDataFile);
}

TEST_F(DataTest, MergeSort) {
  {
    sorting::SortBuffer<Row, decltype(&RowKey)> buffer(512_MiB / sizeof(Row),
                                                       RowKey);
    const auto result =
        sorting::MergeSort<Row, io::BatchIStream<Row>,
                           models::BinaryStreams<Row>, io::BatchOStream<Row>>(
            kDataFile, kTmpOutputFile, 256, buffer);
    ASSERT_EQ(result.status(), arrow::Status::OK());
    AssertOrder();
  }
  {
    sorting::IndicesSortBuffer<Row, decltype(&RowKey)> buffer(
        500_MiB / sizeof(Row), RowKey);
    const auto result =
        sorting::MergeSort<Row, io::BatchIStream<Row>,
                           models::BinaryStreams<Row>, io::BatchOStream<Row>>(
            kDataFile, kTmpOutputFile, 256, buffer);
    ASSERT_EQ(result.status(), arrow::Status::OK());
    AssertOrder();
  }
}

TEST_F(DataTest, BucketSort) {
  {
    sorting::RadixSortBuffer<Row, decltype(&RowKey)> buffer(
        500_MiB / sizeof(Row), RowKey);
    const auto result =
        sorting::BucketSort<Row, io::BatchIStream<Row>,
                            models::BinaryStreams<Row>, io::BatchOStream<Row>>(
            kDataFile, kTmpOutputFile, 256, buffer, 0, -1ul);
    ASSERT_EQ(result, arrow::Status::OK());
    AssertOrder();
  }
  {
    sorting::RadixKeyedIndicesSortBuffer<Row, decltype(&RowKey)> buffer(
        500_MiB / sizeof(Row), RowKey);
    const auto result =
        sorting::BucketSort<Row, io::BatchIStream<Row>,
                            models::BinaryStreams<Row>, io::BatchOStream<Row>>(
            kDataFile, kTmpOutputFile, 256, buffer, 0, -1ul);
    ASSERT_EQ(result, arrow::Status::OK());
    AssertOrder();
  }
}

int main(int argc, char **argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
