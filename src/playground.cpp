#include <iostream>

#include "rows.hpp"

#include <io/row.hpp>
#include <sorting/bucket_sort.hpp>
#include <sorting/merge_sort.hpp>
#include <utils/execution_timer.hpp>

using Row = RowFew;
uint64_t Key(const Row &row) { return row.uniform; }

int main() {
  const size_t memsize = 512_MiB;
  sorting::RadixSortBuffer<Row, uint64_t (*)(const Row &)> buffer(
      memsize / sizeof(Row), static_cast<uint64_t (*)(const Row &)>(&Key));

  {
    const auto res = utils::ResultedTimeExecution(
        &sorting::MergeSort<Row, io::BatchIStream<Row>,
                            models::BinaryStreams<Row>, io::BatchOStream<Row>,
                            decltype(buffer.key)>,
        "static/row_16gib.parquet", "sorted", 64, buffer);

    std::cout << res.ms.count() << ", " << res->fp_read.count() << ", "
              << res->fp_sort.count() << ", " << res->fp_write.count()
              << " ms\n";
  }

  {
    const auto res = utils::TimeExecution(
        &sorting::BucketSort<Row, io::BatchIStream<Row>,
                             models::BinaryStreams<Row>, io::BatchOStream<Row>,
                             decltype(buffer.key)>,
        "static/row_16gib.parquet", "sorted", 64, buffer, 0, -1ul);

    std::cout << res->count() << " ms\n";
  }

  return 0;
}