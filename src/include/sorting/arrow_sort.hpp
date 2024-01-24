#pragma once

#include <arrow/api.h>
#include <arrow/compute/api.h>
#include <arrow/io/file.h>
#include <parquet/arrow/reader.h>
#include <parquet/arrow/schema.h>
#include <parquet/arrow/writer.h>

namespace sorting {

struct ArrowParquetStats {
  std::chrono::duration<long long, std::milli> read{0};
  std::chrono::duration<long long, std::milli> sort_indices{0};
  std::chrono::duration<long long, std::milli> take{0};
  std::chrono::duration<long long, std::milli> write{0};
};

inline arrow::Result<ArrowParquetStats>
ArrowSort(std::string file_input, std::string file_output,
          std::vector<arrow::compute::SortKey> sort_keys) {
  ArrowParquetStats result;

  std::shared_ptr<arrow::io::RandomAccessFile> input;
  ARROW_ASSIGN_OR_RAISE(input, arrow::io::ReadableFile::Open(file_input));

  arrow::MemoryPool *pool = arrow::default_memory_pool();
  std::unique_ptr<parquet::arrow::FileReader> arrow_reader;
  ARROW_RETURN_NOT_OK(parquet::arrow::OpenFile(input, pool, &arrow_reader));

  std::shared_ptr<arrow::Table> table;
  {
    auto begin = std::chrono::high_resolution_clock::now();

    ARROW_RETURN_NOT_OK(arrow_reader->ReadTable(&table));

    result.read += std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::high_resolution_clock::now() - begin);
  }

  arrow::compute::SortOptions sort_options;
  sort_options.sort_keys = std::move(sort_keys);
  arrow::Datum sorted_inds;
  {
    auto begin = std::chrono::high_resolution_clock::now();

    ARROW_ASSIGN_OR_RAISE(
        sorted_inds,
        arrow::compute::CallFunction("sort_indices", {table}, &sort_options));

    result.sort_indices +=
        std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::high_resolution_clock::now() - begin);
  }

  arrow::compute::TakeOptions take_options;
  arrow::Datum sorted_table;
  {
    auto begin = std::chrono::high_resolution_clock::now();

    ARROW_ASSIGN_OR_RAISE(
        sorted_table, arrow::compute::CallFunction("take", {table, sorted_inds},
                                                   &take_options));

    result.take += std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::high_resolution_clock::now() - begin);
  }

  std::shared_ptr<parquet::WriterProperties> props =
      parquet::WriterProperties::Builder()
          .compression(arrow::Compression::UNCOMPRESSED)
          ->encoding(parquet::Encoding::PLAIN)
          ->disable_dictionary()
          ->build();

  std::shared_ptr<parquet::ArrowWriterProperties> arrow_props =
      parquet::ArrowWriterProperties::Builder().store_schema()->build();

  std::shared_ptr<arrow::io::FileOutputStream> outfile;
  ARROW_ASSIGN_OR_RAISE(outfile,
                        arrow::io::FileOutputStream::Open(file_output));
  {
    auto begin = std::chrono::high_resolution_clock::now();

    ARROW_RETURN_NOT_OK(parquet::arrow::WriteTable(
        *sorted_table.table().get(), arrow::default_memory_pool(), outfile,
        1024 * 1024, props, arrow_props));

    result.write += std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::high_resolution_clock::now() - begin);
  }

  return result;
}

} // namespace sorting
