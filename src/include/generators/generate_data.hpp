#pragma once

#include <random>
#include <string>
#include <tuple>

#include <arrow/api.h>
#include <arrow/io/file.h>
#include <parquet/arrow/writer.h>

#include <generators/distributions.hpp>
#include <generators/record_batch.hpp>
#include <io/binary_stream.hpp>
#include <io/row.hpp>

namespace generators {

namespace details {

struct GenerateParquet {
  template <generators::Distribution... Distributions>
  arrow::Status
  operator()(const std::string &file,
             std::tuple<generators::FieldToGenerate<Distributions>...> fields,
             size_t size, size_t chunk_size) const {
    std::mt19937_64 gen;

    chunk_size = std::min(size, chunk_size);
    size_t chunks = size ? (size - 1) / chunk_size : 0;
    const size_t first_chunk = size ? size - chunks * chunk_size : 0;

    std::shared_ptr<parquet::WriterProperties> props =
        parquet::WriterProperties::Builder()
            .compression(parquet::Compression::UNCOMPRESSED)
            ->encoding(parquet::Encoding::PLAIN)
            ->disable_dictionary()
            ->build();

    std::shared_ptr<parquet::ArrowWriterProperties> arrow_props =
        parquet::ArrowWriterProperties::Builder().store_schema()->build();

    ARROW_ASSIGN_OR_RAISE(auto batch,
                          GenerateRecordBatch(fields, first_chunk, gen));

    std::shared_ptr<arrow::io::FileOutputStream> outfile;
    ARROW_ASSIGN_OR_RAISE(outfile, arrow::io::FileOutputStream::Open(file));
    std::unique_ptr<parquet::arrow::FileWriter> writer;
    ARROW_ASSIGN_OR_RAISE(
        writer, parquet::arrow::FileWriter::Open(*batch->schema(),
                                                 arrow::default_memory_pool(),
                                                 outfile, props, arrow_props));

    for (;;) {
      ARROW_RETURN_NOT_OK(writer->WriteRecordBatch(*batch));

      if (chunks == 0) {
        break;
      }
      --chunks;
      ARROW_ASSIGN_OR_RAISE(batch,
                            GenerateRecordBatch(fields, chunk_size, gen));
    }

    ARROW_RETURN_NOT_OK(writer->Close());
    return arrow::Status::OK();
  }
};

template <class... Fields> struct FieldToDistributionAsserter {
  template <generators::Distribution... Distributions>
  void operator()(std::tuple<generators::FieldToGenerate<Distributions>...>) {
    static_assert((... && std::is_same_v<typename Fields::type,
                                         typename Distributions::result_type>));
  }
};

template <class... Fields>
auto GetFieldToDistributionAsserter(io::Row<Fields...>) {
  return FieldToDistributionAsserter<Fields...>{};
}

} // namespace details

constexpr details::GenerateParquet GenerateParquet{};

// TODO: fix Row necessity
template <class Row, generators::Distribution... Distributions>
void GenerateBinary(
    const std::string &file,
    std::tuple<generators::FieldToGenerate<Distributions>...> fields,
    size_t size) {
  std::mt19937_64 gen;

  Row row;
  details::GetFieldToDistributionAsserter(row)(fields);

  io::BinaryOStream<Row> output(file, {1 << 20ul, 1, sizeof(Row)});

  while (size--) {
    row = std::apply(
        [&](auto &...fields) { return Row{{fields.distribution(gen)}...}; },
        fields);
    output << row;
  }
}

} // namespace generators
