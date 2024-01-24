#pragma once

#include <arrow/api.h>
#include <arrow/io/file.h>
#include <parquet/arrow/reader.h>
#include <parquet/arrow/schema.h>

#include <io/literals.hpp>

namespace io {

struct BufferSettings {
  BufferSettings(size_t total_rows, size_t batches_num, size_t row_width)
      : total_rows(total_rows), batches_num(batches_num),
        batch_rows(total_rows / batches_num),
        buffer_size(batch_rows * row_width) {}

public:
  const size_t total_rows;
  const size_t batches_num;
  const size_t batch_rows;
  const size_t buffer_size;
};

struct ParquetSettings {
  ParquetSettings(size_t buffer_size, size_t batch_rows,
                  const std::string &input_filename) {
    auto reader_props = parquet::ReaderProperties(arrow::default_memory_pool());
    reader_props.enable_buffered_stream();
    reader_props.set_buffer_size(buffer_size);

    parquet::ArrowReaderProperties arrow_reader_props;
    arrow_reader_props.set_batch_size(batch_rows);

    std::shared_ptr<parquet::WriterProperties> writer_props =
        parquet::WriterProperties::Builder()
            .compression(parquet::Compression::UNCOMPRESSED)
            ->encoding(parquet::Encoding::PLAIN)
            ->write_batch_size(batch_rows)
            ->max_row_group_length(batch_rows)
            ->disable_dictionary()
            ->build();

    auto arrow_writer_props =
        parquet::ArrowWriterProperties::Builder().store_schema()->build();

    std::shared_ptr<parquet::SchemaDescriptor> schema_descriptor;
    std::shared_ptr<parquet::schema::GroupNode> schema;
    {
      std::unique_ptr<parquet::arrow::FileReader> arrow_reader;
      std::shared_ptr<arrow::io::RandomAccessFile> infile;
      PARQUET_ASSIGN_OR_THROW(infile,
                              arrow::io::ReadableFile::Open(input_filename));
      PARQUET_THROW_NOT_OK(parquet::arrow::OpenFile(
          infile, arrow::default_memory_pool(), &arrow_reader));
      std::shared_ptr<arrow::Schema> arrow_schema;
      PARQUET_THROW_NOT_OK(arrow_reader->GetSchema(&arrow_schema));
      PARQUET_THROW_NOT_OK(parquet::arrow::ToParquetSchema(
          arrow_schema.get(), *writer_props, &schema_descriptor));

      // nasty trick, TODO: find better solution
      schema = std::shared_ptr<parquet::schema::GroupNode>(
          const_cast<parquet::schema::GroupNode *>(
              schema_descriptor->group_node()),
          [](void *) {});
    }

    const_cast<parquet::ReaderProperties &>(this->reader_props) =
        std::move(reader_props);
    const_cast<parquet::ArrowReaderProperties &>(this->arrow_reader_props) =
        std::move(arrow_reader_props);
    const_cast<std::shared_ptr<parquet::WriterProperties> &>(
        this->writer_props) = std::move(writer_props);
    const_cast<std::shared_ptr<parquet::ArrowWriterProperties> &>(
        this->arrow_writer_props) = std::move(arrow_writer_props);
    const_cast<std::shared_ptr<parquet::SchemaDescriptor> &>(
        this->schema_descriptor) = std::move(schema_descriptor);
    const_cast<std::shared_ptr<parquet::schema::GroupNode> &>(this->schema) =
        std::move(schema);
  }

public:
  const parquet::ReaderProperties reader_props;
  const parquet::ArrowReaderProperties arrow_reader_props;
  const std::shared_ptr<parquet::WriterProperties> writer_props;
  const std::shared_ptr<parquet::ArrowWriterProperties> arrow_writer_props;
  const std::shared_ptr<parquet::SchemaDescriptor> schema_descriptor;
  const std::shared_ptr<parquet::schema::GroupNode> schema;
};

struct Settings : public BufferSettings, public ParquetSettings {
  Settings(size_t total_rows, size_t batches_num, size_t row_width,
           const std::string &input_filename)
      : BufferSettings(total_rows, batches_num, row_width),
        ParquetSettings(BufferSettings::buffer_size, BufferSettings::batch_rows,
                        input_filename) {}
};

} // namespace io
