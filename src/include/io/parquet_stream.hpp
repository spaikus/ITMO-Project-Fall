#pragma once

#include <arrow/api.h>
#include <arrow/io/file.h>
#include <parquet/stream_reader.h>
#include <parquet/stream_writer.h>

#include <io/settings.hpp>
#include <models/streamable.hpp>

namespace io {

template <models::Streamable T> class ParquetIStream {
public:
  using type = T;

  ParquetIStream() = default;

  ParquetIStream(const std::string &filename, const ParquetSettings &settings) {
    PARQUET_ASSIGN_OR_THROW(infile_, arrow::io::ReadableFile::Open(filename));
    input_ = parquet::StreamReader{
        parquet::ParquetFileReader::Open(infile_, settings.reader_props)};
  }

  ParquetIStream(const ParquetIStream &) = delete;
  ParquetIStream(ParquetIStream &&) = default;
  ParquetIStream &operator=(ParquetIStream &&) = default;

  ParquetIStream &operator>>(T &val) {
    input_ >> val >> parquet::EndRow;
    return *this;
  }

  bool Eof() const { return input_.eof(); }

private:
  std::shared_ptr<arrow::io::ReadableFile> infile_;
  parquet::StreamReader input_;
};

template <models::Streamable T> class ParquetOStream {
public:
  using type = T;

  ParquetOStream() = default;

  ParquetOStream(const std::string &filename, const ParquetSettings &settings) {
    PARQUET_ASSIGN_OR_THROW(outfile_,
                            arrow::io::FileOutputStream::Open(filename));
    output_ = parquet::StreamWriter{parquet::ParquetFileWriter::Open(
        outfile_, settings.schema, settings.writer_props)};
  }

  ParquetOStream(const ParquetOStream &) = delete;
  ParquetOStream(ParquetOStream &&) = default;
  ParquetOStream &operator=(ParquetOStream &&) = default;

  ParquetOStream &operator<<(const T &val) {
    output_ << val << parquet::EndRow;
    return *this;
  }

private:
  std::shared_ptr<arrow::io::FileOutputStream> outfile_;
  parquet::StreamWriter output_;
};

} // namespace io
