#pragma once

#include <__memory/construct_at.h>
#include <arrow/dataset/file_parquet.h>
#include <memory>
#include <parquet/arrow/writer.h>
#include <parquet/file_writer.h>

#include <io/settings.hpp>

namespace io {

template <class T> class BatchIStream {
  static constexpr int kGroupsPerFetch = 1;

public:
  using type = T;

  BatchIStream() = default;

  BatchIStream(const std::string &filename, const ParquetSettings &settings)
      : last_row_group_(0), inds_(kGroupsPerFetch) {
    parquet::arrow::FileReaderBuilder reader_builder;
    PARQUET_THROW_NOT_OK(
        reader_builder.OpenFile(filename, false, settings.reader_props));
    reader_builder.memory_pool(arrow::default_memory_pool());
    reader_builder.properties(settings.arrow_reader_props);

    PARQUET_ASSIGN_OR_THROW(arrow_reader_, reader_builder.Build());
    // PARQUET_THROW_NOT_OK(arrow_reader_->GetRecordBatchReader(&rb_reader_));

    Fetch();
  }

  BatchIStream(const BatchIStream &) = delete;
  BatchIStream(BatchIStream &&) = default;
  BatchIStream &operator=(BatchIStream &&) = default;

  bool Eof() const { return !batch_; }

  BatchIStream &operator>>(T &row) {
    array_.Read(row, last_row_);
    ++last_row_;

    if (batch_->num_rows() == last_row_) {
      Fetch();
    }

    return *this;
  }

private:
  void Fetch() {
    last_row_ = 0;

    // TODO: previous batches wont flush from ram, idk
    // PARQUET_THROW_NOT_OK(rb_reader_->ReadNext(&batch_));

    if (last_row_group_ != arrow_reader_->num_row_groups()) {
      const int step = std::min(
          arrow_reader_->num_row_groups() - last_row_group_, kGroupsPerFetch);

      inds_.resize(step);
      std::iota(inds_.begin(), inds_.end(), last_row_group_);
      last_row_group_ += step;

      std::shared_ptr<::arrow::Table> table;
      PARQUET_THROW_NOT_OK(arrow_reader_->ReadRowGroups(inds_, &table));
      PARQUET_ASSIGN_OR_THROW(batch_, table->CombineChunksToBatch());
    } else {
      batch_.reset();
    }

    if (batch_ && batch_->num_rows()) {
      array_ = {*batch_};
    } else if (batch_) {
      Fetch();
    }
  }

private:
  std::unique_ptr<parquet::arrow::FileReader> arrow_reader_;
  int last_row_group_;

  //   std::shared_ptr<::arrow::RecordBatchReader> rb_reader_;

  std::vector<int> inds_;
  std::shared_ptr<arrow::RecordBatch> batch_;
  int64_t last_row_;

  typename T::BatchArray array_;
};

template <class T> class BatchOStream {
public:
  using type = T;

  BatchOStream() = default;

  BatchOStream(const std::string &filename, const ParquetSettings &settings)
      : rows_(settings.writer_props->write_batch_size()), builder_(rows_) {
    PARQUET_ASSIGN_OR_THROW(outfile_,
                            arrow::io::FileOutputStream::Open(filename));
    PARQUET_ASSIGN_OR_THROW(
        writer_, parquet::arrow::FileWriter::Open(
                     *T::kSchema.get(), arrow::default_memory_pool(), outfile_,
                     settings.writer_props, settings.arrow_writer_props));
  }

  BatchOStream(const BatchOStream &) = delete;
  BatchOStream(BatchOStream &&) = default;
  BatchOStream &operator=(BatchOStream &&ostream) {
    if (builder_.Size()) {
      Flush();
    }
    std::destroy_at(this);
    std::construct_at(this, std::move(ostream));
    return *this;
  }

  ~BatchOStream() {
    if (builder_.Size()) {
      Flush();
    }
  }

  BatchOStream &operator<<(const T &row) {
    builder_.Append(row);

    if (builder_.Full()) {
      Flush();
    }

    return *this;
  }

private:
  void Flush() {
    PARQUET_THROW_NOT_OK(writer_->WriteRecordBatch(*builder_.Finish()));
  }

private:
  std::shared_ptr<arrow::io::FileOutputStream> outfile_;
  std::unique_ptr<parquet::arrow::FileWriter> writer_;
  size_t rows_;
  typename T::BatchBuilder builder_;
};

} // namespace io
