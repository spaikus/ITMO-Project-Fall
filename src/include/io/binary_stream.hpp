#pragma once

#include <stdio.h>
#include <sys/fcntl.h>
#include <unistd.h>

#include <io/binary_serializer.hpp>
#include <io/settings.hpp>

namespace io {

template <class T> class BinaryIStream {
public:
  using type = T;

  BinaryIStream() = default;

  BinaryIStream(const std::string &filename, const BufferSettings &settings)
      : buf_(std::make_unique<char[]>(settings.buffer_size)), ptr_(buf_.get()),
        buffer_size_(settings.buffer_size),
        fd_(open(filename.c_str(), O_RDONLY, S_IRUSR | S_IWUSR)) {
    if (fd_ == -1) {
      throw std::runtime_error("Cant open file");
    }

    Fetch();
  }

  ~BinaryIStream() {
    if (buf_) {
      close(fd_);
    }
  }

  BinaryIStream(const BinaryIStream &) = delete;
  BinaryIStream(BinaryIStream &&) = default;
  BinaryIStream &operator=(BinaryIStream &&ostream) {
    std::destroy_at(this);
    std::construct_at(this, std::move(ostream));
    return *this;
  }

  bool Eof() const { return left_ == 0; }

  BinaryIStream &operator>>(T &row) {
    size_t size;

    if (left_ < sizeof(size_t)) {
      Fetch();
    }
    size = io::DeserializeValue<size_t>(ptr_);
    left_ -= sizeof(size_t);

    if (left_ < size) {
      Fetch();
    }

    row.Deserialize(ptr_);
    ptr_ += size;
    left_ -= size;

    if (!left_) {
      Fetch();
    }
    return *this;
  }

private:
  void Fetch() {
    std::memcpy(buf_.get(), ptr_, left_);
    ptr_ = buf_.get();

    size_t size;
    do {
      size = read(fd_, ptr_ + left_, buffer_size_ - left_);
      left_ += size;
    } while (size != 0 && buffer_size_ != left_);
  }

private:
  std::unique_ptr<char[]> buf_;
  char *ptr_;
  size_t buffer_size_ = 0;
  size_t left_ = 0;

  int fd_ = -1;
};

template <class T> class BinaryOStream {
public:
  using type = T;

  BinaryOStream() = default;

  BinaryOStream(const std::string &filename, const BufferSettings &settings)
      : buf_(std::make_unique<char[]>(settings.buffer_size)), ptr_(buf_.get()),
        buffer_size_(settings.buffer_size), left_(settings.buffer_size),
        fd_(open(filename.c_str(), O_WRONLY | O_SYNC | O_CREAT | O_TRUNC,
                 S_IRUSR | S_IWUSR)) {
    if (fd_ == -1) {
      throw std::runtime_error("Cant open file");
    }
  }

  BinaryOStream(const BinaryOStream &) = delete;
  BinaryOStream(BinaryOStream &&) = default;
  BinaryOStream &operator=(BinaryOStream &&ostream) {
    std::destroy_at(this);
    std::construct_at(this, std::move(ostream));
    return *this;
  }

  ~BinaryOStream() {
    if (buf_) {
      Flush();
      close(fd_);
    }
  }

  BinaryOStream &operator<<(const T &row) {
    size_t size;

    if (left_ < sizeof(size_t)) {
      Flush();
    }

    size = row.SerializedSize();
    io::SerializeValue(ptr_, size);
    left_ -= sizeof(size_t);

    if (left_ < size) {
      Flush();
    }

    row.Serialize(ptr_);
    ptr_ += size;
    left_ -= size;

    return *this;
  }

private:
  void Flush() {
    size_t size = ptr_ - buf_.get();
    ptr_ = buf_.get();
    do {
      const size_t written = write(fd_, ptr_, size);
      size -= written;
      ptr_ += written;
    } while (size);

    ptr_ = buf_.get();
    left_ = buffer_size_;
  }

private:
  std::unique_ptr<char[]> buf_;
  char *ptr_;
  size_t buffer_size_ = 0;
  size_t left_ = 0;

  int fd_ = -1;
};

} // namespace io
