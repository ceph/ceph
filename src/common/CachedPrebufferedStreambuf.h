#ifndef CEPH_COMMON_CACHED_PREBUFFEREDSTREAMBUF_H
#define CEPH_COMMON_CACHED_PREBUFFEREDSTREAMBUF_H

#include <streambuf>
#include <atomic>
#include <ostream>

/**
 * streambuf using existing buffer, overflowing into a std::string
 *
 * A simple streambuf that uses a preallocated buffer for small
 * strings, and overflows into a std::string when necessary.  If the
 * preallocated buffer size is chosen well, we can optimize for the
 * common case and overflow to a slower heap-allocated buffer when
 * necessary.
 */
struct prebuffered_data
{
private:
  char *m_buf;
  size_t m_buf_len;
  char *m_pptr;
  std::string m_overflow;

public:
  prebuffered_data(char* buf, size_t buf_len)
  : m_buf(buf), m_buf_len(buf_len), m_pptr(nullptr) {}

  /// return a string copy (inefficiently)
  std::string get_str() const;

  // returns current size of content
  size_t size() const;

  // extracts up to avail chars of content
  int snprintf(char* dst, size_t avail) const;
  friend class CachedPrebufferedStreambuf;
};

class CachedPrebufferedStreambuf final : public std::streambuf
{
public:
  static CachedPrebufferedStreambuf* create(prebuffered_data* d);

  std::ostream& get_ostream() {
    return os;
  }

  // called when the buffer fills up
  int_type overflow(int_type c) override;

  // called when we read and need more data
  int_type underflow() override;

  // signals that formatting log has finished
  void finish();

private:
  CachedPrebufferedStreambuf()
    : data(nullptr), os(this) {}
  ~CachedPrebufferedStreambuf();

  // determines if instance is currently used for formatting log
  bool in_use() const {
    return data != nullptr;
  }

  prebuffered_data* data;
  std::ostream os;
  friend class cached_os_t;
};

#endif
