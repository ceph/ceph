#ifndef CEPH_COMMON_PREBUFFEREDSTREAMBUF_H
#define CEPH_COMMON_PREBUFFEREDSTREAMBUF_H

#include <iosfwd>
#include <string>
#include <streambuf>

/**
 * streambuf using existing buffer, overflowing into a std::string
 *
 * A simple streambuf that uses a preallocated buffer for small
 * strings, and overflows into a std::string when necessary.  If the
 * preallocated buffer size is chosen well, we can optimize for the
 * common case and overflow to a slower heap-allocated buffer when
 * necessary.
 */
class PrebufferedStreambuf
  : public std::basic_streambuf<char, std::basic_string<char>::traits_type>
{
  char *m_buf;
  size_t m_buf_len;
  std::string m_overflow;

  typedef std::char_traits<char> traits_ty;
  typedef traits_ty::int_type int_type;
  typedef traits_ty::pos_type pos_type;
  typedef traits_ty::off_type off_type;

public:
  PrebufferedStreambuf(char *buf, size_t len);

  // called when the buffer fills up
  int_type overflow(int_type c);

  // called when we read and need more data
  int_type underflow();

  /// return a string copy (inefficiently)
  std::string get_str() const;

  // returns current size of content
  size_t size() const;

  // extracts up to avail chars of content
  int snprintf(char* dst, size_t avail) const;
};

#endif
