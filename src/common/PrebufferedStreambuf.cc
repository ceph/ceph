
#include "common/PrebufferedStreambuf.h"
#include <string.h>
PrebufferedStreambuf::PrebufferedStreambuf(char *buf, size_t len)
  : m_buf(buf), m_buf_len(len)
{
  // init output buffer
  this->setp(m_buf, m_buf + m_buf_len);

  // so we underflow on first read
  this->setg(0, 0, 0);
}

PrebufferedStreambuf::int_type PrebufferedStreambuf::overflow(int_type c)
{
  int old_len = m_overflow.size();
  if (old_len == 0) {
    m_overflow.resize(80);
  } else {
    m_overflow.resize(old_len * 2);
  }
  m_overflow[old_len] = c;
  this->setp(&m_overflow[old_len + 1], &*m_overflow.begin() + m_overflow.size());
  return std::char_traits<char>::not_eof(c);
}

PrebufferedStreambuf::int_type PrebufferedStreambuf::underflow()
{
  if (this->gptr() == 0) {
    // first read; start with the static buffer
    if (m_overflow.size())
      // there is overflow, so start with entire prealloc buffer
      this->setg(m_buf, m_buf, m_buf + m_buf_len);
    else if (this->pptr() == m_buf)
      // m_buf is empty
      return traits_ty::eof();  // no data
    else
      // set up portion of m_buf we've filled
      this->setg(m_buf, m_buf, this->pptr());
    return *this->gptr();
  }
  if (this->gptr() == m_buf + m_buf_len && m_overflow.size()) {
    // at end of m_buf; continue with the overflow string
    this->setg(&m_overflow[0], &m_overflow[0], this->pptr());
    return *this->gptr();
  }

  // otherwise we must be at the end (of m_buf and/or m_overflow)
  return traits_ty::eof();
}

std::string PrebufferedStreambuf::get_str() const
{
  if (m_overflow.size()) {
    std::string s(m_buf, m_buf + m_buf_len);
    s.append(&m_overflow[0], this->pptr() - &m_overflow[0]);
    return s;
  } else if (this->pptr() == m_buf) {
    return std::string();
  } else {
    return std::string(m_buf, this->pptr() - m_buf);
  }  
}
// returns current size of content
size_t PrebufferedStreambuf::size() const
{
  if (m_overflow.size() == 0) {
    return this->pptr() - m_buf;
  } else {
    return m_buf_len + this->pptr() - &m_overflow[0];
  }
}

// extracts up to avail chars of content
int PrebufferedStreambuf::snprintf(char* dst, size_t avail) const
{
  size_t o_size = m_overflow.size();
  size_t len_a;
  size_t len_b;
  if (o_size>0) {
    len_a = m_buf_len;
    len_b = this->pptr() - &m_overflow[0];
  } else {
    len_a = this->pptr() - m_buf;
    len_b = 0;
  }
  if (avail > len_a + len_b) {
    memcpy(dst, m_buf, len_a);
    memcpy(dst + m_buf_len, m_overflow.c_str(), len_b);
    dst[len_a + len_b] = 0;
  } else {
    if (avail > len_a) {
      memcpy(dst, m_buf, len_a);
      memcpy(dst + m_buf_len, m_overflow.c_str(), avail - len_a - 1);
      dst[avail - 1] = 0;
    } else {
      memcpy(dst, m_buf, avail - 1);
      dst[avail - 1] = 0;
    }
  }
  return len_a + len_b;
}
