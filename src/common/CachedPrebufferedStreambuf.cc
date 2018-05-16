#include "common/CachedPrebufferedStreambuf.h"
#include <string.h>

// std::unique cannot be used here, as deletion will not clear value,
// but thread variable will still exist, causing free-memory-read
struct cached_os_t
{
  CachedPrebufferedStreambuf* streambuf;
  cached_os_t()
    : streambuf(new CachedPrebufferedStreambuf)
  {}
  ~cached_os_t() {
    delete streambuf;
    streambuf = nullptr;
  }
};

thread_local cached_os_t t_os;

CachedPrebufferedStreambuf::~CachedPrebufferedStreambuf()
{
  if (this == t_os.streambuf) {
    // we are deleting thread's PrebufferedStreambuf,
    // clear it so we can create it cleanly again without error
    t_os.streambuf = nullptr;
  }
}

// lock state of streambuf and detach buffer
void CachedPrebufferedStreambuf::finish()
{
  data->m_pptr = this->pptr();
  data = nullptr;
  if (this != t_os.streambuf) {
    // this is extra formatter, not useful anymore
    delete this;
  }
}

CachedPrebufferedStreambuf*
CachedPrebufferedStreambuf::create(prebuffered_data* data)
{
  CachedPrebufferedStreambuf* streambuf;

  if (t_os.streambuf == nullptr || /*this can happen only on process cleanup*/
      t_os.streambuf->in_use() /*this happens when we do recursion in logging*/ ) {
    streambuf = new CachedPrebufferedStreambuf();
  } else {
    streambuf = t_os.streambuf;
    // reset ios flags (failbit, badbit) from previous use
    streambuf->get_ostream().clear();
  }
  streambuf->data = data;
  streambuf->setp(data->m_buf, data->m_buf + data->m_buf_len);
  // so we underflow on first read
  streambuf->setg(0, 0, 0);
  return streambuf;
}

CachedPrebufferedStreambuf::int_type CachedPrebufferedStreambuf::overflow(int_type c)
{
  int old_len = data->m_overflow.size();
  if (old_len == 0) {
    data->m_overflow.resize(80);
  } else {
    data->m_overflow.resize(old_len * 2);
  }
  data->m_overflow[old_len] = c;
  this->setp(&data->m_overflow[old_len + 1], &*data->m_overflow.begin() + data->m_overflow.size());
  return traits_type::not_eof(c);
}

CachedPrebufferedStreambuf::int_type CachedPrebufferedStreambuf::underflow()
{
  if (this->gptr() == 0) {
    // first read; start with the static buffer
    if (!data->m_overflow.empty())
      // there is overflow, so start with entire prealloc buffer
      this->setg(data->m_buf, data->m_buf, data->m_buf + data->m_buf_len);
    else if (this->pptr() == data->m_buf)
      // m_buf is empty
      return traits_type::eof();  // no data
    else
      // set up portion of m_buf we've filled
      this->setg(data->m_buf, data->m_buf, this->pptr());
    return *this->gptr();
  }
  if (this->gptr() == data->m_buf + data->m_buf_len && data->m_overflow.size()) {
    // at end of m_buf; continue with the overflow string
    this->setg(&data->m_overflow[0], &data->m_overflow[0], this->pptr());
    return *this->gptr();
  }

  // otherwise we must be at the end (of m_buf and/or m_overflow)
  return traits_type::eof();
}

/// return a string copy (inefficiently)
std::string prebuffered_data::get_str() const
{
  if (!m_overflow.empty()) {
    std::string s(m_buf, m_buf + m_buf_len);
    s.append(&m_overflow[0], m_pptr - &m_overflow[0]);
    return s;
  } else if (m_pptr == m_buf) {
    return std::string();
  } else {
    return std::string(m_buf, m_pptr - m_buf);
  }
}

// returns current size of content
size_t prebuffered_data::size() const
{
  if (m_overflow.empty()) {
    return m_pptr - m_buf;
  } else {
    return m_buf_len + m_pptr - &m_overflow[0];
  }
}

// extracts up to avail chars of content
int prebuffered_data::snprintf(char* dst, size_t avail) const
{
  size_t len_a;
  size_t len_b;
  if (!m_overflow.empty()) {
    len_a = m_buf_len;
    len_b = m_pptr - &m_overflow[0];
  } else {
    len_a = m_pptr - m_buf;
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
