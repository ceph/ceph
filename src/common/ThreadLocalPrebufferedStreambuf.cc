#include "common/ThreadLocalPrebufferedStreambuf.h"
#include <string.h>

// std::unique cannot be used here, as deletion will not clear value,
// but thread variable will still exist, causing free-memory-read
struct unique_PrebufferedStreambuf
{
  ThreadLocalPrebufferedStreambuf* m_streambuf;
  unique_PrebufferedStreambuf() : m_streambuf(new ThreadLocalPrebufferedStreambuf()) {}
  ~unique_PrebufferedStreambuf() {
    delete m_streambuf;
    m_streambuf = nullptr;
  }
};

thread_local unique_PrebufferedStreambuf t_streambuf;

ThreadLocalPrebufferedStreambuf::~ThreadLocalPrebufferedStreambuf()
{
  if (t_streambuf.m_streambuf == this) {
    // we are deleting thread's PrebufferedStreambuf,
    // clear it so we can create it cleanly again without error
    t_streambuf.m_streambuf = nullptr;
  }
}

// lock state of streambuf and detach buffer
void ThreadLocalPrebufferedStreambuf::finish()
{
  m_data->m_pptr = this->pptr();
  m_data = nullptr;
  if (this != t_streambuf.m_streambuf) {
    // this is extra formatter, not useful anymore
    delete this;
  }
}

/// return a string copy (inefficiently)
std::string ThreadLocalPrebufferedStreambuf::get_str() const
{
  m_data->m_pptr = this->pptr();
  return m_data->get_str();
}

// returns current size of content
size_t ThreadLocalPrebufferedStreambuf::size() const
{
  m_data->m_pptr = this->pptr();
  return m_data->size();
}

// extracts up to avail chars of content
int ThreadLocalPrebufferedStreambuf::snprintf(char* dst, size_t avail) const
{
  m_data->m_pptr = this->pptr();
  return m_data->snprintf(dst,avail);
}


ThreadLocalPrebufferedStreambuf* ThreadLocalPrebufferedStreambuf::get_streambuf(prebuffered_data* data)
{
  ThreadLocalPrebufferedStreambuf* streambuf;

  if (t_streambuf.m_streambuf == nullptr /*this can happen only on process cleanup*/
      ||
    t_streambuf.m_streambuf->in_use() /*this happens when we do recursion in logging*/ ) {
    streambuf = new ThreadLocalPrebufferedStreambuf();
  } else {
    streambuf = t_streambuf.m_streambuf;
  }
  streambuf->m_data = data;
  streambuf->setp(data->m_buf, data->m_buf + data->m_buf_len);

  // so we underflow on first read
  streambuf->setg(0, 0, 0);
  return streambuf;
}



ThreadLocalPrebufferedStreambuf::int_type ThreadLocalPrebufferedStreambuf::overflow(int_type c)
{
  int old_len = m_data->m_overflow.size();
  if (old_len == 0) {
    m_data->m_overflow.resize(80);
  } else {
    m_data->m_overflow.resize(old_len * 2);
  }
  m_data->m_overflow[old_len] = c;
  this->setp(&m_data->m_overflow[old_len + 1], &*m_data->m_overflow.begin() + m_data->m_overflow.size());
  return std::char_traits<char>::not_eof(c);
}

ThreadLocalPrebufferedStreambuf::int_type ThreadLocalPrebufferedStreambuf::underflow()
{
  if (this->gptr() == 0) {
    // first read; start with the static buffer
    if (m_data->m_overflow.size())
      // there is overflow, so start with entire prealloc buffer
      this->setg(m_data->m_buf, m_data->m_buf, m_data->m_buf + m_data->m_buf_len);
    else if (this->pptr() == m_data->m_buf)
      // m_buf is empty
      return traits_ty::eof();  // no data
    else
      // set up portion of m_buf we've filled
      this->setg(m_data->m_buf, m_data->m_buf, this->pptr());
    return *this->gptr();
  }
  if (this->gptr() == m_data->m_buf + m_data->m_buf_len && m_data->m_overflow.size()) {
    // at end of m_buf; continue with the overflow string
    this->setg(&m_data->m_overflow[0], &m_data->m_overflow[0], this->pptr());
    return *this->gptr();
  }

  // otherwise we must be at the end (of m_buf and/or m_overflow)
  return traits_ty::eof();
}

/// return a string copy (inefficiently)
std::string prebuffered_data::get_str() const
{
  if (m_overflow.size()) {
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
  if (m_overflow.size() == 0) {
    return m_pptr - m_buf;
  } else {
    return m_buf_len + m_pptr - &m_overflow[0];
  }
}

// extracts up to avail chars of content
int prebuffered_data::snprintf(char* dst, size_t avail) const
{
  size_t o_size = m_overflow.size();
  size_t len_a;
  size_t len_b;
  if (o_size>0) {
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
