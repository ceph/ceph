// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef __CEPH_LOG_ENTRY_H
#define __CEPH_LOG_ENTRY_H

#include "common/CachedPrebufferedStreambuf.h"
#include <pthread.h>
#include <string>
#include "log/LogClock.h"


namespace ceph {
namespace logging {

struct Entry {
  log_time m_stamp;
  pthread_t m_thread;
  short m_prio, m_subsys;
  Entry *m_next;

  size_t m_buf_len;
  size_t* m_exp_len;
  char m_static_buf[1];

  prebuffered_data m_data;
  CachedPrebufferedStreambuf* m_streambuf;

  Entry()
    : Entry(log_time{}, 0, 0, 0, nullptr)
  {}
  Entry(log_time s, pthread_t t, short pr, short sub,
	const char *msg = NULL)
    : Entry(s, t, pr, sub, m_static_buf, sizeof(m_static_buf), nullptr,
	    msg)
  {}
  Entry(log_time s, pthread_t t, short pr, short sub, char* buf, size_t buf_len, size_t* exp_len,
	const char *msg = NULL)
    : m_stamp(s), m_thread(t), m_prio(pr), m_subsys(sub),
      m_next(NULL),
      m_buf_len(buf_len),
      m_exp_len(exp_len),
      m_data(buf, buf_len),
      m_streambuf(CachedPrebufferedStreambuf::create(&m_data))
  {
    if (msg) {
      get_ostream() << msg;
    }
  }

private:
  ~Entry() = default;

public:
  std::ostream& get_ostream() {
    return m_streambuf->get_ostream();
  }

  // function improves estimate for expected size of message
  void hint_size() {
    if (m_exp_len != NULL) {
      size_t size = m_data.size();
      if (size > __atomic_load_n(m_exp_len, __ATOMIC_RELAXED)) {
        //log larger then expected, just expand
        __atomic_store_n(m_exp_len, size + 10, __ATOMIC_RELAXED);
      }
      else {
        //asymptotically adapt expected size to message size
        __atomic_store_n(m_exp_len, (size + 10 + m_buf_len*31) / 32, __ATOMIC_RELAXED);
      }
    }
  }

  void set_str(const std::string &s) {
    get_ostream() << s;
  }

  std::string get_str() const {
    return m_data.get_str();
  }

  // returns current size of content
  size_t size() const {
    return m_data.size();
  }

  // extracts up to avail chars of content
  int snprintf(char* dst, size_t avail) const {
    return m_data.snprintf(dst, avail);
  }

  void finish() {
    m_streambuf->finish();
  }

  void destroy() {
    if (m_exp_len != NULL) {
      this->~Entry();
      ::operator delete(this);
    } else {
      delete(this);
    }
  }
};

}
}

#endif
