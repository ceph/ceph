// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef __CEPH_LOG_ENTRY_H
#define __CEPH_LOG_ENTRY_H

#include "include/utime.h"
#include "common/PrebufferedStreambuf.h"
#include <pthread.h>
#include <string>

#define CEPH_LOG_ENTRY_PREALLOC 80

namespace ceph {
namespace log {

struct Entry {
  utime_t m_stamp;
  pthread_t m_thread;
  short m_prio, m_subsys;
  Entry *m_next;

  char m_static_buf[CEPH_LOG_ENTRY_PREALLOC];
  PrebufferedStreambuf m_streambuf;

  Entry()
    : m_thread(0), m_prio(0), m_subsys(0),
      m_next(NULL),
      m_streambuf(m_static_buf, sizeof(m_static_buf))
  {}
  Entry(utime_t s, pthread_t t, short pr, short sub,
	const char *msg = NULL)
    : m_stamp(s), m_thread(t), m_prio(pr), m_subsys(sub),
      m_next(NULL),
      m_streambuf(m_static_buf, sizeof(m_static_buf))
  {
    if (msg) {
      ostream os(&m_streambuf);
      os << msg;
    }
  }

  void set_str(const std::string &s) {
    ostream os(&m_streambuf);
    os << s;
  }

  std::string get_str() const {
    return m_streambuf.get_str();
  }
};

}
}

#endif
