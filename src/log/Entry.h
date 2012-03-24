// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef __CEPH_LOG_ENTRY_H
#define __CEPH_LOG_ENTRY_H

#include "include/utime.h"
#include <pthread.h>
#include <string>


namespace ceph {
namespace log {

struct Entry {
  utime_t m_stamp;
  pthread_t m_thread;
  short m_prio, m_subsys;
  Entry *m_next;

  char m_static_buf[80];
  std::string m_str;

  Entry(utime_t s, pthread_t t, short pr, short sub,
	const char *msg = NULL)
    : m_stamp(s), m_thread(t), m_prio(pr), m_subsys(sub),
      m_next(NULL)
  {
    if (msg)
      m_str = msg;
  }
};

}
}

#endif
