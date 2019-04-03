// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#ifndef CACHE_TEST_COMMON_H
#define CACHE_TEST_COMMON_H

#include "common/Mutex.h"
#include "common/Cond.h"

class WaitEvent {
public:
  WaitEvent() : m_signaled(false) {
    pthread_mutex_init(&m_lock, NULL);
    pthread_cond_init(&m_cond, NULL);
  }

  ~WaitEvent() {
    pthread_mutex_destroy(&m_lock);
    pthread_cond_destroy(&m_cond);
  }

  void wait() {
    pthread_mutex_lock(&m_lock);
    while (!m_signaled) {
      pthread_cond_wait(&m_cond, &m_lock);
    }
    m_signaled = false;
    pthread_mutex_unlock(&m_lock);
   }

  void signal() {
    pthread_mutex_lock(&m_lock);
    m_signaled = true;
    pthread_cond_signal(&m_cond);
    pthread_mutex_unlock(&m_lock);
  }
private:
    pthread_mutex_t m_lock;
    pthread_cond_t m_cond;
    bool m_signaled;
};

#endif
