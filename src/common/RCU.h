// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2011 Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software 
 * Foundation.  See file COPYING.
 * 
 */

#include <urcu.h>
#include <pthread.h>

#ifndef CEPH_RCU_H
#define CEPH_RCU_H

namespace ceph {

template <class A=int> class RCU {
public:
  static void RCU_online() {
    rcu_thread_online();
  }
  static void RCU_offline() {
    rcu_thread_offline();
  }
  static void RCU_quiescent_state() {
    rcu_quiescent_state();
  }
  static void RCU_syncrhonize() {
    synchronize_rcu();
  }
  static void RCU_read_lock() {
    rcu_read_lock();
  }
  static void RCU_read_unlock() {
    rcu_read_unlock();
  }
  static void RCU_quiescent() {
    rcu_quiescent_state();
  }
  RCU(pthread_key_t key, const void *value) {
    rcu_register_thread();
    pthread_setspecific(key,value);
  }
  ~RCU() {
    rcu_unregister_thread();
  }
  inline static A *RCU_dereference(A *ptr) {
    return rcu_dereference(ptr);
  }
  inline static void RCU_xchg_and_sync(A **ptr1, A *ptr2) {
    rcu_xchg_pointer(ptr1, ptr2);
    synchronize_rcu();
  }
};
}

#endif
