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

#ifndef CEPH_HEARTBEATMAP_H
#define CEPH_HEARTBEATMAP_H

#include <pthread.h>

#include <string>
#include <list>

#include "RWLock.h"

class CephContext;

namespace ceph {

struct heartbeat_handle_d {
  pthread_t thread;
  std::string name;
  time_t timeout;
  std::list<heartbeat_handle_d*>::iterator list_item;

  heartbeat_handle_d(pthread_t t, const std::string& n)
    : thread(t), name(n),
      timeout(0)
  { }
};

class HeartbeatMap {
 public:
  heartbeat_handle_d *add_worker(pthread_t thread, std::string name);
  void remove_worker(heartbeat_handle_d *h);

  void reset_timeout(heartbeat_handle_d *h, time_t grace);
  void clear_timeout(heartbeat_handle_d *h);

  bool is_healthy();

  HeartbeatMap(CephContext *cct);
  ~HeartbeatMap();

 private:
  CephContext *m_cct;
  RWLock m_rwlock;
  std::list<heartbeat_handle_d*> m_workers;
};

}
#endif
