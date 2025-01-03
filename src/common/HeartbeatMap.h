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

#include <list>
#include <atomic>
#include <string>
#include <pthread.h>

#include "common/ceph_time.h"
#include "common/ceph_mutex.h"
#include "include/common_fwd.h"

namespace ceph {

/*
 * HeartbeatMap -
 *
 * Maintain a set of handles for internal subsystems to periodically
 * check in with a health check and timeout.  Each user can register
 * and get a handle they can use to set or reset a timeout.  
 *
 * A simple is_healthy() method checks for any users who are not within
 * their grace period for a heartbeat.
 */

struct heartbeat_handle_d {
  const std::string name;
  pthread_t thread_id = 0;
  using clock = ceph::coarse_mono_clock;
  using time = ceph::coarse_mono_time;
  std::atomic<time> timeout = clock::zero();
  std::atomic<time> suicide_timeout = clock::zero();
  ceph::timespan grace = ceph::timespan::zero();
  ceph::timespan suicide_grace = ceph::timespan::zero();
  std::list<heartbeat_handle_d*>::iterator list_item;

  explicit heartbeat_handle_d(std::string&& n)
    : name(std::move(n))
  { }
};

class HeartbeatMap {
 public:
  // register/unregister
  heartbeat_handle_d *add_worker(std::string&& name, pthread_t thread_id);
  void remove_worker(const heartbeat_handle_d *h);

  // reset the timeout so that it expects another touch within grace amount of time
  void reset_timeout(heartbeat_handle_d *h,
		     ceph::timespan grace,
		     ceph::timespan suicide_grace);
  // clear the timeout so that it's not checked on
  void clear_timeout(heartbeat_handle_d *h);

  // return false if any of the timeouts are currently expired.
  bool is_healthy();

  // touch cct->_conf->heartbeat_file if is_healthy()
  void check_touch_file();

  // get the number of unhealthy workers
  int get_unhealthy_workers() const;

  // get the number of total workers
  int get_total_workers() const;

  explicit HeartbeatMap(CephContext *cct);
  ~HeartbeatMap();

 private:
  using clock = ceph::coarse_mono_clock;
  CephContext *m_cct;
  ceph::shared_mutex m_rwlock =
    ceph::make_shared_mutex("HeartbeatMap::m_rwlock");
  clock::time_point m_inject_unhealthy_until;
  std::list<heartbeat_handle_d*> m_workers;
  std::atomic<unsigned> m_unhealthy_workers = { 0 };
  std::atomic<unsigned> m_total_workers = { 0 };

  bool _check(const heartbeat_handle_d *h, const char *who,
	      ceph::coarse_mono_time now);
};

}
#endif
