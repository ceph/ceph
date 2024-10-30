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

#include <utime.h>
#include <signal.h>

#include "HeartbeatMap.h"
#include "ceph_context.h"
#include "common/errno.h"
#include "common/valgrind.h"
#include "debug.h"

#define dout_subsys ceph_subsys_heartbeatmap
#undef dout_prefix
#define dout_prefix *_dout << "heartbeat_map "

using std::chrono::duration_cast;
using std::chrono::seconds;
using std::string;

namespace ceph {

HeartbeatMap::HeartbeatMap(CephContext *cct)
  : m_cct(cct),
    m_unhealthy_workers(0),
    m_total_workers(0)
{
}

HeartbeatMap::~HeartbeatMap()
{
  ceph_assert(m_workers.empty());
}

heartbeat_handle_d *HeartbeatMap::add_worker(string&& name, pthread_t thread_id)
{
  std::unique_lock locker{m_rwlock};
  ldout(m_cct, 10) << "add_worker '" << name << "'" << dendl;
  heartbeat_handle_d *h = new heartbeat_handle_d(std::move(name));
  ANNOTATE_BENIGN_RACE_SIZED(&h->timeout, sizeof(h->timeout),
                             "heartbeat_handle_d timeout");
  ANNOTATE_BENIGN_RACE_SIZED(&h->suicide_timeout, sizeof(h->suicide_timeout),
                             "heartbeat_handle_d suicide_timeout");
  m_workers.push_front(h);
  h->list_item = m_workers.begin();
  h->thread_id = thread_id;
  return h;
}

void HeartbeatMap::remove_worker(const heartbeat_handle_d *h)
{
  std::unique_lock locker{m_rwlock};
  ldout(m_cct, 10) << "remove_worker '" << h->name << "'" << dendl;
  m_workers.erase(h->list_item);
  delete h;
}

bool HeartbeatMap::_check(const heartbeat_handle_d *h, const char *who,
			  ceph::coarse_mono_time now)
{
  bool healthy = true;
  if (auto was = h->timeout.load(std::memory_order_relaxed);
      !clock::is_zero(was) && was < now) {
    ldout(m_cct, 1) << who << " '" << h->name << "'"
		    << " had timed out after " << h->grace << dendl;
    healthy = false;
  }
  if (auto was = h->suicide_timeout.load(std::memory_order_relaxed);
      !clock::is_zero(was) && was < now) {
    ldout(m_cct, 1) << who << " '" << h->name << "'"
		    << " had suicide timed out after " << h->suicide_grace << dendl;
    pthread_kill(h->thread_id, SIGABRT);
    sleep(1);
    ceph_abort_msg("hit suicide timeout");
  }
  return healthy;
}

void HeartbeatMap::reset_timeout(heartbeat_handle_d *h,
				 ceph::timespan grace,
				 ceph::timespan suicide_grace)
{
  ldout(m_cct, 20) << "reset_timeout '" << h->name << "' grace " << grace
		   << " suicide " << suicide_grace << dendl;
  const auto now = clock::now();
  _check(h, "reset_timeout", now);

  h->timeout.store(now + grace, std::memory_order_relaxed);
  h->grace = grace;

  if (suicide_grace > ceph::timespan::zero()) {
    h->suicide_timeout.store(now + suicide_grace, std::memory_order_relaxed);
  } else {
    h->suicide_timeout.store(clock::zero(), std::memory_order_relaxed);
  }
  h->suicide_grace = suicide_grace;
}

void HeartbeatMap::clear_timeout(heartbeat_handle_d *h)
{
  ldout(m_cct, 20) << "clear_timeout '" << h->name << "'" << dendl;
  auto now = clock::now();
  _check(h, "clear_timeout", now);
  h->timeout.store(clock::zero(), std::memory_order_relaxed);
  h->suicide_timeout.store(clock::zero(), std::memory_order_relaxed);
}

bool HeartbeatMap::is_healthy()
{
  int unhealthy = 0;
  int total = 0;
  m_rwlock.lock_shared();
  auto now = ceph::coarse_mono_clock::now();
  if (m_cct->_conf->heartbeat_inject_failure) {
    ldout(m_cct, 0) << "is_healthy injecting failure for next " << m_cct->_conf->heartbeat_inject_failure << " seconds" << dendl;
    m_inject_unhealthy_until = now + std::chrono::seconds(m_cct->_conf->heartbeat_inject_failure);
    m_cct->_conf.set_val("heartbeat_inject_failure", "0");
  }

  bool healthy = true;
  if (now < m_inject_unhealthy_until) {
    auto sec = std::chrono::duration_cast<std::chrono::seconds>(m_inject_unhealthy_until - now).count();
    ldout(m_cct, 0) << "is_healthy = false, injected failure for next "
                    << sec << " seconds" << dendl;
    healthy = false;
  }

  for (auto p = m_workers.begin();
       p != m_workers.end();
       ++p) {
    heartbeat_handle_d *h = *p;
    if (!_check(h, "is_healthy", now)) {
      healthy = false;
      unhealthy++;
    }
    total++;
  }
  m_rwlock.unlock_shared();

  m_unhealthy_workers = unhealthy;
  m_total_workers = total;

  ldout(m_cct, 20) << "is_healthy = " << (healthy ? "healthy" : "NOT HEALTHY")
    << ", total workers: " << total << ", number of unhealthy: " << unhealthy << dendl;
  return healthy;
}

int HeartbeatMap::get_unhealthy_workers() const
{
  return m_unhealthy_workers;
}

int HeartbeatMap::get_total_workers() const
{
  return m_total_workers;
}

void HeartbeatMap::check_touch_file()
{
  string path = m_cct->_conf->heartbeat_file;
  if (path.length() && is_healthy()) {
    int fd = ::open(path.c_str(), O_WRONLY|O_CREAT|O_CLOEXEC, 0644);
    if (fd >= 0) {
      ::utime(path.c_str(), NULL);
      ::close(fd);
    } else {
      ldout(m_cct, 0) << "unable to touch " << path << ": "
                     << cpp_strerror(errno) << dendl;
    }
  }
}

}
