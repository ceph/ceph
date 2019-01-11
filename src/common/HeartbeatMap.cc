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

#include <signal.h>

#include "HeartbeatMap.h"
#include "ceph_context.h"
#include "common/errno.h"
#include "debug.h"

#define dout_subsys ceph_subsys_heartbeatmap
#undef dout_prefix
#define dout_prefix *_dout << "heartbeat_map "

namespace ceph {

HeartbeatMap::HeartbeatMap(CephContext *cct)
  : m_cct(cct),
    m_rwlock("HeartbeatMap::m_rwlock"),
    m_unhealthy_workers(0),
    m_total_workers(0)
{
}

HeartbeatMap::~HeartbeatMap()
{
  ceph_assert(m_workers.empty());
}

heartbeat_handle_d *HeartbeatMap::add_worker(const string& name, pthread_t thread_id)
{
  m_rwlock.get_write();
  ldout(m_cct, 10) << "add_worker '" << name << "'" << dendl;
  heartbeat_handle_d *h = new heartbeat_handle_d(name);
  ANNOTATE_BENIGN_RACE_SIZED(&h->timeout, sizeof(h->timeout),
                             "heartbeat_handle_d timeout");
  ANNOTATE_BENIGN_RACE_SIZED(&h->suicide_timeout, sizeof(h->suicide_timeout),
                             "heartbeat_handle_d suicide_timeout");
  m_workers.push_front(h);
  h->list_item = m_workers.begin();
  h->thread_id = thread_id;
  m_rwlock.put_write();
  return h;
}

void HeartbeatMap::remove_worker(const heartbeat_handle_d *h)
{
  m_rwlock.get_write();
  ldout(m_cct, 10) << "remove_worker '" << h->name << "'" << dendl;
  m_workers.erase(h->list_item);
  m_rwlock.put_write();
  delete h;
}

bool HeartbeatMap::_check(const heartbeat_handle_d *h, const char *who,
			  ceph::coarse_mono_clock::rep now)
{
  bool healthy = true;
  auto was = h->timeout.load();
  if (was && was < now) {
    ldout(m_cct, 1) << who << " '" << h->name << "'"
		    << " had timed out after " << h->grace << dendl;
    healthy = false;
  }
  was = h->suicide_timeout;
  if (was && was < now) {
    ldout(m_cct, 1) << who << " '" << h->name << "'"
		    << " had suicide timed out after " << h->suicide_grace << dendl;
    pthread_kill(h->thread_id, SIGABRT);
    sleep(1);
    ceph_abort_msg("hit suicide timeout");
  }
  return healthy;
}

void HeartbeatMap::reset_timeout(heartbeat_handle_d *h,
				 ceph::coarse_mono_clock::rep grace,
				 ceph::coarse_mono_clock::rep suicide_grace)
{
  ldout(m_cct, 20) << "reset_timeout '" << h->name << "' grace " << grace
		   << " suicide " << suicide_grace << dendl;
  auto now = chrono::duration_cast<chrono::seconds>(
	       ceph::coarse_mono_clock::now().time_since_epoch()).count();
  _check(h, "reset_timeout", now);

  h->timeout = now + grace;
  h->grace = grace;

  if (suicide_grace)
    h->suicide_timeout = now + suicide_grace;
  else
    h->suicide_timeout = 0;
  h->suicide_grace = suicide_grace;
}

void HeartbeatMap::clear_timeout(heartbeat_handle_d *h)
{
  ldout(m_cct, 20) << "clear_timeout '" << h->name << "'" << dendl;
  auto now = chrono::duration_cast<std::chrono::seconds>(
	       ceph::coarse_mono_clock::now().time_since_epoch()).count();
  _check(h, "clear_timeout", now);
  h->timeout = 0;
  h->suicide_timeout = 0;
}

bool HeartbeatMap::is_healthy()
{
  int unhealthy = 0;
  int total = 0;
  m_rwlock.get_read();
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

  for (list<heartbeat_handle_d*>::iterator p = m_workers.begin();
       p != m_workers.end();
       ++p) {
    heartbeat_handle_d *h = *p;
    auto epoch = chrono::duration_cast<chrono::seconds>(now.time_since_epoch()).count();
    if (!_check(h, "is_healthy", epoch)) {
      healthy = false;
      unhealthy++;
    }
    total++;
  }
  m_rwlock.put_read();

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
      ::utimes(path.c_str(), NULL);
      ::close(fd);
    } else {
      ldout(m_cct, 0) << "unable to touch " << path << ": "
                     << cpp_strerror(errno) << dendl;
    }
  }
}

}
