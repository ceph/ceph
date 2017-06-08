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

#include <time.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <errno.h>

#include "HeartbeatMap.h"
#include "ceph_context.h"
#include "common/errno.h"
#include "common/valgrind.h"

#include "debug.h"
#define dout_subsys ceph_subsys_heartbeatmap
#undef dout_prefix
#define dout_prefix *_dout << "heartbeat_map "

namespace ceph {

HeartbeatMap::HeartbeatMap(CephContext *cct)
  : m_cct(cct),
    m_rwlock("HeartbeatMap::m_rwlock"),
    m_inject_unhealthy_until(0),
    m_unhealthy_workers(0),
    m_total_workers(0)
{
}

HeartbeatMap::~HeartbeatMap()
{
  assert(m_workers.empty());
}

heartbeat_handle_d *HeartbeatMap::add_worker(const string& name)
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

bool HeartbeatMap::_check(const heartbeat_handle_d *h, const char *who, time_t now)
{
  bool healthy = true;
  time_t was;

  was = h->timeout.read();
  if (was && was < now) {
    ldout(m_cct, 1) << who << " '" << h->name << "'"
		    << " had timed out after " << h->grace << dendl;
    healthy = false;
  }
  was = h->suicide_timeout.read();
  if (was && was < now) {
    ldout(m_cct, 1) << who << " '" << h->name << "'"
		    << " had suicide timed out after " << h->suicide_grace << dendl;
    assert(0 == "hit suicide timeout");
  }
  return healthy;
}

void HeartbeatMap::reset_timeout(heartbeat_handle_d *h, time_t grace, time_t suicide_grace)
{
  ldout(m_cct, 20) << "reset_timeout '" << h->name << "' grace " << grace
		   << " suicide " << suicide_grace << dendl;
  time_t now = time(NULL);
  _check(h, "reset_timeout", now);

  h->timeout.set(now + grace);
  h->grace = grace;

  if (suicide_grace)
    h->suicide_timeout.set(now + suicide_grace);
  else
    h->suicide_timeout.set(0);
  h->suicide_grace = suicide_grace;
}

void HeartbeatMap::clear_timeout(heartbeat_handle_d *h)
{
  ldout(m_cct, 20) << "clear_timeout '" << h->name << "'" << dendl;
  time_t now = time(NULL);
  _check(h, "clear_timeout", now);
  h->timeout.set(0);
  h->suicide_timeout.set(0);
}

bool HeartbeatMap::is_healthy()
{
  int unhealthy = 0;
  int total = 0;
  m_rwlock.get_read();
  time_t now = time(NULL);
  if (m_cct->_conf->heartbeat_inject_failure) {
    ldout(m_cct, 0) << "is_healthy injecting failure for next " << m_cct->_conf->heartbeat_inject_failure << " seconds" << dendl;
    m_inject_unhealthy_until = now + m_cct->_conf->heartbeat_inject_failure;
    m_cct->_conf->set_val("heartbeat_inject_failure", "0");
  }

  bool healthy = true;
  if (now < m_inject_unhealthy_until) {
    ldout(m_cct, 0) << "is_healthy = false, injected failure for next " << (m_inject_unhealthy_until - now) << " seconds" << dendl;
    healthy = false;
  }

  for (list<heartbeat_handle_d*>::iterator p = m_workers.begin();
       p != m_workers.end();
       ++p) {
    heartbeat_handle_d *h = *p;
    if (!_check(h, "is_healthy", now)) {
      healthy = false;
      unhealthy++;
    }
    total++;
  }
  m_rwlock.put_read();

  m_unhealthy_workers.set(unhealthy);
  m_total_workers.set(total);

  ldout(m_cct, 20) << "is_healthy = " << (healthy ? "healthy" : "NOT HEALTHY")
    << ", total workers: " << total << ", number of unhealthy: " << unhealthy << dendl;
  return healthy;
}

int HeartbeatMap::get_unhealthy_workers() const
{
  return m_unhealthy_workers.read();
}

int HeartbeatMap::get_total_workers() const
{
  return m_total_workers.read();
}

void HeartbeatMap::check_touch_file()
{
  if (is_healthy()) {
    string path = m_cct->_conf->heartbeat_file;
    if (path.length()) {
      int fd = ::open(path.c_str(), O_WRONLY|O_CREAT, 0644);
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

}
