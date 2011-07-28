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

#include "HeartbeatMap.h"
#include "ceph_context.h"

#include "debug.h"
#define DOUT_SUBSYS heartbeatmap
#undef dout_prefix
#define dout_prefix *_dout << "heartbeat_map "

namespace ceph {

HeartbeatMap::HeartbeatMap(CephContext *cct)
  : m_cct(cct),
    m_rwlock("HeartbeatMap::m_rwlock")
{
}

HeartbeatMap::~HeartbeatMap()
{
}

heartbeat_handle_d *HeartbeatMap::add_worker(pthread_t thread, string name)
{
  m_rwlock.get_write();
  ldout(m_cct, 10) << "add_worker " << thread << " '" << name << "'" << dendl;
  heartbeat_handle_d *h = new heartbeat_handle_d(thread, name);
  m_workers.push_front(h);
  h->list_item = m_workers.begin();
  m_rwlock.put_write();
  return h;
}

void HeartbeatMap::remove_worker(heartbeat_handle_d *h)
{
  m_rwlock.get_write();
  ldout(m_cct, 10) << "remove_worker " << h->thread << " '" << h->name << "'" << dendl;
  m_workers.erase(h->list_item);
  m_rwlock.put_write();
  delete h;
}

void HeartbeatMap::reset_timeout(heartbeat_handle_d *h, time_t grace)
{
  ldout(m_cct, 20) << "reset_timeout " << h->thread << " grace " << grace << dendl;
  h->timeout = time(NULL) + grace;
}

void HeartbeatMap::clear_timeout(heartbeat_handle_d *h)
{
  ldout(m_cct, 20) << "clear_timeout " << h->thread << dendl;
  h->timeout = 0;
}

bool HeartbeatMap::is_healthy()
{
  m_rwlock.get_read();
  time_t now = time(NULL);
  bool healthy = true;
  for (list<heartbeat_handle_d*>::iterator p = m_workers.begin();
       p != m_workers.end();
       ++p) {
    heartbeat_handle_d *h = *p;
    time_t timeout = h->timeout;
    if (timeout && timeout < now) {
      ldout(m_cct, 0) << "is_healthy " << h->thread << " '" << h->name << "'"
		      << " timed out" << dendl;
      healthy = false;
    }
  }
  m_rwlock.put_read();
  return healthy;
}

}
