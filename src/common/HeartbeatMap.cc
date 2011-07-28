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
  assert(m_workers.count(thread) == 0);
  heartbeat_handle_d *h = new heartbeat_handle_d(thread, name);
  m_workers[thread] = h;
  m_rwlock.put_write();
  return h;
}

void HeartbeatMap::remove_worker(heartbeat_handle_d *h)
{
  m_rwlock.get_write();
  ldout(m_cct, 10) << "remove_worker " << h->thread << " '" << h->name << "'" << dendl;
  map<pthread_t, heartbeat_handle_d*>::iterator p = m_workers.find(h->thread);
  assert(p != m_workers.end());
  m_workers.erase(p);
  m_rwlock.put_write();
  delete h;
}

void HeartbeatMap::touch_worker(heartbeat_handle_d *h, time_t grace)
{
  ldout(m_cct, 20) << "touch_worker " << h->thread << " grace " << grace << dendl;
  h->timeout = time(NULL) + grace;
}

bool HeartbeatMap::is_healthy()
{
  m_rwlock.get_read();
  time_t now = time(NULL);
  bool healthy = true;
  for (map<pthread_t, heartbeat_handle_d*>::iterator p = m_workers.begin();
       p != m_workers.end();
       ++p)
    if (p->second->timeout && p->second->timeout < now) {
      ldout(m_cct, 0) << "is_healthy " << p->first << " '" << p->second->name << "'"
		      << " timed out" << dendl;
      healthy = false;
    }
  m_rwlock.put_read();
  return healthy;
}

