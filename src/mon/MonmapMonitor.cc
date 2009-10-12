// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2009 Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software 
 * Foundation.  See file COPYING.
 * 
 */

#include "MonmapMonitor.h"
#include "Monitor.h"
#include "MonitorStore.h"

#include "messages/MMonCommand.h"
#include "common/Timer.h"

#include <sstream>
#include "config.h"

#define DOUT_SUBSYS mon
#undef dout_prefix
#define dout_prefix _prefix(mon)
static ostream& _prefix(Monitor *mon) {
  return *_dout << dbeginl
		<< "mon" << mon->whoami
		<< (mon->is_starting() ? (const char*)"(starting)":(mon->is_leader() ? (const char*)"(leader)":(mon->is_peon() ? (const char*)"(peon)":(const char*)"(?\?)")))
		<< ".client v" << mon->monmap->epoch << " ";
}

void MonmapMonitor::create_initial(bufferlist& bl)
{
  /* Since the MonMap belongs to the Monitor and is initialized
  by it, we don't need to do anything here. */
}

bool MonmapMonitor::update_from_paxos()
{
  //check versions to see if there's an update
  version_t paxosv = paxos->get_version();
  if (paxosv == mon->monmap->epoch) return true;
  assert(paxosv >= mon->monmap->epoch);

  dout(10) << "update_from_paxos paxosv " << paxosv
	   << ", my v " << mon->monmap->epoch << dendl;

  //read and decode
  monmap_bl.clear();
  bool success = paxos->read(paxosv, monmap_bl);
  assert(success);
  dout(10) << "update_from_paxos got " << paxosv << dendl;
  mon->monmap->decode(monmap_bl);

  //save the bufferlist version in the paxos instance as well
  paxos->stash_latest(paxosv, monmap_bl);

  return true;
}

void MonmapMonitor::create_pending()
{
  pending_map = *mon->monmap;
  pending_map.epoch++;
  pending_map.last_changed = g_clock.now();
  dout(10) << "create_pending monmap epoch " << pending_map.epoch << dendl;
}

void MonmapMonitor::encode_pending(bufferlist& bl)
{
  dout(10) << "encode_pending epoch " << pending_map.epoch << dendl;

  assert(mon->monmap->epoch + 1 == pending_map.epoch);
  pending_map.encode(bl);
}

bool MonmapMonitor::preprocess_query(PaxosServiceMessage *m)
{
  return false;
}

bool MonmapMonitor::prepare_update(PaxosServiceMessage *message)
{
  MMonCommand *m = (MMonCommand *) message;
  if (m->cmd[1] != "add") {
    dout(0) << "Unrecognized MonmapMonitor command!" << dendl;
    delete message;
    return false;
  }
  entity_addr_t addr;
  parse_ip_port(m->cmd[2].c_str(), addr);
  bufferlist rdata;
  if (!pending_map.contains(addr)) {
    pending_map.add(addr);
    pending_map.last_changed = g_clock.now();
    mon->reply_command(m, 0, "added mon to map",
		       rdata, paxos->get_version());
  }
  else {
    mon->reply_command(m, -EINVAL, "mon already exists",
		       rdata, paxos->get_version());
  }
  delete message;
  return true;
}

bool MonmapMonitor::should_propose(double& delay)
{
  delay = 0.0;
  return true;
}

void MonmapMonitor::committed()
{
  //Nothing useful to do here.
}

void MonmapMonitor::tick()
{
  update_from_paxos();
}
