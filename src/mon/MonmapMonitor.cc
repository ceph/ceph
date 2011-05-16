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
#include "mon/MDSMonitor.h"
#include "mon/OSDMonitor.h"
#include "mon/PGMonitor.h"

#include <sstream>
#include "common/config.h"

#define DOUT_SUBSYS mon
#undef dout_prefix
#define dout_prefix _prefix(mon)
static ostream& _prefix(Monitor *mon) {
  return *_dout << "mon." << mon->name << "@" << mon->rank
		<< (mon->is_starting() ? (const char*)"(starting)":(mon->is_leader() ? (const char*)"(leader)":(mon->is_peon() ? (const char*)"(peon)":(const char*)"(?\?)")))
		<< ".monmap v" << mon->monmap->epoch << " ";
}

void MonmapMonitor::create_initial(bufferlist& bl)
{
  pending_map.decode(bl);
  dout(10) << "create_initial was fed epoch " << pending_map.epoch << dendl;
}

bool MonmapMonitor::update_from_paxos()
{
  //check versions to see if there's an update
  version_t paxosv = paxos->get_version();
  if (paxosv <= mon->monmap->epoch) return true;
  //assert(paxosv >= mon->monmap->epoch);

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

  int rank = mon->monmap->get_rank(mon->name);
  if (rank < 0) {
    dout(10) << "Assuming temporary id=mon" << mon->monmap->size() << " for shutdown purposes" << dendl;
    mon->messenger->set_myname(entity_name_t::MON(mon->monmap->size()));
    mon->monmap->add(mon->name, mon->myaddr);
    mon->shutdown();
    return true;
  }

  if (rank != mon->rank) {
    mon->messenger->set_myname(entity_name_t::MON(rank));
    mon->rank = rank;
  }

  // call election?
  if (mon->monmap->size() > 1) {
    mon->call_election();
  } else {
    mon->win_standalone_election();
  }
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

  assert(mon->monmap->epoch + 1 == pending_map.epoch ||
	 pending_map.epoch == 1);  // special case mkfs!
  pending_map.encode(bl);
}

bool MonmapMonitor::preprocess_query(PaxosServiceMessage *m)
{
  switch (m->get_type()) {
    // READs
  case MSG_MON_COMMAND:
    return preprocess_command((MMonCommand*)m);
  default:
    assert(0);
    m->put();
    return true;
  }
}

bool MonmapMonitor::preprocess_command(MMonCommand *m)
{
  int r = -1;
  bufferlist rdata;
  stringstream ss;

  if (m->cmd.size() > 1) {
    if (m->cmd[1] == "stat") {
      mon->monmap->print_summary(ss);
      ss << ", election epoch " << mon->get_epoch() << ", quorum " << mon->get_quorum();
      r = 0;
    }
    else if (m->cmd.size() == 2 && m->cmd[1] == "getmap") {
      mon->monmap->encode(rdata);
      r = 0;
      ss << "got latest monmap";
    }
    else if (m->cmd[1] == "injectargs" && m->cmd.size() == 4) {
      if (m->cmd[2] == "*") {
	for (unsigned i=0; i<mon->monmap->size(); i++)
	  mon->inject_args(mon->monmap->get_inst(i), m->cmd[3]);
	r = 0;
	ss << "ok bcast";
      } else {
	errno = 0;
	int who = strtol(m->cmd[2].c_str(), 0, 10);
	if (!errno && who >= 0) {
	  mon->inject_args(mon->monmap->get_inst(who), m->cmd[3]);
	  r = 0;
	  ss << "ok";
	} else 
	  ss << "specify mon number or *";
      }
    }
    else if (m->cmd[1] == "add")
      return false;
    else if (m->cmd[1] == "remove")
      return false;
  }

  if (r != -1) {
    string rs;
    getline(ss, rs);

    mon->reply_command(m, r, rs, rdata, paxos->get_version());
    return true;
  } else
    return false;
}


bool MonmapMonitor::prepare_update(PaxosServiceMessage *m)
{
  dout(7) << "prepare_update " << *m << " from " << m->get_orig_source_inst() << dendl;
  
  switch (m->get_type()) {
  case MSG_MON_COMMAND:
    return prepare_command((MMonCommand*)m);
  default:
    assert(0);
    m->put();
  }

  return false;
}

bool MonmapMonitor::prepare_command(MMonCommand *m)
{
  stringstream ss;
  string rs;
  int err = -EINVAL;
  if (m->cmd.size() > 1) {
    if (m->cmd.size() == 4 && m->cmd[1] == "add") {
      string name = m->cmd[2];
      entity_addr_t addr;
      bufferlist rdata;

      if (!addr.parse(m->cmd[3].c_str())) {
	err = -EINVAL;
	ss << "addr " << m->cmd[3] << "does not parse";
	goto out;
      }
      if (pending_map.contains(addr) ||
	  pending_map.contains(name)) {
	err = -EEXIST;
	ss << "mon " << name << " " << addr << " already exists";
	goto out;
      }

      pending_map.add(name, addr);
      pending_map.last_changed = g_clock.now();
      ss << "added mon." << name << " at " << addr;
      getline(ss, rs);
      paxos->wait_for_commit(new Monitor::C_Command(mon, m, 0, rs, paxos->get_version()));
      return true;
    }
    else if (m->cmd.size() == 3 && m->cmd[1] == "remove") {
      string name = m->cmd[2];
      if (!pending_map.contains(name)) {
        err = -ENOENT;
        ss << "mon " << name << " does not exist";
        goto out;
      }

      entity_addr_t addr = pending_map.get_addr(name);
      pending_map.remove(name);
      pending_map.last_changed = g_clock.now();
      ss << "removed mon." << name << " at " << addr << ", there are now " << pending_map.size() << " monitors" ;
      getline(ss, rs);
      // send reply immediately in case we get removed
      mon->reply_command(m, 0, rs, paxos->get_version());
      return true;
    }
    else
      ss << "unknown command " << m->cmd[1];
  } else
    ss << "no command?";
  
out:
  getline(ss, rs);
  mon->reply_command(m, err, rs, paxos->get_version());
  return false;
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

enum health_status_t MonmapMonitor::get_health(std::ostream &ss) const
{
  enum health_status_t ret(HEALTH_OK);
  
  int max = mon->monmap->size();
  int actual = mon->get_quorum().size();
  if (actual < max) {
    ret = HEALTH_WARN;
    ss << (max-actual) << " mons down, quorum " << mon->get_quorum();
  }

  return ret;
}
