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
#include "messages/MMonJoin.h"

#include "common/Timer.h"
#include "common/ceph_argparse.h"
#include "mon/MDSMonitor.h"
#include "mon/OSDMonitor.h"
#include "mon/PGMonitor.h"

#include <sstream>
#include "common/config.h"
#include "include/assert.h"

#define dout_subsys ceph_subsys_mon
#undef dout_prefix
#define dout_prefix _prefix(_dout, mon)
static ostream& _prefix(std::ostream *_dout, Monitor *mon) {
  return *_dout << "mon." << mon->name << "@" << mon->rank
		<< "(" << mon->get_state_name()
		<< ").monmap v" << mon->monmap->epoch << " ";
}

void MonmapMonitor::create_initial()
{
  dout(10) << "create_initial using current monmap" << dendl;
  pending_map = *mon->monmap;
  pending_map.epoch = 1;
}

void MonmapMonitor::update_from_paxos()
{
  version_t paxosv = paxos->get_version();
  if (paxosv <= paxos->get_stashed_version() &&
      paxosv <= mon->monmap->get_epoch())
    return;

  dout(10) << "update_from_paxos paxosv " << paxosv
	   << ", my v " << mon->monmap->epoch << dendl;
  
  version_t orig_latest = paxos->get_stashed_version();
  bool need_restart = paxosv != mon->monmap->get_epoch();  
  
  if (paxosv > 0 && (mon->monmap->get_epoch() == 0 ||
		     paxos->get_stashed_version() != paxosv)) {
    bufferlist latest;
    version_t v = paxos->get_stashed(latest);
    if (v) {
      mon->monmap->decode(latest);
    }
  }

  if (paxosv > mon->monmap->get_epoch()) {
    // read and decode
    monmap_bl.clear();
    bool success = paxos->read(paxosv, monmap_bl);
    assert(success);
    dout(10) << "update_from_paxos got " << paxosv << dendl;
    mon->monmap->decode(monmap_bl);

    // save the bufferlist version in the paxos instance as well
    paxos->stash_latest(paxosv, monmap_bl);

    if (orig_latest == 0)
      mon->store->erase_ss("mkfs", "monmap");
  }

  if (need_restart)
    mon->bootstrap();
}

void MonmapMonitor::create_pending()
{
  pending_map = *mon->monmap;
  pending_map.epoch++;
  pending_map.last_changed = ceph_clock_now(g_ceph_context);
  dout(10) << "create_pending monmap epoch " << pending_map.epoch << dendl;
}

void MonmapMonitor::encode_pending(bufferlist& bl)
{
  dout(10) << "encode_pending epoch " << pending_map.epoch << dendl;

  assert(mon->monmap->epoch + 1 == pending_map.epoch ||
	 pending_map.epoch == 1);  // special case mkfs!
  pending_map.encode(bl, mon->get_quorum_features());
}

void MonmapMonitor::on_active()
{
  if (paxos->get_version() >= 1 && !mon->has_ever_joined) {
    // make note of the fact that i was, once, part of the quorum.
    dout(10) << "noting that i was, once, part of an active quorum." << dendl;
    mon->store->put_int(1, "joined");
    mon->has_ever_joined = true;
  }

  if (mon->is_leader())
    mon->clog.info() << "monmap " << *mon->monmap << "\n";
}

bool MonmapMonitor::preprocess_query(PaxosServiceMessage *m)
{
  switch (m->get_type()) {
    // READs
  case MSG_MON_COMMAND:
    return preprocess_command((MMonCommand*)m);
  case MSG_MON_JOIN:
    return preprocess_join((MMonJoin*)m);
  default:
    assert(0);
    m->put();
    return true;
  }
}

void MonmapMonitor::dump_info(Formatter *f)
{
  f->open_object_section("monmap");
  mon->monmap->dump(f);
  f->open_array_section("quorum");
  for (set<int>::iterator q = mon->get_quorum().begin(); q != mon->get_quorum().end(); ++q)
    f->dump_int("mon", *q);
  f->close_section();
  f->close_section();
}

bool MonmapMonitor::preprocess_command(MMonCommand *m)
{
  int r = -1;
  bufferlist rdata;
  stringstream ss;

  MonSession *session = m->get_session();
  if (!session ||
      (!session->caps.get_allow_all() &&
       !session->caps.check_privileges(PAXOS_MONMAP, MON_CAP_R) &&
       !mon->_allowed_command(session, m->cmd))) {
    mon->reply_command(m, -EACCES, "access denied", paxos->get_version());
    return true;
  }

  vector<const char*> args;
  for (unsigned i = 1; i < m->cmd.size(); i++)
    args.push_back(m->cmd[i].c_str());

  if (m->cmd.size() > 1) {
    if (m->cmd[1] == "stat") {
      mon->monmap->print_summary(ss);
      ss << ", election epoch " << mon->get_epoch() << ", quorum " << mon->get_quorum()
	 << " " << mon->get_quorum_names();
      r = 0;
    }
    else if (m->cmd.size() == 2 && m->cmd[1] == "getmap") {
      mon->monmap->encode(rdata, CEPH_FEATURES_ALL);
      r = 0;
      ss << "got latest monmap";
    }
    else if (m->cmd[1] == "dump") {
      string format = "plain";
      string val;
      epoch_t epoch = 0;
      string cmd = args[0];
      for (std::vector<const char*>::iterator i = args.begin()+1; i != args.end(); ) {
	if (ceph_argparse_double_dash(args, i))
	  break;
	else if (ceph_argparse_witharg_daemon(args, i, &val, "-f", "--format",
					      (char*)NULL))
	  format = val;
	else if (!epoch) {
	  long l = parse_pos_long(*i++, &ss);
	  if (l < 0) {
	    r = -EINVAL;
	    goto out;
	  }
	  epoch = l;
	} else
	  i++;
      }

      MonMap *p = mon->monmap;
      if (epoch) {
	/*
	bufferlist b;
	mon->store->get_bl_sn(b,"osdmap_full", epoch);
	if (!b.length()) {
	  p = 0;
	  r = -ENOENT;
	} else {
	  p = new OSDMap;
	  p->decode(b);
	}
	*/
      }
      if (p) {
	stringstream ds;
	if (format == "json") {
	  Formatter *f = new JSONFormatter(true);
	  f->open_object_section("monmap");
	  p->dump(f);
	  f->open_array_section("quorum");
	  for (set<int>::iterator q = mon->get_quorum().begin(); q != mon->get_quorum().end(); ++q)
	    f->dump_int("mon", *q);
	  f->close_section();
	  f->close_section();
	  f->flush(ds);
	  delete f;
	  r = 0;
	} else if (format == "plain") {
	  p->print(ds);
	  r = 0;
	} else {
	  ss << "unrecognized format '" << format << "'";
	  r = -EINVAL;
	}
	if (r == 0) {
	  rdata.append(ds);
	  ss << "dumped monmap epoch " << p->get_epoch();
	}
  	if (p != mon->monmap)
	  delete p;
      }

      

    }
    else if (m->cmd.size() >= 3 && m->cmd[1] == "tell") {
      dout(20) << "got tell: " << m->cmd << dendl;
      if (m->cmd[2] == "*") { // send to all mons and do myself
        for (unsigned i = 0; i < mon->monmap->size(); ++i) {
	  MMonCommand *newm = new MMonCommand(m->fsid, m->version);
	  newm->cmd.insert(newm->cmd.begin(), m->cmd.begin() + 3, m->cmd.end());
	  mon->messenger->send_message(newm, mon->monmap->get_inst(i));
        }
        ss << "bcast to all mons";
        r = 0;
      } else {
        // find target
        long target = parse_pos_long(m->cmd[2].c_str(), &ss);
	if (target < 0) {
	  r = -EINVAL;
	  goto out;
	}
	if (target >= (long)mon->monmap->size()) {
	  ss << "mon." << target << " does not exist";
	  r = -ENOENT;
	  goto out;
	}

	// send to target, or handle if it's me
	stringstream ss;
	MMonCommand *newm = new MMonCommand(m->fsid, m->version);
	newm->cmd.insert(newm->cmd.begin(), m->cmd.begin() + 3, m->cmd.end());
	mon->messenger->send_message(newm, mon->monmap->get_inst(target));
	ss << "fw to mon." << target;
	r = 0;
      }
    }
    else if (m->cmd[1] == "add")
      return false;
    else if (m->cmd[1] == "remove")
      return false;
  }

 out:
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
  case MSG_MON_JOIN:
    return prepare_join((MMonJoin*)m);
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

  MonSession *session = m->get_session();
  if (!session ||
      (!session->caps.get_allow_all() &&
       !session->caps.check_privileges(PAXOS_MONMAP, MON_CAP_R) &&
       !mon->_allowed_command(session, m->cmd))) {
    mon->reply_command(m, -EACCES, "access denied", paxos->get_version());
    return true;
  }

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

      if (addr.get_port() == 0) {
	ss << "port defaulted to " << CEPH_MON_PORT;
	addr.set_port(CEPH_MON_PORT);
      }

      if (pending_map.contains(addr) ||
	  pending_map.contains(name)) {
	err = -EEXIST;
	if (!ss.str().empty())
	  ss << "; ";
	ss << "mon " << name << " " << addr << " already exists";
	goto out;
      }

      pending_map.add(name, addr);
      pending_map.last_changed = ceph_clock_now(g_ceph_context);
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
      pending_map.last_changed = ceph_clock_now(g_ceph_context);
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

bool MonmapMonitor::preprocess_join(MMonJoin *join)
{
  dout(10) << "preprocess_join " << join->name << " at " << join->addr << dendl;

  MonSession *session = join->get_session();
  if (!session ||
      (!session->caps.get_allow_all() &&
       !session->caps.check_privileges(PAXOS_MONMAP, MON_CAP_ALL))) {
    dout(10) << " insufficient caps" << dendl;
    join->put();
    return true;
  }

  if (pending_map.contains(join->name) && !pending_map.get_addr(join->name).is_blank_ip()) {
    dout(10) << " already have " << join->name << dendl;
    join->put();
    return true;
  }
  if (pending_map.contains(join->addr) && pending_map.get_name(join->addr) == join->name) {
    dout(10) << " already have " << join->addr << dendl;
    join->put();
    return true;
  }
  return false;
}
bool MonmapMonitor::prepare_join(MMonJoin *join)
{
  dout(0) << "adding/updating " << join->name << " at " << join->addr << " to monitor cluster" << dendl;
  if (pending_map.contains(join->name))
    pending_map.remove(join->name);
  if (pending_map.contains(join->addr))
    pending_map.remove(pending_map.get_name(join->addr));
  pending_map.add(join->name, join->addr);
  pending_map.last_changed = ceph_clock_now(g_ceph_context);
  join->put();
  return true;
}

bool MonmapMonitor::should_propose(double& delay)
{
  delay = 0.0;
  return true;
}

void MonmapMonitor::tick()
{
  update_from_paxos();
}

void MonmapMonitor::get_health(list<pair<health_status_t, string> >& summary,
			       list<pair<health_status_t, string> > *detail) const
{
  int max = mon->monmap->size();
  int actual = mon->get_quorum().size();
  if (actual < max) {
    ostringstream ss;
    ss << (max-actual) << " mons down, quorum " << mon->get_quorum() << " " << mon->get_quorum_names();
    summary.push_back(make_pair(HEALTH_WARN, ss.str()));
    if (detail) {
      set<int> q = mon->get_quorum();
      for (int i=0; i<max; i++) {
	if (q.count(i) == 0) {
	  ostringstream ss;
	  ss << "mon." << mon->monmap->get_name(i) << " (rank " << i
	     << ") addr " << mon->monmap->get_addr(i)
	     << " is down (out of quorum)";
	  detail->push_back(make_pair(HEALTH_WARN, ss.str()));
	}
      }
    }
  }
}
