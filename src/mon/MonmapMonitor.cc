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
#include "MonitorDBStore.h"

#include "messages/MMonCommand.h"
#include "messages/MMonJoin.h"

#include "common/Timer.h"
#include "common/ceph_argparse.h"
#include "common/errno.h"
#include "mon/MDSMonitor.h"
#include "mon/OSDMonitor.h"
#include "mon/PGMonitor.h"

#include <sstream>
#include "common/config.h"
#include "common/cmdparse.h"
#include "include/str_list.h"
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

void MonmapMonitor::update_from_paxos(bool *need_bootstrap)
{
  version_t version = get_last_committed();
  if (version <= mon->monmap->get_epoch())
    return;

  dout(10) << __func__ << " version " << version
	   << ", my v " << mon->monmap->epoch << dendl;
  
  if (need_bootstrap && version != mon->monmap->get_epoch()) {
    dout(10) << " signaling that we need a bootstrap" << dendl;
    *need_bootstrap = true;
  }

  // read and decode
  monmap_bl.clear();
  int ret = get_version(version, monmap_bl);
  assert(ret == 0);
  assert(monmap_bl.length());

  dout(10) << "update_from_paxos got " << version << dendl;
  mon->monmap->decode(monmap_bl);

  if (mon->store->exists("mkfs", "monmap")) {
    MonitorDBStore::TransactionRef t(new MonitorDBStore::Transaction);
    t->erase("mkfs", "monmap");
    mon->store->apply_transaction(t);
  }
}

void MonmapMonitor::create_pending()
{
  pending_map = *mon->monmap;
  pending_map.epoch++;
  pending_map.last_changed = ceph_clock_now(g_ceph_context);
  dout(10) << "create_pending monmap epoch " << pending_map.epoch << dendl;
}

void MonmapMonitor::encode_pending(MonitorDBStore::TransactionRef t)
{
  dout(10) << "encode_pending epoch " << pending_map.epoch << dendl;

  assert(mon->monmap->epoch + 1 == pending_map.epoch ||
	 pending_map.epoch == 1);  // special case mkfs!
  bufferlist bl;
  pending_map.encode(bl, mon->get_quorum_features());

  put_version(t, pending_map.epoch, bl);
  put_last_committed(t, pending_map.epoch);

  // generate a cluster fingerprint, too?
  if (pending_map.epoch == 1) {
    mon->prepare_new_fingerprint(t);
  }
}

void MonmapMonitor::on_active()
{
  if (get_last_committed() >= 1 && !mon->has_ever_joined) {
    // make note of the fact that i was, once, part of the quorum.
    dout(10) << "noting that i was, once, part of an active quorum." << dendl;

    /* This is some form of nasty in-breeding we have between the MonmapMonitor
       and the Monitor itself. We should find a way to get rid of it given our
       new architecture. Until then, stick with it since we are a
       single-threaded process and, truth be told, no one else relies on this
       thing besides us.
     */
    MonitorDBStore::TransactionRef t(new MonitorDBStore::Transaction);
    t->put(Monitor::MONITOR_NAME, "joined", 1);
    mon->store->apply_transaction(t);
    mon->has_ever_joined = true;
  }

  if (mon->is_leader())
    mon->clog->info() << "monmap " << *mon->monmap << "\n";
}

bool MonmapMonitor::preprocess_query(MonOpRequestRef op)
{
  PaxosServiceMessage *m = static_cast<PaxosServiceMessage*>(op->get_req());
  switch (m->get_type()) {
    // READs
  case MSG_MON_COMMAND:
    return preprocess_command(op);
  case MSG_MON_JOIN:
    return preprocess_join(op);
  default:
    assert(0);
    return true;
  }
}

void MonmapMonitor::dump_info(Formatter *f)
{
  f->dump_unsigned("monmap_first_committed", get_first_committed());
  f->dump_unsigned("monmap_last_committed", get_last_committed());
  f->open_object_section("monmap");
  mon->monmap->dump(f);
  f->close_section();
  f->open_array_section("quorum");
  for (set<int>::iterator q = mon->get_quorum().begin(); q != mon->get_quorum().end(); ++q)
    f->dump_int("mon", *q);
  f->close_section();
}

bool MonmapMonitor::preprocess_command(MonOpRequestRef op)
{
  MMonCommand *m = static_cast<MMonCommand*>(op->get_req());
  int r = -1;
  bufferlist rdata;
  stringstream ss;

  map<string, cmd_vartype> cmdmap;
  if (!cmdmap_from_json(m->cmd, &cmdmap, ss)) {
    string rs = ss.str();
    mon->reply_command(op, -EINVAL, rs, rdata, get_last_committed());
    return true;
  }

  string prefix;
  cmd_getval(g_ceph_context, cmdmap, "prefix", prefix);

  MonSession *session = m->get_session();
  if (!session) {
    mon->reply_command(op, -EACCES, "access denied", get_last_committed());
    return true;
  }

  if (prefix == "mon stat") {
    mon->monmap->print_summary(ss);
    ss << ", election epoch " << mon->get_epoch() << ", quorum " << mon->get_quorum()
       << " " << mon->get_quorum_names();
    rdata.append(ss);
    ss.str("");
    r = 0;

  } else if (prefix == "mon getmap" ||
             prefix == "mon dump") {

    epoch_t epoch;
    int64_t epochnum;
    cmd_getval(g_ceph_context, cmdmap, "epoch", epochnum, (int64_t)0);
    epoch = epochnum;

    MonMap *p = mon->monmap;
    if (epoch) {
      bufferlist bl;
      r = get_version(epoch, bl);
      if (r == -ENOENT) {
        ss << "there is no map for epoch " << epoch;
        goto reply;
      }
      assert(r == 0);
      assert(bl.length() > 0);
      p = new MonMap;
      p->decode(bl);
    }

    assert(p != NULL);

    if (prefix == "mon getmap") {
      p->encode(rdata, m->get_connection()->get_features());
      r = 0;
      ss << "got monmap epoch " << p->get_epoch();
    } else if (prefix == "mon dump") {
      string format;
      cmd_getval(g_ceph_context, cmdmap, "format", format, string("plain"));
      stringstream ds;
      boost::scoped_ptr<Formatter> f(Formatter::create(format));
      if (f) {
        f->open_object_section("monmap");
        p->dump(f.get());
        f->open_array_section("quorum");
        for (set<int>::iterator q = mon->get_quorum().begin();
            q != mon->get_quorum().end(); ++q) {
          f->dump_int("mon", *q);
        }
        f->close_section();
        f->close_section();
        f->flush(ds);
        r = 0;
      } else {
        p->print(ds);
        r = 0;
      }
      rdata.append(ds);
      ss << "dumped monmap epoch " << p->get_epoch();
    }
    if (p != mon->monmap)
       delete p;
  }
  else if (prefix == "mon add")
    return false;
  else if (prefix == "mon remove")
    return false;

reply:
  if (r != -1) {
    string rs;
    getline(ss, rs);

    mon->reply_command(op, r, rs, rdata, get_last_committed());
    return true;
  } else
    return false;
}


bool MonmapMonitor::prepare_update(MonOpRequestRef op)
{
  PaxosServiceMessage *m = static_cast<PaxosServiceMessage*>(op->get_req());
  dout(7) << "prepare_update " << *m << " from " << m->get_orig_source_inst() << dendl;
  
  switch (m->get_type()) {
  case MSG_MON_COMMAND:
    return prepare_command(op);
  case MSG_MON_JOIN:
    return prepare_join(op);
  default:
    assert(0);
  }

  return false;
}

bool MonmapMonitor::prepare_command(MonOpRequestRef op)
{
  MMonCommand *m = static_cast<MMonCommand*>(op->get_req());
  stringstream ss;
  string rs;
  int err = -EINVAL;

  map<string, cmd_vartype> cmdmap;
  if (!cmdmap_from_json(m->cmd, &cmdmap, ss)) {
    string rs = ss.str();
    mon->reply_command(op, -EINVAL, rs, get_last_committed());
    return true;
  }

  string prefix;
  cmd_getval(g_ceph_context, cmdmap, "prefix", prefix);

  MonSession *session = m->get_session();
  if (!session) {
    mon->reply_command(op, -EACCES, "access denied", get_last_committed());
    return true;
  }

  if (prefix == "mon add") {
    string name;
    cmd_getval(g_ceph_context, cmdmap, "name", name);
    string addrstr;
    cmd_getval(g_ceph_context, cmdmap, "addr", addrstr);
    entity_addr_t addr;
    bufferlist rdata;

    if (!addr.parse(addrstr.c_str())) {
      err = -EINVAL;
      ss << "addr " << addrstr << "does not parse";
      goto out;
    }

    if (addr.get_port() == 0) {
      ss << "port defaulted to " << CEPH_MON_PORT;
      addr.set_port(CEPH_MON_PORT);
    }

    /**
     * If we have a monitor with the same name and different addr, then EEXIST
     * If we have a monitor with the same addr and different name, then EEXIST
     * If we have a monitor with the same addr and same name, then return as if
     * we had just added the monitor.
     * If we don't have the monitor, add it.
     */

    err = 0;
    if (!ss.str().empty())
      ss << "; ";

    do {
      if (pending_map.contains(addr)) {
        string n = pending_map.get_name(addr);
        if (n == name)
          break;
      } else if (pending_map.contains(name)) {
        entity_addr_t tmp_addr = pending_map.get_addr(name);
        if (tmp_addr == addr)
          break;
      } else {
        break;
      }
      err = -EEXIST;
      ss << "mon." << name << " at " << addr << " already exists";
      goto out;
    } while (false);

    ss << "added mon." << name << " at " << addr;
    if (pending_map.contains(name)) {
      goto out;
    }

    pending_map.add(name, addr);
    pending_map.last_changed = ceph_clock_now(g_ceph_context);
    getline(ss, rs);
    wait_for_finished_proposal(op, new Monitor::C_Command(mon, op, 0, rs,
                                                     get_last_committed() + 1));
    return true;

  } else if (prefix == "mon remove") {
    string name;
    cmd_getval(g_ceph_context, cmdmap, "name", name);
    if (!pending_map.contains(name)) {
      err = 0;
      ss << "mon " << name << " does not exist or has already been removed";
      goto out;
    }

    if (pending_map.size() == 1) {
      err = -EINVAL;
      ss << "error: refusing removal of last monitor " << name;
      goto out;
    }
    entity_addr_t addr = pending_map.get_addr(name);
    pending_map.remove(name);
    pending_map.last_changed = ceph_clock_now(g_ceph_context);
    ss << "removed mon." << name << " at " << addr << ", there are now " << pending_map.size() << " monitors" ;
    getline(ss, rs);
    // send reply immediately in case we get removed
    mon->reply_command(op, 0, rs, get_last_committed());
    return true;
  }
  else
    ss << "unknown command " << prefix;

out:
  getline(ss, rs);
  mon->reply_command(op, err, rs, get_last_committed());
  return false;
}

bool MonmapMonitor::preprocess_join(MonOpRequestRef op)
{
  MMonJoin *join = static_cast<MMonJoin*>(op->get_req());
  dout(10) << "preprocess_join " << join->name << " at " << join->addr << dendl;

  MonSession *session = join->get_session();
  if (!session ||
      !session->is_capable("mon", MON_CAP_W | MON_CAP_X)) {
    dout(10) << " insufficient caps" << dendl;
    return true;
  }

  if (pending_map.contains(join->name) && !pending_map.get_addr(join->name).is_blank_ip()) {
    dout(10) << " already have " << join->name << dendl;
    return true;
  }
  if (pending_map.contains(join->addr) && pending_map.get_name(join->addr) == join->name) {
    dout(10) << " already have " << join->addr << dendl;
    return true;
  }
  return false;
}
bool MonmapMonitor::prepare_join(MonOpRequestRef op)
{
  MMonJoin *join = static_cast<MMonJoin*>(op->get_req());
  dout(0) << "adding/updating " << join->name << " at " << join->addr << " to monitor cluster" << dendl;
  if (pending_map.contains(join->name))
    pending_map.remove(join->name);
  if (pending_map.contains(join->addr))
    pending_map.remove(pending_map.get_name(join->addr));
  pending_map.add(join->name, join->addr);
  pending_map.last_changed = ceph_clock_now(g_ceph_context);
  return true;
}

bool MonmapMonitor::should_propose(double& delay)
{
  delay = 0.0;
  return true;
}

void MonmapMonitor::tick()
{
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
  if (g_conf->mon_warn_on_old_mons && !mon->get_classic_mons().empty()) {
    ostringstream ss;
    ss << "some monitors are running older code";
    summary.push_back(make_pair(HEALTH_WARN, ss.str()));
    if (detail) {
      for (set<int>::const_iterator i = mon->get_classic_mons().begin();
	  i != mon->get_classic_mons().end();
	  ++i) {
	ostringstream ss;
	ss << "mon." << mon->monmap->get_name(*i)
	     << " only supports the \"classic\" command set";
	detail->push_back(make_pair(HEALTH_WARN, ss.str()));
      }
    }
  }
}

int MonmapMonitor::get_monmap(bufferlist &bl)
{
  version_t latest_ver = get_last_committed();
  dout(10) << __func__ << " ver " << latest_ver << dendl;

  if (!mon->store->exists(get_service_name(), stringify(latest_ver)))
    return -ENOENT;

  int err = get_version(latest_ver, bl);
  if (err < 0) {
    dout(1) << __func__ << " error obtaining monmap: "
            << cpp_strerror(err) << dendl;
    return err;
  }
  return 0;
}

int MonmapMonitor::get_monmap(MonMap &m)
{
  dout(10) << __func__ << dendl;
  bufferlist monmap_bl;

  int err = get_monmap(monmap_bl);
  if (err < 0) {
    return err;
  }
  m.decode(monmap_bl);
  return 0;
}
