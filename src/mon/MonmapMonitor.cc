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

void MonmapMonitor::update_from_paxos()
{
  version_t version = get_version();
  if (version <= mon->monmap->get_epoch())
    return;

  dout(10) << __func__ << " version " << version
	   << ", my v " << mon->monmap->epoch << dendl;
  
  /* It becomes clear here that we used the stashed version as a consistency
   * mechanism. Take the 'if' we use: if our latest committed version is
   * greater than 0 (i.e., exists one), and this version is different from
   * our stashed version, then we will take the stashed monmap as our owm.
   * 
   * This is cleary to address the case in which we have a failure during
   * the old MonitorStore updates. If a stashed version exists and it has
   * a grater value than the last committed version, it means something
   * went awry, and we did stashed a version (either after updating paxos
   * and before proposing a new value, or during paxos itself) but it
   * never became the last committed (for instance, because the system failed
   * in the mean time).
   *
   * We no longer need to address these concerns. We are using transactions
   * now and it should be the Paxos applying them. If the Paxos applies a
   * transaction with the value we proposed, then it will be consistent
   * with the Paxos values themselves. No need to hack our way in the
   * store and create stashed versions to handle inconsistencies that are
   * addressed by our MonitorDBStore.
   *
   * NOTE: this is not entirely true for the remaining services. In this one,
   * the MonmapMonitor, we don't keep incrementals and each version is a full
   * monmap. In the remaining services however, we keep mostly incrementals and
   * we used to stash full versions of each map/summary. We still do it. We
   * just don't need to do it here. Just check the code below and compare it
   * with the code further down the line where we 'get' the latest committed
   * version: it's the same code.
   *
  version_t latest_full = get_version_latest_full();
  if ((latest_full > 0) && (latest_full > mon->monmap->get_epoch())) {
    bufferlist latest_bl;
    int err = get_version_full(latest_full, latest_bl);
    assert(err == 0);
    dout(7) << __func__ << " loading latest full monmap v"
	    << latest_full << dendl;
    if (latest_bl.length() > 0)
      mon->monmap->decode(latest_bl);
  }
   */
  bool need_restart = version != mon->monmap->get_epoch();  

  // read and decode
  monmap_bl.clear();
  int ret = get_version(version, monmap_bl);
  assert(ret == 0);
  assert(monmap_bl.length());

  dout(10) << "update_from_paxos got " << version << dendl;
  mon->monmap->decode(monmap_bl);

  if (exists_key("mfks", get_service_name())) {
    MonitorDBStore::Transaction t;
    erase_mkfs(&t);
    mon->store->apply_transaction(t);
  }

  if (need_restart) {
    mon->bootstrap();
  }
}

void MonmapMonitor::create_pending()
{
  pending_map = *mon->monmap;
  pending_map.epoch++;
  pending_map.last_changed = ceph_clock_now(g_ceph_context);
  dout(10) << "create_pending monmap epoch " << pending_map.epoch << dendl;
}

void MonmapMonitor::encode_pending(MonitorDBStore::Transaction *t)
{
  dout(10) << "encode_pending epoch " << pending_map.epoch << dendl;

  assert(mon->monmap->epoch + 1 == pending_map.epoch ||
	 pending_map.epoch == 1);  // special case mkfs!
  bufferlist bl;
  pending_map.encode(bl, mon->get_quorum_features());

  put_version(t, pending_map.epoch, bl);
  put_last_committed(t, pending_map.epoch);
}

void MonmapMonitor::on_active()
{
  if (get_version() >= 1 && !mon->has_ever_joined) {
    // make note of the fact that i was, once, part of the quorum.
    dout(10) << "noting that i was, once, part of an active quorum." << dendl;

    /* This is some form of nasty in-breeding we have between the MonmapMonitor
       and the Monitor itself. We should find a way to get rid of it given our
       new architecture. Until then, stick with it since we are a
       single-threaded process and, truth be told, no one else relies on this
       thing besides us.
     */
    MonitorDBStore::Transaction t;
    t.put(Monitor::MONITOR_NAME, "joined", 1);
    mon->store->apply_transaction(t);
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
    return preprocess_command(static_cast<MMonCommand*>(m));
  case MSG_MON_JOIN:
    return preprocess_join(static_cast<MMonJoin*>(m));
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

  map<string, cmd_vartype> cmdmap;
  if (!cmdmap_from_json(m->cmd, &cmdmap, ss)) {
    string rs = ss.str();
    mon->reply_command(m, -EINVAL, rs, rdata, get_version());
    return true;
  }

  string prefix;
  cmd_getval(g_ceph_context, cmdmap, "prefix", prefix);

  MonSession *session = m->get_session();
  if (!session ||
      (!session->is_capable("mon", MON_CAP_R) &&
       !mon->_allowed_command(session, cmdmap))) {
    mon->reply_command(m, -EACCES, "access denied", get_version());
    return true;
  }

  if (prefix == "mon stat") {
    mon->monmap->print_summary(ss);
    ss << ", election epoch " << mon->get_epoch() << ", quorum " << mon->get_quorum()
       << " " << mon->get_quorum_names();
    rdata.append(ss);
    ss.str("");
    r = 0;

  } else if (prefix == "mon getmap") {
    mon->monmap->encode(rdata, CEPH_FEATURES_ALL);
    r = 0;
    ss << "got latest monmap";

  } else if (prefix == "mon dump") {
    string format;
    cmd_getval(g_ceph_context, cmdmap, "format", format, string("plain"));
    epoch_t epoch = 0;
    int64_t epochval;
    if (cmd_getval(g_ceph_context, cmdmap, "epoch", epochval))
      epoch = epochval;

    MonMap *p = mon->monmap;
    if (epoch) {
      bufferlist b;
      r = get_version(epoch, b);
      if (r == -ENOENT) {
	p = 0;
      } else {
	p = new MonMap;
	p->decode(b);
      }
    }
    if (p) {
      stringstream ds;
      boost::scoped_ptr<Formatter> f(new_formatter(format));
      if (f) {
	f->open_object_section("monmap");
	p->dump(f.get());
	f->open_array_section("quorum");
	for (set<int>::iterator q = mon->get_quorum().begin(); q != mon->get_quorum().end(); ++q)
	  f->dump_int("mon", *q);
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
      if (p != mon->monmap)
	delete p;
    }
  } else if (prefix == "mon tell") {
    dout(20) << "got tell: " << m->cmd << dendl;
    string whostr;
    cmd_getval(g_ceph_context, cmdmap, "who", whostr);
    vector<string> argvec;
    cmd_getval(g_ceph_context, cmdmap, "args", argvec);

    if (whostr == "*") { // send to all mons and do myself
      for (unsigned i = 0; i < mon->monmap->size(); ++i) {
	MMonCommand *newm = new MMonCommand(m->fsid, m->version);
	newm->cmd.insert(newm->cmd.begin(), argvec.begin(), argvec.end());
	mon->messenger->send_message(newm, mon->monmap->get_inst(i));
      }
      ss << "bcast to all mons";
      r = 0;
    } else {
      // find target.  Ignore error from parsing long as we probably
      // have a string instead
      long who = parse_pos_long(whostr.c_str(), NULL);
      EntityName name;
      if (who < 0) {

	// not numeric; try as name or id, and see if in monmap
	if (!name.from_str(whostr))
	  name.set("mon", whostr);

	if (mon->monmap->contains(name.get_id())) {
	  who = mon->monmap->get_rank(name.get_id());
	} else {
	  ss << "bad mon name \"" << whostr << "\"";
	  r = -ENOENT;
	  goto out;
	}
      } else if (who >= (long)mon->monmap->size()) {
	ss << "mon." << whostr << " does not exist";
	r = -ENOENT;
	goto out;
      }

      // send to target, or handle if it's me
      stringstream ss;
      MMonCommand *newm = new MMonCommand(m->fsid, m->version);
      newm->cmd.insert(newm->cmd.begin(), argvec.begin(), argvec.end());
      mon->messenger->send_message(newm, mon->monmap->get_inst(who));
      ss << "fw to mon." << whostr;
      r = 0;
    }
  }
  else if (prefix == "mon add")
    return false;
  else if (prefix == "mon remove")
    return false;

 out:
  if (r != -1) {
    string rs;
    getline(ss, rs);

    mon->reply_command(m, r, rs, rdata, get_version());
    return true;
  } else
    return false;
}


bool MonmapMonitor::prepare_update(PaxosServiceMessage *m)
{
  dout(7) << "prepare_update " << *m << " from " << m->get_orig_source_inst() << dendl;
  
  switch (m->get_type()) {
  case MSG_MON_COMMAND:
    return prepare_command(static_cast<MMonCommand*>(m));
  case MSG_MON_JOIN:
    return prepare_join(static_cast<MMonJoin*>(m));
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

  map<string, cmd_vartype> cmdmap;
  if (!cmdmap_from_json(m->cmd, &cmdmap, ss)) {
    string rs = ss.str();
    mon->reply_command(m, -EINVAL, rs, get_version());
    return true;
  }

  string prefix;
  cmd_getval(g_ceph_context, cmdmap, "prefix", prefix);

  MonSession *session = m->get_session();
  if (!session ||
      (!session->is_capable("mon", MON_CAP_R) &&
       !mon->_allowed_command(session, cmdmap))) {
    mon->reply_command(m, -EACCES, "access denied", get_version());
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
    wait_for_finished_proposal(new Monitor::C_Command(mon, m, 0, rs, get_version()));
    return true;

  } else if (prefix == "mon remove") {
    string name;
    cmd_getval(g_ceph_context, cmdmap, "name", name);
    if (!pending_map.contains(name)) {
      err = -ENOENT;
      ss << "mon " << name << " does not exist";
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
    mon->reply_command(m, 0, rs, get_version());
    return true;
  }
  else
    ss << "unknown command " << prefix;

out:
  getline(ss, rs);
  mon->reply_command(m, err, rs, get_version());
  return false;
}

bool MonmapMonitor::preprocess_join(MMonJoin *join)
{
  dout(10) << "preprocess_join " << join->name << " at " << join->addr << dendl;

  MonSession *session = join->get_session();
  if (!session ||
      !session->is_capable("mon", MON_CAP_W | MON_CAP_X)) {
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

int MonmapMonitor::get_monmap(bufferlist &bl)
{
  version_t latest_ver = get_last_committed();
  dout(10) << __func__ << " ver " << latest_ver << dendl;

  if (!exists_version(latest_ver))
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
