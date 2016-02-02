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
  pending_map.encode(bl, mon->get_quorum_con_features());

  put_version(t, pending_map.epoch, bl);
  put_last_committed(t, pending_map.epoch);

  // generate a cluster fingerprint, too?
  if (pending_map.epoch == 1) {
    mon->prepare_new_fingerprint(t);
  }
}

void MonmapMonitor::apply_mon_features(const mon_feature_t& features)
{
  if (!is_writeable()) {
    dout(5) << __func__ << " wait for service to be writeable" << dendl;
    wait_for_writeable_ctx(new C_ApplyFeatures(this, features));
    return;
  }

  assert(is_writeable());
  assert(features.contains_all(pending_map.persistent_features));

  mon_feature_t new_features =
    (pending_map.persistent_features ^
     (features & ceph::features::mon::get_persistent()));

  if (new_features.empty()) {
    dout(10) << __func__ << " features match current pending: "
             << features << dendl;
    return;
  }

  new_features |= pending_map.persistent_features;

  dout(5) << __func__ << " applying new features to monmap;"
          << " had " << pending_map.persistent_features
          << ", will have " << new_features << dendl;
  pending_map.persistent_features = new_features;
  propose_pending();
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

  /* We should follow the following rules:
   *
   * - 'monmap' is the current, consistent version of the monmap
   * - 'pending_map' is the uncommitted version of the monmap
   *
   * All checks for the current state must be made against 'monmap'.
   * All changes are made against 'pending_map'.
   *
   * If there are concurrent operations modifying 'pending_map', please
   * follow the following rules.
   *
   * - if pending_map has already been changed, the second operation must
   *   wait for the proposal to finish and be run again; This is the easiest
   *   path to guarantee correctness but may impact performance (i.e., it
   *   will take longer for the user to get a reply).
   *
   * - if the result of the second operation can be guaranteed to be
   *   idempotent, the operation may reply to the user once the proposal
   *   finishes; still needs to wait for the proposal to finish.
   *
   * - An operation _NEVER_ returns to the user based on pending state.
   *
   * If an operation does not modify current stable monmap, it may be
   * serialized before current pending map, regardless of any change that
   * has been made to the pending map -- remember, pending is uncommitted
   * state, thus we are not bound by it.
   */

  assert(mon->monmap);
  MonMap &monmap = *mon->monmap;


  /* Please note:
   *
   * Adding or removing monitors may lead to loss of quorum.
   *
   * Because quorum may be lost, it's important to reply something
   * to the user, lest she end up waiting forever for a reply. And
   * no reply will ever be sent until quorum is formed again.
   *
   * On the other hand, this means we're leaking uncommitted state
   * to the user. As such, please be mindful of the reply message.
   *
   * e.g., 'adding monitor mon.foo' is okay ('adding' is an on-going
   * operation and conveys its not-yet-permanent nature); whereas
   * 'added monitor mon.foo' presumes the action has successfully
   * completed and state has been committed, which may not be true.
   */


  bool propose = false;
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
      goto reply;
    }

    if (addr.get_port() == 0) {
      ss << "port defaulted to " << CEPH_MON_PORT;
      addr.set_port(CEPH_MON_PORT);
    }

    /**
     * If we have a monitor with the same name and different addr, then EEXIST
     * If we have a monitor with the same addr and different name, then EEXIST
     * If we have a monitor with the same addr and same name, then wait for
     * the proposal to finish and return success.
     * If we don't have the monitor, add it.
     */

    err = 0;
    if (!ss.str().empty())
      ss << "; ";

    do {
      if (monmap.contains(name)) {
        if (monmap.get_addr(name) == addr) {
          // stable map contains monitor with the same name at the same address.
          // serialize before current pending map.
          err = 0; // for clarity; this has already been set above.
          ss << "mon." << name << " at " << addr << " already exists";
          goto reply;
        } else {
          ss << "mon." << name
             << " already exists at address " << monmap.get_addr(name);
        }
      } else if (monmap.contains(addr)) {
        // we established on the previous branch that name is different
        ss << "mon." << monmap.get_name(addr)
           << " already exists at address " << addr;
      } else {
        // go ahead and add
        break;
      }
      err = -EEXIST;
      goto reply;
    } while (false);

    /* Given there's no delay between proposals on the MonmapMonitor (see
     * MonmapMonitor::should_propose()), there is no point in checking for
     * a mismatch between name and addr on pending_map.
     *
     * Once we established the monitor does not exist in the committed state,
     * we can simply go ahead and add the monitor.
     */

    pending_map.add(name, addr);
    pending_map.last_changed = ceph_clock_now(g_ceph_context);
    ss << "adding mon." << name << " at " << addr;
    propose = true;
    dout(0) << __func__ << " proposing new mon." << name << dendl;
    goto reply;

  } else if (prefix == "mon remove") {
    string name;
    cmd_getval(g_ceph_context, cmdmap, "name", name);
    if (!monmap.contains(name)) {
      err = 0;
      ss << "mon." << name << " does not exist or has already been removed";
      goto reply;
    }

    if (monmap.size() == 1) {
      err = -EINVAL;
      ss << "error: refusing removal of last monitor " << name;
      goto reply;
    }

    /* At the time of writing, there is no risk of races when multiple clients
     * attempt to use the same name. The reason is simple but may not be
     * obvious.
     *
     * In a nutshell, we do not collate proposals on the MonmapMonitor. As
     * soon as we return 'true' below, PaxosService::dispatch() will check if
     * the service should propose, and - if so - the service will be marked as
     * 'proposing' and a proposal will be triggered. The PaxosService class
     * guarantees that once a service is marked 'proposing' no further writes
     * will be handled.
     *
     * The decision on whether the service should propose or not is, in this
     * case, made by MonmapMonitor::should_propose(), which always considers
     * the proposal delay being 0.0 seconds. This is key for PaxosService to
     * trigger the proposal immediately.
     * 0.0 seconds of delay.
     *
     * From the above, there's no point in performing further checks on the
     * pending_map, as we don't ever have multiple proposals in-flight in
     * this service. As we've established the committed state contains the
     * monitor, we can simply go ahead and remove it.
     *
     * Please note that the code hinges on all of the above to be true. It
     * has been true since time immemorial and we don't see a good reason
     * to make it sturdier at this time - mainly because we don't think it's
     * going to change any time soon, lest for any bug that may be unwillingly
     * introduced.
     */

    entity_addr_t addr = pending_map.get_addr(name);
    pending_map.remove(name);
    pending_map.last_changed = ceph_clock_now(g_ceph_context);
    ss << "removing mon." << name << " at " << addr
       << ", there will be " << pending_map.size() << " monitors" ;
    propose = true;
    goto reply;

  } else {
    ss << "unknown command " << prefix;
    err = -EINVAL;
  }

reply:
  getline(ss, rs);
  mon->reply_command(op, err, rs, get_last_committed());
  // we are returning to the user; do not propose.
  return propose;
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
			       list<pair<health_status_t, string> > *detail,
			       CephContext *cct) const
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
