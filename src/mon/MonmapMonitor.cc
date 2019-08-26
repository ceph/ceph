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
#include "messages/MMonCommand.h"
#include "messages/MMonJoin.h"

#include "common/ceph_argparse.h"
#include "common/errno.h"
#include <sstream>
#include "common/config.h"
#include "common/cmdparse.h"

#include "include/ceph_assert.h"
#include "include/stringify.h"

#include "mon/commands/monmapmon_cmds.h"

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
  dout(10) << __func__ << " using current monmap" << dendl;
  pending_map = *mon->monmap;
  pending_map.epoch = 1;

  if (g_conf()->mon_debug_no_initial_persistent_features) {
    derr << __func__ << " mon_debug_no_initial_persistent_features=true"
	 << dendl;
  } else {
    // initialize with default persistent features for new clusters
    pending_map.persistent_features = ceph::features::mon::get_persistent();
    pending_map.min_mon_release = ceph_release();
  }
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
  ceph_assert(ret == 0);
  ceph_assert(monmap_bl.length());

  dout(10) << __func__ << " got " << version << dendl;
  mon->monmap->decode(monmap_bl);

  if (mon->store->exists("mkfs", "monmap")) {
    auto t(std::make_shared<MonitorDBStore::Transaction>());
    t->erase("mkfs", "monmap");
    mon->store->apply_transaction(t);
  }

  check_subs();

  // make sure we've recorded min_mon_release
  string val;
  if (mon->store->read_meta("min_mon_release", &val) < 0 ||
      val.size() == 0 ||
      atoi(val.c_str()) != (int)ceph_release()) {
    dout(10) << __func__ << " updating min_mon_release meta" << dendl;
    mon->store->write_meta("min_mon_release",
			   stringify(ceph_release()));
  }
}

void MonmapMonitor::create_pending()
{
  pending_map = *mon->monmap;
  pending_map.epoch++;
  pending_map.last_changed = ceph_clock_now();
  dout(10) << __func__ << " monmap epoch " << pending_map.epoch << dendl;
}

void MonmapMonitor::encode_pending(MonitorDBStore::TransactionRef t)
{
  dout(10) << __func__ << " epoch " << pending_map.epoch << dendl;

  ceph_assert(mon->monmap->epoch + 1 == pending_map.epoch ||
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

class C_ApplyFeatures : public Context {
  MonmapMonitor *svc;
  mon_feature_t features;
  ceph_release_t min_mon_release;
public:
  C_ApplyFeatures(MonmapMonitor *s, const mon_feature_t& f, ceph_release_t mmr) :
    svc(s), features(f), min_mon_release(mmr) { }
  void finish(int r) override {
    if (r >= 0) {
      svc->apply_mon_features(features, min_mon_release);
    } else if (r == -EAGAIN || r == -ECANCELED) {
      // discard features if we're no longer on the quorum that
      // established them in the first place.
      return;
    } else {
      ceph_abort_msg("bad C_ApplyFeatures return value");
    }
  }
};

void MonmapMonitor::apply_mon_features(const mon_feature_t& features,
				       ceph_release_t min_mon_release)
{
  if (!is_writeable()) {
    dout(5) << __func__ << " wait for service to be writeable" << dendl;
    wait_for_writeable_ctx(new C_ApplyFeatures(this, features, min_mon_release));
    return;
  }

  // do nothing here unless we have a full quorum
  if (mon->get_quorum().size() < mon->monmap->size()) {
    return;
  }

  ceph_assert(is_writeable());
  ceph_assert(features.contains_all(pending_map.persistent_features));
  // we should never hit this because `features` should be the result
  // of the quorum's supported features. But if it happens, die.
  ceph_assert(ceph::features::mon::get_supported().contains_all(features));

  mon_feature_t new_features =
    (pending_map.persistent_features ^
     (features & ceph::features::mon::get_persistent()));

  if (new_features.empty() &&
      pending_map.min_mon_release == min_mon_release) {
    dout(10) << __func__ << " min_mon_release (" << (int)min_mon_release
	     << ") and features (" << features << ") match" << dendl;
    return;
  }

  if (!new_features.empty()) {
    dout(1) << __func__ << " applying new features "
	    << new_features << ", had " << pending_map.persistent_features
	    << ", will have "
	    << (new_features | pending_map.persistent_features)
	    << dendl;
    pending_map.persistent_features |= new_features;
  }
  if (min_mon_release > pending_map.min_mon_release) {
    dout(1) << __func__ << " increasing min_mon_release to "
	    << ceph::to_integer<int>(min_mon_release) << " (" << min_mon_release
	    << ")" << dendl;
    pending_map.min_mon_release = min_mon_release;
  }

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
    auto t(std::make_shared<MonitorDBStore::Transaction>());
    t->put(Monitor::MONITOR_NAME, "joined", 1);
    mon->store->apply_transaction(t);
    mon->has_ever_joined = true;
  }

  if (mon->is_leader()) {
    mon->clog->debug() << "monmap " << *mon->monmap;
  }

  apply_mon_features(mon->get_quorum_mon_features(),
		     mon->quorum_min_mon_release);
}

bool MonmapMonitor::preprocess_query(MonOpRequestRef op)
{
  auto m = op->get_req<PaxosServiceMessage>();
  switch (m->get_type()) {
    // READs
  case MSG_MON_COMMAND:
    try {
      return preprocess_command(op);
    }
    catch (const bad_cmd_get& e) {
      bufferlist bl;
      mon->reply_command(op, -EINVAL, e.what(), bl, get_last_committed());
      return true;
    }
  case MSG_MON_JOIN:
    return preprocess_join(op);
  default:
    ceph_abort();
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
  auto m = op->get_req<MMonCommand>();
  bufferlist rdata;
  stringstream ss;

  cmdmap_t cmdmap;
  if (!cmdmap_from_json(m->cmd, &cmdmap, ss)) {
    string rs = ss.str();
    mon->reply_command(op, -EINVAL, rs, rdata, get_last_committed());
    return true;
  }

  string prefix;
  cmd_getval(g_ceph_context, cmdmap, "prefix", prefix);

  MonSession *session = op->get_session();
  if (!session) {
    mon->reply_command(op, -EACCES, "access denied", get_last_committed());
    return true;
  }

  list<MonMonReadCommandRef> read_cmds = {
    MonMonReadCommandRef(new MonMonGetMap(mon, this, g_ceph_context)),
    MonMonReadCommandRef(new MonMonStatCommand(mon, this, g_ceph_context)),
    MonMonReadCommandRef(new MonMonFeatureLs(mon, this, g_ceph_context))
  };

  for (auto& cmd: read_cmds) {
    if (cmd->handles_command(prefix)) {
      dout(10) << __func__ << " handling prefix '" << prefix << "'" << dendl;
      bool ret = cmd->preprocess(op, cmdmap, *(mon->monmap));
      ceph_assert(ret == true);
      return true;
    }
  }

  return false;
}


bool MonmapMonitor::prepare_update(MonOpRequestRef op)
{
  auto m = op->get_req<PaxosServiceMessage>();
  dout(7) << __func__ << " " << *m << " from " << m->get_orig_source_inst() << dendl;
  
  switch (m->get_type()) {
  case MSG_MON_COMMAND:
    try {
      return prepare_command(op);
    } catch (const bad_cmd_get& e) {
      bufferlist bl;
      mon->reply_command(op, -EINVAL, e.what(), bl, get_last_committed());
      return true;
    }
  case MSG_MON_JOIN:
    return prepare_join(op);
  default:
    ceph_abort();
  }

  return false;
}

bool MonmapMonitor::prepare_command(MonOpRequestRef op)
{
  auto m = op->get_req<MMonCommand>();
  stringstream ss;
  string rs;

  cmdmap_t cmdmap;
  if (!cmdmap_from_json(m->cmd, &cmdmap, ss)) {
    string rs = ss.str();
    mon->reply_command(op, -EINVAL, rs, get_last_committed());
    return true;
  }

  string prefix;
  cmd_getval(g_ceph_context, cmdmap, "prefix", prefix);

  MonSession *session = op->get_session();
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

  list<MonMonWriteCommandRef> write_cmds = {
    MonMonWriteCommandRef(new MonMonAdd(mon, this, g_ceph_context)),
    MonMonWriteCommandRef(new MonMonRemove(mon, this, g_ceph_context)),
    MonMonWriteCommandRef(new MonMonFeatureSet(mon, this, g_ceph_context)),
    MonMonWriteCommandRef(new MonMonSetRank(mon, this, g_ceph_context)),
    MonMonWriteCommandRef(new MonMonSetAddrs(mon, this, g_ceph_context)),
    MonMonWriteCommandRef(new MonMonSetWeight(mon, this, g_ceph_context)),
    MonMonWriteCommandRef(new MonMonEnableMsgr2(mon, this, g_ceph_context))
  };

  for (auto& cmd: write_cmds) {
    if (!cmd->handles_command(prefix)) {
      continue;
    }
    dout(10) << __func__ << " handling prefix '" << prefix << "'" << dendl;
    bool ret = cmd->prepare(op, cmdmap, pending_map, *(mon->monmap));
    return ret;
  }

  dout(10) << __func__ << " unknown command '" << prefix << "'" << dendl;
  ss << "unknown command '" << prefix << "'";
  getline(ss, rs);
  mon->reply_command(op, -EINVAL, rs, get_last_committed());
  return false;
}

bool MonmapMonitor::preprocess_join(MonOpRequestRef op)
{
  auto join = op->get_req<MMonJoin>();
  dout(10) << __func__ << " " << join->name << " at " << join->addrs << dendl;

  MonSession *session = op->get_session();
  if (!session ||
      !session->is_capable("mon", MON_CAP_W | MON_CAP_X)) {
    dout(10) << " insufficient caps" << dendl;
    return true;
  }

  if (pending_map.contains(join->name) &&
      !pending_map.get_addrs(join->name).front().is_blank_ip()) {
    dout(10) << " already have " << join->name << dendl;
    return true;
  }
  if (pending_map.contains(join->addrs) &&
      pending_map.get_name(join->addrs) == join->name) {
    dout(10) << " already have " << join->addrs << dendl;
    return true;
  }
  return false;
}
bool MonmapMonitor::prepare_join(MonOpRequestRef op)
{
  auto join = op->get_req<MMonJoin>();
  dout(0) << "adding/updating " << join->name
	  << " at " << join->addrs << " to monitor cluster" << dendl;
  if (pending_map.contains(join->name))
    pending_map.remove(join->name);
  if (pending_map.contains(join->addrs))
    pending_map.remove(pending_map.get_name(join->addrs));
  pending_map.add(join->name, join->addrs);
  pending_map.last_changed = ceph_clock_now();
  return true;
}

bool MonmapMonitor::should_propose(double& delay)
{
  delay = 0.0;
  return true;
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

void MonmapMonitor::check_subs()
{
  const string type = "monmap";
  mon->with_session_map([this, &type](const MonSessionMap& session_map) {
      auto subs = session_map.subs.find(type);
      if (subs == session_map.subs.end())
	return;
      for (auto sub : *subs->second) {
	check_sub(sub);
      }
    });
}

void MonmapMonitor::check_sub(Subscription *sub)
{
  const auto epoch = mon->monmap->get_epoch();
  dout(10) << __func__
	   << " monmap next " << sub->next
	   << " have " << epoch << dendl;
  if (sub->next <= epoch) {
    mon->send_latest_monmap(sub->session->con.get());
    if (sub->onetime) {
      mon->with_session_map([sub](MonSessionMap& session_map) {
	  session_map.remove_sub(sub);
	});
    } else {
      sub->next = epoch + 1;
    }
  }
}

void MonmapMonitor::tick()
{
  if (!is_active() ||
      !mon->is_leader()) {
    return;
  }

  if (mon->monmap->created.is_zero()) {
    dout(10) << __func__ << " detected empty created stamp" << dendl;
    utime_t ctime;
    for (version_t v = 1; v <= get_last_committed(); v++) {
      bufferlist bl;
      int r = get_version(v, bl);
      if (r < 0) {
	continue;
      }
      MonMap m;
      auto p = bl.cbegin();
      decode(m, p);
      if (!m.last_changed.is_zero()) {
	dout(10) << __func__ << " first monmap with last_changed is "
		 << v << " with " << m.last_changed << dendl;
	ctime = m.last_changed;
	break;
      }
    }
    if (ctime.is_zero()) {
      ctime = ceph_clock_now();
    }
    dout(10) << __func__ << " updating created stamp to " << ctime << dendl;
    pending_map.created = ctime;
    propose_pending();
  }
}

