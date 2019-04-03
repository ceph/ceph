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
  int min_mon_release;
public:
  C_ApplyFeatures(MonmapMonitor *s, const mon_feature_t& f, int mmr) :
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
				       int min_mon_release)
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
    dout(10) << __func__ << " min_mon_release (" << min_mon_release
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
	    << min_mon_release << " (" << ceph_release_name(min_mon_release)
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
  PaxosServiceMessage *m = static_cast<PaxosServiceMessage*>(op->get_req());
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
  MMonCommand *m = static_cast<MMonCommand*>(op->get_req());
  int r = -1;
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

  string format;
  cmd_getval(g_ceph_context, cmdmap, "format", format, string("plain"));
  boost::scoped_ptr<Formatter> f(Formatter::create(format));

  if (prefix == "mon stat") {
    mon->monmap->print_summary(ss);
    ss << ", election epoch " << mon->get_epoch() << ", leader "
       << mon->get_leader() << " " << mon->get_leader_name()
       << ", quorum " << mon->get_quorum() << " " << mon->get_quorum_names();
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
      ceph_assert(r == 0);
      ceph_assert(bl.length() > 0);
      p = new MonMap;
      p->decode(bl);
    }

    ceph_assert(p);

    if (prefix == "mon getmap") {
      p->encode(rdata, m->get_connection()->get_features());
      r = 0;
      ss << "got monmap epoch " << p->get_epoch();
    } else if (prefix == "mon dump") {
      stringstream ds;
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
    if (p != mon->monmap) {
       delete p;
       p = nullptr;
    }

  } else if (prefix == "mon feature ls") {
   
    bool list_with_value = false;
    string with_value;
    if (cmd_getval(g_ceph_context, cmdmap, "with_value", with_value) &&
        with_value == "--with-value") {
      list_with_value = true;
    }

    MonMap *p = mon->monmap;

    // list features
    mon_feature_t supported = ceph::features::mon::get_supported();
    mon_feature_t persistent = ceph::features::mon::get_persistent();
    mon_feature_t required = p->get_required_features();

    stringstream ds;
    auto print_feature = [&](mon_feature_t& m_features, const char* m_str) {
      if (f) {
        if (list_with_value)
          m_features.dump_with_value(f.get(), m_str);
        else
          m_features.dump(f.get(), m_str);
      } else {
        if (list_with_value)
          m_features.print_with_value(ds);
        else
          m_features.print(ds);
      }
    };

    if (f) {
      f->open_object_section("features");

      f->open_object_section("all");
      print_feature(supported, "supported");
      print_feature(persistent, "persistent");
      f->close_section(); // all

      f->open_object_section("monmap");
      print_feature(p->persistent_features, "persistent");
      print_feature(p->optional_features, "optional");
      print_feature(required, "required");
      f->close_section(); // monmap 

      f->close_section(); // features
      f->flush(ds);

    } else {
      ds << "all features" << std::endl
        << "\tsupported: ";
      print_feature(supported, nullptr);
      ds << std::endl
        << "\tpersistent: ";
      print_feature(persistent, nullptr);
      ds << std::endl
        << std::endl;

      ds << "on current monmap (epoch "
         << p->get_epoch() << ")" << std::endl
         << "\tpersistent: ";
      print_feature(p->persistent_features, nullptr);
      ds << std::endl
        // omit optional features in plain-text
        // makes it easier to read, and they're, currently, empty.
	 << "\trequired: ";
      print_feature(required, nullptr);
      ds << std::endl;
    }
    rdata.append(ds);
    r = 0;
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
  MMonCommand *m = static_cast<MMonCommand*>(op->get_req());
  stringstream ss;
  string rs;
  int err = -EINVAL;

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

  ceph_assert(mon->monmap);
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

    entity_addrvec_t addrs;
    if (monmap.persistent_features.contains_all(
	  ceph::features::mon::FEATURE_NAUTILUS)) {
      if (addr.get_port() == CEPH_MON_PORT_IANA) {
	addr.set_type(entity_addr_t::TYPE_MSGR2);
      }
      if (addr.get_port() == CEPH_MON_PORT_LEGACY) {
	// if they specified the *old* default they probably don't care
	addr.set_port(0);
      }
      if (addr.get_port()) {
	addrs.v.push_back(addr);
      } else {
	addr.set_type(entity_addr_t::TYPE_MSGR2);
	addr.set_port(CEPH_MON_PORT_IANA);
	addrs.v.push_back(addr);
	addr.set_type(entity_addr_t::TYPE_LEGACY);
	addr.set_port(CEPH_MON_PORT_LEGACY);
	addrs.v.push_back(addr);
      }
    } else {
      if (addr.get_port() == 0) {
	addr.set_port(CEPH_MON_PORT_LEGACY);
      }
      addr.set_type(entity_addr_t::TYPE_LEGACY);
      addrs.v.push_back(addr);
    }
    dout(20) << __func__ << " addr " << addr << " -> addrs " << addrs << dendl;

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
        if (monmap.get_addrs(name) == addrs) {
          // stable map contains monitor with the same name at the same address.
          // serialize before current pending map.
          err = 0; // for clarity; this has already been set above.
          ss << "mon." << name << " at " << addrs << " already exists";
          goto reply;
        } else {
          ss << "mon." << name
             << " already exists at address " << monmap.get_addrs(name);
        }
      } else if (monmap.contains(addrs)) {
        // we established on the previous branch that name is different
        ss << "mon." << monmap.get_name(addrs)
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

    pending_map.add(name, addrs);
    pending_map.last_changed = ceph_clock_now();
    ss << "adding mon." << name << " at " << addrs;
    propose = true;
    dout(0) << __func__ << " proposing new mon." << name << dendl;

  } else if (prefix == "mon remove" ||
             prefix == "mon rm") {
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

    entity_addrvec_t addrs = pending_map.get_addrs(name);
    pending_map.remove(name);
    pending_map.last_changed = ceph_clock_now();
    ss << "removing mon." << name << " at " << addrs
       << ", there will be " << pending_map.size() << " monitors" ;
    propose = true;
    err = 0;

  } else if (prefix == "mon feature set") {

    /* PLEASE NOTE:
     *
     * We currently only support setting/unsetting persistent features.
     * This is by design, given at the moment we still don't have optional
     * features, and, as such, there is no point introducing an interface
     * to manipulate them. This allows us to provide a cleaner, more
     * intuitive interface to the user, modifying solely persistent
     * features.
     *
     * In the future we should consider adding another interface to handle
     * optional features/flags; e.g., 'mon feature flag set/unset', or
     * 'mon flag set/unset'.
     */
    string feature_name;
    if (!cmd_getval(g_ceph_context, cmdmap, "feature_name", feature_name)) {
      ss << "missing required feature name";
      err = -EINVAL;
      goto reply;
    }

    mon_feature_t feature;
    feature = ceph::features::mon::get_feature_by_name(feature_name);
    if (feature == ceph::features::mon::FEATURE_NONE) {
      ss << "unknown feature '" << feature_name << "'";
      err = -ENOENT;
      goto reply;
    }

    bool sure = false;
    cmd_getval(g_ceph_context, cmdmap, "yes_i_really_mean_it", sure);
    if (!sure) {
      ss << "please specify '--yes-i-really-mean-it' if you "
         << "really, **really** want to set feature '"
         << feature << "' in the monmap.";
      err = -EPERM;
      goto reply;
    }

    if (!mon->get_quorum_mon_features().contains_all(feature)) {
      ss << "current quorum does not support feature '" << feature
         << "'; supported features: "
         << mon->get_quorum_mon_features();
      err = -EINVAL;
      goto reply;
    }

    ss << "setting feature '" << feature << "'";

    err = 0;
    if (monmap.persistent_features.contains_all(feature)) {
      dout(10) << __func__ << " feature '" << feature
               << "' already set on monmap; no-op." << dendl;
      goto reply;
    }

    pending_map.persistent_features.set_feature(feature);
    pending_map.last_changed = ceph_clock_now();
    propose = true;

    dout(1) << __func__ << " " << ss.str() << "; new features will be: "
            << "persistent = " << pending_map.persistent_features
            // output optional nevertheless, for auditing purposes.
            << ", optional = " << pending_map.optional_features << dendl;

  } else if (prefix == "mon set-rank") {
    string name;
    int64_t rank;
    if (!cmd_getval(g_ceph_context, cmdmap, "name", name) ||
	!cmd_getval(g_ceph_context, cmdmap, "rank", rank)) {
      err = -EINVAL;
      goto reply;
    }
    int oldrank = pending_map.get_rank(name);
    if (oldrank < 0) {
      ss << "mon." << name << " does not exist in monmap";
      err = -ENOENT;
      goto reply;
    }
    err = 0;
    pending_map.set_rank(name, rank);
    pending_map.last_changed = ceph_clock_now();
    propose = true;
  } else if (prefix == "mon set-addrs") {
    string name;
    string addrs;
    if (!cmd_getval(g_ceph_context, cmdmap, "name", name) ||
	!cmd_getval(g_ceph_context, cmdmap, "addrs", addrs)) {
      err = -EINVAL;
      goto reply;
    }
    if (!pending_map.contains(name)) {
      ss << "mon." << name << " does not exist";
      err = -ENOENT;
      goto reply;
    }
    entity_addrvec_t av;
    if (!av.parse(addrs.c_str(), nullptr)) {
      ss << "failed to parse addrs '" << addrs << "'";
      err = -EINVAL;
      goto reply;
    }
    for (auto& a : av.v) {
      a.set_nonce(0);
      if (!a.get_port()) {
	ss << "monitor must bind to a non-zero port, not " << a;
	err = -EINVAL;
	goto reply;
      }
    }
    err = 0;
    pending_map.set_addrvec(name, av);
    pending_map.last_changed = ceph_clock_now();
    propose = true;
  } else if (prefix == "mon set-weight") {
    string name;
    int64_t weight;
    if (!cmd_getval(g_ceph_context, cmdmap, "name", name) ||
        !cmd_getval(g_ceph_context, cmdmap, "weight", weight)) {
      err = -EINVAL;
      goto reply;
    }
    if (!pending_map.contains(name)) {
      ss << "mon." << name << " does not exist";
      err = -ENOENT;
      goto reply;
    }
    err = 0;
    pending_map.set_weight(name, weight);
    pending_map.last_changed = ceph_clock_now();
    propose = true;
  } else if (prefix == "mon enable-msgr2") {
    if (!monmap.get_required_features().contains_all(
	  ceph::features::mon::FEATURE_NAUTILUS)) {
      err = -EACCES;
      ss << "all monitors must be running nautilus to enable v2";
      goto reply;
    }
    for (auto& i : pending_map.mon_info) {
      if (i.second.public_addrs.v.size() == 1 &&
	  i.second.public_addrs.front().is_legacy() &&
	  i.second.public_addrs.front().get_port() == CEPH_MON_PORT_LEGACY) {
	entity_addrvec_t av;
	entity_addr_t a = i.second.public_addrs.front();
	a.set_type(entity_addr_t::TYPE_MSGR2);
	a.set_port(CEPH_MON_PORT_IANA);
	av.v.push_back(a);
	av.v.push_back(i.second.public_addrs.front());
	dout(10) << " setting mon." << i.first
		 << " addrs " << i.second.public_addrs
		 << " -> " << av << dendl;
	pending_map.set_addrvec(i.first, av);
	propose = true;
	pending_map.last_changed = ceph_clock_now();
      }
    }
    err = 0;
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
  MMonJoin *join = static_cast<MMonJoin*>(op->get_req());
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
