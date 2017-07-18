// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2016 John Spray <john.spray@redhat.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 */

#include <boost/tokenizer.hpp>

#include "messages/MMgrBeacon.h"
#include "messages/MMgrMap.h"
#include "messages/MMgrDigest.h"

#include "PGStatService.h"
#include "include/stringify.h"
#include "mgr/MgrContext.h"
#include "OSDMonitor.h"

#include "MgrMonitor.h"

#define dout_subsys ceph_subsys_mon
#undef dout_prefix
#define dout_prefix _prefix(_dout, mon, map)
static ostream& _prefix(std::ostream *_dout, Monitor *mon,
			const MgrMap& mgrmap) {
  return *_dout << "mon." << mon->name << "@" << mon->rank
		<< "(" << mon->get_state_name()
		<< ").mgr e" << mgrmap.get_epoch() << " ";
}


void MgrMonitor::create_initial()
{
  boost::tokenizer<> tok(g_conf->mgr_initial_modules);
  for (auto& m : tok) {
    pending_map.modules.insert(m);
  }
  dout(10) << __func__ << " initial modules " << pending_map.modules << dendl;
}

void MgrMonitor::update_from_paxos(bool *need_bootstrap)
{
  version_t version = get_last_committed();
  if (version != map.epoch) {
    dout(4) << "loading version " << version << dendl;

    bufferlist bl;
    int err = get_version(version, bl);
    assert(err == 0);

    bufferlist::iterator p = bl.begin();
    map.decode(p);

    dout(4) << "active server: " << map.active_addr
	    << "(" << map.active_gid << ")" << dendl;

    ever_had_active_mgr = get_value("ever_had_active_mgr");

    load_health();

    if (map.available) {
      first_seen_inactive = utime_t();
    } else {
      first_seen_inactive = ceph_clock_now();
    }

    check_subs();
  }

  // feed our pet MgrClient
  mon->mgr_client.ms_dispatch(new MMgrMap(map));
}

void MgrMonitor::create_pending()
{
  pending_map = map;
  pending_map.epoch++;
}

health_status_t MgrMonitor::should_warn_about_mgr_down()
{
  utime_t now = ceph_clock_now();
  // we warn if
  //   - we've ever had an active mgr, or
  //   - we have osds AND we've exceeded the grace period
  // which means a new mon cluster and be HEALTH_OK indefinitely as long as
  // no OSDs are ever created.
  if (ever_had_active_mgr ||
      (mon->osdmon()->osdmap.get_num_osds() > 0 &&
       now > mon->monmap->created + g_conf->mon_mgr_mkfs_grace)) {
    health_status_t level = HEALTH_WARN;
    if (first_seen_inactive != utime_t() &&
	now - first_seen_inactive > g_conf->mon_mgr_inactive_grace) {
      level = HEALTH_ERR;
    }
    return level;
  }
  return HEALTH_OK;
}

void MgrMonitor::encode_pending(MonitorDBStore::TransactionRef t)
{
  dout(10) << __func__ << " " << pending_map << dendl;
  bufferlist bl;
  pending_map.encode(bl, mon->get_quorum_con_features());
  put_version(t, pending_map.epoch, bl);
  put_last_committed(t, pending_map.epoch);

  health_check_map_t next;
  if (pending_map.active_gid == 0) {
    auto level = should_warn_about_mgr_down();
    if (level != HEALTH_OK) {
      next.add("MGR_DOWN", level, "no active mgr");
    } else {
      dout(10) << __func__ << " no health warning (never active and new cluster)"
	       << dendl;
    }
  } else {
    put_value(t, "ever_had_active_mgr", 1);
  }
  encode_health(next, t);
}

bool MgrMonitor::check_caps(MonOpRequestRef op, const uuid_d& fsid)
{
  // check permissions
  MonSession *session = op->get_session();
  if (!session)
    return false;
  if (!session->is_capable("mgr", MON_CAP_X)) {
    dout(1) << __func__ << " insufficient caps " << session->caps << dendl;
    return false;
  }
  if (fsid != mon->monmap->fsid) {
    dout(1) << __func__ << " op fsid " << fsid
	    << " != " << mon->monmap->fsid << dendl;
    return false;
  }
  return true;
}

bool MgrMonitor::preprocess_query(MonOpRequestRef op)
{
  PaxosServiceMessage *m = static_cast<PaxosServiceMessage*>(op->get_req());
  switch (m->get_type()) {
    case MSG_MGR_BEACON:
      return preprocess_beacon(op);
    case MSG_MON_COMMAND:
      return preprocess_command(op);
    default:
      mon->no_reply(op);
      derr << "Unhandled message type " << m->get_type() << dendl;
      return true;
  }
}

bool MgrMonitor::prepare_update(MonOpRequestRef op)
{
  PaxosServiceMessage *m = static_cast<PaxosServiceMessage*>(op->get_req());
  switch (m->get_type()) {
    case MSG_MGR_BEACON:
      return prepare_beacon(op);

    case MSG_MON_COMMAND:
      return prepare_command(op);

    default:
      mon->no_reply(op);
      derr << "Unhandled message type " << m->get_type() << dendl;
      return true;
  }
}



class C_Updated : public Context {
  MgrMonitor *mm;
  MonOpRequestRef op;
public:
  C_Updated(MgrMonitor *a, MonOpRequestRef c) :
    mm(a), op(c) {}
  void finish(int r) override {
    if (r >= 0) {
      // Success 
    } else if (r == -ECANCELED) {
      mm->mon->no_reply(op);
    } else {
      mm->dispatch(op);        // try again
    }
  }
};

bool MgrMonitor::preprocess_beacon(MonOpRequestRef op)
{
  MMgrBeacon *m = static_cast<MMgrBeacon*>(op->get_req());
  dout(4) << "beacon from " << m->get_gid() << dendl;

  if (!check_caps(op, m->get_fsid())) {
    // drop it on the floor
    return true;
  }

  // always send this to the leader's prepare_beacon()
  return false;
}

bool MgrMonitor::prepare_beacon(MonOpRequestRef op)
{
  MMgrBeacon *m = static_cast<MMgrBeacon*>(op->get_req());
  dout(4) << "beacon from " << m->get_gid() << dendl;

  // See if we are seeing same name, new GID for the active daemon
  if (m->get_name() == pending_map.active_name
      && m->get_gid() != pending_map.active_gid)
  {
    dout(4) << "Active daemon restart (mgr." << m->get_name() << ")" << dendl;
    mon->clog->info() << "Active manager daemon " << m->get_name()
                      << " restarted";
    drop_active();
  }

  // See if we are seeing same name, new GID for any standbys
  for (const auto &i : pending_map.standbys) {
    const StandbyInfo &s = i.second;
    if (s.name == m->get_name() && s.gid != m->get_gid()) {
      dout(4) << "Standby daemon restart (mgr." << m->get_name() << ")" << dendl;
      mon->clog->debug() << "Standby manager daemon " << m->get_name()
                         << " restarted";
      drop_standby(i.first);
      break;
    }
  }

  last_beacon[m->get_gid()] = ceph::coarse_mono_clock::now();

  // Track whether we modified pending_map
  bool updated = false;

  if (pending_map.active_gid == m->get_gid()) {
    // A beacon from the currently active daemon
    if (pending_map.active_addr != m->get_server_addr()) {
      dout(4) << "learned address " << m->get_server_addr()
	      << " (was " << pending_map.active_addr << ")" << dendl;
      pending_map.active_addr = m->get_server_addr();
      updated = true;
    }

    if (pending_map.get_available() != m->get_available()) {
      dout(4) << "available " << m->get_gid() << dendl;
      mon->clog->info() << "Manager daemon " << pending_map.active_name
                        << " is now available";
      pending_map.available = m->get_available();
      updated = true;
    }
    if (pending_map.available_modules != m->get_available_modules()) {
      dout(4) << "available_modules " << m->get_available_modules()
	      << " (was " << pending_map.available_modules << ")" << dendl;
      pending_map.available_modules = m->get_available_modules();
      updated = true;
    }
  } else if (pending_map.active_gid == 0) {
    // There is no currently active daemon, select this one.
    if (pending_map.standbys.count(m->get_gid())) {
      drop_standby(m->get_gid());
    }
    dout(4) << "selecting new active " << m->get_gid()
	    << " " << m->get_name()
	    << " (was " << pending_map.active_gid << " "
	    << pending_map.active_name << ")" << dendl;
    pending_map.active_gid = m->get_gid();
    pending_map.active_name = m->get_name();
    pending_map.available_modules = m->get_available_modules();

    mon->clog->info() << "Activating manager daemon "
                      << pending_map.active_name;

    updated = true;
  } else {
    if (pending_map.standbys.count(m->get_gid()) > 0) {
      dout(10) << "from existing standby " << m->get_gid() << dendl;
      if (pending_map.standbys[m->get_gid()].available_modules !=
	  m->get_available_modules()) {
	dout(10) << "existing standby " << m->get_gid() << " available_modules "
		 << m->get_available_modules() << " (was "
		 << pending_map.standbys[m->get_gid()].available_modules << ")"
		 << dendl;
	pending_map.standbys[m->get_gid()].available_modules =
	  m->get_available_modules();
	updated = true;
      }
    } else {
      dout(10) << "new standby " << m->get_gid() << dendl;
      mon->clog->debug() << "Standby manager daemon " << m->get_name()
                         << " started";
      pending_map.standbys[m->get_gid()] = {m->get_gid(), m->get_name(),
					    m->get_available_modules()};
      updated = true;
    }
  }

  if (updated) {
    dout(4) << "updating map" << dendl;
    wait_for_finished_proposal(op, new C_Updated(this, op));
  } else {
    dout(10) << "no change" << dendl;
  }

  return updated;
}

void MgrMonitor::check_subs()
{
  const std::string type = "mgrmap";
  if (mon->session_map.subs.count(type) == 0)
    return;
  for (auto sub : *(mon->session_map.subs[type])) {
    check_sub(sub);
  }
}

void MgrMonitor::check_sub(Subscription *sub)
{
  if (sub->type == "mgrmap") {
    if (sub->next <= map.get_epoch()) {
      dout(20) << "Sending map to subscriber " << sub->session->con
	       << " " << sub->session->con->get_peer_addr() << dendl;
      sub->session->con->send_message(new MMgrMap(map));
      if (sub->onetime) {
        mon->session_map.remove_sub(sub);
      } else {
        sub->next = map.get_epoch() + 1;
      }
    }
  } else {
    assert(sub->type == "mgrdigest");
    if (digest_event == nullptr) {
      send_digests();
    }
  }
}

/**
 * Handle digest subscriptions separately (outside of check_sub) because
 * they are going to be periodic rather than version-driven.
 */
void MgrMonitor::send_digests()
{
  cancel_timer();

  if (!is_active()) {
    return;
  }
  dout(10) << __func__ << dendl;

  const std::string type = "mgrdigest";
  if (mon->session_map.subs.count(type) == 0)
    return;

  for (auto sub : *(mon->session_map.subs[type])) {
    dout(10) << __func__ << " sending digest to subscriber " << sub->session->con
	     << " " << sub->session->con->get_peer_addr() << dendl;
    MMgrDigest *mdigest = new MMgrDigest;

    JSONFormatter f;
    mon->get_health_status(true, &f, nullptr, nullptr, nullptr);
    f.flush(mdigest->health_json);
    f.reset();

    std::ostringstream ss;
    mon->get_mon_status(&f, ss);
    f.flush(mdigest->mon_status_json);
    f.reset();

    sub->session->con->send_message(mdigest);
  }

  digest_event = new C_MonContext(mon, [this](int){
      send_digests();
  });
  mon->timer.add_event_after(g_conf->mon_mgr_digest_period, digest_event);
}

void MgrMonitor::cancel_timer()
{
  if (digest_event) {
    mon->timer.cancel_event(digest_event);
    digest_event = nullptr;
  }
}

void MgrMonitor::on_active()
{
  if (mon->is_leader()) {
    mon->clog->debug() << "mgrmap e" << map.epoch << ": " << map;
  }
}

void MgrMonitor::get_health(
  list<pair<health_status_t,string> >& summary,
  list<pair<health_status_t,string> > *detail,
  CephContext *cct) const
{
  // start mgr warnings as soon as the mons and osds are all upgraded,
  // but before the require_luminous osdmap flag is set.  this way the
  // user gets some warning before the osd flag is set and mgr is
  // actually *required*.
  if (!mon->monmap->get_required_features().contains_all(
	ceph::features::mon::FEATURE_LUMINOUS) ||
      !HAVE_FEATURE(mon->osdmon()->osdmap.get_up_osd_features(),
		    SERVER_LUMINOUS)) {
    return;
  }

  if (map.active_gid == 0) {
    auto level = HEALTH_WARN;
    // do not escalate to ERR if they are still upgrading to jewel.
    if (mon->osdmon()->osdmap.require_osd_release >= CEPH_RELEASE_LUMINOUS) {
      utime_t now = ceph_clock_now();
      if (first_seen_inactive != utime_t() &&
	  now - first_seen_inactive > g_conf->mon_mgr_inactive_grace) {
	level = HEALTH_ERR;
      }
    }
    summary.push_back(make_pair(level, "no active mgr"));
  }
}

void MgrMonitor::tick()
{
  if (!is_active() || !mon->is_leader())
    return;

  const auto now = ceph::coarse_mono_clock::now();
  const auto cutoff = now - std::chrono::seconds(g_conf->mon_mgr_beacon_grace);

  // Populate any missing beacons (i.e. no beacon since MgrMonitor
  // instantiation) with the current time, so that they will
  // eventually look laggy if they fail to give us a beacon.
  if (pending_map.active_gid != 0
      && last_beacon.count(pending_map.active_gid) == 0) {
    last_beacon[pending_map.active_gid] = now;
  }
  for (auto s : pending_map.standbys) {
    if (last_beacon.count(s.first) == 0) {
      last_beacon[s.first] = now;
    }
  }

  // Cull standbys first so that any remaining standbys
  // will be eligible to take over from the active if we cull him.
  std::list<uint64_t> dead_standbys;
  for (const auto &i : pending_map.standbys) {
    auto last_beacon_time = last_beacon.at(i.first);
    if (last_beacon_time < cutoff) {
      dead_standbys.push_back(i.first);
    }
  }

  bool propose = false;

  for (auto i : dead_standbys) {
    dout(4) << "Dropping laggy standby " << i << dendl;
    drop_standby(i);
    propose = true;
  }

  if (pending_map.active_gid != 0
      && last_beacon.at(pending_map.active_gid) < cutoff) {
    const std::string old_active_name = pending_map.active_name;
    drop_active();
    propose = true;
    dout(4) << "Dropping active" << pending_map.active_gid << dendl;
    if (promote_standby()) {
      dout(4) << "Promoted standby " << pending_map.active_gid << dendl;
      mon->clog->info() << "Manager daemon " << old_active_name
                        << " is unresponsive, replacing it with standby"
                        << " daemon " << pending_map.active_name;
    } else {
      dout(4) << "Active is laggy but have no standbys to replace it" << dendl;
      mon->clog->warn() << "Manager daemon " << old_active_name
                        << " is unresponsive.  No standby daemons available.";
    }
  } else if (pending_map.active_gid == 0) {
    if (promote_standby()) {
      dout(4) << "Promoted standby " << pending_map.active_gid << dendl;
      mon->clog->info() << "Activating manager daemon "
                        << pending_map.active_name;
      propose = true;
    }
  }

  if (!pending_map.available &&
      !ever_had_active_mgr &&
      should_warn_about_mgr_down() != HEALTH_OK) {
    dout(10) << " exceeded mon_mgr_mkfs_grace " << g_conf->mon_mgr_mkfs_grace
	     << " seconds" << dendl;
    propose = true;
  }

  if (propose) {
    propose_pending();
  }
}

void MgrMonitor::on_restart()
{
  // Clear out the leader-specific state.
  last_beacon.clear();
}


bool MgrMonitor::promote_standby()
{
  assert(pending_map.active_gid == 0);
  if (pending_map.standbys.size()) {
    // Promote a replacement (arbitrary choice of standby)
    auto replacement_gid = pending_map.standbys.begin()->first;
    pending_map.active_gid = replacement_gid;
    pending_map.active_name = pending_map.standbys.at(replacement_gid).name;
    pending_map.available = false;
    pending_map.active_addr = entity_addr_t();

    drop_standby(replacement_gid);
    return true;
  } else {
    return false;
  }
}

void MgrMonitor::drop_active()
{
  if (last_beacon.count(pending_map.active_gid) > 0) {
    last_beacon.erase(pending_map.active_gid);
  }

  pending_map.active_name = "";
  pending_map.active_gid = 0;
  pending_map.available = false;
  pending_map.active_addr = entity_addr_t();

  // So that when new active mgr subscribes to mgrdigest, it will
  // get an immediate response instead of waiting for next timer
  cancel_timer();
}

void MgrMonitor::drop_standby(uint64_t gid)
{
  pending_map.standbys.erase(gid);
  if (last_beacon.count(gid) > 0) {
    last_beacon.erase(gid);
  }

}

bool MgrMonitor::preprocess_command(MonOpRequestRef op)
{
  MMonCommand *m = static_cast<MMonCommand*>(op->get_req());
  std::stringstream ss;
  bufferlist rdata;

  std::map<std::string, cmd_vartype> cmdmap;
  if (!cmdmap_from_json(m->cmd, &cmdmap, ss)) {
    string rs = ss.str();
    mon->reply_command(op, -EINVAL, rs, rdata, get_last_committed());
    return true;
  }

  MonSession *session = m->get_session();
  if (!session) {
    mon->reply_command(op, -EACCES, "access denied", rdata,
		       get_last_committed());
    return true;
  }

  string format;
  cmd_getval(g_ceph_context, cmdmap, "format", format, string("json-pretty"));
  boost::scoped_ptr<Formatter> f(Formatter::create(format));

  string prefix;
  cmd_getval(g_ceph_context, cmdmap, "prefix", prefix);
  int r = 0;

  if (prefix == "mgr dump") {
    int64_t epoch = 0;
    cmd_getval(g_ceph_context, cmdmap, "epoch", epoch, (int64_t)map.get_epoch());
    if (epoch == (int64_t)map.get_epoch()) {
      f->dump_object("mgrmap", map);
    } else {
      bufferlist bl;
      int err = get_version(epoch, bl);
      if (err == -ENOENT) {
	r = -ENOENT;
	ss << "there is no map for epoch " << epoch;
	goto reply;
      }
      MgrMap m;
      auto p = bl.begin();
      m.decode(p);
      f->dump_object("mgrmap", m);
    }
    f->flush(rdata);
  } else if (prefix == "mgr module ls") {
    f->open_array_section("modules");
    for (auto& p : map.modules) {
      f->dump_string("module", p);
    }
    f->close_section();
    f->flush(rdata);
  } else {
    return false;
  }

reply:
  string rs;
  getline(ss, rs);
  mon->reply_command(op, r, rs, rdata, get_last_committed());
  return true;
}

bool MgrMonitor::prepare_command(MonOpRequestRef op)
{
  MMonCommand *m = static_cast<MMonCommand*>(op->get_req());

  std::stringstream ss;
  bufferlist rdata;

  std::map<std::string, cmd_vartype> cmdmap;
  if (!cmdmap_from_json(m->cmd, &cmdmap, ss)) {
    string rs = ss.str();
    mon->reply_command(op, -EINVAL, rs, rdata, get_last_committed());
    return true;
  }

  MonSession *session = m->get_session();
  if (!session) {
    mon->reply_command(op, -EACCES, "access denied", rdata, get_last_committed());
    return true;
  }

  string format;
  cmd_getval(g_ceph_context, cmdmap, "format", format, string("plain"));
  boost::scoped_ptr<Formatter> f(Formatter::create(format));

  string prefix;
  cmd_getval(g_ceph_context, cmdmap, "prefix", prefix);

  int r = 0;

  if (prefix == "mgr fail") {
    string who;
    cmd_getval(g_ceph_context, cmdmap, "who", who);

    std::string err;
    uint64_t gid = strict_strtol(who.c_str(), 10, &err);
    bool changed = false;
    if (!err.empty()) {
      // Does not parse as a gid, treat it as a name
      if (pending_map.active_name == who) {
        drop_active();
        changed = true;
      } else {
        gid = 0;
        for (const auto &i : pending_map.standbys) {
          if (i.second.name == who) {
            gid = i.first;
            break;
          }
        }
        if (gid != 0) {
          drop_standby(gid);
          changed = true;
        } else {
          ss << "Daemon not found '" << who << "', already failed?";
        }
      }
    } else {
      if (pending_map.active_gid == gid) {
        drop_active();
        changed = true;
      } else if (pending_map.standbys.count(gid) > 0) {
        drop_standby(gid);
        changed = true;
      } else {
        ss << "Daemon not found '" << gid << "', already failed?";
      }
    }

    if (changed && pending_map.active_gid == 0) {
      promote_standby();
    }
  } else if (prefix == "mgr module enable") {
    string module;
    cmd_getval(g_ceph_context, cmdmap, "module", module);
    if (module.empty()) {
      r = -EINVAL;
      goto out;
    }
    string force;
    cmd_getval(g_ceph_context, cmdmap, "force", force);
    if (!pending_map.all_support_module(module) &&
	force != "--force") {
      ss << "all mgr daemons do not support module '" << module << "', pass "
	 << "--force to force enablement";
      r = -ENOENT;
      goto out;
    }
    pending_map.modules.insert(module);
  } else if (prefix == "mgr module disable") {
    string module;
    cmd_getval(g_ceph_context, cmdmap, "module", module);
    if (module.empty()) {
      r = -EINVAL;
      goto out;
    }
    pending_map.modules.erase(module);
  } else {
    ss << "Command '" << prefix << "' not implemented!";
    r = -ENOSYS;
  }

out:
  dout(4) << __func__ << " done, r=" << r << dendl;
  /* Compose response */
  string rs;
  getline(ss, rs);

  if (r >= 0) {
    // success.. delay reply
    wait_for_finished_proposal(op, new Monitor::C_Command(mon, op, r, rs,
					      get_last_committed() + 1));
    return true;
  } else {
    // reply immediately
    mon->reply_command(op, r, rs, rdata, get_last_committed());
    return false;
  }
}

void MgrMonitor::init()
{
  if (digest_event == nullptr) {
    send_digests();  // To get it to schedule its own event
  }
}

void MgrMonitor::on_shutdown()
{
  cancel_timer();
}


