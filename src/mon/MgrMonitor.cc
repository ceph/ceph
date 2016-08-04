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

#include "messages/MMgrBeacon.h"
#include "messages/MMgrMap.h"
#include "messages/MMgrDigest.h"

#include "PGMap.h"
#include "PGMonitor.h"
#include "include/stringify.h"
#include "mgr/MgrContext.h"

#include "MgrMonitor.h"

#define dout_subsys ceph_subsys_mon
#undef dout_prefix
#define dout_prefix *_dout << "MgrMonitor " << __func__ << " "

void MgrMonitor::create_initial()
{
}

void MgrMonitor::update_from_paxos(bool *need_bootstrap)
{
  version_t version = get_last_committed();
  if (version == map.epoch) {
    return;
  }

  dout(4) << "loading version " << version << dendl;

  bufferlist bl;
  int err = get_version(version, bl);
  assert(err == 0);

  bufferlist::iterator p = bl.begin();
  map.decode(p);

  dout(4) << "active server: " << map.active_addr
          << "(" << map.active_gid << ")" << dendl;

  check_subs();
}

void MgrMonitor::create_pending()
{
  pending_map = map;
  pending_map.epoch++;
}

void MgrMonitor::encode_pending(MonitorDBStore::TransactionRef t)
{
  bufferlist bl;
  pending_map.encode(bl, 0);
  put_version(t, pending_map.epoch, bl);
  put_last_committed(t, pending_map.epoch);
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
  void finish(int r) {
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

  last_beacon[m->get_gid()] = ceph_clock_now(g_ceph_context);

  if (pending_map.active_gid == m->get_gid()
      && pending_map.active_addr == m->get_server_addr()
      && pending_map.get_available() == m->get_available()) {
    dout(4) << "Daemon already active in map" << dendl;
    return true;
  }

  if (pending_map.standbys.count(m->get_gid()) > 0
      && pending_map.active_gid != 0) {
    dout(4) << "Daemon already standby in map" << dendl;
    return true;
  }

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
    drop_active();
  }

  // See if we are seeing same name, new GID for any standbys
  for (const auto &i : pending_map.standbys) {
    const StandbyInfo &s = i.second;
    if (s.name == m->get_name() && s.gid != m->get_gid()) {
      dout(4) << "Standby daemon restart (mgr." << m->get_name() << ")" << dendl;
      drop_standby(i.first);
      break;
    }
  }

  last_beacon[m->get_gid()] = ceph_clock_now(g_ceph_context);

  // Track whether we modified pending_map
  bool updated = false;

  if (pending_map.active_gid == m->get_gid()) {
    // A beacon from the currently active daemon
    if (pending_map.active_addr != m->get_server_addr()) {
      dout(4) << "learned address " << m->get_server_addr() << dendl;
      pending_map.active_addr = m->get_server_addr();
      updated = true;
    }

    if (pending_map.get_available() != m->get_available()) {
      dout(4) << "available " << m->get_gid() << dendl;
      pending_map.available = m->get_available();
      updated = true;
    }
  } else if (pending_map.active_gid == 0) {
    // There is no currently active daemon, select this one.
    if (pending_map.standbys.count(m->get_gid())) {
      drop_standby(m->get_gid());
    }
    pending_map.active_gid = m->get_gid();
    pending_map.active_name = m->get_name();

    dout(4) << "selecting new active in epoch " << pending_map.epoch << dendl;
    wait_for_finished_proposal(op, new C_Updated(this, op));
  } else {
    if (pending_map.standbys.count(m->get_gid()) > 0) {
      dout(10) << "from existing standby " << m->get_gid() << dendl;
    } else {
      dout(10) << "new standby " << m->get_gid() << dendl;
      pending_map.standbys[m->get_gid()] = {m->get_gid(), m->get_name()};
      updated = true;
    }
  }

  if (updated) {
    dout(4) << "updating map" << dendl;
    wait_for_finished_proposal(op, new C_Updated(this, op));
  } else {
    dout(10) << "no change" << dendl;
  }

  return true;
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
      dout(20) << "Sending map to subscriber " << sub->session->con << dendl;
      sub->session->con->send_message(new MMgrMap(map));
      if (sub->onetime) {
        mon->session_map.remove_sub(sub);
      } else {
        sub->next = map.get_epoch() + 1;
      }
    }
  } else {
    assert(sub->type == "mgrdigest");
    send_digests();
  }
}

/**
 * Handle digest subscriptions separately (outside of check_sub) because
 * they are going to be periodic rather than version-driven.
 */
void MgrMonitor::send_digests()
{
  digest_callback = nullptr;

  const std::string type = "mgrdigest";
  if (mon->session_map.subs.count(type) == 0)
    return;

  for (auto sub : *(mon->session_map.subs[type])) {
    MMgrDigest *mdigest = new MMgrDigest;

    JSONFormatter f;
    std::list<std::string> health_strs;
    mon->get_health(health_strs, nullptr, &f);
    f.flush(mdigest->health_json);
    f.reset();

    std::ostringstream ss;
    mon->get_mon_status(&f, ss);
    f.flush(mdigest->mon_status_json);
    f.reset();

    sub->session->con->send_message(mdigest);
  }

  digest_callback = new C_StdFunction([this](){
      send_digests();
  });
  mon->timer.add_event_after(5, digest_callback);
}

void MgrMonitor::tick()
{
  const utime_t now = ceph_clock_now(g_ceph_context);
  utime_t cutoff = now;
  cutoff -= g_conf->mon_mgr_beacon_grace;

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

  for (auto i : dead_standbys) {
    dout(4) << "Dropping laggy standby " << i << dendl;
    drop_standby(i);
  }

  bool propose = false;

  if (pending_map.active_gid != 0
      && last_beacon.at(pending_map.active_gid) < cutoff) {

    drop_active();
    dout(4) << "Dropping active" << pending_map.active_gid << dendl;
    if (promote_standby()) {
      dout(4) << "Promoted standby " << pending_map.active_gid << dendl;
      propose = true;
    } else {
      dout(4) << "Active is laggy but have no standbys to replace it" << dendl;
    }
  } else if (pending_map.active_gid == 0) {
    if (promote_standby()) {
      dout(4) << "Promoted standby " << pending_map.active_gid << dendl;
      propose = true;
    }
  }

  if (propose) {
    propose_pending();
  }
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
  return false;

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

    if (changed) {
      tick();
    }
  } else {
    r = -ENOSYS;
  }

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
  send_digests();  // To get it to schedule its own event
}

void MgrMonitor::on_shutdown()
{
  if (digest_callback) {
    mon->timer.cancel_event(digest_callback);
  }
}

