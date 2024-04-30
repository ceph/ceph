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

#include "include/stringify.h"
#include "mgr/MgrContext.h"
#include "mgr/mgr_commands.h"
#include "OSDMonitor.h"
#include "ConfigMonitor.h"
#include "HealthMonitor.h"

#include "common/TextTable.h"
#include "include/stringify.h"

#include "MgrMonitor.h"

#define MGR_METADATA_PREFIX "mgr_metadata"

#define dout_subsys ceph_subsys_mon
#undef dout_prefix
#define dout_prefix _prefix(_dout, mon, map)
using namespace TOPNSPC::common;

using std::dec;
using std::hex;
using std::list;
using std::map;
using std::make_pair;
using std::ostream;
using std::ostringstream;
using std::pair;
using std::set;
using std::string;
using std::stringstream;
using std::to_string;
using std::vector;

using ceph::bufferlist;
using ceph::decode;
using ceph::encode;
using ceph::ErasureCodeInterfaceRef;
using ceph::ErasureCodeProfile;
using ceph::Formatter;
using ceph::JSONFormatter;
using ceph::make_message;
using ceph::mono_clock;
using ceph::mono_time;

static ostream& _prefix(std::ostream *_dout, Monitor &mon,
			const MgrMap& mgrmap) {
  return *_dout << "mon." << mon.name << "@" << mon.rank
		<< "(" << mon.get_state_name()
		<< ").mgr e" << mgrmap.get_epoch() << " ";
}

// the system treats always_on_modules as if they provide built-in functionality
// by ensuring that they are always enabled.
static const std::map<uint32_t, std::set<std::string>>& always_on_modules() {
  static const std::set<std::string> octopus_modules = {
    "crash",
    "status",
    "progress",
    "balancer",
    "devicehealth",
    "orchestrator",
#ifdef WITH_RBD
    "rbd_support",
#endif
#ifdef WITH_CEPHFS
    "volumes",
#endif
    "pg_autoscaler",
    "telemetry",
  };
  static const std::map<uint32_t, std::set<std::string>> always_on_modules_map = {
    { CEPH_RELEASE_OCTOPUS, octopus_modules },
    { CEPH_RELEASE_PACIFIC, octopus_modules },
    { CEPH_RELEASE_QUINCY, octopus_modules },
    { CEPH_RELEASE_REEF, octopus_modules },
    { CEPH_RELEASE_SQUID, octopus_modules },
  };
  return always_on_modules_map;
};

// Prefix for mon store of active mgr's command descriptions
const static std::string command_descs_prefix = "mgr_command_descs";

const Option *MgrMonitor::find_module_option(const string& name)
{
  // we have two forms of names: "mgr/$module/$option" and
  // localized "mgr/$module/$instance/$option".  normalize to the
  // former by stripping out $instance.
  string real_name;
  if (name.substr(0, 4) != "mgr/") {
    return nullptr;
  }
  auto second_slash = name.find('/', 5);
  if (second_slash == std::string::npos) {
    return nullptr;
  }
  auto third_slash = name.find('/', second_slash + 1);
  if (third_slash != std::string::npos) {
    // drop the $instance part between the second and third slash
    real_name = name.substr(0, second_slash) + name.substr(third_slash);
  } else {
    real_name = name;
  }
  auto p = mgr_module_options.find(real_name);
  if (p != mgr_module_options.end()) {
    return &p->second;
  }
  return nullptr;
}

version_t MgrMonitor::get_trim_to() const
{
  int64_t max = g_conf().get_val<int64_t>("mon_max_mgrmap_epochs");
  if (map.epoch > max) {
    return map.epoch - max;
  }
  return 0;
}

void MgrMonitor::create_initial()
{
  // Take a local copy of initial_modules for tokenizer to iterate over.
  auto initial_modules = g_conf().get_val<std::string>("mgr_initial_modules");
  boost::tokenizer<> tok(initial_modules);
  for (auto& m : tok) {
    pending_map.modules.insert(m);
  }
  pending_map.always_on_modules = always_on_modules();
  pending_command_descs = mgr_commands;
  dout(10) << __func__ << " initial modules " << pending_map.modules
	   << ", always on modules " << pending_map.get_always_on_modules()
           << ", " << pending_command_descs.size() << " commands"
	   << dendl;
}

void MgrMonitor::get_store_prefixes(std::set<string>& s) const
{
  s.insert(service_name);
  s.insert(command_descs_prefix);
  s.insert(MGR_METADATA_PREFIX);
}

void MgrMonitor::update_from_paxos(bool *need_bootstrap)
{
  version_t version = get_last_committed();
  if (version != map.epoch) {
    dout(4) << "loading version " << version << dendl;

    bufferlist bl;
    int err = get_version(version, bl);
    ceph_assert(err == 0);

    bool old_available = map.get_available();
    uint64_t old_gid = map.get_active_gid();

    auto p = bl.cbegin();
    map.decode(p);

    dout(4) << "active server: " << map.active_addrs
	    << "(" << map.active_gid << ")" << dendl;

    ever_had_active_mgr = get_value("ever_had_active_mgr");

    load_health();

    if (map.available) {
      first_seen_inactive = utime_t();
    } else {
      first_seen_inactive = ceph_clock_now();
    }

    check_subs();

    if (version == 1
        || command_descs.empty()
        || (map.get_available()
            && (!old_available || old_gid != map.get_active_gid()))) {
      dout(4) << "mkfs or daemon transitioned to available, loading commands"
	      << dendl;
      bufferlist loaded_commands;
      int r = mon.store->get(command_descs_prefix, "", loaded_commands);
      if (r < 0) {
        derr << "Failed to load mgr commands: " << cpp_strerror(r) << dendl;
      } else {
        auto p = loaded_commands.cbegin();
        decode(command_descs, p);
      }
    }
  }

  // populate module options
  mgr_module_options.clear();
  misc_option_strings.clear();
  for (auto& i : map.available_modules) {
    for (auto& j : i.module_options) {
      string name = string("mgr/") + i.name + "/" + j.second.name;
      auto p = mgr_module_options.emplace(
	name,
	Option(name, static_cast<Option::type_t>(j.second.type),
	       static_cast<Option::level_t>(j.second.level)));
      Option& opt = p.first->second;
      opt.set_flags(static_cast<Option::flag_t>(j.second.flags));
      opt.set_flag(Option::FLAG_MGR);
      opt.set_description(j.second.desc.c_str());
      opt.set_long_description(j.second.long_desc.c_str());
      for (auto& k : j.second.tags) {
	opt.add_tag(k.c_str());
      }
      for (auto& k : j.second.see_also) {
	if (i.module_options.count(k)) {
	  // it's another module option
	  misc_option_strings.push_back(string("mgr/") + i.name + "/" + k);
	  opt.add_see_also(misc_option_strings.back().c_str());
	} else {
	  // it's a native option
	  opt.add_see_also(k.c_str());
	}
      }
      Option::value_t v, v2;
      std::string err;
      if (j.second.default_value.size() &&
	  !opt.parse_value(j.second.default_value, &v, &err)) {
	opt.set_default(v);
      }
      if (j.second.min.size() &&
	  j.second.max.size() &&
	  !opt.parse_value(j.second.min, &v, &err) &&
	  !opt.parse_value(j.second.max, &v2, &err)) {
	opt.set_min_max(v, v2);
      }
      std::vector<const char *> enum_allowed;
      for (auto& k : j.second.enum_allowed) {
	enum_allowed.push_back(k.c_str());
      }
      opt.set_enum_allowed(enum_allowed);
    }
  }
  // force ConfigMonitor to refresh, since it uses const Option *
  // pointers into our mgr_module_options (which we just rebuilt).
  mon.configmon()->load_config();

  if (!mon.is_init()) {
    // feed our pet MgrClient, unless we are in Monitor::[pre]init()
    prime_mgr_client();
  }
}

void MgrMonitor::prime_mgr_client()
{
  dout(10) << __func__ << dendl;
  mon.mgr_client.ms_dispatch2(make_message<MMgrMap>(map));
}

void MgrMonitor::create_pending()
{
  pending_map = map;
  pending_map.epoch++;
}

health_status_t MgrMonitor::should_warn_about_mgr_down()
{
  utime_t now = ceph_clock_now();
  // we warn if we have osds AND we've exceeded the grace period
  // which means a new mon cluster and be HEALTH_OK indefinitely as long as
  // no OSDs are ever created.
  if (mon.osdmon()->osdmap.get_num_osds() > 0 &&
       now > mon.monmap->created + g_conf().get_val<int64_t>("mon_mgr_mkfs_grace")) {
    health_status_t level = HEALTH_WARN;
    if (first_seen_inactive != utime_t() &&
	now - first_seen_inactive > g_conf().get_val<int64_t>("mon_mgr_inactive_grace")) {
      level = HEALTH_ERR;
    }
    return level;
  }
  return HEALTH_OK;
}

void MgrMonitor::post_paxos_update()
{
  // are we handling digest subscribers?
  if (digest_event) {
    bool send = false;
    if (prev_health_checks.empty()) {
      prev_health_checks.resize(mon.paxos_service.size());
      send = true;
    }
    ceph_assert(prev_health_checks.size() == mon.paxos_service.size());
    for (auto i = 0u; i < prev_health_checks.size(); i++) {
      const auto& curr = mon.paxos_service[i]->get_health_checks();
      if (!send && curr != prev_health_checks[i]) {
        send = true;
      }
      prev_health_checks[i] = curr;
    }
    if (send) {
      if (is_active()) {
        send_digests();
      } else {
        cancel_timer();
        wait_for_active_ctx(new C_MonContext{&mon, [this](int) {
          send_digests();
        }});
      }
    }
  }
}

void MgrMonitor::encode_pending(MonitorDBStore::TransactionRef t)
{
  dout(10) << __func__ << " " << pending_map << dendl;
  bufferlist bl;
  pending_map.encode(bl, mon.get_quorum_con_features());
  put_version(t, pending_map.epoch, bl);
  put_last_committed(t, pending_map.epoch);

  for (auto& p : pending_metadata) {
    dout(10) << __func__ << " set metadata for " << p.first << dendl;
    t->put(MGR_METADATA_PREFIX, p.first, p.second);
  }
  for (auto& name : pending_metadata_rm) {
    dout(10) << __func__ << " rm metadata for " << name << dendl;
    t->erase(MGR_METADATA_PREFIX, name);
  }
  pending_metadata.clear();
  pending_metadata_rm.clear();

  health_check_map_t next;
  if (pending_map.active_gid == 0) {
    auto level = should_warn_about_mgr_down();
    if (level != HEALTH_OK) {
      next.add("MGR_DOWN", level, "no active mgr", 0);
    } else {
      dout(10) << __func__ << " no health warning (never active and new cluster)"
	       << dendl;
    }
  } else {
    put_value(t, "ever_had_active_mgr", 1);
  }
  encode_health(next, t);

  if (pending_command_descs.size()) {
    dout(4) << __func__ << " encoding " << pending_command_descs.size()
            << " command_descs" << dendl;
    for (auto& p : pending_command_descs) {
      p.set_flag(MonCommand::FLAG_MGR);
    }
    bufferlist bl;
    encode(pending_command_descs, bl);
    t->put(command_descs_prefix, "", bl);
    pending_command_descs.clear();
  }
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
  if (fsid != mon.monmap->fsid) {
    dout(1) << __func__ << " op fsid " << fsid
	    << " != " << mon.monmap->fsid << dendl;
    return false;
  }
  return true;
}

bool MgrMonitor::preprocess_query(MonOpRequestRef op)
{
  auto m = op->get_req<PaxosServiceMessage>();
  switch (m->get_type()) {
    case MSG_MGR_BEACON:
      return preprocess_beacon(op);
    case MSG_MON_COMMAND:
      try {
	return preprocess_command(op);
      } catch (const bad_cmd_get& e) {
      bufferlist bl;
      mon.reply_command(op, -EINVAL, e.what(), bl, get_last_committed());
      return true;
    }

    default:
      mon.no_reply(op);
      derr << "Unhandled message type " << m->get_type() << dendl;
      return true;
  }
}

bool MgrMonitor::prepare_update(MonOpRequestRef op)
{
  auto m = op->get_req<PaxosServiceMessage>();
  switch (m->get_type()) {
    case MSG_MGR_BEACON:
      return prepare_beacon(op);

    case MSG_MON_COMMAND:
      try {
	return prepare_command(op);
      } catch (const bad_cmd_get& e) {
	bufferlist bl;
	mon.reply_command(op, -EINVAL, e.what(), bl, get_last_committed());
	return false; /* nothing to propose! */
      }

    default:
      mon.no_reply(op);
      derr << "Unhandled message type " << m->get_type() << dendl;
      return false; /* nothing to propose! */
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
      mm->mon.no_reply(op);
    } else {
      mm->dispatch(op);        // try again
    }
  }
};

bool MgrMonitor::preprocess_beacon(MonOpRequestRef op)
{
  auto m = op->get_req<MMgrBeacon>();
  mon.no_reply(op); // we never reply to beacons
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
  auto m = op->get_req<MMgrBeacon>();
  dout(4) << "beacon from " << m->get_gid() << dendl;

  // Track whether we modified pending_map
  bool updated = false;
  bool plugged = false;

  // See if we are seeing same name, new GID for the active daemon
  if (m->get_name() == pending_map.active_name
      && m->get_gid() != pending_map.active_gid)
  {
    dout(4) << "Active daemon restart (mgr." << m->get_name() << ")" << dendl;
    mon.clog->info() << "Active manager daemon " << m->get_name()
                      << " restarted";
    if (!mon.osdmon()->is_writeable()) {
      dout(1) << __func__ << ":  waiting for osdmon writeable to"
                 " blocklist old instance." << dendl;
      mon.osdmon()->wait_for_writeable(op, new C_RetryMessage(this, op));
      return false;
    }
    plugged |= drop_active();
    updated = true;
  }

  // See if we are seeing same name, new GID for any standbys
  for (const auto &i : pending_map.standbys) {
    const MgrMap::StandbyInfo &s = i.second;
    if (s.name == m->get_name() && s.gid != m->get_gid()) {
      dout(4) << "Standby daemon restart (mgr." << m->get_name() << ")" << dendl;
      mon.clog->debug() << "Standby manager daemon " << m->get_name()
                         << " restarted";
      drop_standby(i.first);
      updated = true;
      break;
    }
  }

  last_beacon[m->get_gid()] = ceph::coarse_mono_clock::now();

  if (pending_map.active_gid == m->get_gid()) {
    if (pending_map.services != m->get_services()) {
      dout(4) << "updated services from mgr." << m->get_name()
              << ": " << m->get_services() << dendl;
      pending_map.services = m->get_services();
      updated = true;
    }

    // A beacon from the currently active daemon
    if (pending_map.active_addrs != m->get_server_addrs()) {
      dout(4) << "learned address " << m->get_server_addrs()
	      << " (was " << pending_map.active_addrs << ")" << dendl;
      pending_map.active_addrs = m->get_server_addrs();
      updated = true;
    }

    if (pending_map.get_available() != m->get_available()) {
      dout(4) << "available " << m->get_gid() << dendl;
      mon.clog->info() << "Manager daemon " << pending_map.active_name
                        << " is now available";

      // This beacon should include command descriptions
      pending_command_descs = m->get_command_descs();
      if (pending_command_descs.empty()) {
        // This should not happen, but it also isn't fatal: we just
        // won't successfully update our list of commands.
        dout(4) << "First available beacon from " << pending_map.active_name
                << "(" << m->get_gid() << ") does not include command descs"
                << dendl;
      } else {
        dout(4) << "First available beacon from " << pending_map.active_name
                << "(" << m->get_gid() << ") includes "
                << pending_command_descs.size() << " command descs" << dendl;
      }

      pending_map.available = m->get_available();
      updated = true;
    }
    if (pending_map.available_modules != m->get_available_modules()) {
      dout(4) << "available_modules " << m->get_available_modules()
	      << " (was " << pending_map.available_modules << ")" << dendl;
      pending_map.available_modules = m->get_available_modules();
      updated = true;
    }
    const auto& clients = m->get_clients();
    if (pending_map.clients != clients) {
      dout(4) << "active's RADOS clients " << clients
	      << " (was " << pending_map.clients << ")" << dendl;
      pending_map.clients = clients;
      updated = true;
    }
  } else if (m->get_available()) {
    dout(4) << "mgr thinks it is active but it is not, dropping!" << dendl;
    auto m = make_message<MMgrMap>(MgrMap::create_null_mgrmap());
    mon.send_reply(op, m.detach());
    goto out;
  } else if (pending_map.active_gid == 0) {
    // There is no currently active daemon, select this one.
    if (pending_map.standbys.count(m->get_gid())) {
      drop_standby(m->get_gid(), false);
    }
    if (!(pending_map.flags & MgrMap::FLAG_DOWN)) {
      dout(4) << "selecting new active " << m->get_gid()
	      << " " << m->get_name()
	      << " (was " << pending_map.active_gid << " "
	      << pending_map.active_name << ")" << dendl;
      pending_map.active_gid = m->get_gid();
      pending_map.active_name = m->get_name();
      pending_map.active_change = ceph_clock_now();
      pending_map.active_mgr_features = m->get_mgr_features();
      pending_map.available_modules = m->get_available_modules();
      encode(m->get_metadata(), pending_metadata[m->get_name()]);
      pending_metadata_rm.erase(m->get_name());

      mon.clog->info() << "Activating manager daemon "
                       << pending_map.active_name;
      updated = true;
    }
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
      mon.clog->debug() << "Standby manager daemon " << m->get_name()
                         << " started";
      pending_map.standbys[m->get_gid()] = {m->get_gid(), m->get_name(),
					    m->get_available_modules(),
					    m->get_mgr_features()};
      encode(m->get_metadata(), pending_metadata[m->get_name()]);
      pending_metadata_rm.erase(m->get_name());
      updated = true;
    }
  }

  if (updated) {
    dout(4) << "updating map" << dendl;
    wait_for_finished_proposal(op, new C_Updated(this, op));
  } else {
    dout(10) << "no change" << dendl;
  }

out:

  if (plugged) {
    paxos.unplug();
  }

  return updated;
}

void MgrMonitor::check_subs()
{
  const std::string type = "mgrmap";
  if (mon.session_map.subs.count(type) == 0)
    return;
  for (auto sub : *(mon.session_map.subs[type])) {
    check_sub(sub);
  }
}

void MgrMonitor::check_sub(Subscription *sub)
{
  if (sub->type == "mgrmap") {
    if (sub->next <= map.get_epoch()) {
      dout(20) << "Sending map to subscriber " << sub->session->con
	       << " " << sub->session->con->get_peer_addr() << dendl;
      sub->session->con->send_message2(make_message<MMgrMap>(map));
      if (sub->onetime) {
        mon.session_map.remove_sub(sub);
      } else {
        sub->next = map.get_epoch() + 1;
      }
    }
  } else {
    ceph_assert(sub->type == "mgrdigest");
    if (sub->next == 0) {
      // new registration; cancel previous timer
      cancel_timer();
    }
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

  const std::string type = "mgrdigest";
  if (mon.session_map.subs.count(type) == 0) {
    prev_health_checks.clear();
    return;
  }

  if (!is_active()) {
    // if paxos is currently not active, don't send a digest but reenable timer
    goto timer;
  }
  dout(10) << __func__ << dendl;

  for (auto sub : *(mon.session_map.subs[type])) {
    dout(10) << __func__ << " sending digest to subscriber " << sub->session->con
	     << " " << sub->session->con->get_peer_addr() << dendl;
    auto mdigest = make_message<MMgrDigest>();

    JSONFormatter f;
    mon.healthmon()->get_health_status(true, &f, nullptr, nullptr, nullptr);
    f.flush(mdigest->health_json);
    f.reset();

    mon.get_mon_status(&f);
    f.flush(mdigest->mon_status_json);
    f.reset();

    sub->session->con->send_message2(mdigest);
  }

timer:
  digest_event = mon.timer.add_event_after(
    g_conf().get_val<int64_t>("mon_mgr_digest_period"),
    new C_MonContext{&mon, [this](int) {
      send_digests();
  }});
}

void MgrMonitor::cancel_timer()
{
  if (digest_event) {
    mon.timer.cancel_event(digest_event);
    digest_event = nullptr;
  }
}

void MgrMonitor::on_active()
{
  if (!mon.is_leader()) {
    return;
  }
  mon.clog->debug() << "mgrmap e" << map.epoch << ": " << map;
  assert(HAVE_FEATURE(mon.get_quorum_con_features(), SERVER_NAUTILUS));
  if (pending_map.always_on_modules == always_on_modules()) {
    return;
  }
  dout(4) << "always on modules changed, pending "
          << pending_map.always_on_modules << " != wanted "
          << always_on_modules() << dendl;
  pending_map.always_on_modules = always_on_modules();
  propose_pending();
}

void MgrMonitor::tick()
{
  if (!is_active() || !mon.is_leader())
    return;

  const auto now = ceph::coarse_mono_clock::now();

  const auto mgr_beacon_grace =
      g_conf().get_val<std::chrono::seconds>("mon_mgr_beacon_grace");

  // Note that this is the mgr daemon's tick period, not ours (the
  // beacon is sent with this period).
  const auto mgr_tick_period =
      g_conf().get_val<std::chrono::seconds>("mgr_tick_period");

  if (last_tick != ceph::coarse_mono_clock::zero()
      && (now - last_tick > (mgr_beacon_grace - mgr_tick_period))) {
    // This case handles either local slowness (calls being delayed
    // for whatever reason) or cluster election slowness (a long gap
    // between calls while an election happened)
    dout(4) << __func__ << ": resetting beacon timeouts due to mon delay "
            "(slow election?) of " << now - last_tick << " seconds" << dendl;
    for (auto &i : last_beacon) {
      i.second = now;
    }
  }

  last_tick = now;

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
  const auto cutoff = now - mgr_beacon_grace;
  for (const auto &i : pending_map.standbys) {
    auto last_beacon_time = last_beacon.at(i.first);
    if (last_beacon_time < cutoff) {
      dead_standbys.push_back(i.first);
    }
  }

  bool propose = false;
  bool plugged = false;

  for (auto i : dead_standbys) {
    dout(4) << "Dropping laggy standby " << i << dendl;
    drop_standby(i);
    propose = true;
  }

  if (pending_map.active_gid != 0
      && last_beacon.at(pending_map.active_gid) < cutoff
      && mon.osdmon()->is_writeable()) {
    const std::string old_active_name = pending_map.active_name;
    plugged |= drop_active();
    propose = true;
    dout(4) << "Dropping active" << pending_map.active_gid << dendl;
    if (promote_standby()) {
      dout(4) << "Promoted standby " << pending_map.active_gid << dendl;
      mon.clog->info() << "Manager daemon " << old_active_name
                        << " is unresponsive, replacing it with standby"
                        << " daemon " << pending_map.active_name;
    } else {
      dout(4) << "Active is laggy but have no standbys to replace it" << dendl;
      mon.clog->info() << "Manager daemon " << old_active_name
                        << " is unresponsive.  No standby daemons available.";
    }
  } else if (pending_map.active_gid == 0) {
    if (promote_standby()) {
      dout(4) << "Promoted standby " << pending_map.active_gid << dendl;
      mon.clog->info() << "Activating manager daemon "
                      << pending_map.active_name;
      propose = true;
    }
  }

  if (!pending_map.available &&
      !ever_had_active_mgr &&
      should_warn_about_mgr_down() != HEALTH_OK) {
    dout(10) << " exceeded mon_mgr_mkfs_grace "
             << g_conf().get_val<int64_t>("mon_mgr_mkfs_grace")
             << " seconds" << dendl;
    propose = true;
  }

  // obsolete modules?
  if (mon.monmap->min_mon_release >= ceph_release_t::octopus &&
      pending_map.module_enabled("orchestrator_cli")) {
    dout(10) << " disabling obsolete/renamed 'orchestrator_cli'" << dendl;
    // we don't need to enable 'orchestrator' because it's now always-on
    pending_map.modules.erase("orchestrator_cli");
    propose = true;
  }

  if (propose) {
    propose_pending();
  }
  if (plugged) {
    paxos.unplug();
    ceph_assert(propose);
    paxos.trigger_propose();
  }
}

void MgrMonitor::on_restart()
{
  // Clear out the leader-specific state.
  last_beacon.clear();
  last_tick = ceph::coarse_mono_clock::now();
}


bool MgrMonitor::promote_standby()
{
  ceph_assert(pending_map.active_gid == 0);
  if (pending_map.flags & MgrMap::FLAG_DOWN) {
    return false;
  }
  if (pending_map.standbys.size()) {
    // Promote a replacement (arbitrary choice of standby)
    auto replacement_gid = pending_map.standbys.begin()->first;
    pending_map.active_gid = replacement_gid;
    pending_map.active_name = pending_map.standbys.at(replacement_gid).name;
    pending_map.available_modules =
      pending_map.standbys.at(replacement_gid).available_modules;
    pending_map.active_mgr_features =
      pending_map.standbys.at(replacement_gid).mgr_features;
    pending_map.available = false;
    pending_map.active_addrs = entity_addrvec_t();
    pending_map.active_change = ceph_clock_now();

    mon.clog->info() << "Activating manager daemon "
                     << pending_map.active_name;

    drop_standby(replacement_gid, false);

    return true;
  } else {
    return false;
  }
}

bool MgrMonitor::drop_active()
{
  ceph_assert(mon.osdmon()->is_writeable());

  bool plugged = false;
  if (!paxos.is_plugged()) {
    paxos.plug();
    plugged = true;
  }

  if (last_beacon.count(pending_map.active_gid) > 0) {
    last_beacon.erase(pending_map.active_gid);
  }

  ceph_assert(pending_map.active_gid > 0);
  auto until = ceph_clock_now();
  until += g_conf().get_val<double>("mon_mgr_blocklist_interval");
  dout(5) << "blocklisting previous mgr." << pending_map.active_name << "."
          << pending_map.active_gid << " ("
          << pending_map.active_addrs << ")" << dendl;
  auto blocklist_epoch = mon.osdmon()->blocklist(pending_map.active_addrs, until);

  /* blocklist RADOS clients in use by the mgr */
  for (const auto& a : pending_map.clients) {
    mon.osdmon()->blocklist(a.second, until);
  }
  request_proposal(mon.osdmon());

  pending_metadata_rm.insert(pending_map.active_name);
  pending_metadata.erase(pending_map.active_name);
  pending_map.active_name = "";
  pending_map.active_gid = 0;
  pending_map.active_change = ceph_clock_now();
  pending_map.active_mgr_features = 0;
  pending_map.available = false;
  pending_map.active_addrs = entity_addrvec_t();
  pending_map.services.clear();
  pending_map.clients.clear();
  pending_map.last_failure_osd_epoch = blocklist_epoch;

  /* If we are dropping the active, we need to notify clients immediately.
   * Additionally, avoid logical races with ::prepare_beacon which cannot
   * accurately determine if a mgr is a standby or an old active.
   */
  force_immediate_propose();

  // So that when new active mgr subscribes to mgrdigest, it will
  // get an immediate response instead of waiting for next timer
  cancel_timer();
  return plugged;
}

void MgrMonitor::drop_standby(uint64_t gid, bool drop_meta)
{
  if (drop_meta) {
    pending_metadata_rm.insert(pending_map.standbys[gid].name);
    pending_metadata.erase(pending_map.standbys[gid].name);
  }
  pending_map.standbys.erase(gid);
  if (last_beacon.count(gid) > 0) {
    last_beacon.erase(gid);
  }
}

bool MgrMonitor::preprocess_command(MonOpRequestRef op)
{
  auto m = op->get_req<MMonCommand>();
  std::stringstream ss;
  bufferlist rdata;

  cmdmap_t cmdmap;
  if (!cmdmap_from_json(m->cmd, &cmdmap, ss)) {
    string rs = ss.str();
    mon.reply_command(op, -EINVAL, rs, rdata, get_last_committed());
    return true;
  }

  MonSession *session = op->get_session();
  if (!session) {
    mon.reply_command(op, -EACCES, "access denied", rdata,
		       get_last_committed());
    return true;
  }

  string format = cmd_getval_or<string>(cmdmap, "format", "plain");
  boost::scoped_ptr<Formatter> f(Formatter::create(format));

  string prefix;
  cmd_getval(cmdmap, "prefix", prefix);
  int r = 0;

  if (prefix == "mgr stat") {
    if (!f) {
      f.reset(Formatter::create(format, "json-pretty", "json-pretty"));
    }
    f->open_object_section("stat");
    f->dump_unsigned("epoch", map.get_epoch());
    f->dump_bool("available", map.get_available());
    f->dump_string("active_name", map.get_active_name());
    f->dump_unsigned("num_standby", map.get_num_standby());
    f->close_section();
    f->flush(rdata);
  } else if (prefix == "mgr dump") {
    if (!f) {
      f.reset(Formatter::create(format, "json-pretty", "json-pretty"));
    }
    int64_t epoch = cmd_getval_or<int64_t>(cmdmap, "epoch", map.get_epoch());
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
      auto p = bl.cbegin();
      m.decode(p);
      f->dump_object("mgrmap", m);
    }
    f->flush(rdata);
  } else if (prefix == "mgr module ls") {
    if (f) {
      f->open_object_section("modules");
      {
        f->open_array_section("always_on_modules");
        for (auto& p : map.get_always_on_modules()) {
          f->dump_string("module", p);
        }
        f->close_section();
        f->open_array_section("enabled_modules");
        for (auto& p : map.modules) {
          if (map.get_always_on_modules().count(p) > 0)
            continue;
          // We only show the name for enabled modules.  The any errors
          // etc will show up as a health checks.
          f->dump_string("module", p);
        }
        f->close_section();
        f->open_array_section("disabled_modules");
        for (auto& p : map.available_modules) {
          if (map.modules.count(p.name) == 0 &&
            map.get_always_on_modules().count(p.name) == 0) {
            // For disabled modules, we show the full info if the detail
            // parameter is enabled, to give a hint about whether enabling it will work
            p.dump(f.get());
          }
        }
        f->close_section();
      }
      f->close_section();
      f->flush(rdata);
    } else {
      TextTable tbl;
      tbl.define_column("MODULE", TextTable::LEFT, TextTable::LEFT);
      tbl.define_column("      ", TextTable::LEFT, TextTable::LEFT);

      for (auto& p : map.get_always_on_modules()) {
        tbl << p;
        tbl << "on (always on)";
        tbl << TextTable::endrow;
      }
      for (auto& p : map.modules) {
        if (map.get_always_on_modules().count(p) > 0)
          continue;
        tbl << p;
        tbl << "on";
        tbl << TextTable::endrow;
      }
      for (auto& p : map.available_modules) {
        if (map.modules.count(p.name) == 0 &&
            map.get_always_on_modules().count(p.name) == 0) {
          tbl << p.name;
          tbl << "-";
          tbl << TextTable::endrow;
        }
      }
      rdata.append(stringify(tbl));
    }
  } else if (prefix == "mgr services") {
    if (!f) {
      f.reset(Formatter::create(format, "json-pretty", "json-pretty"));
    }
    f->open_object_section("services");
    for (const auto &i : map.services) {
      f->dump_string(i.first.c_str(), i.second);
    }
    f->close_section();
    f->flush(rdata);
  } else if (prefix == "mgr metadata") {
    if (!f) {
      f.reset(Formatter::create(format, "json-pretty", "json-pretty"));
    }
    string name;
    cmd_getval(cmdmap, "who", name);
    if (name.size() > 0 && !map.have_name(name)) {
      ss << "mgr." << name << " does not exist";
      r = -ENOENT;
      goto reply;
    }
    if (name.size()) {
      f->open_object_section("mgr_metadata");
      f->dump_string("name", name);
      r = dump_metadata(name, f.get(), &ss);
      if (r < 0)
        goto reply;
      f->close_section();
    } else {
      r = 0;
      f->open_array_section("mgr_metadata");
      for (auto& i : map.get_all_names()) {
	f->open_object_section("mgr");
	f->dump_string("name", i);
	r = dump_metadata(i, f.get(), NULL);
	if (r == -EINVAL || r == -ENOENT) {
	  // Drop error, continue to get other daemons' metadata
	  dout(4) << "No metadata for mgr." << i << dendl;
	  r = 0;
	} else if (r < 0) {
	  // Unexpected error
	  goto reply;
	}
	f->close_section();
      }
      f->close_section();
    }
    f->flush(rdata);
  } else if (prefix == "mgr versions") {
    if (!f) {
      f.reset(Formatter::create(format, "json-pretty", "json-pretty"));
    }
    count_metadata("ceph_version", f.get());
    f->flush(rdata);
    r = 0;
  } else if (prefix == "mgr count-metadata") {
    if (!f) {
      f.reset(Formatter::create(format, "json-pretty", "json-pretty"));
    }
    string field;
    cmd_getval(cmdmap, "property", field);
    count_metadata(field, f.get());
    f->flush(rdata);
    r = 0;
  } else {
    return false;
  }

reply:
  string rs;
  getline(ss, rs);
  mon.reply_command(op, r, rs, rdata, get_last_committed());
  return true;
}

bool MgrMonitor::prepare_command(MonOpRequestRef op)
{
  auto m = op->get_req<MMonCommand>();

  std::stringstream ss;
  bufferlist rdata;

  cmdmap_t cmdmap;
  if (!cmdmap_from_json(m->cmd, &cmdmap, ss)) {
    string rs = ss.str();
    mon.reply_command(op, -EINVAL, rs, rdata, get_last_committed());
    return true;
  }

  MonSession *session = op->get_session();
  if (!session) {
    mon.reply_command(op, -EACCES, "access denied", rdata, get_last_committed());
    return true;
  }

  string format = cmd_getval_or<string>(cmdmap, "format", "plain");
  boost::scoped_ptr<Formatter> f(Formatter::create(format));

  const auto prefix = cmd_getval_or<string>(cmdmap, "prefix", string{});
  int r = 0;
  bool plugged = false;

  if (prefix == "mgr set") {
    std::string var;
    if (!cmd_getval(cmdmap, "var", var) || var.empty()) {
      ss << "Invalid variable";
      return -EINVAL;
    }
    string val;
    if (!cmd_getval(cmdmap, "val", val)) {
      return -EINVAL;
    }

    if (var == "down") {
      bool enable_down = false;
      int r = parse_bool(val, &enable_down, ss);
      if (r != 0) {
        return r;
      }
      if (enable_down) {
        bool has_active = !!pending_map.active_gid;
        if (has_active && !mon.osdmon()->is_writeable()) {
          mon.osdmon()->wait_for_writeable(op, new C_RetryMessage(this, op));
          return false;
        }
        pending_map.flags |= MgrMap::FLAG_DOWN;
        if (has_active) {
          plugged |= drop_active();
        }
      } else {
        pending_map.flags &= ~(MgrMap::FLAG_DOWN);
        if (pending_map.active_gid == 0) {
          promote_standby();
        }
      }
    } else {
      return -EINVAL;
    }
  } else if (prefix == "mgr fail") {
    string who;
    if (!cmd_getval(cmdmap, "who", who)) {
      if (!map.active_gid) {
	ss << "Currently no active mgr";
	goto out;
      }
      who = map.active_name;
    }

    std::string err;
    uint64_t gid = strict_strtol(who.c_str(), 10, &err);
    bool changed = false;
    if (!err.empty()) {
      // Does not parse as a gid, treat it as a name
      if (pending_map.active_name == who) {
        if (!mon.osdmon()->is_writeable()) {
          mon.osdmon()->wait_for_writeable(op, new C_RetryMessage(this, op));
          return false;
        }
        plugged |= drop_active();
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
        if (!mon.osdmon()->is_writeable()) {
          mon.osdmon()->wait_for_writeable(op, new C_RetryMessage(this, op));
          return false;
        }
        plugged |= drop_active();
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
    cmd_getval(cmdmap, "module", module);
    if (module.empty()) {
      r = -EINVAL;
      goto out;
    }
    if (pending_map.get_always_on_modules().count(module) > 0) {
      ss << "module '" << module << "' is already enabled (always-on)";
      goto out;
    }
    bool force = false;
    cmd_getval_compat_cephbool(cmdmap, "force", force);
    if (!pending_map.all_support_module(module) &&
	!force) {
      ss << "all mgr daemons do not support module '" << module << "', pass "
	 << "--force to force enablement";
      r = -ENOENT;
      goto out;
    }

    std::string can_run_error;
    if (!force && !pending_map.can_run_module(module, &can_run_error)) {
      ss << "module '" << module << "' reports that it cannot run on the active "
            "manager daemon: " << can_run_error << " (pass --force to force "
            "enablement)";
      r = -ENOENT;
      goto out;
    }

    if (pending_map.module_enabled(module)) {
      ss << "module '" << module << "' is already enabled";
      r = 0;
      goto out;
    }
    pending_map.modules.insert(module);
  } else if (prefix == "mgr module disable") {
    string module;
    cmd_getval(cmdmap, "module", module);
    if (module.empty()) {
      r = -EINVAL;
      goto out;
    }
    if (pending_map.get_always_on_modules().count(module) > 0) {
      ss << "module '" << module << "' cannot be disabled (always-on)";
      r = -EINVAL;
      goto out;
    }
    if (!pending_map.module_enabled(module)) {
      ss << "module '" << module << "' is already disabled";
      r = 0;
      goto out;
    }
    if (!pending_map.modules.count(module)) {
      ss << "module '" << module << "' is not enabled";
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
    wait_for_commit(op, new Monitor::C_Command(mon, op, r, rs,
					      get_last_committed() + 1));
  } else {
    // reply immediately
    mon.reply_command(op, r, rs, rdata, get_last_committed());
  }

  if (plugged) {
    paxos.unplug();
  }

  return r >= 0;
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

int MgrMonitor::load_metadata(const string& name, std::map<string, string>& m,
			      ostream *err) const
{
  bufferlist bl;
  int r = mon.store->get(MGR_METADATA_PREFIX, name, bl);
  if (r < 0)
    return r;
  try {
    auto p = bl.cbegin();
    decode(m, p);
  }
  catch (ceph::buffer::error& e) {
    if (err)
      *err << "mgr." << name << " metadata is corrupt";
    return -EIO;
  }
  return 0;
}

void MgrMonitor::count_metadata(const string& field, std::map<string,int> *out)
{
  std::set<string> ls = map.get_all_names();
  for (auto& name : ls) {
    std::map<string,string> meta;
    load_metadata(name, meta, nullptr);
    auto p = meta.find(field);
    if (p == meta.end()) {
      (*out)["unknown"]++;
    } else {
      (*out)[p->second]++;
    }
  }
}

void MgrMonitor::count_metadata(const string& field, Formatter *f)
{
  std::map<string,int> by_val;
  count_metadata(field, &by_val);
  f->open_object_section(field.c_str());
  for (auto& p : by_val) {
    f->dump_int(p.first.c_str(), p.second);
  }
  f->close_section();
}

void MgrMonitor::get_versions(std::map<string, list<string> > &versions)
{
  std::set<string> ls = map.get_all_names();
  for (auto& name : ls) {
    std::map<string,string> meta;
    load_metadata(name, meta, nullptr);
    auto p = meta.find("ceph_version_short");
    if (p == meta.end()) continue;
    versions[p->second].push_back(string("mgr.") + name);
  }
}

int MgrMonitor::dump_metadata(const string& name, Formatter *f, ostream *err)
{
  std::map<string,string> m;
  if (int r = load_metadata(name, m, err))
    return r;
  for (auto& p : m) {
    f->dump_string(p.first.c_str(), p.second);
  }
  return 0;
}

void MgrMonitor::print_nodes(Formatter *f) const
{
  ceph_assert(f);

  std::map<string, list<string> > mgrs; // hostname => mgr
  auto ls = map.get_all_names();
  for (auto& name : ls) {
    std::map<string,string> meta;
    if (load_metadata(name, meta, nullptr)) {
      continue;
    }
    auto hostname = meta.find("hostname");
    if (hostname == meta.end()) {
      // not likely though
      continue;
    }
    mgrs[hostname->second].push_back(name);
  }

  dump_services(f, mgrs, "mgr");
}

const std::vector<MonCommand> &MgrMonitor::get_command_descs() const
{
  if (command_descs.empty()) {
    // must have just upgraded; fallback to static commands
    return mgr_commands;
  } else {
    return command_descs;
  }
}
