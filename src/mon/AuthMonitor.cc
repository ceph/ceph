// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include <sstream>

#include "mon/AuthMonitor.h"
#include "mon/Monitor.h"
#include "mon/MonitorDBStore.h"
#include "mon/OSDMonitor.h"
#include "mon/MDSMonitor.h"
#include "mon/ConfigMonitor.h"

#include "messages/MMonCommand.h"
#include "messages/MAuth.h"
#include "messages/MAuthReply.h"
#include "messages/MMonGlobalID.h"
#include "messages/MMonUsedPendingKeys.h"
#include "msg/Messenger.h"

#include "auth/AuthServiceHandler.h"
#include "auth/KeyRing.h"
#include "include/stringify.h"
#include "include/ceph_assert.h"

#include "mds/MDSAuthCaps.h"
#include "mgr/MgrCap.h"
#include "osd/OSDCap.h"

#define dout_subsys ceph_subsys_mon
#undef dout_prefix
#define dout_prefix _prefix(_dout, mon, get_last_committed())
using namespace TOPNSPC::common;

using std::cerr;
using std::cout;
using std::dec;
using std::hex;
using std::list;
using std::map;
using std::make_pair;
using std::ostream;
using std::ostringstream;
using std::pair;
using std::set;
using std::setfill;
using std::string;
using std::stringstream;
using std::to_string;
using std::vector;
using std::unique_ptr;

using ceph::bufferlist;
using ceph::decode;
using ceph::encode;
using ceph::Formatter;
using ceph::JSONFormatter;
using ceph::make_message;
using ceph::mono_clock;
using ceph::mono_time;
using ceph::timespan_str;
static ostream& _prefix(std::ostream *_dout, Monitor &mon, version_t v) {
  return *_dout << "mon." << mon.name << "@" << mon.rank
		<< "(" << mon.get_state_name()
		<< ").auth v" << v << " ";
}

ostream& operator<<(ostream &out, const AuthMonitor &pm)
{
  return out << "auth";
}

bool AuthMonitor::check_rotate()
{
  KeyServerData::Incremental rot_inc;
  rot_inc.op = KeyServerData::AUTH_INC_SET_ROTATING;
  if (mon.key_server.prepare_rotating_update(rot_inc.rotating_bl)) {
    dout(10) << __func__ << " updating rotating" << dendl;
    push_cephx_inc(rot_inc);
    return true;
  }
  return false;
}

void AuthMonitor::process_used_pending_keys(
  const std::map<EntityName,CryptoKey>& used_pending_keys)
{
  for (auto& [name, used_key] : used_pending_keys) {
    dout(10) << __func__ << " used pending_key for " << name << dendl;
    KeyServerData::Incremental inc;
    inc.op = KeyServerData::AUTH_INC_ADD;
    inc.name = name;

    mon.key_server.get_auth(name, inc.auth);
    for (auto& p : pending_auth) {
      if (p.inc_type == AUTH_DATA) {
	KeyServerData::Incremental auth_inc;
	auto q = p.auth_data.cbegin();
	decode(auth_inc, q);
	if (auth_inc.op == KeyServerData::AUTH_INC_ADD &&
	    auth_inc.name == name) {
	  dout(10) << __func__ << "  starting with pending uncommitted" << dendl;
	  inc.auth = auth_inc.auth;
	}
      }
    }
    if (stringify(inc.auth.pending_key) == stringify(used_key)) {
      dout(10) << __func__ << " committing pending_key -> key for "
	       << name << dendl;
      inc.auth.key = inc.auth.pending_key;
      inc.auth.pending_key.clear();
      push_cephx_inc(inc);
    }
  }
}

/*
 Tick function to update the map based on performance every N seconds
*/

void AuthMonitor::tick()
{
  if (!is_active()) return;

  dout(10) << *this << dendl;

  // increase global_id?
  bool propose = false;
  bool increase;
  {
    std::lock_guard l(mon.auth_lock);
    increase = _should_increase_max_global_id();
  }
  if (increase) {
    if (mon.is_leader()) {
      increase_max_global_id();
      propose = true;
    } else {
      dout(10) << __func__ << "requesting more ids from leader" << dendl;
      MMonGlobalID *req = new MMonGlobalID();
      req->old_max_id = max_global_id;
      mon.send_mon_message(req, mon.get_leader());
    }
  }

  if (mon.monmap->min_mon_release >= ceph_release_t::quincy) {
    auto used_pending_keys = mon.key_server.get_used_pending_keys();
    if (!used_pending_keys.empty()) {
      dout(10) << __func__ << " " << used_pending_keys.size() << " used pending_keys"
	       << dendl;
      if (mon.is_leader()) {
	process_used_pending_keys(used_pending_keys);
	propose = true;
      } else {
	MMonUsedPendingKeys *req = new MMonUsedPendingKeys();
	req->used_pending_keys = used_pending_keys;
	mon.send_mon_message(req, mon.get_leader());
      }
    }
  }

  if (!mon.is_leader()) {
    return;
  }

  if (check_rotate()) {
    propose = true;
  }

  if (propose) {
    propose_pending();
  }
}

void AuthMonitor::on_active()
{
  dout(10) << "AuthMonitor::on_active()" << dendl;

  if (!mon.is_leader())
    return;

  mon.key_server.start_server();
  mon.key_server.clear_used_pending_keys();

  if (is_writeable()) {
    bool propose = false;
    if (check_rotate()) {
      propose = true;
    }
    bool increase;
    {
      std::lock_guard l(mon.auth_lock);
      increase = _should_increase_max_global_id();
    }
    if (increase) {
      increase_max_global_id();
      propose = true;
    }
    if (propose) {
      propose_pending();
    }
  }
}

bufferlist _encode_cap(const string& cap)
{
  bufferlist bl;
  encode(cap, bl);
  return bl;
}

void AuthMonitor::get_initial_keyring(KeyRing *keyring)
{
  dout(10) << __func__ << dendl;
  ceph_assert(keyring != nullptr);

  bufferlist bl;
  int ret = mon.store->get("mkfs", "keyring", bl);
  if (ret == -ENOENT) {
    return;
  }
  // fail hard only if there's an error we're not expecting to see
  ceph_assert(ret == 0);

  auto p = bl.cbegin();
  decode(*keyring, p);
}

void _generate_bootstrap_keys(
    list<pair<EntityName,EntityAuth> >* auth_lst)
{
  ceph_assert(auth_lst != nullptr);

  map<string,map<string,bufferlist> > bootstrap = {
    { "admin", {
      { "mon", _encode_cap("allow *") },
      { "osd", _encode_cap("allow *") },
      { "mds", _encode_cap("allow *") },
      { "mgr", _encode_cap("allow *") }
    } },
    { "bootstrap-osd", {
      { "mon", _encode_cap("allow profile bootstrap-osd") }
    } },
    { "bootstrap-rgw", {
      { "mon", _encode_cap("allow profile bootstrap-rgw") }
    } },
    { "bootstrap-mds", {
      { "mon", _encode_cap("allow profile bootstrap-mds") }
    } },
    { "bootstrap-mgr", {
      { "mon", _encode_cap("allow profile bootstrap-mgr") }
    } },
    { "bootstrap-rbd", {
      { "mon", _encode_cap("allow profile bootstrap-rbd") }
    } },
    { "bootstrap-rbd-mirror", {
      { "mon", _encode_cap("allow profile bootstrap-rbd-mirror") }
    } }
  };

  for (auto &p : bootstrap) {
    EntityName name;
    name.from_str("client." + p.first);
    EntityAuth auth;
    auth.key.create(g_ceph_context, CEPH_CRYPTO_AES);
    auth.caps = p.second;

    auth_lst->push_back(make_pair(name, auth));
  }
}

void AuthMonitor::create_initial_keys(KeyRing *keyring)
{
  dout(10) << __func__ << " with keyring" << dendl;
  ceph_assert(keyring != nullptr);

  list<pair<EntityName,EntityAuth> > auth_lst;
  _generate_bootstrap_keys(&auth_lst);

  for (auto &p : auth_lst) {
    if (keyring->exists(p.first)) {
      continue;
    }
    keyring->add(p.first, p.second);
  }
}

void AuthMonitor::create_initial()
{
  dout(10) << "create_initial -- creating initial map" << dendl;

  // initialize rotating keys
  mon.key_server.clear_secrets();
  check_rotate();
  ceph_assert(pending_auth.size() == 1);

  if (mon.is_keyring_required()) {
    KeyRing keyring;
    // attempt to obtain an existing mkfs-time keyring
    get_initial_keyring(&keyring);
    // create missing keys in the keyring
    create_initial_keys(&keyring);
    // import the resulting keyring
    import_keyring(keyring);
  }

  max_global_id = MIN_GLOBAL_ID;

  Incremental inc;
  inc.inc_type = GLOBAL_ID;
  inc.max_global_id = max_global_id;
  pending_auth.push_back(inc);

  format_version = 3;
}

void AuthMonitor::update_from_paxos(bool *need_bootstrap)
{
  dout(10) << __func__ << dendl;
  load_health();

  version_t version = get_last_committed();
  version_t keys_ver = mon.key_server.get_ver();
  if (version == keys_ver)
    return;
  ceph_assert(version > keys_ver);

  version_t latest_full = get_version_latest_full();

  dout(10) << __func__ << " version " << version << " keys ver " << keys_ver
           << " latest " << latest_full << dendl;

  if ((latest_full > 0) && (latest_full > keys_ver)) {
    bufferlist latest_bl;
    int err = get_version_full(latest_full, latest_bl);
    ceph_assert(err == 0);
    ceph_assert(latest_bl.length() != 0);
    dout(7) << __func__ << " loading summary e " << latest_full << dendl;
    dout(7) << __func__ << " latest length " << latest_bl.length() << dendl;
    auto p = latest_bl.cbegin();
    __u8 struct_v;
    decode(struct_v, p);
    decode(max_global_id, p);
    decode(mon.key_server, p);
    mon.key_server.set_ver(latest_full);
    keys_ver = latest_full;
  }

  dout(10) << __func__ << " key server version " << mon.key_server.get_ver() << dendl;

  // walk through incrementals
  while (version > keys_ver) {
    bufferlist bl;
    int ret = get_version(keys_ver+1, bl);
    ceph_assert(ret == 0);
    ceph_assert(bl.length());

    // reset if we are moving to initial state.  we will normally have
    // keys in here temporarily for bootstrapping that we need to
    // clear out.
    if (keys_ver == 0)
      mon.key_server.clear_secrets();

    dout(20) << __func__ << " walking through version " << (keys_ver+1)
             << " len " << bl.length() << dendl;

    auto p = bl.cbegin();
    __u8 v;
    decode(v, p);
    while (!p.end()) {
      Incremental inc;
      decode(inc, p);
      switch (inc.inc_type) {
      case GLOBAL_ID:
	max_global_id = inc.max_global_id;
	break;

      case AUTH_DATA:
        {
          KeyServerData::Incremental auth_inc;
          auto iter = inc.auth_data.cbegin();
          decode(auth_inc, iter);
          mon.key_server.apply_data_incremental(auth_inc);
          break;
        }
      }
    }

    keys_ver++;
    mon.key_server.set_ver(keys_ver);

    if (keys_ver == 1 && mon.is_keyring_required()) {
      auto t(std::make_shared<MonitorDBStore::Transaction>());
      t->erase("mkfs", "keyring");
      mon.store->apply_transaction(t);
    }
  }

  {
    std::lock_guard l(mon.auth_lock);
    if (last_allocated_id == 0) {
      last_allocated_id = max_global_id;
      dout(10) << __func__ << " last_allocated_id initialized to "
	       << max_global_id << dendl;
    }
  }

  dout(10) << __func__ << " max_global_id=" << max_global_id
	   << " format_version " << format_version
	   << dendl;

  mon.key_server.dump();
}

bool AuthMonitor::_should_increase_max_global_id()
{
  ceph_assert(ceph_mutex_is_locked(mon.auth_lock));
  auto num_prealloc = g_conf()->mon_globalid_prealloc;
  if (max_global_id < num_prealloc ||
      (last_allocated_id + 1) >= max_global_id - num_prealloc / 2) {
    return true;
  }
  return false;
}

void AuthMonitor::increase_max_global_id()
{
  ceph_assert(mon.is_leader());

  Incremental inc;
  inc.inc_type = GLOBAL_ID;
  inc.max_global_id = max_global_id + g_conf()->mon_globalid_prealloc;
  dout(10) << "increasing max_global_id to " << inc.max_global_id << dendl;
  pending_auth.push_back(inc);
}

bool AuthMonitor::should_propose(double& delay)
{
  return (!pending_auth.empty());
}

void AuthMonitor::create_pending()
{
  pending_auth.clear();
  dout(10) << "create_pending v " << (get_last_committed() + 1) << dendl;
}

void AuthMonitor::encode_pending(MonitorDBStore::TransactionRef t)
{
  dout(10) << __func__ << " v " << (get_last_committed() + 1) << dendl;

  bufferlist bl;

  __u8 v = 1;
  encode(v, bl);
  vector<Incremental>::iterator p;
  for (p = pending_auth.begin(); p != pending_auth.end(); ++p)
    p->encode(bl, mon.get_quorum_con_features());

  version_t version = get_last_committed() + 1;
  put_version(t, version, bl);
  put_last_committed(t, version);

  // health
  health_check_map_t next;
  map<string,list<string>> bad_detail;  // entity -> details
  for (auto i = mon.key_server.secrets_begin();
       i != mon.key_server.secrets_end();
       ++i) {
    for (auto& p : i->second.caps) {
      ostringstream ss;
      if (!valid_caps(p.first, p.second, &ss)) {
	ostringstream ss2;
	ss2 << i->first << " " << ss.str();
	bad_detail[i->first.to_str()].push_back(ss2.str());
      }
    }
  }
  for (auto& inc : pending_auth) {
    if (inc.inc_type == AUTH_DATA) {
      KeyServerData::Incremental auth_inc;
      auto iter = inc.auth_data.cbegin();
      decode(auth_inc, iter);
      if (auth_inc.op == KeyServerData::AUTH_INC_DEL) {
	bad_detail.erase(auth_inc.name.to_str());
      } else if (auth_inc.op == KeyServerData::AUTH_INC_ADD) {
	for (auto& p : auth_inc.auth.caps) {
	  ostringstream ss;
	  if (!valid_caps(p.first, p.second, &ss)) {
	    ostringstream ss2;
	    ss2 << auth_inc.name << " " << ss.str();
	    bad_detail[auth_inc.name.to_str()].push_back(ss2.str());
	  }
	}
      }
    }
  }
  if (bad_detail.size()) {
    ostringstream ss;
    ss << bad_detail.size() << " auth entities have invalid capabilities";
    health_check_t *check = &next.add("AUTH_BAD_CAPS", HEALTH_ERR, ss.str(),
				      bad_detail.size());
    for (auto& i : bad_detail) {
      for (auto& j : i.second) {
	check->detail.push_back(j);
      }
    }
  }
  encode_health(next, t);
}

void AuthMonitor::encode_full(MonitorDBStore::TransactionRef t)
{
  version_t version = mon.key_server.get_ver();
  // do not stash full version 0 as it will never be removed nor read
  if (version == 0)
    return;

  dout(10) << __func__ << " auth v " << version << dendl;
  ceph_assert(get_last_committed() == version);

  bufferlist full_bl;
  std::scoped_lock l{mon.key_server.get_lock()};
  dout(20) << __func__ << " key server has "
           << (mon.key_server.has_secrets() ? "" : "no ")
           << "secrets!" << dendl;
  __u8 v = 1;
  encode(v, full_bl);
  encode(max_global_id, full_bl);
  encode(mon.key_server, full_bl);

  put_version_full(t, version, full_bl);
  put_version_latest_full(t, version);
}

version_t AuthMonitor::get_trim_to() const
{
  unsigned max = g_conf()->paxos_max_join_drift * 2;
  version_t version = get_last_committed();
  if (mon.is_leader() && (version > max))
    return version - max;
  return 0;
}

bool AuthMonitor::preprocess_query(MonOpRequestRef op)
{
  auto m = op->get_req<PaxosServiceMessage>();
  dout(10) << "preprocess_query " << *m << " from " << m->get_orig_source_inst() << dendl;
  switch (m->get_type()) {
  case MSG_MON_COMMAND:
    try {
      return preprocess_command(op);
    } catch (const bad_cmd_get& e) {
      bufferlist bl;
      mon.reply_command(op, -EINVAL, e.what(), bl, get_last_committed());
      return true;
    }

  case CEPH_MSG_AUTH:
    return prep_auth(op, false);

  case MSG_MON_GLOBAL_ID:
    return false;

  case MSG_MON_USED_PENDING_KEYS:
    return false;

  default:
    ceph_abort();
    return true;
  }
}

bool AuthMonitor::prepare_update(MonOpRequestRef op)
{
  auto m = op->get_req<PaxosServiceMessage>();
  dout(10) << "prepare_update " << *m << " from " << m->get_orig_source_inst() << dendl;
  switch (m->get_type()) {
  case MSG_MON_COMMAND:
    try {
      return prepare_command(op);
    } catch (const bad_cmd_get& e) {
      bufferlist bl;
      mon.reply_command(op, -EINVAL, e.what(), bl, get_last_committed());
      return true;
    }
  case MSG_MON_GLOBAL_ID:
    return prepare_global_id(op);
  case MSG_MON_USED_PENDING_KEYS:
    return prepare_used_pending_keys(op);
  case CEPH_MSG_AUTH:
    return prep_auth(op, true);
  default:
    ceph_abort();
    return false;
  }
}

void AuthMonitor::_set_mon_num_rank(int num, int rank)
{
  dout(10) << __func__ << " num " << num << " rank " << rank << dendl;
  ceph_assert(ceph_mutex_is_locked(mon.auth_lock));
  mon_num = num;
  mon_rank = rank;
}

uint64_t AuthMonitor::_assign_global_id()
{
  ceph_assert(ceph_mutex_is_locked(mon.auth_lock));
  if (mon_num < 1 || mon_rank < 0) {
    dout(10) << __func__ << " inactive (num_mon " << mon_num
	     << " rank " << mon_rank << ")" << dendl;
    return 0;
  }
  if (!last_allocated_id) {
    dout(10) << __func__ << " last_allocated_id == 0" << dendl;
    return 0;
  }

  uint64_t id = last_allocated_id + 1;
  int remainder = id % mon_num;
  if (remainder) {
    remainder = mon_num - remainder;
  }
  id += remainder + mon_rank;

  if (id >= max_global_id) {
    dout(10) << __func__ << " failed (max " << max_global_id << ")" << dendl;
    return 0;
  }

  last_allocated_id = id;
  dout(10) << __func__ << " " << id << " (max " << max_global_id << ")"
	   << dendl;
  return id;
}

uint64_t AuthMonitor::assign_global_id(bool should_increase_max)
{
  uint64_t id;
  {
    std::lock_guard l(mon.auth_lock);
    id =_assign_global_id();
    if (should_increase_max) {
      should_increase_max = _should_increase_max_global_id();
    }
  }
  if (mon.is_leader() &&
      should_increase_max) {
    increase_max_global_id();
  }
  return id;
}

bool AuthMonitor::prep_auth(MonOpRequestRef op, bool paxos_writable)
{
  auto m = op->get_req<MAuth>();
  dout(10) << "prep_auth() blob_size=" << m->get_auth_payload().length() << dendl;

  MonSession *s = op->get_session();
  if (!s) {
    dout(10) << "no session, dropping" << dendl;
    return true;
  }

  int ret = 0;
  MAuthReply *reply;
  bufferlist response_bl;
  auto indata = m->auth_payload.cbegin();
  __u32 proto = m->protocol;
  bool start = false;
  bool finished = false;
  EntityName entity_name;
  bool is_new_global_id = false;

  // set up handler?
  if (m->protocol == 0 && !s->auth_handler) {
    set<__u32> supported;

    try {
      __u8 struct_v = 1;
      decode(struct_v, indata);
      decode(supported, indata);
      decode(entity_name, indata);
      decode(s->con->peer_global_id, indata);
    } catch (const ceph::buffer::error &e) {
      dout(10) << "failed to decode initial auth message" << dendl;
      ret = -EINVAL;
      goto reply;
    }

    // do we require cephx signatures?

    if (!m->get_connection()->has_feature(CEPH_FEATURE_MSG_AUTH)) {
      if (entity_name.get_type() == CEPH_ENTITY_TYPE_MON ||
	  entity_name.get_type() == CEPH_ENTITY_TYPE_OSD ||
	  entity_name.get_type() == CEPH_ENTITY_TYPE_MDS ||
	  entity_name.get_type() == CEPH_ENTITY_TYPE_MGR) {
	if (g_conf()->cephx_cluster_require_signatures ||
	    g_conf()->cephx_require_signatures) {
	  dout(1) << m->get_source_inst()
                  << " supports cephx but not signatures and"
                  << " 'cephx [cluster] require signatures = true';"
                  << " disallowing cephx" << dendl;
	  supported.erase(CEPH_AUTH_CEPHX);
	}
      } else {
	if (g_conf()->cephx_service_require_signatures ||
	    g_conf()->cephx_require_signatures) {
	  dout(1) << m->get_source_inst()
                  << " supports cephx but not signatures and"
                  << " 'cephx [service] require signatures = true';"
                  << " disallowing cephx" << dendl;
	  supported.erase(CEPH_AUTH_CEPHX);
	}
      }
    } else if (!m->get_connection()->has_feature(CEPH_FEATURE_CEPHX_V2)) {
      if (entity_name.get_type() == CEPH_ENTITY_TYPE_MON ||
	  entity_name.get_type() == CEPH_ENTITY_TYPE_OSD ||
	  entity_name.get_type() == CEPH_ENTITY_TYPE_MDS ||
	  entity_name.get_type() == CEPH_ENTITY_TYPE_MGR) {
	if (g_conf()->cephx_cluster_require_version >= 2 ||
	    g_conf()->cephx_require_version >= 2) {
	  dout(1) << m->get_source_inst()
                  << " supports cephx but not v2 and"
                  << " 'cephx [cluster] require version >= 2';"
                  << " disallowing cephx" << dendl;
	  supported.erase(CEPH_AUTH_CEPHX);
	}
      } else {
	if (g_conf()->cephx_service_require_version >= 2 ||
	    g_conf()->cephx_require_version >= 2) {
	  dout(1) << m->get_source_inst()
                  << " supports cephx but not v2 and"
                  << " 'cephx [service] require version >= 2';"
                  << " disallowing cephx" << dendl;
	  supported.erase(CEPH_AUTH_CEPHX);
	}
      }
    }

    int type;
    if (entity_name.get_type() == CEPH_ENTITY_TYPE_MON ||
	entity_name.get_type() == CEPH_ENTITY_TYPE_OSD ||
	entity_name.get_type() == CEPH_ENTITY_TYPE_MDS ||
	entity_name.get_type() == CEPH_ENTITY_TYPE_MGR)
      type = mon.auth_cluster_required.pick(supported);
    else
      type = mon.auth_service_required.pick(supported);

    s->auth_handler = get_auth_service_handler(type, g_ceph_context, &mon.key_server);
    if (!s->auth_handler) {
      dout(1) << "client did not provide supported auth type" << dendl;
      ret = -ENOTSUP;
      goto reply;
    }
    start = true;
    proto = type;
  } else if (!s->auth_handler) {
      dout(10) << "protocol specified but no s->auth_handler" << dendl;
      ret = -EINVAL;
      goto reply;
  }

  /* assign a new global_id? we assume this should only happen on the first
     request. If a client tries to send it later, it'll screw up its auth
     session */
  if (!s->con->peer_global_id) {
    s->con->peer_global_id = assign_global_id(paxos_writable);
    if (!s->con->peer_global_id) {

      delete s->auth_handler;
      s->auth_handler = NULL;

      if (mon.is_leader() && paxos_writable) {
        dout(10) << "increasing global id, waitlisting message" << dendl;
        wait_for_active(op, new C_RetryMessage(this, op));
        goto done;
      }

      if (!mon.is_leader()) {
	dout(10) << "not the leader, requesting more ids from leader" << dendl;
	int leader = mon.get_leader();
	MMonGlobalID *req = new MMonGlobalID();
	req->old_max_id = max_global_id;
	mon.send_mon_message(req, leader);
	wait_for_finished_proposal(op, new C_RetryMessage(this, op));
	return true;
      }

      ceph_assert(!paxos_writable);
      return false;
    }
    is_new_global_id = true;
  }

  try {
    if (start) {
      // new session
      ret = s->auth_handler->start_session(entity_name,
					   s->con->peer_global_id,
					   is_new_global_id,
					   &response_bl,
					   &s->con->peer_caps_info);
    } else {
      // request
      ret = s->auth_handler->handle_request(
	indata,
	0, // no connection_secret needed
	&response_bl,
	&s->con->peer_caps_info,
	nullptr, nullptr);
    }
    if (ret == -EIO) {
      wait_for_active(op, new C_RetryMessage(this,op));
      goto done;
    }
    if (ret > 0) {
      if (!s->authenticated &&
	  mon.ms_handle_fast_authentication(s->con.get()) > 0) {
	finished = true;
      }
      ret = 0;
    }
  } catch (const ceph::buffer::error &err) {
    ret = -EINVAL;
    dout(0) << "caught error when trying to handle auth request, probably malformed request" << dendl;
  }

reply:
  reply = new MAuthReply(proto, &response_bl, ret, s->con->peer_global_id);
  mon.send_reply(op, reply);
  if (finished) {
    // always send the latest monmap.
    if (m->monmap_epoch < mon.monmap->get_epoch())
      mon.send_latest_monmap(m->get_connection().get());

    mon.configmon()->check_sub(s);
  }
done:
  return true;
}

bool AuthMonitor::preprocess_command(MonOpRequestRef op)
{
  auto m = op->get_req<MMonCommand>();
  int r = -1;
  bufferlist rdata;
  stringstream ss, ds;

  cmdmap_t cmdmap;
  if (!cmdmap_from_json(m->cmd, &cmdmap, ss)) {
    // ss has reason for failure
    string rs = ss.str();
    mon.reply_command(op, -EINVAL, rs, rdata, get_last_committed());
    return true;
  }

  string prefix;
  cmd_getval(cmdmap, "prefix", prefix);
  if (prefix == "auth add" ||
      prefix == "auth del" ||
      prefix == "auth rm" ||
      prefix == "auth get-or-create" ||
      prefix == "auth get-or-create-key" ||
      prefix == "auth get-or-create-pending" ||
      prefix == "auth clear-pending" ||
      prefix == "auth commit-pending" ||
      prefix == "fs authorize" ||
      prefix == "auth import" ||
      prefix == "auth caps") {
    return false;
  }

  MonSession *session = op->get_session();
  if (!session) {
    mon.reply_command(op, -EACCES, "access denied", rdata, get_last_committed());
    return true;
  }

  // entity might not be supplied, but if it is, it should be valid
  string entity_name;
  cmd_getval(cmdmap, "entity", entity_name);
  EntityName entity;
  if (!entity_name.empty() && !entity.from_str(entity_name)) {
    ss << "invalid entity_auth " << entity_name;
    mon.reply_command(op, -EINVAL, ss.str(), get_last_committed());
    return true;
  }

  string format = cmd_getval_or<string>(cmdmap, "format", "plain");
  boost::scoped_ptr<Formatter> f(Formatter::create(format));

  if (prefix == "auth export") {
    KeyRing keyring;
    export_keyring(keyring);
    if (!entity_name.empty()) {
      EntityAuth eauth;
      if (keyring.get_auth(entity, eauth)) {
	KeyRing kr;
	kr.add(entity, eauth);
	if (f)
	  kr.encode_formatted("auth", f.get(), rdata);
	else
	  kr.encode_plaintext(rdata);
	r = 0;
      } else {
	ss << "no key for " << eauth;
	r = -ENOENT;
      }
    } else {
      if (f)
	keyring.encode_formatted("auth", f.get(), rdata);
      else
	keyring.encode_plaintext(rdata);
      r = 0;
    }
  } else if (prefix == "auth get" && !entity_name.empty()) {
    KeyRing keyring;
    EntityAuth entity_auth;
    if (!mon.key_server.get_auth(entity, entity_auth)) {
      ss << "failed to find " << entity_name << " in keyring";
      r = -ENOENT;
    } else {
      keyring.add(entity, entity_auth);
      if (f)
	keyring.encode_formatted("auth", f.get(), rdata);
      else
	keyring.encode_plaintext(rdata);
      r = 0;
    }
  } else if (prefix == "auth print-key" ||
	     prefix == "auth print_key" ||
	     prefix == "auth get-key") {
    EntityAuth auth;
    if (!mon.key_server.get_auth(entity, auth)) {
      ss << "don't have " << entity;
      r = -ENOENT;
      goto done;
    }
    if (f) {
      auth.key.encode_formatted("auth", f.get(), rdata);
    } else {
      auth.key.encode_plaintext(rdata);
    }
    r = 0;
  } else if (prefix == "auth list" ||
	     prefix == "auth ls") {
    if (f) {
      mon.key_server.encode_formatted("auth", f.get(), rdata);
    } else {
      mon.key_server.encode_plaintext(rdata);
    }
    r = 0;
    goto done;
  } else {
    ss << "invalid command";
    r = -EINVAL;
  }

 done:
  rdata.append(ds);
  string rs;
  getline(ss, rs, '\0');
  mon.reply_command(op, r, rs, rdata, get_last_committed());
  return true;
}

void AuthMonitor::export_keyring(KeyRing& keyring)
{
  mon.key_server.export_keyring(keyring);
}

int AuthMonitor::import_keyring(KeyRing& keyring)
{
  dout(10) << __func__ << " " << keyring.size() << " keys" << dendl;

  for (map<EntityName, EntityAuth>::iterator p = keyring.get_keys().begin();
       p != keyring.get_keys().end();
       ++p) {
    if (p->second.caps.empty()) {
      dout(0) << "import: no caps supplied" << dendl;
      return -EINVAL;
    }
    int err = add_entity(p->first, p->second);
    ceph_assert(err == 0);
  }
  return 0;
}

int AuthMonitor::remove_entity(const EntityName &entity)
{
  dout(10) << __func__ << " " << entity << dendl;
  if (!mon.key_server.contains(entity))
    return -ENOENT;

  KeyServerData::Incremental auth_inc;
  auth_inc.name = entity;
  auth_inc.op = KeyServerData::AUTH_INC_DEL;
  push_cephx_inc(auth_inc);

  return 0;
}

bool AuthMonitor::entity_is_pending(EntityName& entity)
{
  // are we about to have it?
  for (auto& p : pending_auth) {
    if (p.inc_type == AUTH_DATA) {
      KeyServerData::Incremental inc;
      auto q = p.auth_data.cbegin();
      decode(inc, q);
      if (inc.op == KeyServerData::AUTH_INC_ADD &&
          inc.name == entity) {
        return true;
      }
    }
  }
  return false;
}

int AuthMonitor::exists_and_matches_entity(
    const auth_entity_t& entity,
    bool has_secret,
    stringstream& ss)
{
  return exists_and_matches_entity(entity.name, entity.auth,
                                   entity.auth.caps, has_secret, ss);
}

int AuthMonitor::exists_and_matches_entity(
    const EntityName& name,
    const EntityAuth& auth,
    const map<string,bufferlist>& caps,
    bool has_secret,
    stringstream& ss)
{

  dout(20) << __func__ << " entity " << name << " auth " << auth
           << " caps " << caps << " has_secret " << has_secret << dendl;

  EntityAuth existing_auth;
  // does entry already exist?
  if (mon.key_server.get_auth(name, existing_auth)) {
    // key match?
    if (has_secret) {
      if (existing_auth.key.get_secret().cmp(auth.key.get_secret())) {
        ss << "entity " << name << " exists but key does not match";
        return -EEXIST;
      }
    }

    // caps match?
    if (caps.size() != existing_auth.caps.size()) {
      ss << "entity " << name << " exists but caps do not match";
      return -EINVAL;
    }
    for (auto& it : caps) {
      if (existing_auth.caps.count(it.first) == 0 ||
          !existing_auth.caps[it.first].contents_equal(it.second)) {
        ss << "entity " << name << " exists but cap "
          << it.first << " does not match";
        return -EINVAL;
      }
    }

    // they match, no-op
    return 0;
  }
  return -ENOENT;
}

int AuthMonitor::add_entity(
    const EntityName& name,
    const EntityAuth& auth)
{

  // okay, add it.
  KeyServerData::Incremental auth_inc;
  auth_inc.op = KeyServerData::AUTH_INC_ADD;
  auth_inc.name = name;
  auth_inc.auth = auth;

  dout(10) << " add auth entity " << auth_inc.name << dendl;
  dout(30) << "    " << auth_inc.auth << dendl;
  push_cephx_inc(auth_inc);
  return 0;
}

int AuthMonitor::validate_osd_destroy(
    int32_t id,
    const uuid_d& uuid,
    EntityName& cephx_entity,
    EntityName& lockbox_entity,
    stringstream& ss)
{
  ceph_assert(paxos.is_plugged());

  dout(10) << __func__ << " id " << id << " uuid " << uuid << dendl;

  string cephx_str = "osd." + stringify(id);
  string lockbox_str = "client.osd-lockbox." + stringify(uuid);

  if (!cephx_entity.from_str(cephx_str)) {
    dout(10) << __func__ << " invalid cephx entity '"
             << cephx_str << "'" << dendl;
    ss << "invalid cephx key entity '" << cephx_str << "'";
    return -EINVAL;
  }

  if (!lockbox_entity.from_str(lockbox_str)) {
    dout(10) << __func__ << " invalid lockbox entity '"
             << lockbox_str << "'" << dendl;
    ss << "invalid lockbox key entity '" << lockbox_str << "'";
    return -EINVAL;
  }

  if (!mon.key_server.contains(cephx_entity) &&
      !mon.key_server.contains(lockbox_entity)) {
    return -ENOENT;
  }

  return 0;
}

int AuthMonitor::do_osd_destroy(
    const EntityName& cephx_entity,
    const EntityName& lockbox_entity)
{
  ceph_assert(paxos.is_plugged());

  dout(10) << __func__ << " cephx " << cephx_entity
                       << " lockbox " << lockbox_entity << dendl;

  bool removed = false;

  int err = remove_entity(cephx_entity);
  if (err == -ENOENT) {
    dout(10) << __func__ << " " << cephx_entity << " does not exist" << dendl;
  } else {
    removed = true;
  }

  err = remove_entity(lockbox_entity);
  if (err == -ENOENT) {
    dout(10) << __func__ << " " << lockbox_entity << " does not exist" << dendl;
  } else {
    removed = true;
  }

  if (!removed) {
    dout(10) << __func__ << " entities do not exist -- no-op." << dendl;
    return 0;
  }

  // given we have paxos plugged, this will not result in a proposal
  // being triggered, but it will still be needed so that we get our
  // pending state encoded into the paxos' pending transaction.
  propose_pending();
  return 0;
}

int _create_auth(
    EntityAuth& auth,
    const string& key,
    const map<string,bufferlist>& caps)
{
  if (key.empty())
    return -EINVAL;
  try {
    auth.key.decode_base64(key);
  } catch (ceph::buffer::error& e) {
    return -EINVAL;
  }
  auth.caps = caps;
  return 0;
}

int AuthMonitor::validate_osd_new(
    int32_t id,
    const uuid_d& uuid,
    const string& cephx_secret,
    const string& lockbox_secret,
    auth_entity_t& cephx_entity,
    auth_entity_t& lockbox_entity,
    stringstream& ss)
{

  dout(10) << __func__ << " osd." << id << " uuid " << uuid << dendl;

  map<string,bufferlist> cephx_caps = {
    { "osd", _encode_cap("allow *") },
    { "mon", _encode_cap("allow profile osd") },
    { "mgr", _encode_cap("allow profile osd") }
  };
  map<string,bufferlist> lockbox_caps = {
    { "mon", _encode_cap("allow command \"config-key get\" "
        "with key=\"dm-crypt/osd/" +
        stringify(uuid) +
        "/luks\"") }
  };

  bool has_lockbox = !lockbox_secret.empty();

  string cephx_name = "osd." + stringify(id);
  string lockbox_name = "client.osd-lockbox." + stringify(uuid);

  if (!cephx_entity.name.from_str(cephx_name)) {
    dout(10) << __func__ << " invalid cephx entity '"
             << cephx_name << "'" << dendl;
    ss << "invalid cephx key entity '" << cephx_name << "'";
    return -EINVAL;
  }

  if (has_lockbox) {
    if (!lockbox_entity.name.from_str(lockbox_name)) {
      dout(10) << __func__ << " invalid cephx lockbox entity '"
               << lockbox_name << "'" << dendl;
      ss << "invalid cephx lockbox entity '" << lockbox_name << "'";
      return -EINVAL;
    }
  }

  if (entity_is_pending(cephx_entity.name) ||
      (has_lockbox && entity_is_pending(lockbox_entity.name))) {
    // If we have pending entities for either the cephx secret or the
    // lockbox secret, then our safest bet is to retry the command at
    // a later time. These entities may be pending because an `osd new`
    // command has been run (which is unlikely, due to the nature of
    // the operation, which will force a paxos proposal), or (more likely)
    // because a competing client created those entities before we handled
    // the `osd new` command. Regardless, let's wait and see.
    return -EAGAIN;
  }

  if (!is_valid_cephx_key(cephx_secret)) {
    ss << "invalid cephx secret.";
    return -EINVAL;
  }

  if (has_lockbox && !is_valid_cephx_key(lockbox_secret)) {
    ss << "invalid cephx lockbox secret.";
    return -EINVAL;
  }

  int err = _create_auth(cephx_entity.auth, cephx_secret, cephx_caps);
  ceph_assert(0 == err);

  bool cephx_is_idempotent = false, lockbox_is_idempotent = false;
  err = exists_and_matches_entity(cephx_entity, true, ss);

  if (err != -ENOENT) {
    if (err < 0) {
      return err;
    }
    ceph_assert(0 == err);
    cephx_is_idempotent = true;
  }

  if (has_lockbox) {
    err = _create_auth(lockbox_entity.auth, lockbox_secret, lockbox_caps);
    ceph_assert(err == 0);
    err = exists_and_matches_entity(lockbox_entity, true, ss);
    if (err != -ENOENT) {
      if (err < 0) {
        return err;
      }
      ceph_assert(0 == err);
      lockbox_is_idempotent = true;
    }
  }

  if (cephx_is_idempotent && (!has_lockbox || lockbox_is_idempotent)) {
    return EEXIST;
  }

  return 0;
}

int AuthMonitor::do_osd_new(
    const auth_entity_t& cephx_entity,
    const auth_entity_t& lockbox_entity,
    bool has_lockbox)
{
  ceph_assert(paxos.is_plugged());

  dout(10) << __func__ << " cephx " << cephx_entity.name
           << " lockbox ";
  if (has_lockbox) {
    *_dout << lockbox_entity.name;
  } else {
    *_dout << "n/a";
  }
  *_dout << dendl;

  // we must have validated before reaching this point.
  // if keys exist, then this means they also match; otherwise we would
  // have failed before calling this function.
  bool cephx_exists = mon.key_server.contains(cephx_entity.name);

  if (!cephx_exists) {
    int err = add_entity(cephx_entity.name, cephx_entity.auth);
    ceph_assert(0 == err);
  }

  if (has_lockbox &&
      !mon.key_server.contains(lockbox_entity.name)) {
    int err = add_entity(lockbox_entity.name, lockbox_entity.auth);
    ceph_assert(0 == err);
  }

  // given we have paxos plugged, this will not result in a proposal
  // being triggered, but it will still be needed so that we get our
  // pending state encoded into the paxos' pending transaction.
  propose_pending();
  return 0;
}

bool AuthMonitor::valid_caps(
    const string& type,
    const string& caps,
    ostream *out)
{
  if (type == "mon") {
    MonCap moncap;
    if (!moncap.parse(caps, out)) {
      return false;
    }
    return true;
  }

  if (!g_conf().get_val<bool>("mon_auth_validate_all_caps")) {
    return true;
  }

  if (type == "mgr") {
    MgrCap mgrcap;
    if (!mgrcap.parse(caps, out)) {
      return false;
    }
  } else if (type == "osd") {
    OSDCap ocap;
    if (!ocap.parse(caps, out)) {
      return false;
    }
  } else if (type == "mds") {
    MDSAuthCaps mdscap;
    if (!mdscap.parse(caps, out)) {
      return false;
    }
  } else {
    if (out) {
      *out << "unknown cap type '" << type << "'";
    }
    return false;
  }
  return true;
}

bool AuthMonitor::valid_caps(const vector<string>& caps, ostream *out)
{
  for (vector<string>::const_iterator p = caps.begin();
       p != caps.end(); p += 2) {
    if ((p+1) == caps.end()) {
      *out << "cap '" << *p << "' has no value";
      return false;
    }
    if (!valid_caps(*p, *(p+1), out)) {
      return false;
    }
  }
  return true;
}

bool AuthMonitor::prepare_command(MonOpRequestRef op)
{
  auto m = op->get_req<MMonCommand>();
  stringstream ss, ds;
  bufferlist rdata;
  string rs;
  int err = -EINVAL;

  cmdmap_t cmdmap;
  if (!cmdmap_from_json(m->cmd, &cmdmap, ss)) {
    // ss has reason for failure
    string rs = ss.str();
    mon.reply_command(op, -EINVAL, rs, rdata, get_last_committed());
    return true;
  }

  string prefix;
  vector<string>caps_vec;
  string entity_name;
  EntityName entity;

  cmd_getval(cmdmap, "prefix", prefix);

  string format = cmd_getval_or<string>(cmdmap, "format", "plain");
  boost::scoped_ptr<Formatter> f(Formatter::create(format));

  MonSession *session = op->get_session();
  if (!session) {
    mon.reply_command(op, -EACCES, "access denied", rdata, get_last_committed());
    return true;
  }

  cmd_getval(cmdmap, "caps", caps_vec);
  // fs authorize command's can have odd number of caps arguments
  if ((prefix != "fs authorize") && (caps_vec.size() % 2) != 0) {
    ss << "bad capabilities request; odd number of arguments";
    err = -EINVAL;
    goto done;
  }

  cmd_getval(cmdmap, "entity", entity_name);
  if (!entity_name.empty() && !entity.from_str(entity_name)) {
    ss << "bad entity name";
    err = -EINVAL;
    goto done;
  }

  if (prefix == "auth import") {
    bufferlist bl = m->get_data();
    if (bl.length() == 0) {
      ss << "auth import: no data supplied";
      getline(ss, rs);
      mon.reply_command(op, -EINVAL, rs, get_last_committed());
      return true;
    }
    auto iter = bl.cbegin();
    KeyRing keyring;
    try {
      decode(keyring, iter);
    } catch (const ceph::buffer::error &ex) {
      ss << "error decoding keyring" << " " << ex.what();
      err = -EINVAL;
      goto done;
    }
    err = import_keyring(keyring);
    if (err < 0) {
      ss << "auth import: no caps supplied";
      getline(ss, rs);
      mon.reply_command(op, -EINVAL, rs, get_last_committed());
      return true;
    }
    err = 0;
    wait_for_finished_proposal(op, new Monitor::C_Command(mon, op, 0, rs,
					      get_last_committed() + 1));
    return true;
  } else if (prefix == "auth add" && !entity_name.empty()) {
    /* expected behavior:
     *  - if command reproduces current state, return 0.
     *  - if command adds brand new entity, handle it.
     *  - if command adds new state to existing entity, return error.
     */
    KeyServerData::Incremental auth_inc;
    auth_inc.name = entity;
    bufferlist bl = m->get_data();
    bool has_keyring = (bl.length() > 0);
    map<string,bufferlist> new_caps;

    KeyRing new_keyring;
    if (has_keyring) {
      auto iter = bl.cbegin();
      try {
        decode(new_keyring, iter);
      } catch (const ceph::buffer::error &ex) {
        ss << "error decoding keyring";
        err = -EINVAL;
        goto done;
      }
    }

    if (!valid_caps(caps_vec, &ss)) {
      err = -EINVAL;
      goto done;
    }

    // are we about to have it?
    if (entity_is_pending(entity)) {
      wait_for_finished_proposal(op,
          new Monitor::C_Command(mon, op, 0, rs, get_last_committed() + 1));
      return true;
    }

    // build new caps from provided arguments (if available)
    for (vector<string>::iterator it = caps_vec.begin();
	 it != caps_vec.end() && (it + 1) != caps_vec.end();
	 it += 2) {
      string sys = *it;
      bufferlist cap;
      encode(*(it+1), cap);
      new_caps[sys] = cap;
    }

    // pull info out of provided keyring
    EntityAuth new_inc;
    if (has_keyring) {
      if (!new_keyring.get_auth(auth_inc.name, new_inc)) {
	ss << "key for " << auth_inc.name
	   << " not found in provided keyring";
	err = -EINVAL;
	goto done;
      }
      if (!new_caps.empty() && !new_inc.caps.empty()) {
	ss << "caps cannot be specified both in keyring and in command";
	err = -EINVAL;
	goto done;
      }
      if (new_caps.empty()) {
	new_caps = new_inc.caps;
      }
    }

    err = exists_and_matches_entity(auth_inc.name, new_inc,
                                    new_caps, has_keyring, ss);
    // if entity/key/caps do not exist in the keyring, just fall through
    // and add the entity; otherwise, make sure everything matches (in
    // which case it's a no-op), because if not we must fail.
    if (err != -ENOENT) {
      if (err < 0) {
        goto done;
      }
      // no-op.
      ceph_assert(err == 0);
      goto done;
    }
    err = 0;

    // okay, add it.
    if (!has_keyring) {
      dout(10) << "AuthMonitor::prepare_command generating random key for "
        << auth_inc.name << dendl;
      new_inc.key.create(g_ceph_context, CEPH_CRYPTO_AES);
    }
    new_inc.caps = new_caps;

    err = add_entity(auth_inc.name, new_inc);
    ceph_assert(err == 0);

    ss << "added key for " << auth_inc.name;
    getline(ss, rs);
    wait_for_finished_proposal(op, new Monitor::C_Command(mon, op, 0, rs,
						   get_last_committed() + 1));
    return true;
  } else if ((prefix == "auth get-or-create-pending" ||
	      prefix == "auth clear-pending" ||
	      prefix == "auth commit-pending")) {
    if (mon.monmap->min_mon_release < ceph_release_t::quincy) {
      err = -EPERM;
      ss << "pending_keys are not available until after upgrading to quincy";
      goto done;
    }

    EntityAuth entity_auth;
    if (!mon.key_server.get_auth(entity, entity_auth)) {
      ss << "entity " << entity << " does not exist";
      err = -ENOENT;
      goto done;
    }

    // is there an uncommitted pending_key? (or any change for this entity)
    for (auto& p : pending_auth) {
      if (p.inc_type == AUTH_DATA) {
	KeyServerData::Incremental auth_inc;
	auto q = p.auth_data.cbegin();
	decode(auth_inc, q);
	if (auth_inc.op == KeyServerData::AUTH_INC_ADD &&
	    auth_inc.name == entity) {
	  wait_for_finished_proposal(op, new Monitor::C_Command(mon, op, 0, rs,
						get_last_committed() + 1));
	  return true;
	}
      }
    }

    if (prefix == "auth get-or-create-pending") {
      KeyRing kr;
      bool exists = false;
      if (!entity_auth.pending_key.empty()) {
	kr.add(entity, entity_auth.key, entity_auth.pending_key);
	err = 0;
	exists = true;
      } else {
	KeyServerData::Incremental auth_inc;
	auth_inc.op = KeyServerData::AUTH_INC_ADD;
	auth_inc.name = entity;
	auth_inc.auth = entity_auth;
	auth_inc.auth.pending_key.create(g_ceph_context, CEPH_CRYPTO_AES);
	push_cephx_inc(auth_inc);
	kr.add(entity, auth_inc.auth.key, auth_inc.auth.pending_key);
        push_cephx_inc(auth_inc);
      }
      if (f) {
	kr.encode_formatted("auth", f.get(), rdata);
      } else {
	kr.encode_plaintext(rdata);
      }
      if (exists) {
	goto done;
      }
    } else if (prefix == "auth clear-pending") {
      if (entity_auth.pending_key.empty()) {
	err = 0;
	goto done;
      }
      KeyServerData::Incremental auth_inc;
      auth_inc.op = KeyServerData::AUTH_INC_ADD;
      auth_inc.name = entity;
      auth_inc.auth = entity_auth;
      auth_inc.auth.pending_key.clear();
      push_cephx_inc(auth_inc);
    } else if (prefix == "auth commit-pending") {
      if (entity_auth.pending_key.empty()) {
	err = 0;
	ss << "no pending key";
	goto done;
      }
      KeyServerData::Incremental auth_inc;
      auth_inc.op = KeyServerData::AUTH_INC_ADD;
      auth_inc.name = entity;
      auth_inc.auth = entity_auth;
      auth_inc.auth.key = auth_inc.auth.pending_key;
      auth_inc.auth.pending_key.clear();
      push_cephx_inc(auth_inc);
    }
    wait_for_finished_proposal(op, new Monitor::C_Command(mon, op, 0, rs, rdata,
					      get_last_committed() + 1));
    return true;
  } else if ((prefix == "auth get-or-create-key" ||
	      prefix == "auth get-or-create") &&
	     !entity_name.empty()) {
    // auth get-or-create <name> [mon osdcapa osd osdcapb ...]

    if (!valid_caps(caps_vec, &ss)) {
      err = -EINVAL;
      goto done;
    }

    // Parse the list of caps into a map
    std::map<std::string, bufferlist> wanted_caps;
    for (vector<string>::const_iterator it = caps_vec.begin();
	 it != caps_vec.end() && (it + 1) != caps_vec.end();
	 it += 2) {
      const std::string &sys = *it;
      bufferlist cap;
      encode(*(it+1), cap);
      wanted_caps[sys] = cap;
    }

    // do we have it?
    EntityAuth entity_auth;
    if (mon.key_server.get_auth(entity, entity_auth)) {
      for (const auto &sys_cap : wanted_caps) {
	if (entity_auth.caps.count(sys_cap.first) == 0 ||
	    !entity_auth.caps[sys_cap.first].contents_equal(sys_cap.second)) {
	  ss << "key for " << entity << " exists but cap " << sys_cap.first
            << " does not match";
	  err = -EINVAL;
	  goto done;
	}
      }

      if (prefix == "auth get-or-create-key") {
        if (f) {
          entity_auth.key.encode_formatted("auth", f.get(), rdata);
        } else {
          ds << entity_auth.key;
        }
      } else {
	KeyRing kr;
	kr.add(entity, entity_auth.key, entity_auth.pending_key);
        if (f) {
          kr.set_caps(entity, entity_auth.caps);
          kr.encode_formatted("auth", f.get(), rdata);
        } else {
          kr.encode_plaintext(rdata);
        }
      }
      err = 0;
      goto done;
    }

    // ...or are we about to?
    for (vector<Incremental>::iterator p = pending_auth.begin();
	 p != pending_auth.end();
	 ++p) {
      if (p->inc_type == AUTH_DATA) {
	KeyServerData::Incremental auth_inc;
	auto q = p->auth_data.cbegin();
	decode(auth_inc, q);
	if (auth_inc.op == KeyServerData::AUTH_INC_ADD &&
	    auth_inc.name == entity) {
	  wait_for_finished_proposal(op, new Monitor::C_Command(mon, op, 0, rs,
						get_last_committed() + 1));
	  return true;
	}
      }
    }

    // create it
    KeyServerData::Incremental auth_inc;
    auth_inc.op = KeyServerData::AUTH_INC_ADD;
    auth_inc.name = entity;
    auth_inc.auth.key.create(g_ceph_context, CEPH_CRYPTO_AES);
    auth_inc.auth.caps = wanted_caps;

    push_cephx_inc(auth_inc);

    if (prefix == "auth get-or-create-key") {
      if (f) {
        auth_inc.auth.key.encode_formatted("auth", f.get(), rdata);
      } else {
        ds << auth_inc.auth.key;
      }
    } else {
      KeyRing kr;
      kr.add(entity, auth_inc.auth.key);
      if (f) {
        kr.set_caps(entity, wanted_caps);
        kr.encode_formatted("auth", f.get(), rdata);
      } else {
        kr.encode_plaintext(rdata);
      }
    }

    rdata.append(ds);
    getline(ss, rs);
    wait_for_finished_proposal(op, new Monitor::C_Command(mon, op, 0, rs, rdata,
					      get_last_committed() + 1));
    return true;
  } else if (prefix == "fs authorize") {
    string filesystem;
    cmd_getval(cmdmap, "filesystem", filesystem);
    string mon_cap_string = "allow r";
    string mds_cap_string, osd_cap_string;
    string osd_cap_wanted = "r";

    std::shared_ptr<const Filesystem> fs;
    if (filesystem != "*" && filesystem != "all") {
      fs = mon.mdsmon()->get_fsmap().get_filesystem(filesystem);
      if (fs == nullptr) {
	ss << "filesystem " << filesystem << " does not exist.";
	err = -EINVAL;
	goto done;
      } else {
	mon_cap_string += " fsname=" + std::string(fs->mds_map.get_fs_name());
      }
    }

    for (auto it = caps_vec.begin();
	 it != caps_vec.end() && (it + 1) != caps_vec.end();
	 it += 2) {
      const string &path = *it;
      const string &cap = *(it+1);
      bool root_squash = false;
      if ((it + 2) != caps_vec.end() && *(it+2) == "root_squash") {
	root_squash = true;
	++it;
      }

      if (cap.compare(0, 2, "rw") == 0)
	osd_cap_wanted = "rw";

      char last='\0';
      for (size_t i = 2; i < cap.size(); ++i) {
	char c = cap.at(i);
	if (last >= c) {
	  ss << "Permission flags (except 'rw') must be specified in alphabetical order.";
	  err = -EINVAL;
	  goto done;
	}
	switch (c) {
	case 'p':
	  break;
	case 's':
	  break;
	default:
	  ss << "Unknown permission flag '" << c << "'.";
	  err = -EINVAL;
	  goto done;
	}
      }

      mds_cap_string += mds_cap_string.empty() ? "" : ", ";
      mds_cap_string += "allow " + cap;

      if (filesystem != "*" && filesystem != "all" && fs != nullptr) {
	mds_cap_string += " fsname=" + std::string(fs->mds_map.get_fs_name());
      }

      if (path != "/") {
	mds_cap_string += " path=" + path;
      }

      if (root_squash) {
	mds_cap_string += " root_squash";
      }
    }

    osd_cap_string += osd_cap_string.empty() ? "" : ", ";
    osd_cap_string += "allow " + osd_cap_wanted
      + " tag " + pg_pool_t::APPLICATION_NAME_CEPHFS
      + " data=" + filesystem;

    std::map<string, bufferlist> wanted_caps = {
      { "mon", _encode_cap(mon_cap_string) },
      { "osd", _encode_cap(osd_cap_string) },
      { "mds", _encode_cap(mds_cap_string) }
    };

    if (!valid_caps("mon", mon_cap_string, &ss) ||
        !valid_caps("osd", osd_cap_string, &ss) ||
	!valid_caps("mds", mds_cap_string, &ss)) {
      err = -EINVAL;
      goto done;
    }

    EntityAuth entity_auth;
    if (mon.key_server.get_auth(entity, entity_auth)) {
      for (const auto &sys_cap : wanted_caps) {
	if (entity_auth.caps.count(sys_cap.first) == 0 ||
	    !entity_auth.caps[sys_cap.first].contents_equal(sys_cap.second)) {
	  ss << entity << " already has fs capabilities that differ from "
	     << "those supplied. To generate a new auth key for " << entity
	     << ", first remove " << entity << " from configuration files, "
	     << "execute 'ceph auth rm " << entity << "', then execute this "
	     << "command again.";
	  err = -EINVAL;
	  goto done;
	}
      }

      KeyRing kr;
      kr.add(entity, entity_auth.key);
      if (f) {
	kr.set_caps(entity, entity_auth.caps);
	kr.encode_formatted("auth", f.get(), rdata);
      } else {
	kr.encode_plaintext(rdata);
      }
      err = 0;
      goto done;
    }

    KeyServerData::Incremental auth_inc;
    auth_inc.op = KeyServerData::AUTH_INC_ADD;
    auth_inc.name = entity;
    auth_inc.auth.key.create(g_ceph_context, CEPH_CRYPTO_AES);
    auth_inc.auth.caps = wanted_caps;

    push_cephx_inc(auth_inc);
    KeyRing kr;
    kr.add(entity, auth_inc.auth.key);
    if (f) {
      kr.set_caps(entity, wanted_caps);
      kr.encode_formatted("auth", f.get(), rdata);
    } else {
      kr.encode_plaintext(rdata);
    }

    rdata.append(ds);
    getline(ss, rs);
    wait_for_finished_proposal(op, new Monitor::C_Command(mon, op, 0, rs, rdata,
						  get_last_committed() + 1));
    return true;
  } else if (prefix == "auth caps" && !entity_name.empty()) {
    KeyServerData::Incremental auth_inc;
    auth_inc.name = entity;
    if (!mon.key_server.get_auth(auth_inc.name, auth_inc.auth)) {
      ss << "couldn't find entry " << auth_inc.name;
      err = -ENOENT;
      goto done;
    }

    if (!valid_caps(caps_vec, &ss)) {
      err = -EINVAL;
      goto done;
    }

    map<string,bufferlist> newcaps;
    for (vector<string>::iterator it = caps_vec.begin();
	 it != caps_vec.end(); it += 2)
      encode(*(it+1), newcaps[*it]);

    auth_inc.op = KeyServerData::AUTH_INC_ADD;
    auth_inc.auth.caps = newcaps;
    push_cephx_inc(auth_inc);

    ss << "updated caps for " << auth_inc.name;
    getline(ss, rs);
    wait_for_finished_proposal(op, new Monitor::C_Command(mon, op, 0, rs,
					      get_last_committed() + 1));
    return true;
  } else if ((prefix == "auth del" || prefix == "auth rm") &&
             !entity_name.empty()) {
    KeyServerData::Incremental auth_inc;
    auth_inc.name = entity;
    if (!mon.key_server.contains(auth_inc.name)) {
      err = 0;
      goto done;
    }
    auth_inc.op = KeyServerData::AUTH_INC_DEL;
    push_cephx_inc(auth_inc);

    wait_for_finished_proposal(op, new Monitor::C_Command(mon, op, 0, rs,
					      get_last_committed() + 1));
    return true;
  }
done:
  rdata.append(ds);
  getline(ss, rs, '\0');
  mon.reply_command(op, err, rs, rdata, get_last_committed());
  return false;
}

bool AuthMonitor::prepare_global_id(MonOpRequestRef op)
{
  dout(10) << "AuthMonitor::prepare_global_id" << dendl;
  increase_max_global_id();
  return true;
}

bool AuthMonitor::prepare_used_pending_keys(MonOpRequestRef op)
{
  dout(10) << __func__ << " " << op << dendl;
  auto m = op->get_req<MMonUsedPendingKeys>();
  process_used_pending_keys(m->used_pending_keys);
  return true;
}

bool AuthMonitor::_upgrade_format_to_dumpling()
{
  dout(1) << __func__ << " upgrading from format 0 to 1" << dendl;
  ceph_assert(format_version == 0);

  bool changed = false;
  map<EntityName, EntityAuth>::iterator p;
  for (p = mon.key_server.secrets_begin();
       p != mon.key_server.secrets_end();
       ++p) {
    // grab mon caps, if any
    string mon_caps;
    if (p->second.caps.count("mon") == 0)
      continue;
    try {
      auto it = p->second.caps["mon"].cbegin();
      decode(mon_caps, it);
    }
    catch (const ceph::buffer::error&) {
      dout(10) << __func__ << " unable to parse mon cap for "
	       << p->first << dendl;
      continue;
    }

    string n = p->first.to_str();
    string new_caps;

    // set daemon profiles
    if ((p->first.is_osd() || p->first.is_mds()) &&
        mon_caps == "allow rwx") {
      new_caps = string("allow profile ") + std::string(p->first.get_type_name());
    }

    // update bootstrap keys
    if (n == "client.bootstrap-osd") {
      new_caps = "allow profile bootstrap-osd";
    }
    if (n == "client.bootstrap-mds") {
      new_caps = "allow profile bootstrap-mds";
    }

    if (new_caps.length() > 0) {
      dout(5) << __func__ << " updating " << p->first << " mon cap from "
	      << mon_caps << " to " << new_caps << dendl;

      bufferlist bl;
      encode(new_caps, bl);

      KeyServerData::Incremental auth_inc;
      auth_inc.name = p->first;
      auth_inc.auth = p->second;
      auth_inc.auth.caps["mon"] = bl;
      auth_inc.op = KeyServerData::AUTH_INC_ADD;
      push_cephx_inc(auth_inc);
      changed = true;
    }
  }
  return changed;
}

bool AuthMonitor::_upgrade_format_to_luminous()
{
  dout(1) << __func__ << " upgrading from format 1 to 2" << dendl;
  ceph_assert(format_version == 1);

  bool changed = false;
  map<EntityName, EntityAuth>::iterator p;
  for (p = mon.key_server.secrets_begin();
       p != mon.key_server.secrets_end();
       ++p) {
    string n = p->first.to_str();

    string newcap;
    if (n == "client.admin") {
      // admin gets it all
      newcap = "allow *";
    } else if (n.find("osd.") == 0 ||
	       n.find("mds.") == 0 ||
	       n.find("mon.") == 0) {
      // daemons follow their profile
      string type = n.substr(0, 3);
      newcap = "allow profile " + type;
    } else if (p->second.caps.count("mon")) {
      // if there are any mon caps, give them 'r' mgr caps
      newcap = "allow r";
    }

    if (newcap.length() > 0) {
      dout(5) << " giving " << n << " mgr '" << newcap << "'" << dendl;
      bufferlist bl;
      encode(newcap, bl);

      EntityAuth auth = p->second;
      auth.caps["mgr"] = bl;

      add_entity(p->first, auth);
      changed = true;
    }

    if (n.find("mgr.") == 0 &&
	p->second.caps.count("mon")) {
      // the kraken ceph-mgr@.service set the mon cap to 'allow *'.
      auto blp = p->second.caps["mon"].cbegin();
      string oldcaps;
      decode(oldcaps, blp);
      if (oldcaps == "allow *") {
	dout(5) << " fixing " << n << " mon cap to 'allow profile mgr'"
		<< dendl;
	bufferlist bl;
	encode("allow profile mgr", bl);

	EntityAuth auth = p->second;
	auth.caps["mon"] = bl;
	add_entity(p->first, p->second);
	changed = true;
      }
    }
  }

  // add bootstrap key if it does not already exist
  // (might have already been get-or-create'd by
  //  ceph-create-keys)
  EntityName bootstrap_mgr_name;
  int r = bootstrap_mgr_name.from_str("client.bootstrap-mgr");
  ceph_assert(r);
  if (!mon.key_server.contains(bootstrap_mgr_name)) {

    EntityName name = bootstrap_mgr_name;
    EntityAuth auth;
    encode("allow profile bootstrap-mgr", auth.caps["mon"]);
    auth.key.create(g_ceph_context, CEPH_CRYPTO_AES);
    add_entity(name, auth);
    changed = true;
  }
  return changed;
}

bool AuthMonitor::_upgrade_format_to_mimic()
{
  dout(1) << __func__ << " upgrading from format 2 to 3" << dendl;
  ceph_assert(format_version == 2);

  list<pair<EntityName,EntityAuth> > auth_lst;
  _generate_bootstrap_keys(&auth_lst);

  bool changed = false;
  for (auto &p : auth_lst) {
    if (mon.key_server.contains(p.first)) {
      continue;
    }
    int err = add_entity(p.first, p.second);
    ceph_assert(err == 0);
    changed = true;
  }

  return changed;
}

void AuthMonitor::upgrade_format()
{
  constexpr unsigned int FORMAT_NONE = 0;
  constexpr unsigned int FORMAT_DUMPLING = 1;
  constexpr unsigned int FORMAT_LUMINOUS = 2;
  constexpr unsigned int FORMAT_MIMIC = 3;

  // when upgrading from the current format to a new format, ensure that
  // the new format doesn't break the older format. I.e., if a given format N
  // changes or adds something, ensure that when upgrading from N-1 to N+1, we
  // still observe the changes for format N if those have not been superseded
  // by N+1.

  unsigned int current = FORMAT_MIMIC;
  if (!mon.get_quorum_mon_features().contains_all(
	ceph::features::mon::FEATURE_LUMINOUS)) {
    // pre-luminous quorum
    current = FORMAT_DUMPLING;
  } else if (!mon.get_quorum_mon_features().contains_all(
	ceph::features::mon::FEATURE_MIMIC)) {
    // pre-mimic quorum
    current = FORMAT_LUMINOUS;
  }
  if (format_version >= current) {
    dout(20) << __func__ << " format " << format_version
	     << " is current" << dendl;
    return;
  }

  // perform a rolling upgrade of the new format, if necessary.
  // i.e., if we are moving from format NONE to MIMIC, we will first upgrade
  // to DUMPLING, then to LUMINOUS, and finally to MIMIC, in several different
  // proposals.

  bool changed = false;
  if (format_version == FORMAT_NONE) {
    changed = _upgrade_format_to_dumpling();

  } else if (format_version == FORMAT_DUMPLING) {
    changed = _upgrade_format_to_luminous();
  } else if (format_version == FORMAT_LUMINOUS) {
    changed = _upgrade_format_to_mimic();
  }

  if (changed) {
    // note new format
    dout(10) << __func__ << " proposing update from format " << format_version
	     << " -> " << current << dendl;
    format_version = current;
    propose_pending();
  }
}

void AuthMonitor::dump_info(Formatter *f)
{
  /*** WARNING: do not include any privileged information here! ***/
  f->open_object_section("auth");
  f->dump_unsigned("first_committed", get_first_committed());
  f->dump_unsigned("last_committed", get_last_committed());
  f->dump_unsigned("num_secrets", mon.key_server.get_num_secrets());
  f->close_section();
}
