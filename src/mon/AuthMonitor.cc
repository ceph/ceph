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

#include "messages/MMonCommand.h"
#include "messages/MAuth.h"
#include "messages/MAuthReply.h"
#include "messages/MMonGlobalID.h"

#include "include/str_list.h"
#include "common/Timer.h"

#include "auth/AuthServiceHandler.h"
#include "auth/KeyRing.h"

#include "osd/osd_types.h"

#include "common/config.h"
#include "include/assert.h"
#include "common/cmdparse.h"
#include "include/str_list.h"

#define dout_subsys ceph_subsys_mon
#undef dout_prefix
#define dout_prefix _prefix(_dout, mon, get_version())
static ostream& _prefix(std::ostream *_dout, Monitor *mon, version_t v) {
  return *_dout << "mon." << mon->name << "@" << mon->rank
		<< "(" << mon->get_state_name()
		<< ").auth v" << v << " ";
}

ostream& operator<<(ostream& out, AuthMonitor& pm)
{
  return out << "auth";
}

void AuthMonitor::check_rotate()
{
  KeyServerData::Incremental rot_inc;
  rot_inc.op = KeyServerData::AUTH_INC_SET_ROTATING;
  if (!mon->key_server.updated_rotating(rot_inc.rotating_bl, last_rotating_ver))
    return;
  dout(10) << "AuthMonitor::tick() updated rotating, now calling propose_pending" << dendl;
  push_cephx_inc(rot_inc);
  propose_pending();
}

/*
 Tick function to update the map based on performance every N seconds
*/

void AuthMonitor::tick() 
{
  if (!is_active()) return;

  update_from_paxos();
  dout(10) << *this << dendl;

  if (!mon->is_leader()) return; 

  check_rotate();
}

void AuthMonitor::on_active()
{
  dout(10) << "AuthMonitor::on_active()" << dendl;

  if (!mon->is_leader())
    return;
  mon->key_server.start_server();
/*
  check_rotate();
*/
}

void AuthMonitor::create_initial()
{
  dout(10) << "create_initial -- creating initial map" << dendl;

  KeyRing keyring;
  bufferlist bl;
  int ret = mon->store->get("mkfs", "keyring", bl);
  assert(ret == 0);
  bufferlist::iterator p = bl.begin();
  ::decode(keyring, p);

  import_keyring(keyring);

  max_global_id = MIN_GLOBAL_ID;

  Incremental inc;
  inc.inc_type = GLOBAL_ID;
  inc.max_global_id = max_global_id;
  pending_auth.push_back(inc);
}

void AuthMonitor::update_from_paxos()
{
  dout(10) << __func__ << dendl;
  version_t version = get_version();
  version_t keys_ver = mon->key_server.get_ver();
  if (version == keys_ver)
    return;
  assert(version >= keys_ver);

  version_t latest_full = get_version_latest_full();

  dout(10) << __func__ << " version " << version << " keys ver " << keys_ver
           << " latest " << latest_full << dendl;

  if ((latest_full > 0) && (latest_full > keys_ver)) {
    bufferlist latest_bl;
    int err = get_version_full(latest_full, latest_bl);
    assert(err == 0);
    assert(latest_bl.length() != 0);
    dout(7) << __func__ << " loading summary e " << latest_full << dendl;
    dout(7) << __func__ << " latest length " << latest_bl.length() << dendl;
    bufferlist::iterator p = latest_bl.begin();
    __u8 struct_v;
    ::decode(struct_v, p);
    ::decode(max_global_id, p);
    ::decode(mon->key_server, p);
    mon->key_server.set_ver(latest_full);
    keys_ver = latest_full;
  }

  dout(10) << __func__ << " key server version " << mon->key_server.get_ver() << dendl;

  // walk through incrementals
  while (version > keys_ver) {
    bufferlist bl;
    int ret = get_version(keys_ver+1, bl);
    assert(ret == 0);
    assert(bl.length());

    // reset if we are moving to initial state.  we will normally have
    // keys in here temporarily for bootstrapping that we need to
    // clear out.
    if (keys_ver == 0) 
      mon->key_server.clear_secrets();

    dout(20) << __func__ << " walking through version " << (keys_ver+1)
             << " len " << bl.length() << dendl;

    bufferlist::iterator p = bl.begin();
    __u8 v;
    ::decode(v, p);
    while (!p.end()) {
      Incremental inc;
      ::decode(inc, p);
      switch (inc.inc_type) {
      case GLOBAL_ID:
	max_global_id = inc.max_global_id;
	break;

      case AUTH_DATA:
        {
          KeyServerData::Incremental auth_inc;
          bufferlist::iterator iter = inc.auth_data.begin();
          ::decode(auth_inc, iter);
          mon->key_server.apply_data_incremental(auth_inc);
          break;
        }
      }
    }

    keys_ver++;
    mon->key_server.set_ver(keys_ver);

    if (keys_ver == 1) {
      MonitorDBStore::Transaction t;
      t.erase("mkfs", "keyring");
      mon->store->apply_transaction(t);
    }
  }

  if (last_allocated_id == 0)
    last_allocated_id = max_global_id;

  dout(10) << "update_from_paxos() last_allocated_id=" << last_allocated_id
	   << " max_global_id=" << max_global_id << dendl;
 
  /*
  bufferlist bl;
  __u8 v = 1;
  ::encode(v, bl);
  ::encode(max_global_id, bl);
  Mutex::Locker l(mon->key_server.get_lock());
  ::encode(mon->key_server, bl);
  paxos->stash_latest(version, bl);
  */
}

void AuthMonitor::increase_max_global_id()
{
  assert(mon->is_leader());

  max_global_id += g_conf->mon_globalid_prealloc;
  dout(10) << "increasing max_global_id to " << max_global_id << dendl;
  Incremental inc;
  inc.inc_type = GLOBAL_ID;
  inc.max_global_id = max_global_id;
  pending_auth.push_back(inc);
}

bool AuthMonitor::should_propose(double& delay)
{
  return (!pending_auth.empty());
}

void AuthMonitor::create_pending()
{
  pending_auth.clear();
  dout(10) << "create_pending v " << (get_version() + 1) << dendl;
}

void AuthMonitor::encode_pending(MonitorDBStore::Transaction *t)
{
  dout(10) << __func__ << " v " << (get_version() + 1) << dendl;

  bufferlist bl;

  __u8 v = 1;
  ::encode(v, bl);
  vector<Incremental>::iterator p;
  for (p = pending_auth.begin(); p != pending_auth.end(); ++p)
    p->encode(bl, mon->get_quorum_features());

  version_t version = get_version() + 1;
  put_version(t, version, bl);
  put_last_committed(t, version);
}

void AuthMonitor::encode_full(MonitorDBStore::Transaction *t)
{
  version_t version = mon->key_server.get_ver();
  dout(10) << __func__ << " auth v " << version << dendl;
  assert(get_version() == version);

  bufferlist full_bl;
  Mutex::Locker l(mon->key_server.get_lock());
  if (mon->key_server.has_secrets()) {
    dout(20) << __func__ << " key server has secrets!" << dendl;
    __u8 v = 1;
    ::encode(v, full_bl);
    ::encode(max_global_id, full_bl);
    ::encode(mon->key_server, full_bl);

    put_version_full(t, version, full_bl);
    put_version_latest_full(t, version);
  } else {
    dout(20) << __func__
	     << " key server has no secrets; do not put them in tx" << dendl;
  }
}

void AuthMonitor::update_trim()
{
  unsigned max = g_conf->paxos_max_join_drift * 2;
  version_t version = get_version();
  if (mon->is_leader() && (version > max))
    set_trim_to(version - max);
}

bool AuthMonitor::preprocess_query(PaxosServiceMessage *m)
{
  dout(10) << "preprocess_query " << *m << " from " << m->get_orig_source_inst() << dendl;
  switch (m->get_type()) {
  case MSG_MON_COMMAND:
    return preprocess_command((MMonCommand*)m);

  case CEPH_MSG_AUTH:
    return prep_auth((MAuth *)m, false);

  case MSG_MON_GLOBAL_ID:
    return false;

  default:
    assert(0);
    m->put();
    return true;
  }
}

bool AuthMonitor::prepare_update(PaxosServiceMessage *m)
{
  dout(10) << "prepare_update " << *m << " from " << m->get_orig_source_inst() << dendl;
  switch (m->get_type()) {
  case MSG_MON_COMMAND:
    return prepare_command((MMonCommand*)m);
  case MSG_MON_GLOBAL_ID:
    return prepare_global_id((MMonGlobalID*)m); 
  case CEPH_MSG_AUTH:
    return prep_auth((MAuth *)m, true);
  default:
    assert(0);
    m->put();
    return false;
  }
}

uint64_t AuthMonitor::assign_global_id(MAuth *m, bool should_increase_max)
{
  int total_mon = mon->monmap->size();
  dout(10) << "AuthMonitor::assign_global_id m=" << *m << " mon=" << mon->rank << "/" << total_mon
	   << " last_allocated=" << last_allocated_id << " max_global_id=" <<  max_global_id << dendl;

  uint64_t next_global_id = last_allocated_id + 1;
  int remainder = next_global_id % total_mon;
  if (remainder)
    remainder = total_mon - remainder;
  next_global_id += remainder + mon->rank;
  dout(10) << "next_global_id should be " << next_global_id << dendl;

  // if we can't bump the max, bail out now on an out-of-bounds gid
  if (next_global_id > max_global_id &&
      (!mon->is_leader() || !should_increase_max)) {
    return 0;
  }

  // can we return a gid?
  bool return_next = (next_global_id <= max_global_id);

  // bump the max?
  while (mon->is_leader() &&
	 next_global_id >= max_global_id - g_conf->mon_globalid_prealloc / 2) {
    increase_max_global_id();
  }

  if (return_next) {
    last_allocated_id = next_global_id;
    return next_global_id;
  } else {
    return 0;
  }
}


bool AuthMonitor::prep_auth(MAuth *m, bool paxos_writable)
{
  dout(10) << "prep_auth() blob_size=" << m->get_auth_payload().length() << dendl;

  MonSession *s = (MonSession *)m->get_connection()->get_priv();
  if (!s) {
    dout(10) << "no session, dropping" << dendl;
    m->put();
    return true;
  }

  int ret = 0;
  AuthCapsInfo caps_info;
  MAuthReply *reply;
  bufferlist response_bl;
  bufferlist::iterator indata = m->auth_payload.begin();
  __u32 proto = m->protocol;
  bool start = false;
  EntityName entity_name;

  // set up handler?
  if (m->protocol == 0 && !s->auth_handler) {
    set<__u32> supported;
    
    try {
      __u8 struct_v = 1;
      ::decode(struct_v, indata);
      ::decode(supported, indata);
      ::decode(entity_name, indata);
      ::decode(s->global_id, indata);
    } catch (const buffer::error &e) {
      dout(10) << "failed to decode initial auth message" << dendl;
      ret = -EINVAL;
      goto reply;
    }

    // do we require cephx signatures?

    if (!m->get_connection()->has_feature(CEPH_FEATURE_MSG_AUTH)) {
      if (entity_name.get_type() == CEPH_ENTITY_TYPE_MON ||
	  entity_name.get_type() == CEPH_ENTITY_TYPE_OSD ||
	  entity_name.get_type() == CEPH_ENTITY_TYPE_MDS) {
	if (g_conf->cephx_cluster_require_signatures ||
	    g_conf->cephx_require_signatures) {
	  dout(1) << m->get_source_inst() << " supports cephx but not signatures and 'cephx [cluster] require signatures = true'; disallowing cephx" << dendl;
	  supported.erase(CEPH_AUTH_CEPHX);
	}
      } else {
	if (g_conf->cephx_service_require_signatures ||
	    g_conf->cephx_require_signatures) {
	  dout(1) << m->get_source_inst() << " supports cephx but not signatures and 'cephx [service] require signatures = true'; disallowing cephx" << dendl;
	  supported.erase(CEPH_AUTH_CEPHX);
	}
      }
    }

    int type;
    if (entity_name.get_type() == CEPH_ENTITY_TYPE_MON ||
	entity_name.get_type() == CEPH_ENTITY_TYPE_OSD ||
	entity_name.get_type() == CEPH_ENTITY_TYPE_MDS)
      type = mon->auth_cluster_required.pick(supported);
    else
      type = mon->auth_service_required.pick(supported);

    s->auth_handler = get_auth_service_handler(type, g_ceph_context, &mon->key_server);
    if (!s->auth_handler) {
      dout(1) << "client did not provide supported auth type" << dendl;
      ret = -ENOTSUP;
      goto reply;
    }
    start = true;
  } else if (!s->auth_handler) {
      dout(10) << "protocol specified but no s->auth_handler" << dendl;
      ret = -EINVAL;
      goto reply;
  }

  /* assign a new global_id? we assume this should only happen on the first
     request. If a client tries to send it later, it'll screw up its auth
     session */
  if (!s->global_id) {
    s->global_id = assign_global_id(m, paxos_writable);
    if (!s->global_id) {

      delete s->auth_handler;
      s->auth_handler = NULL;

      if (mon->is_leader() && paxos_writable) {
        dout(10) << "increasing global id, waitlisting message" << dendl;
        wait_for_active(new C_RetryMessage(this, m));
        goto done;
      }

      s->put();

      if (!mon->is_leader()) {
	dout(10) << "not the leader, requesting more ids from leader" << dendl;
	int leader = mon->get_leader();
	MMonGlobalID *req = new MMonGlobalID();
	req->old_max_id = max_global_id;
	mon->messenger->send_message(req, mon->monmap->get_inst(leader));
	wait_for_finished_proposal(new C_RetryMessage(this, m));
	return true;
      }

      assert(!paxos_writable);
      return false;
    }
  }

  try {
    uint64_t auid = 0;
    if (start) {
      // new session

      // always send the latest monmap.
      if (m->monmap_epoch < mon->monmap->get_epoch())
	mon->send_latest_monmap(m->get_connection());

      proto = s->auth_handler->start_session(entity_name, indata, response_bl, caps_info);
      ret = 0;
      if (caps_info.allow_all)
	s->caps.set_allow_all();
    } else {
      // request
      ret = s->auth_handler->handle_request(indata, response_bl, s->global_id, caps_info, &auid);
    }
    if (ret == -EIO) {
      wait_for_active(new C_RetryMessage(this,m));
      //paxos->wait_for_active(new C_RetryMessage(this, m));
      goto done;
    }
    if (caps_info.caps.length()) {
      bufferlist::iterator p = caps_info.caps.begin();
      string str;
      try {
	::decode(str, p);
      } catch (const buffer::error &err) {
	derr << "corrupt cap data for " << entity_name << " in auth db" << dendl;
	str.clear();
      }
      s->caps.parse(str, NULL);
      s->auid = auid;
    }
  } catch (const buffer::error &err) {
    ret = -EINVAL;
    dout(0) << "caught error when trying to handle auth request, probably malformed request" << dendl;
  }

reply:
  reply = new MAuthReply(proto, &response_bl, ret, s->global_id);
  mon->send_reply(m, reply);
  m->put();
done:
  s->put();
  return true;
}

void AuthMonitor::auth_usage(stringstream& ss)
{
  ss << "error: usage:" << std::endl;
  ss << "              auth (add | del | get-or-create | get-or-create-key | caps) <name> <--in-file=filename>" << std::endl;
  ss << "              auth (export | get | get-key | print-key) <name>" << std::endl;
  ss << "              auth list" << std::endl;
}

bool AuthMonitor::preprocess_command(MMonCommand *m)
{
  int r = -1;
  bufferlist rdata;
  stringstream ss, ds;

  map<string, cmd_vartype> cmdmap;
  if (!cmdmap_from_json(m->cmd, &cmdmap, ss)) {
    // ss has reason for failure
    string rs = ss.str();
    mon->reply_command(m, -EINVAL, rs, rdata, get_version());
    return true;
  }

  string prefix;
  cmd_getval(g_ceph_context, cmdmap, "prefix", prefix);
  if (prefix == "auth add" ||
      prefix == "auth del" ||
      prefix == "auth get-or-create" ||
      prefix == "auth get-or-create-key" ||
      prefix == "auth import" ||
      prefix == "auth caps") {
    return false;
  }

  MonSession *session = m->get_session();
  if (!session ||
      (!mon->_allowed_command(session, cmdmap))) {
    mon->reply_command(m, -EACCES, "access denied", rdata, get_version());
    return true;
  }

  // entity might not be supplied, but if it is, it should be valid
  string entity_name;
  cmd_getval(g_ceph_context, cmdmap, "entity", entity_name);
  EntityName entity;
  if (!entity_name.empty() && !entity.from_str(entity_name)) {
    ss << "invalid entity_auth " << entity_name;
    r = -EINVAL;
  }

  if (prefix == "auth export") {
    KeyRing keyring;
    export_keyring(keyring);
    if (!entity_name.empty()) {
      EntityAuth eauth;
      if (keyring.get_auth(entity, eauth)) {
	KeyRing kr;
	kr.add(entity, eauth);
	kr.encode_plaintext(rdata);
	ds << "export " << eauth;
	r = 0;
      } else {
	ss << "no key for " << eauth;
	r = -ENOENT;
      }
    } else {
      keyring.encode_plaintext(rdata);
      ss << "exported master keyring";
      r = 0;
    }
  } else if (prefix == "auth get" && !entity_name.empty()) {
    KeyRing keyring;
    EntityAuth entity_auth;
    if(!mon->key_server.get_auth(entity, entity_auth)) {
      ss << "failed to find " << entity_name << " in keyring";
      r = -ENOENT;
    } else {
      keyring.add(entity, entity_auth);
      keyring.encode_plaintext(rdata);
      ss << "exported keyring for " << entity_name;
      r = 0;
    }
  } else if (prefix == "auth print-key" ||
	     prefix == "auth print_key" ||
	     prefix == "auth get-key") {
    EntityAuth auth;
    if (!mon->key_server.get_auth(entity, auth)) {
      ss << "don't have " << entity;
      r = -ENOENT;
      goto done;
    }
    ds << auth.key;
    r = 0;
  } else if (prefix == "auth list") {
    mon->key_server.list_secrets(ss, ds);
    r = 0;
    goto done;
  } else {
    auth_usage(ss);
    r = -EINVAL;
  }

 done:
  rdata.append(ds);
  string rs;
  getline(ss, rs, '\0');
  mon->reply_command(m, r, rs, rdata, get_version());
  return true;
}

void AuthMonitor::export_keyring(KeyRing& keyring)
{
  mon->key_server.export_keyring(keyring);
}

void AuthMonitor::import_keyring(KeyRing& keyring)
{
  for (map<EntityName, EntityAuth>::iterator p = keyring.get_keys().begin();
       p != keyring.get_keys().end();
       ++p) {
    KeyServerData::Incremental auth_inc;
    auth_inc.name = p->first;
    auth_inc.auth = p->second; 
    auth_inc.op = KeyServerData::AUTH_INC_ADD;
    dout(10) << " importing " << auth_inc.name << dendl;
    dout(30) << "    " << auth_inc.auth << dendl;
    push_cephx_inc(auth_inc);
  }
}

bool AuthMonitor::prepare_command(MMonCommand *m)
{
  stringstream ss, ds;
  bufferlist rdata;
  string rs;
  int err = -EINVAL;

  map<string, cmd_vartype> cmdmap;
  if (!cmdmap_from_json(m->cmd, &cmdmap, ss)) {
    // ss has reason for failure
    string rs = ss.str();
    mon->reply_command(m, -EINVAL, rs, rdata, get_version());
    return true;
  }

  string prefix;
  vector<string>caps_vec;
  string entity_name;
  EntityName entity;

  cmd_getval(g_ceph_context, cmdmap, "prefix", prefix);

  MonSession *session = m->get_session();
  if (!session ||
      (!mon->_allowed_command(session, cmdmap))) {
    mon->reply_command(m, -EACCES, "access denied", rdata, get_version());
    return true;
  }

  cmd_getval(g_ceph_context, cmdmap, "caps", caps_vec);
  if ((caps_vec.size() % 2) != 0) {
    ss << "bad capabilities request; odd number of arguments";
    err = -EINVAL;
    goto done;
  }

  cmd_getval(g_ceph_context, cmdmap, "entity", entity_name);
  if (!entity_name.empty() && !entity.from_str(entity_name)) {
    ss << "bad entity name";
    err = -EINVAL;
    goto done;
  }

  if (prefix == "auth import") {
    bufferlist bl = m->get_data();
    bufferlist::iterator iter = bl.begin();
    KeyRing keyring;
    try {
      ::decode(keyring, iter);
    } catch (const buffer::error &ex) {
      ss << "error decoding keyring";
      rs = err;
      goto done;
    }
    import_keyring(keyring);
    ss << "imported keyring";
    getline(ss, rs);
    err = 0;
    wait_for_finished_proposal(new Monitor::C_Command(mon, m, 0, rs, get_version()));
    //paxos->wait_for_commit(new Monitor::C_Command(mon, m, 0, rs, get_version()));
    return true;
  } else if (prefix == "auth add") {
    KeyServerData::Incremental auth_inc;
    auth_inc.name = entity;
    bufferlist bl = m->get_data();
    dout(10) << "AuthMonitor::prepare_command bl.length()=" << bl.length() << dendl;
    if (bl.length()) {
      bufferlist::iterator iter = bl.begin();
      KeyRing keyring;
      try {
	::decode(keyring, iter);
      } catch (const buffer::error &ex) {
	ss << "error decoding keyring";
	err = -EINVAL;
	goto done;
      }
      if (!keyring.get_auth(auth_inc.name, auth_inc.auth)) {
	ss << "key for " << auth_inc.name << " not found in provided keyring";
	err = -EINVAL;
	goto done;
      }
    } else {
      // generate a new random key
      dout(10) << "AuthMonitor::prepare_command generating random key for " << auth_inc.name << dendl;
      auth_inc.auth.key.create(g_ceph_context, CEPH_CRYPTO_AES);
    }

    auth_inc.op = KeyServerData::AUTH_INC_ADD;

    for (vector<string>::iterator it = caps_vec.begin();
	 it != caps_vec.end(); it += 2)
      ::encode(*(it+1), auth_inc.auth.caps[*it]);

    dout(10) << " importing " << auth_inc.name << dendl;
    dout(30) << "    " << auth_inc.auth << dendl;
    push_cephx_inc(auth_inc);

    ss << "added key for " << auth_inc.name;
    getline(ss, rs);
    wait_for_finished_proposal(new Monitor::C_Command(mon, m, 0, rs, get_version()));
    //paxos->wait_for_commit(new Monitor::C_Command(mon, m, 0, rs, get_version()));
    return true;
  } else if ((prefix == "auth get-or-create-key" ||
	     prefix == "auth get-or-create") &&
	     !entity_name.empty()) {
    // auth get-or-create <name> [mon osdcapa osd osdcapb ...]

    // do we have it?
    EntityAuth entity_auth;
    if (mon->key_server.get_auth(entity, entity_auth)) {
      for (vector<string>::iterator it = caps_vec.begin();
	   it != caps_vec.end(); it += 2) {
	string sys = *it;
	bufferlist cap;
	::encode(*(it+1), cap);
	if (entity_auth.caps.count(sys) == 0 ||
	    !entity_auth.caps[sys].contents_equal(cap)) {
	  ss << "key for " << entity << " exists but cap " << sys << " does not match";
	  err = -EINVAL;
	  goto done;
	}
      }

      if (prefix == "auth get-or-create-key") {
	ds << entity_auth.key;
      } else {
	KeyRing kr;
	kr.add(entity, entity_auth.key);
	kr.encode_plaintext(rdata);
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
	bufferlist::iterator q = p->auth_data.begin();
	::decode(auth_inc, q);
	if (auth_inc.op == KeyServerData::AUTH_INC_ADD &&
	    auth_inc.name == entity) {
	  wait_for_finished_proposal(new Monitor::C_Command(mon, m, 0, rs, get_version()));
	  //paxos->wait_for_commit(new C_RetryMessage(this, m));
	  return true;
	}
      }
    }

    // create it
    KeyServerData::Incremental auth_inc;
    auth_inc.op = KeyServerData::AUTH_INC_ADD;
    auth_inc.name = entity;
    auth_inc.auth.key.create(g_ceph_context, CEPH_CRYPTO_AES);
    for (vector<string>::iterator it = caps_vec.begin();
	 it != caps_vec.end(); it += 2)
      ::encode(*(it+1), auth_inc.auth.caps[*it]);

    push_cephx_inc(auth_inc);

    if (prefix == "auth get-or-create-key") {
      ds << auth_inc.auth.key;
    } else {
      KeyRing kr;
      kr.add(entity, auth_inc.auth.key);
      kr.encode_plaintext(rdata);
    }

    rdata.append(ds);
    getline(ss, rs);
    wait_for_finished_proposal(new Monitor::C_Command(mon, m, 0, rs, rdata, get_version()));
    //paxos->wait_for_commit(new Monitor::C_Command(mon, m, 0, rs, get_version()));
    return true;
  } else if (prefix == "auth caps" && !entity_name.empty()) {
    KeyServerData::Incremental auth_inc;
    auth_inc.name = entity;
    if (!mon->key_server.get_auth(auth_inc.name, auth_inc.auth)) {
      ss << "couldn't find entry " << auth_inc.name;
      err = -ENOENT;
      goto done;
    }

    map<string,bufferlist> newcaps;
    for (vector<string>::iterator it = caps_vec.begin();
	 it != caps_vec.end(); it += 2)
      ::encode(*(it+1), newcaps[*it]);

    auth_inc.op = KeyServerData::AUTH_INC_ADD;
    auth_inc.auth.caps = newcaps;
    push_cephx_inc(auth_inc);

    ss << "updated caps for " << auth_inc.name;
    getline(ss, rs);
    wait_for_finished_proposal(new Monitor::C_Command(mon, m, 0, rs, get_version()));
    //paxos->wait_for_commit(new Monitor::C_Command(mon, m, 0, rs, get_version()));
    return true;
  } else if (prefix == "auth del" && !entity_name.empty()) {
    KeyServerData::Incremental auth_inc;
    auth_inc.name = entity;
    if (!mon->key_server.contains(auth_inc.name)) {
      ss << "couldn't find entry " << entity;
      err = -ENOENT;
      goto done;
    }
    auth_inc.op = KeyServerData::AUTH_INC_DEL;
    push_cephx_inc(auth_inc);

    ss << "updated";
    getline(ss, rs);
    wait_for_finished_proposal(new Monitor::C_Command(mon, m, 0, rs, get_version()));
    //paxos->wait_for_commit(new Monitor::C_Command(mon, m, 0, rs, get_version()));
    return true;
  }

done:
  rdata.append(ds);
  getline(ss, rs, '\0');
  mon->reply_command(m, err, rs, rdata, get_version());
  return false;
}

bool AuthMonitor::prepare_global_id(MMonGlobalID *m)
{
  dout(10) << "AuthMonitor::prepare_global_id" << dendl;
  increase_max_global_id();

  m->put();
  return true;
}
