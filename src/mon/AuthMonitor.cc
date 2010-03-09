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


#include "AuthMonitor.h"
#include "Monitor.h"
#include "MonitorStore.h"

#include "messages/MMonCommand.h"
#include "messages/MAuth.h"
#include "messages/MAuthReply.h"
#include "messages/MMonGlobalID.h"

#include "include/str_list.h"
#include "common/Timer.h"

#include "auth/AuthServiceHandler.h"
#include "auth/KeyRing.h"

#include "osd/osd_types.h"
#include "osd/PG.h"  // yuck

#include "config.h"
#include <sstream>

#define DOUT_SUBSYS mon
#undef dout_prefix
#define dout_prefix _prefix(mon, paxos->get_version())
static ostream& _prefix(Monitor *mon, version_t v) {
  return *_dout << dbeginl
		<< "mon" << mon->whoami
		<< (mon->is_starting() ? (const char*)"(starting)":(mon->is_leader() ? (const char*)"(leader)":(mon->is_peon() ? (const char*)"(peon)":(const char*)"(?\?)")))
		<< ".auth v" << v << " ";
}

ostream& operator<<(ostream& out, AuthMonitor& pm)
{
  std::stringstream ss;
 
  return out << "auth";
}

void AuthMonitor::check_rotate()
{
  KeyServerData::Incremental rot_inc;
  rot_inc.op = KeyServerData::AUTH_INC_SET_ROTATING;
  if (!mon->key_server.updated_rotating(rot_inc.rotating_bl, last_rotating_ver))
    return;
  dout(0) << "AuthMonitor::tick() updated rotating, now calling propose_pending" << dendl;
  push_cephx_inc(rot_inc);
  propose_pending();
}

/*
 Tick function to update the map based on performance every N seconds
*/

void AuthMonitor::tick() 
{
  if (!paxos->is_active()) return;

  update_from_paxos();
  dout(10) << *this << dendl;

  if (!mon->is_leader()) return; 

  check_rotate();
}

void AuthMonitor::on_active()
{
  dout(0) << "AuthMonitor::on_active()" << dendl;

  if (!mon->is_leader())
    return;
  mon->key_server.start_server();
/*
  check_rotate();
*/
}

void AuthMonitor::create_initial(bufferlist& bl)
{
  dout(0) << "create_initial -- creating initial map" << dendl;
  if (g_conf.keyring) {
    dout(0) << "reading initial keyring " << dendl;
    bufferlist bl;

    string k = g_conf.keyring;
    list<string> ls;
    get_str_list(k, ls);
    int r = -1;
    for (list<string>::iterator p = ls.begin(); p != ls.end(); p++)
      if ((r = bl.read_file(g_conf.keyring)) >= 0)
	break;
    if (r >= 0) {
      KeyRing keyring;
      bool read_ok = false;
      try {
        bufferlist::iterator iter = bl.begin();
        ::decode(keyring, iter);
        read_ok = true;
      } catch (buffer::error *err) {
        cerr << "error reading file " << g_conf.keyring << std::endl;
      }
      if (read_ok)
	import_keyring(keyring);
    }
  }

  max_global_id = MIN_GLOBAL_ID;

  Incremental inc;
  inc.inc_type = GLOBAL_ID;
  inc.max_global_id = max_global_id;
  pending_auth.push_back(inc);
}

bool AuthMonitor::update_from_paxos()
{
  dout(0) << "AuthMonitor::update_from_paxos()" << dendl;
  version_t paxosv = paxos->get_version();
  version_t keys_ver = mon->key_server.get_ver();
  if (paxosv == keys_ver) return true;
  assert(paxosv >= keys_ver);

  if (keys_ver == 0 && paxosv > 1) {
    // startup: just load latest full map
    bufferlist latest;
    version_t v = paxos->get_latest(latest);
    if (v) {
      dout(7) << "update_from_paxos startup: loading summary e" << v << dendl;
      bufferlist::iterator p = latest.begin();
      __u8 v;
      ::decode(v, p);
      ::decode(max_global_id, p);
      ::decode(mon->key_server, p);
    }
  } 

  // walk through incrementals
  while (paxosv > keys_ver) {
    bufferlist bl;
    bool success = paxos->read(keys_ver+1, bl);
    assert(success);

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
  }

  if (last_allocated_id == 0)
    last_allocated_id = max_global_id;

  dout(10) << "update_from_paxos() last_allocated_id=" << last_allocated_id
	   << " max_global_id=" << max_global_id << dendl;

  bufferlist bl;
  __u8 v = 1;
  ::encode(v, bl);
  ::encode(max_global_id, bl);
  Mutex::Locker l(mon->key_server.get_lock());
  ::encode(mon->key_server, bl);
  paxos->stash_latest(paxosv, bl);

  return true;
}

void AuthMonitor::increase_max_global_id()
{
  assert(mon->is_leader());

  max_global_id += g_conf.mon_globalid_prealloc;
  dout(0) << "increasing max_global_id to " << max_global_id << dendl;
  Incremental inc;
  inc.inc_type = GLOBAL_ID;
  inc.max_global_id = max_global_id;
  pending_auth.push_back(inc);
}

bool AuthMonitor::should_propose(double& delay)
{
  return (pending_auth.size() > 0);
}

void AuthMonitor::init()
{
  version_t paxosv = paxos->get_version();
  version_t keys_ver = mon->key_server.get_ver();

  dout(0) << "AuthMonitor::init() paxosv=" << paxosv << dendl;

  if (paxosv == keys_ver) return;
  assert(paxosv >= keys_ver);

  if (keys_ver == 0 && paxosv > 1) {
    // startup: just load latest full map
    bufferlist latest;
    version_t v = paxos->get_latest(latest);
    if (v) {
      dout(0) << "AuthMonitor::init() startup: loading summary e" << v << dendl;
      bufferlist::iterator p = latest.begin();
      __u8 v;
      ::decode(v, p);
      ::decode(max_global_id, p);
      ::decode(mon->key_server, p);
    }
  }

  last_allocated_id = max_global_id;

  /* should only happen on the first time */
  update_from_paxos();
}

void AuthMonitor::create_pending()
{
  pending_auth.clear();
  dout(10) << "create_pending v " << (paxos->get_version() + 1) << dendl;
}

void AuthMonitor::encode_pending(bufferlist &bl)
{
  dout(10) << "encode_pending v " << (paxos->get_version() + 1) << dendl;
  __u8 v = 1;
  ::encode(v, bl);
  for (vector<Incremental>::iterator p = pending_auth.begin();
       p != pending_auth.end();
       p++)
    p->encode(bl);
}

bool AuthMonitor::preprocess_query(PaxosServiceMessage *m)
{
  dout(0) << "preprocess_query " << *m << " from " << m->get_orig_source_inst() << dendl;
  switch (m->get_type()) {
  case MSG_MON_COMMAND:
    return preprocess_command((MMonCommand*)m);

  case CEPH_MSG_AUTH:
    return prep_auth((MAuth *)m, false);

  case MSG_MON_GLOBAL_ID:
    return false;

  default:
    assert(0);
    delete m;
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
    delete m;
    return false;
  }
}

void AuthMonitor::committed()
{

}

void AuthMonitor::election_finished()
{
  dout(10) << "AuthMonitor::election_starting" << dendl;
  last_allocated_id = 0;
}

uint64_t AuthMonitor::assign_global_id(MAuth *m, bool should_increase_max)
{
  int total_mon = mon->monmap->size();
  dout(10) << "AuthMonitor::assign_global_id m=" << *m << " mon=" << mon->whoami << "/" << total_mon
	   << " last_allocated=" << last_allocated_id << " max_global_id=" <<  max_global_id << dendl;

  uint64_t next_global_id = last_allocated_id + 1;

  if (next_global_id < max_global_id) {
    int remainder = next_global_id % total_mon;
    if (remainder)
      remainder = total_mon - remainder;
    next_global_id += remainder + mon->whoami;
    dout(10) << "next_global_id should be " << next_global_id << dendl;
  }

  while (next_global_id >= max_global_id) {
    if (!mon->is_leader()) {
      dout(10) << "not the leader, requesting more ids from leader" << dendl;
      int leader = mon->get_leader();
      MMonGlobalID *req = new MMonGlobalID();
      req->old_max_id = max_global_id;
      mon->messenger->send_message(req, mon->monmap->get_inst(leader));
      paxos->wait_for_commit(new C_RetryMessage(this, m));
      return 0;
    } else {
      if (!should_increase_max)
        return 0;

      dout(10) << "increasing max_global_id" << dendl;
      increase_max_global_id();
    }
  }

  last_allocated_id = next_global_id;

  return next_global_id;
}


bool AuthMonitor::prep_auth(MAuth *m, bool paxos_writable)
{
  dout(0) << "prep_auth() blob_size=" << m->get_auth_payload().length() << dendl;

  Session *s = (Session *)m->get_connection()->get_priv();
  if (!s) {
    dout(10) << "no session, dropping" << dendl;
    delete m;
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
    } catch (buffer::error *e) {
      dout(0) << "failed to decode initial auth message" << dendl;
      ret = -EINVAL;
      goto reply;
    }

    s->auth_handler = get_auth_service_handler(&mon->key_server, supported);
    if (!s->auth_handler) {
      ret = -ENOTSUP;
      goto reply;
    }
    start = true;
  } else if (!s->auth_handler) {
      dout(0) << "protocol specified but no s->auth_handler" << dendl;
      ret = -EINVAL;
      goto reply;
  }

  /* assign a new global_id? we assume this should only happen on the first
     request. If a client tries to send it later, it'll screw up its auth
     session */
  if (!s->global_id) {
    s->global_id = assign_global_id(m, paxos_writable);
    if (!s->global_id) {
      s->put();

      delete s->auth_handler;
      s->auth_handler = NULL;

      if (mon->is_leader())
	return false;
      return true;
    }
  }

  try {
    __u64 auid;
    if (start) {
      // new session
      proto = s->auth_handler->start_session(entity_name, indata, response_bl, caps_info);
      ret = 0;
      s->caps.set_allow_all(caps_info.allow_all);
    } else {
      // request
      ret = s->auth_handler->handle_request(indata, response_bl, s->global_id, caps_info, &auid);
    }
    if (ret == -EIO) {
      paxos->wait_for_active(new C_RetryMessage(this, m));
      goto done;
    }
    if (caps_info.caps.length()) {
      bufferlist::iterator iter = caps_info.caps.begin();
      s->caps.parse(iter);
      s->caps.set_auid(auid);
    }
  } catch (buffer::error *err) {
    ret = -EINVAL;
    dout(0) << "caught error when trying to handle auth request, probably malformed request" << dendl;
  }

reply:
  reply = new MAuthReply(proto, &response_bl, ret, s->global_id);
  mon->messenger->send_message(reply, m->get_orig_source_inst());
done:
  s->put();
  return true;
}

void AuthMonitor::auth_usage(stringstream& ss)
{
  ss << "error: usage:" << std::endl;
  ss << "              auth <add | del> <name> <--in-file=filename>" << std::endl;
  ss << "              auth <list>" << std::endl;
}

bool AuthMonitor::preprocess_command(MMonCommand *m)
{
  int r = -1;
  bufferlist rdata;
  stringstream ss;

  if (m->cmd.size() > 1) {
    if (m->cmd[1] == "add" ||
        m->cmd[1] == "del" ||
        m->cmd[1] == "list") {
      return false;
    }
    else if (m->cmd[1] == "export") {
      KeyRing keyring;
      export_keyring(keyring);
      if (m->cmd.size() > 2) {
	EntityName ename;
	EntityAuth eauth;
	if (ename.from_str(m->cmd[2])) {
	  if (keyring.get_auth(ename, eauth)) {
	    KeyRing kr;
	    kr.add(ename, eauth);
	    ::encode(kr, rdata);
	    ss << "export " << eauth;
	    r = 0;
	  } else {
	    ss << "no key for " << eauth;
	    r = -ENOENT;
	  }
	} else {
	  ss << "invalid entity_auth " << m->cmd[2];
	  r = -EINVAL;
	}
      } else {
	::encode(keyring, rdata);
	ss << "exported master keyring";
	r = 0;
      }
    } else {
      auth_usage(ss);
      r = -EINVAL;
    }
  } else {
    auth_usage(ss);
    r = -EINVAL;
  }

  string rs;
  getline(ss, rs, '\0');
  mon->reply_command(m, r, rs, rdata, paxos->get_version());
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
       p++) {
    KeyServerData::Incremental auth_inc;
    auth_inc.name = p->first;
    auth_inc.auth = p->second; 
    auth_inc.op = KeyServerData::AUTH_INC_ADD;
    dout(10) << " importing " << auth_inc.name << " " << auth_inc.auth << dendl;
    push_cephx_inc(auth_inc);
  }
}

bool AuthMonitor::prepare_command(MMonCommand *m)
{
  stringstream ss;
  string rs;
  int err = -EINVAL;

  // nothing here yet
  if (m->cmd.size() > 1) {
    if (m->cmd[1] == "import") {
      bufferlist bl = m->get_data();
      bufferlist::iterator iter = bl.begin();
      KeyRing keyring;
      try {
        ::decode(keyring, iter);
      } catch (buffer::error *err) {
        ss << "error decoding keyring";
        rs = -EINVAL;
        goto done;
      }
      import_keyring(keyring);
      ss << "imported keyring";
      getline(ss, rs);
      paxos->wait_for_commit(new Monitor::C_Command(mon, m, 0, rs, paxos->get_version()));
      return true;
    }
    else if (m->cmd[1] == "add" && m->cmd.size() >= 2) {
      KeyServerData::Incremental auth_inc;
      if (m->cmd.size() >= 3) {
        if (!auth_inc.name.from_str(m->cmd[2])) {
          ss << "bad entity name";
          rs = -EINVAL;
          goto done;
        }
      }

      bufferlist bl = m->get_data();
      dout(0) << "AuthMonitor::prepare_command bl.length()=" << bl.length() << dendl;
      bufferlist::iterator iter = bl.begin();
      KeyRing keyring;
      try {
        ::decode(keyring, iter);
      } catch (buffer::error *err) {
        ss << "error decoding keyring";
        rs = -EINVAL;
        goto done;
      }

      if (!keyring.get_auth(auth_inc.name, auth_inc.auth)) {
	ss << "key for " << auth_inc.name << " not found in provided keyring";
        rs = -EINVAL;
        goto done;
      }
      auth_inc.op = KeyServerData::AUTH_INC_ADD;
      dout(10) << " importing " << auth_inc.name << " " << auth_inc.auth << dendl;
      push_cephx_inc(auth_inc);

      ss << "added key for " << auth_inc.name;
      getline(ss, rs);
      paxos->wait_for_commit(new Monitor::C_Command(mon, m, 0, rs, paxos->get_version()));
      return true;
    }
    else if (m->cmd[1] == "del" && m->cmd.size() >= 3) {
      string name = m->cmd[2];
      KeyServerData::Incremental auth_inc;
      auth_inc.name.from_str(name);
      if (!mon->key_server.contains(auth_inc.name)) {
        ss << "couldn't find entry " << name;
        rs = -ENOENT;
        goto done;
      }
      auth_inc.op = KeyServerData::AUTH_INC_DEL;
      push_cephx_inc(auth_inc);

      ss << "updated";
      getline(ss, rs);
      paxos->wait_for_commit(new Monitor::C_Command(mon, m, 0, rs, paxos->get_version()));
      return true;
    }
    else if (m->cmd[1] == "list") {
      mon->key_server.list_secrets(ss);
      err = 0;
      goto done;
    }
    else {
      auth_usage(ss);
    }
  } else {
    auth_usage(ss);
  }

done:
  getline(ss, rs, '\0');
  mon->reply_command(m, err, rs, paxos->get_version());
  return false;
}

bool AuthMonitor::prepare_global_id(MMonGlobalID *m)
{
  dout(10) << "AuthMonitor::prepare_global_id" << dendl;
  increase_max_global_id();

  return true;
}
