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

#include "include/str_list.h"
#include "common/Timer.h"

#include "auth/AuthServiceHandler.h"

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
  KeyServerData::Incremental inc;
  inc.op = KeyServerData::AUTH_INC_SET_ROTATING;
  if (!mon->key_server.updated_rotating(inc.rotating_bl, last_rotating_ver))
    return;
  dout(0) << "AuthMonitor::tick() updated rotating, now calling propose_pending" << dendl;
  pending_auth.push_back(inc);
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
  mon->key_server.start_server(true);
/*
  check_rotate();
*/
}

void AuthMonitor::create_initial(bufferlist& bl)
{
  dout(0) << "create_initial -- creating initial map" << dendl;
  if (g_conf.keys_file) {
    map<string, EntityAuth> keys_map;
    dout(0) << "reading initial keys file " << dendl;
    bufferlist bl;

    string k = g_conf.keys_file;
    list<string> ls;
    get_str_list(k, ls);
    int r;
    for (list<string>::iterator p = ls.begin(); p != ls.end(); p++)
      if ((r = bl.read_file(g_conf.keys_file)) >= 0)
	break;
    if (r >= 0) {
      bool read_ok = false;
      try {
        bufferlist::iterator iter = bl.begin();
        ::decode(keys_map, iter);
        read_ok = true;
      } catch (buffer::error *err) {
        cerr << "error reading file " << g_conf.keys_file << std::endl;
      }
      if (read_ok) {
        map<string, EntityAuth>::iterator iter = keys_map.begin();
        for (; iter != keys_map.end(); ++iter) {
          string n = iter->first;
          if (!n.empty()) {
            dout(0) << "read key for entry: " << n << dendl;
	    KeyServerData::Incremental inc;
            if (!inc.name.from_str(n)) {
              dout(0) << "bad entity name " << n << dendl;
              continue;
            }
            inc.auth = iter->second; 
            inc.op = KeyServerData::AUTH_INC_ADD;
            pending_auth.push_back(inc);
          }
        }
      }

    }
  }

  KeyServerData::Incremental inc;
  inc.op = KeyServerData::AUTH_INC_NOP;
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
      ::decode(mon->key_server, p);
    }
  } 

  // walk through incrementals
  while (paxosv > keys_ver) {
    bufferlist bl;
    bool success = paxos->read(keys_ver+1, bl);
    assert(success);

    bufferlist::iterator p = bl.begin();
    while (!p.end()) {
      KeyServerData::Incremental inc;
      ::decode(inc, p);
      mon->key_server.apply_data_incremental(inc);
    }

    keys_ver++;
    mon->key_server.set_ver(keys_ver);
  }

  bufferlist bl;
  Mutex::Locker l(mon->key_server.get_lock());
  ::encode(mon->key_server, bl);
  paxos->stash_latest(paxosv, bl);

  return true;
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
      ::decode(mon->key_server, p);
    }
  }

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
  for (vector<KeyServerData::Incremental>::iterator p = pending_auth.begin();
       p != pending_auth.end();
       p++)
    p->encode(bl);
}

bool AuthMonitor::preprocess_query(PaxosServiceMessage *m)
{
  dout(10) << "preprocess_query " << *m << " from " << m->get_orig_source_inst() << dendl;
  switch (m->get_type()) {
  case MSG_MON_COMMAND:
    return preprocess_command((MMonCommand*)m);

  case CEPH_MSG_AUTH:
    return preprocess_auth((MAuth *)m);

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
  default:
    assert(0);
    delete m;
    return false;
  }
}

void AuthMonitor::committed()
{

}


bool AuthMonitor::preprocess_auth(MAuth *m)
{
  dout(0) << "preprocess_auth() blob_size=" << m->get_auth_payload().length() << dendl;
  int ret = 0;
  AuthCapsInfo caps_info;
  MAuthReply *reply;

  Session *s = (Session *)m->get_connection()->get_priv();

  bufferlist response_bl;
  bufferlist::iterator indata = m->auth_payload.begin();

  __u32 proto = m->protocol;

  // set up handler?
  if (m->protocol == 0) {
    EntityName entity_name;

    if (!s->auth_handler) {
      set<__u32> supported;
      
      try {
	::decode(supported, indata);
        ::decode(entity_name, indata);
      } catch (buffer::error *e) {
	dout(0) << "failed to decode initial auth message" << dendl;
	ret = -EINVAL;
      }
      
      if (!ret) {
	s->auth_handler = get_auth_service_handler(&mon->key_server, supported);
	if (!s->auth_handler)
	  ret = -EPERM;
	else {
	  proto = s->auth_handler->start_session(entity_name, indata, response_bl);
	}
      }
    } else {
      ret = -EINVAL;  // can only select protocol once per connection
    }
  } else if (s->auth_handler) {
    // handle the request
    try {
      ret = s->auth_handler->handle_request(indata, response_bl, caps_info);
      dout(20) << "handled request for entity_name=" << s->auth_handler->get_entity_name().to_str() << dendl;
      s->caps.set_allow_all(caps_info.allow_all);
      if (caps_info.caps.length()) {
        bufferlist::iterator iter = caps_info.caps.begin();
        s->caps.parse(iter);
      }
    } catch (buffer::error *err) {
      ret = -EINVAL;
      dout(0) << "caught error when trying to handle auth request, probably malformed request" << dendl;
    }
    if (ret == -EIO) {
      paxos->wait_for_active(new C_RetryMessage(this, m));
      goto done;
    }
  } else {
    ret = -EINVAL;  // no protocol selected?
  }
  reply = new MAuthReply(proto, &response_bl, ret);
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
  }

  auth_usage(ss);
  r = -EINVAL;

  string rs;
  getline(ss, rs, '\0');
  mon->reply_command(m, r, rs, rdata, paxos->get_version());
  return true;
}


bool AuthMonitor::prepare_command(MMonCommand *m)
{
  stringstream ss;
  string rs;
  int err = -EINVAL;

  // nothing here yet
  if (m->cmd.size() > 1) {
    if (m->cmd[1] == "add" && m->cmd.size() >= 2) {
      string entity_name;
      KeyServerData::Incremental inc;
      if (m->cmd.size() >= 3) {
        entity_name = m->cmd[2];
        if (!inc.name.from_str(entity_name)) {
          ss << "bad entity name";
          rs = -EINVAL;
          goto done;
        }
      }

      bufferlist bl = m->get_data();
      dout(0) << "AuthMonitor::prepare_command bl.length()=" << bl.length() << dendl;
      bufferlist::iterator iter = bl.begin();
      map<string, EntityAuth> crypto_map;
      try {
        ::decode(crypto_map, iter);
      } catch (buffer::error *err) {
        ss << "error decoding key";
        rs = -EINVAL;
        goto done;
      }

      for (map<string, EntityAuth>::iterator miter = crypto_map.begin(); miter != crypto_map.end(); ++miter) {
	KeyServerData::Incremental inc;
        dout(0) << "storing auth for " << entity_name  << dendl;
        if (miter->first.empty()) {
          if (entity_name.empty())
            continue;
          inc.name.from_str(entity_name);
        } else {
          string s = miter->first;
          inc.name.from_str(s);
        }
        inc.auth = miter->second;
        inc.op = KeyServerData::AUTH_INC_ADD;
        pending_auth.push_back(inc);
      }
      ss << "updated";
      getline(ss, rs);
      paxos->wait_for_commit(new Monitor::C_Command(mon, m, 0, rs, paxos->get_version()));
      return true;
    } else if (m->cmd[1] == "del" && m->cmd.size() >= 3) {
      string name = m->cmd[2];
      KeyServerData::Incremental inc;
      inc.name.from_str(name);
      if (!mon->key_server.contains(inc.name)) {
        ss << "couldn't find entry " << name;
        rs = -ENOENT;
        goto done;
      }
      inc.op = KeyServerData::AUTH_INC_DEL;
      pending_auth.push_back(inc);

      ss << "updated";
      getline(ss, rs);
      paxos->wait_for_commit(new Monitor::C_Command(mon, m, 0, rs, paxos->get_version()));
      return true;
    } else if (m->cmd[1] == "list") {
      mon->key_server.list_secrets(ss);
      err = 0;
      goto done;
    } else {
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

