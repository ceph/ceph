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
#include "messages/MAuthMon.h"
#include "messages/MAuthMonAck.h"
#include "messages/MAuthRotating.h"

#include "include/AuthLibrary.h"
#include "common/Timer.h"

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
  AuthLibEntry entry;
  if (!mon->keys_server.updated_rotating(entry.rotating_bl, last_rotating_ver))
    return;
  dout(0) << "AuthMonitor::tick() updated rotating, now calling propose_pending" << dendl;

  AuthLibIncremental inc;
  inc.op = AUTH_INC_SET_ROTATING;
  entry.rotating = true;
  ::encode(entry, inc.info);
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
  mon->keys_server.start_server(true);

  check_rotate();
}

void AuthMonitor::create_initial(bufferlist& bl)
{
  dout(0) << "create_initial -- creating initial map" << dendl;
  AuthLibIncremental inc;

  if (g_conf.keys_file) {
    map<string, CryptoKey> keys_map;
    dout(0) << "reading initial keys file " << dendl;
    bufferlist bl;
    int r = bl.read_file(g_conf.keys_file);
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
        map<string, CryptoKey>::iterator iter = keys_map.begin();
        for (; iter != keys_map.end(); ++iter) {
          string n = iter->first;
          if (!n.empty()) {
            dout(0) << "read key for entry: " << n << dendl;
            AuthLibEntry entry;
            if (!entry.name.from_str(n)) {
              dout(0) << "bad entity name " << n << dendl;
              continue;
            }
            entry.secret = iter->second; 
            ::encode(entry, inc.info);
            inc.op = AUTH_INC_ADD;
            pending_auth.push_back(inc);
          }
        }
      }

    }
  }

  AuthLibEntry l;
  ::encode(l, inc.info);
  inc.op = AUTH_INC_NOP;
  pending_auth.push_back(inc);
}

bool AuthMonitor::store_entry(AuthLibEntry& entry)
{
  string entry_str;

  entry.name.to_str(entry_str);

  bufferlist bl;
  ::encode(entry, bl);
  mon->store->put_bl_ss(bl, "auth_lib", entry_str.c_str());
  dout(0) << "adding name=" << entry_str << dendl;

  return true;
}


bool AuthMonitor::update_from_paxos()
{
  dout(0) << "AuthMonitor::update_from_paxos()" << dendl;
  version_t paxosv = paxos->get_version();
  version_t keys_ver = mon->keys_server.get_ver();
  if (paxosv == keys_ver) return true;
  assert(paxosv >= keys_ver);

  if (keys_ver == 0 && paxosv > 1) {
    // startup: just load latest full map
    bufferlist latest;
    version_t v = paxos->get_latest(latest);
    if (v) {
      dout(7) << "update_from_paxos startup: loading summary e" << v << dendl;
      bufferlist::iterator p = latest.begin();
      ::decode(mon->keys_server, p);
    }
  } 

  // walk through incrementals
  while (paxosv > keys_ver) {
    bufferlist bl;
    bool success = paxos->read(keys_ver+1, bl);
    assert(success);

    bufferlist::iterator p = bl.begin();
    AuthLibIncremental inc;
    while (!p.end()) {
      ::decode(inc, p);
      AuthLibEntry entry;
      inc.decode_entry(entry);
      switch (inc.op) {
      case AUTH_INC_ADD:
        if (!entry.rotating) {
          mon->keys_server.add_secret(entry.name, entry.secret);
        } else {
          derr(0) << "got AUTH_INC_ADD with entry.rotating" << dendl;
        }
        break;
      case AUTH_INC_DEL:
        mon->keys_server.remove_secret(entry.name);
        break;
      case AUTH_INC_SET_ROTATING:
        {
          dout(0) << "AuthMonitor::update_from_paxos: decode_rotating" << dendl;
          mon->keys_server.decode_rotating(entry.rotating_bl);
        }
        break;
      case AUTH_INC_NOP:
        break;
      default:
        assert(0);
      }
    }
    keys_ver++;
    mon->keys_server.set_ver(keys_ver);
  }

  bufferlist bl;
  Mutex::Locker l(mon->keys_server.get_lock());
  ::encode(mon->keys_server, bl);
  paxos->stash_latest(paxosv, bl);

  return true;
}

void AuthMonitor::create_pending()
{
  pending_auth.clear();
  dout(10) << "create_pending v " << (paxos->get_version() + 1) << dendl;
}

void AuthMonitor::encode_pending(bufferlist &bl)
{
  dout(10) << "encode_pending v " << (paxos->get_version() + 1) << dendl;
  for (vector<AuthLibIncremental>::iterator p = pending_auth.begin();
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

  case MSG_AUTH_ROTATING:
    return preprocess_auth_rotating((MAuthRotating *)m);

  case MSG_AUTHMON:
    return preprocess_auth_mon((MAuthMon*)m);

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
  case MSG_AUTHMON:
    return prepare_auth_mon((MAuthMon*)m);
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

  Session *s = (Session *)m->get_connection()->get_priv();
  s->put();

  bufferlist response_bl;
  bufferlist::iterator indata = m->auth_payload.begin();

  CephXPremable pre;
  ::decode(pre, indata);
  dout(0) << "CephXPremable id=" << pre.trans_id << dendl;
  ::encode(pre, response_bl);

  // set up handler?
  if (!s->auth_handler) {
    set<__u32> supported;
   
    try {
      ::decode(supported, indata);
    } catch (buffer::error *e) {
      dout(0) << "failed to decode message auth message" << dendl;
      ret = -EINVAL;
    }

    if (!ret) {
      s->auth_handler = auth_mgr.get_auth_handler(supported);
      if (!s->auth_handler)
	ret = -EPERM;
    }
  }

  if (s->auth_handler && !ret) {
    // handle the request
    try {
      ret = s->auth_handler->handle_request(indata, response_bl);
    } catch (buffer::error *err) {
      ret = -EINVAL;
      dout(0) << "caught error when trying to handle auth request, probably malformed request" << dendl;
    }
  }
  MAuthReply *reply = new MAuthReply(&response_bl, ret);
  mon->messenger->send_message(reply, m->get_orig_source_inst());
  return true;
}


bool AuthMonitor::preprocess_auth_rotating(MAuthRotating *m)
{
  dout(10) << "handle_request " << *m << " from " << m->get_orig_source() << dendl;
  MAuthRotating *reply = new MAuthRotating();

  if (!reply)
    return true;

  if (mon->keys_server.get_rotating_encrypted(m->entity_name, reply->response_bl)) {
    reply->status = 0;
  } else {
    reply->status = -EPERM;
  }
  
  mon->messenger->send_message(reply, m->get_orig_source_inst());
  delete m;
  return true;
}


// auth mon

bool AuthMonitor::preprocess_auth_mon(MAuthMon *m)
{
  dout(10) << "preprocess_auth_mon " << *m << " from " << m->get_orig_source() << dendl;
  
  int num_new = 0;
  for (deque<AuthLibEntry>::iterator p = m->info.begin();
       p != m->info.end();
       p++) {
    if (!mon->keys_server.contains((*p).name))
      num_new++;
  }
  if (!num_new) {
    dout(10) << "  nothing new" << dendl;
    return true;
  }
  return false;
}

bool AuthMonitor::prepare_auth_mon(MAuthMon *m) 
{
  dout(10) << "prepare_auth " << *m << " from " << m->get_orig_source() << dendl;

  if (ceph_fsid_compare(&m->fsid, &mon->monmap->fsid)) {
    dout(0) << "handle_auth on fsid " << m->fsid << " != " << mon->monmap->fsid << dendl;
    delete m;
    return false;
  }
  for (deque<AuthLibEntry>::iterator p = m->info.begin();
       p != m->info.end(); p++) {
    dout(10) << " writing auth " << *p << dendl;
    AuthLibIncremental inc;
    ::encode(*p, inc.info);
    pending_auth.push_back(inc);
  }

  paxos->wait_for_commit(new C_Auth(this, m, m->get_orig_source_inst()));
  return true;
}

void AuthMonitor::_updated_auth(MAuthMon *m, entity_inst_t who)
{
  dout(7) << "_updated_auth for " << who << dendl;
  mon->messenger->send_message(new MAuthMonAck(m->fsid, m->last), who);
  delete m;
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
    if (m->cmd[1] == "add" && m->cmd.size() >= 3) {
      string entity_name = m->cmd[2];

      AuthLibEntry entry;
      if (!entry.name.from_str(entity_name)) {
        ss << "bad entity name";
        rs = -EINVAL;
        goto done;
      }

      bufferlist bl = m->get_data();
      dout(0) << "AuthMonitor::prepare_command bl.length()=" << bl.length() << dendl;
      bufferlist::iterator iter = bl.begin();
      try {
        ::decode(entry.secret, iter);
      } catch (buffer::error *err) {
        ss << "error decoding key";
        rs = -EINVAL;
        goto done;
      }

      if (entry.rotating) {
        ss << "can't apply a rotating key";
        rs = -EINVAL;
        goto done;
      }

      AuthLibIncremental inc;
      dout(0) << "storing auth for " << entity_name  << dendl;
      ::encode(entry, inc.info);
      inc.op = AUTH_INC_ADD;
      pending_auth.push_back(inc);
      ss << "updated";
      getline(ss, rs);
      paxos->wait_for_commit(new Monitor::C_Command(mon, m, 0, rs, paxos->get_version()));
      return true;
    } else if (m->cmd[1] == "del" && m->cmd.size() >= 3) {
      string name = m->cmd[2];
      AuthLibEntry entry;
      entry.name.from_str(name);
      if (!mon->keys_server.contains(entry.name)) {
        ss << "couldn't find entry " << name;
        rs = -ENOENT;
        goto done;
      }
      AuthLibIncremental inc;
      ::encode(entry, inc.info);
      inc.op = AUTH_INC_DEL;
      pending_auth.push_back(inc);

      ss << "updated";
      getline(ss, rs);
      paxos->wait_for_commit(new Monitor::C_Command(mon, m, 0, rs, paxos->get_version()));
      return true;
    } else if (m->cmd[1] == "list") {
      mon->keys_server.list_secrets(ss);
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

