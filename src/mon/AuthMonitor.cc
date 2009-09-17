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
#include "messages/MAuthMon.h"
#include "messages/MAuthMonAck.h"

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
		<< ".class v" << v << " ";
}

ostream& operator<<(ostream& out, AuthMonitor& pm)
{
  std::stringstream ss;
 
  return out << "auth";
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

}

void AuthMonitor::create_initial(bufferlist& bl)
{
  dout(0) << "create_initial -- creating initial map" << dendl;
  AuthLibEntry l;
  AuthLibIncremental inc;
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
  version_t paxosv = paxos->get_version();
  if (paxosv == list.version) return true;
  assert(paxosv >= list.version);

  bufferlist blog;

  if (list.version == 0 && paxosv > 1) {
    // startup: just load latest full map
    bufferlist latest;
    version_t v = paxos->get_latest(latest);
    if (v) {
      dout(7) << "update_from_paxos startup: loading summary e" << v << dendl;
      bufferlist::iterator p = latest.begin();
      ::decode(list, p);
    }
  } 

  // walk through incrementals
  while (paxosv > list.version) {
    bufferlist bl;
    bool success = paxos->read(list.version+1, bl);
    assert(success);

    bufferlist::iterator p = bl.begin();
    AuthLibIncremental inc;
    ::decode(inc, p);
    AuthLibEntry entry;
    inc.decode_entry(entry);
    switch (inc.op) {
    case AUTH_INC_ADD:
      list.add(entry);
      break;
    case AUTH_INC_DEL:
      list.remove(entry.name);
      break;
    case AUTH_INC_NOP:
      break;
    default:
      assert(0);
    }

    list.version++;
  }

  bufferlist bl;
  ::encode(list, bl);
  paxos->stash_latest(paxosv, bl);

  return true;
}

void AuthMonitor::create_pending()
{
  pending_auth.clear();
  pending_list = list;
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

  case MSG_AUTHMON:
    return preprocess_auth((MAuthMon*)m);

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
    return prepare_auth((MAuthMon*)m);
  default:
    assert(0);
    delete m;
    return false;
  }
}

void AuthMonitor::committed()
{

}

bool AuthMonitor::preprocess_auth(MAuthMon *m)
{
  dout(10) << "preprocess_auth " << *m << " from " << m->get_orig_source() << dendl;
  
  int num_new = 0;
  for (deque<AuthLibEntry>::iterator p = m->info.begin();
       p != m->info.end();
       p++) {
    if (!pending_list.contains((*p).name))
      num_new++;
  }
  if (!num_new) {
    dout(10) << "  nothing new" << dendl;
    return true;
  }
  return false;
}

bool AuthMonitor::prepare_auth(MAuthMon *m) 
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
    if (!pending_list.contains((*p).name)) {
      AuthLibIncremental inc;
      ::encode(*p, inc.info);
      pending_list.add(*p);
      pending_auth.push_back(inc);
    }
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
        m->cmd[1] == "activate" ||
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
      entry.name.from_str(entity_name);

      bufferlist bl = m->get_data();
      bufferlist::iterator iter = bl.begin();
      ::decode(entry.secret, iter);

      AuthLibIncremental inc;
      dout(0) << "storing auth for " << entity_name  << dendl;
      ::encode(entry, inc.info);
      inc.op = AUTH_INC_ADD;
      pending_list.add(entry);
      pending_auth.push_back(inc);
      ss << "updated";
      getline(ss, rs);
      paxos->wait_for_commit(new Monitor::C_Command(mon, m, 0, rs, paxos->get_version()));
      return true;
    } else if (m->cmd[1] == "del" && m->cmd.size() >= 5) {
      string name = m->cmd[2];
      AuthLibEntry entry;
      entry.name.from_str(name);
      map<EntityName, AuthLibEntry>::iterator iter = list.library_map.find(entry.name);
      if (iter == list.library_map.end()) {
        ss << "couldn't find entry " << name;
        rs = -ENOENT;
        goto done;
      }
      AuthLibIncremental inc;
      ::encode(entry, inc.info);
      inc.op = AUTH_INC_DEL;
      pending_list.add(entry);
      pending_auth.push_back(inc);

      ss << "updated";
      getline(ss, rs);
      paxos->wait_for_commit(new Monitor::C_Command(mon, m, 0, rs, paxos->get_version()));
      return true;
    } else if (m->cmd[1] == "list") {
      map<EntityName, AuthLibEntry>::iterator mapiter = list.library_map.begin();
      if (mapiter != list.library_map.end()) {
        ss << "installed auth entries: " << std::endl;      

       while (mapiter != list.library_map.end()) {
         AuthLibEntry& entry = mapiter->second;
         ss << entry.name.to_str() << std::endl;
          
         ++mapiter;
       }
      } else {
        ss << "no installed auth entries!";
      }
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
