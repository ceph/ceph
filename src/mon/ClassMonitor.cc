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


#include "ClassMonitor.h"
#include "Monitor.h"
#include "MonitorStore.h"

#include "messages/MMonCommand.h"
#include "messages/MClass.h"
#include "messages/MClassAck.h"

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

ostream& operator<<(ostream& out, ClassMonitor& pm)
{
  std::stringstream ss;
 
  return out << "class";
}

/*
 Tick function to update the map based on performance every N seconds
*/

void ClassMonitor::tick() 
{
  if (!paxos->is_active()) return;

  update_from_paxos();
  dout(10) << *this << dendl;

  if (!mon->is_leader()) return; 

}

void ClassMonitor::create_initial(bufferlist& bl)
{
  dout(10) << "create_initial -- creating initial map" << dendl;
  ClassImpl i;
  i.name = "test";
  i.version = 12;
  i.seq = 0;
  i.stamp = g_clock.now();
  ClassLibraryIncremental inc;
  ::encode(i, inc.impl);
  inc.add = true;

#if 0
  pending_class.insert(pair<utime_t,ClassImpl>(e.stamp, e));
  e.name = "test2";
  e.version = 12;
  e.seq = 1;
#endif
  pending_class.insert(pair<utime_t,ClassLibraryIncremental>(i.stamp, inc));
}

bool ClassMonitor::update_from_paxos()
{
  version_t paxosv = paxos->get_version();

  if (paxosv == list.version) return true;
  assert(paxosv >= list.version);

  dout(0) << "ClassMonitor::update_from_paxos() paxosv=" << paxosv << " list.version=" << list.version << dendl;

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
    ClassLibraryIncremental inc;
    ::decode(inc, p);
    ClassImpl impl;
    inc.decode_impl(impl);
    if (inc.add) {
      mon->store->put_bl_ss(inc.impl, "class_impl", impl.name.c_str());
      dout(0) << "adding name=" << impl.name << " version=" << impl.version << dendl;
      list.add(impl.name, impl.version);
    } else {
      list.remove(impl.name, impl.version);
    }

    list.version++;
  }

  bufferlist bl;
  ::encode(list, bl);
  paxos->stash_latest(paxosv, bl);

  return true;
}

void ClassMonitor::create_pending()
{
  pending_class.clear();
  pending_list = list;
  dout(10) << "create_pending v " << (paxos->get_version() + 1) << dendl;
}

void ClassMonitor::encode_pending(bufferlist &bl)
{
  dout(10) << "encode_pending v " << (paxos->get_version() + 1) << dendl;
  for (multimap<utime_t,ClassLibraryIncremental>::iterator p = pending_class.begin();
       p != pending_class.end();
       p++)
    p->second.encode(bl);
}

bool ClassMonitor::preprocess_query(Message *m)
{
  dout(10) << "preprocess_query " << *m << " from " << m->get_orig_source_inst() << dendl;
  switch (m->get_type()) {
  case MSG_MON_COMMAND:
    return preprocess_command((MMonCommand*)m);

  case MSG_LOG:
    return preprocess_class((MClass*)m);

  default:
    assert(0);
    delete m;
    return true;
  }
}

bool ClassMonitor::prepare_update(Message *m)
{
  dout(10) << "prepare_update " << *m << " from " << m->get_orig_source_inst() << dendl;
  switch (m->get_type()) {
  case MSG_MON_COMMAND:
    return prepare_command((MMonCommand*)m);
  case MSG_CLASS:
    return prepare_class((MClass*)m);
  default:
    assert(0);
    delete m;
    return false;
  }
}

void ClassMonitor::committed()
{

}

bool ClassMonitor::preprocess_class(MClass *m)
{
  dout(10) << "preprocess_class " << *m << " from " << m->get_orig_source() << dendl;
  
  int num_new = 0;
  for (deque<ClassLibraryIncremental>::iterator p = m->entries.begin();
       p != m->entries.end();
       p++) {
    ClassImpl impl;
    p->decode_impl(impl);
    if (!pending_list.contains(impl.name))
      num_new++;
  }
  if (!num_new) {
    dout(10) << "  nothing new" << dendl;
    return true;
  }
  return false;
}

bool ClassMonitor::prepare_class(MClass *m) 
{
  dout(10) << "prepare_class " << *m << " from " << m->get_orig_source() << dendl;

  if (ceph_fsid_compare(&m->fsid, &mon->monmap->fsid)) {
    dout(0) << "handle_class on fsid " << m->fsid << " != " << mon->monmap->fsid << dendl;
    delete m;
    return false;
  }

  for (deque<ClassLibraryIncremental>::iterator p = m->entries.begin();
       p != m->entries.end();
       p++) {
    ClassImpl impl;
    p->decode_impl(impl);
    dout(10) << " writing class " << impl << dendl;
    if (!pending_list.contains(impl.name)) {
      pending_list.add(impl.name, impl.version);
      pending_class.insert(pair<utime_t,ClassLibraryIncremental>(impl.stamp, *p));
    }
  }

  paxos->wait_for_commit(new C_Class(this, m, m->get_orig_source_inst()));
  return true;
}

void ClassMonitor::_updated_class(MClass *m, entity_inst_t who)
{
  dout(7) << "_updated_class for " << who << dendl;
  ClassImpl impl;
  m->entries.rbegin()->decode_impl(impl);
  mon->messenger->send_message(new MClassAck(m->fsid, impl.seq), who);
  delete m;
}



bool ClassMonitor::preprocess_command(MMonCommand *m)
{
  int r = -1;
  bufferlist rdata;
  stringstream ss;

  if (r != -1) {
    string rs;
    getline(ss, rs);
    mon->reply_command(m, r, rs, rdata);
    return true;
  } else
    return false;
}


bool ClassMonitor::prepare_command(MMonCommand *m)
{
  stringstream ss;
  string rs;
  int err = -EINVAL;

  // nothing here yet
  ss << "unrecognized command";

  getline(ss, rs);
  mon->reply_command(m, err, rs);
  return false;
}
