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
    default:
      mon->no_reply(op);
      derr << "Unhandled message type " << m->get_type() << dendl;
      return true;
  }
}

bool MgrMonitor::preprocess_beacon(MonOpRequestRef op)
{
  //MMgrBeacon *m = static_cast<MMgrBeacon*>(op->get_req());

  return false;
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

bool MgrMonitor::prepare_beacon(MonOpRequestRef op)
{
  MMgrBeacon *m = static_cast<MMgrBeacon*>(op->get_req());

  pending_map.active_gid = m->get_gid();
  pending_map.active_addr = m->get_server_addr();

  dout(4) << "proposing epoch " << pending_map.epoch << dendl;
  wait_for_finished_proposal(op, new C_Updated(this, op));

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
}

void MgrMonitor::tick()
{
  // TODO control frequency independently of the global tick frequency
  send_digests();
}

