// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2013 Inktank Storage, Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */
#include "ReplicatedBackend.h"
#include "messages/MOSDSubOp.h"
#include "messages/MOSDSubOpReply.h"
#include "messages/MOSDPGPush.h"
#include "messages/MOSDPGPull.h"
#include "messages/MOSDPGPushReply.h"

#define dout_subsys ceph_subsys_osd
#define DOUT_PREFIX_ARGS this
#undef dout_prefix
#define dout_prefix _prefix(_dout, this)
static ostream& _prefix(std::ostream *_dout, ReplicatedBackend *pgb) {
  return *_dout << pgb->get_parent()->gen_dbg_prefix();
}

ReplicatedBackend::ReplicatedBackend(
  PGBackend::Listener *pg, coll_t coll, OSDService *osd) :
  PGBackend(pg), temp_created(false),
  temp_coll(coll_t::make_temp_coll(pg->get_info().pgid)),
  coll(coll), osd(osd), cct(osd->cct) {}

void ReplicatedBackend::run_recovery_op(
  PGBackend::RecoveryHandle *_h,
  int priority)
{
  RPGHandle *h = static_cast<RPGHandle *>(_h);
  send_pushes(priority, h->pushes);
  send_pulls(priority, h->pulls);
  delete h;
}

void ReplicatedBackend::recover_object(
  const hobject_t &hoid,
  ObjectContextRef head,
  ObjectContextRef obc,
  RecoveryHandle *_h
  )
{
  dout(10) << __func__ << ": " << hoid << dendl;
  RPGHandle *h = static_cast<RPGHandle *>(_h);
  if (get_parent()->get_local_missing().is_missing(hoid)) {
    assert(!obc);
    // pull
    prepare_pull(
      hoid,
      head,
      h);
    return;
  } else {
    assert(obc);
    int started = start_pushes(
      hoid,
      obc,
      h);
    assert(started > 0);
  }
}

void ReplicatedBackend::check_recovery_sources(const OSDMapRef osdmap)
{
  for(map<int, set<hobject_t> >::iterator i = pull_from_peer.begin();
      i != pull_from_peer.end();
      ) {
    if (osdmap->is_down(i->first)) {
      dout(10) << "check_recovery_sources resetting pulls from osd." << i->first
	       << ", osdmap has it marked down" << dendl;
      for (set<hobject_t>::iterator j = i->second.begin();
	   j != i->second.end();
	   ++j) {
	assert(pulling.count(*j) == 1);
	get_parent()->cancel_pull(*j);
	pulling.erase(*j);
      }
      pull_from_peer.erase(i++);
    } else {
      ++i;
    }
  }
}

bool ReplicatedBackend::handle_message(
  OpRequestRef op
  )
{
  dout(10) << __func__ << ": " << op << dendl;
  switch (op->request->get_type()) {
  case MSG_OSD_PG_PUSH:
    // TODOXXX: needs to be active possibly
    do_push(op);
    return true;

  case MSG_OSD_PG_PULL:
    do_pull(op);
    return true;

  case MSG_OSD_PG_PUSH_REPLY:
    do_push_reply(op);
    return true;

  case MSG_OSD_SUBOP: {
    MOSDSubOp *m = static_cast<MOSDSubOp*>(op->request);
    if (m->ops.size() >= 1) {
      OSDOp *first = &m->ops[0];
      switch (first->op.op) {
      case CEPH_OSD_OP_PULL:
	sub_op_pull(op);
	return true;
      case CEPH_OSD_OP_PUSH:
        // TODOXXX: needs to be active possibly
	sub_op_push(op);
	return true;
      default:
	break;
      }
    }
    break;
  }

  case MSG_OSD_SUBOPREPLY: {
    MOSDSubOpReply *r = static_cast<MOSDSubOpReply*>(op->request);
    if (r->ops.size() >= 1) {
      OSDOp &first = r->ops[0];
      switch (first.op.op) {
      case CEPH_OSD_OP_PUSH:
	// continue peer recovery
	sub_op_push_reply(op);
	return true;
      }
    }
    break;
  }

  default:
    break;
  }
  return false;
}

void ReplicatedBackend::clear_state()
{
  // clear pushing/pulling maps
  pushing.clear();
  pulling.clear();
  pull_from_peer.clear();
}

void ReplicatedBackend::on_change(ObjectStore::Transaction *t)
{
  dout(10) << __func__ << dendl;
  // clear temp
  for (set<hobject_t>::iterator i = temp_contents.begin();
       i != temp_contents.end();
       ++i) {
    dout(10) << __func__ << ": Removing oid "
	     << *i << " from the temp collection" << dendl;
    t->remove(get_temp_coll(t), *i);
  }
  temp_contents.clear();
  clear_state();
}

coll_t ReplicatedBackend::get_temp_coll(ObjectStore::Transaction *t)
{
  if (temp_created)
    return temp_coll;
  if (!osd->store->collection_exists(temp_coll))
      t->create_collection(temp_coll);
  temp_created = true;
  return temp_coll;
}

void ReplicatedBackend::on_flushed()
{
  if (have_temp_coll() &&
      !osd->store->collection_empty(get_temp_coll())) {
    vector<hobject_t> objects;
    osd->store->collection_list(get_temp_coll(), objects);
    derr << __func__ << ": found objects in the temp collection: "
	 << objects << ", crashing now"
	 << dendl;
    assert(0 == "found garbage in the temp collection");
  }
}
