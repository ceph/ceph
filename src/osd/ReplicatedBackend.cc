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
#include "messages/MOSDOp.h"
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

bool ReplicatedBackend::can_handle_while_inactive(OpRequestRef op)
{
  dout(10) << __func__ << ": " << op << dendl;
  switch (op->get_req()->get_type()) {
  case MSG_OSD_PG_PULL:
    return true;
  case MSG_OSD_SUBOP: {
    MOSDSubOp *m = static_cast<MOSDSubOp*>(op->get_req());
    if (m->ops.size() >= 1) {
      OSDOp *first = &m->ops[0];
      switch (first->op.op) {
      case CEPH_OSD_OP_PULL:
	return true;
      default:
	return false;
      }
    } else {
      return false;
    }
  }
  default:
    return false;
  }
}

bool ReplicatedBackend::handle_message(
  OpRequestRef op
  )
{
  dout(10) << __func__ << ": " << op << dendl;
  switch (op->get_req()->get_type()) {
  case MSG_OSD_PG_PUSH:
    do_push(op);
    return true;

  case MSG_OSD_PG_PULL:
    do_pull(op);
    return true;

  case MSG_OSD_PG_PUSH_REPLY:
    do_push_reply(op);
    return true;

  case MSG_OSD_SUBOP: {
    MOSDSubOp *m = static_cast<MOSDSubOp*>(op->get_req());
    if (m->ops.size() >= 1) {
      OSDOp *first = &m->ops[0];
      switch (first->op.op) {
      case CEPH_OSD_OP_PULL:
	sub_op_pull(op);
	return true;
      case CEPH_OSD_OP_PUSH:
	sub_op_push(op);
	return true;
      default:
	break;
      }
    } else {
      sub_op_modify(op);
    }
    break;
  }

  case MSG_OSD_SUBOPREPLY: {
    MOSDSubOpReply *r = static_cast<MOSDSubOpReply*>(op->get_req());
    if (r->ops.size() >= 1) {
      OSDOp &first = r->ops[0];
      switch (first.op.op) {
      case CEPH_OSD_OP_PUSH:
	// continue peer recovery
	sub_op_push_reply(op);
	return true;
      }
    } else {
      sub_op_modify_reply(op);
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
  for (map<tid_t, InProgressOp>::iterator i = in_progress_ops.begin();
       i != in_progress_ops.end();
       in_progress_ops.erase(i++)) {
    if (i->second.on_commit)
      delete i->second.on_commit;
    if (i->second.on_applied)
      delete i->second.on_applied;
  }
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


int ReplicatedBackend::objects_list_partial(
  const hobject_t &begin,
  int min,
  int max,
  snapid_t seq,
  vector<hobject_t> *ls,
  hobject_t *next)
{
  assert(ls);
  ghobject_t _next(begin);
  ls->reserve(max);
  int r = 0;
  while (!_next.is_max() && ls->size() < (unsigned)min) {
    vector<ghobject_t> objects;
    int r = osd->store->collection_list_partial(
      coll,
      _next,
      min - ls->size(),
      max - ls->size(),
      seq,
      &objects,
      &_next);
    if (r != 0)
      break;
    for (vector<ghobject_t>::iterator i = objects.begin();
	 i != objects.end();
	 ++i) {
      assert(i->is_no_shard());
      if (i->is_no_gen()) {
	ls->push_back(i->hobj);
      }
    }
  }
  if (r == 0)
    *next = _next.hobj;
  return r;
}

int ReplicatedBackend::objects_list_range(
  const hobject_t &start,
  const hobject_t &end,
  snapid_t seq,
  vector<hobject_t> *ls)
{
  assert(ls);
  vector<ghobject_t> objects;
  int r = osd->store->collection_list_range(
    coll,
    start,
    end,
    seq,
    &objects);
  ls->reserve(objects.size());
  for (vector<ghobject_t>::iterator i = objects.begin();
       i != objects.end();
       ++i) {
    assert(i->is_no_shard());
    if (i->is_no_gen()) {
      ls->push_back(i->hobj);
    }
  }
  return r;
}

int ReplicatedBackend::objects_get_attr(
  const hobject_t &hoid,
  const string &attr,
  bufferlist *out)
{
  bufferptr bp;
  int r = osd->store->getattr(
    coll,
    hoid,
    attr.c_str(),
    bp);
  if (r >= 0 && out) {
    out->clear();
    out->push_back(bp);
  }
  return r;
}

int ReplicatedBackend::objects_get_attrs(
  const hobject_t &hoid,
  map<string, bufferlist> *out)
{
  return osd->store->getattrs(
    coll,
    hoid,
    *out);
}

int ReplicatedBackend::objects_read_sync(
  const hobject_t &hoid,
  uint64_t off,
  uint64_t len,
  bufferlist *bl)
{
  return osd->store->read(coll, hoid, off, len, *bl);
}

struct AsyncReadCallback : public GenContext<ThreadPool::TPHandle&> {
  int r;
  Context *c;
  AsyncReadCallback(int r, Context *c) : r(r), c(c) {}
  void finish(ThreadPool::TPHandle&) {
    c->complete(r);
    c = NULL;
  }
  ~AsyncReadCallback() {
    delete c;
  }
};
void ReplicatedBackend::objects_read_async(
  const hobject_t &hoid,
  const list<pair<pair<uint64_t, uint64_t>,
		  pair<bufferlist*, Context*> > > &to_read,
  Context *on_complete)
{
  int r = 0;
  for (list<pair<pair<uint64_t, uint64_t>,
		 pair<bufferlist*, Context*> > >::const_iterator i =
	   to_read.begin();
       i != to_read.end() && r >= 0;
       ++i) {
    int _r = osd->store->read(coll, hoid, i->first.first,
			      i->first.second, *(i->second.first));
    if (i->second.second) {
      osd->gen_wq.queue(
	get_parent()->bless_gencontext(
	  new AsyncReadCallback(_r, i->second.second)));
    }
    if (_r < 0)
      r = _r;
  }
  osd->gen_wq.queue(
    get_parent()->bless_gencontext(
      new AsyncReadCallback(r, on_complete)));
}


class RPGTransaction : public PGBackend::PGTransaction {
  coll_t coll;
  coll_t temp_coll;
  set<hobject_t> temp_added;
  set<hobject_t> temp_cleared;
  ObjectStore::Transaction *t;
  const coll_t &get_coll_ct(const hobject_t &hoid) {
    if (hoid.is_temp()) {
      temp_cleared.erase(hoid);
      temp_added.insert(hoid);
    }
    return get_coll(hoid);
  }
  const coll_t &get_coll_rm(const hobject_t &hoid) {
    if (hoid.is_temp()) {
      temp_added.erase(hoid);
      temp_cleared.insert(hoid);
    }
    return get_coll(hoid);
  }
  const coll_t &get_coll(const hobject_t &hoid) {
    if (hoid.is_temp())
      return temp_coll;
    else
      return coll;
  }
public:
  RPGTransaction(coll_t coll, coll_t temp_coll)
  : coll(coll), t(new ObjectStore::Transaction)
  {}

  /// Yields ownership of contained transaction
  ObjectStore::Transaction *get_transaction() {
    ObjectStore::Transaction *_t = t;
    t = 0;
    return _t;
  }
  const set<hobject_t> &get_temp_added() {
    return temp_added;
  }
  const set<hobject_t> &get_temp_cleared() {
    return temp_cleared;
  }

  void write(
    const hobject_t &hoid,
    uint64_t off,
    uint64_t len,
    bufferlist &bl
    ) {
    t->write(get_coll_ct(hoid), hoid, off, len, bl);
  }
  void remove(
    const hobject_t &hoid
    ) {
    t->remove(get_coll_rm(hoid), hoid);
  }
  void stash(
    const hobject_t &hoid,
    version_t former_version) {
    t->collection_move_rename(
      coll, hoid, coll,
      ghobject_t(hoid, former_version, 0));
  }
  void setattrs(
    const hobject_t &hoid,
    map<string, bufferlist> &attrs
    ) {
    t->setattrs(get_coll(hoid), hoid, attrs);
  }
  void setattr(
    const hobject_t &hoid,
    const string &attrname,
    bufferlist &bl
    ) {
    t->setattr(get_coll(hoid), hoid, attrname, bl);
  }
  void rmattr(
    const hobject_t &hoid,
    const string &attrname
    ) {
    t->rmattr(get_coll(hoid), hoid, attrname);
  }
  void omap_setkeys(
    const hobject_t &hoid,
    map<string, bufferlist> &attrs
    ) {
    return t->omap_setkeys(get_coll(hoid), hoid, attrs);
  }
  void omap_rmkeys(
    const hobject_t &hoid,
    set<string> &attrs
    ) {
    t->omap_rmkeys(get_coll(hoid), hoid, attrs);
  }
  void omap_clear(
    const hobject_t &hoid
    ) {
    t->omap_clear(get_coll(hoid), hoid);
  }
  void omap_setheader(
    const hobject_t &hoid,
    bufferlist &header
    ) {
    t->omap_setheader(get_coll(hoid), hoid, header);
  }
  void clone_range(
    const hobject_t &from,
    const hobject_t &to,
    uint64_t fromoff,
    uint64_t len,
    uint64_t tooff
    ) {
    assert(get_coll(from) == get_coll_ct(to)  && get_coll(from) == coll);
    t->clone_range(coll, from, to, fromoff, len, tooff);
  }
  void clone(
    const hobject_t &from,
    const hobject_t &to
    ) {
    assert(get_coll(from) == get_coll_ct(to)  && get_coll(from) == coll);
    t->clone(coll, from, to);
  }
  void rename(
    const hobject_t &from,
    const hobject_t &to
    ) {
    t->collection_move_rename(
      get_coll_rm(from),
      from,
      get_coll_ct(to),
      to);
  }

  void touch(
    const hobject_t &hoid
    ) {
    t->touch(get_coll_ct(hoid), hoid);
  }

  void truncate(
    const hobject_t &hoid,
    uint64_t off
    ) {
    t->truncate(get_coll(hoid), hoid, off);
  }
  void zero(
    const hobject_t &hoid,
    uint64_t off,
    uint64_t len
    ) {
    t->zero(get_coll(hoid), hoid, off, len);
  }

  void append(
    PGTransaction *_to_append
    ) {
    RPGTransaction *to_append = dynamic_cast<RPGTransaction*>(_to_append);
    t->append(*(to_append->t));
    for (set<hobject_t>::iterator i = to_append->temp_added.begin();
	 i != to_append->temp_added.end();
	 ++i) {
      temp_cleared.erase(*i);
      temp_added.insert(*i);
    }
    for (set<hobject_t>::iterator i = to_append->temp_cleared.begin();
	 i != to_append->temp_cleared.end();
	 ++i) {
      temp_added.erase(*i);
      temp_cleared.insert(*i);
    }
  }
  void nop() {
    t->nop();
  }
  bool empty() const {
    return t->empty();
  }
  uint64_t get_bytes_written() const {
    return t->get_encoded_bytes();
  }
  ~RPGTransaction() { delete t; }
};

PGBackend::PGTransaction *ReplicatedBackend::get_transaction()
{
  return new RPGTransaction(coll, get_temp_coll());
}

class C_OSD_OnOpCommit : public Context {
  ReplicatedBackend *pg;
  ReplicatedBackend::InProgressOp *op;
public:
  C_OSD_OnOpCommit(ReplicatedBackend *pg, ReplicatedBackend::InProgressOp *op) 
    : pg(pg), op(op) {}
  void finish(int) {
    pg->op_commit(op);
  }
};

class C_OSD_OnOpApplied : public Context {
  ReplicatedBackend *pg;
  ReplicatedBackend::InProgressOp *op;
public:
  C_OSD_OnOpApplied(ReplicatedBackend *pg, ReplicatedBackend::InProgressOp *op) 
    : pg(pg), op(op) {}
  void finish(int) {
    pg->op_applied(op);
  }
};

void ReplicatedBackend::submit_transaction(
  const hobject_t &soid,
  const eversion_t &at_version,
  PGTransaction *_t,
  const eversion_t &trim_to,
  vector<pg_log_entry_t> &log_entries,
  Context *on_local_applied_sync,
  Context *on_all_acked,
  Context *on_all_commit,
  tid_t tid,
  osd_reqid_t reqid,
  OpRequestRef orig_op)
{
  RPGTransaction *t = dynamic_cast<RPGTransaction*>(_t);
  ObjectStore::Transaction *op_t = t->get_transaction();

  assert(t->get_temp_added().size() <= 1);
  assert(t->get_temp_cleared().size() <= 1);

  assert(!in_progress_ops.count(tid));
  InProgressOp &op = in_progress_ops.insert(
    make_pair(
      tid,
      InProgressOp(
	tid, on_all_commit, on_all_acked,
	orig_op, at_version)
      )
    ).first->second;

  issue_op(
    soid,
    at_version,
    tid,
    reqid,
    trim_to,
    t->get_temp_added().size() ? *(t->get_temp_added().begin()) : hobject_t(),
    t->get_temp_cleared().size() ?
      *(t->get_temp_cleared().begin()) :hobject_t(),
    log_entries,
    &op,
    op_t);

  // add myself to gather set
  op.waiting_for_applied.insert(parent->get_actingbackfill()[0]);
  op.waiting_for_commit.insert(parent->get_actingbackfill()[0]);
  ObjectStore::Transaction local_t;
  if (t->get_temp_added().size()) {
    get_temp_coll(&local_t);
    temp_contents.insert(t->get_temp_added().begin(), t->get_temp_added().end());
  }
  for (set<hobject_t>::const_iterator i = t->get_temp_cleared().begin();
       i != t->get_temp_cleared().end();
       ++i) {
    temp_contents.erase(*i);
  }
  parent->log_operation(log_entries, trim_to, true, &local_t);
  local_t.append(*op_t);
  local_t.swap(*op_t);
  
  op_t->register_on_applied_sync(on_local_applied_sync);
  op_t->register_on_applied(
    parent->bless_context(
      new C_OSD_OnOpApplied(this, &op)));
  op_t->register_on_applied(
    new ObjectStore::C_DeleteTransaction(op_t));
  op_t->register_on_commit(
    parent->bless_context(
      new C_OSD_OnOpCommit(this, &op)));
      
  parent->queue_transaction(op_t, op.op);
  delete t;
}

void ReplicatedBackend::op_applied(
  InProgressOp *op)
{
  dout(10) << __func__ << ": " << op->tid << dendl;
  if (op->op)
    op->op->mark_event("op_applied");

  op->waiting_for_applied.erase(osd->whoami);
  parent->op_applied(op->v);

  if (op->waiting_for_applied.empty()) {
    op->on_applied->complete(0);
    op->on_applied = 0;
  }
  if (op->done()) {
    assert(!op->on_commit && !op->on_applied);
    in_progress_ops.erase(op->tid);
  }
}

void ReplicatedBackend::op_commit(
  InProgressOp *op)
{
  dout(10) << __func__ << ": " << op->tid << dendl;
  if (op->op)
    op->op->mark_event("op_commit");

  op->waiting_for_commit.erase(osd->whoami);

  if (op->waiting_for_commit.empty()) {
    op->on_commit->complete(0);
    op->on_commit = 0;
  }
  if (op->done()) {
    assert(!op->on_commit && !op->on_applied);
    in_progress_ops.erase(op->tid);
  }
}

void ReplicatedBackend::sub_op_modify_reply(OpRequestRef op)
{
  MOSDSubOpReply *r = static_cast<MOSDSubOpReply*>(op->get_req());
  assert(r->get_header().type == MSG_OSD_SUBOPREPLY);

  op->mark_started();

  // must be replication.
  tid_t rep_tid = r->get_tid();
  int fromosd = r->get_source().num();

  if (in_progress_ops.count(rep_tid)) {
    map<tid_t, InProgressOp>::iterator iter =
      in_progress_ops.find(rep_tid);
    InProgressOp &ip_op = iter->second;
    MOSDOp *m;
    if (ip_op.op)
      m = static_cast<MOSDOp *>(ip_op.op->get_req());

    if (m)
      dout(7) << __func__ << ": tid " << ip_op.tid << " op " //<< *m
	      << " ack_type " << r->ack_type
	      << " from osd." << fromosd
	      << dendl;
    else
      dout(7) << __func__ << ": tid " << ip_op.tid << " (no op) "
	      << " ack_type " << r->ack_type
	      << " from osd." << fromosd
	      << dendl;

    // oh, good.

    if (r->ack_type & CEPH_OSD_FLAG_ONDISK) {
      assert(ip_op.waiting_for_commit.count(fromosd));
      ip_op.waiting_for_commit.erase(fromosd);
      if (ip_op.op)
	ip_op.op->mark_event("sub_op_commit_rec");
    } else {
      assert(ip_op.waiting_for_applied.count(fromosd));
      if (ip_op.op)
	ip_op.op->mark_event("sub_op_applied_rec");
    }
    ip_op.waiting_for_applied.erase(fromosd);

    parent->update_peer_last_complete_ondisk(
      fromosd,
      r->get_last_complete_ondisk());

    if (ip_op.waiting_for_applied.empty() &&
        ip_op.on_applied) {
      ip_op.on_applied->complete(0);
      ip_op.on_applied = 0;
    }
    if (ip_op.waiting_for_commit.empty() &&
        ip_op.on_commit) {
      ip_op.on_commit->complete(0);
      ip_op.on_commit= 0;
    }
    if (ip_op.done()) {
      assert(!ip_op.on_commit && !ip_op.on_applied);
      in_progress_ops.erase(iter);
    }
  }
}
