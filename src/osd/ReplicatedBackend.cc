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
#include "common/errno.h"
#include "ReplicatedBackend.h"
#include "messages/MOSDOp.h"
#include "messages/MOSDSubOp.h"
#include "messages/MOSDRepOp.h"
#include "messages/MOSDSubOpReply.h"
#include "messages/MOSDRepOpReply.h"
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

static void log_subop_stats(
  PerfCounters *logger,
  OpRequestRef op, int subop)
{
  utime_t now = ceph_clock_now(g_ceph_context);
  utime_t latency = now;
  latency -= op->get_req()->get_recv_stamp();


  logger->inc(l_osd_sop);
  logger->tinc(l_osd_sop_lat, latency);
  logger->inc(subop);

  if (subop != l_osd_sop_pull) {
    uint64_t inb = op->get_req()->get_data().length();
    logger->inc(l_osd_sop_inb, inb);
    if (subop == l_osd_sop_w) {
      logger->inc(l_osd_sop_w_inb, inb);
      logger->tinc(l_osd_sop_w_lat, latency);
    } else if (subop == l_osd_sop_push) {
      logger->inc(l_osd_sop_push_inb, inb);
      logger->tinc(l_osd_sop_push_lat, latency);
    } else
      assert("no support subop" == 0);
  } else {
    logger->tinc(l_osd_sop_pull_lat, latency);
  }
}

ReplicatedBackend::ReplicatedBackend(
  PGBackend::Listener *pg,
  coll_t coll,
  ObjectStore *store,
  CephContext *cct) :
  PGBackend(pg, store, coll),
  cct(cct) {}

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
  eversion_t v,
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
      v,
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
  for(map<pg_shard_t, set<hobject_t, hobject_t::BitwiseComparator> >::iterator i = pull_from_peer.begin();
      i != pull_from_peer.end();
      ) {
    if (osdmap->is_down(i->first.osd)) {
      dout(10) << "check_recovery_sources resetting pulls from osd." << i->first
	       << ", osdmap has it marked down" << dendl;
      for (set<hobject_t, hobject_t::BitwiseComparator>::iterator j = i->second.begin();
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
      return true;
    }
    break;
  }

  case MSG_OSD_REPOP: {
    sub_op_modify(op);
    return true;
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
    }
    else {
      sub_op_modify_reply<MOSDSubOpReply, MSG_OSD_SUBOPREPLY>(op);
      return true;
    }
    break;
  }

  case MSG_OSD_REPOPREPLY: {
    sub_op_modify_reply<MOSDRepOpReply, MSG_OSD_REPOPREPLY>(op);
    return true;
  }

  default:
    break;
  }
  return false;
}

void ReplicatedBackend::clear_recovery_state()
{
  // clear pushing/pulling maps
  pushing.clear();
  pulling.clear();
  pull_from_peer.clear();
}

void ReplicatedBackend::on_change()
{
  dout(10) << __func__ << dendl;
  for (map<ceph_tid_t, InProgressOp>::iterator i = in_progress_ops.begin();
       i != in_progress_ops.end();
       in_progress_ops.erase(i++)) {
    if (i->second.on_commit)
      delete i->second.on_commit;
    if (i->second.on_applied)
      delete i->second.on_applied;
  }
  clear_recovery_state();
}

void ReplicatedBackend::on_flushed()
{
}

int ReplicatedBackend::objects_read_sync(
  const hobject_t &hoid,
  uint64_t off,
  uint64_t len,
  uint32_t op_flags,
  bufferlist *bl)
{
  return store->read(coll, ghobject_t(hoid), off, len, *bl, op_flags);
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
  const list<pair<boost::tuple<uint64_t, uint64_t, uint32_t>,
		  pair<bufferlist*, Context*> > > &to_read,
  Context *on_complete,
  bool fast_read)
{
  // There is no fast read implementation for replication backend yet
  assert(!fast_read);

  int r = 0;
  for (list<pair<boost::tuple<uint64_t, uint64_t, uint32_t>,
		 pair<bufferlist*, Context*> > >::const_iterator i =
	   to_read.begin();
       i != to_read.end() && r >= 0;
       ++i) {
    int _r = store->read(coll, ghobject_t(hoid), i->first.get<0>(),
			 i->first.get<1>(), *(i->second.first),
			 i->first.get<2>());
    if (i->second.second) {
      get_parent()->schedule_recovery_work(
	get_parent()->bless_gencontext(
	  new AsyncReadCallback(_r, i->second.second)));
    }
    if (_r < 0)
      r = _r;
  }
  get_parent()->schedule_recovery_work(
    get_parent()->bless_gencontext(
      new AsyncReadCallback(r, on_complete)));
}


class RPGTransaction : public PGBackend::PGTransaction {
  coll_t coll;
  set<hobject_t, hobject_t::BitwiseComparator> temp_added;
  set<hobject_t, hobject_t::BitwiseComparator> temp_cleared;
  ObjectStore::Transaction *t;
  uint64_t written;
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
    return coll;
  }
public:
  RPGTransaction(coll_t coll, bool use_tbl)
    : coll(coll), t(new ObjectStore::Transaction), written(0) {
    t->set_use_tbl(use_tbl);
  }

  /// Yields ownership of contained transaction
  ObjectStore::Transaction *get_transaction() {
    ObjectStore::Transaction *_t = t;
    t = 0;
    return _t;
  }
  const set<hobject_t, hobject_t::BitwiseComparator> &get_temp_added() {
    return temp_added;
  }
  const set<hobject_t, hobject_t::BitwiseComparator> &get_temp_cleared() {
    return temp_cleared;
  }

  void write(
    const hobject_t &hoid,
    uint64_t off,
    uint64_t len,
    bufferlist &bl,
    uint32_t fadvise_flags
    ) {
    written += len;
    t->write(get_coll_ct(hoid), ghobject_t(hoid), off, len, bl, fadvise_flags);
  }
  void remove(
    const hobject_t &hoid
    ) {
    t->remove(get_coll_rm(hoid), ghobject_t(hoid));
  }
  void stash(
    const hobject_t &hoid,
    version_t former_version) {
    t->collection_move_rename(
      coll, ghobject_t(hoid), coll,
      ghobject_t(hoid, former_version, shard_id_t::NO_SHARD));
  }
  void setattrs(
    const hobject_t &hoid,
    map<string, bufferlist> &attrs
    ) {
    t->setattrs(get_coll(hoid), ghobject_t(hoid), attrs);
  }
  void setattr(
    const hobject_t &hoid,
    const string &attrname,
    bufferlist &bl
    ) {
    t->setattr(get_coll(hoid), ghobject_t(hoid), attrname, bl);
  }
  void rmattr(
    const hobject_t &hoid,
    const string &attrname
    ) {
    t->rmattr(get_coll(hoid), ghobject_t(hoid), attrname);
  }
  void omap_setkeys(
    const hobject_t &hoid,
    map<string, bufferlist> &keys
    ) {
    for (map<string, bufferlist>::iterator p = keys.begin(); p != keys.end(); ++p)
      written += p->first.length() + p->second.length();
    return t->omap_setkeys(get_coll(hoid), ghobject_t(hoid), keys);
  }
  void omap_setkeys(
    const hobject_t &hoid,
    bufferlist &keys_bl
    ) {
    written += keys_bl.length();
    return t->omap_setkeys(get_coll(hoid), ghobject_t(hoid), keys_bl);
  }
  void omap_rmkeys(
    const hobject_t &hoid,
    set<string> &keys
    ) {
    t->omap_rmkeys(get_coll(hoid), ghobject_t(hoid), keys);
  }
  void omap_rmkeys(
    const hobject_t &hoid,
    bufferlist &keys_bl
    ) {
    t->omap_rmkeys(get_coll(hoid), ghobject_t(hoid), keys_bl);
  }
  void omap_clear(
    const hobject_t &hoid
    ) {
    t->omap_clear(get_coll(hoid), ghobject_t(hoid));
  }
  void omap_setheader(
    const hobject_t &hoid,
    bufferlist &header
    ) {
    written += header.length();
    t->omap_setheader(get_coll(hoid), ghobject_t(hoid), header);
  }
  void clone_range(
    const hobject_t &from,
    const hobject_t &to,
    uint64_t fromoff,
    uint64_t len,
    uint64_t tooff
    ) {
    assert(get_coll(from) == get_coll_ct(to)  && get_coll(from) == coll);
    t->clone_range(coll, ghobject_t(from), ghobject_t(to), fromoff, len, tooff);
  }
  void clone(
    const hobject_t &from,
    const hobject_t &to
    ) {
    assert(get_coll(from) == get_coll_ct(to)  && get_coll(from) == coll);
    t->clone(coll, ghobject_t(from), ghobject_t(to));
  }
  void rename(
    const hobject_t &from,
    const hobject_t &to
    ) {
    t->collection_move_rename(
      get_coll_rm(from),
      ghobject_t(from),
      get_coll_ct(to),
      ghobject_t(to));
  }

  void touch(
    const hobject_t &hoid
    ) {
    t->touch(get_coll_ct(hoid), ghobject_t(hoid));
  }

  void truncate(
    const hobject_t &hoid,
    uint64_t off
    ) {
    t->truncate(get_coll(hoid), ghobject_t(hoid), off);
  }
  void zero(
    const hobject_t &hoid,
    uint64_t off,
    uint64_t len
    ) {
    t->zero(get_coll(hoid), ghobject_t(hoid), off, len);
  }

  void set_alloc_hint(
    const hobject_t &hoid,
    uint64_t expected_object_size,
    uint64_t expected_write_size
    ) {
    t->set_alloc_hint(get_coll(hoid), ghobject_t(hoid), expected_object_size,
                      expected_write_size);
  }

  using PGBackend::PGTransaction::append;
  void append(
    PGTransaction *_to_append
    ) {
    RPGTransaction *to_append = dynamic_cast<RPGTransaction*>(_to_append);
    assert(to_append);
    written += to_append->written;
    to_append->written = 0;
    t->append(*(to_append->t));
    for (set<hobject_t, hobject_t::BitwiseComparator>::iterator i = to_append->temp_added.begin();
	 i != to_append->temp_added.end();
	 ++i) {
      temp_cleared.erase(*i);
      temp_added.insert(*i);
    }
    for (set<hobject_t, hobject_t::BitwiseComparator>::iterator i = to_append->temp_cleared.begin();
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
    return written;
  }
  ~RPGTransaction() { delete t; }
};

PGBackend::PGTransaction *ReplicatedBackend::get_transaction()
{
  return new RPGTransaction(coll, parent->transaction_use_tbl());
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
  const eversion_t &trim_rollback_to,
  const vector<pg_log_entry_t> &log_entries,
  boost::optional<pg_hit_set_history_t> &hset_history,
  Context *on_local_applied_sync,
  Context *on_all_acked,
  Context *on_all_commit,
  ceph_tid_t tid,
  osd_reqid_t reqid,
  OpRequestRef orig_op)
{
  RPGTransaction *t = dynamic_cast<RPGTransaction*>(_t);
  assert(t);
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

  op.waiting_for_applied.insert(
    parent->get_actingbackfill_shards().begin(),
    parent->get_actingbackfill_shards().end());
  op.waiting_for_commit.insert(
    parent->get_actingbackfill_shards().begin(),
    parent->get_actingbackfill_shards().end());


  issue_op(
    soid,
    at_version,
    tid,
    reqid,
    trim_to,
    trim_rollback_to,
    t->get_temp_added().empty() ? hobject_t() : *(t->get_temp_added().begin()),
    t->get_temp_cleared().empty() ?
      hobject_t() : *(t->get_temp_cleared().begin()),
    log_entries,
    hset_history,
    &op,
    op_t);

  if (!(t->get_temp_added().empty())) {
    add_temp_objs(t->get_temp_added());
  }
  clear_temp_objs(t->get_temp_cleared());

  parent->log_operation(
    log_entries,
    hset_history,
    trim_to,
    trim_rollback_to,
    true,
    op_t);
  
  op_t->register_on_applied_sync(on_local_applied_sync);
  op_t->register_on_applied(
    parent->bless_context(
      new C_OSD_OnOpApplied(this, &op)));
  op_t->register_on_applied(
    new ObjectStore::C_DeleteTransaction(op_t));
  op_t->register_on_commit(
    parent->bless_context(
      new C_OSD_OnOpCommit(this, &op)));

  list<ObjectStore::Transaction*> tls;
  tls.push_back(op_t);
  parent->queue_transactions(tls, op.op);
  delete t;
}

void ReplicatedBackend::op_applied(
  InProgressOp *op)
{
  dout(10) << __func__ << ": " << op->tid << dendl;
  if (op->op)
    op->op->mark_event("op_applied");

  op->waiting_for_applied.erase(get_parent()->whoami_shard());
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

  op->waiting_for_commit.erase(get_parent()->whoami_shard());

  if (op->waiting_for_commit.empty()) {
    op->on_commit->complete(0);
    op->on_commit = 0;
  }
  if (op->done()) {
    assert(!op->on_commit && !op->on_applied);
    in_progress_ops.erase(op->tid);
  }
}

template<typename T, int MSGTYPE>
void ReplicatedBackend::sub_op_modify_reply(OpRequestRef op)
{
  T *r = static_cast<T *>(op->get_req());
  r->finish_decode();
  assert(r->get_header().type == MSGTYPE);
  assert(MSGTYPE == MSG_OSD_SUBOPREPLY || MSGTYPE == MSG_OSD_REPOPREPLY);

  op->mark_started();

  // must be replication.
  ceph_tid_t rep_tid = r->get_tid();
  pg_shard_t from = r->from;

  if (in_progress_ops.count(rep_tid)) {
    map<ceph_tid_t, InProgressOp>::iterator iter =
      in_progress_ops.find(rep_tid);
    InProgressOp &ip_op = iter->second;
    MOSDOp *m = NULL;
    if (ip_op.op)
      m = static_cast<MOSDOp *>(ip_op.op->get_req());

    if (m)
      dout(7) << __func__ << ": tid " << ip_op.tid << " op " //<< *m
	      << " ack_type " << (int)r->ack_type
	      << " from " << from
	      << dendl;
    else
      dout(7) << __func__ << ": tid " << ip_op.tid << " (no op) "
	      << " ack_type " << (int)r->ack_type
	      << " from " << from
	      << dendl;

    // oh, good.

    if (r->ack_type & CEPH_OSD_FLAG_ONDISK) {
      assert(ip_op.waiting_for_commit.count(from));
      ip_op.waiting_for_commit.erase(from);
      if (ip_op.op) {
        ostringstream ss;
        ss << "sub_op_commit_rec from " << from;
	ip_op.op->mark_event(ss.str());
      }
    } else {
      assert(ip_op.waiting_for_applied.count(from));
      if (ip_op.op) {
        ostringstream ss;
        ss << "sub_op_applied_rec from " << from;
	ip_op.op->mark_event(ss.str());
      }
    }
    ip_op.waiting_for_applied.erase(from);

    parent->update_peer_last_complete_ondisk(
      from,
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

void ReplicatedBackend::be_deep_scrub(
  const hobject_t &poid,
  uint32_t seed,
  ScrubMap::object &o,
  ThreadPool::TPHandle &handle)
{
  dout(10) << __func__ << " " << poid << " seed " << seed << dendl;
  bufferhash h(seed), oh(seed);
  bufferlist bl, hdrbl;
  int r;
  __u64 pos = 0;

  uint32_t fadvise_flags = CEPH_OSD_OP_FLAG_FADVISE_SEQUENTIAL | CEPH_OSD_OP_FLAG_FADVISE_DONTNEED;

  while ( (r = store->read(
             coll,
             ghobject_t(
               poid, ghobject_t::NO_GEN, get_parent()->whoami_shard().shard),
             pos,
             cct->_conf->osd_deep_scrub_stride, bl,
             fadvise_flags, true)) > 0) {
    handle.reset_tp_timeout();
    h << bl;
    pos += bl.length();
    bl.clear();
  }
  if (r == -EIO) {
    dout(25) << __func__ << "  " << poid << " got "
	     << r << " on read, read_error" << dendl;
    o.read_error = true;
    return;
  }
  o.digest = h.digest();
  o.digest_present = true;

  bl.clear();
  r = store->omap_get_header(
    coll,
    ghobject_t(
      poid, ghobject_t::NO_GEN, get_parent()->whoami_shard().shard),
    &hdrbl, true);
  // NOTE: bobtail to giant, we would crc the head as (len, head).
  // that changes at the same time we start using a non-zero seed.
  if (r == 0 && hdrbl.length()) {
    dout(25) << "CRC header " << string(hdrbl.c_str(), hdrbl.length())
             << dendl;
    if (seed == 0) {
      // legacy
      bufferlist bl;
      ::encode(hdrbl, bl);
      oh << bl;
    } else {
      oh << hdrbl;
    }
  } else if (r == -EIO) {
    dout(25) << __func__ << "  " << poid << " got "
	     << r << " on omap header read, read_error" << dendl;
    o.read_error = true;
    return;
  }

  ObjectMap::ObjectMapIterator iter = store->get_omap_iterator(
    coll,
    ghobject_t(
      poid, ghobject_t::NO_GEN, get_parent()->whoami_shard().shard));
  assert(iter);
  uint64_t keys_scanned = 0;
  for (iter->seek_to_first(); iter->valid() ; iter->next(false)) {
    if (cct->_conf->osd_scan_list_ping_tp_interval &&
	(keys_scanned % cct->_conf->osd_scan_list_ping_tp_interval == 0)) {
      handle.reset_tp_timeout();
    }
    ++keys_scanned;

    dout(25) << "CRC key " << iter->key() << " value "
	     << string(iter->value().c_str(), iter->value().length()) << dendl;

    ::encode(iter->key(), bl);
    ::encode(iter->value(), bl);
    oh << bl;
    bl.clear();
  }
  if (iter->status() == -EIO) {
    dout(25) << __func__ << "  " << poid << " got "
	     << r << " on omap scan, read_error" << dendl;
    o.read_error = true;
    return;
  }
  //Store final calculated CRC32 of omap header & key/values
  o.omap_digest = oh.digest();
  o.omap_digest_present = true;
}

void ReplicatedBackend::_do_push(OpRequestRef op)
{
  MOSDPGPush *m = static_cast<MOSDPGPush *>(op->get_req());
  assert(m->get_type() == MSG_OSD_PG_PUSH);
  pg_shard_t from = m->from;

  vector<PushReplyOp> replies;
  ObjectStore::Transaction *t = new ObjectStore::Transaction;
  for (vector<PushOp>::iterator i = m->pushes.begin();
       i != m->pushes.end();
       ++i) {
    replies.push_back(PushReplyOp());
    handle_push(from, *i, &(replies.back()), t);
  }

  MOSDPGPushReply *reply = new MOSDPGPushReply;
  reply->from = get_parent()->whoami_shard();
  reply->set_priority(m->get_priority());
  reply->pgid = get_info().pgid;
  reply->map_epoch = m->map_epoch;
  reply->replies.swap(replies);
  reply->compute_cost(cct);

  t->register_on_complete(
    new PG_SendMessageOnConn(
      get_parent(), reply, m->get_connection()));

  t->register_on_applied(
    new ObjectStore::C_DeleteTransaction(t));
  get_parent()->queue_transaction(t);
}

struct C_ReplicatedBackend_OnPullComplete : GenContext<ThreadPool::TPHandle&> {
  ReplicatedBackend *bc;
  list<hobject_t> to_continue;
  int priority;
  C_ReplicatedBackend_OnPullComplete(ReplicatedBackend *bc, int priority)
    : bc(bc), priority(priority) {}

  void finish(ThreadPool::TPHandle &handle) {
    ReplicatedBackend::RPGHandle *h = bc->_open_recovery_op();
    for (list<hobject_t>::iterator i =
	   to_continue.begin();
	 i != to_continue.end();
	 ++i) {
      map<hobject_t, ReplicatedBackend::PullInfo, hobject_t::BitwiseComparator>::iterator j =
	bc->pulling.find(*i);
      assert(j != bc->pulling.end());
      if (!bc->start_pushes(*i, j->second.obc, h)) {
	bc->get_parent()->on_global_recover(
	  *i);
      }
      bc->pulling.erase(*i);
      handle.reset_tp_timeout();
    }
    bc->run_recovery_op(h, priority);
  }
};

void ReplicatedBackend::_do_pull_response(OpRequestRef op)
{
  MOSDPGPush *m = static_cast<MOSDPGPush *>(op->get_req());
  assert(m->get_type() == MSG_OSD_PG_PUSH);
  pg_shard_t from = m->from;

  vector<PullOp> replies(1);
  ObjectStore::Transaction *t = new ObjectStore::Transaction;
  list<hobject_t> to_continue;
  for (vector<PushOp>::iterator i = m->pushes.begin();
       i != m->pushes.end();
       ++i) {
    bool more = handle_pull_response(from, *i, &(replies.back()), &to_continue, t);
    if (more)
      replies.push_back(PullOp());
  }
  if (!to_continue.empty()) {
    C_ReplicatedBackend_OnPullComplete *c =
      new C_ReplicatedBackend_OnPullComplete(
	this,
	m->get_priority());
    c->to_continue.swap(to_continue);
    t->register_on_complete(
      new PG_RecoveryQueueAsync(
	get_parent(),
	get_parent()->bless_gencontext(c)));
  }
  replies.erase(replies.end() - 1);

  if (replies.size()) {
    MOSDPGPull *reply = new MOSDPGPull;
    reply->from = parent->whoami_shard();
    reply->set_priority(m->get_priority());
    reply->pgid = get_info().pgid;
    reply->map_epoch = m->map_epoch;
    reply->pulls.swap(replies);
    reply->compute_cost(cct);

    t->register_on_complete(
      new PG_SendMessageOnConn(
	get_parent(), reply, m->get_connection()));
  }

  t->register_on_applied(
    new ObjectStore::C_DeleteTransaction(t));
  get_parent()->queue_transaction(t);
}

void ReplicatedBackend::do_pull(OpRequestRef op)
{
  MOSDPGPull *m = static_cast<MOSDPGPull *>(op->get_req());
  assert(m->get_type() == MSG_OSD_PG_PULL);
  pg_shard_t from = m->from;

  map<pg_shard_t, vector<PushOp> > replies;
  for (vector<PullOp>::iterator i = m->pulls.begin();
       i != m->pulls.end();
       ++i) {
    replies[from].push_back(PushOp());
    handle_pull(from, *i, &(replies[from].back()));
  }
  send_pushes(m->get_priority(), replies);
}

void ReplicatedBackend::do_push_reply(OpRequestRef op)
{
  MOSDPGPushReply *m = static_cast<MOSDPGPushReply *>(op->get_req());
  assert(m->get_type() == MSG_OSD_PG_PUSH_REPLY);
  pg_shard_t from = m->from;

  vector<PushOp> replies(1);
  for (vector<PushReplyOp>::iterator i = m->replies.begin();
       i != m->replies.end();
       ++i) {
    bool more = handle_push_reply(from, *i, &(replies.back()));
    if (more)
      replies.push_back(PushOp());
  }
  replies.erase(replies.end() - 1);

  map<pg_shard_t, vector<PushOp> > _replies;
  _replies[from].swap(replies);
  send_pushes(m->get_priority(), _replies);
}

template<typename T, int MSGTYPE>
Message * ReplicatedBackend::generate_subop(
  const hobject_t &soid,
  const eversion_t &at_version,
  ceph_tid_t tid,
  osd_reqid_t reqid,
  eversion_t pg_trim_to,
  eversion_t pg_trim_rollback_to,
  hobject_t new_temp_oid,
  hobject_t discard_temp_oid,
  const vector<pg_log_entry_t> &log_entries,
  boost::optional<pg_hit_set_history_t> &hset_hist,
  InProgressOp *op,
  ObjectStore::Transaction *op_t,
  pg_shard_t peer,
  const pg_info_t &pinfo)
{
  int acks_wanted = CEPH_OSD_FLAG_ACK | CEPH_OSD_FLAG_ONDISK;
  assert(MSGTYPE == MSG_OSD_SUBOP || MSGTYPE == MSG_OSD_REPOP);
  // forward the write/update/whatever
  T *wr = new T(
    reqid, parent->whoami_shard(),
    spg_t(get_info().pgid.pgid, peer.shard),
    soid, acks_wanted,
    get_osdmap()->get_epoch(),
    tid, at_version);

  // ship resulting transaction, log entries, and pg_stats
  if (!parent->should_send_op(peer, soid)) {
    dout(10) << "issue_repop shipping empty opt to osd." << peer
	     <<", object " << soid
	     << " beyond MAX(last_backfill_started "
	     << ", pinfo.last_backfill "
	     << pinfo.last_backfill << ")" << dendl;
    ObjectStore::Transaction t;
    t.set_use_tbl(op_t->get_use_tbl());
    ::encode(t, wr->get_data());
  } else {
    ::encode(*op_t, wr->get_data());
  }

  ::encode(log_entries, wr->logbl);

  if (pinfo.is_incomplete())
    wr->pg_stats = pinfo.stats;  // reflects backfill progress
  else
    wr->pg_stats = get_info().stats;

  wr->pg_trim_to = pg_trim_to;
  wr->pg_trim_rollback_to = pg_trim_rollback_to;

  wr->new_temp_oid = new_temp_oid;
  wr->discard_temp_oid = discard_temp_oid;
  wr->updated_hit_set_history = hset_hist;
  return wr;
}

void ReplicatedBackend::issue_op(
  const hobject_t &soid,
  const eversion_t &at_version,
  ceph_tid_t tid,
  osd_reqid_t reqid,
  eversion_t pg_trim_to,
  eversion_t pg_trim_rollback_to,
  hobject_t new_temp_oid,
  hobject_t discard_temp_oid,
  const vector<pg_log_entry_t> &log_entries,
  boost::optional<pg_hit_set_history_t> &hset_hist,
  InProgressOp *op,
  ObjectStore::Transaction *op_t)
{

  if (parent->get_actingbackfill_shards().size() > 1) {
    ostringstream ss;
    set<pg_shard_t> replicas = parent->get_actingbackfill_shards();
    replicas.erase(parent->whoami_shard());
    ss << "waiting for subops from " << replicas;
    if (op->op)
      op->op->mark_sub_op_sent(ss.str());
  }
  for (set<pg_shard_t>::const_iterator i =
	 parent->get_actingbackfill_shards().begin();
       i != parent->get_actingbackfill_shards().end();
       ++i) {
    if (*i == parent->whoami_shard()) continue;
    pg_shard_t peer = *i;
    const pg_info_t &pinfo = parent->get_shard_info().find(peer)->second;

    Message *wr;
    uint64_t min_features = parent->min_peer_features();
    if (!(min_features & CEPH_FEATURE_OSD_REPOP)) {
      dout(20) << "Talking to old version of OSD, doesn't support RepOp, fall back to SubOp" << dendl;
      wr = generate_subop<MOSDSubOp, MSG_OSD_SUBOP>(
	    soid,
	    at_version,
	    tid,
	    reqid,
	    pg_trim_to,
	    pg_trim_rollback_to,
	    new_temp_oid,
	    discard_temp_oid,
	    log_entries,
	    hset_hist,
	    op,
	    op_t,
	    peer,
	    pinfo);
    } else {
      wr = generate_subop<MOSDRepOp, MSG_OSD_REPOP>(
	    soid,
	    at_version,
	    tid,
	    reqid,
	    pg_trim_to,
	    pg_trim_rollback_to,
	    new_temp_oid,
	    discard_temp_oid,
	    log_entries,
	    hset_hist,
	    op,
	    op_t,
	    peer,
	    pinfo);
    }

    get_parent()->send_message_osd_cluster(
      peer.osd, wr, get_osdmap()->get_epoch());
  }
}

// sub op modify
void ReplicatedBackend::sub_op_modify(OpRequestRef op) {
  Message *m = op->get_req();
  int msg_type = m->get_type();
  if (msg_type == MSG_OSD_SUBOP) {
    sub_op_modify_impl<MOSDSubOp, MSG_OSD_SUBOP>(op);
  } else if (msg_type == MSG_OSD_REPOP) {
    sub_op_modify_impl<MOSDRepOp, MSG_OSD_REPOP>(op);
  } else {
    assert(0);
  }
}

template<typename T, int MSGTYPE>
void ReplicatedBackend::sub_op_modify_impl(OpRequestRef op)
{
  T *m = static_cast<T *>(op->get_req());
  m->finish_decode();
  int msg_type = m->get_type();
  assert(MSGTYPE == msg_type);
  assert(msg_type == MSG_OSD_SUBOP || msg_type == MSG_OSD_REPOP);

  const hobject_t& soid = m->poid;

  dout(10) << "sub_op_modify trans"
           << " " << soid
           << " v " << m->version
	   << (m->logbl.length() ? " (transaction)" : " (parallel exec")
	   << " " << m->logbl.length()
	   << dendl;

  // sanity checks
  assert(m->map_epoch >= get_info().history.same_interval_since);

  // we better not be missing this.
  assert(!parent->get_log().get_missing().is_missing(soid));

  int ackerosd = m->get_source().num();

  op->mark_started();

  RepModifyRef rm(new RepModify);
  rm->op = op;
  rm->ackerosd = ackerosd;
  rm->last_complete = get_info().last_complete;
  rm->epoch_started = get_osdmap()->get_epoch();

  assert(m->logbl.length());
  // shipped transaction and log entries
  vector<pg_log_entry_t> log;

  bufferlist::iterator p = m->get_data().begin();
  ::decode(rm->opt, p);
  rm->localt.set_use_tbl(rm->opt.get_use_tbl());

  if (m->new_temp_oid != hobject_t()) {
    dout(20) << __func__ << " start tracking temp " << m->new_temp_oid << dendl;
    add_temp_obj(m->new_temp_oid);
  }
  if (m->discard_temp_oid != hobject_t()) {
    dout(20) << __func__ << " stop tracking temp " << m->discard_temp_oid << dendl;
    if (rm->opt.empty()) {
      dout(10) << __func__ << ": removing object " << m->discard_temp_oid
	       << " since we won't get the transaction" << dendl;
      rm->localt.remove(coll, ghobject_t(m->discard_temp_oid));
    }
    clear_temp_obj(m->discard_temp_oid);
  }

  p = m->logbl.begin();
  ::decode(log, p);
  rm->opt.set_fadvise_flag(CEPH_OSD_OP_FLAG_FADVISE_DONTNEED);

  bool update_snaps = false;
  if (!rm->opt.empty()) {
    // If the opt is non-empty, we infer we are before
    // last_backfill (according to the primary, not our
    // not-quite-accurate value), and should update the
    // collections now.  Otherwise, we do it later on push.
    update_snaps = true;
  }
  parent->update_stats(m->pg_stats);
  parent->log_operation(
    log,
    m->updated_hit_set_history,
    m->pg_trim_to,
    m->pg_trim_rollback_to,
    update_snaps,
    &(rm->localt));

  rm->bytes_written = rm->opt.get_encoded_bytes();

  rm->opt.register_on_commit(
    parent->bless_context(
      new C_OSD_RepModifyCommit(this, rm)));
  rm->localt.register_on_applied(
    parent->bless_context(
      new C_OSD_RepModifyApply(this, rm)));
  list<ObjectStore::Transaction*> tls;
  tls.push_back(&(rm->localt));
  tls.push_back(&(rm->opt));
  parent->queue_transactions(tls, op);
  // op is cleaned up by oncommit/onapply when both are executed
}

void ReplicatedBackend::sub_op_modify_applied(RepModifyRef rm)
{
  rm->op->mark_event("sub_op_applied");
  rm->applied = true;

  dout(10) << "sub_op_modify_applied on " << rm << " op "
	   << *rm->op->get_req() << dendl;
  Message *m = rm->op->get_req();

  Message *ack = NULL;
  eversion_t version;

  if (m->get_type() == MSG_OSD_SUBOP) {
    // doesn't have CLIENT SUBOP feature ,use Subop
    MOSDSubOp *req = static_cast<MOSDSubOp*>(m);
    version = req->version;
    if (!rm->committed)
      ack = new MOSDSubOpReply(
	req, parent->whoami_shard(),
	0, get_osdmap()->get_epoch(), CEPH_OSD_FLAG_ACK);
  } else if (m->get_type() == MSG_OSD_REPOP) {
    MOSDRepOp *req = static_cast<MOSDRepOp*>(m);
    version = req->version;
    if (!rm->committed)
      ack = new MOSDRepOpReply(
	static_cast<MOSDRepOp*>(m), parent->whoami_shard(),
	0, get_osdmap()->get_epoch(), CEPH_OSD_FLAG_ACK);
  } else {
    assert(0);
  }

  // send ack to acker only if we haven't sent a commit already
  if (ack) {
    ack->set_priority(CEPH_MSG_PRIO_HIGH); // this better match commit priority!
    get_parent()->send_message_osd_cluster(
      rm->ackerosd, ack, get_osdmap()->get_epoch());
  }

  parent->op_applied(version);
}

void ReplicatedBackend::sub_op_modify_commit(RepModifyRef rm)
{
  rm->op->mark_commit_sent();
  rm->committed = true;

  // send commit.
  dout(10) << "sub_op_modify_commit on op " << *rm->op->get_req()
	   << ", sending commit to osd." << rm->ackerosd
	   << dendl;

  assert(get_osdmap()->is_up(rm->ackerosd));
  get_parent()->update_last_complete_ondisk(rm->last_complete);

  Message *m = rm->op->get_req();
  Message *commit = NULL;
  if (m->get_type() == MSG_OSD_SUBOP) {
    // doesn't have CLIENT SUBOP feature ,use Subop
    MOSDSubOpReply  *reply = new MOSDSubOpReply(
      static_cast<MOSDSubOp*>(m),
      get_parent()->whoami_shard(),
      0, get_osdmap()->get_epoch(), CEPH_OSD_FLAG_ONDISK);
    reply->set_last_complete_ondisk(rm->last_complete);
    commit = reply;
  } else if (m->get_type() == MSG_OSD_REPOP) {
    MOSDRepOpReply *reply = new MOSDRepOpReply(
      static_cast<MOSDRepOp*>(m),
      get_parent()->whoami_shard(),
      0, get_osdmap()->get_epoch(), CEPH_OSD_FLAG_ONDISK);
    reply->set_last_complete_ondisk(rm->last_complete);
    commit = reply;
  }
  else {
    assert(0);
  }

  commit->set_priority(CEPH_MSG_PRIO_HIGH); // this better match ack priority!
  get_parent()->send_message_osd_cluster(
    rm->ackerosd, commit, get_osdmap()->get_epoch());

  log_subop_stats(get_parent()->get_logger(), rm->op, l_osd_sop_w);
}


// ===========================================================

void ReplicatedBackend::calc_head_subsets(
  ObjectContextRef obc, SnapSet& snapset, const hobject_t& head,
  const pg_missing_t& missing,
  const hobject_t &last_backfill,
  interval_set<uint64_t>& data_subset,
  map<hobject_t, interval_set<uint64_t>, hobject_t::BitwiseComparator>& clone_subsets)
{
  dout(10) << "calc_head_subsets " << head
	   << " clone_overlap " << snapset.clone_overlap << dendl;

  uint64_t size = obc->obs.oi.size;
  if (size)
    data_subset.insert(0, size);

  if (get_parent()->get_pool().allow_incomplete_clones()) {
    dout(10) << __func__ << ": caching (was) enabled, skipping clone subsets" << dendl;
    return;
  }

  if (!cct->_conf->osd_recover_clone_overlap) {
    dout(10) << "calc_head_subsets " << head << " -- osd_recover_clone_overlap disabled" << dendl;
    return;
  }


  interval_set<uint64_t> cloning;
  interval_set<uint64_t> prev;
  if (size)
    prev.insert(0, size);

  for (int j=snapset.clones.size()-1; j>=0; j--) {
    hobject_t c = head;
    c.snap = snapset.clones[j];
    prev.intersection_of(snapset.clone_overlap[snapset.clones[j]]);
    if (!missing.is_missing(c) &&
	cmp(c, last_backfill, get_parent()->sort_bitwise()) < 0) {
      dout(10) << "calc_head_subsets " << head << " has prev " << c
	       << " overlap " << prev << dendl;
      clone_subsets[c] = prev;
      cloning.union_of(prev);
      break;
    }
    dout(10) << "calc_head_subsets " << head << " does not have prev " << c
	     << " overlap " << prev << dendl;
  }


  if (cloning.num_intervals() > cct->_conf->osd_recover_clone_overlap_limit) {
    dout(10) << "skipping clone, too many holes" << dendl;
    clone_subsets.clear();
    cloning.clear();
  }

  // what's left for us to push?
  data_subset.subtract(cloning);

  dout(10) << "calc_head_subsets " << head
	   << "  data_subset " << data_subset
	   << "  clone_subsets " << clone_subsets << dendl;
}

void ReplicatedBackend::calc_clone_subsets(
  SnapSet& snapset, const hobject_t& soid,
  const pg_missing_t& missing,
  const hobject_t &last_backfill,
  interval_set<uint64_t>& data_subset,
  map<hobject_t, interval_set<uint64_t>, hobject_t::BitwiseComparator>& clone_subsets)
{
  dout(10) << "calc_clone_subsets " << soid
	   << " clone_overlap " << snapset.clone_overlap << dendl;

  uint64_t size = snapset.clone_size[soid.snap];
  if (size)
    data_subset.insert(0, size);

  if (get_parent()->get_pool().allow_incomplete_clones()) {
    dout(10) << __func__ << ": caching (was) enabled, skipping clone subsets" << dendl;
    return;
  }

  if (!cct->_conf->osd_recover_clone_overlap) {
    dout(10) << "calc_clone_subsets " << soid << " -- osd_recover_clone_overlap disabled" << dendl;
    return;
  }

  unsigned i;
  for (i=0; i < snapset.clones.size(); i++)
    if (snapset.clones[i] == soid.snap)
      break;

  // any overlap with next older clone?
  interval_set<uint64_t> cloning;
  interval_set<uint64_t> prev;
  if (size)
    prev.insert(0, size);
  for (int j=i-1; j>=0; j--) {
    hobject_t c = soid;
    c.snap = snapset.clones[j];
    prev.intersection_of(snapset.clone_overlap[snapset.clones[j]]);
    if (!missing.is_missing(c) &&
	cmp(c, last_backfill, get_parent()->sort_bitwise()) < 0) {
      dout(10) << "calc_clone_subsets " << soid << " has prev " << c
	       << " overlap " << prev << dendl;
      clone_subsets[c] = prev;
      cloning.union_of(prev);
      break;
    }
    dout(10) << "calc_clone_subsets " << soid << " does not have prev " << c
	     << " overlap " << prev << dendl;
  }

  // overlap with next newest?
  interval_set<uint64_t> next;
  if (size)
    next.insert(0, size);
  for (unsigned j=i+1; j<snapset.clones.size(); j++) {
    hobject_t c = soid;
    c.snap = snapset.clones[j];
    next.intersection_of(snapset.clone_overlap[snapset.clones[j-1]]);
    if (!missing.is_missing(c) &&
	cmp(c, last_backfill, get_parent()->sort_bitwise()) < 0) {
      dout(10) << "calc_clone_subsets " << soid << " has next " << c
	       << " overlap " << next << dendl;
      clone_subsets[c] = next;
      cloning.union_of(next);
      break;
    }
    dout(10) << "calc_clone_subsets " << soid << " does not have next " << c
	     << " overlap " << next << dendl;
  }

  if (cloning.num_intervals() > cct->_conf->osd_recover_clone_overlap_limit) {
    dout(10) << "skipping clone, too many holes" << dendl;
    clone_subsets.clear();
    cloning.clear();
  }


  // what's left for us to push?
  data_subset.subtract(cloning);

  dout(10) << "calc_clone_subsets " << soid
	   << "  data_subset " << data_subset
	   << "  clone_subsets " << clone_subsets << dendl;
}

void ReplicatedBackend::prepare_pull(
  eversion_t v,
  const hobject_t& soid,
  ObjectContextRef headctx,
  RPGHandle *h)
{
  assert(get_parent()->get_local_missing().missing.count(soid));
  eversion_t _v = get_parent()->get_local_missing().missing.find(
    soid)->second.need;
  assert(_v == v);
  const map<hobject_t, set<pg_shard_t>, hobject_t::BitwiseComparator> &missing_loc(
    get_parent()->get_missing_loc_shards());
  const map<pg_shard_t, pg_missing_t > &peer_missing(
    get_parent()->get_shard_missing());
  map<hobject_t, set<pg_shard_t>, hobject_t::BitwiseComparator>::const_iterator q = missing_loc.find(soid);
  assert(q != missing_loc.end());
  assert(!q->second.empty());

  // pick a pullee
  vector<pg_shard_t> shuffle(q->second.begin(), q->second.end());
  random_shuffle(shuffle.begin(), shuffle.end());
  vector<pg_shard_t>::iterator p = shuffle.begin();
  assert(get_osdmap()->is_up(p->osd));
  pg_shard_t fromshard = *p;

  dout(7) << "pull " << soid
	  << " v " << v
	  << " on osds " << *p
	  << " from osd." << fromshard
	  << dendl;

  assert(peer_missing.count(fromshard));
  const pg_missing_t &pmissing = peer_missing.find(fromshard)->second;
  if (pmissing.is_missing(soid, v)) {
    assert(pmissing.missing.find(soid)->second.have != v);
    dout(10) << "pulling soid " << soid << " from osd " << fromshard
	     << " at version " << pmissing.missing.find(soid)->second.have
	     << " rather than at version " << v << dendl;
    v = pmissing.missing.find(soid)->second.have;
    assert(get_parent()->get_log().get_log().objects.count(soid) &&
	   (get_parent()->get_log().get_log().objects.find(soid)->second->op ==
	    pg_log_entry_t::LOST_REVERT) &&
	   (get_parent()->get_log().get_log().objects.find(
	     soid)->second->reverting_to ==
	    v));
  }

  ObjectRecoveryInfo recovery_info;

  if (soid.is_snap()) {
    assert(!get_parent()->get_local_missing().is_missing(
	     soid.get_head()) ||
	   !get_parent()->get_local_missing().is_missing(
	     soid.get_snapdir()));
    assert(headctx);
    // check snapset
    SnapSetContext *ssc = headctx->ssc;
    assert(ssc);
    dout(10) << " snapset " << ssc->snapset << dendl;
    calc_clone_subsets(ssc->snapset, soid, get_parent()->get_local_missing(),
		       get_info().last_backfill,
		       recovery_info.copy_subset,
		       recovery_info.clone_subset);
    // FIXME: this may overestimate if we are pulling multiple clones in parallel...
    dout(10) << " pulling " << recovery_info << dendl;

    assert(ssc->snapset.clone_size.count(soid.snap));
    recovery_info.size = ssc->snapset.clone_size[soid.snap];
  } else {
    // pulling head or unversioned object.
    // always pull the whole thing.
    recovery_info.copy_subset.insert(0, (uint64_t)-1);
    recovery_info.size = ((uint64_t)-1);
  }

  h->pulls[fromshard].push_back(PullOp());
  PullOp &op = h->pulls[fromshard].back();
  op.soid = soid;

  op.recovery_info = recovery_info;
  op.recovery_info.soid = soid;
  op.recovery_info.version = v;
  op.recovery_progress.data_complete = false;
  op.recovery_progress.omap_complete = false;
  op.recovery_progress.data_recovered_to = 0;
  op.recovery_progress.first = true;

  assert(!pulling.count(soid));
  pull_from_peer[fromshard].insert(soid);
  PullInfo &pi = pulling[soid];
  pi.head_ctx = headctx;
  pi.recovery_info = op.recovery_info;
  pi.recovery_progress = op.recovery_progress;
  pi.cache_dont_need = h->cache_dont_need;
}

/*
 * intelligently push an object to a replica.  make use of existing
 * clones/heads and dup data ranges where possible.
 */
void ReplicatedBackend::prep_push_to_replica(
  ObjectContextRef obc, const hobject_t& soid, pg_shard_t peer,
  PushOp *pop, bool cache_dont_need)
{
  const object_info_t& oi = obc->obs.oi;
  uint64_t size = obc->obs.oi.size;

  dout(10) << __func__ << ": " << soid << " v" << oi.version
	   << " size " << size << " to osd." << peer << dendl;

  map<hobject_t, interval_set<uint64_t>, hobject_t::BitwiseComparator> clone_subsets;
  interval_set<uint64_t> data_subset;

  // are we doing a clone on the replica?
  if (soid.snap && soid.snap < CEPH_NOSNAP) {
    hobject_t head = soid;
    head.snap = CEPH_NOSNAP;

    // try to base push off of clones that succeed/preceed poid
    // we need the head (and current SnapSet) locally to do that.
    if (get_parent()->get_local_missing().is_missing(head)) {
      dout(15) << "push_to_replica missing head " << head << ", pushing raw clone" << dendl;
      return prep_push(obc, soid, peer, pop);
    }
    hobject_t snapdir = head;
    snapdir.snap = CEPH_SNAPDIR;
    if (get_parent()->get_local_missing().is_missing(snapdir)) {
      dout(15) << "push_to_replica missing snapdir " << snapdir
	       << ", pushing raw clone" << dendl;
      return prep_push(obc, soid, peer, pop);
    }

    SnapSetContext *ssc = obc->ssc;
    assert(ssc);
    dout(15) << "push_to_replica snapset is " << ssc->snapset << dendl;
    map<pg_shard_t, pg_missing_t>::const_iterator pm =
      get_parent()->get_shard_missing().find(peer);
    assert(pm != get_parent()->get_shard_missing().end());
    map<pg_shard_t, pg_info_t>::const_iterator pi =
      get_parent()->get_shard_info().find(peer);
    assert(pi != get_parent()->get_shard_info().end());
    calc_clone_subsets(ssc->snapset, soid,
		       pm->second,
		       pi->second.last_backfill,
		       data_subset, clone_subsets);
  } else if (soid.snap == CEPH_NOSNAP) {
    // pushing head or unversioned object.
    // base this on partially on replica's clones?
    SnapSetContext *ssc = obc->ssc;
    assert(ssc);
    dout(15) << "push_to_replica snapset is " << ssc->snapset << dendl;
    calc_head_subsets(
      obc,
      ssc->snapset, soid, get_parent()->get_shard_missing().find(peer)->second,
      get_parent()->get_shard_info().find(peer)->second.last_backfill,
      data_subset, clone_subsets);
  }

  prep_push(obc, soid, peer, oi.version, data_subset, clone_subsets, pop, cache_dont_need);
}

void ReplicatedBackend::prep_push(ObjectContextRef obc,
			     const hobject_t& soid, pg_shard_t peer,
			     PushOp *pop)
{
  interval_set<uint64_t> data_subset;
  if (obc->obs.oi.size)
    data_subset.insert(0, obc->obs.oi.size);
  map<hobject_t, interval_set<uint64_t>, hobject_t::BitwiseComparator> clone_subsets;

  prep_push(obc, soid, peer,
	    obc->obs.oi.version, data_subset, clone_subsets,
	    pop);
}

void ReplicatedBackend::prep_push(
  ObjectContextRef obc,
  const hobject_t& soid, pg_shard_t peer,
  eversion_t version,
  interval_set<uint64_t> &data_subset,
  map<hobject_t, interval_set<uint64_t>, hobject_t::BitwiseComparator>& clone_subsets,
  PushOp *pop,
  bool cache_dont_need)
{
  get_parent()->begin_peer_recover(peer, soid);
  // take note.
  PushInfo &pi = pushing[soid][peer];
  pi.obc = obc;
  pi.recovery_info.size = obc->obs.oi.size;
  pi.recovery_info.copy_subset = data_subset;
  pi.recovery_info.clone_subset = clone_subsets;
  pi.recovery_info.soid = soid;
  pi.recovery_info.oi = obc->obs.oi;
  pi.recovery_info.version = version;
  pi.recovery_progress.first = true;
  pi.recovery_progress.data_recovered_to = 0;
  pi.recovery_progress.data_complete = 0;
  pi.recovery_progress.omap_complete = 0;

  ObjectRecoveryProgress new_progress;
  int r = build_push_op(pi.recovery_info,
			pi.recovery_progress,
			&new_progress,
			pop,
			&(pi.stat), cache_dont_need);
  assert(r == 0);
  pi.recovery_progress = new_progress;
}

int ReplicatedBackend::send_pull_legacy(int prio, pg_shard_t peer,
					const ObjectRecoveryInfo &recovery_info,
					ObjectRecoveryProgress progress)
{
  // send op
  ceph_tid_t tid = get_parent()->get_tid();
  osd_reqid_t rid(get_parent()->get_cluster_msgr_name(), 0, tid);

  dout(10) << "send_pull_op " << recovery_info.soid << " "
	   << recovery_info.version
	   << " first=" << progress.first
	   << " data " << recovery_info.copy_subset
	   << " from osd." << peer
	   << " tid " << tid << dendl;

  MOSDSubOp *subop = new MOSDSubOp(
    rid, parent->whoami_shard(),
    get_info().pgid, recovery_info.soid,
    CEPH_OSD_FLAG_ACK,
    get_osdmap()->get_epoch(), tid,
    recovery_info.version);
  subop->set_priority(prio);
  subop->ops = vector<OSDOp>(1);
  subop->ops[0].op.op = CEPH_OSD_OP_PULL;
  subop->ops[0].op.extent.length = cct->_conf->osd_recovery_max_chunk;
  subop->recovery_info = recovery_info;
  subop->recovery_progress = progress;

  get_parent()->send_message_osd_cluster(
    peer.osd, subop, get_osdmap()->get_epoch());

  get_parent()->get_logger()->inc(l_osd_pull);
  return 0;
}

void ReplicatedBackend::submit_push_data(
  ObjectRecoveryInfo &recovery_info,
  bool first,
  bool complete,
  bool cache_dont_need,
  const interval_set<uint64_t> &intervals_included,
  bufferlist data_included,
  bufferlist omap_header,
  map<string, bufferlist> &attrs,
  map<string, bufferlist> &omap_entries,
  ObjectStore::Transaction *t)
{
  hobject_t target_oid;
  if (first && complete) {
    target_oid = recovery_info.soid;
  } else {
    target_oid = get_parent()->get_temp_recovery_object(recovery_info.version,
							recovery_info.soid.snap);
    if (first) {
      dout(10) << __func__ << ": Adding oid "
	       << target_oid << " in the temp collection" << dendl;
      add_temp_obj(target_oid);
    }
  }

  if (first) {
    t->remove(coll, ghobject_t(target_oid));
    t->touch(coll, ghobject_t(target_oid));
    t->truncate(coll, ghobject_t(target_oid), recovery_info.size);
    t->omap_setheader(coll, ghobject_t(target_oid), omap_header);
  }
  uint64_t off = 0;
  uint32_t fadvise_flags = CEPH_OSD_OP_FLAG_FADVISE_SEQUENTIAL;
  if (cache_dont_need)
    fadvise_flags |= CEPH_OSD_OP_FLAG_FADVISE_DONTNEED;
  for (interval_set<uint64_t>::const_iterator p = intervals_included.begin();
       p != intervals_included.end();
       ++p) {
    bufferlist bit;
    bit.substr_of(data_included, off, p.get_len());
    t->write(coll, ghobject_t(target_oid),
	     p.get_start(), p.get_len(), bit, fadvise_flags);
    off += p.get_len();
  }

  if (!omap_entries.empty())
    t->omap_setkeys(coll, ghobject_t(target_oid), omap_entries);
  if (!attrs.empty())
    t->setattrs(coll, ghobject_t(target_oid), attrs);

  if (complete) {
    if (!first) {
      dout(10) << __func__ << ": Removing oid "
	       << target_oid << " from the temp collection" << dendl;
      clear_temp_obj(target_oid);
      t->remove(coll, ghobject_t(recovery_info.soid));
      t->collection_move_rename(coll, ghobject_t(target_oid),
				coll, ghobject_t(recovery_info.soid));
    }

    submit_push_complete(recovery_info, t);
  }
}

void ReplicatedBackend::submit_push_complete(ObjectRecoveryInfo &recovery_info,
					     ObjectStore::Transaction *t)
{
  for (map<hobject_t, interval_set<uint64_t>, hobject_t::BitwiseComparator>::const_iterator p =
	 recovery_info.clone_subset.begin();
       p != recovery_info.clone_subset.end();
       ++p) {
    for (interval_set<uint64_t>::const_iterator q = p->second.begin();
	 q != p->second.end();
	 ++q) {
      dout(15) << " clone_range " << p->first << " "
	       << q.get_start() << "~" << q.get_len() << dendl;
      t->clone_range(coll, ghobject_t(p->first), ghobject_t(recovery_info.soid),
		     q.get_start(), q.get_len(), q.get_start());
    }
  }
}

ObjectRecoveryInfo ReplicatedBackend::recalc_subsets(
  const ObjectRecoveryInfo& recovery_info,
  SnapSetContext *ssc)
{
  if (!recovery_info.soid.snap || recovery_info.soid.snap >= CEPH_NOSNAP)
    return recovery_info;
  ObjectRecoveryInfo new_info = recovery_info;
  new_info.copy_subset.clear();
  new_info.clone_subset.clear();
  assert(ssc);
  calc_clone_subsets(ssc->snapset, new_info.soid, get_parent()->get_local_missing(),
		     get_info().last_backfill,
		     new_info.copy_subset, new_info.clone_subset);
  return new_info;
}

bool ReplicatedBackend::handle_pull_response(
  pg_shard_t from, PushOp &pop, PullOp *response,
  list<hobject_t> *to_continue,
  ObjectStore::Transaction *t
  )
{
  interval_set<uint64_t> data_included = pop.data_included;
  bufferlist data;
  data.claim(pop.data);
  dout(10) << "handle_pull_response "
	   << pop.recovery_info
	   << pop.after_progress
	   << " data.size() is " << data.length()
	   << " data_included: " << data_included
	   << dendl;
  if (pop.version == eversion_t()) {
    // replica doesn't have it!
    _failed_push(from, pop.soid);
    return false;
  }

  hobject_t &hoid = pop.soid;
  assert((data_included.empty() && data.length() == 0) ||
	 (!data_included.empty() && data.length() > 0));

  if (!pulling.count(hoid)) {
    return false;
  }

  PullInfo &pi = pulling[hoid];
  if (pi.recovery_info.size == (uint64_t(-1))) {
    pi.recovery_info.size = pop.recovery_info.size;
    pi.recovery_info.copy_subset.intersection_of(
      pop.recovery_info.copy_subset);
  }

  bool first = pi.recovery_progress.first;
  if (first) {
    // attrs only reference the origin bufferlist (decode from MOSDPGPush message)
    // whose size is much greater than attrs in recovery. If obc cache it (get_obc maybe
    // cache the attr), this causes the whole origin bufferlist would not be free until
    // obc is evicted from obc cache. So rebuild the bufferlist before cache it.
    for (map<string, bufferlist>::iterator it = pop.attrset.begin();
         it != pop.attrset.end();
         ++it) {
      it->second.rebuild();
    }
    pi.obc = get_parent()->get_obc(pi.recovery_info.soid, pop.attrset);
    pi.recovery_info.oi = pi.obc->obs.oi;
    pi.recovery_info = recalc_subsets(pi.recovery_info, pi.obc->ssc);
  }


  interval_set<uint64_t> usable_intervals;
  bufferlist usable_data;
  trim_pushed_data(pi.recovery_info.copy_subset,
		   data_included,
		   data,
		   &usable_intervals,
		   &usable_data);
  data_included = usable_intervals;
  data.claim(usable_data);


  pi.recovery_progress = pop.after_progress;

  pi.stat.num_bytes_recovered += data.length();

  dout(10) << "new recovery_info " << pi.recovery_info
	   << ", new progress " << pi.recovery_progress
	   << dendl;

  bool complete = pi.is_complete();

  submit_push_data(pi.recovery_info, first,
		   complete, pi.cache_dont_need,
		   data_included, data,
		   pop.omap_header,
		   pop.attrset,
		   pop.omap_entries,
		   t);

  pi.stat.num_keys_recovered += pop.omap_entries.size();

  if (complete) {
    to_continue->push_back(hoid);
    pi.stat.num_objects_recovered++;
    get_parent()->on_local_recover(
      hoid, pi.stat, pi.recovery_info, pi.obc, t);
    pull_from_peer[from].erase(hoid);
    if (pull_from_peer[from].empty())
      pull_from_peer.erase(from);
    return false;
  } else {
    response->soid = pop.soid;
    response->recovery_info = pi.recovery_info;
    response->recovery_progress = pi.recovery_progress;
    return true;
  }
}

void ReplicatedBackend::handle_push(
  pg_shard_t from, PushOp &pop, PushReplyOp *response,
  ObjectStore::Transaction *t)
{
  dout(10) << "handle_push "
	   << pop.recovery_info
	   << pop.after_progress
	   << dendl;
  bufferlist data;
  data.claim(pop.data);
  bool first = pop.before_progress.first;
  bool complete = pop.after_progress.data_complete &&
    pop.after_progress.omap_complete;

  response->soid = pop.recovery_info.soid;
  submit_push_data(pop.recovery_info,
		   first,
		   complete,
		   true, // must be replicate
		   pop.data_included,
		   data,
		   pop.omap_header,
		   pop.attrset,
		   pop.omap_entries,
		   t);

  if (complete)
    get_parent()->on_local_recover(
      pop.recovery_info.soid,
      object_stat_sum_t(),
      pop.recovery_info,
      ObjectContextRef(), // ok, is replica
      t);
}

void ReplicatedBackend::send_pushes(int prio, map<pg_shard_t, vector<PushOp> > &pushes)
{
  for (map<pg_shard_t, vector<PushOp> >::iterator i = pushes.begin();
       i != pushes.end();
       ++i) {
    ConnectionRef con = get_parent()->get_con_osd_cluster(
      i->first.osd,
      get_osdmap()->get_epoch());
    if (!con)
      continue;
    vector<PushOp>::iterator j = i->second.begin();
    while (j != i->second.end()) {
      uint64_t cost = 0;
      uint64_t pushes = 0;
      MOSDPGPush *msg = new MOSDPGPush();
      msg->from = get_parent()->whoami_shard();
      msg->pgid = get_parent()->primary_spg_t();
      msg->map_epoch = get_osdmap()->get_epoch();
      msg->set_priority(prio);
      for (;
           (j != i->second.end() &&
	    cost < cct->_conf->osd_max_push_cost &&
	    pushes < cct->_conf->osd_max_push_objects) ;
	   ++j) {
	dout(20) << __func__ << ": sending push " << *j
		 << " to osd." << i->first << dendl;
	cost += j->cost(cct);
	pushes += 1;
	msg->pushes.push_back(*j);
      }
      msg->compute_cost(cct);
      get_parent()->send_message_osd_cluster(msg, con);
    }
  }
}

void ReplicatedBackend::send_pulls(int prio, map<pg_shard_t, vector<PullOp> > &pulls)
{
  for (map<pg_shard_t, vector<PullOp> >::iterator i = pulls.begin();
       i != pulls.end();
       ++i) {
    ConnectionRef con = get_parent()->get_con_osd_cluster(
      i->first.osd,
      get_osdmap()->get_epoch());
    if (!con)
      continue;
    dout(20) << __func__ << ": sending pulls " << i->second
	     << " to osd." << i->first << dendl;
    MOSDPGPull *msg = new MOSDPGPull();
    msg->from = parent->whoami_shard();
    msg->set_priority(prio);
    msg->pgid = get_parent()->primary_spg_t();
    msg->map_epoch = get_osdmap()->get_epoch();
    msg->pulls.swap(i->second);
    msg->compute_cost(cct);
    get_parent()->send_message_osd_cluster(msg, con);
  }
}

int ReplicatedBackend::build_push_op(const ObjectRecoveryInfo &recovery_info,
				     const ObjectRecoveryProgress &progress,
				     ObjectRecoveryProgress *out_progress,
				     PushOp *out_op,
				     object_stat_sum_t *stat,
                                     bool cache_dont_need)
{
  ObjectRecoveryProgress _new_progress;
  if (!out_progress)
    out_progress = &_new_progress;
  ObjectRecoveryProgress &new_progress = *out_progress;
  new_progress = progress;

  dout(7) << "send_push_op " << recovery_info.soid
	  << " v " << recovery_info.version
	  << " size " << recovery_info.size
	  << " recovery_info: " << recovery_info
          << dendl;

  if (progress.first) {
    store->omap_get_header(coll, ghobject_t(recovery_info.soid), &out_op->omap_header);
    store->getattrs(coll, ghobject_t(recovery_info.soid), out_op->attrset);

    // Debug
    bufferlist bv = out_op->attrset[OI_ATTR];
    object_info_t oi(bv);

    if (oi.version != recovery_info.version) {
      get_parent()->clog_error() << get_info().pgid << " push "
				 << recovery_info.soid << " v "
				 << recovery_info.version
				 << " failed because local copy is "
				 << oi.version << "\n";
      return -EINVAL;
    }

    new_progress.first = false;
  }

  uint64_t available = cct->_conf->osd_recovery_max_chunk;
  if (!progress.omap_complete) {
    ObjectMap::ObjectMapIterator iter =
      store->get_omap_iterator(coll,
			       ghobject_t(recovery_info.soid));
    for (iter->lower_bound(progress.omap_recovered_to);
	 iter->valid();
	 iter->next(false)) {
      if (!out_op->omap_entries.empty() &&
	  available <= (iter->key().size() + iter->value().length()))
	break;
      out_op->omap_entries.insert(make_pair(iter->key(), iter->value()));

      if ((iter->key().size() + iter->value().length()) <= available)
	available -= (iter->key().size() + iter->value().length());
      else
	available = 0;
    }
    if (!iter->valid())
      new_progress.omap_complete = true;
    else
      new_progress.omap_recovered_to = iter->key();
  }

  if (available > 0) {
    if (!recovery_info.copy_subset.empty()) {
      interval_set<uint64_t> copy_subset = recovery_info.copy_subset;
      bufferlist bl;
      int r = store->fiemap(coll, ghobject_t(recovery_info.soid), 0,
                            copy_subset.range_end(), bl);
      if (r >= 0)  {
        interval_set<uint64_t> fiemap_included;
        map<uint64_t, uint64_t> m;
        bufferlist::iterator iter = bl.begin();
        ::decode(m, iter);
        map<uint64_t, uint64_t>::iterator miter;
        for (miter = m.begin(); miter != m.end(); ++miter) {
          fiemap_included.insert(miter->first, miter->second);
        }

        copy_subset.intersection_of(fiemap_included);
      }
      out_op->data_included.span_of(copy_subset, progress.data_recovered_to,
                                    available);
      if (out_op->data_included.empty()) // zero filled section, skip to end!
        new_progress.data_recovered_to = recovery_info.copy_subset.range_end();
      else
        new_progress.data_recovered_to = out_op->data_included.range_end();
    }
  } else {
    out_op->data_included.clear();
  }

  for (interval_set<uint64_t>::iterator p = out_op->data_included.begin();
       p != out_op->data_included.end();
       ++p) {
    bufferlist bit;
    store->read(coll, ghobject_t(recovery_info.soid),
		p.get_start(), p.get_len(), bit,
                cache_dont_need ? CEPH_OSD_OP_FLAG_FADVISE_DONTNEED: 0);
    if (p.get_len() != bit.length()) {
      dout(10) << " extent " << p.get_start() << "~" << p.get_len()
	       << " is actually " << p.get_start() << "~" << bit.length()
	       << dendl;
      interval_set<uint64_t>::iterator save = p++;
      if (bit.length() == 0)
        out_op->data_included.erase(save);     //Remove this empty interval
      else
        save.set_len(bit.length());
      // Remove any other intervals present
      while (p != out_op->data_included.end()) {
        interval_set<uint64_t>::iterator save = p++;
        out_op->data_included.erase(save);
      }
      new_progress.data_complete = true;
      out_op->data.claim_append(bit);
      break;
    }
    out_op->data.claim_append(bit);
  }

  if (new_progress.is_complete(recovery_info)) {
    new_progress.data_complete = true;
    if (stat)
      stat->num_objects_recovered++;
  }

  if (stat) {
    stat->num_keys_recovered += out_op->omap_entries.size();
    stat->num_bytes_recovered += out_op->data.length();
  }

  get_parent()->get_logger()->inc(l_osd_push);
  get_parent()->get_logger()->inc(l_osd_push_outb, out_op->data.length());

  // send
  out_op->version = recovery_info.version;
  out_op->soid = recovery_info.soid;
  out_op->recovery_info = recovery_info;
  out_op->after_progress = new_progress;
  out_op->before_progress = progress;
  return 0;
}

int ReplicatedBackend::send_push_op_legacy(int prio, pg_shard_t peer, PushOp &pop)
{
  ceph_tid_t tid = get_parent()->get_tid();
  osd_reqid_t rid(get_parent()->get_cluster_msgr_name(), 0, tid);
  MOSDSubOp *subop = new MOSDSubOp(
    rid, parent->whoami_shard(),
    spg_t(get_info().pgid.pgid, peer.shard), pop.soid,
    0, get_osdmap()->get_epoch(),
    tid, pop.recovery_info.version);
  subop->ops = vector<OSDOp>(1);
  subop->ops[0].op.op = CEPH_OSD_OP_PUSH;

  subop->set_priority(prio);
  subop->version = pop.version;
  subop->ops[0].indata.claim(pop.data);
  subop->data_included.swap(pop.data_included);
  subop->omap_header.claim(pop.omap_header);
  subop->omap_entries.swap(pop.omap_entries);
  subop->attrset.swap(pop.attrset);
  subop->recovery_info = pop.recovery_info;
  subop->current_progress = pop.before_progress;
  subop->recovery_progress = pop.after_progress;

  get_parent()->send_message_osd_cluster(peer.osd, subop, get_osdmap()->get_epoch());
  return 0;
}

void ReplicatedBackend::prep_push_op_blank(const hobject_t& soid, PushOp *op)
{
  op->recovery_info.version = eversion_t();
  op->version = eversion_t();
  op->soid = soid;
}

void ReplicatedBackend::sub_op_push_reply(OpRequestRef op)
{
  MOSDSubOpReply *reply = static_cast<MOSDSubOpReply*>(op->get_req());
  const hobject_t& soid = reply->get_poid();
  assert(reply->get_type() == MSG_OSD_SUBOPREPLY);
  dout(10) << "sub_op_push_reply from " << reply->get_source() << " " << *reply << dendl;
  pg_shard_t peer = reply->from;

  op->mark_started();

  PushReplyOp rop;
  rop.soid = soid;
  PushOp pop;
  bool more = handle_push_reply(peer, rop, &pop);
  if (more)
    send_push_op_legacy(op->get_req()->get_priority(), peer, pop);
}

bool ReplicatedBackend::handle_push_reply(pg_shard_t peer, PushReplyOp &op, PushOp *reply)
{
  const hobject_t &soid = op.soid;
  if (pushing.count(soid) == 0) {
    dout(10) << "huh, i wasn't pushing " << soid << " to osd." << peer
	     << ", or anybody else"
	     << dendl;
    return false;
  } else if (pushing[soid].count(peer) == 0) {
    dout(10) << "huh, i wasn't pushing " << soid << " to osd." << peer
	     << dendl;
    return false;
  } else {
    PushInfo *pi = &pushing[soid][peer];

    if (!pi->recovery_progress.data_complete) {
      dout(10) << " pushing more from, "
	       << pi->recovery_progress.data_recovered_to
	       << " of " << pi->recovery_info.copy_subset << dendl;
      ObjectRecoveryProgress new_progress;
      int r = build_push_op(
	pi->recovery_info,
	pi->recovery_progress, &new_progress, reply,
	&(pi->stat));
      assert(r == 0);
      pi->recovery_progress = new_progress;
      return true;
    } else {
      // done!
      get_parent()->on_peer_recover(
	peer, soid, pi->recovery_info,
	pi->stat);

      pushing[soid].erase(peer);
      pi = NULL;


      if (pushing[soid].empty()) {
	get_parent()->on_global_recover(soid);
	pushing.erase(soid);
      } else {
	dout(10) << "pushed " << soid << ", still waiting for push ack from "
		 << pushing[soid].size() << " others" << dendl;
      }
      return false;
    }
  }
}

/** op_pull
 * process request to pull an entire object.
 * NOTE: called from opqueue.
 */
void ReplicatedBackend::sub_op_pull(OpRequestRef op)
{
  MOSDSubOp *m = static_cast<MOSDSubOp*>(op->get_req());
  assert(m->get_type() == MSG_OSD_SUBOP);

  op->mark_started();

  const hobject_t soid = m->poid;

  dout(7) << "pull" << soid << " v " << m->version
          << " from " << m->get_source()
          << dendl;

  assert(!is_primary());  // we should be a replica or stray.

  PullOp pop;
  pop.soid = soid;
  pop.recovery_info = m->recovery_info;
  pop.recovery_progress = m->recovery_progress;

  PushOp reply;
  handle_pull(m->from, pop, &reply);
  send_push_op_legacy(
    m->get_priority(),
    m->from,
    reply);

  log_subop_stats(get_parent()->get_logger(), op, l_osd_sop_pull);
}

void ReplicatedBackend::handle_pull(pg_shard_t peer, PullOp &op, PushOp *reply)
{
  const hobject_t &soid = op.soid;
  struct stat st;
  int r = store->stat(coll, ghobject_t(soid), &st);
  if (r != 0) {
    get_parent()->clog_error() << get_info().pgid << " "
			       << peer << " tried to pull " << soid
			       << " but got " << cpp_strerror(-r) << "\n";
    prep_push_op_blank(soid, reply);
  } else {
    ObjectRecoveryInfo &recovery_info = op.recovery_info;
    ObjectRecoveryProgress &progress = op.recovery_progress;
    if (progress.first && recovery_info.size == ((uint64_t)-1)) {
      // Adjust size and copy_subset
      recovery_info.size = st.st_size;
      recovery_info.copy_subset.clear();
      if (st.st_size)
        recovery_info.copy_subset.insert(0, st.st_size);
      assert(recovery_info.clone_subset.empty());
    }

    r = build_push_op(recovery_info, progress, 0, reply);
    if (r < 0)
      prep_push_op_blank(soid, reply);
  }
}

/**
 * trim received data to remove what we don't want
 *
 * @param copy_subset intervals we want
 * @param data_included intervals we got
 * @param data_recieved data we got
 * @param intervals_usable intervals we want to keep
 * @param data_usable matching data we want to keep
 */
void ReplicatedBackend::trim_pushed_data(
  const interval_set<uint64_t> &copy_subset,
  const interval_set<uint64_t> &intervals_received,
  bufferlist data_received,
  interval_set<uint64_t> *intervals_usable,
  bufferlist *data_usable)
{
  if (intervals_received.subset_of(copy_subset)) {
    *intervals_usable = intervals_received;
    *data_usable = data_received;
    return;
  }

  intervals_usable->intersection_of(copy_subset,
				    intervals_received);

  uint64_t off = 0;
  for (interval_set<uint64_t>::const_iterator p = intervals_received.begin();
       p != intervals_received.end();
       ++p) {
    interval_set<uint64_t> x;
    x.insert(p.get_start(), p.get_len());
    x.intersection_of(copy_subset);
    for (interval_set<uint64_t>::const_iterator q = x.begin();
	 q != x.end();
	 ++q) {
      bufferlist sub;
      uint64_t data_off = off + (q.get_start() - p.get_start());
      sub.substr_of(data_received, data_off, q.get_len());
      data_usable->claim_append(sub);
    }
    off += p.get_len();
  }
}

/** op_push
 * NOTE: called from opqueue.
 */
void ReplicatedBackend::sub_op_push(OpRequestRef op)
{
  op->mark_started();
  MOSDSubOp *m = static_cast<MOSDSubOp *>(op->get_req());

  PushOp pop;
  pop.soid = m->recovery_info.soid;
  pop.version = m->version;
  m->claim_data(pop.data);
  pop.data_included.swap(m->data_included);
  pop.omap_header.swap(m->omap_header);
  pop.omap_entries.swap(m->omap_entries);
  pop.attrset.swap(m->attrset);
  pop.recovery_info = m->recovery_info;
  pop.before_progress = m->current_progress;
  pop.after_progress = m->recovery_progress;
  ObjectStore::Transaction *t = new ObjectStore::Transaction;

  if (is_primary()) {
    PullOp resp;
    RPGHandle *h = _open_recovery_op();
    list<hobject_t> to_continue;
    bool more = handle_pull_response(
      m->from, pop, &resp,
      &to_continue, t);
    if (more) {
      send_pull_legacy(
	m->get_priority(),
	m->from,
	resp.recovery_info,
	resp.recovery_progress);
    } else {
      C_ReplicatedBackend_OnPullComplete *c =
	new C_ReplicatedBackend_OnPullComplete(
	  this,
	  op->get_req()->get_priority());
      c->to_continue.swap(to_continue);
      t->register_on_complete(
	new PG_RecoveryQueueAsync(
	  get_parent(),
	  get_parent()->bless_gencontext(c)));
    }
    run_recovery_op(h, op->get_req()->get_priority());
  } else {
    PushReplyOp resp;
    MOSDSubOpReply *reply = new MOSDSubOpReply(
      m, parent->whoami_shard(), 0,
      get_osdmap()->get_epoch(), CEPH_OSD_FLAG_ACK);
    reply->set_priority(m->get_priority());
    assert(entity_name_t::TYPE_OSD == m->get_connection()->peer_type);
    handle_push(m->from, pop, &resp, t);
    t->register_on_complete(new PG_SendMessageOnConn(
			      get_parent(), reply, m->get_connection()));
  }
  t->register_on_applied(
    new ObjectStore::C_DeleteTransaction(t));
  get_parent()->queue_transaction(t);
  return;
}

void ReplicatedBackend::_failed_push(pg_shard_t from, const hobject_t &soid)
{
  get_parent()->failed_push(from, soid);
  pull_from_peer[from].erase(soid);
  if (pull_from_peer[from].empty())
    pull_from_peer.erase(from);
  pulling.erase(soid);
}

int ReplicatedBackend::start_pushes(
  const hobject_t &soid,
  ObjectContextRef obc,
  RPGHandle *h)
{
  int pushes = 0;
  // who needs it?
  assert(get_parent()->get_actingbackfill_shards().size() > 0);
  for (set<pg_shard_t>::iterator i =
	 get_parent()->get_actingbackfill_shards().begin();
       i != get_parent()->get_actingbackfill_shards().end();
       ++i) {
    if (*i == get_parent()->whoami_shard()) continue;
    pg_shard_t peer = *i;
    map<pg_shard_t, pg_missing_t>::const_iterator j =
      get_parent()->get_shard_missing().find(peer);
    assert(j != get_parent()->get_shard_missing().end());
    if (j->second.is_missing(soid)) {
      ++pushes;
      h->pushes[peer].push_back(PushOp());
      prep_push_to_replica(obc, soid, peer,
			   &(h->pushes[peer].back()), h->cache_dont_need);
    }
  }
  return pushes;
}
