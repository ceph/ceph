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

#include <boost/variant.hpp>
#include <boost/optional/optional_io.hpp>
#include <iostream>
#include <sstream>

#include "ECUtil.h"
#include "ECBackend.h"
#include "messages/MOSDPGPush.h"
#include "messages/MOSDPGPushReply.h"
#include "ReplicatedPG.h"

class ReplicatedPG;

#define dout_subsys ceph_subsys_osd
#define DOUT_PREFIX_ARGS this
#undef dout_prefix
#define dout_prefix _prefix(_dout, this)
static ostream& _prefix(std::ostream *_dout, ECBackend *pgb) {
  return *_dout << pgb->get_parent()->gen_dbg_prefix();
}

struct ECRecoveryHandle : public PGBackend::RecoveryHandle {
  list<ECBackend::RecoveryOp> ops;
};

static ostream &operator<<(ostream &lhs, const map<pg_shard_t, bufferlist> &rhs)
{
  lhs << "[";
  for (map<pg_shard_t, bufferlist>::const_iterator i = rhs.begin();
       i != rhs.end();
       ++i) {
    if (i != rhs.begin())
      lhs << ", ";
    lhs << make_pair(i->first, i->second.length());
  }
  return lhs << "]";
}

static ostream &operator<<(ostream &lhs, const map<int, bufferlist> &rhs)
{
  lhs << "[";
  for (map<int, bufferlist>::const_iterator i = rhs.begin();
       i != rhs.end();
       ++i) {
    if (i != rhs.begin())
      lhs << ", ";
    lhs << make_pair(i->first, i->second.length());
  }
  return lhs << "]";
}

static ostream &operator<<(
  ostream &lhs,
  const boost::tuple<uint64_t, uint64_t, map<pg_shard_t, bufferlist> > &rhs)
{
  return lhs << "(" << rhs.get<0>() << ", "
	     << rhs.get<1>() << ", " << rhs.get<2>() << ")";
}

ostream &operator<<(ostream &lhs, const ECBackend::read_request_t &rhs)
{
  return lhs << "read_request_t(to_read=[" << rhs.to_read << "]"
	     << ", need=" << rhs.need
	     << ", want_attrs=" << rhs.want_attrs
	     << ")";
}

ostream &operator<<(ostream &lhs, const ECBackend::read_result_t &rhs)
{
  lhs << "read_result_t(r=" << rhs.r
      << ", errors=" << rhs.errors;
  if (rhs.attrs) {
    lhs << ", attrs=" << rhs.attrs.get();
  } else {
    lhs << ", noattrs";
  }
  return lhs << ", returned=" << rhs.returned;
}

ostream &operator<<(ostream &lhs, const ECBackend::ReadOp &rhs)
{
  lhs << "ReadOp(tid=" << rhs.tid;
  if (rhs.op && rhs.op->get_req()) {
    lhs << ", op=";
    rhs.op->get_req()->print(lhs);
  }
  return lhs << ", to_read=" << rhs.to_read
	     << ", complete=" << rhs.complete
	     << ", priority=" << rhs.priority
	     << ", obj_to_source=" << rhs.obj_to_source
	     << ", source_to_obj=" << rhs.source_to_obj
	     << ", in_progress=" << rhs.in_progress << ")";
}

void ECBackend::ReadOp::dump(Formatter *f) const
{
  f->dump_unsigned("tid", tid);
  if (op && op->get_req()) {
    f->dump_stream("op") << *(op->get_req());
  }
  f->dump_stream("to_read") << to_read;
  f->dump_stream("complete") << complete;
  f->dump_int("priority", priority);
  f->dump_stream("obj_to_source") << obj_to_source;
  f->dump_stream("source_to_obj") << source_to_obj;
  f->dump_stream("in_progress") << in_progress;
}

ostream &operator<<(ostream &lhs, const ECBackend::Op &rhs)
{
  lhs << "Op(" << rhs.hoid
      << " v=" << rhs.version
      << " tt=" << rhs.trim_to
      << " tid=" << rhs.tid
      << " reqid=" << rhs.reqid;
  if (rhs.client_op && rhs.client_op->get_req()) {
    lhs << " client_op=";
    rhs.client_op->get_req()->print(lhs);
  }
  lhs << " pending_commit=" << rhs.pending_commit
      << " pending_apply=" << rhs.pending_apply
      << ")";
  return lhs;
}

ostream &operator<<(ostream &lhs, const ECBackend::RecoveryOp &rhs)
{
  return lhs << "RecoveryOp("
	     << "hoid=" << rhs.hoid
	     << " v=" << rhs.v
	     << " missing_on=" << rhs.missing_on
	     << " missing_on_shards=" << rhs.missing_on_shards
	     << " recovery_info=" << rhs.recovery_info
	     << " recovery_progress=" << rhs.recovery_progress
	     << " pending_read=" << rhs.pending_read
	     << " obc refcount=" << rhs.obc.use_count()
	     << " state=" << ECBackend::RecoveryOp::tostr(rhs.state)
	     << " waiting_on_pushes=" << rhs.waiting_on_pushes
	     << " extent_requested=" << rhs.extent_requested;
}

void ECBackend::RecoveryOp::dump(Formatter *f) const
{
  f->dump_stream("hoid") << hoid;
  f->dump_stream("v") << v;
  f->dump_stream("missing_on") << missing_on;
  f->dump_stream("missing_on_shards") << missing_on_shards;
  f->dump_stream("recovery_info") << recovery_info;
  f->dump_stream("recovery_progress") << recovery_progress;
  f->dump_bool("pending_read", pending_read);
  f->dump_stream("state") << tostr(state);
  f->dump_stream("waiting_on_pushes") << waiting_on_pushes;
  f->dump_stream("extent_requested") << extent_requested;
}

ECBackend::ECBackend(
  PGBackend::Listener *pg,
  coll_t coll,
  coll_t temp_coll,
  ObjectStore *store,
  CephContext *cct,
  ErasureCodeInterfaceRef ec_impl,
  uint64_t stripe_width)
  : PGBackend(pg, store, coll, temp_coll),
    cct(cct),
    ec_impl(ec_impl),
    sinfo(ec_impl->get_data_chunk_count(), stripe_width) {
  assert((ec_impl->get_data_chunk_count() *
	  ec_impl->get_chunk_size(stripe_width)) == stripe_width);
}

PGBackend::RecoveryHandle *ECBackend::open_recovery_op()
{
  return new ECRecoveryHandle;
}

struct OnRecoveryReadComplete :
  public GenContext<pair<RecoveryMessages*, ECBackend::read_result_t& > &> {
  ECBackend *pg;
  hobject_t hoid;
  set<int> want;
  OnRecoveryReadComplete(ECBackend *pg, const hobject_t &hoid)
    : pg(pg), hoid(hoid) {}
  void finish(pair<RecoveryMessages *, ECBackend::read_result_t &> &in) {
    ECBackend::read_result_t &res = in.second;
    assert(res.r == 0);
    assert(res.errors.empty());
    assert(res.returned.size() == 1);
    pg->handle_recovery_read_complete(
      hoid,
      res.returned.back(),
      res.attrs,
      in.first);
  }
};

struct RecoveryMessages {
  map<hobject_t,
      ECBackend::read_request_t> reads;
  void read(
    ECBackend *ec,
    const hobject_t &hoid, uint64_t off, uint64_t len,
    const set<pg_shard_t> &need,
    bool attrs) {
    list<boost::tuple<uint64_t, uint64_t, uint32_t> > to_read;
    to_read.push_back(boost::make_tuple(off, len, 0));
    assert(!reads.count(hoid));
    reads.insert(
      make_pair(
	hoid,
	ECBackend::read_request_t(
	  hoid,
	  to_read,
	  need,
	  attrs,
	  new OnRecoveryReadComplete(
	    ec,
	    hoid))));
  }

  map<pg_shard_t, vector<PushOp> > pushes;
  map<pg_shard_t, vector<PushReplyOp> > push_replies;
  ObjectStore::Transaction *t;
  RecoveryMessages() : t(new ObjectStore::Transaction) {}
  ~RecoveryMessages() { assert(!t); }
};

void ECBackend::handle_recovery_push(
  PushOp &op,
  RecoveryMessages *m)
{
  bool oneshot = op.before_progress.first && op.after_progress.data_complete;
  coll_t tcoll = oneshot ? coll : get_temp_coll(m->t);
  if (op.before_progress.first) {
    get_parent()->on_local_recover_start(
      op.soid,
      m->t);
    m->t->remove(
      get_temp_coll(m->t),
      ghobject_t(
	op.soid, ghobject_t::NO_GEN, get_parent()->whoami_shard().shard));
    m->t->touch(
      tcoll,
      ghobject_t(
	op.soid, ghobject_t::NO_GEN, get_parent()->whoami_shard().shard));
  }

  if (!op.data_included.empty()) {
    uint64_t start = op.data_included.range_start();
    uint64_t end = op.data_included.range_end();
    assert(op.data.length() == (end - start));

    m->t->write(
      tcoll,
      ghobject_t(
	op.soid, ghobject_t::NO_GEN, get_parent()->whoami_shard().shard),
      start,
      op.data.length(),
      op.data);
  } else {
    assert(op.data.length() == 0);
  }

  if (op.before_progress.first) {
    if (!oneshot)
      add_temp_obj(op.soid);
    assert(op.attrset.count(string("_")));
    m->t->setattrs(
      tcoll,
      ghobject_t(
	op.soid, ghobject_t::NO_GEN, get_parent()->whoami_shard().shard),
      op.attrset);
  }

  if (op.after_progress.data_complete && !oneshot) {
    clear_temp_obj(op.soid);
    m->t->collection_move(
      coll,
      tcoll,
      ghobject_t(
	op.soid, ghobject_t::NO_GEN, get_parent()->whoami_shard().shard));
  }
  if (op.after_progress.data_complete) {
    if ((get_parent()->pgb_is_primary())) {
      assert(recovery_ops.count(op.soid));
      assert(recovery_ops[op.soid].obc);
      object_stat_sum_t stats;
      stats.num_objects_recovered = 1;
      stats.num_bytes_recovered = recovery_ops[op.soid].obc->obs.oi.size;
      get_parent()->on_local_recover(
	op.soid,
	stats,
	op.recovery_info,
	recovery_ops[op.soid].obc,
	m->t);
    } else {
      get_parent()->on_local_recover(
	op.soid,
	object_stat_sum_t(),
	op.recovery_info,
	ObjectContextRef(),
	m->t);
    }
  }
  m->push_replies[get_parent()->primary_shard()].push_back(PushReplyOp());
  m->push_replies[get_parent()->primary_shard()].back().soid = op.soid;
}

void ECBackend::handle_recovery_push_reply(
  PushReplyOp &op,
  pg_shard_t from,
  RecoveryMessages *m)
{
  if (!recovery_ops.count(op.soid))
    return;
  RecoveryOp &rop = recovery_ops[op.soid];
  assert(rop.waiting_on_pushes.count(from));
  rop.waiting_on_pushes.erase(from);
  continue_recovery_op(rop, m);
}

void ECBackend::handle_recovery_read_complete(
  const hobject_t &hoid,
  boost::tuple<uint64_t, uint64_t, map<pg_shard_t, bufferlist> > &to_read,
  boost::optional<map<string, bufferlist> > attrs,
  RecoveryMessages *m)
{
  dout(10) << __func__ << ": returned " << hoid << " "
	   << "(" << to_read.get<0>()
	   << ", " << to_read.get<1>()
	   << ", " << to_read.get<2>()
	   << ")"
	   << dendl;
  assert(recovery_ops.count(hoid));
  RecoveryOp &op = recovery_ops[hoid];
  assert(op.returned_data.empty());
  map<int, bufferlist*> target;
  for (set<shard_id_t>::iterator i = op.missing_on_shards.begin();
       i != op.missing_on_shards.end();
       ++i) {
    target[*i] = &(op.returned_data[*i]);
  }
  map<int, bufferlist> from;
  for(map<pg_shard_t, bufferlist>::iterator i = to_read.get<2>().begin();
      i != to_read.get<2>().end();
      ++i) {
    from[i->first.shard].claim(i->second);
  }
  dout(10) << __func__ << ": " << from << dendl;
  if (ECUtil::decode(sinfo, ec_impl, from, target) != 0) {
    derr << __func__ << ": inconsistent shard sizes " << hoid << " "
	 << " the offending shard must be manually removed "
	 << " after verifying there are enough shards to recover "
	 << "(" << to_read.get<0>()
	 << ", " << to_read.get<1>()
	 << ", " << to_read.get<2>()
	 << ")"
	 << dendl;
    assert(0);
  }
  if (attrs) {
    op.xattrs.swap(*attrs);

    if (!op.obc) {
      // attrs only reference the origin bufferlist (decode from ECSubReadReply message)
      // whose size is much greater than attrs in recovery. If obc cache it (get_obc maybe
      // cache the attr), this causes the whole origin bufferlist would not be free until
      // obc is evicted from obc cache. So rebuild the bufferlist before cache it.
      for (map<string, bufferlist>::iterator it = op.xattrs.begin();
           it != op.xattrs.end();
           ++it) {
        it->second.rebuild();
      }
      op.obc = get_parent()->get_obc(hoid, op.xattrs);
      op.recovery_info.size = op.obc->obs.oi.size;
      op.recovery_info.oi = op.obc->obs.oi;
    }

    ECUtil::HashInfo hinfo(ec_impl->get_chunk_count());
    if (op.obc->obs.oi.size > 0) {
      assert(op.xattrs.count(ECUtil::get_hinfo_key()));
      bufferlist::iterator bp = op.xattrs[ECUtil::get_hinfo_key()].begin();
      ::decode(hinfo, bp);
    }
    op.hinfo = unstable_hashinfo_registry.lookup_or_create(hoid, hinfo);
  }
  assert(op.xattrs.size());
  assert(op.obc);
  continue_recovery_op(op, m);
}

struct SendPushReplies : public Context {
  PGBackend::Listener *l;
  epoch_t epoch;
  map<int, MOSDPGPushReply*> replies;
  SendPushReplies(
    PGBackend::Listener *l,
    epoch_t epoch,
    map<int, MOSDPGPushReply*> &in) : l(l), epoch(epoch) {
    replies.swap(in);
  }
  void finish(int) {
    for (map<int, MOSDPGPushReply*>::iterator i = replies.begin();
	 i != replies.end();
	 ++i) {
      l->send_message_osd_cluster(i->first, i->second, epoch);
    }
    replies.clear();
  }
  ~SendPushReplies() {
    for (map<int, MOSDPGPushReply*>::iterator i = replies.begin();
	 i != replies.end();
	 ++i) {
      i->second->put();
    }
    replies.clear();
  }
};

void ECBackend::dispatch_recovery_messages(RecoveryMessages &m, int priority)
{
  for (map<pg_shard_t, vector<PushOp> >::iterator i = m.pushes.begin();
       i != m.pushes.end();
       m.pushes.erase(i++)) {
    MOSDPGPush *msg = new MOSDPGPush();
    msg->set_priority(priority);
    msg->map_epoch = get_parent()->get_epoch();
    msg->from = get_parent()->whoami_shard();
    msg->pgid = spg_t(get_parent()->get_info().pgid.pgid, i->first.shard);
    msg->pushes.swap(i->second);
    msg->compute_cost(cct);
    get_parent()->send_message(
      i->first.osd,
      msg);
  }
  map<int, MOSDPGPushReply*> replies;
  for (map<pg_shard_t, vector<PushReplyOp> >::iterator i =
	 m.push_replies.begin();
       i != m.push_replies.end();
       m.push_replies.erase(i++)) {
    MOSDPGPushReply *msg = new MOSDPGPushReply();
    msg->set_priority(priority);
    msg->map_epoch = get_parent()->get_epoch();
    msg->from = get_parent()->whoami_shard();
    msg->pgid = spg_t(get_parent()->get_info().pgid.pgid, i->first.shard);
    msg->replies.swap(i->second);
    msg->compute_cost(cct);
    replies.insert(make_pair(i->first.osd, msg));
  }
  m.t->register_on_complete(
    get_parent()->bless_context(
      new SendPushReplies(
	get_parent(),
	get_parent()->get_epoch(),
	replies)));
  m.t->register_on_applied(
    new ObjectStore::C_DeleteTransaction(m.t));
  get_parent()->queue_transaction(m.t);
  m.t = NULL;
  if (m.reads.empty())
    return;
  start_read_op(
    priority,
    m.reads,
    OpRequestRef());
}

void ECBackend::continue_recovery_op(
  RecoveryOp &op,
  RecoveryMessages *m)
{
  dout(10) << __func__ << ": continuing " << op << dendl;
  while (1) {
    switch (op.state) {
    case RecoveryOp::IDLE: {
      // start read
      op.state = RecoveryOp::READING;
      assert(!op.recovery_progress.data_complete);
      set<int> want(op.missing_on_shards.begin(), op.missing_on_shards.end());
      set<pg_shard_t> to_read;
      uint64_t recovery_max_chunk = get_recovery_chunk_size();
      int r = get_min_avail_to_read_shards(
	op.hoid, want, true, &to_read);
      if (r != 0) {
	// we must have lost a recovery source
	assert(!op.recovery_progress.first);
	dout(10) << __func__ << ": canceling recovery op for obj " << op.hoid
		 << dendl;
	get_parent()->cancel_pull(op.hoid);
	recovery_ops.erase(op.hoid);
	return;
      }
      m->read(
	this,
	op.hoid,
	op.recovery_progress.data_recovered_to,
	recovery_max_chunk,
	to_read,
	op.recovery_progress.first);
      op.extent_requested = make_pair(op.recovery_progress.data_recovered_to,
				      recovery_max_chunk);
      dout(10) << __func__ << ": IDLE return " << op << dendl;
      return;
    }
    case RecoveryOp::READING: {
      // read completed, start write
      assert(op.xattrs.size());
      assert(op.returned_data.size());
      op.state = RecoveryOp::WRITING;
      ObjectRecoveryProgress after_progress = op.recovery_progress;
      after_progress.data_recovered_to += op.extent_requested.second;
      after_progress.first = false;
      if (after_progress.data_recovered_to >= op.obc->obs.oi.size) {
	after_progress.data_recovered_to =
	  sinfo.logical_to_next_stripe_offset(
	    op.obc->obs.oi.size);
	after_progress.data_complete = true;
      }
      for (set<pg_shard_t>::iterator mi = op.missing_on.begin();
	   mi != op.missing_on.end();
	   ++mi) {
	assert(op.returned_data.count(mi->shard));
	m->pushes[*mi].push_back(PushOp());
	PushOp &pop = m->pushes[*mi].back();
	pop.soid = op.hoid;
	pop.version = op.v;
	pop.data = op.returned_data[mi->shard];
	dout(10) << __func__ << ": before_progress=" << op.recovery_progress
		 << ", after_progress=" << after_progress
		 << ", pop.data.length()=" << pop.data.length()
		 << ", size=" << op.obc->obs.oi.size << dendl;
	assert(
	  pop.data.length() ==
	  sinfo.aligned_logical_offset_to_chunk_offset(
	    after_progress.data_recovered_to -
	    op.recovery_progress.data_recovered_to)
	  );
	if (pop.data.length())
	  pop.data_included.insert(
	    sinfo.aligned_logical_offset_to_chunk_offset(
	      op.recovery_progress.data_recovered_to),
	    pop.data.length()
	    );
	if (op.recovery_progress.first) {
	  pop.attrset = op.xattrs;
	}
	pop.recovery_info = op.recovery_info;
	pop.before_progress = op.recovery_progress;
	pop.after_progress = after_progress;
	if (*mi != get_parent()->primary_shard())
	  get_parent()->begin_peer_recover(
	    *mi,
	    op.hoid);
      }
      op.returned_data.clear();
      op.waiting_on_pushes = op.missing_on;
      op.recovery_progress = after_progress;
      dout(10) << __func__ << ": READING return " << op << dendl;
      return;
    }
    case RecoveryOp::WRITING: {
      if (op.waiting_on_pushes.empty()) {
	if (op.recovery_progress.data_complete) {
	  op.state = RecoveryOp::COMPLETE;
	  for (set<pg_shard_t>::iterator i = op.missing_on.begin();
	       i != op.missing_on.end();
	       ++i) {
	    if (*i != get_parent()->primary_shard()) {
	      dout(10) << __func__ << ": on_peer_recover on " << *i
		       << ", obj " << op.hoid << dendl;
	      get_parent()->on_peer_recover(
		*i,
		op.hoid,
		op.recovery_info,
		object_stat_sum_t());
	    }
	  }
	  get_parent()->on_global_recover(op.hoid);
	  dout(10) << __func__ << ": WRITING return " << op << dendl;
	  recovery_ops.erase(op.hoid);
	  return;
	} else {
	  op.state = RecoveryOp::IDLE;
	  dout(10) << __func__ << ": WRITING continue " << op << dendl;
	  continue;
	}
      }
      return;
    }
    case RecoveryOp::COMPLETE: {
      assert(0); // should never be called once complete
    };
    default:
      assert(0);
    }
  }
}

void ECBackend::run_recovery_op(
  RecoveryHandle *_h,
  int priority)
{
  ECRecoveryHandle *h = static_cast<ECRecoveryHandle*>(_h);
  RecoveryMessages m;
  for (list<RecoveryOp>::iterator i = h->ops.begin();
       i != h->ops.end();
       ++i) {
    dout(10) << __func__ << ": starting " << *i << dendl;
    assert(!recovery_ops.count(i->hoid));
    RecoveryOp &op = recovery_ops.insert(make_pair(i->hoid, *i)).first->second;
    continue_recovery_op(op, &m);
  }
  dispatch_recovery_messages(m, priority);
  delete _h;
}

void ECBackend::recover_object(
  const hobject_t &hoid,
  eversion_t v,
  ObjectContextRef head,
  ObjectContextRef obc,
  RecoveryHandle *_h)
{
  ECRecoveryHandle *h = static_cast<ECRecoveryHandle*>(_h);
  h->ops.push_back(RecoveryOp());
  h->ops.back().v = v;
  h->ops.back().hoid = hoid;
  h->ops.back().obc = obc;
  h->ops.back().recovery_info.soid = hoid;
  h->ops.back().recovery_info.version = v;
  if (obc) {
    h->ops.back().recovery_info.size = obc->obs.oi.size;
    h->ops.back().recovery_info.oi = obc->obs.oi;
  }
  h->ops.back().recovery_progress.omap_complete = true;
  for (set<pg_shard_t>::const_iterator i =
	 get_parent()->get_actingbackfill_shards().begin();
       i != get_parent()->get_actingbackfill_shards().end();
       ++i) {
    dout(10) << "checking " << *i << dendl;
    if (get_parent()->get_shard_missing(*i).is_missing(hoid)) {
      h->ops.back().missing_on.insert(*i);
      h->ops.back().missing_on_shards.insert(i->shard);
    }
  }
  dout(10) << __func__ << ": built op " << h->ops.back() << dendl;
}

bool ECBackend::can_handle_while_inactive(
  OpRequestRef _op)
{
  return false;
}

bool ECBackend::handle_message(
  OpRequestRef _op)
{
  dout(10) << __func__ << ": " << *_op->get_req() << dendl;
  int priority = _op->get_req()->get_priority();
  switch (_op->get_req()->get_type()) {
  case MSG_OSD_EC_WRITE: {
    MOSDECSubOpWrite *op = static_cast<MOSDECSubOpWrite*>(_op->get_req());
    handle_sub_write(op->op.from, _op, op->op);
    return true;
  }
  case MSG_OSD_EC_WRITE_REPLY: {
    MOSDECSubOpWriteReply *op = static_cast<MOSDECSubOpWriteReply*>(
      _op->get_req());
    op->set_priority(priority);
    handle_sub_write_reply(op->op.from, op->op);
    return true;
  }
  case MSG_OSD_EC_READ: {
    MOSDECSubOpRead *op = static_cast<MOSDECSubOpRead*>(_op->get_req());
    MOSDECSubOpReadReply *reply = new MOSDECSubOpReadReply;
    reply->pgid = get_parent()->primary_spg_t();
    reply->map_epoch = get_parent()->get_epoch();
    handle_sub_read(op->op.from, op->op, &(reply->op));
    op->set_priority(priority);
    get_parent()->send_message_osd_cluster(
      op->op.from.osd, reply, get_parent()->get_epoch());
    return true;
  }
  case MSG_OSD_EC_READ_REPLY: {
    MOSDECSubOpReadReply *op = static_cast<MOSDECSubOpReadReply*>(
      _op->get_req());
    RecoveryMessages rm;
    handle_sub_read_reply(op->op.from, op->op, &rm);
    dispatch_recovery_messages(rm, priority);
    return true;
  }
  case MSG_OSD_PG_PUSH: {
    MOSDPGPush *op = static_cast<MOSDPGPush *>(_op->get_req());
    RecoveryMessages rm;
    for (vector<PushOp>::iterator i = op->pushes.begin();
	 i != op->pushes.end();
	 ++i) {
      handle_recovery_push(*i, &rm);
    }
    dispatch_recovery_messages(rm, priority);
    return true;
  }
  case MSG_OSD_PG_PUSH_REPLY: {
    MOSDPGPushReply *op = static_cast<MOSDPGPushReply *>(_op->get_req());
    RecoveryMessages rm;
    for (vector<PushReplyOp>::iterator i = op->replies.begin();
	 i != op->replies.end();
	 ++i) {
      handle_recovery_push_reply(*i, op->from, &rm);
    }
    dispatch_recovery_messages(rm, priority);
    return true;
  }
  default:
    return false;
  }
  return false;
}

struct SubWriteCommitted : public Context {
  ECBackend *pg;
  OpRequestRef msg;
  ceph_tid_t tid;
  eversion_t version;
  eversion_t last_complete;
  SubWriteCommitted(
    ECBackend *pg,
    OpRequestRef msg,
    ceph_tid_t tid,
    eversion_t version,
    eversion_t last_complete)
    : pg(pg), msg(msg), tid(tid),
      version(version), last_complete(last_complete) {}
  void finish(int) {
    if (msg)
      msg->mark_event("sub_op_committed");
    pg->sub_write_committed(tid, version, last_complete);
  }
};
void ECBackend::sub_write_committed(
  ceph_tid_t tid, eversion_t version, eversion_t last_complete) {
  if (get_parent()->pgb_is_primary()) {
    ECSubWriteReply reply;
    reply.tid = tid;
    reply.last_complete = last_complete;
    reply.committed = true;
    reply.from = get_parent()->whoami_shard();
    handle_sub_write_reply(
      get_parent()->whoami_shard(),
      reply);
  } else {
    get_parent()->update_last_complete_ondisk(last_complete);
    MOSDECSubOpWriteReply *r = new MOSDECSubOpWriteReply;
    r->pgid = get_parent()->primary_spg_t();
    r->map_epoch = get_parent()->get_epoch();
    r->op.tid = tid;
    r->op.last_complete = last_complete;
    r->op.committed = true;
    r->op.from = get_parent()->whoami_shard();
    get_parent()->send_message_osd_cluster(
      get_parent()->primary_shard().osd, r, get_parent()->get_epoch());
  }
}

struct SubWriteApplied : public Context {
  ECBackend *pg;
  OpRequestRef msg;
  ceph_tid_t tid;
  eversion_t version;
  SubWriteApplied(
    ECBackend *pg,
    OpRequestRef msg,
    ceph_tid_t tid,
    eversion_t version)
    : pg(pg), msg(msg), tid(tid), version(version) {}
  void finish(int) {
    if (msg)
      msg->mark_event("sub_op_applied");
    pg->sub_write_applied(tid, version);
  }
};
void ECBackend::sub_write_applied(
  ceph_tid_t tid, eversion_t version) {
  parent->op_applied(version);
  if (get_parent()->pgb_is_primary()) {
    ECSubWriteReply reply;
    reply.from = get_parent()->whoami_shard();
    reply.tid = tid;
    reply.applied = true;
    handle_sub_write_reply(
      get_parent()->whoami_shard(),
      reply);
  } else {
    MOSDECSubOpWriteReply *r = new MOSDECSubOpWriteReply;
    r->pgid = get_parent()->primary_spg_t();
    r->map_epoch = get_parent()->get_epoch();
    r->op.from = get_parent()->whoami_shard();
    r->op.tid = tid;
    r->op.applied = true;
    get_parent()->send_message_osd_cluster(
      get_parent()->primary_shard().osd, r, get_parent()->get_epoch());
  }
}

void ECBackend::handle_sub_write(
  pg_shard_t from,
  OpRequestRef msg,
  ECSubWrite &op,
  Context *on_local_applied_sync)
{
  if (msg)
    msg->mark_started();
  assert(!get_parent()->get_log().get_missing().is_missing(op.soid));
  if (!get_parent()->pgb_is_primary())
    get_parent()->update_stats(op.stats);
  ObjectStore::Transaction *localt = new ObjectStore::Transaction;
  localt->set_use_tbl(op.t.get_use_tbl());
  if (!op.temp_added.empty()) {
    get_temp_coll(localt);
    add_temp_objs(op.temp_added);
  }
  if (op.t.empty()) {
    for (set<hobject_t>::iterator i = op.temp_removed.begin();
	 i != op.temp_removed.end();
	 ++i) {
      dout(10) << __func__ << ": removing object " << *i
	       << " since we won't get the transaction" << dendl;
      localt->remove(
	temp_coll,
	ghobject_t(
	  *i,
	  ghobject_t::NO_GEN,
	  get_parent()->whoami_shard().shard));
    }
  }
  clear_temp_objs(op.temp_removed);
  get_parent()->log_operation(
    op.log_entries,
    op.updated_hit_set_history,
    op.trim_to,
    op.trim_rollback_to,
    !(op.t.empty()),
    localt);

  if (!(dynamic_cast<ReplicatedPG *>(get_parent())->is_undersized()) &&
      get_parent()->whoami_shard().shard >= ec_impl->get_data_chunk_count())
    op.t.set_fadvise_flag(CEPH_OSD_OP_FLAG_FADVISE_DONTNEED);

  localt->append(op.t);
  if (on_local_applied_sync) {
    dout(10) << "Queueing onreadable_sync: " << on_local_applied_sync << dendl;
    localt->register_on_applied_sync(on_local_applied_sync);
  }
  localt->register_on_commit(
    get_parent()->bless_context(
      new SubWriteCommitted(
	this, msg, op.tid,
	op.at_version,
	get_parent()->get_info().last_complete)));
  localt->register_on_applied(
    get_parent()->bless_context(
      new SubWriteApplied(this, msg, op.tid, op.at_version)));
  localt->register_on_applied(
    new ObjectStore::C_DeleteTransaction(localt));
  get_parent()->queue_transaction(localt, msg);
}

void ECBackend::handle_sub_read(
  pg_shard_t from,
  ECSubRead &op,
  ECSubReadReply *reply)
{
  for(map<hobject_t, list<boost::tuple<uint64_t, uint64_t, uint32_t> > >::iterator i =
        op.to_read.begin();
      i != op.to_read.end();
      ++i) {
    for (list<boost::tuple<uint64_t, uint64_t, uint32_t> >::iterator j = i->second.begin();
	 j != i->second.end();
	 ++j) {
      bufferlist bl;
      int r = store->read(
	i->first.is_temp() ? temp_coll : coll,
	ghobject_t(
	  i->first, ghobject_t::NO_GEN, get_parent()->whoami_shard().shard),
	j->get<0>(),
	j->get<1>(),
	bl, j->get<2>(),
	false);
      if (r < 0) {
	assert(0);
	reply->buffers_read.erase(i->first);
	reply->errors[i->first] = r;
	break;
      } else {
	reply->buffers_read[i->first].push_back(
	  make_pair(
	    j->get<0>(),
	    bl)
	  );
      }
    }
  }
  for (set<hobject_t>::iterator i = op.attrs_to_read.begin();
       i != op.attrs_to_read.end();
       ++i) {
    dout(10) << __func__ << ": fulfilling attr request on "
	     << *i << dendl;
    if (reply->errors.count(*i))
      continue;
    int r = store->getattrs(
      i->is_temp() ? temp_coll : coll,
      ghobject_t(
	*i, ghobject_t::NO_GEN, get_parent()->whoami_shard().shard),
      reply->attrs_read[*i]);
    if (r < 0) {
      assert(0);
      reply->buffers_read.erase(*i);
      reply->errors[*i] = r;
    }
  }
  reply->from = get_parent()->whoami_shard();
  reply->tid = op.tid;
}

void ECBackend::handle_sub_write_reply(
  pg_shard_t from,
  ECSubWriteReply &op)
{
  map<ceph_tid_t, Op>::iterator i = tid_to_op_map.find(op.tid);
  assert(i != tid_to_op_map.end());
  if (op.committed) {
    assert(i->second.pending_commit.count(from));
    i->second.pending_commit.erase(from);
    if (from != get_parent()->whoami_shard()) {
      get_parent()->update_peer_last_complete_ondisk(from, op.last_complete);
    }
  }
  if (op.applied) {
    assert(i->second.pending_apply.count(from));
    i->second.pending_apply.erase(from);
  }
  check_op(&(i->second));
}

void ECBackend::handle_sub_read_reply(
  pg_shard_t from,
  ECSubReadReply &op,
  RecoveryMessages *m)
{
  dout(10) << __func__ << ": reply " << op << dendl;
  map<ceph_tid_t, ReadOp>::iterator iter = tid_to_read_map.find(op.tid);
  if (iter == tid_to_read_map.end()) {
    //canceled
    return;
  }
  ReadOp &rop = iter->second;
  for (map<hobject_t, list<pair<uint64_t, bufferlist> > >::iterator i =
	 op.buffers_read.begin();
       i != op.buffers_read.end();
       ++i) {
    assert(!op.errors.count(i->first));
    if (!rop.to_read.count(i->first)) {
      // We canceled this read! @see filter_read_op
      continue;
    }
    list<boost::tuple<uint64_t, uint64_t, uint32_t> >::const_iterator req_iter =
      rop.to_read.find(i->first)->second.to_read.begin();
    list<
      boost::tuple<
	uint64_t, uint64_t, map<pg_shard_t, bufferlist> > >::iterator riter =
      rop.complete[i->first].returned.begin();
    for (list<pair<uint64_t, bufferlist> >::iterator j = i->second.begin();
	 j != i->second.end();
	 ++j, ++req_iter, ++riter) {
      assert(req_iter != rop.to_read.find(i->first)->second.to_read.end());
      assert(riter != rop.complete[i->first].returned.end());
      pair<uint64_t, uint64_t> adjusted =
	sinfo.aligned_offset_len_to_chunk(
	  make_pair(req_iter->get<0>(), req_iter->get<1>()));
      assert(adjusted.first == j->first);
      riter->get<2>()[from].claim(j->second);
    }
  }
  for (map<hobject_t, map<string, bufferlist> >::iterator i = op.attrs_read.begin();
       i != op.attrs_read.end();
       ++i) {
    assert(!op.errors.count(i->first));
    if (!rop.to_read.count(i->first)) {
      // We canceled this read! @see filter_read_op
      continue;
    }
    rop.complete[i->first].attrs = map<string, bufferlist>();
    (*(rop.complete[i->first].attrs)).swap(i->second);
  }
  for (map<hobject_t, int>::iterator i = op.errors.begin();
       i != op.errors.end();
       ++i) {
    rop.complete[i->first].errors.insert(
      make_pair(
	from,
	i->second));
    if (rop.complete[i->first].r == 0)
      rop.complete[i->first].r = i->second;
  }

  map<pg_shard_t, set<ceph_tid_t> >::iterator siter =
shard_to_read_map.find(from);
  assert(siter != shard_to_read_map.end());
  assert(siter->second.count(op.tid));
  siter->second.erase(op.tid);

  assert(rop.in_progress.count(from));
  rop.in_progress.erase(from);
  if (!rop.in_progress.empty()) {
    dout(10) << __func__ << " readop not complete: " << rop << dendl;
  } else {
    dout(10) << __func__ << " readop complete: " << rop << dendl;
    complete_read_op(rop, m);
  }
}

void ECBackend::complete_read_op(ReadOp &rop, RecoveryMessages *m)
{
  map<hobject_t, read_request_t>::iterator reqiter =
    rop.to_read.begin();
  map<hobject_t, read_result_t>::iterator resiter =
    rop.complete.begin();
  assert(rop.to_read.size() == rop.complete.size());
  for (; reqiter != rop.to_read.end(); ++reqiter, ++resiter) {
    if (reqiter->second.cb) {
      pair<RecoveryMessages *, read_result_t &> arg(
	m, resiter->second);
      reqiter->second.cb->complete(arg);
      reqiter->second.cb = NULL;
    }
  }
  tid_to_read_map.erase(rop.tid);
}

struct FinishReadOp : public GenContext<ThreadPool::TPHandle&>  {
  ECBackend *ec;
  ceph_tid_t tid;
  FinishReadOp(ECBackend *ec, ceph_tid_t tid) : ec(ec), tid(tid) {}
  void finish(ThreadPool::TPHandle &handle) {
    assert(ec->tid_to_read_map.count(tid));
    int priority = ec->tid_to_read_map[tid].priority;
    RecoveryMessages rm;
    ec->complete_read_op(ec->tid_to_read_map[tid], &rm);
    ec->dispatch_recovery_messages(rm, priority);
  }
};

void ECBackend::filter_read_op(
  const OSDMapRef osdmap,
  ReadOp &op)
{
  set<hobject_t> to_cancel;
  for (map<pg_shard_t, set<hobject_t> >::iterator i = op.source_to_obj.begin();
       i != op.source_to_obj.end();
       ++i) {
    if (osdmap->is_down(i->first.osd)) {
      to_cancel.insert(i->second.begin(), i->second.end());
      op.in_progress.erase(i->first);
      continue;
    }
  }

  if (to_cancel.empty())
    return;

  for (map<pg_shard_t, set<hobject_t> >::iterator i = op.source_to_obj.begin();
       i != op.source_to_obj.end();
       ) {
    for (set<hobject_t>::iterator j = i->second.begin();
	 j != i->second.end();
	 ) {
      if (to_cancel.count(*j))
	i->second.erase(j++);
      else
	++j;
    }
    if (i->second.empty()) {
      op.source_to_obj.erase(i++);
    } else {
      assert(!osdmap->is_down(i->first.osd));
      ++i;
    }
  }

  for (set<hobject_t>::iterator i = to_cancel.begin();
       i != to_cancel.end();
       ++i) {
    get_parent()->cancel_pull(*i);

    assert(op.to_read.count(*i));
    read_request_t &req = op.to_read.find(*i)->second;
    dout(10) << __func__ << ": canceling " << req
	     << "  for obj " << *i << dendl;
    assert(req.cb);
    delete req.cb;
    req.cb = NULL;

    op.to_read.erase(*i);
    op.complete.erase(*i);
    recovery_ops.erase(*i);
  }

  if (op.in_progress.empty()) {
    get_parent()->schedule_recovery_work(
      get_parent()->bless_gencontext(
	new FinishReadOp(this, op.tid)));
  }
}

void ECBackend::check_recovery_sources(const OSDMapRef osdmap)
{
  set<ceph_tid_t> tids_to_filter;
  for (map<pg_shard_t, set<ceph_tid_t> >::iterator 
       i = shard_to_read_map.begin();
       i != shard_to_read_map.end();
       ) {
    if (osdmap->is_down(i->first.osd)) {
      tids_to_filter.insert(i->second.begin(), i->second.end());
      shard_to_read_map.erase(i++);
    } else {
      ++i;
    }
  }
  for (set<ceph_tid_t>::iterator i = tids_to_filter.begin();
       i != tids_to_filter.end();
       ++i) {
    map<ceph_tid_t, ReadOp>::iterator j = tid_to_read_map.find(*i);
    assert(j != tid_to_read_map.end());
    filter_read_op(osdmap, j->second);
  }
}

void ECBackend::on_change()
{
  dout(10) << __func__ << dendl;
  writing.clear();
  tid_to_op_map.clear();
  for (map<ceph_tid_t, ReadOp>::iterator i = tid_to_read_map.begin();
       i != tid_to_read_map.end();
       ++i) {
    dout(10) << __func__ << ": cancelling " << i->second << dendl;
    for (map<hobject_t, read_request_t>::iterator j =
	   i->second.to_read.begin();
	 j != i->second.to_read.end();
	 ++j) {
      delete j->second.cb;
      j->second.cb = 0;
    }
  }
  tid_to_read_map.clear();
  for (list<ClientAsyncReadStatus>::iterator i = in_progress_client_reads.begin();
       i != in_progress_client_reads.end();
       ++i) {
    delete i->on_complete;
    i->on_complete = NULL;
  }
  in_progress_client_reads.clear();
  shard_to_read_map.clear();
  clear_recovery_state();
}

void ECBackend::clear_recovery_state()
{
  recovery_ops.clear();
}

void ECBackend::on_flushed()
{
}

void ECBackend::dump_recovery_info(Formatter *f) const
{
  f->open_array_section("recovery_ops");
  for (map<hobject_t, RecoveryOp>::const_iterator i = recovery_ops.begin();
       i != recovery_ops.end();
       ++i) {
    f->open_object_section("op");
    i->second.dump(f);
    f->close_section();
  }
  f->close_section();
  f->open_array_section("read_ops");
  for (map<ceph_tid_t, ReadOp>::const_iterator i = tid_to_read_map.begin();
       i != tid_to_read_map.end();
       ++i) {
    f->open_object_section("read_op");
    i->second.dump(f);
    f->close_section();
  }
  f->close_section();
}

PGBackend::PGTransaction *ECBackend::get_transaction()
{
  return new ECTransaction;
}

struct MustPrependHashInfo : public ObjectModDesc::Visitor {
  enum { EMPTY, FOUND_APPEND, FOUND_CREATE_STASH } state;
  MustPrependHashInfo() : state(EMPTY) {}
  void append(uint64_t) {
    if (state == EMPTY) {
      state = FOUND_APPEND;
    }
  }
  void rmobject(version_t) {
    if (state == EMPTY) {
      state = FOUND_CREATE_STASH;
    }
  }
  void create() {
    if (state == EMPTY) {
      state = FOUND_CREATE_STASH;
    }
  }
  bool must_prepend_hash_info() const { return state == FOUND_APPEND; }
};

void ECBackend::submit_transaction(
  const hobject_t &hoid,
  const eversion_t &at_version,
  PGTransaction *_t,
  const eversion_t &trim_to,
  const eversion_t &trim_rollback_to,
  const vector<pg_log_entry_t> &log_entries,
  boost::optional<pg_hit_set_history_t> &hset_history,
  Context *on_local_applied_sync,
  Context *on_all_applied,
  Context *on_all_commit,
  ceph_tid_t tid,
  osd_reqid_t reqid,
  OpRequestRef client_op
  )
{
  assert(!tid_to_op_map.count(tid));
  Op *op = &(tid_to_op_map[tid]);
  op->hoid = hoid;
  op->version = at_version;
  op->trim_to = trim_to;
  op->trim_rollback_to = trim_rollback_to;
  op->log_entries = log_entries;
  std::swap(op->updated_hit_set_history, hset_history);
  op->on_local_applied_sync = on_local_applied_sync;
  op->on_all_applied = on_all_applied;
  op->on_all_commit = on_all_commit;
  op->tid = tid;
  op->reqid = reqid;
  op->client_op = client_op;
  
  op->t = static_cast<ECTransaction*>(_t);

  set<hobject_t> need_hinfos;
  op->t->get_append_objects(&need_hinfos);
  for (set<hobject_t>::iterator i = need_hinfos.begin();
       i != need_hinfos.end();
       ++i) {
    ECUtil::HashInfoRef ref = get_hash_info(*i);
    if (!ref) {
      derr << __func__ << ": get_hash_info(" << *i << ")"
	   << " returned a null pointer and there is no "
	   << " way to recover from such an error in this "
	   << " context" << dendl;
      assert(0);
    }
    op->unstable_hash_infos.insert(
      make_pair(
	*i,
	ref));
  }

  for (vector<pg_log_entry_t>::iterator i = op->log_entries.begin();
       i != op->log_entries.end();
       ++i) {
    MustPrependHashInfo vis;
    i->mod_desc.visit(&vis);
    if (vis.must_prepend_hash_info()) {
      dout(10) << __func__ << ": stashing HashInfo for "
	       << i->soid << " for entry " << *i << dendl;
      assert(op->unstable_hash_infos.count(i->soid));
      ObjectModDesc desc;
      map<string, boost::optional<bufferlist> > old_attrs;
      bufferlist old_hinfo;
      ::encode(*(op->unstable_hash_infos[i->soid]), old_hinfo);
      old_attrs[ECUtil::get_hinfo_key()] = old_hinfo;
      desc.setattrs(old_attrs);
      i->mod_desc.swap(desc);
      i->mod_desc.claim_append(desc);
      assert(i->mod_desc.can_rollback());
    }
  }

  dout(10) << __func__ << ": op " << *op << " starting" << dendl;
  start_write(op);
  writing.push_back(op);
  dout(10) << "onreadable_sync: " << op->on_local_applied_sync << dendl;
}

int ECBackend::get_min_avail_to_read_shards(
  const hobject_t &hoid,
  const set<int> &want,
  bool for_recovery,
  set<pg_shard_t> *to_read)
{
  map<hobject_t, set<pg_shard_t> >::const_iterator miter =
    get_parent()->get_missing_loc_shards().find(hoid);

  set<int> have;
  map<shard_id_t, pg_shard_t> shards;

  for (set<pg_shard_t>::const_iterator i =
	 get_parent()->get_acting_shards().begin();
       i != get_parent()->get_acting_shards().end();
       ++i) {
    dout(10) << __func__ << ": checking acting " << *i << dendl;
    const pg_missing_t &missing = get_parent()->get_shard_missing(*i);
    if (!missing.is_missing(hoid)) {
      assert(!have.count(i->shard));
      have.insert(i->shard);
      assert(!shards.count(i->shard));
      shards.insert(make_pair(i->shard, *i));
    }
  }

  if (for_recovery) {
    for (set<pg_shard_t>::const_iterator i =
	   get_parent()->get_backfill_shards().begin();
	 i != get_parent()->get_backfill_shards().end();
	 ++i) {
      if (have.count(i->shard)) {
	assert(shards.count(i->shard));
	continue;
      }
      dout(10) << __func__ << ": checking backfill " << *i << dendl;
      assert(!shards.count(i->shard));
      const pg_info_t &info = get_parent()->get_shard_info(*i);
      const pg_missing_t &missing = get_parent()->get_shard_missing(*i);
      if (hoid < info.last_backfill && !missing.is_missing(hoid)) {
	have.insert(i->shard);
	shards.insert(make_pair(i->shard, *i));
      }
    }

    if (miter != get_parent()->get_missing_loc_shards().end()) {
      for (set<pg_shard_t>::iterator i = miter->second.begin();
	   i != miter->second.end();
	   ++i) {
	dout(10) << __func__ << ": checking missing_loc " << *i << dendl;
	boost::optional<const pg_missing_t &> m =
	  get_parent()->maybe_get_shard_missing(*i);
	if (m) {
	  assert(!(*m).is_missing(hoid));
	}
	have.insert(i->shard);
	shards.insert(make_pair(i->shard, *i));
      }
    }
  }

  set<int> need;
  int r = ec_impl->minimum_to_decode(want, have, &need);
  if (r < 0)
    return r;

  if (!to_read)
    return 0;

  for (set<int>::iterator i = need.begin();
       i != need.end();
       ++i) {
    assert(shards.count(shard_id_t(*i)));
    to_read->insert(shards[shard_id_t(*i)]);
  }
  return 0;
}

void ECBackend::start_read_op(
  int priority,
  map<hobject_t, read_request_t> &to_read,
  OpRequestRef _op)
{
  ceph_tid_t tid = get_parent()->get_tid();
  assert(!tid_to_read_map.count(tid));
  ReadOp &op(tid_to_read_map[tid]);
  op.priority = priority;
  op.tid = tid;
  op.to_read.swap(to_read);
  op.op = _op;
  dout(10) << __func__ << ": starting " << op << dendl;

  map<pg_shard_t, ECSubRead> messages;
  for (map<hobject_t, read_request_t>::iterator i = op.to_read.begin();
       i != op.to_read.end();
       ++i) {
    list<boost::tuple<
      uint64_t, uint64_t, map<pg_shard_t, bufferlist> > > &reslist =
      op.complete[i->first].returned;
    bool need_attrs = i->second.want_attrs;
    for (set<pg_shard_t>::const_iterator j = i->second.need.begin();
	 j != i->second.need.end();
	 ++j) {
      if (need_attrs) {
	messages[*j].attrs_to_read.insert(i->first);
	need_attrs = false;
      }
      op.obj_to_source[i->first].insert(*j);
      op.source_to_obj[*j].insert(i->first);
    }
    for (list<boost::tuple<uint64_t, uint64_t, uint32_t> >::const_iterator j =
	   i->second.to_read.begin();
	 j != i->second.to_read.end();
	 ++j) {
      reslist.push_back(
	boost::make_tuple(
	  j->get<0>(),
	  j->get<1>(),
	  map<pg_shard_t, bufferlist>()));
      pair<uint64_t, uint64_t> chunk_off_len =
	sinfo.aligned_offset_len_to_chunk(make_pair(j->get<0>(), j->get<1>()));
      for (set<pg_shard_t>::const_iterator k = i->second.need.begin();
	   k != i->second.need.end();
	   ++k) {
	messages[*k].to_read[i->first].push_back(boost::make_tuple(chunk_off_len.first,
								    chunk_off_len.second,
								    j->get<2>()));
      }
      assert(!need_attrs);
    }
  }

  for (map<pg_shard_t, ECSubRead>::iterator i = messages.begin();
       i != messages.end();
       ++i) {
    op.in_progress.insert(i->first);
    shard_to_read_map[i->first].insert(op.tid);
    i->second.tid = tid;
    MOSDECSubOpRead *msg = new MOSDECSubOpRead;
    msg->set_priority(priority);
    msg->pgid = spg_t(
      get_parent()->whoami_spg_t().pgid,
      i->first.shard);
    msg->map_epoch = get_parent()->get_epoch();
    msg->op = i->second;
    msg->op.from = get_parent()->whoami_shard();
    msg->op.tid = tid;
    get_parent()->send_message_osd_cluster(
      i->first.osd,
      msg,
      get_parent()->get_epoch());
  }
  dout(10) << __func__ << ": started " << op << dendl;
}

ECUtil::HashInfoRef ECBackend::get_hash_info(
  const hobject_t &hoid)
{
  dout(10) << __func__ << ": Getting attr on " << hoid << dendl;
  ECUtil::HashInfoRef ref = unstable_hashinfo_registry.lookup(hoid);
  if (!ref) {
    dout(10) << __func__ << ": not in cache " << hoid << dendl;
    struct stat st;
    int r = store->stat(
      hoid.is_temp() ? temp_coll : coll,
      ghobject_t(hoid, ghobject_t::NO_GEN, get_parent()->whoami_shard().shard),
      &st);
    ECUtil::HashInfo hinfo(ec_impl->get_chunk_count());
    if (r >= 0 && st.st_size > 0) {
      dout(10) << __func__ << ": found on disk, size " << st.st_size << dendl;
      bufferlist bl;
      r = store->getattr(
	hoid.is_temp() ? temp_coll : coll,
	ghobject_t(hoid, ghobject_t::NO_GEN, get_parent()->whoami_shard().shard),
	ECUtil::get_hinfo_key(),
	bl);
      if (r >= 0) {
	bufferlist::iterator bp = bl.begin();
	::decode(hinfo, bp);
	assert(hinfo.get_total_chunk_size() == (uint64_t)st.st_size);
      } else {
	return ECUtil::HashInfoRef();
      }
    }
    ref = unstable_hashinfo_registry.lookup_or_create(hoid, hinfo);
  }
  return ref;
}

void ECBackend::check_op(Op *op)
{
  if (op->pending_apply.empty() && op->on_all_applied) {
    dout(10) << __func__ << " Calling on_all_applied on " << *op << dendl;
    op->on_all_applied->complete(0);
    op->on_all_applied = 0;
  }
  if (op->pending_commit.empty() && op->on_all_commit) {
    dout(10) << __func__ << " Calling on_all_commit on " << *op << dendl;
    op->on_all_commit->complete(0);
    op->on_all_commit = 0;
  }
  if (op->pending_apply.empty() && op->pending_commit.empty()) {
    // done!
    assert(writing.front() == op);
    dout(10) << __func__ << " Completing " << *op << dendl;
    writing.pop_front();
    tid_to_op_map.erase(op->tid);
  }
  for (map<ceph_tid_t, Op>::iterator i = tid_to_op_map.begin();
       i != tid_to_op_map.end();
       ++i) {
    dout(20) << __func__ << " tid " << i->first <<": " << i->second << dendl;
  }
}

void ECBackend::start_write(Op *op) {
  map<shard_id_t, ObjectStore::Transaction> trans;
  for (set<pg_shard_t>::const_iterator i =
	 get_parent()->get_actingbackfill_shards().begin();
       i != get_parent()->get_actingbackfill_shards().end();
       ++i) {
    trans[i->shard];
    trans[i->shard].set_use_tbl(parent->transaction_use_tbl());
  }
  ObjectStore::Transaction empty;
  empty.set_use_tbl(parent->transaction_use_tbl());

  op->t->generate_transactions(
    op->unstable_hash_infos,
    ec_impl,
    get_parent()->get_info().pgid.pgid,
    sinfo,
    &trans,
    &(op->temp_added),
    &(op->temp_cleared));

  dout(10) << "onreadable_sync: " << op->on_local_applied_sync << dendl;

  for (set<pg_shard_t>::const_iterator i =
	 get_parent()->get_actingbackfill_shards().begin();
       i != get_parent()->get_actingbackfill_shards().end();
       ++i) {
    op->pending_apply.insert(*i);
    op->pending_commit.insert(*i);
    map<shard_id_t, ObjectStore::Transaction>::iterator iter =
      trans.find(i->shard);
    assert(iter != trans.end());
    bool should_send = get_parent()->should_send_op(*i, op->hoid);
    pg_stat_t stats =
      should_send ?
      get_info().stats :
      parent->get_shard_info().find(*i)->second.stats;

    ECSubWrite sop(
      get_parent()->whoami_shard(),
      op->tid,
      op->reqid,
      op->hoid,
      stats,
      should_send ? iter->second : empty,
      op->version,
      op->trim_to,
      op->trim_rollback_to,
      op->log_entries,
      op->updated_hit_set_history,
      op->temp_added,
      op->temp_cleared);
    if (*i == get_parent()->whoami_shard()) {
      handle_sub_write(
	get_parent()->whoami_shard(),
	op->client_op,
	sop,
	op->on_local_applied_sync);
      op->on_local_applied_sync = 0;
    } else {
      MOSDECSubOpWrite *r = new MOSDECSubOpWrite(sop);
      r->pgid = spg_t(get_parent()->primary_spg_t().pgid, i->shard);
      r->map_epoch = get_parent()->get_epoch();
      get_parent()->send_message_osd_cluster(
	i->osd, r, get_parent()->get_epoch());
    }
  }
}

int ECBackend::objects_read_sync(
  const hobject_t &hoid,
  uint64_t off,
  uint64_t len,
  uint32_t op_flags,
  bufferlist *bl)
{
  return -EOPNOTSUPP;
}

struct CallClientContexts :
  public GenContext<pair<RecoveryMessages*, ECBackend::read_result_t& > &> {
  ECBackend *ec;
  ECBackend::ClientAsyncReadStatus *status;
  list<pair<boost::tuple<uint64_t, uint64_t, uint32_t>,
	    pair<bufferlist*, Context*> > > to_read;
  CallClientContexts(
    ECBackend *ec,
    ECBackend::ClientAsyncReadStatus *status,
    const list<pair<boost::tuple<uint64_t, uint64_t, uint32_t>,
		    pair<bufferlist*, Context*> > > &to_read)
    : ec(ec), status(status), to_read(to_read) {}
  void finish(pair<RecoveryMessages *, ECBackend::read_result_t &> &in) {
    ECBackend::read_result_t &res = in.second;
    assert(res.returned.size() == to_read.size());
    assert(res.r == 0);
    assert(res.errors.empty());
    for (list<pair<boost::tuple<uint64_t, uint64_t, uint32_t>,
		   pair<bufferlist*, Context*> > >::iterator i = to_read.begin();
	 i != to_read.end();
	 to_read.erase(i++)) {
      pair<uint64_t, uint64_t> adjusted =
	ec->sinfo.offset_len_to_stripe_bounds(make_pair(i->first.get<0>(), i->first.get<1>()));
      assert(res.returned.front().get<0>() == adjusted.first &&
	     res.returned.front().get<1>() == adjusted.second);
      map<int, bufferlist> to_decode;
      bufferlist bl;
      for (map<pg_shard_t, bufferlist>::iterator j =
	     res.returned.front().get<2>().begin();
	   j != res.returned.front().get<2>().end();
	   ++j) {
	to_decode[j->first.shard].claim(j->second);
      }
      ECUtil::decode(
	ec->sinfo,
	ec->ec_impl,
	to_decode,
	&bl);
      assert(i->second.second);
      assert(i->second.first);
      i->second.first->substr_of(
	bl,
	i->first.get<0>() - adjusted.first,
	MIN(i->first.get<1>(), bl.length() - (i->first.get<0>() - adjusted.first)));
      if (i->second.second) {
	i->second.second->complete(i->second.first->length());
      }
      res.returned.pop_front();
    }
    status->complete = true;
    list<ECBackend::ClientAsyncReadStatus> &ip =
      ec->in_progress_client_reads;
    while (ip.size() && ip.front().complete) {
      if (ip.front().on_complete) {
	ip.front().on_complete->complete(0);
	ip.front().on_complete = NULL;
      }
      ip.pop_front();
    }
  }
  ~CallClientContexts() {
    for (list<pair<boost::tuple<uint64_t, uint64_t, uint32_t>,
		   pair<bufferlist*, Context*> > >::iterator i = to_read.begin();
	 i != to_read.end();
	 to_read.erase(i++)) {
      delete i->second.second;
    }
  }
};

void ECBackend::objects_read_async(
  const hobject_t &hoid,
  const list<pair<boost::tuple<uint64_t, uint64_t, uint32_t>,
		  pair<bufferlist*, Context*> > > &to_read,
  Context *on_complete)
{
  in_progress_client_reads.push_back(ClientAsyncReadStatus(on_complete));
  CallClientContexts *c = new CallClientContexts(
    this, &(in_progress_client_reads.back()), to_read);

  list<boost::tuple<uint64_t, uint64_t, uint32_t> > offsets;
  pair<uint64_t, uint64_t> tmp;
  for (list<pair<boost::tuple<uint64_t, uint64_t, uint32_t>,
		 pair<bufferlist*, Context*> > >::const_iterator i =
	 to_read.begin();
       i != to_read.end();
       ++i) {
    tmp = sinfo.offset_len_to_stripe_bounds(make_pair(i->first.get<0>(), i->first.get<1>()));
    offsets.push_back(boost::make_tuple(tmp.first, tmp.second, i->first.get<2>()));
  }

  const vector<int> &chunk_mapping = ec_impl->get_chunk_mapping();
  set<int> want_to_read;
  for (int i = 0; i < (int)ec_impl->get_data_chunk_count(); ++i) {
    int chunk = (int)chunk_mapping.size() > i ? chunk_mapping[i] : i;
    want_to_read.insert(chunk);
  }
  set<pg_shard_t> shards;
  int r = get_min_avail_to_read_shards(
    hoid,
    want_to_read,
    false,
    &shards);
  assert(r == 0);

  map<hobject_t, read_request_t> for_read_op;
  for_read_op.insert(
    make_pair(
      hoid,
      read_request_t(
	hoid,
	offsets,
	shards,
	false,
	c)));

  start_read_op(
    CEPH_MSG_PRIO_DEFAULT,
    for_read_op,
    OpRequestRef());
  return;
}


int ECBackend::objects_get_attrs(
  const hobject_t &hoid,
  map<string, bufferlist> *out)
{
  int r = store->getattrs(
    coll,
    ghobject_t(hoid, ghobject_t::NO_GEN, get_parent()->whoami_shard().shard),
    *out);
  if (r < 0)
    return r;

  for (map<string, bufferlist>::iterator i = out->begin();
       i != out->end();
       ) {
    if (ECUtil::is_hinfo_key_string(i->first))
      out->erase(i++);
    else
      ++i;
  }
  return r;
}

void ECBackend::rollback_append(
  const hobject_t &hoid,
  uint64_t old_size,
  ObjectStore::Transaction *t)
{
  assert(old_size % sinfo.get_stripe_width() == 0);
  t->truncate(
    coll,
    ghobject_t(hoid, ghobject_t::NO_GEN, get_parent()->whoami_shard().shard),
    sinfo.aligned_logical_offset_to_chunk_offset(
      old_size));
}

void ECBackend::be_deep_scrub(
  const hobject_t &poid,
  uint32_t seed,
  ScrubMap::object &o,
  ThreadPool::TPHandle &handle) {
  bufferhash h(-1); // we always used -1
  int r;
  uint64_t stride = cct->_conf->osd_deep_scrub_stride;
  if (stride % sinfo.get_chunk_size())
    stride += sinfo.get_chunk_size() - (stride % sinfo.get_chunk_size());
  uint64_t pos = 0;
  while (true) {
    bufferlist bl;
    handle.reset_tp_timeout();
    r = store->read(
      coll,
      ghobject_t(
	poid, ghobject_t::NO_GEN, get_parent()->whoami_shard().shard),
      pos,
      stride, bl,
      true);
    if (r < 0)
      break;
    if (bl.length() % sinfo.get_chunk_size()) {
      r = -EIO;
      break;
    }
    pos += r;
    h << bl;
    if ((unsigned)r < stride)
      break;
  }

  if (r == -EIO) {
    dout(0) << "_scan_list  " << poid << " got "
	    << r << " on read, read_error" << dendl;
    o.read_error = true;
  }

  ECUtil::HashInfoRef hinfo = get_hash_info(poid);
  if (!hinfo) {
    dout(0) << "_scan_list  " << poid << " could not retrieve hash info" << dendl;
    o.read_error = true;
    o.digest_present = false;
  } else {
    if (hinfo->get_chunk_hash(get_parent()->whoami_shard().shard) != h.digest()) {
      dout(0) << "_scan_list  " << poid << " got incorrect hash on read" << dendl;
      o.read_error = true;
    }

    if (hinfo->get_total_chunk_size() != pos) {
      dout(0) << "_scan_list  " << poid << " got incorrect size on read" << dendl;
      o.read_error = true;
    }

    /* We checked above that we match our own stored hash.  We cannot
     * send a hash of the actual object, so instead we simply send
     * our locally stored hash of shard 0 on the assumption that if
     * we match our chunk hash and our recollection of the hash for
     * chunk 0 matches that of our peers, there is likely no corruption.
     */
    o.digest = hinfo->get_chunk_hash(0);
    o.digest_present = true;
  }

  o.omap_digest = seed;
  o.omap_digest_present = true;
}
