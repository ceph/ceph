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

#include <iostream>
#include <sstream>

#include "ECBackend.h"
#include "messages/MOSDPGPush.h"
#include "messages/MOSDPGPushReply.h"
#include "messages/MOSDECSubOpWrite.h"
#include "messages/MOSDECSubOpWriteReply.h"
#include "messages/MOSDECSubOpRead.h"
#include "messages/MOSDECSubOpReadReply.h"
#include "ECMsgTypes.h"

#include "PrimaryLogPG.h"
#include "osd_tracer.h"

#define dout_context cct
#define dout_subsys ceph_subsys_osd
#define DOUT_PREFIX_ARGS this
#undef dout_prefix
#define dout_prefix _prefix(_dout, this)

using std::dec;
using std::hex;
using std::less;
using std::list;
using std::make_pair;
using std::map;
using std::pair;
using std::ostream;
using std::set;
using std::string;
using std::unique_ptr;
using std::vector;

using ceph::bufferhash;
using ceph::bufferlist;
using ceph::bufferptr;
using ceph::ErasureCodeInterfaceRef;
using ceph::Formatter;

static ostream& _prefix(std::ostream *_dout, ECBackend *pgb) {
  return pgb->get_parent()->gen_dbg_prefix(*_dout);
}

static ostream& _prefix(std::ostream *_dout, ECBackend::ECRecoveryBackend *pgb) {
  return pgb->get_parent()->gen_dbg_prefix(*_dout);
}

struct ECBackend::ECRecoveryBackend::ECRecoveryHandle : public PGBackend::RecoveryHandle {
  list<ECCommon::RecoveryBackend::RecoveryOp> ops;
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

ECBackend::ECBackend(
  PGBackend::Listener *pg,
  const coll_t &coll,
  ObjectStore::CollectionHandle &ch,
  ObjectStore *store,
  CephContext *cct,
  ErasureCodeInterfaceRef ec_impl,
  uint64_t stripe_width)
  : PGBackend(cct, pg, store, coll, ch),
    read_pipeline(cct, ec_impl, this->sinfo, get_parent()->get_eclistener(), *this),
    rmw_pipeline(cct, ec_impl, this->sinfo, get_parent()->get_eclistener(), *this),
    recovery_backend(cct, this->coll, ec_impl, this->sinfo, read_pipeline, unstable_hashinfo_registry, get_parent(), this),
    ec_impl(ec_impl),
    sinfo(ec_impl->get_data_chunk_count(), stripe_width),
    unstable_hashinfo_registry(cct, ec_impl) {
  ceph_assert((ec_impl->get_data_chunk_count() *
	  ec_impl->get_chunk_size(stripe_width)) == stripe_width);
}

PGBackend::RecoveryHandle *ECBackend::open_recovery_op()
{
  return static_cast<PGBackend::RecoveryHandle*>(
    recovery_backend.open_recovery_op());
}

ECBackend::ECRecoveryBackend::ECRecoveryHandle*
ECBackend::ECRecoveryBackend::open_recovery_op()
{
  return new ECRecoveryHandle;
}

void ECBackend::handle_recovery_push(
  const PushOp &op,
  RecoveryMessages *m,
  bool is_repair)
{
  if (get_parent()->pg_is_remote_backfilling()) {
    get_parent()->pg_add_local_num_bytes(op.data.length());
    get_parent()->pg_add_num_bytes(op.data.length() * get_ec_data_chunk_count());
    dout(10) << __func__ << " " << op.soid
             << " add new actual data by " << op.data.length()
             << " add new num_bytes by " << op.data.length() * get_ec_data_chunk_count()
             << dendl;
  }

  recovery_backend.handle_recovery_push(op, m, is_repair);

  if (op.after_progress.data_complete &&
     !(get_parent()->pgb_is_primary()) &&
     get_parent()->pg_is_remote_backfilling()) {
    struct stat st;
    int r = store->stat(ch, ghobject_t(op.soid, ghobject_t::NO_GEN,
                        get_parent()->whoami_shard().shard), &st);
    if (r == 0) {
      get_parent()->pg_sub_local_num_bytes(st.st_size);
      // XXX: This can be way overestimated for small objects
      get_parent()->pg_sub_num_bytes(st.st_size * get_ec_data_chunk_count());
      dout(10) << __func__ << " " << op.soid
               << " sub actual data by " << st.st_size
               << " sub num_bytes by " << st.st_size * get_ec_data_chunk_count()
               << dendl;
    }
  }
}

void ECBackend::ECRecoveryBackend::maybe_load_obc(
  const std::map<std::string, ceph::bufferlist, std::less<>>& raw_attrs,
  RecoveryOp &op)
{
  if (!op.obc) {
    // attrs only reference the origin bufferlist (decode from
    // ECSubReadReply message) whose size is much greater than attrs
    // in recovery. If obc cache it (get_obc maybe cache the attr),
    // this causes the whole origin bufferlist would not be free
    // until obc is evicted from obc cache. So rebuild the
    // bufferlist before cache it.
    for (map<string, bufferlist>::iterator it = op.xattrs.begin();
         it != op.xattrs.end();
         ++it) {
      it->second.rebuild();
    }
    // Need to remove ECUtil::get_hinfo_key() since it should not leak out
    // of the backend (see bug #12983)
    map<string, bufferlist, less<>> sanitized_attrs(op.xattrs);
    sanitized_attrs.erase(ECUtil::get_hinfo_key());
    op.obc = get_parent()->get_obc(op.hoid, sanitized_attrs);
    ceph_assert(op.obc);
    op.recovery_info.size = op.obc->obs.oi.size;
    op.recovery_info.oi = op.obc->obs.oi;
  }
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
  void finish(int) override {
    std::vector<std::pair<int, Message*>> messages;
    messages.reserve(replies.size());
    for (map<int, MOSDPGPushReply*>::iterator i = replies.begin();
	 i != replies.end();
	 ++i) {
      messages.push_back(std::make_pair(i->first, i->second));
    }
    if (!messages.empty()) {
      l->send_message_osd_cluster(messages, epoch);
    }
    replies.clear();
  }
  ~SendPushReplies() override {
    for (map<int, MOSDPGPushReply*>::iterator i = replies.begin();
	 i != replies.end();
	 ++i) {
      i->second->put();
    }
    replies.clear();
  }
};

void ECBackend::ECRecoveryBackend::commit_txn_send_replies(
  ceph::os::Transaction&& txn,
  std::map<int, MOSDPGPushReply*> replies)
{
  txn.register_on_complete(
      get_parent()->bless_context(
        new SendPushReplies(
          get_parent(),
          get_osdmap_epoch(),
          replies)));
  get_parent()->queue_transaction(std::move(txn));
}

void ECBackend::run_recovery_op(
  RecoveryHandle *_h,
  int priority)
{
  ceph_assert(_h);
  auto &h = static_cast<ECBackend::ECRecoveryBackend::ECRecoveryHandle&>(*_h);
  recovery_backend.run_recovery_op(h, priority);
  send_recovery_deletes(priority, h.deletes);
  delete _h;
}

void ECBackend::ECRecoveryBackend::run_recovery_op(
  ECBackend::ECRecoveryBackend::ECRecoveryHandle &h,
  int priority)
{
  RecoveryMessages m;
  for (list<RecoveryOp>::iterator i = h.ops.begin();
       i != h.ops.end();
       ++i) {
    dout(10) << __func__ << ": starting " << *i << dendl;
    ceph_assert(!recovery_ops.count(i->hoid));
    RecoveryOp &op = recovery_ops.insert(make_pair(i->hoid, *i)).first->second;
    continue_recovery_op(op, &m);
  }
  dispatch_recovery_messages(m, priority);
}

int ECBackend::recover_object(
  const hobject_t &hoid,
  eversion_t v,
  ObjectContextRef head,
  ObjectContextRef obc,
  RecoveryHandle *_h)
{
  auto *h = static_cast<ECBackend::ECRecoveryBackend::ECRecoveryHandle*>(_h);
  h->ops.push_back(recovery_backend.recover_object(hoid, v, head, obc));
  return 0;
}

bool ECBackend::can_handle_while_inactive(
  OpRequestRef _op)
{
  return false;
}

bool ECBackend::_handle_message(
  OpRequestRef _op)
{
  dout(10) << __func__ << ": " << *_op->get_req() << dendl;
  int priority = _op->get_req()->get_priority();
  switch (_op->get_req()->get_type()) {
  case MSG_OSD_EC_WRITE: {
    // NOTE: this is non-const because handle_sub_write modifies the embedded
    // ObjectStore::Transaction in place (and then std::move's it).  It does
    // not conflict with ECSubWrite's operator<<.
    MOSDECSubOpWrite *op = static_cast<MOSDECSubOpWrite*>(
      _op->get_nonconst_req());
    parent->maybe_preempt_replica_scrub(op->op.soid);
    handle_sub_write(op->op.from, _op, op->op, _op->pg_trace, *get_parent()->get_eclistener());
    return true;
  }
  case MSG_OSD_EC_WRITE_REPLY: {
    const MOSDECSubOpWriteReply *op = static_cast<const MOSDECSubOpWriteReply*>(
      _op->get_req());
    handle_sub_write_reply(op->op.from, op->op, _op->pg_trace);
    return true;
  }
  case MSG_OSD_EC_READ: {
    auto op = _op->get_req<MOSDECSubOpRead>();
    MOSDECSubOpReadReply *reply = new MOSDECSubOpReadReply;
    reply->pgid = get_parent()->primary_spg_t();
    reply->map_epoch = get_osdmap_epoch();
    reply->min_epoch = get_parent()->get_interval_start_epoch();
    handle_sub_read(op->op.from, op->op, &(reply->op), _op->pg_trace);
    reply->trace = _op->pg_trace;
    get_parent()->send_message_osd_cluster(
      reply, _op->get_req()->get_connection());
    return true;
  }
  case MSG_OSD_EC_READ_REPLY: {
    // NOTE: this is non-const because handle_sub_read_reply steals resulting
    // buffers.  It does not conflict with ECSubReadReply operator<<.
    MOSDECSubOpReadReply *op = static_cast<MOSDECSubOpReadReply*>(
      _op->get_nonconst_req());
    handle_sub_read_reply(op->op.from, op->op, _op->pg_trace);
    // dispatch_recovery_messages() in the case of recovery_reads
    // is called via the `on_complete` callback
    return true;
  }
  case MSG_OSD_PG_PUSH: {
    auto op = _op->get_req<MOSDPGPush>();
    RecoveryMessages rm;
    for (vector<PushOp>::const_iterator i = op->pushes.begin();
	 i != op->pushes.end();
	 ++i) {
      handle_recovery_push(*i, &rm, op->is_repair);
    }
    recovery_backend.dispatch_recovery_messages(rm, priority);
    return true;
  }
  case MSG_OSD_PG_PUSH_REPLY: {
    const MOSDPGPushReply *op = static_cast<const MOSDPGPushReply *>(
      _op->get_req());
    RecoveryMessages rm;
    for (vector<PushReplyOp>::const_iterator i = op->replies.begin();
	 i != op->replies.end();
	 ++i) {
      recovery_backend.handle_recovery_push_reply(*i, op->from, &rm);
    }
    recovery_backend.dispatch_recovery_messages(rm, priority);
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
  const ZTracer::Trace trace;
  SubWriteCommitted(
    ECBackend *pg,
    OpRequestRef msg,
    ceph_tid_t tid,
    eversion_t version,
    eversion_t last_complete,
    const ZTracer::Trace &trace)
    : pg(pg), msg(msg), tid(tid),
      version(version), last_complete(last_complete), trace(trace) {}
  void finish(int) override {
    if (msg)
      msg->mark_event("sub_op_committed");
    pg->sub_write_committed(tid, version, last_complete, trace);
  }
};
void ECBackend::sub_write_committed(
  ceph_tid_t tid, eversion_t version, eversion_t last_complete,
  const ZTracer::Trace &trace) {
  if (get_parent()->pgb_is_primary()) {
    ECSubWriteReply reply;
    reply.tid = tid;
    reply.last_complete = last_complete;
    reply.committed = true;
    reply.applied = true;
    reply.from = get_parent()->whoami_shard();
    handle_sub_write_reply(
      get_parent()->whoami_shard(),
      reply, trace);
  } else {
    get_parent()->update_last_complete_ondisk(last_complete);
    MOSDECSubOpWriteReply *r = new MOSDECSubOpWriteReply;
    r->pgid = get_parent()->primary_spg_t();
    r->map_epoch = get_osdmap_epoch();
    r->min_epoch = get_parent()->get_interval_start_epoch();
    r->op.tid = tid;
    r->op.last_complete = last_complete;
    r->op.committed = true;
    r->op.applied = true;
    r->op.from = get_parent()->whoami_shard();
    r->set_priority(CEPH_MSG_PRIO_HIGH);
    r->trace = trace;
    r->trace.event("sending sub op commit");
    get_parent()->send_message_osd_cluster(
      get_parent()->primary_shard().osd, r, get_osdmap_epoch());
  }
}

void ECBackend::handle_sub_write(
  pg_shard_t from,
  OpRequestRef msg,
  ECSubWrite &op,
  const ZTracer::Trace &trace,
  ECListener&)
{
  if (msg) {
    msg->mark_event("sub_op_started");
  }
  trace.event("handle_sub_write");

  if (!get_parent()->pgb_is_primary())
    get_parent()->update_stats(op.stats);
  ObjectStore::Transaction localt;
  if (!op.temp_added.empty()) {
    add_temp_objs(op.temp_added);
  }
  if (op.backfill_or_async_recovery) {
    for (set<hobject_t>::iterator i = op.temp_removed.begin();
	 i != op.temp_removed.end();
	 ++i) {
      dout(10) << __func__ << ": removing object " << *i
	       << " since we won't get the transaction" << dendl;
      localt.remove(
	coll,
	ghobject_t(
	  *i,
	  ghobject_t::NO_GEN,
	  get_parent()->whoami_shard().shard));
    }
  }
  clear_temp_objs(op.temp_removed);
  dout(30) << __func__ << " missing before " << get_parent()->get_log().get_missing().get_items() << dendl;
  // flag set to true during async recovery
  bool async = false;
  pg_missing_tracker_t pmissing = get_parent()->get_local_missing();
  if (pmissing.is_missing(op.soid)) {
    async = true;
    dout(30) << __func__ << " is_missing " << pmissing.is_missing(op.soid) << dendl;
    for (auto &&e: op.log_entries) {
      dout(30) << " add_next_event entry " << e << dendl;
      get_parent()->add_local_next_event(e);
      dout(30) << " entry is_delete " << e.is_delete() << dendl;
    }
  }
  get_parent()->log_operation(
    std::move(op.log_entries),
    op.updated_hit_set_history,
    op.trim_to,
    op.pg_committed_to,
    op.pg_committed_to,
    !op.backfill_or_async_recovery,
    localt,
    async);

  if (!get_parent()->pg_is_undersized() &&
      (unsigned)get_parent()->whoami_shard().shard >=
      ec_impl->get_data_chunk_count())
    op.t.set_fadvise_flag(CEPH_OSD_OP_FLAG_FADVISE_DONTNEED);

  localt.register_on_commit(
    get_parent()->bless_context(
      new SubWriteCommitted(
	this, msg, op.tid,
	op.at_version,
	get_parent()->get_info().last_complete, trace)));
  vector<ObjectStore::Transaction> tls;
  tls.reserve(2);
  tls.push_back(std::move(op.t));
  tls.push_back(std::move(localt));
  get_parent()->queue_transactions(tls, msg);
  dout(30) << __func__ << " missing after" << get_parent()->get_log().get_missing().get_items() << dendl;
  if (op.at_version != eversion_t()) {
    // dummy rollforward transaction doesn't get at_version (and doesn't advance it)
    get_parent()->op_applied(op.at_version);
  }
}

void ECBackend::handle_sub_read(
  pg_shard_t from,
  const ECSubRead &op,
  ECSubReadReply *reply,
  const ZTracer::Trace &trace)
{
  trace.event("handle sub read");
  shard_id_t shard = get_parent()->whoami_shard().shard;
  for(auto i = op.to_read.begin();
      i != op.to_read.end();
      ++i) {
    int r = 0;
    for (auto j = i->second.begin(); j != i->second.end(); ++j) {
      bufferlist bl;
      if ((op.subchunks.find(i->first)->second.size() == 1) && 
          (op.subchunks.find(i->first)->second.front().second == 
                                            ec_impl->get_sub_chunk_count())) {
        dout(20) << __func__ << " case1: reading the complete chunk/shard." << dendl;
        r = store->read(
	  ch,
	  ghobject_t(i->first, ghobject_t::NO_GEN, shard),
	  j->get<0>(),
	  j->get<1>(),
	  bl, j->get<2>()); // Allow EIO return
      } else {
        int subchunk_size =
          sinfo.get_chunk_size() / ec_impl->get_sub_chunk_count();
        dout(20) << __func__ << " case2: going to do fragmented read;"
		 << " subchunk_size=" << subchunk_size
		 << " chunk_size=" << sinfo.get_chunk_size() << dendl;
        bool error = false;
        for (int m = 0; m < (int)j->get<1>() && !error;
             m += sinfo.get_chunk_size()) {
          for (auto &&k:op.subchunks.find(i->first)->second) {
            bufferlist bl0;
            r = store->read(
                ch,
                ghobject_t(i->first, ghobject_t::NO_GEN, shard),
                j->get<0>() + m + (k.first)*subchunk_size,
                (k.second)*subchunk_size,
                bl0, j->get<2>());
            if (r < 0) {
              error = true;
              break;
            }
            bl.claim_append(bl0);
          }
        }
      }

      if (r < 0) {
	// if we are doing fast reads, it's possible for one of the shard
	// reads to cross paths with another update and get a (harmless)
	// ENOENT.  Suppress the message to the cluster log in that case.
	if (r == -ENOENT && get_parent()->get_pool().fast_read) {
	  dout(5) << __func__ << ": Error " << r
		  << " reading " << i->first << ", fast read, probably ok"
		  << dendl;
	} else {
	  get_parent()->clog_error() << "Error " << r
				     << " reading object "
				     << i->first;
	  dout(5) << __func__ << ": Error " << r
		  << " reading " << i->first << dendl;
	}
	goto error;
      } else {
        dout(20) << __func__ << " read request=" << j->get<1>() << " r=" << r << " len=" << bl.length() << dendl;
	reply->buffers_read[i->first].push_back(
	  make_pair(
	    j->get<0>(),
	    bl)
	  );
      }

      if (!get_parent()->get_pool().allows_ecoverwrites()) {
	// This shows that we still need deep scrub because large enough files
	// are read in sections, so the digest check here won't be done here.
	// Do NOT check osd_read_eio_on_bad_digest here.  We need to report
	// the state of our chunk in case other chunks could substitute.
        ECUtil::HashInfoRef hinfo;
        map<string, bufferlist, less<>> attrs;
	struct stat st;
	int r = object_stat(i->first, &st);
        if (r >= 0) {
	  dout(10) << __func__ << ": found on disk, size " << st.st_size << dendl;
	  r = PGBackend::objects_get_attrs(i->first, &attrs);
	}
	if (r >= 0) {
	  hinfo = unstable_hashinfo_registry.get_hash_info(i->first, false, attrs, st.st_size);
	} else {
	  derr << __func__ << ": access (attrs) on " << i->first << " failed: "
	       << cpp_strerror(r) << dendl;
	}
        if (!hinfo) {
          r = -EIO;
          get_parent()->clog_error() << "Corruption detected: object "
                                     << i->first
                                     << " is missing hash_info";
          dout(5) << __func__ << ": No hinfo for " << i->first << dendl;
          goto error;
        }
	ceph_assert(hinfo->has_chunk_hash());
	if ((bl.length() == hinfo->get_total_chunk_size()) &&
	    (j->get<0>() == 0)) {
	  dout(20) << __func__ << ": Checking hash of " << i->first << dendl;
	  bufferhash h(-1);
	  h << bl;
	  if (h.digest() != hinfo->get_chunk_hash(shard)) {
	    get_parent()->clog_error() << "Bad hash for " << i->first << " digest 0x"
				       << hex << h.digest() << " expected 0x" << hinfo->get_chunk_hash(shard) << dec;
	    dout(5) << __func__ << ": Bad hash for " << i->first << " digest 0x"
		    << hex << h.digest() << " expected 0x" << hinfo->get_chunk_hash(shard) << dec << dendl;
	    r = -EIO;
	    goto error;
	  }
	}
      }
    }
    continue;
error:
    // Do NOT check osd_read_eio_on_bad_digest here.  We need to report
    // the state of our chunk in case other chunks could substitute.
    reply->buffers_read.erase(i->first);
    reply->errors[i->first] = r;
  }
  for (set<hobject_t>::iterator i = op.attrs_to_read.begin();
       i != op.attrs_to_read.end();
       ++i) {
    dout(10) << __func__ << ": fulfilling attr request on "
	     << *i << dendl;
    if (reply->errors.count(*i))
      continue;
    int r = store->getattrs(
      ch,
      ghobject_t(
	*i, ghobject_t::NO_GEN, shard),
      reply->attrs_read[*i]);
    if (r < 0) {
      // If we read error, we should not return the attrs too.
      reply->attrs_read.erase(*i);
      reply->buffers_read.erase(*i);
      reply->errors[*i] = r;
    }
  }
  reply->from = get_parent()->whoami_shard();
  reply->tid = op.tid;
}

void ECBackend::handle_sub_read_n_reply(
  pg_shard_t from,
  ECSubRead &op,
  const ZTracer::Trace &trace)
{
  ECSubReadReply reply;
  handle_sub_read(from, op, &reply, trace);
  handle_sub_read_reply(from, reply, trace);
}

void ECBackend::handle_sub_write_reply(
  pg_shard_t from,
  const ECSubWriteReply &op,
  const ZTracer::Trace &trace)
{
  map<ceph_tid_t, RMWPipeline::OpRef>::iterator i = rmw_pipeline.tid_to_op_map.find(op.tid);
  ceph_assert(i != rmw_pipeline.tid_to_op_map.end());
  if (op.committed) {
    trace.event("sub write committed");
    ceph_assert(i->second->pending_commit.count(from));
    i->second->pending_commit.erase(from);
    if (from != get_parent()->whoami_shard()) {
      get_parent()->update_peer_last_complete_ondisk(from, op.last_complete);
    }
  }
  if (op.applied) {
    trace.event("sub write applied");
    ceph_assert(i->second->pending_apply.count(from));
    i->second->pending_apply.erase(from);
  }

  if (i->second->pending_commit.empty() &&
      i->second->on_all_commit &&
      // also wait for apply, to preserve ordering with luminous peers.
      i->second->pending_apply.empty()) {
    dout(10) << __func__ << " Calling on_all_commit on " << i->second << dendl;
    i->second->on_all_commit->complete(0);
    i->second->on_all_commit = 0;
    i->second->trace.event("ec write all committed");
  }
  rmw_pipeline.check_ops();
}

void ECBackend::handle_sub_read_reply(
  pg_shard_t from,
  ECSubReadReply &op,
  const ZTracer::Trace &trace)
{
  trace.event("ec sub read reply");
  dout(10) << __func__ << ": reply " << op << dendl;
  map<ceph_tid_t, ReadOp>::iterator iter = read_pipeline.tid_to_read_map.find(op.tid);
  if (iter == read_pipeline.tid_to_read_map.end()) {
    //canceled
    dout(20) << __func__ << ": dropped " << op << dendl;
    return;
  }
  ReadOp &rop = iter->second;
  for (auto i = op.buffers_read.begin();
       i != op.buffers_read.end();
       ++i) {
    ceph_assert(!op.errors.count(i->first));	// If attribute error we better not have sent a buffer
    if (!rop.to_read.count(i->first)) {
      // We canceled this read! @see filter_read_op
      dout(20) << __func__ << " to_read skipping" << dendl;
      continue;
    }
    list<ec_align_t>::const_iterator req_iter =
      rop.to_read.find(i->first)->second.to_read.begin();
    list<
      boost::tuple<
	uint64_t, uint64_t, map<pg_shard_t, bufferlist> > >::iterator riter =
      rop.complete[i->first].returned.begin();
    for (list<pair<uint64_t, bufferlist> >::iterator j = i->second.begin();
	 j != i->second.end();
	 ++j, ++req_iter, ++riter) {
      ceph_assert(req_iter != rop.to_read.find(i->first)->second.to_read.end());
      ceph_assert(riter != rop.complete[i->first].returned.end());
      pair<uint64_t, uint64_t> aligned =
	sinfo.chunk_aligned_offset_len_to_chunk(
	  make_pair(req_iter->offset, req_iter->size));
      ceph_assert(aligned.first == j->first);
      riter->get<2>()[from] = std::move(j->second);
    }
  }
  for (auto i = op.attrs_read.begin();
       i != op.attrs_read.end();
       ++i) {
    ceph_assert(!op.errors.count(i->first));	// if read error better not have sent an attribute
    if (!rop.to_read.count(i->first)) {
      // We canceled this read! @see filter_read_op
      dout(20) << __func__ << " to_read skipping" << dendl;
      continue;
    }
    rop.complete[i->first].attrs.emplace();
    (*(rop.complete[i->first].attrs)).swap(i->second);
  }
  for (auto i = op.errors.begin();
       i != op.errors.end();
       ++i) {
    rop.complete[i->first].errors.insert(
      make_pair(
	from,
	i->second));
    dout(20) << __func__ << " shard=" << from << " error=" << i->second << dendl;
  }

  map<pg_shard_t, set<ceph_tid_t> >::iterator siter =
					read_pipeline.shard_to_read_map.find(from);
  ceph_assert(siter != read_pipeline.shard_to_read_map.end());
  ceph_assert(siter->second.count(op.tid));
  siter->second.erase(op.tid);

  ceph_assert(rop.in_progress.count(from));
  rop.in_progress.erase(from);
  unsigned is_complete = 0;
  bool need_resend = false;
  // For redundant reads check for completion as each shard comes in,
  // or in a non-recovery read check for completion once all the shards read.
  if (rop.do_redundant_reads || rop.in_progress.empty()) {
    for (map<hobject_t, read_result_t>::const_iterator iter =
        rop.complete.begin();
      iter != rop.complete.end();
      ++iter) {
      set<int> have;
      for (map<pg_shard_t, bufferlist>::const_iterator j =
          iter->second.returned.front().get<2>().begin();
        j != iter->second.returned.front().get<2>().end();
        ++j) {
        have.insert(j->first.shard);
        dout(20) << __func__ << " have shard=" << j->first.shard << dendl;
      }
      map<int, vector<pair<int, int>>> dummy_minimum;
      int err;
      if ((err = ec_impl->minimum_to_decode(rop.want_to_read[iter->first], have, &dummy_minimum)) < 0) {
	dout(20) << __func__ << " minimum_to_decode failed" << dendl;
        if (rop.in_progress.empty()) {
	  // If we don't have enough copies, try other pg_shard_ts if available.
	  // During recovery there may be multiple osds with copies of the same shard,
	  // so getting EIO from one may result in multiple passes through this code path.
	  if (!rop.do_redundant_reads) {
	    int r = read_pipeline.send_all_remaining_reads(iter->first, rop);
	    if (r == 0) {
	      // We changed the rop's to_read and not incrementing is_complete
	      need_resend = true;
	      continue;
	    }
	    // Couldn't read any additional shards so handle as completed with errors
	  }
	  // We don't want to confuse clients / RBD with objectstore error
	  // values in particular ENOENT.  We may have different error returns
	  // from different shards, so we'll return minimum_to_decode() error
	  // (usually EIO) to reader.  It is likely an error here is due to a
	  // damaged pg.
	  rop.complete[iter->first].r = err;
	  ++is_complete;
	}
      } else {
        ceph_assert(rop.complete[iter->first].r == 0);
	if (!rop.complete[iter->first].errors.empty()) {
	  if (cct->_conf->osd_read_ec_check_for_errors) {
	    dout(10) << __func__ << ": Not ignoring errors, use one shard err=" << err << dendl;
	    err = rop.complete[iter->first].errors.begin()->second;
            rop.complete[iter->first].r = err;
	  } else {
	    get_parent()->clog_warn() << "Error(s) ignored for "
				       << iter->first << " enough copies available";
	    dout(10) << __func__ << " Error(s) ignored for " << iter->first
		     << " enough copies available" << dendl;
	    rop.complete[iter->first].errors.clear();
	  }
	}
	// avoid re-read for completed object as we may send remaining reads for uncopmpleted objects
	rop.to_read.at(iter->first).need.clear();
	rop.to_read.at(iter->first).want_attrs = false;
	++is_complete;
      }
    }
  }
  if (need_resend) {
    read_pipeline.do_read_op(rop);
  } else if (rop.in_progress.empty() || 
             is_complete == rop.complete.size()) {
    dout(20) << __func__ << " Complete: " << rop << dendl;
    rop.trace.event("ec read complete");
    read_pipeline.complete_read_op(rop);
  } else {
    dout(10) << __func__ << " readop not complete: " << rop << dendl;
  }
}

void ECBackend::check_recovery_sources(const OSDMapRef& osdmap)
{
  struct FinishReadOp : public GenContext<ThreadPool::TPHandle&>  {
    ECCommon::ReadPipeline& read_pipeline;
    ceph_tid_t tid;
    FinishReadOp(ECCommon::ReadPipeline& read_pipeline, ceph_tid_t tid)
      : read_pipeline(read_pipeline), tid(tid) {}
    void finish(ThreadPool::TPHandle&) override {
      auto ropiter = read_pipeline.tid_to_read_map.find(tid);
      ceph_assert(ropiter != read_pipeline.tid_to_read_map.end());
      read_pipeline.complete_read_op(ropiter->second);
    }
  };
  read_pipeline.check_recovery_sources(
    osdmap,
    [this] (const hobject_t& obj) {
      recovery_backend.recovery_ops.erase(obj);
    },
    [this] (const ReadOp& op) {
      get_parent()->schedule_recovery_work(
        get_parent()->bless_unlocked_gencontext(
          new FinishReadOp(read_pipeline, op.tid)),
        1);
    });
}

void ECBackend::on_change()
{
  rmw_pipeline.on_change();
  read_pipeline.on_change();
  clear_recovery_state();
}

void ECBackend::clear_recovery_state()
{
  recovery_backend.recovery_ops.clear();
}

void ECBackend::dump_recovery_info(Formatter *f) const
{
  f->open_array_section("recovery_ops");
  for (map<hobject_t, RecoveryBackend::RecoveryOp>::const_iterator i = recovery_backend.recovery_ops.begin();
       i != recovery_backend.recovery_ops.end();
       ++i) {
    f->open_object_section("op");
    i->second.dump(f);
    f->close_section();
  }
  f->close_section();
  f->open_array_section("read_ops");
  for (map<ceph_tid_t, ReadOp>::const_iterator i = read_pipeline.tid_to_read_map.begin();
       i != read_pipeline.tid_to_read_map.end();
       ++i) {
    f->open_object_section("read_op");
    i->second.dump(f);
    f->close_section();
  }
  f->close_section();
}

struct ECClassicalOp : ECCommon::RMWPipeline::Op {
  PGTransactionUPtr t;

  void generate_transactions(
      ceph::ErasureCodeInterfaceRef &ecimpl,
      pg_t pgid,
      const ECUtil::stripe_info_t &sinfo,
      std::map<hobject_t,extent_map> *written,
      std::map<shard_id_t, ObjectStore::Transaction> *transactions,
      DoutPrefixProvider *dpp,
      const ceph_release_t require_osd_release) final
  {
    assert(t);
    ECTransaction::generate_transactions(
      t.get(),
      plan,
      ecimpl,
      pgid,
      sinfo,
      remote_read_result,
      log_entries,
      written,
      transactions,
      &temp_added,
      &temp_cleared,
      dpp,
      require_osd_release);
  }

  template <typename F>
  static ECTransaction::WritePlan get_write_plan(
    const ECUtil::stripe_info_t &sinfo,
    PGTransaction& t,
    F &&get_hinfo,
    DoutPrefixProvider *dpp)
  {
    return ECTransaction::get_write_plan(
      sinfo,
      t,
      std::forward<F>(get_hinfo),
      dpp);
  }
};

std::tuple<
  int,
  map<string, bufferlist, less<>>,
  size_t
> ECBackend::get_attrs_n_size_from_disk(const hobject_t& hoid)
{
  struct stat st;
  if (int r = object_stat(hoid, &st); r < 0) {
    dout(10) << __func__ << ": stat error " << r << " on" << hoid << dendl;
    return { r, {}, 0 };
  }
  map<string, bufferlist, less<>> real_attrs;
  if (int r = PGBackend::objects_get_attrs(hoid, &real_attrs); r < 0) {
    dout(10) << __func__ << ": get attr error " << r << " on" << hoid << dendl;
    return { r, {}, 0 };
  }
  return { 0, real_attrs, st.st_size };
}

void ECBackend::submit_transaction(
  const hobject_t &hoid,
  const object_stat_sum_t &delta_stats,
  const eversion_t &at_version,
  PGTransactionUPtr &&t,
  const eversion_t &trim_to,
  const eversion_t &pg_committed_to,
  vector<pg_log_entry_t>&& log_entries,
  std::optional<pg_hit_set_history_t> &hset_history,
  Context *on_all_commit,
  ceph_tid_t tid,
  osd_reqid_t reqid,
  OpRequestRef client_op
  )
{
  auto op = std::make_unique<ECClassicalOp>();
  op->t = std::move(t);
  op->hoid = hoid;
  op->delta_stats = delta_stats;
  op->version = at_version;
  op->trim_to = trim_to;
  /* We update PeeringState::pg_committed_to via the callback
   * invoked from ECBackend::handle_sub_write_reply immediately
   * before updating rmw_pipeline.commited_to via
   * rmw_pipeline.check_ops()->try_finish_rmw(), so these will
   * *usually* match.  However, the PrimaryLogPG::submit_log_entries
   * pathway can perform an out-of-band log update which updates
   * PeeringState::pg_committed_to independently.  Thus, the value
   * passed in is the right one to use. */
  op->pg_committed_to = pg_committed_to;
  op->log_entries = log_entries;
  std::swap(op->updated_hit_set_history, hset_history);
  op->on_all_commit = on_all_commit;
  op->tid = tid;
  op->reqid = reqid;
  op->client_op = client_op;
  if (client_op) {
    op->trace = client_op->pg_trace;
  }
  op->plan = op->get_write_plan(
    sinfo,
    *(op->t),
    [&](const hobject_t &i) {
      dout(10) << "submit_transaction: obtaining hash info for get_write_plan" << dendl;
      ECUtil::HashInfoRef ref;
      if (auto [r, attrs, size] = get_attrs_n_size_from_disk(i); r >= 0 || r == -ENOENT) {
        ref = unstable_hashinfo_registry.get_hash_info(
	  i,
	  true,
	  attrs, //op->t->obc_map[hoid]->attr_cache,
	  size); //op->t->obc_map[hoid]->obs.oi.size);
      }
      if (!ref) {
	derr << __func__ << ": get_hash_info(" << i << ")"
	     << " returned a null pointer and there is no "
	     << " way to recover from such an error in this "
	     << " context" << dendl;
	ceph_abort();
      }
      return ref;
    },
    get_parent()->get_dpp());
  dout(10) << __func__ << ": op " << *op << " starting" << dendl;
  rmw_pipeline.start_rmw(std::move(op));
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

static bool should_partial_read(
  const ECUtil::stripe_info_t& sinfo,
  uint64_t off,
  uint32_t len,
  bool fast_read)
{
  // Don't partial read if we are doing a fast_read
  if (fast_read) {
    return false;
  }
  // Same stripe only
  return sinfo.offset_length_is_same_stripe(off, len);
}

void ECBackend::objects_read_async(
  const hobject_t &hoid,
  const list<pair<ECCommon::ec_align_t,
                  pair<bufferlist*, Context*>>> &to_read,
  Context *on_complete,
  bool fast_read)
{
  map<hobject_t,std::list<ec_align_t>> reads;

  uint32_t flags = 0;
  extent_set es;
  for (const auto& [read, ctx] : to_read) {
    pair<uint64_t, uint64_t> tmp;
    if (!cct->_conf->osd_ec_partial_reads ||
	!should_partial_read(sinfo, read.offset, read.size, fast_read)) {
      tmp = sinfo.offset_len_to_stripe_bounds(make_pair(read.offset, read.size));
    } else {
      tmp = sinfo.offset_len_to_chunk_bounds(make_pair(read.offset, read.size));
    }
    es.union_insert(tmp.first, tmp.second);
    flags |= read.flags;
  }

  if (!es.empty()) {
    auto &offsets = reads[hoid];
    for (auto j = es.begin();
	 j != es.end();
	 ++j) {
      offsets.emplace_back(ec_align_t{j.get_start(), j.get_len(), flags});
    }
  }

  struct cb {
    ECBackend *ec;
    hobject_t hoid;
    list<pair<ECCommon::ec_align_t,
	      pair<bufferlist*, Context*> > > to_read;
    unique_ptr<Context> on_complete;
    cb(const cb&) = delete;
    cb(cb &&) = default;
    cb(ECBackend *ec,
       const hobject_t &hoid,
       const list<pair<ECCommon::ec_align_t,
                  pair<bufferlist*, Context*> > > &to_read,
       Context *on_complete)
      : ec(ec),
	hoid(hoid),
	to_read(to_read),
	on_complete(on_complete) {}
    void operator()(ECCommon::ec_extents_t &&results) {
      auto dpp = ec->get_parent()->get_dpp();
      ldpp_dout(dpp, 20) << "objects_read_async_cb: got: " << results
			 << dendl;
      ldpp_dout(dpp, 20) << "objects_read_async_cb: cache: " << ec->rmw_pipeline.cache
			 << dendl;

      auto &got = results[hoid];

      int r = 0;
      for (auto &&read: to_read) {
	if (got.err < 0) {
	  // error handling
	  if (read.second.second) {
	    read.second.second->complete(got.err);
	  }
	  if (r == 0)
	    r = got.err;
	} else {
	  ceph_assert(read.second.first);
	  uint64_t offset = read.first.offset;
	  uint64_t length = read.first.size;
	  auto range = got.emap.get_containing_range(offset, length);
	  ceph_assert(range.first != range.second);
	  ceph_assert(range.first.get_off() <= offset);
          ldpp_dout(dpp, 20) << "offset: " << offset << dendl;
          ldpp_dout(dpp, 20) << "range offset: " << range.first.get_off() << dendl;
          ldpp_dout(dpp, 20) << "length: " << length << dendl;
          ldpp_dout(dpp, 20) << "range length: " << range.first.get_len()  << dendl;
	  ceph_assert(
	    (offset + length) <=
	    (range.first.get_off() + range.first.get_len()));
	  read.second.first->substr_of(
	    range.first.get_val(),
	    offset - range.first.get_off(),
	    length);
	  if (read.second.second) {
	    read.second.second->complete(length);
	    read.second.second = nullptr;
	  }
	}
      }
      to_read.clear();
      if (on_complete) {
	on_complete.release()->complete(r);
      }
    }
    ~cb() {
      for (auto &&i: to_read) {
	delete i.second.second;
      }
      to_read.clear();
    }
  };
  objects_read_and_reconstruct(
    reads,
    fast_read,
    make_gen_lambda_context<
      ECCommon::ec_extents_t &&, cb>(
	cb(this,
	   hoid,
	   to_read,
	   on_complete)));
}

void ECBackend::objects_read_and_reconstruct(
  const map<hobject_t,
    std::list<ECBackend::ec_align_t>
  > &reads,
  bool fast_read,
  GenContextURef<ECCommon::ec_extents_t &&> &&func)
{
  return read_pipeline.objects_read_and_reconstruct(
    reads, fast_read, std::move(func));
}

void ECBackend::kick_reads() {
  read_pipeline.kick_reads();
}

int ECBackend::object_stat(
  const hobject_t &hoid,
  struct stat* st)
{
  int r = store->stat(
    ch,
    ghobject_t{hoid, ghobject_t::NO_GEN, get_parent()->whoami_shard().shard},
    st);
  return r;
}

int ECBackend::objects_get_attrs(
  const hobject_t &hoid,
  map<string, bufferlist, less<>> *out)
{
  // call from parents -- get raw attrs, without any filtering for hinfo
  int r = PGBackend::objects_get_attrs(hoid, out);
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
  ceph_assert(old_size % sinfo.get_stripe_width() == 0);
  t->truncate(
    coll,
    ghobject_t(hoid, ghobject_t::NO_GEN, get_parent()->whoami_shard().shard),
    sinfo.aligned_logical_offset_to_chunk_offset(
      old_size));
}

int ECBackend::be_deep_scrub(
  const hobject_t &poid,
  ScrubMap &map,
  ScrubMapBuilder &pos,
  ScrubMap::object &o)
{
  dout(10) << __func__ << " " << poid << " pos " << pos << dendl;
  int r;

  uint32_t fadvise_flags = CEPH_OSD_OP_FLAG_FADVISE_SEQUENTIAL |
                           CEPH_OSD_OP_FLAG_FADVISE_DONTNEED | 
                           CEPH_OSD_OP_FLAG_BYPASS_CLEAN_CACHE;

  utime_t sleeptime;
  sleeptime.set_from_double(cct->_conf->osd_debug_deep_scrub_sleep);
  if (sleeptime != utime_t()) {
    lgeneric_derr(cct) << __func__ << " sleeping for " << sleeptime << dendl;
    sleeptime.sleep();
  }

  if (pos.data_pos == 0) {
    pos.data_hash = bufferhash(-1);
  }

  uint64_t stride = cct->_conf->osd_deep_scrub_stride;
  if (stride % sinfo.get_chunk_size())
    stride += sinfo.get_chunk_size() - (stride % sinfo.get_chunk_size());

  bufferlist bl;
  r = store->read(
    ch,
    ghobject_t(
      poid, ghobject_t::NO_GEN, get_parent()->whoami_shard().shard),
    pos.data_pos,
    stride, bl,
    fadvise_flags);
  if (r < 0) {
    dout(20) << __func__ << "  " << poid << " got "
	     << r << " on read, read_error" << dendl;
    o.read_error = true;
    return 0;
  }
  if (bl.length() % sinfo.get_chunk_size()) {
    dout(20) << __func__ << "  " << poid << " got "
	     << r << " on read, not chunk size " << sinfo.get_chunk_size() << " aligned"
	     << dendl;
    o.read_error = true;
    return 0;
  }
  if (r > 0) {
    pos.data_hash << bl;
  }
  pos.data_pos += r;
  if (r == (int)stride) {
    return -EINPROGRESS;
  }

  ECUtil::HashInfoRef hinfo = unstable_hashinfo_registry.get_hash_info(poid, false, o.attrs, o.size);
  if (!hinfo) {
    dout(0) << "_scan_list  " << poid << " could not retrieve hash info" << dendl;
    o.read_error = true;
    o.digest_present = false;
    return 0;
  } else {
    if (!get_parent()->get_pool().allows_ecoverwrites()) {
      if (!hinfo->has_chunk_hash()) {
        dout(0) << "_scan_list  " << poid << " got invalid hash info" << dendl;
        o.ec_size_mismatch = true;
        return 0;
      }
      if (hinfo->get_total_chunk_size() != (unsigned)pos.data_pos) {
	dout(0) << "_scan_list  " << poid << " got incorrect size on read 0x"
		<< std::hex << pos
		<< " expected 0x" << hinfo->get_total_chunk_size() << std::dec
		<< dendl;
	o.ec_size_mismatch = true;
	return 0;
      }

      if (hinfo->get_chunk_hash(get_parent()->whoami_shard().shard) !=
	  pos.data_hash.digest()) {
	dout(0) << "_scan_list  " << poid << " got incorrect hash on read 0x"
		<< std::hex << pos.data_hash.digest() << " !=  expected 0x"
		<< hinfo->get_chunk_hash(get_parent()->whoami_shard().shard)
		<< std::dec << dendl;
	o.ec_hash_mismatch = true;
	return 0;
      }

      /* We checked above that we match our own stored hash.  We cannot
       * send a hash of the actual object, so instead we simply send
       * our locally stored hash of shard 0 on the assumption that if
       * we match our chunk hash and our recollection of the hash for
       * chunk 0 matches that of our peers, there is likely no corruption.
       */
      o.digest = hinfo->get_chunk_hash(0);
      o.digest_present = true;
    } else {
      /* Hack! We must be using partial overwrites, and partial overwrites
       * don't support deep-scrub yet
       */
      o.digest = 0;
      o.digest_present = true;
    }
  }

  o.omap_digest = -1;
  o.omap_digest_present = true;
  return 0;
}
