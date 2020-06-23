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
#include "messages/MOSDRepOp.h"
#include "messages/MOSDRepOpReply.h"
#include "messages/MOSDPGPush.h"
#include "messages/MOSDPGPull.h"
#include "messages/MOSDPGPushReply.h"
#include "common/EventTrace.h"
#include "include/random.h"
#include "include/util.h"
#include "OSD.h"

#define dout_context cct
#define dout_subsys ceph_subsys_osd
#define DOUT_PREFIX_ARGS this
#undef dout_prefix
#define dout_prefix _prefix(_dout, this)
static ostream& _prefix(std::ostream *_dout, ReplicatedBackend *pgb) {
  return pgb->get_parent()->gen_dbg_prefix(*_dout);
}

using std::list;
using std::make_pair;
using std::map;
using std::ostringstream;
using std::set;
using std::pair;
using std::string;
using std::unique_ptr;
using std::vector;

using ceph::bufferhash;
using ceph::bufferlist;
using ceph::decode;
using ceph::encode;

namespace {
class PG_SendMessageOnConn: public Context {
  PGBackend::Listener *pg;
  Message *reply;
  ConnectionRef conn;
  public:
  PG_SendMessageOnConn(
    PGBackend::Listener *pg,
    Message *reply,
    ConnectionRef conn) : pg(pg), reply(reply), conn(conn) {}
  void finish(int) override {
    pg->send_message_osd_cluster(reply, conn.get());
  }
};

class PG_RecoveryQueueAsync : public Context {
  PGBackend::Listener *pg;
  unique_ptr<GenContext<ThreadPool::TPHandle&>> c;
  public:
  PG_RecoveryQueueAsync(
    PGBackend::Listener *pg,
    GenContext<ThreadPool::TPHandle&> *c) : pg(pg), c(c) {}
  void finish(int) override {
    pg->schedule_recovery_work(c.release());
  }
};
}

struct ReplicatedBackend::C_OSD_RepModifyCommit : public Context {
  ReplicatedBackend *pg;
  RepModifyRef rm;
  C_OSD_RepModifyCommit(ReplicatedBackend *pg, RepModifyRef r)
    : pg(pg), rm(r) {}
  void finish(int r) override {
    pg->repop_commit(rm);
  }
};

static void log_subop_stats(
  PerfCounters *logger,
  OpRequestRef op, int subop)
{
  utime_t now = ceph_clock_now();
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
      ceph_abort_msg("no support subop");
  } else {
    logger->tinc(l_osd_sop_pull_lat, latency);
  }
}

ReplicatedBackend::ReplicatedBackend(
  PGBackend::Listener *pg,
  const coll_t &coll,
  ObjectStore::CollectionHandle &c,
  ObjectStore *store,
  CephContext *cct) :
  PGBackend(cct, pg, store, coll, c) {}

void ReplicatedBackend::run_recovery_op(
  PGBackend::RecoveryHandle *_h,
  int priority)
{
  RPGHandle *h = static_cast<RPGHandle *>(_h);
  send_pushes(priority, h->pushes);
  send_pulls(priority, h->pulls);
  send_recovery_deletes(priority, h->deletes);
  delete h;
}

int ReplicatedBackend::recover_object(
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
    ceph_assert(!obc);
    // pull
    prepare_pull(
      v,
      hoid,
      head,
      h);
  } else {
    ceph_assert(obc);
    int started = start_pushes(
      hoid,
      obc,
      h);
    if (started < 0) {
      pushing[hoid].clear();
      return started;
    }
  }
  return 0;
}

void ReplicatedBackend::check_recovery_sources(const OSDMapRef& osdmap)
{
  for(map<pg_shard_t, set<hobject_t> >::iterator i = pull_from_peer.begin();
      i != pull_from_peer.end();
      ) {
    if (osdmap->is_down(i->first.osd)) {
      dout(10) << "check_recovery_sources resetting pulls from osd." << i->first
	       << ", osdmap has it marked down" << dendl;
      for (set<hobject_t>::iterator j = i->second.begin();
	   j != i->second.end();
	   ++j) {
	get_parent()->cancel_pull(*j);
	clear_pull(pulling.find(*j), false);
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
  default:
    return false;
  }
}

bool ReplicatedBackend::_handle_message(
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

  case MSG_OSD_REPOP: {
    do_repop(op);
    return true;
  }

  case MSG_OSD_REPOPREPLY: {
    do_repop_reply(op);
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
  for (auto &&i: pushing) {
    for (auto &&j: i.second) {
      get_parent()->release_locks(j.second.lock_manager);
    }
  }
  pushing.clear();

  for (auto &&i: pulling) {
    get_parent()->release_locks(i.second.lock_manager);
  }
  pulling.clear();
  pull_from_peer.clear();
}

void ReplicatedBackend::on_change()
{
  dout(10) << __func__ << dendl;
  for (auto& op : in_progress_ops) {
    delete op.second->on_commit;
    op.second->on_commit = nullptr;
  }
  in_progress_ops.clear();
  clear_recovery_state();
}

int ReplicatedBackend::objects_read_sync(
  const hobject_t &hoid,
  uint64_t off,
  uint64_t len,
  uint32_t op_flags,
  bufferlist *bl)
{
  return store->read(ch, ghobject_t(hoid), off, len, *bl, op_flags);
}

int ReplicatedBackend::objects_readv_sync(
  const hobject_t &hoid,
  map<uint64_t, uint64_t>&& m,
  uint32_t op_flags,
  bufferlist *bl)
{
  interval_set<uint64_t> im(std::move(m));
  auto r = store->readv(ch, ghobject_t(hoid), im, *bl, op_flags);
  if (r >= 0) {
    m = std::move(im).detach();
  }
  return r;
}

void ReplicatedBackend::objects_read_async(
  const hobject_t &hoid,
  const list<pair<boost::tuple<uint64_t, uint64_t, uint32_t>,
		  pair<bufferlist*, Context*> > > &to_read,
  Context *on_complete,
  bool fast_read)
{
  ceph_abort_msg("async read is not used by replica pool");
}

class C_OSD_OnOpCommit : public Context {
  ReplicatedBackend *pg;
  ceph::ref_t<ReplicatedBackend::InProgressOp> op;
public:
  C_OSD_OnOpCommit(ReplicatedBackend *pg, ceph::ref_t<ReplicatedBackend::InProgressOp> op)
    : pg(pg), op(std::move(op)) {}
  void finish(int) override {
    pg->op_commit(op);
  }
};

void generate_transaction(
  PGTransactionUPtr &pgt,
  const coll_t &coll,
  vector<pg_log_entry_t> &log_entries,
  ObjectStore::Transaction *t,
  set<hobject_t> *added,
  set<hobject_t> *removed,
  const ceph_release_t require_osd_release = ceph_release_t::unknown )
{
  ceph_assert(t);
  ceph_assert(added);
  ceph_assert(removed);

  for (auto &&le: log_entries) {
    le.mark_unrollbackable();
    auto oiter = pgt->op_map.find(le.soid);
    if (oiter != pgt->op_map.end() && oiter->second.updated_snaps) {
      bufferlist bl(oiter->second.updated_snaps->second.size() * 8 + 8);
      encode(oiter->second.updated_snaps->second, bl);
      le.snaps.swap(bl);
      le.snaps.reassign_to_mempool(mempool::mempool_osd_pglog);
    }
  }

  pgt->safe_create_traverse(
    [&](pair<const hobject_t, PGTransaction::ObjectOperation> &obj_op) {
      const hobject_t &oid = obj_op.first;
      const ghobject_t goid =
	ghobject_t(oid, ghobject_t::NO_GEN, shard_id_t::NO_SHARD);
      const PGTransaction::ObjectOperation &op = obj_op.second;

      if (oid.is_temp()) {
	if (op.is_fresh_object()) {
	  added->insert(oid);
	} else if (op.is_delete()) {
	  removed->insert(oid);
	}
      }

      if (op.delete_first) {
	t->remove(coll, goid);
      }

      match(
	op.init_type,
	[&](const PGTransaction::ObjectOperation::Init::None &) {
	},
	[&](const PGTransaction::ObjectOperation::Init::Create &op) {
	  if (require_osd_release >= ceph_release_t::octopus) {
	    t->create(coll, goid);
	  } else {
	    t->touch(coll, goid);
	  }
	},
	[&](const PGTransaction::ObjectOperation::Init::Clone &op) {
	  t->clone(
	    coll,
	    ghobject_t(
	      op.source, ghobject_t::NO_GEN, shard_id_t::NO_SHARD),
	    goid);
	},
	[&](const PGTransaction::ObjectOperation::Init::Rename &op) {
	  ceph_assert(op.source.is_temp());
	  t->collection_move_rename(
	    coll,
	    ghobject_t(
	      op.source, ghobject_t::NO_GEN, shard_id_t::NO_SHARD),
	    coll,
	    goid);
	});

      if (op.truncate) {
	t->truncate(coll, goid, op.truncate->first);
	if (op.truncate->first != op.truncate->second)
	  t->truncate(coll, goid, op.truncate->second);
      }

      if (!op.attr_updates.empty()) {
	map<string, bufferlist> attrs;
	for (auto &&p: op.attr_updates) {
	  if (p.second)
	    attrs[p.first] = *(p.second);
	  else
	    t->rmattr(coll, goid, p.first);
	}
	t->setattrs(coll, goid, attrs);
      }

      if (op.clear_omap)
	t->omap_clear(coll, goid);
      if (op.omap_header)
	t->omap_setheader(coll, goid, *(op.omap_header));

      for (auto &&up: op.omap_updates) {
	using UpdateType = PGTransaction::ObjectOperation::OmapUpdateType;
	switch (up.first) {
	case UpdateType::Remove:
	  t->omap_rmkeys(coll, goid, up.second);
	  break;
	case UpdateType::Insert:
	  t->omap_setkeys(coll, goid, up.second);
	  break;
	case UpdateType::RemoveRange:
	  t->omap_rmkeyrange(coll, goid, up.second);
	  break;
	}
      }

      // updated_snaps doesn't matter since we marked unrollbackable

      if (op.alloc_hint) {
	auto &hint = *(op.alloc_hint);
	t->set_alloc_hint(
	  coll,
	  goid,
	  hint.expected_object_size,
	  hint.expected_write_size,
	  hint.flags);
      }

      for (auto &&extent: op.buffer_updates) {
	using BufferUpdate = PGTransaction::ObjectOperation::BufferUpdate;
	match(
	  extent.get_val(),
	  [&](const BufferUpdate::Write &op) {
	    t->write(
	      coll,
	      goid,
	      extent.get_off(),
	      extent.get_len(),
	      op.buffer,
	      op.fadvise_flags);
	  },
	  [&](const BufferUpdate::Zero &op) {
	    t->zero(
	      coll,
	      goid,
	      extent.get_off(),
	      extent.get_len());
	  },
	  [&](const BufferUpdate::CloneRange &op) {
	    ceph_assert(op.len == extent.get_len());
	    t->clone_range(
	      coll,
	      ghobject_t(op.from, ghobject_t::NO_GEN, shard_id_t::NO_SHARD),
	      goid,
	      op.offset,
	      extent.get_len(),
	      extent.get_off());
	  });
      }
    });
}

void ReplicatedBackend::submit_transaction(
  const hobject_t &soid,
  const object_stat_sum_t &delta_stats,
  const eversion_t &at_version,
  PGTransactionUPtr &&_t,
  const eversion_t &trim_to,
  const eversion_t &min_last_complete_ondisk,
  vector<pg_log_entry_t>&& _log_entries,
  std::optional<pg_hit_set_history_t> &hset_history,
  Context *on_all_commit,
  ceph_tid_t tid,
  osd_reqid_t reqid,
  OpRequestRef orig_op)
{
  parent->apply_stats(
    soid,
    delta_stats);

  vector<pg_log_entry_t> log_entries(_log_entries);
  ObjectStore::Transaction op_t;
  PGTransactionUPtr t(std::move(_t));
  set<hobject_t> added, removed;
  generate_transaction(
    t,
    coll,
    log_entries,
    &op_t,
    &added,
    &removed,
    get_osdmap()->require_osd_release);
  ceph_assert(added.size() <= 1);
  ceph_assert(removed.size() <= 1);

  auto insert_res = in_progress_ops.insert(
    make_pair(
      tid,
      ceph::make_ref<InProgressOp>(
	tid, on_all_commit,
	orig_op, at_version)
      )
    );
  ceph_assert(insert_res.second);
  InProgressOp &op = *insert_res.first->second;

  op.waiting_for_commit.insert(
    parent->get_acting_recovery_backfill_shards().begin(),
    parent->get_acting_recovery_backfill_shards().end());

  issue_op(
    soid,
    at_version,
    tid,
    reqid,
    trim_to,
    min_last_complete_ondisk,
    added.size() ? *(added.begin()) : hobject_t(),
    removed.size() ? *(removed.begin()) : hobject_t(),
    log_entries,
    hset_history,
    &op,
    op_t);

  add_temp_objs(added);
  clear_temp_objs(removed);

  parent->log_operation(
    std::move(log_entries),
    hset_history,
    trim_to,
    at_version,
    min_last_complete_ondisk,
    true,
    op_t);
  
  op_t.register_on_commit(
    parent->bless_context(
      new C_OSD_OnOpCommit(this, &op)));

  vector<ObjectStore::Transaction> tls;
  tls.push_back(std::move(op_t));

  parent->queue_transactions(tls, op.op);
  if (at_version != eversion_t()) {
    parent->op_applied(at_version);
  }
}

void ReplicatedBackend::op_commit(const ceph::ref_t<InProgressOp>& op)
{
  if (op->on_commit == nullptr) {
    // aborted
    return;
  }

  FUNCTRACE(cct);
  OID_EVENT_TRACE_WITH_MSG((op && op->op) ? op->op->get_req() : NULL, "OP_COMMIT_BEGIN", true);
  dout(10) << __func__ << ": " << op->tid << dendl;
  if (op->op) {
    op->op->mark_event("op_commit");
    op->op->pg_trace.event("op commit");
  }

  op->waiting_for_commit.erase(get_parent()->whoami_shard());

  if (op->waiting_for_commit.empty()) {
    op->on_commit->complete(0);
    op->on_commit = 0;
    in_progress_ops.erase(op->tid);
  }
}

void ReplicatedBackend::do_repop_reply(OpRequestRef op)
{
  static_cast<MOSDRepOpReply*>(op->get_nonconst_req())->finish_decode();
  auto r = op->get_req<MOSDRepOpReply>();
  ceph_assert(r->get_header().type == MSG_OSD_REPOPREPLY);

  op->mark_started();

  // must be replication.
  ceph_tid_t rep_tid = r->get_tid();
  pg_shard_t from = r->from;

  auto iter = in_progress_ops.find(rep_tid);
  if (iter != in_progress_ops.end()) {
    InProgressOp &ip_op = *iter->second;
    const MOSDOp *m = nullptr;
    if (ip_op.op)
      m = ip_op.op->get_req<MOSDOp>();

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
      ceph_assert(ip_op.waiting_for_commit.count(from));
      ip_op.waiting_for_commit.erase(from);
      if (ip_op.op) {
	ip_op.op->mark_event("sub_op_commit_rec");
	ip_op.op->pg_trace.event("sub_op_commit_rec");
      }
    } else {
      // legacy peer; ignore
    }

    parent->update_peer_last_complete_ondisk(
      from,
      r->get_last_complete_ondisk());

    if (ip_op.waiting_for_commit.empty() &&
        ip_op.on_commit) {
      ip_op.on_commit->complete(0);
      ip_op.on_commit = 0;
      in_progress_ops.erase(iter);
    }
  }
}

int ReplicatedBackend::be_deep_scrub(
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

  ceph_assert(poid == pos.ls[pos.pos]);
  if (!pos.data_done()) {
    if (pos.data_pos == 0) {
      pos.data_hash = bufferhash(-1);
    }

    bufferlist bl;
    r = store->read(
      ch,
      ghobject_t(
	poid, ghobject_t::NO_GEN, get_parent()->whoami_shard().shard),
      pos.data_pos,
      cct->_conf->osd_deep_scrub_stride, bl,
      fadvise_flags);
    if (r < 0) {
      dout(20) << __func__ << "  " << poid << " got "
	       << r << " on read, read_error" << dendl;
      o.read_error = true;
      return 0;
    }
    if (r > 0) {
      pos.data_hash << bl;
    }
    pos.data_pos += r;
    if (r == cct->_conf->osd_deep_scrub_stride) {
      dout(20) << __func__ << "  " << poid << " more data, digest so far 0x"
	       << std::hex << pos.data_hash.digest() << std::dec << dendl;
      return -EINPROGRESS;
    }
    // done with bytes
    pos.data_pos = -1;
    o.digest = pos.data_hash.digest();
    o.digest_present = true;
    dout(20) << __func__ << "  " << poid << " done with data, digest 0x"
	     << std::hex << o.digest << std::dec << dendl;
  }

  // omap header
  if (pos.omap_pos.empty()) {
    pos.omap_hash = bufferhash(-1);

    bufferlist hdrbl;
    r = store->omap_get_header(
      ch,
      ghobject_t(
	poid, ghobject_t::NO_GEN, get_parent()->whoami_shard().shard),
      &hdrbl, true);
    if (r == -EIO) {
      dout(20) << __func__ << "  " << poid << " got "
	       << r << " on omap header read, read_error" << dendl;
      o.read_error = true;
      return 0;
    }
    if (r == 0 && hdrbl.length()) {
      bool encoded = false;
      dout(25) << "CRC header " << cleanbin(hdrbl, encoded, true) << dendl;
      pos.omap_hash << hdrbl;
    }
  }

  // omap
  ObjectMap::ObjectMapIterator iter = store->get_omap_iterator(
    ch,
    ghobject_t(
      poid, ghobject_t::NO_GEN, get_parent()->whoami_shard().shard));
  ceph_assert(iter);
  if (pos.omap_pos.length()) {
    iter->lower_bound(pos.omap_pos);
  } else {
    iter->seek_to_first();
  }
  int max = g_conf()->osd_deep_scrub_keys;
  while (iter->status() == 0 && iter->valid()) {
    pos.omap_bytes += iter->value().length();
    ++pos.omap_keys;
    --max;
    // fixme: we can do this more efficiently.
    bufferlist bl;
    encode(iter->key(), bl);
    encode(iter->value(), bl);
    pos.omap_hash << bl;

    iter->next();

    if (iter->valid() && max == 0) {
      pos.omap_pos = iter->key();
      return -EINPROGRESS;
    }
    if (iter->status() < 0) {
      dout(25) << __func__ << "  " << poid
	       << " on omap scan, db status error" << dendl;
      o.read_error = true;
      return 0;
    }
  }

  if (pos.omap_keys > cct->_conf->
	osd_deep_scrub_large_omap_object_key_threshold ||
      pos.omap_bytes > cct->_conf->
	osd_deep_scrub_large_omap_object_value_sum_threshold) {
    dout(25) << __func__ << " " << poid
	     << " large omap object detected. Object has " << pos.omap_keys
	     << " keys and size " << pos.omap_bytes << " bytes" << dendl;
    o.large_omap_object_found = true;
    o.large_omap_object_key_count = pos.omap_keys;
    o.large_omap_object_value_size = pos.omap_bytes;
    map.has_large_omap_object_errors = true;
  }

  o.omap_digest = pos.omap_hash.digest();
  o.omap_digest_present = true;
  dout(20) << __func__ << " done with " << poid << " omap_digest "
	   << std::hex << o.omap_digest << std::dec << dendl;

  // Sum up omap usage
  if (pos.omap_keys > 0 || pos.omap_bytes > 0) {
    dout(25) << __func__ << " adding " << pos.omap_keys << " keys and "
             << pos.omap_bytes << " bytes to pg_stats sums" << dendl;
    map.has_omap_keys = true;
    o.object_omap_bytes = pos.omap_bytes;
    o.object_omap_keys = pos.omap_keys;
  }

  // done!
  return 0;
}

void ReplicatedBackend::_do_push(OpRequestRef op)
{
  auto m = op->get_req<MOSDPGPush>();
  ceph_assert(m->get_type() == MSG_OSD_PG_PUSH);
  pg_shard_t from = m->from;

  op->mark_started();

  vector<PushReplyOp> replies;
  ObjectStore::Transaction t;
  if (get_parent()->check_failsafe_full()) {
    dout(10) << __func__ << " Out of space (failsafe) processing push request." << dendl;
    ceph_abort();
  }
  for (vector<PushOp>::const_iterator i = m->pushes.begin();
       i != m->pushes.end();
       ++i) {
    replies.push_back(PushReplyOp());
    handle_push(from, *i, &(replies.back()), &t, m->is_repair);
  }

  MOSDPGPushReply *reply = new MOSDPGPushReply;
  reply->from = get_parent()->whoami_shard();
  reply->set_priority(m->get_priority());
  reply->pgid = get_info().pgid;
  reply->map_epoch = m->map_epoch;
  reply->min_epoch = m->min_epoch;
  reply->replies.swap(replies);
  reply->compute_cost(cct);

  t.register_on_complete(
    new PG_SendMessageOnConn(
      get_parent(), reply, m->get_connection()));

  get_parent()->queue_transaction(std::move(t));
}

struct C_ReplicatedBackend_OnPullComplete : GenContext<ThreadPool::TPHandle&> {
  ReplicatedBackend *bc;
  list<ReplicatedBackend::pull_complete_info> to_continue;
  int priority;
  C_ReplicatedBackend_OnPullComplete(ReplicatedBackend *bc, int priority)
    : bc(bc), priority(priority) {}

  void finish(ThreadPool::TPHandle &handle) override {
    ReplicatedBackend::RPGHandle *h = bc->_open_recovery_op();
    for (auto &&i: to_continue) {
      auto j = bc->pulling.find(i.hoid);
      ceph_assert(j != bc->pulling.end());
      ObjectContextRef obc = j->second.obc;
      bc->clear_pull(j, false /* already did it */);
      int started = bc->start_pushes(i.hoid, obc, h);
      if (started < 0) {
	bc->pushing[i.hoid].clear();
	bc->get_parent()->on_failed_pull(
	  { bc->get_parent()->whoami_shard() },
	  i.hoid, obc->obs.oi.version);
      } else if (!started) {
	bc->get_parent()->on_global_recover(
	  i.hoid, i.stat, false);
      }
      handle.reset_tp_timeout();
    }
    bc->run_recovery_op(h, priority);
  }
};

void ReplicatedBackend::_do_pull_response(OpRequestRef op)
{
  auto m = op->get_req<MOSDPGPush>();
  ceph_assert(m->get_type() == MSG_OSD_PG_PUSH);
  pg_shard_t from = m->from;

  op->mark_started();

  vector<PullOp> replies(1);
  if (get_parent()->check_failsafe_full()) {
    dout(10) << __func__ << " Out of space (failsafe) processing pull response (push)." << dendl;
    ceph_abort();
  }

  ObjectStore::Transaction t;
  list<pull_complete_info> to_continue;
  for (vector<PushOp>::const_iterator i = m->pushes.begin();
       i != m->pushes.end();
       ++i) {
    bool more = handle_pull_response(from, *i, &(replies.back()), &to_continue, &t);
    if (more)
      replies.push_back(PullOp());
  }
  if (!to_continue.empty()) {
    C_ReplicatedBackend_OnPullComplete *c =
      new C_ReplicatedBackend_OnPullComplete(
	this,
	m->get_priority());
    c->to_continue.swap(to_continue);
    t.register_on_complete(
      new PG_RecoveryQueueAsync(
	get_parent(),
	get_parent()->bless_unlocked_gencontext(c)));
  }
  replies.erase(replies.end() - 1);

  if (replies.size()) {
    MOSDPGPull *reply = new MOSDPGPull;
    reply->from = parent->whoami_shard();
    reply->set_priority(m->get_priority());
    reply->pgid = get_info().pgid;
    reply->map_epoch = m->map_epoch;
    reply->min_epoch = m->min_epoch;
    reply->set_pulls(&replies);
    reply->compute_cost(cct);

    t.register_on_complete(
      new PG_SendMessageOnConn(
	get_parent(), reply, m->get_connection()));
  }

  get_parent()->queue_transaction(std::move(t));
}

void ReplicatedBackend::do_pull(OpRequestRef op)
{
  MOSDPGPull *m = static_cast<MOSDPGPull *>(op->get_nonconst_req());
  ceph_assert(m->get_type() == MSG_OSD_PG_PULL);
  pg_shard_t from = m->from;

  map<pg_shard_t, vector<PushOp> > replies;
  vector<PullOp> pulls;
  m->take_pulls(&pulls);
  for (auto& i : pulls) {
    replies[from].push_back(PushOp());
    handle_pull(from, i, &(replies[from].back()));
  }
  send_pushes(m->get_priority(), replies);
}

void ReplicatedBackend::do_push_reply(OpRequestRef op)
{
  auto m = op->get_req<MOSDPGPushReply>();
  ceph_assert(m->get_type() == MSG_OSD_PG_PUSH_REPLY);
  pg_shard_t from = m->from;

  vector<PushOp> replies(1);
  for (vector<PushReplyOp>::const_iterator i = m->replies.begin();
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

Message * ReplicatedBackend::generate_subop(
  const hobject_t &soid,
  const eversion_t &at_version,
  ceph_tid_t tid,
  osd_reqid_t reqid,
  eversion_t pg_trim_to,
  eversion_t min_last_complete_ondisk,
  hobject_t new_temp_oid,
  hobject_t discard_temp_oid,
  const bufferlist &log_entries,
  std::optional<pg_hit_set_history_t> &hset_hist,
  ObjectStore::Transaction &op_t,
  pg_shard_t peer,
  const pg_info_t &pinfo)
{
  int acks_wanted = CEPH_OSD_FLAG_ACK | CEPH_OSD_FLAG_ONDISK;
  // forward the write/update/whatever
  MOSDRepOp *wr = new MOSDRepOp(
    reqid, parent->whoami_shard(),
    spg_t(get_info().pgid.pgid, peer.shard),
    soid, acks_wanted,
    get_osdmap_epoch(),
    parent->get_last_peering_reset_epoch(),
    tid, at_version);

  // ship resulting transaction, log entries, and pg_stats
  if (!parent->should_send_op(peer, soid)) {
    ObjectStore::Transaction t;
    encode(t, wr->get_data());
  } else {
    encode(op_t, wr->get_data());
    wr->get_header().data_off = op_t.get_data_alignment();
  }

  wr->logbl = log_entries;

  if (pinfo.is_incomplete())
    wr->pg_stats = pinfo.stats;  // reflects backfill progress
  else
    wr->pg_stats = get_info().stats;

  wr->pg_trim_to = pg_trim_to;

  if (HAVE_FEATURE(parent->min_peer_features(), OSD_REPOP_MLCOD)) {
    wr->min_last_complete_ondisk = min_last_complete_ondisk;
  } else {
    /* Some replicas need this field to be at_version.  New replicas
     * will ignore it */
    wr->set_rollback_to(at_version);
  }

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
  eversion_t min_last_complete_ondisk,
  hobject_t new_temp_oid,
  hobject_t discard_temp_oid,
  const vector<pg_log_entry_t> &log_entries,
  std::optional<pg_hit_set_history_t> &hset_hist,
  InProgressOp *op,
  ObjectStore::Transaction &op_t)
{
  if (parent->get_acting_recovery_backfill_shards().size() > 1) {
    if (op->op) {
      op->op->pg_trace.event("issue replication ops");
      ostringstream ss;
      set<pg_shard_t> replicas = parent->get_acting_recovery_backfill_shards();
      replicas.erase(parent->whoami_shard());
      ss << "waiting for subops from " << replicas;
      op->op->mark_sub_op_sent(ss.str());
    }

    // avoid doing the same work in generate_subop
    bufferlist logs;
    encode(log_entries, logs);

    for (const auto& shard : get_parent()->get_acting_recovery_backfill_shards()) {
      if (shard == parent->whoami_shard()) continue;
      const pg_info_t &pinfo = parent->get_shard_info().find(shard)->second;

      Message *wr;
      wr = generate_subop(
	  soid,
	  at_version,
	  tid,
	  reqid,
	  pg_trim_to,
	  min_last_complete_ondisk,
	  new_temp_oid,
	  discard_temp_oid,
	  logs,
	  hset_hist,
	  op_t,
	  shard,
	  pinfo);
      if (op->op && op->op->pg_trace)
	wr->trace.init("replicated op", nullptr, &op->op->pg_trace);
      get_parent()->send_message_osd_cluster(
	  shard.osd, wr, get_osdmap_epoch());
    }
  }
}

// sub op modify
void ReplicatedBackend::do_repop(OpRequestRef op)
{
  static_cast<MOSDRepOp*>(op->get_nonconst_req())->finish_decode();
  auto m = op->get_req<MOSDRepOp>();
  int msg_type = m->get_type();
  ceph_assert(MSG_OSD_REPOP == msg_type);

  const hobject_t& soid = m->poid;

  dout(10) << __func__ << " " << soid
           << " v " << m->version
	   << (m->logbl.length() ? " (transaction)" : " (parallel exec")
	   << " " << m->logbl.length()
	   << dendl;

  // sanity checks
  ceph_assert(m->map_epoch >= get_info().history.same_interval_since);

  dout(30) << __func__ << " missing before " << get_parent()->get_log().get_missing().get_items() << dendl;
  parent->maybe_preempt_replica_scrub(soid);

  int ackerosd = m->get_source().num();

  op->mark_started();

  RepModifyRef rm(std::make_shared<RepModify>());
  rm->op = op;
  rm->ackerosd = ackerosd;
  rm->last_complete = get_info().last_complete;
  rm->epoch_started = get_osdmap_epoch();

  ceph_assert(m->logbl.length());
  // shipped transaction and log entries
  vector<pg_log_entry_t> log;

  auto p = const_cast<bufferlist&>(m->get_data()).cbegin();
  decode(rm->opt, p);

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

  p = const_cast<bufferlist&>(m->logbl).begin();
  decode(log, p);
  rm->opt.set_fadvise_flag(CEPH_OSD_OP_FLAG_FADVISE_DONTNEED);

  bool update_snaps = false;
  if (!rm->opt.empty()) {
    // If the opt is non-empty, we infer we are before
    // last_backfill (according to the primary, not our
    // not-quite-accurate value), and should update the
    // collections now.  Otherwise, we do it later on push.
    update_snaps = true;
  }

  // flag set to true during async recovery
  bool async = false;
  pg_missing_tracker_t pmissing = get_parent()->get_local_missing();
  if (pmissing.is_missing(soid)) {
    async = true;
    dout(30) << __func__ << " is_missing " << pmissing.is_missing(soid) << dendl;
    for (auto &&e: log) {
      dout(30) << " add_next_event entry " << e << dendl;
      get_parent()->add_local_next_event(e);
      dout(30) << " entry is_delete " << e.is_delete() << dendl;
    }
  }

  parent->update_stats(m->pg_stats);
  parent->log_operation(
    std::move(log),
    m->updated_hit_set_history,
    m->pg_trim_to,
    m->version, /* Replicated PGs don't have rollback info */
    m->min_last_complete_ondisk,
    update_snaps,
    rm->localt,
    async);

  rm->opt.register_on_commit(
    parent->bless_context(
      new C_OSD_RepModifyCommit(this, rm)));
  vector<ObjectStore::Transaction> tls;
  tls.reserve(2);
  tls.push_back(std::move(rm->localt));
  tls.push_back(std::move(rm->opt));
  parent->queue_transactions(tls, op);
  // op is cleaned up by oncommit/onapply when both are executed
  dout(30) << __func__ << " missing after" << get_parent()->get_log().get_missing().get_items() << dendl;
}

void ReplicatedBackend::repop_commit(RepModifyRef rm)
{
  rm->op->mark_commit_sent();
  rm->op->pg_trace.event("sup_op_commit");
  rm->committed = true;

  // send commit.
  auto m = rm->op->get_req<MOSDRepOp>();
  ceph_assert(m->get_type() == MSG_OSD_REPOP);
  dout(10) << __func__ << " on op " << *m
	   << ", sending commit to osd." << rm->ackerosd
	   << dendl;
  ceph_assert(get_osdmap()->is_up(rm->ackerosd));

  get_parent()->update_last_complete_ondisk(rm->last_complete);

  MOSDRepOpReply *reply = new MOSDRepOpReply(
    m,
    get_parent()->whoami_shard(),
    0, get_osdmap_epoch(), m->get_min_epoch(), CEPH_OSD_FLAG_ONDISK);
  reply->set_last_complete_ondisk(rm->last_complete);
  reply->set_priority(CEPH_MSG_PRIO_HIGH); // this better match ack priority!
  reply->trace = rm->op->pg_trace;
  get_parent()->send_message_osd_cluster(
    rm->ackerosd, reply, get_osdmap_epoch());

  log_subop_stats(get_parent()->get_logger(), rm->op, l_osd_sop_w);
}


// ===========================================================

void ReplicatedBackend::calc_head_subsets(
  ObjectContextRef obc, SnapSet& snapset, const hobject_t& head,
  const pg_missing_t& missing,
  const hobject_t &last_backfill,
  interval_set<uint64_t>& data_subset,
  map<hobject_t, interval_set<uint64_t>>& clone_subsets,
  ObcLockManager &manager)
{
  dout(10) << "calc_head_subsets " << head
	   << " clone_overlap " << snapset.clone_overlap << dendl;

  uint64_t size = obc->obs.oi.size;
  if (size)
    data_subset.insert(0, size);

  if (HAVE_FEATURE(parent->min_peer_features(), SERVER_OCTOPUS)) {
    const auto it = missing.get_items().find(head);
    assert(it != missing.get_items().end());
    data_subset.intersection_of(it->second.clean_regions.get_dirty_regions());
    dout(10) << "calc_head_subsets " << head
             << " data_subset " << data_subset << dendl;
  }

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
  hobject_t c = head;
  if (size)
    prev.insert(0, size);

  for (int j=snapset.clones.size()-1; j>=0; j--) {
    c.snap = snapset.clones[j];
    prev.intersection_of(snapset.clone_overlap[snapset.clones[j]]);
    if (!missing.is_missing(c) &&
	c < last_backfill &&
	get_parent()->try_lock_for_read(c, manager)) {
      dout(10) << "calc_head_subsets " << head << " has prev " << c
	       << " overlap " << prev << dendl;
      cloning = prev;
      break;
    }
    dout(10) << "calc_head_subsets " << head << " does not have prev " << c
	     << " overlap " << prev << dendl;
  }

  cloning.intersection_of(data_subset);
  if (cloning.empty()) {
    dout(10) << "skipping clone, nothing needs to clone" << dendl;
    return;
  }

  if (cloning.num_intervals() > g_conf().get_val<uint64_t>("osd_recover_clone_overlap_limit")) {
    dout(10) << "skipping clone, too many holes" << dendl;
    get_parent()->release_locks(manager);
    clone_subsets.clear();
    cloning.clear();
    return;
  }

  // what's left for us to push?
  clone_subsets[c] = cloning;
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
  map<hobject_t, interval_set<uint64_t>>& clone_subsets,
  ObcLockManager &manager)
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
	c < last_backfill &&
	get_parent()->try_lock_for_read(c, manager)) {
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
	c < last_backfill &&
	get_parent()->try_lock_for_read(c, manager)) {
      dout(10) << "calc_clone_subsets " << soid << " has next " << c
	       << " overlap " << next << dendl;
      clone_subsets[c] = next;
      cloning.union_of(next);
      break;
    }
    dout(10) << "calc_clone_subsets " << soid << " does not have next " << c
	     << " overlap " << next << dendl;
  }

  if (cloning.num_intervals() > g_conf().get_val<uint64_t>("osd_recover_clone_overlap_limit")) {
    dout(10) << "skipping clone, too many holes" << dendl;
    get_parent()->release_locks(manager);
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
  const auto missing_iter = get_parent()->get_local_missing().get_items().find(soid);
  ceph_assert(missing_iter != get_parent()->get_local_missing().get_items().end());
  eversion_t _v = missing_iter->second.need;
  ceph_assert(_v == v);
  const map<hobject_t, set<pg_shard_t>> &missing_loc(
    get_parent()->get_missing_loc_shards());
  const map<pg_shard_t, pg_missing_t > &peer_missing(
    get_parent()->get_shard_missing());
  map<hobject_t, set<pg_shard_t>>::const_iterator q = missing_loc.find(soid);
  ceph_assert(q != missing_loc.end());
  ceph_assert(!q->second.empty());

  // pick a pullee
  auto p = q->second.end();
  if (cct->_conf->osd_debug_feed_pullee >= 0) {
    for (auto it = q->second.begin(); it != q->second.end(); it++) {
      if (it->osd == cct->_conf->osd_debug_feed_pullee) {
        p = it;
        break;
      }
    }
  }
  if (p == q->second.end()) {
    // probably because user feed a wrong pullee
    p = q->second.begin();
    std::advance(p,
                 ceph::util::generate_random_number<int>(0,
							 q->second.size() - 1));
  }
  ceph_assert(get_osdmap()->is_up(p->osd));
  pg_shard_t fromshard = *p;

  dout(7) << "pull " << soid
	  << " v " << v
	  << " on osds " << q->second
	  << " from osd." << fromshard
	  << dendl;

  ceph_assert(peer_missing.count(fromshard));
  const pg_missing_t &pmissing = peer_missing.find(fromshard)->second;
  if (pmissing.is_missing(soid, v)) {
    ceph_assert(pmissing.get_items().find(soid)->second.have != v);
    dout(10) << "pulling soid " << soid << " from osd " << fromshard
	     << " at version " << pmissing.get_items().find(soid)->second.have
	     << " rather than at version " << v << dendl;
    v = pmissing.get_items().find(soid)->second.have;
    ceph_assert(get_parent()->get_log().get_log().objects.count(soid) &&
	   (get_parent()->get_log().get_log().objects.find(soid)->second->op ==
	    pg_log_entry_t::LOST_REVERT) &&
	   (get_parent()->get_log().get_log().objects.find(
	     soid)->second->reverting_to ==
	    v));
  }

  ObjectRecoveryInfo recovery_info;
  ObcLockManager lock_manager;

  if (soid.is_snap()) {
    ceph_assert(!get_parent()->get_local_missing().is_missing(soid.get_head()));
    ceph_assert(headctx);
    // check snapset
    SnapSetContext *ssc = headctx->ssc;
    ceph_assert(ssc);
    dout(10) << " snapset " << ssc->snapset << dendl;
    recovery_info.ss = ssc->snapset;
    calc_clone_subsets(
      ssc->snapset, soid, get_parent()->get_local_missing(),
      get_info().last_backfill,
      recovery_info.copy_subset,
      recovery_info.clone_subset,
      lock_manager);
    // FIXME: this may overestimate if we are pulling multiple clones in parallel...
    dout(10) << " pulling " << recovery_info << dendl;

    ceph_assert(ssc->snapset.clone_size.count(soid.snap));
    recovery_info.size = ssc->snapset.clone_size[soid.snap];
    recovery_info.object_exist = missing_iter->second.clean_regions.object_is_exist();
  } else {
    // pulling head or unversioned object.
    // always pull the whole thing.
    recovery_info.copy_subset.insert(0, (uint64_t)-1);
    if (HAVE_FEATURE(parent->min_peer_features(), SERVER_OCTOPUS))
      recovery_info.copy_subset.intersection_of(missing_iter->second.clean_regions.get_dirty_regions());
    recovery_info.size = ((uint64_t)-1);
    recovery_info.object_exist = missing_iter->second.clean_regions.object_is_exist();
  }

  h->pulls[fromshard].push_back(PullOp());
  PullOp &op = h->pulls[fromshard].back();
  op.soid = soid;

  op.recovery_info = recovery_info;
  op.recovery_info.soid = soid;
  op.recovery_info.version = v;
  op.recovery_progress.data_complete = false;
  op.recovery_progress.omap_complete = !missing_iter->second.clean_regions.omap_is_dirty() 
                                && HAVE_FEATURE(parent->min_peer_features(), SERVER_OCTOPUS);
  op.recovery_progress.data_recovered_to = 0;
  op.recovery_progress.first = true;

  ceph_assert(!pulling.count(soid));
  pull_from_peer[fromshard].insert(soid);
  PullInfo &pi = pulling[soid];
  pi.from = fromshard;
  pi.soid = soid;
  pi.head_ctx = headctx;
  pi.recovery_info = op.recovery_info;
  pi.recovery_progress = op.recovery_progress;
  pi.cache_dont_need = h->cache_dont_need;
  pi.lock_manager = std::move(lock_manager);
}

/*
 * intelligently push an object to a replica.  make use of existing
 * clones/heads and dup data ranges where possible.
 */
int ReplicatedBackend::prep_push_to_replica(
  ObjectContextRef obc, const hobject_t& soid, pg_shard_t peer,
  PushOp *pop, bool cache_dont_need)
{
  const object_info_t& oi = obc->obs.oi;
  uint64_t size = obc->obs.oi.size;

  dout(10) << __func__ << ": " << soid << " v" << oi.version
	   << " size " << size << " to osd." << peer << dendl;

  map<hobject_t, interval_set<uint64_t>> clone_subsets;
  interval_set<uint64_t> data_subset;

  ObcLockManager lock_manager;
  // are we doing a clone on the replica?
  if (soid.snap && soid.snap < CEPH_NOSNAP) {
    hobject_t head = soid;
    head.snap = CEPH_NOSNAP;

    // try to base push off of clones that succeed/preceed poid
    // we need the head (and current SnapSet) locally to do that.
    if (get_parent()->get_local_missing().is_missing(head)) {
      dout(15) << "push_to_replica missing head " << head << ", pushing raw clone" << dendl;
      return prep_push(obc, soid, peer, pop, cache_dont_need);
    }

    SnapSetContext *ssc = obc->ssc;
    ceph_assert(ssc);
    dout(15) << "push_to_replica snapset is " << ssc->snapset << dendl;
    pop->recovery_info.ss = ssc->snapset;
    map<pg_shard_t, pg_missing_t>::const_iterator pm =
      get_parent()->get_shard_missing().find(peer);
    ceph_assert(pm != get_parent()->get_shard_missing().end());
    map<pg_shard_t, pg_info_t>::const_iterator pi =
      get_parent()->get_shard_info().find(peer);
    ceph_assert(pi != get_parent()->get_shard_info().end());
    calc_clone_subsets(
      ssc->snapset, soid,
      pm->second,
      pi->second.last_backfill,
      data_subset, clone_subsets,
      lock_manager);
  } else if (soid.snap == CEPH_NOSNAP) {
    // pushing head or unversioned object.
    // base this on partially on replica's clones?
    SnapSetContext *ssc = obc->ssc;
    ceph_assert(ssc);
    dout(15) << "push_to_replica snapset is " << ssc->snapset << dendl;
    calc_head_subsets(
      obc,
      ssc->snapset, soid, get_parent()->get_shard_missing().find(peer)->second,
      get_parent()->get_shard_info().find(peer)->second.last_backfill,
      data_subset, clone_subsets,
      lock_manager);
  }

  return prep_push(
    obc,
    soid,
    peer,
    oi.version,
    data_subset,
    clone_subsets,
    pop,
    cache_dont_need,
    std::move(lock_manager));
}

int ReplicatedBackend::prep_push(ObjectContextRef obc,
			     const hobject_t& soid, pg_shard_t peer,
			     PushOp *pop, bool cache_dont_need)
{
  interval_set<uint64_t> data_subset;
  if (obc->obs.oi.size)
    data_subset.insert(0, obc->obs.oi.size);
  map<hobject_t, interval_set<uint64_t>> clone_subsets;

  return prep_push(obc, soid, peer,
	    obc->obs.oi.version, data_subset, clone_subsets,
	    pop, cache_dont_need, ObcLockManager());
}

int ReplicatedBackend::prep_push(
  ObjectContextRef obc,
  const hobject_t& soid, pg_shard_t peer,
  eversion_t version,
  interval_set<uint64_t> &data_subset,
  map<hobject_t, interval_set<uint64_t>>& clone_subsets,
  PushOp *pop,
  bool cache_dont_need,
  ObcLockManager &&lock_manager)
{
  get_parent()->begin_peer_recover(peer, soid);
  const auto pmissing_iter = get_parent()->get_shard_missing().find(peer);
  const auto missing_iter = pmissing_iter->second.get_items().find(soid);
  assert(missing_iter != pmissing_iter->second.get_items().end());
  // take note.
  PushInfo &pi = pushing[soid][peer];
  pi.obc = obc;
  pi.recovery_info.size = obc->obs.oi.size;
  pi.recovery_info.copy_subset = data_subset;
  pi.recovery_info.clone_subset = clone_subsets;
  pi.recovery_info.soid = soid;
  pi.recovery_info.oi = obc->obs.oi;
  pi.recovery_info.ss = pop->recovery_info.ss;
  pi.recovery_info.version = version;
  pi.recovery_info.object_exist = missing_iter->second.clean_regions.object_is_exist();
  pi.recovery_progress.omap_complete = !missing_iter->second.clean_regions.omap_is_dirty() &&
    HAVE_FEATURE(parent->min_peer_features(), SERVER_OCTOPUS);
  pi.lock_manager = std::move(lock_manager);

  ObjectRecoveryProgress new_progress;
  int r = build_push_op(pi.recovery_info,
			pi.recovery_progress,
			&new_progress,
			pop,
			&(pi.stat), cache_dont_need);
  if (r < 0)
    return r;
  pi.recovery_progress = new_progress;
  return 0;
}

void ReplicatedBackend::submit_push_data(
  const ObjectRecoveryInfo &recovery_info,
  bool first,
  bool complete,
  bool clear_omap,
  bool cache_dont_need,
  interval_set<uint64_t> &data_zeros,
  const interval_set<uint64_t> &intervals_included,
  bufferlist data_included,
  bufferlist omap_header,
  const map<string, bufferlist> &attrs,
  const map<string, bufferlist> &omap_entries,
  ObjectStore::Transaction *t)
{
  hobject_t target_oid;
  if (first && complete) {
    target_oid = recovery_info.soid;
  } else {
    target_oid = get_parent()->get_temp_recovery_object(recovery_info.soid,
							recovery_info.version);
    if (first) {
      dout(10) << __func__ << ": Adding oid "
	       << target_oid << " in the temp collection" << dendl;
      add_temp_obj(target_oid);
    }
  }

  if (first) {
    if (!complete) {
      t->remove(coll, ghobject_t(target_oid));
      t->touch(coll, ghobject_t(target_oid));
      bufferlist bv = attrs.at(OI_ATTR);
      object_info_t oi(bv);
      t->set_alloc_hint(coll, ghobject_t(target_oid),
		        oi.expected_object_size,
		        oi.expected_write_size,
		        oi.alloc_hint_flags);
      } else {
        if (!recovery_info.object_exist) {
	  t->remove(coll, ghobject_t(target_oid));
          t->touch(coll, ghobject_t(target_oid));
          bufferlist bv = attrs.at(OI_ATTR);
          object_info_t oi(bv);
          t->set_alloc_hint(coll, ghobject_t(target_oid),
                            oi.expected_object_size,
                            oi.expected_write_size,
                            oi.alloc_hint_flags);
        }
        //remove xattr and update later if overwrite on original object
        t->rmattrs(coll, ghobject_t(target_oid));
        //if need update omap, clear the previous content first
        if (clear_omap)
          t->omap_clear(coll, ghobject_t(target_oid));
      }

    t->truncate(coll, ghobject_t(target_oid), recovery_info.size);
    if (omap_header.length())
      t->omap_setheader(coll, ghobject_t(target_oid), omap_header);

    struct stat st;
    int r = store->stat(ch, ghobject_t(recovery_info.soid), &st);
    if (get_parent()->pg_is_remote_backfilling()) {
      uint64_t size = 0;
      if (r == 0)
        size = st.st_size;
      // Don't need to do anything if object is still the same size
      if (size != recovery_info.oi.size) {
        get_parent()->pg_add_local_num_bytes((int64_t)recovery_info.oi.size - (int64_t)size);
        get_parent()->pg_add_num_bytes((int64_t)recovery_info.oi.size - (int64_t)size);
        dout(10) << __func__ << " " << recovery_info.soid
               << " backfill size " << recovery_info.oi.size
               << " previous size " << size
               << " net size " << recovery_info.oi.size - size
               << dendl;
      }
    }
    if (!complete) {
      //clone overlap content in local object
      if (recovery_info.object_exist) {
        assert(r == 0);
        uint64_t local_size = std::min(recovery_info.size, (uint64_t)st.st_size);
        interval_set<uint64_t> local_intervals_included, local_intervals_excluded;
        if (local_size) {
          local_intervals_included.insert(0, local_size);
          local_intervals_excluded.intersection_of(local_intervals_included, recovery_info.copy_subset);
          local_intervals_included.subtract(local_intervals_excluded);
        }
       for (interval_set<uint64_t>::const_iterator q = local_intervals_included.begin();
          q != local_intervals_included.end();
         ++q) {
         dout(15) << " clone_range " << recovery_info.soid << " "
                  << q.get_start() << "~" << q.get_len() << dendl;
         t->clone_range(coll, ghobject_t(recovery_info.soid), ghobject_t(target_oid),
             q.get_start(), q.get_len(), q.get_start());
        }
      }
    }
  }
  uint64_t off = 0;
  uint32_t fadvise_flags = CEPH_OSD_OP_FLAG_FADVISE_SEQUENTIAL;
  if (cache_dont_need)
    fadvise_flags |= CEPH_OSD_OP_FLAG_FADVISE_DONTNEED;
  // Punch zeros for data, if fiemap indicates nothing but it is marked dirty
  if (data_zeros.size() > 0) {
    data_zeros.intersection_of(recovery_info.copy_subset);
    assert(intervals_included.subset_of(data_zeros));
    data_zeros.subtract(intervals_included);

    dout(20) << __func__ <<" recovering object " << recovery_info.soid
             << " copy_subset: " << recovery_info.copy_subset
             << " intervals_included: " << intervals_included
             << " data_zeros: " << data_zeros << dendl;

    for (auto p = data_zeros.begin(); p != data_zeros.end(); ++p)
      t->zero(coll, ghobject_t(target_oid), p.get_start(), p.get_len());
  }
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

void ReplicatedBackend::submit_push_complete(
  const ObjectRecoveryInfo &recovery_info,
  ObjectStore::Transaction *t)
{
  for (map<hobject_t, interval_set<uint64_t>>::const_iterator p =
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
  SnapSetContext *ssc,
  ObcLockManager &manager)
{
  if (!recovery_info.soid.snap || recovery_info.soid.snap >= CEPH_NOSNAP)
    return recovery_info;
  ObjectRecoveryInfo new_info = recovery_info;
  new_info.copy_subset.clear();
  new_info.clone_subset.clear();
  ceph_assert(ssc);
  get_parent()->release_locks(manager); // might already have locks
  calc_clone_subsets(
    ssc->snapset, new_info.soid, get_parent()->get_local_missing(),
    get_info().last_backfill,
    new_info.copy_subset, new_info.clone_subset,
    manager);
  return new_info;
}

bool ReplicatedBackend::handle_pull_response(
  pg_shard_t from, const PushOp &pop, PullOp *response,
  list<pull_complete_info> *to_continue,
  ObjectStore::Transaction *t)
{
  interval_set<uint64_t> data_included = pop.data_included;
  bufferlist data;
  data = pop.data;
  dout(10) << "handle_pull_response "
	   << pop.recovery_info
	   << pop.after_progress
	   << " data.size() is " << data.length()
	   << " data_included: " << data_included
	   << dendl;
  if (pop.version == eversion_t()) {
    // replica doesn't have it!
    _failed_pull(from, pop.soid);
    return false;
  }

  const hobject_t &hoid = pop.soid;
  ceph_assert((data_included.empty() && data.length() == 0) ||
         (!data_included.empty() && data.length() > 0));

  auto piter = pulling.find(hoid);
  if (piter == pulling.end()) {
    return false;
  }

  PullInfo &pi = piter->second;
  if (pi.recovery_info.size == (uint64_t(-1))) {
    pi.recovery_info.size = pop.recovery_info.size;
    pi.recovery_info.copy_subset.intersection_of(
      pop.recovery_info.copy_subset);
  }
  // If primary doesn't have object info and didn't know version
  if (pi.recovery_info.version == eversion_t()) {
    pi.recovery_info.version = pop.version;
  }

  bool first = pi.recovery_progress.first;
  if (first) {
    // attrs only reference the origin bufferlist (decode from
    // MOSDPGPush message) whose size is much greater than attrs in
    // recovery. If obc cache it (get_obc maybe cache the attr), this
    // causes the whole origin bufferlist would not be free until obc
    // is evicted from obc cache. So rebuild the bufferlists before
    // cache it.
    auto attrset = pop.attrset;
    for (auto& a : attrset) {
      a.second.rebuild();
    }
    pi.obc = get_parent()->get_obc(pi.recovery_info.soid, attrset);
    if (attrset.find(SS_ATTR) != attrset.end()) {
      bufferlist ssbv = attrset.at(SS_ATTR);
      SnapSet ss(ssbv);
      assert(!pi.obc->ssc->exists || ss.seq  == pi.obc->ssc->snapset.seq);
    }
    pi.recovery_info.oi = pi.obc->obs.oi;
    pi.recovery_info = recalc_subsets(
      pi.recovery_info,
      pi.obc->ssc,
      pi.lock_manager);
  }


  interval_set<uint64_t> usable_intervals;
  bufferlist usable_data;
  trim_pushed_data(pi.recovery_info.copy_subset,
		   data_included,
		   data,
		   &usable_intervals,
		   &usable_data);
  data_included = usable_intervals;
  data = std::move(usable_data);


  pi.recovery_progress = pop.after_progress;

  dout(10) << "new recovery_info " << pi.recovery_info
           << ", new progress " << pi.recovery_progress
           << dendl;
  interval_set<uint64_t> data_zeros;
  uint64_t z_offset = pop.before_progress.data_recovered_to;
  uint64_t z_length = pop.after_progress.data_recovered_to - pop.before_progress.data_recovered_to;
  if(z_length)
    data_zeros.insert(z_offset, z_length);
  bool complete = pi.is_complete();
  bool clear_omap = !pop.before_progress.omap_complete;

  submit_push_data(pi.recovery_info,
                  first,
                  complete,
                  clear_omap,
                  pi.cache_dont_need,
                  data_zeros,
                  data_included,
                  data,
                  pop.omap_header,
                  pop.attrset,
                  pop.omap_entries,
                  t);

  pi.stat.num_keys_recovered += pop.omap_entries.size();
  pi.stat.num_bytes_recovered += data.length();
  get_parent()->get_logger()->inc(l_osd_rbytes, pop.omap_entries.size() + data.length());

  if (complete) {
    pi.stat.num_objects_recovered++;
    // XXX: This could overcount if regular recovery is needed right after a repair
    if (get_parent()->pg_is_repair()) {
      pi.stat.num_objects_repaired++;
      get_parent()->inc_osd_stat_repaired();
    }
    clear_pull_from(piter);
    to_continue->push_back({hoid, pi.stat});
    get_parent()->on_local_recover(
      hoid, pi.recovery_info, pi.obc, false, t);
    return false;
  } else {
    response->soid = pop.soid;
    response->recovery_info = pi.recovery_info;
    response->recovery_progress = pi.recovery_progress;
    return true;
  }
}

void ReplicatedBackend::handle_push(
  pg_shard_t from, const PushOp &pop, PushReplyOp *response,
  ObjectStore::Transaction *t, bool is_repair)
{
  dout(10) << "handle_push "
	   << pop.recovery_info
	   << pop.after_progress
	   << dendl;
  bufferlist data;
  data = pop.data;
  bool first = pop.before_progress.first;
  bool complete = pop.after_progress.data_complete &&
    pop.after_progress.omap_complete;
  bool clear_omap = !pop.before_progress.omap_complete;
  interval_set<uint64_t> data_zeros;
  uint64_t z_offset = pop.before_progress.data_recovered_to;
  uint64_t z_length = pop.after_progress.data_recovered_to - pop.before_progress.data_recovered_to;
  if(z_length)
    data_zeros.insert(z_offset, z_length);
  response->soid = pop.recovery_info.soid;

  submit_push_data(pop.recovery_info,
		   first,
		   complete,
		   clear_omap,
		   true, // must be replicate
		   data_zeros,
		   pop.data_included,
		   data,
		   pop.omap_header,
		   pop.attrset,
		   pop.omap_entries,
		   t);

  if (complete) {
    if (is_repair) {
      get_parent()->inc_osd_stat_repaired();
      dout(20) << __func__ << " repair complete" << dendl;
    }
    get_parent()->on_local_recover(
      pop.recovery_info.soid,
      pop.recovery_info,
      ObjectContextRef(), // ok, is replica
      false,
      t);
  }
}

void ReplicatedBackend::send_pushes(int prio, map<pg_shard_t, vector<PushOp> > &pushes)
{
  for (map<pg_shard_t, vector<PushOp> >::iterator i = pushes.begin();
       i != pushes.end();
       ++i) {
    ConnectionRef con = get_parent()->get_con_osd_cluster(
      i->first.osd,
      get_osdmap_epoch());
    if (!con)
      continue;
    vector<PushOp>::iterator j = i->second.begin();
    while (j != i->second.end()) {
      uint64_t cost = 0;
      uint64_t pushes = 0;
      MOSDPGPush *msg = new MOSDPGPush();
      msg->from = get_parent()->whoami_shard();
      msg->pgid = get_parent()->primary_spg_t();
      msg->map_epoch = get_osdmap_epoch();
      msg->min_epoch = get_parent()->get_last_peering_reset_epoch();
      msg->set_priority(prio);
      msg->is_repair = get_parent()->pg_is_repair();
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
      msg->set_cost(cost);
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
      get_osdmap_epoch());
    if (!con)
      continue;
    dout(20) << __func__ << ": sending pulls " << i->second
	     << " to osd." << i->first << dendl;
    MOSDPGPull *msg = new MOSDPGPull();
    msg->from = parent->whoami_shard();
    msg->set_priority(prio);
    msg->pgid = get_parent()->primary_spg_t();
    msg->map_epoch = get_osdmap_epoch();
    msg->min_epoch = get_parent()->get_last_peering_reset_epoch();
    msg->set_pulls(&i->second);
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

  dout(7) << __func__ << " " << recovery_info.soid
	  << " v " << recovery_info.version
	  << " size " << recovery_info.size
	  << " recovery_info: " << recovery_info
          << dendl;

  eversion_t v  = recovery_info.version;
  object_info_t oi;
  if (progress.first) {
    int r = store->omap_get_header(ch, ghobject_t(recovery_info.soid), &out_op->omap_header);
    if(r < 0) {
      dout(1) << __func__ << " get omap header failed: " << cpp_strerror(-r) << dendl; 
      return r;
    }
    r = store->getattrs(ch, ghobject_t(recovery_info.soid), out_op->attrset);
    if(r < 0) {
      dout(1) << __func__ << " getattrs failed: " << cpp_strerror(-r) << dendl;
      return r;
    }

    // Debug
    bufferlist bv = out_op->attrset[OI_ATTR];
    try {
     auto bliter = bv.cbegin();
     decode(oi, bliter);
    } catch (...) {
      dout(0) << __func__ << ": bad object_info_t: " << recovery_info.soid << dendl;
      return -EINVAL;
    }

    // If requestor didn't know the version, use ours
    if (v == eversion_t()) {
      v = oi.version;
    } else if (oi.version != v) {
      get_parent()->clog_error() << get_info().pgid << " push "
				 << recovery_info.soid << " v "
				 << recovery_info.version
				 << " failed because local copy is "
				 << oi.version;
      return -EINVAL;
    }

    new_progress.first = false;
  }
  // Once we provide the version subsequent requests will have it, so
  // at this point it must be known.
  ceph_assert(v != eversion_t());

  uint64_t available = cct->_conf->osd_recovery_max_chunk;
  if (!progress.omap_complete) {
    ObjectMap::ObjectMapIterator iter =
      store->get_omap_iterator(ch,
			       ghobject_t(recovery_info.soid));
    ceph_assert(iter);
    for (iter->lower_bound(progress.omap_recovered_to);
	 iter->valid();
	 iter->next()) {
      if (!out_op->omap_entries.empty() &&
	  ((cct->_conf->osd_recovery_max_omap_entries_per_chunk > 0 &&
	    out_op->omap_entries.size() >= cct->_conf->osd_recovery_max_omap_entries_per_chunk) ||
	   available <= iter->key().size() + iter->value().length()))
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
      map<uint64_t, uint64_t> m;
      int r = store->fiemap(ch, ghobject_t(recovery_info.soid), 0,
                            copy_subset.range_end(), m);
      if (r >= 0)  {
        interval_set<uint64_t> fiemap_included(std::move(m));
        copy_subset.intersection_of(fiemap_included);
      } else {
        // intersection of copy_subset and empty interval_set would be empty anyway
        copy_subset.clear();
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

  auto origin_size = out_op->data_included.size();
  bufferlist bit;
  int r = store->readv(ch, ghobject_t(recovery_info.soid),
		       out_op->data_included, bit,
                       cache_dont_need ? CEPH_OSD_OP_FLAG_FADVISE_DONTNEED: 0);
  if (cct->_conf->osd_debug_random_push_read_error &&
        (rand() % (int)(cct->_conf->osd_debug_random_push_read_error * 100.0)) == 0) {
    dout(0) << __func__ << ": inject EIO " << recovery_info.soid << dendl;
    r = -EIO;
  }
  if (r < 0) {
    return r;
  }
  if (out_op->data_included.size() != origin_size) {
    dout(10) << __func__ << " some extents get pruned "
             << out_op->data_included.size() << "/" << origin_size
             << dendl;
    new_progress.data_complete = true;
  }
  out_op->data.claim_append(bit);
  if (progress.first && !out_op->data_included.empty() &&
      out_op->data_included.begin().get_start() == 0 &&
      out_op->data.length() == oi.size && oi.is_data_digest()) {
    uint32_t crc = out_op->data.crc32c(-1);
    if (oi.data_digest != crc) {
      dout(0) << __func__ << " " << coll << std::hex
                         << " full-object read crc 0x" << crc
                         << " != expected 0x" << oi.data_digest
                         << std::dec << " on " << recovery_info.soid << dendl;
      return -EIO;
    }
  }

  if (new_progress.is_complete(recovery_info)) {
    new_progress.data_complete = true;
    if (stat) {
      stat->num_objects_recovered++;
      if (get_parent()->pg_is_repair())
        stat->num_objects_repaired++;
    }
  } else if (progress.first && progress.omap_complete) {
    // If omap is not changed, we need recovery omap when recovery cannot be completed once
    new_progress.omap_complete = false;
  }

  if (stat) {
    stat->num_keys_recovered += out_op->omap_entries.size();
    stat->num_bytes_recovered += out_op->data.length();
    get_parent()->get_logger()->inc(l_osd_rbytes, out_op->omap_entries.size() + out_op->data.length());
  }

  get_parent()->get_logger()->inc(l_osd_push);
  get_parent()->get_logger()->inc(l_osd_push_outb, out_op->data.length());

  // send
  out_op->version = v;
  out_op->soid = recovery_info.soid;
  out_op->recovery_info = recovery_info;
  out_op->after_progress = new_progress;
  out_op->before_progress = progress;
  return 0;
}

void ReplicatedBackend::prep_push_op_blank(const hobject_t& soid, PushOp *op)
{
  op->recovery_info.version = eversion_t();
  op->version = eversion_t();
  op->soid = soid;
}

bool ReplicatedBackend::handle_push_reply(
  pg_shard_t peer, const PushReplyOp &op, PushOp *reply)
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
    bool error = pushing[soid].begin()->second.recovery_progress.error;

    if (!pi->recovery_progress.data_complete && !error) {
      dout(10) << " pushing more from, "
	       << pi->recovery_progress.data_recovered_to
	       << " of " << pi->recovery_info.copy_subset << dendl;
      ObjectRecoveryProgress new_progress;
      int r = build_push_op(
	pi->recovery_info,
	pi->recovery_progress, &new_progress, reply,
	&(pi->stat));
      // Handle the case of a read error right after we wrote, which is
      // hopefully extremely rare.
      if (r < 0) {
        dout(5) << __func__ << ": oid " << soid << " error " << r << dendl;

	error = true;
	goto done;
      }
      pi->recovery_progress = new_progress;
      return true;
    } else {
      // done!
done:
      if (!error)
	get_parent()->on_peer_recover( peer, soid, pi->recovery_info);

      get_parent()->release_locks(pi->lock_manager);
      object_stat_sum_t stat = pi->stat;
      eversion_t v = pi->recovery_info.version;
      pushing[soid].erase(peer);
      pi = NULL;

      if (pushing[soid].empty()) {
	if (!error)
	  get_parent()->on_global_recover(soid, stat, false);
	else
	  get_parent()->on_failed_pull(
	    std::set<pg_shard_t>{ get_parent()->whoami_shard() },
	    soid,
	    v);
	pushing.erase(soid);
      } else {
	// This looks weird, but we erased the current peer and need to remember
	// the error on any other one, while getting more acks.
	if (error)
	  pushing[soid].begin()->second.recovery_progress.error = true;
	dout(10) << "pushed " << soid << ", still waiting for push ack from "
		 << pushing[soid].size() << " others" << dendl;
      }
      return false;
    }
  }
}

void ReplicatedBackend::handle_pull(pg_shard_t peer, PullOp &op, PushOp *reply)
{
  const hobject_t &soid = op.soid;
  struct stat st;
  int r = store->stat(ch, ghobject_t(soid), &st);
  if (r != 0) {
    get_parent()->clog_error() << get_info().pgid << " "
			       << peer << " tried to pull " << soid
			       << " but got " << cpp_strerror(-r);
    prep_push_op_blank(soid, reply);
  } else {
    ObjectRecoveryInfo &recovery_info = op.recovery_info;
    ObjectRecoveryProgress &progress = op.recovery_progress;
    if (progress.first && recovery_info.size == ((uint64_t)-1)) {
      // Adjust size and copy_subset
      recovery_info.size = st.st_size;
      if (st.st_size) {
        interval_set<uint64_t> object_range;
        object_range.insert(0, st.st_size);
        recovery_info.copy_subset.intersection_of(object_range);
      } else {
        recovery_info.copy_subset.clear();
      }
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

void ReplicatedBackend::_failed_pull(pg_shard_t from, const hobject_t &soid)
{
  dout(20) << __func__ << ": " << soid << " from " << from << dendl;
  auto it = pulling.find(soid);
  assert(it != pulling.end());
  get_parent()->on_failed_pull(
    { from },
    soid,
    it->second.recovery_info.version);

  clear_pull(it);
}

void ReplicatedBackend::clear_pull_from(
  map<hobject_t, PullInfo>::iterator piter)
{
  auto from = piter->second.from;
  pull_from_peer[from].erase(piter->second.soid);
  if (pull_from_peer[from].empty())
    pull_from_peer.erase(from);
}

void ReplicatedBackend::clear_pull(
  map<hobject_t, PullInfo>::iterator piter,
  bool clear_pull_from_peer)
{
  if (clear_pull_from_peer) {
    clear_pull_from(piter);
  }
  get_parent()->release_locks(piter->second.lock_manager);
  pulling.erase(piter);
}

int ReplicatedBackend::start_pushes(
  const hobject_t &soid,
  ObjectContextRef obc,
  RPGHandle *h)
{
  list< map<pg_shard_t, pg_missing_t>::const_iterator > shards;

  dout(20) << __func__ << " soid " << soid << dendl;
  // who needs it?
  ceph_assert(get_parent()->get_acting_recovery_backfill_shards().size() > 0);
  for (set<pg_shard_t>::iterator i =
	 get_parent()->get_acting_recovery_backfill_shards().begin();
       i != get_parent()->get_acting_recovery_backfill_shards().end();
       ++i) {
    if (*i == get_parent()->whoami_shard()) continue;
    pg_shard_t peer = *i;
    map<pg_shard_t, pg_missing_t>::const_iterator j =
      get_parent()->get_shard_missing().find(peer);
    ceph_assert(j != get_parent()->get_shard_missing().end());
    if (j->second.is_missing(soid)) {
      shards.push_back(j);
    }
  }

  // If more than 1 read will occur ignore possible request to not cache
  bool cache = shards.size() == 1 ? h->cache_dont_need : false;

  for (auto j : shards) {
    pg_shard_t peer = j->first;
    h->pushes[peer].push_back(PushOp());
    int r = prep_push_to_replica(obc, soid, peer,
	    &(h->pushes[peer].back()), cache);
    if (r < 0) {
      // Back out all failed reads
      for (auto k : shards) {
	pg_shard_t p = k->first;
	dout(10) << __func__ << " clean up peer " << p << dendl;
	h->pushes[p].pop_back();
	if (p == peer) break;
      }
      return r;
    }
  }
  return shards.size();
}
