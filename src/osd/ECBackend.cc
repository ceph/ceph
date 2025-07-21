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

#include "ECBackend.h"

#include <iostream>

#include "ECInject.h"
#include "messages/MOSDPGPush.h"
#include "messages/MOSDPGPushReply.h"
#include "messages/MOSDECSubOpWrite.h"
#include "messages/MOSDECSubOpWriteReply.h"
#include "messages/MOSDECSubOpRead.h"
#include "messages/MOSDECSubOpReadReply.h"
#include "common/debug.h"
#include "ECMsgTypes.h"
#include "ECTypes.h"
#include "ECSwitch.h"

#include "PrimaryLogPG.h"

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

static ostream &_prefix(std::ostream *_dout, ECBackend *pgb) {
  return pgb->get_parent()->gen_dbg_prefix(*_dout);
}

static ostream &_prefix(std::ostream *_dout, ECBackend::RecoveryBackend *pgb) {
  return pgb->get_parent()->gen_dbg_prefix(*_dout);
}

struct ECRecoveryHandle : public PGBackend::RecoveryHandle {
  list<ECBackend::RecoveryBackend::RecoveryOp> ops;
};

void ECBackend::RecoveryBackend::RecoveryOp::dump(Formatter *f) const {
  f->dump_stream("hoid") << hoid;
  f->dump_stream("v") << v;
  f->dump_stream("missing_on") << missing_on;
  f->dump_stream("missing_on_shards") << missing_on_shards;
  f->dump_stream("recovery_info") << recovery_info;
  f->dump_stream("recovery_progress") << recovery_progress;
  f->dump_stream("state") << tostr(state);
  f->dump_stream("waiting_on_pushes") << waiting_on_pushes;
}

ECBackend::ECBackend(
  PGBackend::Listener *pg,
  CephContext *cct,
  ErasureCodeInterfaceRef ec_impl,
  uint64_t stripe_width,
  ECSwitch *s,
  ECExtentCache::LRU &ec_extent_cache_lru)
  : parent(pg), cct(cct), switcher(s),
    read_pipeline(cct, ec_impl, this->sinfo, get_parent()->get_eclistener()),
    rmw_pipeline(cct, ec_impl, this->sinfo, get_parent()->get_eclistener(),
                 *this, ec_extent_cache_lru),
    recovery_backend(cct, switcher->coll, ec_impl, this->sinfo, read_pipeline,
                     unstable_hashinfo_registry, get_parent(), this),
    ec_impl(ec_impl),
    sinfo(ec_impl, &(get_parent()->get_pool()), stripe_width),
    unstable_hashinfo_registry(cct, ec_impl) {

  /* EC makes some assumptions about how the plugin organises the *data* shards:
   * - The chunk size is constant for a particular profile.
   * - A stripe consists of k chunks.
   */
  ceph_assert((ec_impl->get_data_chunk_count() *
    ec_impl->get_chunk_size(stripe_width)) == stripe_width);
}

PGBackend::RecoveryHandle *ECBackend::open_recovery_op() {
  return recovery_backend.open_recovery_op();
}

ECBackend::RecoveryBackend::RecoveryBackend(
  CephContext *cct,
  const coll_t &coll,
  ceph::ErasureCodeInterfaceRef ec_impl,
  const ECUtil::stripe_info_t &sinfo,
  ReadPipeline &read_pipeline,
  UnstableHashInfoRegistry &unstable_hashinfo_registry,
  ECListener *parent,
  ECBackend *ecbackend)
  : cct(cct),
    coll(coll),
    ec_impl(std::move(ec_impl)),
    sinfo(sinfo),
    read_pipeline(read_pipeline),
    unstable_hashinfo_registry(unstable_hashinfo_registry),
    parent(parent),
    ecbackend(ecbackend) {}

PGBackend::RecoveryHandle *ECBackend::RecoveryBackend::open_recovery_op() {
  return new ECRecoveryHandle;
}

void ECBackend::RecoveryBackend::_failed_push(const hobject_t &hoid,
                                              ECCommon::read_result_t &res) {
  dout(10) << __func__ << ": Read error " << hoid << " r="
	   << res.r << " errors=" << res.errors << dendl;
  dout(10) << __func__ << ": canceling recovery op for obj " << hoid
	   << dendl;
  ceph_assert(recovery_ops.count(hoid));
  eversion_t v = recovery_ops[hoid].v;
  recovery_ops.erase(hoid);

  set<pg_shard_t> fl;
  for (auto &&i: res.errors) {
    fl.insert(i.first);
  }
  get_parent()->on_failed_pull(fl, hoid, v);
}

struct RecoveryMessages {
  map<hobject_t, ECCommon::read_request_t> recovery_reads;

  void recovery_read(const hobject_t &hoid,
                     const ECCommon::read_request_t &read_request) {
    ceph_assert(!recovery_reads.count(hoid));
    recovery_reads.insert(make_pair(hoid, read_request));
  }

  map<pg_shard_t, vector<PushOp>> pushes;
  map<pg_shard_t, vector<PushReplyOp>> push_replies;
  ObjectStore::Transaction t;
};

void ECBackend::handle_recovery_push(
  const PushOp &op,
  RecoveryMessages *m,
  bool is_repair) {
  if (get_parent()->pg_is_remote_backfilling()) {
    get_parent()->pg_add_local_num_bytes(op.data.length());
    get_parent()->pg_add_num_bytes(op.data.length() * sinfo.get_k());
    dout(10) << __func__ << " " << op.soid
             << " add new actual data by " << op.data.length()
             << " add new num_bytes by " << op.data.length() * sinfo.get_k()
             << dendl;
  }

  recovery_backend.handle_recovery_push(op, m, is_repair);

  if (op.after_progress.data_complete &&
    !(get_parent()->pgb_is_primary()) &&
    get_parent()->pg_is_remote_backfilling()) {
    struct stat st;
    int r = switcher->store->stat(switcher->ch, ghobject_t(
                                    op.soid, ghobject_t::NO_GEN,
                                    get_parent()->whoami_shard().shard), &st);
    if (r == 0) {
      get_parent()->pg_sub_local_num_bytes(st.st_size);
      // XXX: This can be way overestimated for small objects
      get_parent()->pg_sub_num_bytes(st.st_size * sinfo.get_k());
      dout(10) << __func__ << " " << op.soid
               << " sub actual data by " << st.st_size
               << " sub num_bytes by " << st.st_size * sinfo.get_k()
               << dendl;
    }
  }
}

void ECBackend::RecoveryBackend::handle_recovery_push(
  const PushOp &op,
  RecoveryMessages *m,
  bool is_repair) {
  if (get_parent()->check_failsafe_full()) {
    dout(10) << __func__ << " Out of space (failsafe) processing push request."
             << dendl;
    ceph_abort();
  }

  bool oneshot = op.before_progress.first && op.after_progress.data_complete;
  ghobject_t tobj;
  if (oneshot) {
    tobj = ghobject_t(op.soid, ghobject_t::NO_GEN,
                      get_parent()->whoami_shard().shard);
  } else {
    tobj = ghobject_t(get_parent()->get_temp_recovery_object(op.soid,
                                                             op.version),
                      ghobject_t::NO_GEN,
                      get_parent()->whoami_shard().shard);
    if (op.before_progress.first) {
      dout(10) << __func__ << ": Adding oid "
	       << tobj.hobj << " in the temp collection" << dendl;
      add_temp_obj(tobj.hobj);
    }
  }

  if (op.before_progress.first) {
    m->t.remove(coll, tobj);
    m->t.touch(coll, tobj);
  }

  if (!op.data_included.empty()) {
    uint64_t start = op.data_included.range_start();
    uint64_t end = op.data_included.range_end();
    ceph_assert(op.data.length() == (end - start));

    m->t.write(
      coll,
      tobj,
      start,
      op.data.length(),
      op.data);
  } else {
    ceph_assert(op.data.length() == 0);
  }

  if (op.before_progress.first) {
    ceph_assert(op.attrset.contains(OI_ATTR));
    m->t.setattrs(
      coll,
      tobj,
      op.attrset);
  }

  if (op.after_progress.data_complete && !oneshot) {
    dout(10) << __func__ << ": Removing oid "
	     << tobj.hobj << " from the temp collection" << dendl;
    clear_temp_obj(tobj.hobj);
    m->t.remove(coll, ghobject_t(
                  op.soid, ghobject_t::NO_GEN,
                  get_parent()->whoami_shard().shard));
    m->t.collection_move_rename(
      coll, tobj,
      coll, ghobject_t(
        op.soid, ghobject_t::NO_GEN, get_parent()->whoami_shard().shard));
  }
  if (op.after_progress.data_complete) {
    if ((get_parent()->pgb_is_primary())) {
      ceph_assert(recovery_ops.count(op.soid));
      ceph_assert(recovery_ops[op.soid].obc);
      if (get_parent()->pg_is_repair() || is_repair)
        get_parent()->inc_osd_stat_repaired();
      get_parent()->on_local_recover(
        op.soid,
        op.recovery_info,
        recovery_ops[op.soid].obc,
        false,
        &m->t);
    } else {
      // If primary told us this is a repair, bump osd_stat_t::num_objects_repaired
      if (is_repair)
        get_parent()->inc_osd_stat_repaired();
      get_parent()->on_local_recover(
        op.soid,
        op.recovery_info,
        ObjectContextRef(),
        false,
        &m->t);
    }
  }
  m->push_replies[get_parent()->primary_shard()].push_back(PushReplyOp());
  m->push_replies[get_parent()->primary_shard()].back().soid = op.soid;
}

void ECBackend::RecoveryBackend::handle_recovery_push_reply(
    const PushReplyOp &op,
    pg_shard_t from,
    RecoveryMessages *m) {
  if (!recovery_ops.count(op.soid))
    return;
  RecoveryOp &rop = recovery_ops[op.soid];
  ceph_assert(rop.waiting_on_pushes.contains(from));
  rop.waiting_on_pushes.erase(from);
  continue_recovery_op(rop, m);
}

void ECBackend::RecoveryBackend::handle_recovery_read_complete(
    const hobject_t &hoid,
    ECUtil::shard_extent_map_t &&buffers_read,
    std::optional<map<string, bufferlist, less<>>> attrs,
    const ECUtil::shard_extent_set_t &want_to_read,
    RecoveryMessages *m) {
  dout(10) << __func__ << ": returned " << hoid << " " << buffers_read << dendl;
  ceph_assert(recovery_ops.contains(hoid));
  RecoveryBackend::RecoveryOp &op = recovery_ops[hoid];

  if (attrs) {
    op.xattrs.swap(*attrs);

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
      op.obc = get_parent()->get_obc(hoid, sanitized_attrs);
      ceph_assert(op.obc);
      op.recovery_info.size = op.obc->obs.oi.size;
      op.recovery_info.oi = op.obc->obs.oi;
    }

    if (sinfo.get_is_hinfo_required()) {
      ECUtil::HashInfo hinfo(sinfo.get_k_plus_m());
      if (op.obc->obs.oi.size > 0) {
        ceph_assert(op.xattrs.count(ECUtil::get_hinfo_key()));
        auto bp = op.xattrs[ECUtil::get_hinfo_key()].cbegin();
        decode(hinfo, bp);
      }
      op.hinfo = unstable_hashinfo_registry.maybe_put_hash_info(
        hoid, std::move(hinfo));
    }
  }
  ceph_assert(op.xattrs.size());
  ceph_assert(op.obc);

  op.returned_data.emplace(std::move(buffers_read));

  ECUtil::shard_extent_set_t read_mask(sinfo.get_k_plus_m());
  sinfo.ro_size_to_read_mask(op.recovery_info.size, read_mask);
  ECUtil::shard_extent_set_t shard_want_to_read(sinfo.get_k_plus_m());

  for (auto &[shard, eset] : want_to_read) {
    /* Read buffers do not need recovering! */
    if (buffers_read.contains(shard)) {
      continue;
    }

    /* Read-buffers will be truncated to the end-of-object. Do not attempt
     * to recover off-the-end.
     */
    shard_want_to_read[shard].intersection_of(read_mask.get(shard),eset);

    /* Some shards may be empty */
    if (shard_want_to_read[shard].empty()) {
      shard_want_to_read.erase(shard);
    }
  }

  uint64_t aligned_size = ECUtil::align_next(op.obc->obs.oi.size);

  int r = op.returned_data->decode(ec_impl, shard_want_to_read, aligned_size);
  ceph_assert(r == 0);

  // Finally, we don't want to write any padding, so truncate the buffer
  // to remove it.
  op.returned_data->erase_after_ro_offset(aligned_size);

  for (auto &&shard: op.missing_on_shards) {
    if (read_mask.contains(shard) && op.returned_data->contains_shard(shard)) {
      ceph_assert(read_mask.at(shard).range_end() >=
        op.returned_data->get_extent_map(shard).get_end_off());
    }
  }

  dout(20) << __func__ << ": oid=" << op.hoid << dendl;
  dout(30) << __func__ << "EC_DEBUG_BUFFERS: "
           << op.returned_data->debug_string(2048, 8)
           << dendl;

  continue_recovery_op(op, m);
}

struct SendPushReplies : public Context {
  PGBackend::Listener *l;
  epoch_t epoch;
  std::map<int, MOSDPGPushReply*> replies;

  SendPushReplies(
    PGBackend::Listener *l,
    epoch_t epoch,
    std::map<int, MOSDPGPushReply*> &in) : l(l), epoch(epoch) {
    replies.swap(in);
  }

  void finish(int) override {
    std::vector<std::pair<int, Message*>> messages;
    messages.reserve(replies.size());
    for (auto & reply : replies) {
      messages.push_back(reply);
    }
    if (!messages.empty()) {
      l->send_message_osd_cluster(messages, epoch);
    }
    replies.clear();
  }

  ~SendPushReplies() override {
    for (auto & [_, reply] : replies) {
      reply->put();
    }
    replies.clear();
  }
};

struct RecoveryReadCompleter : ECCommon::ReadCompleter {
  RecoveryReadCompleter(ECBackend::RecoveryBackend &backend)
    : backend(backend) {}

  void finish_single_request(
      const hobject_t &hoid,
      ECCommon::read_result_t &&res,
      ECCommon::read_request_t &req) override {
    if (!(res.r == 0 && res.errors.empty())) {
      backend._failed_push(hoid, res);
      return;
    }
    ceph_assert(req.to_read.size() == 0);
    backend.handle_recovery_read_complete(
      hoid,
      std::move(res.buffers_read),
      res.attrs,
      req.shard_want_to_read,
      &rm);
  }

  void finish(int priority) && override {
    backend.dispatch_recovery_messages(rm, priority);
  }

  ECBackend::RecoveryBackend &backend;
  RecoveryMessages rm;
};

void ECBackend::ECRecoveryBackend::commit_txn_send_replies(
  ceph::os::Transaction &&txn,
  std::map<int, MOSDPGPushReply*> replies) {
  txn.register_on_complete(
    get_parent()->bless_context(
      new SendPushReplies(
        get_parent(),
        get_osdmap_epoch(),
        replies)));
  get_parent()->queue_transaction(std::move(txn));
}

void ECBackend::RecoveryBackend::dispatch_recovery_messages(
  RecoveryMessages &m, int priority) {
  for (map<pg_shard_t, vector<PushOp>>::iterator i = m.pushes.begin();
       i != m.pushes.end();
       m.pushes.erase(i++)) {
    MOSDPGPush *msg = new MOSDPGPush();
    msg->set_priority(priority);
    msg->map_epoch = get_parent()->pgb_get_osdmap_epoch();
    msg->min_epoch = get_parent()->get_last_peering_reset_epoch();
    msg->from = get_parent()->whoami_shard();
    msg->pgid = spg_t(get_parent()->get_info().pgid.pgid, i->first.shard);
    msg->pushes.swap(i->second);
    msg->compute_cost(cct);
    msg->is_repair = get_parent()->pg_is_repair();
    std::vector wrapped_msg{
      std::make_pair(i->first.osd, static_cast<Message*>(msg))
    };
    get_parent()->send_message_osd_cluster(wrapped_msg, msg->map_epoch);
  }
  std::map<int, MOSDPGPushReply*> replies;
  for (map<pg_shard_t, vector<PushReplyOp>>::iterator i =
         m.push_replies.begin();
       i != m.push_replies.end();
       m.push_replies.erase(i++)) {
    MOSDPGPushReply *msg = new MOSDPGPushReply();
    msg->set_priority(priority);
    msg->map_epoch = get_parent()->pgb_get_osdmap_epoch();
    msg->min_epoch = get_parent()->get_last_peering_reset_epoch();
    msg->from = get_parent()->whoami_shard();
    msg->pgid = spg_t(get_parent()->get_info().pgid.pgid, i->first.shard);
    msg->replies.swap(i->second);
    msg->compute_cost(cct);
    replies.insert(std::pair(i->first.osd, msg));
  }

#if 1
  if (!replies.empty()) {
    commit_txn_send_replies(std::move(m.t), std::move(replies));
  }
#endif

  if (m.recovery_reads.empty())
    return;
  read_pipeline.start_read_op(
    priority,
    m.recovery_reads,
    false,
    true,
    std::make_unique<RecoveryReadCompleter>(*this));
}

void ECBackend::RecoveryBackend::continue_recovery_op(
  RecoveryBackend::RecoveryOp &op,
  RecoveryMessages *m) {
  dout(10) << __func__ << ": continuing " << op << dendl;
  using RecoveryOp = RecoveryBackend::RecoveryOp;
  while (1) {
    switch (op.state) {
    case RecoveryOp::IDLE: {
      ceph_assert(!op.recovery_progress.data_complete);
      ECUtil::shard_extent_set_t want(sinfo.get_k_plus_m());

      op.state = RecoveryOp::READING;

      /* When beginning recovery, the OI may not be known. As such the object
       * size is not known. For the first read, attempt to read the default
       * size.  If this is larger than the object sizes, then the OSD will
       * return truncated reads.  If the object size is known, then attempt
       * correctly sized reads.
       */
      uint64_t read_size = get_recovery_chunk_size();
      if (op.obc) {
        uint64_t read_to_end = ECUtil::align_next(op.obc->obs.oi.size) -
          op.recovery_progress.data_recovered_to;

        if (read_to_end < read_size) {
          read_size = read_to_end;
        }
      }
      sinfo.ro_range_to_shard_extent_set_with_parity(
        op.recovery_progress.data_recovered_to, read_size, want);

      op.recovery_progress.data_recovered_to += read_size;

      // We only need to recover shards that are missing.
      for (auto shard : shard_id_set::difference(sinfo.get_all_shards(), op.missing_on_shards)) {
        want.erase(shard);
      }

      if (op.recovery_progress.first && op.obc) {
        op.xattrs = op.obc->attr_cache;
        if (sinfo.get_is_hinfo_required()) {
          if (auto [r, attrs, size] = ecbackend->get_attrs_n_size_from_disk(
              op.hoid);
            r >= 0 || r == -ENOENT) {
            op.hinfo = unstable_hashinfo_registry.get_hash_info(
              op.hoid, false, attrs, size);
          } else {
            derr << __func__ << ": can't stat-or-getattr on " << op.hoid <<
          dendl;
          }
          if (!op.hinfo) {
            derr << __func__ << ": " << op.hoid << " has inconsistent hinfo"
               << dendl;
            ceph_assert(recovery_ops.count(op.hoid));
            eversion_t v = recovery_ops[op.hoid].v;
            recovery_ops.erase(op.hoid);
            // TODO: not in crimson yet
            get_parent()->on_failed_pull({get_parent()->whoami_shard()},
                                         op.hoid, v);
            return;
          }
          encode(*(op.hinfo), op.xattrs[ECUtil::get_hinfo_key()]);
        }
      }

      read_request_t read_request(std::move(want),
                                  op.recovery_progress.first && !op.obc,
                                  op.obc
                                    ? op.obc->obs.oi.size
                                    : get_recovery_chunk_size());

      int r = read_pipeline.get_min_avail_to_read_shards(
        op.hoid, true, false, read_request);

      if (r != 0) {
        // we must have lost a recovery source
        ceph_assert(!op.recovery_progress.first);
        dout(10) << __func__ << ": canceling recovery op for obj " << op.hoid
                 << dendl;
        // in crimson
        get_parent()->cancel_pull(op.hoid);
        recovery_ops.erase(op.hoid);
        return;
      }
      if (read_request.shard_reads.empty()) {
        ceph_assert(op.obc);
        /* This can happen for several reasons
         * - A zero-sized object.
         * - The missing shards have no data.
         * - The previous recovery did not need the last data shard. In this
         *   case, data_recovered_to may indicate that the last shard still
         *   needs recovery, when it does not.
         * We can just skip the read and fall through below.
         */
        dout(10) << __func__ << " No reads required " << op << dendl;
        // Create an empty read result and fall through.
        op.returned_data.emplace(&sinfo);
      } else {
        m->recovery_read(
          op.hoid,
          read_request);
        dout(10) << __func__ << ": IDLE return " << op << dendl;
        return;
      }
    }
      [[fallthrough]];
    case RecoveryOp::READING: {
      // read completed, start write
      ceph_assert(op.xattrs.size());
      ceph_assert(op.returned_data);
      dout(20) << __func__ << ": returned_data=" << op.returned_data << dendl;
      op.state = RecoveryOp::WRITING;
      ObjectRecoveryProgress after_progress = op.recovery_progress;
      after_progress.first = false;
      if (after_progress.data_recovered_to >= op.obc->obs.oi.size) {
        after_progress.data_complete = true;
      }
      for (auto &&pg_shard: op.missing_on) {
        m->pushes[pg_shard].push_back(PushOp());
        PushOp &pop = m->pushes[pg_shard].back();
        pop.soid = op.hoid;
        pop.version = op.recovery_info.oi.get_version_for_shard(pg_shard.shard);
        op.returned_data->get_shard_first_buffer(pg_shard.shard, pop.data);
        dout(10) << __func__ << ": pop shard=" << pg_shard
                 << ", oid=" << pop.soid
                 << ", before_progress=" << op.recovery_progress
		 << ", after_progress=" << after_progress
		 << ", pop.data.length()=" << pop.data.length()
		 << ", size=" << op.obc->obs.oi.size << dendl;
        if (pop.data.length())
          pop.data_included.union_insert(
            op.returned_data->get_shard_first_offset(pg_shard.shard),
            pop.data.length());
        if (op.recovery_progress.first) {
          if (sinfo.is_nonprimary_shard(pg_shard.shard)) {
            if (pop.version == op.recovery_info.oi.version) {
              dout(10) << __func__ << ": copy OI attr only" << dendl;
              pop.attrset[OI_ATTR] = op.xattrs[OI_ATTR];
            } else {
              // We are recovering a partial write - make sure we push the correct
              // version in the OI or a scrub error will occur.
              object_info_t oi(op.recovery_info.oi);
              oi.shard_versions.clear();
              oi.version = pop.version;
              dout(10) << __func__ << ": partial write OI attr: oi=" << oi << dendl;
              bufferlist bl;
              oi.encode(bl, get_osdmap()->get_features(
                CEPH_ENTITY_TYPE_OSD, nullptr));
              pop.attrset[OI_ATTR] = bl;
            }
          } else {
            dout(10) << __func__ << ": push all attrs (not nonprimary)" << dendl;
            pop.attrset = op.xattrs;
          }
        }
        pop.recovery_info = op.recovery_info;
        pop.before_progress = op.recovery_progress;
        pop.after_progress = after_progress;
        if (pg_shard != get_parent()->primary_shard()) {
          // already in crimson -- junction point with PeeringState
          get_parent()->begin_peer_recover(
            pg_shard,
            op.hoid);
        }
      }
      op.returned_data.reset();
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
                op.recovery_info);
            }
          }
          object_stat_sum_t stat;
          stat.num_bytes_recovered = op.recovery_info.size;
          stat.num_keys_recovered = 0; // ??? op ... omap_entries.size(); ?
          stat.num_objects_recovered = 1;
          // TODO: not in crimson yet
          if (get_parent()->pg_is_repair())
            stat.num_objects_repaired = 1;
          // pg_recovery.cc in crimson has it
          get_parent()->on_global_recover(op.hoid, stat, false);
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
    // should never be called once complete
    case RecoveryOp::COMPLETE:
    default: {
      ceph_abort();
    };
    }
  }
}

void ECBackend::run_recovery_op(
  PGBackend::RecoveryHandle *_h,
  int priority) {
  ceph_assert(_h);
  ECRecoveryHandle &h = static_cast<ECRecoveryHandle&>(*_h);
  recovery_backend.run_recovery_op(h, priority);
  switcher->send_recovery_deletes(priority, h.deletes);
  delete _h;
}

void ECBackend::RecoveryBackend::run_recovery_op(
  ECRecoveryHandle &h,
  int priority) {
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
  PGBackend::RecoveryHandle *_h) {
  return recovery_backend.recover_object(hoid, v, head, obc, _h);
}

int ECBackend::RecoveryBackend::recover_object(
  const hobject_t &hoid,
  eversion_t v,
  ObjectContextRef head,
  ObjectContextRef obc,
  PGBackend::RecoveryHandle *_h) {
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
  if (hoid.is_snap()) {
    if (obc) {
      ceph_assert(obc->ssc);
      h->ops.back().recovery_info.ss = obc->ssc->snapset;
    } else if (head) {
      ceph_assert(head->ssc);
      h->ops.back().recovery_info.ss = head->ssc->snapset;
    } else {
      ceph_abort_msg("neither obc nor head set for a snap object");
    }
  }
  h->ops.back().recovery_progress.omap_complete = true;
  for (set<pg_shard_t>::const_iterator i =
         get_parent()->get_acting_recovery_backfill_shards().begin();
       i != get_parent()->get_acting_recovery_backfill_shards().end();
       ++i) {
    dout(10) << "checking " << *i << dendl;
    if (get_parent()->get_shard_missing(*i).is_missing(hoid)) {
      h->ops.back().missing_on.insert(*i);
      h->ops.back().missing_on_shards.insert(i->shard);
    }
  }
  dout(10) << __func__ << ": built op " << h->ops.back() << dendl;
  return 0;
}

bool ECBackend::can_handle_while_inactive(
  OpRequestRef _op) {
  return false;
}

bool ECBackend::_handle_message(
  OpRequestRef _op) {
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
    handle_sub_write(op->op.from, _op, op->op, _op->pg_trace,
                     *get_parent()->get_eclistener());
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
    reply->map_epoch = switcher->get_osdmap_epoch();
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
    const MOSDPGPushReply *op = static_cast<const MOSDPGPushReply*>(
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
    r->map_epoch = switcher->get_osdmap_epoch();
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
      get_parent()->primary_shard().osd, r, switcher->get_osdmap_epoch());
  }
}

void ECBackend::handle_sub_write(
  pg_shard_t from,
  OpRequestRef msg,
  ECSubWrite &op,
  const ZTracer::Trace &trace,
  ECListener &) {
  if (msg) {
    msg->mark_event("sub_op_started");
  }
  trace.event("handle_sub_write");

  if (cct->_conf->bluestore_debug_inject_read_err &&
    ECInject::test_write_error3(op.soid)) {
    ceph_abort_msg("Error inject - OSD down");
  }
  if (!get_parent()->pgb_is_primary())
    get_parent()->update_stats(op.stats);
  ObjectStore::Transaction localt;
  if (!op.temp_added.empty()) {
    switcher->add_temp_objs(op.temp_added);
  }
  if (op.backfill_or_async_recovery) {
    for (set<hobject_t>::iterator i = op.temp_removed.begin();
         i != op.temp_removed.end();
         ++i) {
      dout(10) << __func__ << ": removing object " << *i
	       << " since we won't get the transaction" << dendl;
      localt.remove(
        switcher->coll,
        ghobject_t(
          *i,
          ghobject_t::NO_GEN,
          get_parent()->whoami_shard().shard));
    }
  }
  switcher->clear_temp_objs(op.temp_removed);
  dout(30) << __func__ << " missing before " <<
    get_parent()->get_log().get_missing().get_items() << dendl;
  // flag set to true during async recovery
  bool async = false;
  pg_missing_tracker_t pmissing = get_parent()->get_local_missing();
  if (pmissing.is_missing(op.soid)) {
    async = true;
    dout(30) << __func__ << " is_missing " <<
      pmissing.is_missing(op.soid) << dendl;
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
    get_parent()->whoami_shard().shard >= sinfo.get_k())
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
  dout(30) << __func__ << " missing after" << get_parent()->get_log().
                                                            get_missing().
                                                            get_items() << dendl
  ;
  if (op.at_version != eversion_t()) {
    // dummy rollforward transaction doesn't get at_version (and doesn't advance it)
    get_parent()->op_applied(op.at_version);
  }
}

void ECBackend::handle_sub_read(
  pg_shard_t from,
  const ECSubRead &op,
  ECSubReadReply *reply,
  const ZTracer::Trace &trace) {
  trace.event("handle sub read");
  shard_id_t shard = get_parent()->whoami_shard().shard;
  for (auto &&[hoid, to_read]: op.to_read) {
    int r = 0;
    for (auto &&[offset, len, flags]: to_read) {
      bufferlist bl;
      auto &subchunks = op.subchunks.at(hoid);
      if ((subchunks.size() == 1) &&
        (subchunks.front().second == ec_impl->get_sub_chunk_count())) {
        dout(20) << __func__ << " case1: reading the complete chunk/shard." << dendl;
        r = switcher->store->read(
          switcher->ch,
          ghobject_t(hoid, ghobject_t::NO_GEN, shard),
          offset, len, bl, flags); // Allow EIO return
      } else {
        int subchunk_size =
          sinfo.get_chunk_size() / ec_impl->get_sub_chunk_count();
        dout(20) << __func__ << " case2: going to do fragmented read;"
		 << " subchunk_size=" << subchunk_size
		 << " chunk_size=" << sinfo.get_chunk_size() << dendl;
        bool error = false;
        for (int m = 0; m < (int)len && !error;
             m += sinfo.get_chunk_size()) {
          for (auto &&k: subchunks) {
            bufferlist bl0;
            r = switcher->store->read(
              switcher->ch,
              ghobject_t(hoid, ghobject_t::NO_GEN, shard),
              offset + m + (k.first) * subchunk_size,
              (k.second) * subchunk_size,
              bl0, flags);
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
		  << " reading " << hoid << ", fast read, probably ok"
		  << dendl;
        } else {
          get_parent()->clog_error() << "Error " << r
            << " reading object " << hoid;
          dout(5) << __func__ << ": Error " << r
		  << " reading " << hoid << dendl;
        }
        goto error;
      } else {
        dout(20) << __func__ << " read request=" << len << " r=" << r << " len="
          << bl.length() << dendl;
        reply->buffers_read[hoid].push_back(make_pair(offset, bl));
      }

      if (!sinfo.supports_ec_overwrites()) {
        // This shows that we still need deep scrub because large enough files
        // are read in sections, so the digest check here won't be done here.
        // Do NOT check osd_read_eio_on_bad_digest here.  We need to report
        // the state of our chunk in case other chunks could substitute.
        ECUtil::HashInfoRef hinfo;
        map<string, bufferlist, less<>> attrs;
        struct stat st;
        int r = object_stat(hoid, &st);
        if (r >= 0) {
          dout(10) << __func__ << ": found on disk, size " << st.st_size << dendl;
          r = switcher->objects_get_attrs_with_hinfo(hoid, &attrs);
        }
        if (r >= 0) {
          hinfo = unstable_hashinfo_registry.get_hash_info(
            hoid, false, attrs, st.st_size);
        } else {
          derr << __func__ << ": access (attrs) on " << hoid << " failed: "
	       << cpp_strerror(r) << dendl;
        }
        if (!hinfo) {
          r = -EIO;
          get_parent()->clog_error() << "Corruption detected: object "
            << hoid << " is missing hash_info";
          dout(5) << __func__ << ": No hinfo for " << hoid << dendl;
          goto error;
        }
        ceph_assert(hinfo->has_chunk_hash());
        if ((bl.length() == hinfo->get_total_chunk_size()) &&
          (offset == 0)) {
          dout(20) << __func__ << ": Checking hash of " << hoid << dendl;
          bufferhash h(-1);
          h << bl;
          if (h.digest() != hinfo->get_chunk_hash(shard)) {
            get_parent()->clog_error() << "Bad hash for " << hoid <<
              " digest 0x"
              << hex << h.digest() << " expected 0x" << hinfo->
              get_chunk_hash(shard) << dec;
            dout(5) << __func__ << ": Bad hash for " << hoid << " digest 0x"
		    << hex << h.digest() << " expected 0x"
                    << hinfo->get_chunk_hash(shard) << dec << dendl;
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
    reply->buffers_read.erase(hoid);
    reply->errors[hoid] = r;
  }
  for (set<hobject_t>::iterator i = op.attrs_to_read.begin();
       i != op.attrs_to_read.end();
       ++i) {
    dout(10) << __func__ << ": fulfilling attr request on "
	     << *i << dendl;
    if (reply->errors.contains(*i))
      continue;
    int r = switcher->store->getattrs(
      switcher->ch,
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

void ECBackend::handle_sub_write_reply(
  pg_shard_t from,
  const ECSubWriteReply &ec_write_reply_op,
  const ZTracer::Trace &trace) {
  RMWPipeline::OpRef &op = rmw_pipeline.tid_to_op_map.at(ec_write_reply_op.tid);
  if (ec_write_reply_op.committed) {
    trace.event("sub write committed");
    ceph_assert(op->pending_commits > 0);
    op->pending_commits--;
    if (from != get_parent()->whoami_shard()) {
      get_parent()->update_peer_last_complete_ondisk(
        from, ec_write_reply_op.last_complete);
    }
  }

  if (cct->_conf->bluestore_debug_inject_read_err &&
    (op->pending_commits == 1) &&
    ECInject::test_write_error2(op->hoid)) {
    std::string cmd =
      "{ \"prefix\": \"osd down\", \"ids\": [\"" + std::to_string(
        get_parent()->whoami()) + "\"] }";
    vector<std::string> vcmd{cmd};
    dout(0) << __func__ << " Error inject - marking OSD down" << dendl;
    get_parent()->start_mon_command(vcmd, {}, nullptr, nullptr, nullptr);
  }

  if (op->pending_commits == 0) {
    rmw_pipeline.try_finish_rmw();
  }
}

void ECBackend::handle_sub_read_reply(
    pg_shard_t from,
    ECSubReadReply &op,
    const ZTracer::Trace &trace) {
  trace.event("ec sub read reply");
  dout(10) << __func__ << ": reply " << op << dendl;
  map<ceph_tid_t, ReadOp>::iterator iter = read_pipeline.tid_to_read_map.
                                                         find(op.tid);
  if (iter == read_pipeline.tid_to_read_map.end()) {
    //canceled
    dout(20) << __func__ << ": dropped " << op << dendl;
    return;
  }
  ReadOp &rop = iter->second;
  if (cct->_conf->bluestore_debug_inject_read_err) {
    for (auto i = op.buffers_read.begin();
         i != op.buffers_read.end();
         ++i) {
      if (ECInject::test_read_error0(
        ghobject_t(i->first, ghobject_t::NO_GEN, op.from.shard))) {
        dout(0) << __func__ << " Error inject - EIO error for shard " << op.from
                                                                           .shard
 << dendl;
        op.buffers_read.erase(i->first);
        op.attrs_read.erase(i->first);
        op.errors[i->first] = -EIO;
        rop.debug_log.emplace_back(ECUtil::INJECT_EIO, op.from);
      }
    }
  }
  for (auto &&[hoid, offset_buffer_map]: op.buffers_read) {
    ceph_assert(!op.errors.contains(hoid));
    // If attribute error we better not have sent a buffer
    if (!rop.to_read.contains(hoid)) {
      rop.debug_log.emplace_back(ECUtil::CANCELLED, op.from);

      // We canceled this read! @see filter_read_op
      dout(20) << __func__ << " to_read skipping" << dendl;
      continue;
    }

    if (!rop.complete.contains(hoid)) {
      rop.complete.emplace(hoid, &sinfo);
    }

    auto &buffers_read = rop.complete.at(hoid).buffers_read;
    for (auto &&[offset, buffer_list]: offset_buffer_map) {
      buffers_read.insert_in_shard(from.shard, offset, buffer_list);
    }
    rop.debug_log.emplace_back(ECUtil::READ_DONE, op.from, buffers_read);
  }
  for (auto &&[hoid, req]: rop.to_read) {
    if (!rop.complete.contains(hoid)) {
      rop.complete.emplace(hoid, &sinfo);
    }
    auto &complete = rop.complete.at(hoid);
    if (!req.shard_reads.contains(from.shard)) {
      continue;
    }
    const shard_read_t &read = req.shard_reads.at(from.shard);
    if (!complete.errors.contains(from)) {
      dout(20) << __func__ <<" read:" << read << dendl;
      complete.processed_read_requests[from.shard].union_of(read.extents);
    }
  }
  for (auto &&[hoid, attr]: op.attrs_read) {
    ceph_assert(!op.errors.count(hoid));
    // if read error better not have sent an attribute
    if (!rop.to_read.contains(hoid)) {
      // We canceled this read! @see filter_read_op
      dout(20) << __func__ << " to_read skipping" << dendl;
      continue;
    }
    if (!rop.complete.contains(hoid)) {
      rop.complete.emplace(hoid, &sinfo);
    }
    rop.complete.at(hoid).attrs.emplace();
    (*(rop.complete.at(hoid).attrs)).swap(attr);
  }
  for (auto &&[hoid, err]: op.errors) {
    if (!rop.complete.contains(hoid)) {
      rop.complete.emplace(hoid, &sinfo);
    }
    auto &complete = rop.complete.at(hoid);
    complete.errors.emplace(from, err);
    rop.debug_log.emplace_back(ECUtil::ERROR, op.from, complete.buffers_read);
    complete.buffers_read.erase_shard(from.shard);
    complete.processed_read_requests.erase(from.shard);
    dout(20) << __func__ << " shard=" << from << " error=" << err << dendl;
  }

  map<pg_shard_t, set<ceph_tid_t>>::iterator siter =
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
    for (auto &&[oid, read_result]: rop.complete) {
      shard_id_set have;
      read_result.processed_read_requests.populate_shard_id_set(have);
      shard_id_set dummy_minimum;
      shard_id_set want_to_read;
      rop.to_read.at(oid).shard_want_to_read.
          populate_shard_id_set(want_to_read);

      dout(20) << __func__ << " read_result: " << read_result << dendl;

      int err = ec_impl->minimum_to_decode(want_to_read, have, dummy_minimum,
                                            nullptr);
      if (err) {
        dout(20) << __func__ << " minimum_to_decode failed" << dendl;
        if (rop.in_progress.empty()) {
          // If we don't have enough copies, try other pg_shard_ts if available.
          // During recovery there may be multiple osds with copies of the same shard,
          // so getting EIO from one may result in multiple passes through this code path.
          if (!rop.do_redundant_reads) {
            rop.debug_log.emplace_back(ECUtil::REQUEST_MISSING, op.from);
            int r = read_pipeline.send_all_remaining_reads(oid, rop);
            if (r == 0) {
              // We found that new reads are required to do a decode.
              need_resend = true;
              continue;
            } else if (r > 0) {
              // No new reads were requested. This means that some parity
              // shards can be assumed to be zeros.
              err = 0;
            }
            // else insufficient shards are available, keep the errors.
          }
          // Couldn't read any additional shards so handle as completed with errors
          // We don't want to confuse clients / RBD with objectstore error
          // values in particular ENOENT.  We may have different error returns
          // from different shards, so we'll return minimum_to_decode() error
          // (usually EIO) to reader.  It is likely an error here is due to a
          // damaged pg.
          rop.complete.at(oid).r = err;
          ++is_complete;
        }
      }

      if (!err) {
        ceph_assert(rop.complete.at(oid).r == 0);
        if (!rop.complete.at(oid).errors.empty()) {
          if (cct->_conf->osd_read_ec_check_for_errors) {
            rop.debug_log.emplace_back(ECUtil::COMPLETE_ERROR, op.from);
            dout(10) << __func__ << ": Not ignoring errors, use one shard" << dendl;
            err = rop.complete.at(oid).errors.begin()->second;
            rop.complete.at(oid).r = err;
          } else {
            get_parent()->clog_warn() << "Error(s) ignored for "
              << iter->first << " enough copies available";
            dout(10) << __func__ << " Error(s) ignored for " << iter->first
		     << " enough copies available" << dendl;
            rop.debug_log.emplace_back(ECUtil::ERROR_CLEAR, op.from);
            rop.complete.at(oid).errors.clear();
          }
        }
        // avoid re-read for completed object as we may send remaining reads for
        // uncompleted objects
        rop.to_read.at(oid).shard_reads.clear();
        rop.to_read.at(oid).want_attrs = false;
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
    rop.debug_log.emplace_back(ECUtil::COMPLETE, op.from);
    read_pipeline.complete_read_op(std::move(rop));
  } else {
    dout(10) << __func__ << " readop not complete: " << rop << dendl;
  }
}

void ECBackend::check_recovery_sources(const OSDMapRef &osdmap) {
  struct FinishReadOp : public GenContext<ThreadPool::TPHandle&> {
    ECCommon::ReadPipeline &read_pipeline;
    ceph_tid_t tid;

    FinishReadOp(ECCommon::ReadPipeline &read_pipeline, ceph_tid_t tid)
      : read_pipeline(read_pipeline), tid(tid) {}

    void finish(ThreadPool::TPHandle &) override {
      auto ropiter = read_pipeline.tid_to_read_map.find(tid);
      ceph_assert(ropiter != read_pipeline.tid_to_read_map.end());
      read_pipeline.complete_read_op(std::move(ropiter->second));
    }
  };
  read_pipeline.check_recovery_sources(
    osdmap,
    [this](const hobject_t &obj) {
      recovery_backend.recovery_ops.erase(obj);
    },
    [this](const ReadOp &op) {
      get_parent()->schedule_recovery_work(
        get_parent()->bless_unlocked_gencontext(
          new FinishReadOp(read_pipeline, op.tid)),
        1);
    });
}

void ECBackend::on_change() {
  rmw_pipeline.on_change();
  read_pipeline.on_change();
  rmw_pipeline.on_change2();
  clear_recovery_state();
}

void ECBackend::clear_recovery_state() {
  recovery_backend.recovery_ops.clear();
}

void ECBackend::dump_recovery_info(Formatter *f) const {
  f->open_array_section("recovery_ops");
  for (map<hobject_t, RecoveryBackend::RecoveryOp>::const_iterator i =
         recovery_backend.recovery_ops.begin();
       i != recovery_backend.recovery_ops.end();
       ++i) {
    f->open_object_section("op");
    i->second.dump(f);
    f->close_section();
  }
  f->close_section();
  f->open_array_section("read_ops");
  for (map<ceph_tid_t, ReadOp>::const_iterator i = read_pipeline.tid_to_read_map
                                                                .begin();
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
    ceph::ErasureCodeInterfaceRef &ec_impl,
    pg_t pgid,
    const ECUtil::stripe_info_t &sinfo,
    map<hobject_t, ECUtil::shard_extent_map_t> *written,
    shard_id_map<ObjectStore::Transaction> *transactions,
    DoutPrefixProvider *dpp,
    const OSDMapRef &osdmap) final {
    assert(t);
    ECTransaction::generate_transactions(
      t.get(),
      plan,
      ec_impl,
      pgid,
      sinfo,
      remote_shard_extent_map,
      log_entries,
      written,
      transactions,
      &temp_added,
      &temp_cleared,
      dpp,
      osdmap);
  }

  bool skip_transaction(
      std::set<shard_id_t> &pending_roll_forward,
      shard_id_t shard,
      ceph::os::Transaction &transaction) final {
    if (transaction.empty()) {
      return true;
    }
    pending_roll_forward.insert(shard);
    return false;
  }
};

std::tuple<
  int,
  map<string, bufferlist, less<>>,
  size_t
> ECBackend::get_attrs_n_size_from_disk(const hobject_t &hoid) {
  struct stat st;
  if (int r = object_stat(hoid, &st); r < 0) {
    dout(10) << __func__ << ": stat error " << r << " on" << hoid << dendl;
    return {r, {}, 0};
  }
  map<string, bufferlist, less<>> real_attrs;
  if (int r = switcher->objects_get_attrs_with_hinfo(hoid, &real_attrs); r < 0) {
    dout(10) << __func__ << ": get attr error " << r << " on" << hoid << dendl;
    return {r, {}, 0};
  }
  return {0, real_attrs, st.st_size};
}

ECUtil::HashInfoRef ECBackend::get_hinfo_from_disk(hobject_t oid) {
  auto [r, attrs, size] = get_attrs_n_size_from_disk(oid);
  ceph_assert(r >= 0 || r == -ENOENT);
  ECUtil::HashInfoRef hinfo = unstable_hashinfo_registry.get_hash_info(
    oid, true, attrs, size);
  return hinfo;
}

std::optional<object_info_t> ECBackend::get_object_info_from_obc(
    ObjectContextRef &obc) {
  std::optional<object_info_t> ret;

  auto attr_cache = obc->attr_cache;
  if (!attr_cache.contains(OI_ATTR))
    return ret;

  ret.emplace(attr_cache.at(OI_ATTR));
  return ret;
}

void ECBackend::submit_transaction(
  const hobject_t &hoid,
  const object_stat_sum_t &delta_stats,
  const eversion_t &at_version,
  PGTransactionUPtr &&t,
  const eversion_t &trim_to,
  const eversion_t &pg_committed_to,
  vector<pg_log_entry_t> &&log_entries,
  std::optional<pg_hit_set_history_t> &hset_history,
  Context *on_all_commit,
  ceph_tid_t tid,
  osd_reqid_t reqid,
  OpRequestRef client_op
) {
  auto op = std::make_shared<ECClassicalOp>();
  auto obc_map = t->obc_map;
  op->t = std::move(t);
  op->hoid = hoid;
  op->delta_stats = delta_stats;
  op->version = at_version;
  op->trim_to = trim_to;
  /* We update PeeringState::pg_committed_to via the callback
   * invoked from ECBackend::handle_sub_write_reply immediately
   * before updating rmw_pipeline.commited_to via
   * rmw_pipeline.check_ops()->finish_rmw(), so these will
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
  op->pipeline = &rmw_pipeline;
  if (client_op) {
    op->trace = client_op->pg_trace;
  }
  ECTransaction::WritePlan &plans = op->plan;

  op->t->safe_create_traverse(
    [&](std::pair<const hobject_t, PGTransaction::ObjectOperation> &i) {
      const auto &[oid, inner_op] = i;
      ECUtil::HashInfoRef shinfo;
      auto &obc = obc_map.at(oid);
      object_info_t oi = obc->obs.oi;
      std::optional<object_info_t> soi;
      ECUtil::HashInfoRef hinfo;

      if (!sinfo.supports_ec_overwrites()) {
        hinfo = get_hinfo_from_disk(oid);
      }

      hobject_t source;
      if (inner_op.has_source(&source)) {
        if (!sinfo.supports_ec_overwrites()) {
          shinfo = get_hinfo_from_disk(source);
        }
        if (!inner_op.is_rename()) {
          soi = get_object_info_from_obc(obc_map.at(source));
        }
      }

      uint64_t old_object_size = 0;
      bool object_in_cache = false;
      if (rmw_pipeline.extent_cache.contains_object(oid)) {
        /* We have a valid extent cache for this object. If we need to read, we
         * need to behave as if the object is already the size projected by the
         * extent cache, or we may not read enough data.
         */
        old_object_size = rmw_pipeline.extent_cache.get_projected_size(oid);
        object_in_cache = true;
      } else {
        std::optional<object_info_t> old_oi = get_object_info_from_obc(obc);
        if (old_oi && !inner_op.delete_first) {
          old_object_size = old_oi->size;
        }
      }

      auto [readable_shards, writable_shards] =
        read_pipeline.get_readable_writable_shard_id_sets();
      ECTransaction::WritePlanObj plan(oid, inner_op, sinfo, readable_shards,
                                       writable_shards,
                                       object_in_cache, old_object_size,
                                       oi, soi, std::move(hinfo),
                                       std::move(shinfo),
                                       rmw_pipeline.ec_pdw_write_mode);

      if (plan.to_read) plans.want_read = true;
      plans.plans.emplace_back(std::move(plan));
    });
  ldpp_dout(get_parent()->get_dpp(), 20) << __func__
             << " plans=" << plans
             << dendl;
  rmw_pipeline.start_rmw(std::move(op));
}

int ECBackend::objects_read_sync(
    const hobject_t &hoid,
    uint64_t off,
    uint64_t len,
    uint32_t op_flags,
    bufferlist *bl) {
  return -EOPNOTSUPP;
}

void ECBackend::objects_read_async(
    const hobject_t &hoid,
    uint64_t object_size,
    const list<pair<ec_align_t,
                    pair<bufferlist*, Context*>>> &to_read,
    Context *on_complete,
    bool fast_read) {
  map<hobject_t, std::list<ec_align_t>> reads;

  uint32_t flags = 0;
  extent_set es;
  for (const auto &[read, ctx]: to_read) {
    pair<uint64_t, uint64_t> tmp;
    if (!cct->_conf->osd_ec_partial_reads) {
      tmp = sinfo.ro_offset_len_to_stripe_ro_offset_len(read.offset, read.size);
    } else {
      tmp.first = read.offset;
      tmp.second = read.size;
    }
    es.union_insert(tmp.first, tmp.second);
    flags |= read.flags;
  }

  if (!es.empty()) {
    auto &offsets = reads[hoid];
    for (auto [off, len]: es) {
      offsets.emplace_back(ec_align_t{off, len, flags});
    }
  }

  struct cb {
    ECBackend *ec;
    hobject_t hoid;
    list<pair<ec_align_t,
              pair<bufferlist*, Context*>>> to_read;
    unique_ptr<Context> on_complete;
    cb(const cb &) = delete;
    cb(cb &&) = default;

    cb(ECBackend *ec,
       const hobject_t &hoid,
       const list<pair<ec_align_t,
                       pair<bufferlist*, Context*>>> &to_read,
       Context *on_complete)
      : ec(ec),
        hoid(hoid),
        to_read(to_read),
        on_complete(on_complete) {}

    void operator()(ECCommon::ec_extents_t &&results) {
      auto dpp = ec->get_parent()->get_dpp();
      ldpp_dout(dpp, 20) << "objects_read_async_cb: got: " << results
			 << dendl;

      auto &got = results.at(hoid);

      int r = 0;
      for (auto &&[read, result]: to_read) {
        auto &&[bufs, ctx] = result;
        if (got.err < 0) {
          // error handling
          if (ctx) {
            ctx->complete(got.err);
          }
          if (r == 0)
            r = got.err;
        } else {
          ceph_assert(bufs);
          uint64_t offset = read.offset;
          uint64_t length = read.size;
          auto range = got.emap.get_containing_range(offset, length);
          uint64_t range_offset = range.first.get_off();
          uint64_t range_length = range.first.get_len();
          ceph_assert(range.first != range.second);
          ceph_assert(range_offset <= offset);
          ldpp_dout(dpp, 20) << "offset: " << offset << dendl;
          ldpp_dout(dpp, 20) << "range offset: " << range_offset << dendl;
          ldpp_dout(dpp, 20) << "length: " << length << dendl;
          ldpp_dout(dpp, 20) << "range length: " << range_length << dendl;
          ceph_assert((offset + length) <= (range_offset + range_length));
          bufs->substr_of(
            range.first.get_val(),
            offset - range_offset,
            length);
          if (ctx) {
            ctx->complete(length);
            ctx = nullptr;
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
    object_size,
    make_gen_lambda_context<
      ECCommon::ec_extents_t&&, cb>(
      cb(this,
         hoid,
         to_read,
         on_complete)));
}

void ECBackend::objects_read_and_reconstruct(
  const map<hobject_t, std::list<ec_align_t>> &reads,
  bool fast_read,
  uint64_t object_size,
  GenContextURef<ECCommon::ec_extents_t&&> &&func) {
  return read_pipeline.objects_read_and_reconstruct(
    reads, fast_read, object_size, std::move(func));
}

void ECBackend::objects_read_and_reconstruct_for_rmw(
  map<hobject_t, read_request_t> &&to_read,
  GenContextURef<ECCommon::ec_extents_t&&> &&func) {
  return read_pipeline.objects_read_and_reconstruct_for_rmw(
    std::move(to_read), std::move(func));
}

void ECBackend::kick_reads() {
  read_pipeline.kick_reads();
}

int ECBackend::object_stat(
  const hobject_t &hoid,
  struct stat *st) {
  int r = switcher->store->stat(
    switcher->ch,
    ghobject_t{hoid, ghobject_t::NO_GEN, get_parent()->whoami_shard().shard},
    st);
  return r;
}

int ECBackend::objects_get_attrs(
  const hobject_t &hoid,
  map<string, bufferlist, less<>> *out) {
  for (map<string, bufferlist>::iterator i = out->begin();
       i != out->end();
  ) {
    if (ECUtil::is_hinfo_key_string(i->first))
      out->erase(i++);
    else
      ++i;
  }
  return 0;
}

int ECBackend::be_deep_scrub(
  const Scrub::ScrubCounterSet& io_counters,
  const hobject_t &poid,
  ScrubMap &map,
  ScrubMapBuilder &pos,
  ScrubMap::object &o) {
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

  auto& perf_logger = *(get_parent()->get_logger());
  perf_logger.inc(io_counters.read_cnt);
  bufferlist bl;
  r = switcher->store->read(
    switcher->ch,
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
  if (r > 0) {
    pos.data_hash << bl;
  }
  perf_logger.inc(io_counters.read_bytes, r);
  pos.data_pos += r;
  if (r == (int)stride) {
    return -EINPROGRESS;
  }

  if (!sinfo.get_is_hinfo_required()) {
    o.digest = 0;
    o.digest_present = true;
    o.omap_digest = -1;
    o.omap_digest_present = true;
    return 0;
  }

  ECUtil::HashInfoRef hinfo = unstable_hashinfo_registry.get_hash_info(
    poid, false, o.attrs, o.size);
  if (!hinfo) {
    dout(0) << "_scan_list  " << poid << " could not retrieve hash info" << dendl;
    o.read_error = true;
    o.digest_present = false;
    return 0;
  }
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
  o.digest = hinfo->get_chunk_hash(shard_id_t(0));
  o.digest_present = true;
  o.omap_digest = -1;
  o.omap_digest_present = true;
  return 0;
}
