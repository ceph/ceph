// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

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

#include "ECCommon.h"

#include <iostream>
#include <sstream>
#include <ranges>
#include <fmt/ostream.h>

#include "ECInject.h"
#include "messages/MOSDECSubOpWrite.h"
#include "messages/MOSDECSubOpRead.h"
#include "common/debug.h"
#include "ECMsgTypes.h"
#include "PGLog.h"
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

static ostream &_prefix(std::ostream *_dout,
                        ECCommon::RMWPipeline const *rmw_pipeline) {
  return rmw_pipeline->get_parent()->gen_dbg_prefix(*_dout);
}

static ostream &_prefix(std::ostream *_dout,
                        ECCommon::ReadPipeline const *read_pipeline) {
  return read_pipeline->get_parent()->gen_dbg_prefix(*_dout);
}

static ostream &_prefix(std::ostream *_dout,
                        ECCommon::RecoveryBackend *pgb) {
  return pgb->get_parent()->gen_dbg_prefix(*_dout);
}

static ostream &_prefix(std::ostream *_dout,
                        struct ClientReadCompleter const *read_completer
  );

void ECCommon::ReadOp::dump(Formatter *f) const {
  f->dump_unsigned("tid", tid);
#ifndef WITH_CRIMSON
  if (op && op->get_req()) {
    f->dump_stream("op") << *(op->get_req());
  }
#endif
  f->dump_stream("to_read") << to_read;
  f->dump_stream("complete") << complete;
  f->dump_int("priority", priority);
  f->dump_stream("obj_to_source") << obj_to_source;
  f->dump_stream("source_to_obj") << source_to_obj;
  f->dump_stream("in_progress") << in_progress;
}

void ECCommon::RecoveryBackend::RecoveryOp::dump(Formatter *f) const {
  f->dump_stream("hoid") << hoid;
  f->dump_stream("v") << v;
  f->dump_stream("missing_on") << missing_on;
  f->dump_stream("missing_on_shards") << missing_on_shards;
  f->dump_stream("recovery_info") << recovery_info;
  f->dump_stream("recovery_progress") << recovery_progress;
  f->dump_stream("state") << tostr(state);
  f->dump_stream("waiting_on_pushes") << waiting_on_pushes;
}


void ECCommon::ReadPipeline::complete_read_op(ReadOp &&rop) {
  dout(20) << __func__ << " completing " << rop << dendl;
  auto req_iter = rop.to_read.begin();
  auto resiter = rop.complete.begin();
  ceph_assert(rop.to_read.size() == rop.complete.size());
  for (; req_iter != rop.to_read.end(); ++req_iter, ++resiter) {
    auto &hoid = req_iter->first;
    read_result_t &res = resiter->second;
    read_request_t &req = req_iter->second;
    rop.on_complete->finish_single_request(
      hoid, std::move(res), req);
  }
  ceph_assert(rop.on_complete);
  std::move(*rop.on_complete).finish(rop.priority);
  rop.on_complete = nullptr;

  // if the read op is over. clean all the data of this tid.
  for (auto &pg_shard: rop.in_progress) {
    shard_to_read_map[pg_shard].erase(rop.tid);
  }
  rop.in_progress.clear();
  tid_to_read_map.erase(rop.tid);
}

void ECCommon::ReadPipeline::on_change() {
  for (auto &rop: std::views::keys(tid_to_read_map)) {
    dout(10) << __func__ << ": cancelling " << rop << dendl;
  }
  tid_to_read_map.clear();
  shard_to_read_map.clear();
  in_progress_client_reads.clear();
}

std::pair<const shard_id_set, const shard_id_set>
ECCommon::ReadPipeline::get_readable_writable_shard_id_sets() {
  shard_id_set readable;
  shard_id_set writable;

  for (auto &&pg_shard: get_parent()->get_acting_shards()) {
    readable.insert(pg_shard.shard);
  }

  writable = get_parent()->get_acting_recovery_backfill_shard_id_set();
  return std::make_pair(std::move(readable), std::move(writable));
}

void ECCommon::ReadPipeline::get_all_avail_shards(
    const hobject_t &hoid,
    shard_id_set &have,
    shard_id_map<pg_shard_t> &shards,
    const bool for_recovery,
    const std::optional<set<pg_shard_t>> &error_shards) {
  for (auto &&pg_shard: get_parent()->get_acting_shards()) {
    dout(10) << __func__ << ": checking acting " << pg_shard << dendl;
    const pg_missing_t &missing = get_parent()->get_shard_missing(pg_shard);
    if (error_shards && error_shards->contains(pg_shard)) {
      continue;
    }
    const shard_id_t &shard = pg_shard.shard;
    if (cct->_conf->bluestore_debug_inject_read_err &&
      ECInject::test_read_error1(ghobject_t(hoid, ghobject_t::NO_GEN, shard))) {
      dout(0) << __func__ << " Error inject - Missing shard " << shard << dendl;
      continue;
    }
    if (!missing.is_missing(hoid)) {
      ceph_assert(!have.contains(shard));
      have.insert(shard);
      ceph_assert(!shards.contains(shard));
      shards.insert(shard, pg_shard);
    }
  }

  if (for_recovery) {
    for (auto &&pg_shard: get_parent()->get_backfill_shards()) {
      if (error_shards && error_shards->contains(pg_shard)) {
        continue;
      }
      const shard_id_t &shard = pg_shard.shard;
      if (have.contains(shard)) {
        ceph_assert(shards.contains(shard));
        continue;
      }
      dout(10) << __func__ << ": checking backfill " << pg_shard << dendl;
      ceph_assert(!shards.count(shard));
      const pg_info_t &info = get_parent()->get_shard_info(pg_shard);
      if (hoid < info.last_backfill &&
        !get_parent()->get_shard_missing(pg_shard).is_missing(hoid)) {
        have.insert(shard);
        shards.insert(shard, pg_shard);
      }
    }

    auto miter = get_parent()->get_missing_loc_shards().find(hoid);
    if (miter != get_parent()->get_missing_loc_shards().end()) {
      for (auto &&pg_shard: miter->second) {
        dout(10) << __func__ << ": checking missing_loc " << pg_shard << dendl;
        if (const auto m = get_parent()->maybe_get_shard_missing(pg_shard)) {
          ceph_assert(!m->is_missing(hoid));
        }
        if (error_shards && error_shards->contains(pg_shard)) {
          continue;
        }
        have.insert(pg_shard.shard);
        shards.insert(pg_shard.shard, pg_shard);
      }
    }
  }
}

int ECCommon::ReadPipeline::get_min_avail_to_read_shards(
    const hobject_t &hoid,
    bool for_recovery,
    bool do_redundant_reads,
    read_request_t &read_request,
    const std::optional<set<pg_shard_t>> &error_shards) {
  // Make sure we don't do redundant reads for recovery
  ceph_assert(!for_recovery || !do_redundant_reads);

  if (read_request.object_size == 0) {
    dout(10) << __func__ << " empty read" << dendl;
    return 0;
  }

  shard_id_set have;
  shard_id_map<pg_shard_t> shards(sinfo.get_k_plus_m());

  get_all_avail_shards(hoid, have, shards, for_recovery, error_shards);

  std::unique_ptr<shard_id_map<vector<pair<int, int>>>> need_sub_chunks =
      nullptr;
  if (sinfo.supports_sub_chunks()) {
    need_sub_chunks = std::make_unique<shard_id_map<vector<pair<int, int>>>>(
      sinfo.get_k_plus_m());
  }
  shard_id_set need_set;
  shard_id_set want;

  read_request.shard_want_to_read.populate_shard_id_set(want);

  int r = 0;
  auto kth_iter = want.find_nth(sinfo.get_k());
  if (kth_iter != want.end()) {
    // If we support partial reads, we are making the assumption that only
    // K shards need to be read to recover data.  We opt here for minimising
    // the number of reads over minimising the amount of parity calculations
    // that are needed.
    shard_id_set want_for_plugin = want;
    shard_id_t kth = *kth_iter;
    want_for_plugin.erase_range(kth, sinfo.get_k_plus_m() - (int)kth);
    r = ec_impl->minimum_to_decode(want_for_plugin, have, need_set,
                                     need_sub_chunks.get());
  } else {
    r = ec_impl->minimum_to_decode(want, have, need_set,
                                     need_sub_chunks.get());
  }

  if (r < 0) {
    dout(20) << "minimum_to_decode_failed r: " << r << "want: " << want
      << " have: " << have << " need: " << need_set << dendl;
    return r;
  }

  if (do_redundant_reads) {
    if (need_sub_chunks) {
      vector<pair<int, int>> subchunks_list;
      subchunks_list.push_back(make_pair(0, ec_impl->get_sub_chunk_count()));
      for (auto &&i: have) {
        (*need_sub_chunks)[i] = subchunks_list;
      }
    }
    need_set.insert(have);
  }

  extent_set extra_extents;
  ECUtil::shard_extent_set_t read_mask(sinfo.get_k_plus_m());
  ECUtil::shard_extent_set_t zero_mask(sinfo.get_k_plus_m());

  sinfo.ro_size_to_read_mask(read_request.object_size, read_mask);
  sinfo.ro_size_to_zero_mask(read_request.object_size, zero_mask);

  /* First deal with missing shards */
  for (auto &&[shard, extent_set]: read_request.shard_want_to_read) {
    /* Work out what extra extents we need to read on each shard. If do
     * redundant reads is set, then we want to have the same reads on
     * every extent. Otherwise, we need to read every shard only if the
     * necessary shard is missing.
     * In some (recovery) scenarios, we can "want" a shard, but not "need" to
     * read it.  This typically happens when we do not have a data shard and the
     * recovery for this will read enough shards to also generate all the parity.
     *
     * Since parity shards are often larger than data shards, we must make sure
     * to read the extra bit!
     */
    if (!have.contains(shard) || do_redundant_reads ||
        (want.contains(shard) && !need_set.contains(shard))) {
      extra_extents.union_of(extent_set);
    }
  }

  read_request.zeros_for_decode.clear();
  for (auto &shard: need_set) {
    if (!have.contains(shard)) {
      continue;
    }
    shard_id_t shard_id(shard);
    extent_set extents = extra_extents;
    shard_read_t shard_read;
    if (need_sub_chunks) {
      shard_read.subchunk = need_sub_chunks->at(shard_id);
    }
    shard_read.pg_shard = shards[shard_id];

    if (read_request.shard_want_to_read.contains(shard)) {
      extents.union_of(read_request.shard_want_to_read.at(shard));
    }

    extents.align(EC_ALIGN_SIZE);
    if (read_mask.contains(shard)) {
      shard_read.extents.intersection_of(extents, read_mask.at(shard));
    }

    if (zero_mask.contains(shard)) {
      extents.intersection_of(zero_mask.at(shard));

      /* Any remaining extents can be assumed ot be zeros... so record these. */
      if (!extents.empty()) {
        read_request.zeros_for_decode.emplace(shard, std::move(extents));
      }
    }

    if (!shard_read.extents.empty()) {
      read_request.shard_reads[shard_id] = std::move(shard_read);
    }
  }

  dout(20) << __func__ << " for_recovery: " << for_recovery
    << " do_redundant_reads: " << do_redundant_reads
    << " read_request: " << read_request
    << " error_shards: " << error_shards
    << dendl;
  return 0;
}


void ECCommon::ReadPipeline::get_min_want_to_read_shards(
    const ec_align_t &to_read,
    ECUtil::shard_extent_set_t &want_shard_reads) {
  sinfo.ro_range_to_shard_extent_set(to_read.offset, to_read.size,
                                     want_shard_reads);
  dout(20) << __func__ << ": to_read " << to_read
	   << " read_request " << want_shard_reads << dendl;
}

int ECCommon::ReadPipeline::get_remaining_shards(
    const hobject_t &hoid,
    read_result_t &read_result,
    read_request_t &read_request,
    const bool for_recovery,
    bool want_attrs) {
  set<pg_shard_t> error_shards;
  for (auto &shard: std::views::keys(read_result.errors)) {
    error_shards.insert(shard);
  }

  /* fast-reads should already have scheduled reads to everything, so
   * this function is irrelevant. */
  const int r = get_min_avail_to_read_shards(
    hoid,
    for_recovery,
    false,
    read_request,
    error_shards);

  if (r) {
    dout(0) << __func__ << " not enough shards left to try for " << hoid
	    << " read result was " << read_result << dendl;
    return -EIO;
  }

  bool need_attr_request = want_attrs;
  read_request.want_attrs = want_attrs;

  // Rather than repeating whole read, we can remove everything we already have.
  for (auto iter = read_request.shard_reads.begin();
       iter != read_request.shard_reads.end();) {
    auto &&[shard_id, shard_read] = *iter;
    bool do_erase = false;

    // Ignore where shard has not been read at all.
    if (read_result.processed_read_requests.contains(shard_id)) {
      shard_read.extents.subtract(
        read_result.processed_read_requests.at(shard_id));
      do_erase = shard_read.extents.empty();
    }

    if (do_erase) {
      iter = read_request.shard_reads.erase(iter);
    } else {
      if (need_attr_request && !sinfo.is_nonprimary_shard(shard_id)) {
        // We have a suitable candidate for attr requests.
        need_attr_request = false;
      }
      ++iter;
    }
  }

  if (need_attr_request) {
    // This happens if we got an error from the shard where we were requesting
    // the attributes from and the recovery does not require any non primary
    // shards. The example seen in test was a 2+1 EC being recovered. shards 0
    // and 2 were being requested and read as part of recovery. Shard was reading
    // the attributes and failed. The recovery required shard 1, but that does
    // not have valid attributes on it, so the attribute read failed.
    // This is a pretty obscure case, so no need to optimise that much. Do an
    // empty read!
    shard_id_set have;
    shard_id_map<pg_shard_t> pg_shards(sinfo.get_k_plus_m());
    get_all_avail_shards(hoid, have, pg_shards, for_recovery, error_shards);
    for (auto shard : have) {
      if (!sinfo.is_nonprimary_shard(shard)) {
        shard_read_t shard_read;
        shard_read.pg_shard = pg_shards[shard];
        read_request.shard_reads.insert(shard, shard_read_t());
        break;
      }
    }
  }

  return 0;
}

void ECCommon::ReadPipeline::start_read_op(
    const int priority,
    map<hobject_t, read_request_t> &to_read,
    const bool do_redundant_reads,
    const bool for_recovery,
    std::unique_ptr<ReadCompleter> on_complete) {
  ceph_tid_t tid = get_parent()->get_tid();
  ceph_assert(!tid_to_read_map.contains(tid));
  auto &op = tid_to_read_map.emplace(
    tid,
    ReadOp(
      priority,
      tid,
      do_redundant_reads,
      for_recovery,
      std::move(on_complete),
      std::move(to_read))).first->second;
  dout(10) << __func__ << ": starting " << op << dendl;
  if (op.op) {
#ifndef WITH_CRIMSON
    op.trace = op.op->pg_trace;
#endif
    op.trace.event("start ec read");
  }
  do_read_op(op);
}

void ECCommon::ReadPipeline::do_read_op(ReadOp &rop) {
  const int priority = rop.priority;
  const ceph_tid_t tid = rop.tid;
  bool reads_sent = false;

  dout(10) << __func__ << ": starting read " << rop << dendl;
  ceph_assert(!rop.to_read.empty());

  map<pg_shard_t, ECSubRead> messages;
  for (auto &&[hoid, read_request]: rop.to_read) {
    bool need_attrs = read_request.want_attrs;

    for (auto &&[shard, shard_read]: read_request.shard_reads) {
      if (need_attrs && !sinfo.is_nonprimary_shard(shard)) {
        messages[shard_read.pg_shard].attrs_to_read.insert(hoid);
        need_attrs = false;
      }
      if (shard_read.subchunk) {
        messages[shard_read.pg_shard].subchunks[hoid] = *shard_read.subchunk;
      } else {
        static const std::vector default_sub_chunk = {make_pair(0, 1)};
        messages[shard_read.pg_shard].subchunks[hoid] = default_sub_chunk;
      }
      rop.obj_to_source[hoid].insert(shard_read.pg_shard);
      rop.source_to_obj[shard_read.pg_shard].insert(hoid);
    }
    for (auto &[_, shard_read]: read_request.shard_reads) {
      ceph_assert(!shard_read.extents.empty());
      rop.debug_log.emplace_back(ECUtil::READ_REQUEST, shard_read.pg_shard,
                                   shard_read.extents);
      for (auto &[start, len]: shard_read.extents) {
        messages[shard_read.pg_shard].to_read[hoid].emplace_back(
          boost::make_tuple(start, len, read_request.flags));
        reads_sent = true;
      }
    }
    ceph_assert(!need_attrs);
    ceph_assert(reads_sent);
  }

  std::optional<ECSubRead> local_read_op;
  std::vector<std::pair<int, Message*>> m;
  m.reserve(messages.size());
  for (auto &&[pg_shard, read]: messages) {
    rop.in_progress.insert(pg_shard);
    shard_to_read_map[pg_shard].insert(rop.tid);
    read.tid = tid;
#ifdef WITH_CRIMSON // crimson only
    if (pg_shard == get_parent()->whoami_shard()) {
      local_read_op = std::move(read);
      continue;
    }
#endif
    auto *msg = new MOSDECSubOpRead;
    msg->set_priority(priority);
    msg->pgid = spg_t(get_info().pgid.pgid, pg_shard.shard);
    msg->map_epoch = get_osdmap_epoch();
    msg->min_epoch = get_parent()->get_interval_start_epoch();
    msg->op = read;
    msg->op.from = get_parent()->whoami_shard();
    msg->op.tid = tid;
    if (rop.trace) {
      // initialize a child span for this shard
      msg->trace.init("ec sub read", nullptr, &rop.trace);
      msg->trace.keyval("shard", pg_shard.shard.id);
    }
    m.push_back(std::make_pair(pg_shard.osd, msg));
    dout(10) << __func__ << ": will send msg " << *msg
             << " to osd." << pg_shard << dendl;
  }
  if (!m.empty()) {
    get_parent()->send_message_osd_cluster(m, get_osdmap_epoch());
  }

#if WITH_CRIMSON
  if (local_read_op) {
    dout(10) << __func__ << ": doing local read for " << rop << dendl;
    handle_sub_read_n_reply(
      get_parent()->whoami_shard(),
      *local_read_op,
      rop.trace);
  }
#endif
  dout(10) << __func__ << ": started " << rop << dendl;
}

void ECCommon::ReadPipeline::get_want_to_read_shards(
    const list<ec_align_t> &to_read,
    ECUtil::shard_extent_set_t &want_shard_reads) {
  if (sinfo.supports_partial_reads()) {
    // Optimised.
    for (const auto &single_region: to_read) {
      get_min_want_to_read_shards(single_region, want_shard_reads);
    }
    return;
  }

  // Non-optimised version.
  for (const shard_id_t shard: sinfo.get_data_shards()) {
    for (auto &&read: to_read) {
      auto &&[offset, len] = sinfo.chunk_aligned_ro_range_to_shard_ro_range(
        read.offset, read.size);
      want_shard_reads[shard].union_insert(offset, len);
    }
  }
}

void ECCommon::ReadPipeline::get_want_to_read_all_shards(
    const list<ec_align_t> &to_read,
    ECUtil::shard_extent_set_t &want_shard_reads)
{
  for (const auto &single_region: to_read) {
    sinfo.ro_range_to_shard_extent_set_with_parity(single_region.offset,
                                                   single_region.size,
                                                   want_shard_reads);
  }
  dout(20) << __func__ << ": to_read " << to_read
  << " read_request " << want_shard_reads << dendl;
}

/**
 * Create a buffer containing both the ordered data and parity shards
 *
 * @param buffers_read shard_extent_map_t of shard indexes and their corresponding extent maps
 * @param read ec_align_t Read size and offset for rados object
 * @param outbl Pointer to output buffer
 */
void ECCommon::ReadPipeline::create_parity_read_buffer(
  ECUtil::shard_extent_map_t buffers_read,
  ec_align_t read,
  bufferlist *outbl)
{
  bufferlist data, parity;
  data = buffers_read.get_ro_buffer(read.offset, read.size);

  for (raw_shard_id_t raw_shard(sinfo.get_k());
       raw_shard < sinfo.get_k_plus_m(); ++raw_shard) {
    shard_id_t shard = sinfo.get_shard(raw_shard);
    buffers_read.get_shard_first_buffer(shard, parity);
  }

  outbl->append(data);
  outbl->append(parity);
}

struct ClientReadCompleter final : ECCommon::ReadCompleter {
  ClientReadCompleter(ECCommon::ReadPipeline &read_pipeline,
                      ECCommon::ClientAsyncReadStatus *status
    )
    : read_pipeline(read_pipeline),
      status(status) {}

  void finish_single_request(
      const hobject_t &hoid,
      ECCommon::read_result_t &&res,
      ECCommon::read_request_t &req) override {
#ifndef WITH_CRIMSON
    auto *cct = read_pipeline.cct;
#endif
    dout(20) << __func__ << " completing hoid=" << hoid
             << " res=" << res << " req=" << req << dendl;
    extent_map result;
    if (res.r == 0) {
      ceph_assert(res.errors.empty());
      dout(30) << __func__ << ": before decode: "
               << res.buffers_read.debug_string(2048, 0)
               << dendl;
      /* Decode any missing buffers */
      res.buffers_read.add_zero_padding_for_decode(req.zeros_for_decode);
      int r = res.buffers_read.decode(read_pipeline.ec_impl,
                                  req.shard_want_to_read,
                                  req.object_size,
                                  read_pipeline.get_parent()->get_dpp());
      ceph_assert( r == 0 );
      dout(30) << __func__ << ": after decode: "
               << res.buffers_read.debug_string(2048, 0)
               << dendl;

      for (auto &&read: req.to_read) {
        // Return a buffer containing both data and parity
        // if the parity read inject is set
        if (cct->_conf->bluestore_debug_inject_read_err &&
            ECInject::test_parity_read(hoid)) {
          bufferlist data_and_parity;
          read_pipeline.create_parity_read_buffer(res.buffers_read, read, &data_and_parity);
          result.insert(read.offset, data_and_parity.length(), data_and_parity);
        } else {
          result.insert(read.offset, read.size,
                        res.buffers_read.get_ro_buffer(read.offset, read.size));
        }
      }
    }
    dout(20) << __func__ << " calling complete_object with result="
             << result << dendl;
    status->complete_object(hoid, res.r, std::move(result),
                            std::move(res.buffers_read));
    read_pipeline.kick_reads();
  }

  void finish(int priority) && override {
    // NOP
  }

  ECCommon::ReadPipeline &read_pipeline;
  ECCommon::ClientAsyncReadStatus *status;
};

static ostream &_prefix(std::ostream *_dout,
                        ClientReadCompleter const *read_completer) {
  return _prefix(_dout, &read_completer->read_pipeline);
}

void ECCommon::ReadPipeline::objects_read_and_reconstruct(
    const map<hobject_t, std::list<ec_align_t>> &reads,
    const bool fast_read,
    const uint64_t object_size,
    GenContextURef<ec_extents_t&&> &&func) {
  in_progress_client_reads.emplace_back(reads.size(), std::move(func));
  if (!reads.size()) {
    kick_reads();
    return;
  }

  map<hobject_t, read_request_t> for_read_op;
  for (auto &&[hoid, to_read]: reads) {
    ECUtil::shard_extent_set_t want_shard_reads(sinfo.get_k_plus_m());
    if (cct->_conf->bluestore_debug_inject_read_err &&
        ECInject::test_parity_read(hoid)) {
      get_want_to_read_all_shards(to_read, want_shard_reads);
    }
    else {
      get_want_to_read_shards(to_read, want_shard_reads);
    }

    read_request_t read_request(to_read, want_shard_reads, false, object_size);
    const int r = get_min_avail_to_read_shards(
      hoid,
      false,
      fast_read,
      read_request);
    ceph_assert(r == 0);

    const int subchunk_size =
        sinfo.get_chunk_size() / ec_impl->get_sub_chunk_count();
    dout(20) << __func__
             << " to_read=" << to_read
             << " subchunk_size=" << subchunk_size
             << " chunk_size=" << sinfo.get_chunk_size() << dendl;

    for_read_op.insert(make_pair(hoid, read_request));
  }

  start_read_op(
    CEPH_MSG_PRIO_DEFAULT,
    for_read_op,
    fast_read,
    false,
    std::make_unique<ClientReadCompleter>(
      *this, &(in_progress_client_reads.back())));
}

void ECCommon::ReadPipeline::objects_read_and_reconstruct_for_rmw(
    map<hobject_t, read_request_t> &&to_read,
    GenContextURef<ec_extents_t&&> &&func) {
  in_progress_client_reads.emplace_back(to_read.size(), std::move(func));
  if (!to_read.size()) {
    kick_reads();
    return;
  }

  map<hobject_t, read_request_t> for_read_op;
  for (auto &&[hoid, read_request]: to_read) {
    const int r =
        get_min_avail_to_read_shards(hoid, false, false, read_request);
    ceph_assert(r == 0);

    const int subchunk_size = sinfo.get_chunk_size() / ec_impl->
        get_sub_chunk_count();
    dout(20) << __func__
             << " read_request=" << read_request
             << " subchunk_size=" << subchunk_size
             << " chunk_size=" << sinfo.get_chunk_size() << dendl;

    for_read_op.insert(make_pair(hoid, read_request));
  }

  start_read_op(
    CEPH_MSG_PRIO_DEFAULT,
    for_read_op, false, false,
    std::make_unique<ClientReadCompleter>(
      *this, &(in_progress_client_reads.back())));
}


int ECCommon::ReadPipeline::send_all_remaining_reads(
    const hobject_t &hoid,
    ReadOp &rop) {
  // (Note cuixf) If we need to read attrs and we read failed, try to read again.
  const bool want_attrs =
      rop.to_read.at(hoid).want_attrs &&
      (!rop.complete.at(hoid).attrs || rop.complete.at(hoid).attrs->empty());
  if (want_attrs) {
    dout(10) << __func__ << " want attrs again" << dendl;
  }

  read_request_t &read_request = rop.to_read.at(hoid);
  // reset the old shard reads, we are going to read them again.
  read_request.shard_reads.clear();
  return get_remaining_shards(hoid, rop.complete.at(hoid), read_request,
                              rop.for_recovery, want_attrs);
}

void ECCommon::ReadPipeline::kick_reads() {
  while (in_progress_client_reads.size() &&
         in_progress_client_reads.front().is_complete()) {
         in_progress_client_reads.front().run();
         in_progress_client_reads.pop_front();
  }
}

bool ec_align_t::operator==(const ec_align_t &other) const {
  return offset == other.offset && size == other.size && flags == other.flags;
}

bool ECCommon::shard_read_t::operator==(const shard_read_t &other) const {
  return extents == other.extents &&
      subchunk == other.subchunk &&
      pg_shard == other.pg_shard;
}

bool ECCommon::read_request_t::operator==(const read_request_t &other) const {
  return to_read == other.to_read &&
      flags == other.flags &&
      shard_want_to_read == other.shard_want_to_read &&
      shard_reads == other.shard_reads &&
      want_attrs == other.want_attrs;
}

void ECCommon::RMWPipeline::start_rmw(OpRef op) {
  dout(20) << __func__ << " op=" << *op << dendl;

  ceph_assert(!tid_to_op_map.contains(op->tid));
  tid_to_op_map[op->tid] = op;

  op->pending_cache_ops = op->plan.plans.size();
  waiting_commit.push_back(op);

  for (auto &plan: op->plan.plans) {
    ECExtentCache::OpRef cache_op = extent_cache.prepare(plan.hoid,
      plan.to_read,
      plan.will_write,
      plan.orig_size,
      plan.projected_size,
      plan.invalidates_cache,
      [op](ECExtentCache::OpRef const &cop)
      {
        op->cache_ready(cop->get_hoid(), cop->get_result());
      });
    op->cache_ops.emplace_back(std::move(cache_op));
  }
  extent_cache.execute(op->cache_ops);
}

void ECCommon::RMWPipeline::cache_ready(Op &op) {
  get_parent()->apply_stats(
    op.hoid,
    op.delta_stats);

  shard_id_map<ObjectStore::Transaction> trans(sinfo.get_k_plus_m());
  for (auto &&shard: get_parent()->
       get_acting_recovery_backfill_shard_id_set()) {
    trans[shard];
  }

  op.trace.event("start ec write");

  map<hobject_t, ECUtil::shard_extent_map_t> written;
  op.generate_transactions(
    ec_impl,
    get_parent()->get_info().pgid.pgid,
    sinfo,
    &written,
    &trans,
    get_parent()->get_dpp(),
    get_osdmap());

  dout(20) << __func__ << ": written: " << written << ", op: " << op << dendl;

  ObjectStore::Transaction empty;
  bool should_write_local = false;
  ECSubWrite local_write_op;
  std::vector<std::pair<int, Message*>> messages;
  messages.reserve(get_parent()->get_acting_recovery_backfill_shards().size());
  set<pg_shard_t> backfill_shards = get_parent()->get_backfill_shards();

  if (op.version.version != 0) {
    if (oid_to_version.contains(op.hoid)) {
      ceph_assert(oid_to_version.at(op.hoid) <= op.version);
    }
    oid_to_version[op.hoid] = op.version;
  }
  for (auto &&pg_shard: get_parent()->get_acting_recovery_backfill_shards()) {
    ObjectStore::Transaction &transaction = trans.at(pg_shard.shard);
    shard_id_t shard = pg_shard.shard;
    if (transaction.empty()) {
      dout(20) << __func__ << " Transaction for osd." << pg_shard.osd << " shard " << shard << " is empty" << dendl;
    } else {
      // NOTE: All code between dout and dendl is executed conditionally on
      //       debug level.
      dout(20) << __func__ << " Transaction for osd." << pg_shard.osd << " shard " << shard << " contents ";
      Formatter *f = Formatter::create("json");
      f->open_object_section("t");
      transaction.dump(f);
      f->close_section();
      f->flush(*_dout);
      delete f;
      *_dout << dendl;
    }
    bool should_send = get_parent()->should_send_op(pg_shard, op.hoid);
    /* should_send being false indicates that a recovery is going on to this
     * object this makes it critical that the log on the non-primary shards is
     * complete:- We may need to update "missing" with the latest version.
     * As such we must never skip a transaction completely.  Note that if
     * should_send is false, then an empty transaction is sent.
     */
    if (!next_write_all_shards && should_send && op.skip_transaction(pending_roll_forward, shard, transaction)) {
      // Must be an empty transaction
      ceph_assert(transaction.empty());
      dout(20) << __func__ << " Skipping transaction for shard " << shard << dendl;
      continue;
    }
    if (!should_send || transaction.empty()) {
      dout(20) << __func__ << " Sending empty transaction for shard " << shard << dendl;
    }
    op.pending_commits++;
    const pg_stat_t &stats =
        (should_send || !backfill_shards.contains(pg_shard))
          ? get_info().stats
          : get_parent()->get_shard_info().find(pg_shard)->second.stats;

    ECSubWrite sop(
      get_parent()->whoami_shard(),
      op.tid,
      op.reqid,
      op.hoid,
      stats,
      should_send ? transaction : empty,
      op.version,
      op.trim_to,
      op.pg_committed_to,
      op.log_entries,
      op.updated_hit_set_history,
      op.temp_added,
      op.temp_cleared,
      !should_send);

    ZTracer::Trace trace;
    if (op.trace) {
      // initialize a child span for this shard
      trace.init("ec sub write", nullptr, &op.trace);
      trace.keyval("shard", pg_shard.shard.id);
    }

    if (pg_shard == get_parent()->whoami_shard()) {
      should_write_local = true;
      local_write_op.claim(sop);
    } else if (cct->_conf->bluestore_debug_inject_read_err &&
      ECInject::test_write_error1(ghobject_t(op.hoid,
                                             ghobject_t::NO_GEN,
                                             pg_shard.shard))) {
      dout(0) << " Error inject - Dropping write message to shard " <<
          pg_shard.shard << dendl;
    } else {
      auto *r = new MOSDECSubOpWrite(sop);
      r->pgid = spg_t(get_parent()->primary_spg_t().pgid, pg_shard.shard);
      r->map_epoch = get_osdmap_epoch();
      r->min_epoch = get_parent()->get_interval_start_epoch();
      r->trace = trace;
      messages.push_back(std::make_pair(pg_shard.osd, r));
    }
  }

  next_write_all_shards = false;

  if (!messages.empty()) {
    get_parent()->send_message_osd_cluster(messages, get_osdmap_epoch());
  }

  if (should_write_local) {
    handle_sub_write(
      get_parent()->whoami_shard(),
      op.client_op,
      local_write_op,
      op.trace);
  }


  for (auto &cop: op.cache_ops) {
    const hobject_t &oid = cop->get_hoid();
    if (written.contains(oid)) {
      extent_cache.write_done(cop, std::move(written.at(oid)));
    } else {
      extent_cache.write_done(cop, ECUtil::shard_extent_map_t(&sinfo));
    }
  }
}

struct ECDummyOp final : ECCommon::RMWPipeline::Op {
  void generate_transactions(
      ceph::ErasureCodeInterfaceRef &ec_impl,
      pg_t pgid,
      const ECUtil::stripe_info_t &sinfo,
      map<hobject_t, ECUtil::shard_extent_map_t> *written,
      shard_id_map<ObjectStore::Transaction> *transactions,
      DoutPrefixProvider *dpp,
      const OSDMapRef &osdmap
    ) override {
    // NOP, as -- in contrast to ECClassicalOp -- there is no
    // transaction involved
  }

  bool skip_transaction(
      std::set<shard_id_t> &pending_roll_forward,
      const shard_id_t shard,
      ceph::os::Transaction &transaction
    ) override {
    return !pending_roll_forward.erase(shard);
  }
};

void ECCommon::RMWPipeline::try_finish_rmw() {
  while (waiting_commit.size() > 0) {
    OpRef op = waiting_commit.front();

    if (op->pending_commits != 0 || op->pending_cache_ops != 0) {
      return;
    }

    waiting_commit.pop_front();
    finish_rmw(op);
  }
}

void ECCommon::RMWPipeline::finish_rmw(OpRef const &op) {
  dout(20) << __func__ << " op=" << *op << dendl;

  if (op->on_all_commit) {
    dout(10) << __func__ << " Calling on_all_commit on " << *op << dendl;
    op->on_all_commit->complete(0);
    op->on_all_commit = nullptr;
    op->trace.event("ec write all committed");
  }

  if (op->pg_committed_to > completed_to)
    completed_to = op->pg_committed_to;
  if (op->version > committed_to)
    committed_to = op->version;

  op->cache_ops.clear();

  if (extent_cache.idle()) {
    if (op->version > get_parent()->get_log().get_can_rollback_to()) {
      dout(20) << __func__ << " cache idle " << op->version << dendl;
      // submit a dummy, transaction-empty op to kick the rollforward
      const auto tid = get_parent()->get_tid();
      const auto nop = std::make_shared<ECDummyOp>();
      nop->hoid = op->hoid;
      nop->trim_to = op->trim_to;
      nop->pg_committed_to = op->version;
      nop->tid = tid;
      nop->reqid = op->reqid;
      nop->pending_cache_ops = 1;
      nop->pipeline = this;

      tid_to_op_map[tid] = nop;

      /* The cache is idle (we checked above) and this IO never blocks for reads
       * so we can skip the extent cache and immediately call the completion.
       */
      nop->cache_ready(nop->hoid, ECUtil::shard_extent_map_t(&sinfo));
    }
  }

  tid_to_op_map.erase(op->tid);
}

void ECCommon::RMWPipeline::on_change() {
  dout(10) << __func__ << dendl;

  completed_to = eversion_t();
  committed_to = eversion_t();
  extent_cache.on_change();
  tid_to_op_map.clear();
  oid_to_version.clear();
  waiting_commit.clear();
  next_write_all_shards = false;
}

void ECCommon::RMWPipeline::on_change2() {
  extent_cache.on_change2();
}

void ECCommon::RMWPipeline::call_write_ordered(std::function<void(void)> &&cb) {
  next_write_all_shards = true;
  extent_cache.add_on_write(std::move(cb));
}

ECCommon::RecoveryBackend::RecoveryBackend(
  CephContext *cct,
  const coll_t &coll,
  ceph::ErasureCodeInterfaceRef ec_impl,
  const ECUtil::stripe_info_t &sinfo,
  ReadPipeline &read_pipeline,
  ECListener *parent)
  : cct(cct),
    coll(coll),
    ec_impl(std::move(ec_impl)),
    sinfo(sinfo),
    read_pipeline(read_pipeline),
    parent(parent) {}

void ECCommon::RecoveryBackend::_failed_push(const hobject_t &hoid,
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

void ECCommon::RecoveryBackend::handle_recovery_push(
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

  ceph_assert(op.data.length() == op.data_included.size());
  uint64_t tobj_size = 0;

  uint64_t cursor = 0;
  for (auto [off, len] : op.data_included) {
    bufferlist bl;
    if (len != op.data.length()) {
      bl.substr_of(op.data, cursor, len);
    } else {
      bl = op.data;
    }
    m->t.write(coll, tobj, off, len, bl);
    tobj_size = off + len;
    cursor += len;
  }

  if (op.before_progress.first) {
    ceph_assert(op.attrset.contains(OI_ATTR));
    m->t.setattrs(
      coll,
      tobj,
      op.attrset);
  }

  if (op.after_progress.data_complete) {
    uint64_t shard_size = sinfo.object_size_to_shard_size(op.recovery_info.size,
      get_parent()->whoami_shard().shard);
    ceph_assert(shard_size >= tobj_size);
    if (shard_size != tobj_size) {
      m->t.truncate( coll, tobj, shard_size);
    }
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

void ECCommon::RecoveryBackend::handle_recovery_push_reply(
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

void ECCommon::RecoveryBackend::update_object_size_after_read(
    uint64_t size,
    read_result_t &res,
    read_request_t &req) {
  // We didn't know the size before, meaning the zero for decode calculations
  // will be off. Recalculate them!
  ECUtil::shard_extent_set_t zero_mask(sinfo.get_k_plus_m());
  sinfo.ro_size_to_zero_mask(size, zero_mask);
  ECUtil::shard_extent_set_t read_mask(sinfo.get_k_plus_m());
  sinfo.ro_size_to_read_mask(size, read_mask);
  extent_set superset = res.buffers_read.get_extent_superset();

  for (auto &&[shard, eset] : zero_mask) {
    eset.intersection_of(superset);
    if (!eset.empty() &&
        (res.zero_length_reads.contains(shard) ||
          res.buffers_read.contains(shard))) {
      req.zeros_for_decode[shard].insert(eset);
    }
  }

  /* Correct the shard_want_to_read, to make sure everything is within scope
   * of the newly found object size.
   */
  for (auto iter = req.shard_want_to_read.begin(); iter != req.shard_want_to_read.end();) {
    auto &&[shard, eset] = *iter;
    bool erase = false;

    if (read_mask.contains(shard)) {
      eset.intersection_of(read_mask.get(shard));
      erase = eset.empty();
    } else {
      erase = true;
    }

    /* Some shards may be empty */
    if (erase) {
      iter = req.shard_want_to_read.erase(iter);
    } else {
      ++iter;
    }
  }

  dout(20) << "Update want and zeros from read:size=" << size
           << " res=" << res
           << " req=" << req
           << dendl;
}

void ECCommon::RecoveryBackend::handle_recovery_read_complete(
    const hobject_t &hoid,
    read_result_t &&res,
    read_request_t &req,
    RecoveryMessages *m) {
  dout(10) << __func__ << ": returned " << hoid << " " << res << dendl;
  ceph_assert(recovery_ops.contains(hoid));
  RecoveryBackend::RecoveryOp &op = recovery_ops[hoid];

  if (res.attrs) {
    op.xattrs.swap(*(res.attrs));
    const auto empty_obc = !op.obc;
    maybe_load_obc(op.xattrs, op);
#ifdef WITH_CRIMSON
    ceph_assert(hoid == op.hoid);
#endif
    if (empty_obc) {
      update_object_size_after_read(op.recovery_info.size, res, req);
    }
  }
  ceph_assert(op.xattrs.size());
  ceph_assert(op.obc);

  op.returned_data.emplace(std::move(res.buffers_read));
  uint64_t aligned_size = ECUtil::align_next(op.obc->obs.oi.size);

  dout(30) << __func__ << " before decode: oid=" << op.hoid << " EC_DEBUG_BUFFERS: "
         << op.returned_data->debug_string(2048, 0)
         << dendl;

  op.returned_data->add_zero_padding_for_decode(req.zeros_for_decode);
  int r = op.returned_data->decode(ec_impl, req.shard_want_to_read, aligned_size, get_parent()->get_dpp(), true);
  ceph_assert(r == 0);

  // Finally, we don't want to write any padding, so truncate the buffer
  // to remove it.
  op.returned_data->erase_after_ro_offset(aligned_size);

  dout(20) << __func__ << ": oid=" << op.hoid << dendl;
  dout(30) << __func__ << " after decode: oid=" << op.hoid << " EC_DEBUG_BUFFERS: "
           << op.returned_data->debug_string(2048, 0)
           << dendl;

  continue_recovery_op(op, m);
}


struct RecoveryReadCompleter : ECCommon::ReadCompleter {
  RecoveryReadCompleter(ECCommon::RecoveryBackend &backend)
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
      std::move(res),
      req,
      &rm);
  }

  void finish(int priority) && override {
    backend.dispatch_recovery_messages(rm, priority);
  }

  ECCommon::RecoveryBackend &backend;
  RecoveryMessages rm;
};

void ECCommon::RecoveryBackend::dispatch_recovery_messages(
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
    get_parent()->send_message_osd_cluster(i->first.osd, msg, msg->map_epoch);
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

  if (!replies.empty()) {
    dout(20) << __func__ << " recovery_transactions=";
    Formatter *f = Formatter::create("json");
    f->open_object_section("t");
    m.t.dump(f);
    f->close_section();
    f->flush(*_dout);
    delete f;
    *_dout << dendl;
    commit_txn_send_replies(std::move(m.t), std::move(replies));
  }

  if (m.recovery_reads.empty())
    return;
  read_pipeline.start_read_op(
    priority,
    m.recovery_reads,
    false,
    true,
    std::make_unique<RecoveryReadCompleter>(*this));
}

void ECCommon::RecoveryBackend::continue_recovery_op(
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

        op.returned_data->get_sparse_buffer(pg_shard.shard, pop.data, pop.data_included);
        ceph_assert(pop.data.length() == pop.data_included.size());

        dout(10) << __func__ << ": pop shard=" << pg_shard
                 << ", oid=" << pop.soid
                 << ", before_progress=" << op.recovery_progress
		 << ", after_progress=" << after_progress
		 << ", pop.data.length()=" << pop.data.length()
                 << ", pop.data_included=" << pop.data_included
		 << ", size=" << op.obc->obs.oi.size << dendl;

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

          // Following an upgrade, or turning of overwrites, we can take this
          // opportunity to clean up hinfo.
          if (pop.attrset.contains(ECUtil::get_hinfo_key())) {
            pop.attrset.erase(ECUtil::get_hinfo_key());
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

ECCommon::RecoveryBackend::RecoveryOp
ECCommon::RecoveryBackend::recover_object(
  const hobject_t &hoid,
  eversion_t v,
  ObjectContextRef head,
  ObjectContextRef obc) {
  RecoveryOp op;
  op.v = v;
  op.hoid = hoid;
  op.obc = obc;
  op.recovery_info.soid = hoid;
  op.recovery_info.version = v;
  if (obc) {
    op.recovery_info.size = obc->obs.oi.size;
    op.recovery_info.oi = obc->obs.oi;
  }
  if (hoid.is_snap()) {
    if (obc) {
      ceph_assert(obc->ssc);
      op.recovery_info.ss = obc->ssc->snapset;
    } else if (head) {
      ceph_assert(head->ssc);
      op.recovery_info.ss = head->ssc->snapset;
    } else {
      ceph_abort_msg("neither obc nor head set for a snap object");
    }
  }
  op.recovery_progress.omap_complete = true;
  for (set<pg_shard_t>::const_iterator i =
         get_parent()->get_acting_recovery_backfill_shards().begin();
       i != get_parent()->get_acting_recovery_backfill_shards().end();
       ++i) {
    dout(10) << "checking " << *i << dendl;
    if (get_parent()->get_shard_missing(*i).is_missing(hoid)) {
      op.missing_on.insert(*i);
      op.missing_on_shards.insert(i->shard);
    }
  }
  dout(10) << __func__ << ": built op " << op << dendl;
  return op;
}

std::optional<object_info_t> ECCommon::get_object_info_from_obc(
    ObjectContextRef &obc) {
  std::optional<object_info_t> ret;

  auto attr_cache = obc->attr_cache;
  if (!attr_cache.contains(OI_ATTR))
    return ret;

  ret.emplace(attr_cache.at(OI_ATTR));
  return ret;
}

ECTransaction::WritePlan ECCommon::get_write_plan(
  const ECUtil::stripe_info_t &sinfo,
  PGTransaction &t,
  ECCommon::ReadPipeline &read_pipeline,
  ECCommon::RMWPipeline &rmw_pipeline,
  DoutPrefixProvider *dpp) {
  ECTransaction::WritePlan plans;
  auto obc_map = t.obc_map;
  t.safe_create_traverse(
    [&](std::pair<const hobject_t, PGTransaction::ObjectOperation> &i) {
      const auto &[oid, inner_op] = i;
      auto &obc = obc_map.at(oid);
      object_info_t oi = obc->obs.oi;
      std::optional<object_info_t> soi;

      hobject_t source;
      if (inner_op.has_source(&source)) {
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
                                       oi, soi,
                                       rmw_pipeline.ec_pdw_write_mode);

      if (plan.to_read) plans.want_read = true;
      plans.plans.emplace_back(std::move(plan));
  });
  ldpp_dout(dpp, 20) << __func__ << " plans=" << plans << dendl;
  return plans;
}


END_IGNORE_DEPRECATED
