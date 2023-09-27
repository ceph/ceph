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

#include "ECCommon.h"
#include "messages/MOSDPGPush.h"
#include "messages/MOSDPGPushReply.h"
#include "messages/MOSDECSubOpWrite.h"
#include "messages/MOSDECSubOpWriteReply.h"
#include "messages/MOSDECSubOpRead.h"
#include "messages/MOSDECSubOpReadReply.h"
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

static ostream& _prefix(std::ostream *_dout, ECCommon::RMWPipeline *rmw_pipeline) {
  return rmw_pipeline->get_parent()->gen_dbg_prefix(*_dout);
}
static ostream& _prefix(std::ostream *_dout, ECCommon::ReadPipeline *read_pipeline) {
  return read_pipeline->get_parent()->gen_dbg_prefix(*_dout);
}
static ostream& _prefix(std::ostream *_dout,
			ECCommon::UnstableHashInfoRegistry *unstable_hash_info_registry) {
  // TODO: backref to ECListener?
  return *_dout;
}

ostream &operator<<(ostream &lhs, const ECCommon::RMWPipeline::pipeline_state_t &rhs) {
  switch (rhs.pipeline_state) {
  case ECCommon::RMWPipeline::pipeline_state_t::CACHE_VALID:
    return lhs << "CACHE_VALID";
  case ECCommon::RMWPipeline::pipeline_state_t::CACHE_INVALID:
    return lhs << "CACHE_INVALID";
  default:
    ceph_abort_msg("invalid pipeline state");
  }
  return lhs; // unreachable
}

ostream &operator<<(ostream &lhs, const ECCommon::read_request_t &rhs)
{
  return lhs << "read_request_t(to_read=[" << rhs.to_read << "]"
	     << ", need=" << rhs.need
	     << ", want_attrs=" << rhs.want_attrs
	     << ")";
}

ostream &operator<<(ostream &lhs, const ECCommon::read_result_t &rhs)
{
  lhs << "read_result_t(r=" << rhs.r
      << ", errors=" << rhs.errors;
  if (rhs.attrs) {
    lhs << ", attrs=" << *(rhs.attrs);
  } else {
    lhs << ", noattrs";
  }
  return lhs << ", returned=" << rhs.returned << ")";
}

ostream &operator<<(ostream &lhs, const ECCommon::ReadOp &rhs)
{
  lhs << "ReadOp(tid=" << rhs.tid;
#ifndef WITH_SEASTAR
  if (rhs.op && rhs.op->get_req()) {
    lhs << ", op=";
    rhs.op->get_req()->print(lhs);
  }
#endif
  return lhs << ", to_read=" << rhs.to_read
	     << ", complete=" << rhs.complete
	     << ", priority=" << rhs.priority
	     << ", obj_to_source=" << rhs.obj_to_source
	     << ", source_to_obj=" << rhs.source_to_obj
	     << ", in_progress=" << rhs.in_progress << ")";
}

void ECCommon::ReadOp::dump(Formatter *f) const
{
  f->dump_unsigned("tid", tid);
#ifndef WITH_SEASTAR
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

ostream &operator<<(ostream &lhs, const ECCommon::RMWPipeline::Op &rhs)
{
  lhs << "Op(" << rhs.hoid
      << " v=" << rhs.version
      << " tt=" << rhs.trim_to
      << " tid=" << rhs.tid
      << " reqid=" << rhs.reqid;
#ifndef WITH_SEASTAR
  if (rhs.client_op && rhs.client_op->get_req()) {
    lhs << " client_op=";
    rhs.client_op->get_req()->print(lhs);
  }
#endif
  lhs << " roll_forward_to=" << rhs.roll_forward_to
      << " temp_added=" << rhs.temp_added
      << " temp_cleared=" << rhs.temp_cleared
      << " pending_read=" << rhs.pending_read
      << " remote_read=" << rhs.remote_read
      << " remote_read_result=" << rhs.remote_read_result
      << " pending_apply=" << rhs.pending_apply
      << " pending_commit=" << rhs.pending_commit
      << " plan.to_read=" << rhs.plan.to_read
      << " plan.will_write=" << rhs.plan.will_write
      << ")";
  return lhs;
}

void ECCommon::ReadPipeline::complete_read_op(ReadOp &rop)
{
  map<hobject_t, read_request_t>::iterator reqiter =
    rop.to_read.begin();
  map<hobject_t, read_result_t>::iterator resiter =
    rop.complete.begin();
  ceph_assert(rop.to_read.size() == rop.complete.size());
  for (; reqiter != rop.to_read.end(); ++reqiter, ++resiter) {
    rop.on_complete->finish_single_request(
      reqiter->first,
      resiter->second,
      reqiter->second.to_read);
  }
  ceph_assert(rop.on_complete);
  std::move(*rop.on_complete).finish(rop.priority);
  rop.on_complete = nullptr;
  // if the read op is over. clean all the data of this tid.
  for (set<pg_shard_t>::iterator iter = rop.in_progress.begin();
    iter != rop.in_progress.end();
    iter++) {
    shard_to_read_map[*iter].erase(rop.tid);
  }
  rop.in_progress.clear();
  tid_to_read_map.erase(rop.tid);
}

template <class F, class G>
void ECCommon::ReadPipeline::check_recovery_sources(
  const OSDMapRef& osdmap,
  F&& on_erase,
  G&& on_schedule_recovery)
{
  std::set<ceph_tid_t> tids_to_filter;
  for (std::map<pg_shard_t, std::set<ceph_tid_t> >::iterator 
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
  for (std::set<ceph_tid_t>::iterator i = tids_to_filter.begin();
       i != tids_to_filter.end();
       ++i) {
    std::map<ceph_tid_t, ReadOp>::iterator j = tid_to_read_map.find(*i);
    ceph_assert(j != tid_to_read_map.end());
    filter_read_op(osdmap, j->second, on_erase, on_schedule_recovery);
  }
}

template <class F, class G>
void ECCommon::ReadPipeline::filter_read_op(
  const OSDMapRef& osdmap,
  ReadOp &op,
  F&& on_erase,
  G&& on_schedule_recovery)
{
  std::set<hobject_t> to_cancel;
  for (std::map<pg_shard_t, std::set<hobject_t> >::iterator i = op.source_to_obj.begin();
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

  for (std::map<pg_shard_t, std::set<hobject_t> >::iterator i = op.source_to_obj.begin();
       i != op.source_to_obj.end();
       ) {
    for (std::set<hobject_t>::iterator j = i->second.begin();
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
      ceph_assert(!osdmap->is_down(i->first.osd));
      ++i;
    }
  }

  for (std::set<hobject_t>::iterator i = to_cancel.begin();
       i != to_cancel.end();
       ++i) {
    get_parent()->cancel_pull(*i);

    ceph_assert(op.to_read.count(*i));
    op.to_read.erase(*i);
    op.complete.erase(*i);
    on_erase(*i);
  }

  if (op.in_progress.empty()) {
    /* This case is odd.  filter_read_op gets called while processing
     * an OSDMap.  Normal, non-recovery reads only happen from acting
     * set osds.  For this op to have had a read source go down and
     * there not be an interval change, it must be part of a pull during
     * log-based recovery.
     *
     * This callback delays calling complete_read_op until later to avoid
     * dealing with recovery while handling an OSDMap.  We assign a
     * cost here of 1 because:
     * 1) This should be very rare, and the operation itself was already
     *    throttled.
     * 2) It shouldn't result in IO, rather it should result in restarting
     *    the pull on the affected objects and pushes from in-memory buffers
     *    on any now complete unaffected objects.
     */
    on_schedule_recovery(op);
  }
}

void ECCommon::ReadPipeline::on_change()
{
  for (map<ceph_tid_t, ReadOp>::iterator i = tid_to_read_map.begin();
       i != tid_to_read_map.end();
       ++i) {
    dout(10) << __func__ << ": cancelling " << i->second << dendl;
  }
  tid_to_read_map.clear();
  shard_to_read_map.clear();
  in_progress_client_reads.clear();
}

void ECCommon::ReadPipeline::get_all_avail_shards(
  const hobject_t &hoid,
  const set<pg_shard_t> &error_shards,
  set<int> &have,
  map<shard_id_t, pg_shard_t> &shards,
  bool for_recovery)
{
  for (set<pg_shard_t>::const_iterator i =
	 get_parent()->get_acting_shards().begin();
       i != get_parent()->get_acting_shards().end();
       ++i) {
    dout(10) << __func__ << ": checking acting " << *i << dendl;
    const pg_missing_t &missing = get_parent()->get_shard_missing(*i);
    if (error_shards.find(*i) != error_shards.end())
      continue;
    if (!missing.is_missing(hoid)) {
      ceph_assert(!have.count(i->shard));
      have.insert(i->shard);
      ceph_assert(!shards.count(i->shard));
      shards.insert(make_pair(i->shard, *i));
    }
  }

  if (for_recovery) {
    for (set<pg_shard_t>::const_iterator i =
	   get_parent()->get_backfill_shards().begin();
	 i != get_parent()->get_backfill_shards().end();
	 ++i) {
      if (error_shards.find(*i) != error_shards.end())
	continue;
      if (have.count(i->shard)) {
	ceph_assert(shards.count(i->shard));
	continue;
      }
      dout(10) << __func__ << ": checking backfill " << *i << dendl;
      ceph_assert(!shards.count(i->shard));
      const pg_info_t &info = get_parent()->get_shard_info(*i);
      const pg_missing_t &missing = get_parent()->get_shard_missing(*i);
      if (hoid < info.last_backfill &&
	  !missing.is_missing(hoid)) {
	have.insert(i->shard);
	shards.insert(make_pair(i->shard, *i));
      }
    }

    map<hobject_t, set<pg_shard_t>>::const_iterator miter =
      get_parent()->get_missing_loc_shards().find(hoid);
    if (miter != get_parent()->get_missing_loc_shards().end()) {
      for (set<pg_shard_t>::iterator i = miter->second.begin();
	   i != miter->second.end();
	   ++i) {
	dout(10) << __func__ << ": checking missing_loc " << *i << dendl;
	auto m = get_parent()->maybe_get_shard_missing(*i);
	if (m) {
	  ceph_assert(!(*m).is_missing(hoid));
	}
	if (error_shards.find(*i) != error_shards.end())
	  continue;
	have.insert(i->shard);
	shards.insert(make_pair(i->shard, *i));
      }
    }
  }
}

int ECCommon::ReadPipeline::get_min_avail_to_read_shards(
  const hobject_t &hoid,
  const set<int> &want,
  bool for_recovery,
  bool do_redundant_reads,
  map<pg_shard_t, vector<pair<int, int>>> *to_read)
{
  // Make sure we don't do redundant reads for recovery
  ceph_assert(!for_recovery || !do_redundant_reads);

  set<int> have;
  map<shard_id_t, pg_shard_t> shards;
  set<pg_shard_t> error_shards;

  get_all_avail_shards(hoid, error_shards, have, shards, for_recovery);

  map<int, vector<pair<int, int>>> need;
  int r = ec_impl->minimum_to_decode(want, have, &need);
  if (r < 0)
    return r;

  if (do_redundant_reads) {
      vector<pair<int, int>> subchunks_list;
      subchunks_list.push_back(make_pair(0, ec_impl->get_sub_chunk_count()));
      for (auto &&i: have) {
        need[i] = subchunks_list;
      }
  } 

  if (!to_read)
    return 0;

  for (auto &&i:need) {
    ceph_assert(shards.count(shard_id_t(i.first)));
    to_read->insert(make_pair(shards[shard_id_t(i.first)], i.second));
  }
  return 0;
}

int ECCommon::ReadPipeline::get_remaining_shards(
  const hobject_t &hoid,
  const set<int> &avail,
  const set<int> &want,
  const read_result_t &result,
  map<pg_shard_t, vector<pair<int, int>>> *to_read,
  bool for_recovery)
{
  ceph_assert(to_read);

  set<int> have;
  map<shard_id_t, pg_shard_t> shards;
  set<pg_shard_t> error_shards;
  for (auto &p : result.errors) {
    error_shards.insert(p.first);
  }

  get_all_avail_shards(hoid, error_shards, have, shards, for_recovery);

  map<int, vector<pair<int, int>>> need;
  int r = ec_impl->minimum_to_decode(want, have, &need);
  if (r < 0) {
    dout(0) << __func__ << " not enough shards left to try for " << hoid
	    << " read result was " << result << dendl;
    return -EIO;
  }

  set<int> shards_left;
  for (auto p : need) {
    if (avail.find(p.first) == avail.end()) {
      shards_left.insert(p.first);
    }
  }

  vector<pair<int, int>> subchunks;
  subchunks.push_back(make_pair(0, ec_impl->get_sub_chunk_count()));
  for (set<int>::iterator i = shards_left.begin();
       i != shards_left.end();
       ++i) {
    ceph_assert(shards.count(shard_id_t(*i)));
    ceph_assert(avail.find(*i) == avail.end());
    to_read->insert(make_pair(shards[shard_id_t(*i)], subchunks));
  }
  return 0;
}

void ECCommon::ReadPipeline::start_read_op(
  int priority,
  map<hobject_t, set<int>> &want_to_read,
  map<hobject_t, read_request_t> &to_read,
  OpRequestRef _op,
  bool do_redundant_reads,
  bool for_recovery,
  std::unique_ptr<ECCommon::ReadCompleter> on_complete)
{
  ceph_tid_t tid = get_parent()->get_tid();
  ceph_assert(!tid_to_read_map.count(tid));
  auto &op = tid_to_read_map.emplace(
    tid,
    ReadOp(
      priority,
      tid,
      do_redundant_reads,
      for_recovery,
      std::move(on_complete),
      _op,
      std::move(want_to_read),
      std::move(to_read))).first->second;
  dout(10) << __func__ << ": starting " << op << dendl;
  if (_op) {
#ifndef WITH_SEASTAR
    op.trace = _op->pg_trace;
#endif
    op.trace.event("start ec read");
  }
  do_read_op(op);
}

void ECCommon::ReadPipeline::do_read_op(ReadOp &op)
{
  int priority = op.priority;
  ceph_tid_t tid = op.tid;

  dout(10) << __func__ << ": starting read " << op << dendl;

  map<pg_shard_t, ECSubRead> messages;
  for (map<hobject_t, read_request_t>::iterator i = op.to_read.begin();
       i != op.to_read.end();
       ++i) {
    bool need_attrs = i->second.want_attrs;

    for (auto j = i->second.need.begin();
	 j != i->second.need.end();
	 ++j) {
      if (need_attrs) {
	messages[j->first].attrs_to_read.insert(i->first);
	need_attrs = false;
      }
      messages[j->first].subchunks[i->first] = j->second;
      op.obj_to_source[i->first].insert(j->first);
      op.source_to_obj[j->first].insert(i->first);
    }
    for (list<boost::tuple<uint64_t, uint64_t, uint32_t> >::const_iterator j =
	   i->second.to_read.begin();
	 j != i->second.to_read.end();
	 ++j) {
      pair<uint64_t, uint64_t> chunk_off_len =
	sinfo.aligned_offset_len_to_chunk(make_pair(j->get<0>(), j->get<1>()));
      for (auto k = i->second.need.begin();
	   k != i->second.need.end();
	   ++k) {
	messages[k->first].to_read[i->first].push_back(
	  boost::make_tuple(
	    chunk_off_len.first,
	    chunk_off_len.second,
	    j->get<2>()));
      }
      ceph_assert(!need_attrs);
    }
  }

  std::vector<std::pair<int, Message*>> m;
  m.reserve(messages.size());
  for (map<pg_shard_t, ECSubRead>::iterator i = messages.begin();
       i != messages.end();
       ++i) {
    op.in_progress.insert(i->first);
    shard_to_read_map[i->first].insert(op.tid);
    i->second.tid = tid;
    MOSDECSubOpRead *msg = new MOSDECSubOpRead;
    msg->set_priority(priority);
    msg->pgid = spg_t(
      get_info().pgid.pgid,
      i->first.shard);
    msg->map_epoch = get_osdmap_epoch();
    msg->min_epoch = get_parent()->get_interval_start_epoch();
    msg->op = i->second;
    msg->op.from = get_parent()->whoami_shard();
    msg->op.tid = tid;
    if (op.trace) {
      // initialize a child span for this shard
      msg->trace.init("ec sub read", nullptr, &op.trace);
      msg->trace.keyval("shard", i->first.shard.id);
    }
    m.push_back(std::make_pair(i->first.osd, msg));
  }
  if (!m.empty()) {
    get_parent()->send_message_osd_cluster(m, get_osdmap_epoch());
  }

  dout(10) << __func__ << ": started " << op << dendl;
}

void ECCommon::ReadPipeline::get_want_to_read_shards(
  std::set<int> *want_to_read) const
{
  const std::vector<int> &chunk_mapping = ec_impl->get_chunk_mapping();
  for (int i = 0; i < (int)ec_impl->get_data_chunk_count(); ++i) {
    int chunk = (int)chunk_mapping.size() > i ? chunk_mapping[i] : i;
    want_to_read->insert(chunk);
  }
}

struct ClientReadCompleter : ECCommon::ReadCompleter {
  ClientReadCompleter(ECCommon::ReadPipeline &read_pipeline,
                      ECCommon::ClientAsyncReadStatus *status)
    : read_pipeline(read_pipeline),
      status(status) {}

  void finish_single_request(
    const hobject_t &hoid,
    ECCommon::read_result_t &res,
    list<boost::tuple<uint64_t, uint64_t, uint32_t> > to_read) override
  {
    extent_map result;
    if (res.r != 0)
      goto out;
    ceph_assert(res.returned.size() == to_read.size());
    ceph_assert(res.errors.empty());
    for (auto &&read: to_read) {
      pair<uint64_t, uint64_t> adjusted =
	read_pipeline.sinfo.offset_len_to_stripe_bounds(
	  make_pair(read.get<0>(), read.get<1>()));
      ceph_assert(res.returned.front().get<0>() == adjusted.first);
      ceph_assert(res.returned.front().get<1>() == adjusted.second);
      map<int, bufferlist> to_decode;
      bufferlist bl;
      for (map<pg_shard_t, bufferlist>::iterator j =
	     res.returned.front().get<2>().begin();
	   j != res.returned.front().get<2>().end();
	   ++j) {
	to_decode[j->first.shard] = std::move(j->second);
      }
      int r = ECUtil::decode(
	read_pipeline.sinfo,
	read_pipeline.ec_impl,
	to_decode,
	&bl);
      if (r < 0) {
        res.r = r;
        goto out;
      }
      bufferlist trimmed;
      trimmed.substr_of(
	bl,
	read.get<0>() - adjusted.first,
	std::min(read.get<1>(),
	    bl.length() - (read.get<0>() - adjusted.first)));
      result.insert(
	read.get<0>(), trimmed.length(), std::move(trimmed));
      res.returned.pop_front();
    }
out:
    status->complete_object(hoid, res.r, std::move(result));
    read_pipeline.kick_reads();
  }

  void finish(int priority) && override
  {
    // NOP
  }

  ECCommon::ReadPipeline &read_pipeline;
  ECCommon::ClientAsyncReadStatus *status;
};

void ECCommon::ReadPipeline::objects_read_and_reconstruct(
  const map<hobject_t,
    std::list<boost::tuple<uint64_t, uint64_t, uint32_t> >
  > &reads,
  bool fast_read,
  GenContextURef<map<hobject_t,pair<int, extent_map> > &&> &&func)
{
  in_progress_client_reads.emplace_back(
    reads.size(), std::move(func));
  if (!reads.size()) {
    kick_reads();
    return;
  }

  map<hobject_t, set<int>> obj_want_to_read;
  set<int> want_to_read;
  get_want_to_read_shards(&want_to_read);
    
  map<hobject_t, read_request_t> for_read_op;
  for (auto &&to_read: reads) {
    map<pg_shard_t, vector<pair<int, int>>> shards;
    int r = get_min_avail_to_read_shards(
      to_read.first,
      want_to_read,
      false,
      fast_read,
      &shards);
    ceph_assert(r == 0);

    for_read_op.insert(
      make_pair(
	to_read.first,
	read_request_t(
	  to_read.second,
	  shards,
	  false)));
    obj_want_to_read.insert(make_pair(to_read.first, want_to_read));
  }

  start_read_op(
    CEPH_MSG_PRIO_DEFAULT,
    obj_want_to_read,
    for_read_op,
    OpRequestRef(),
    fast_read,
    false,
    std::make_unique<ClientReadCompleter>(*this, &(in_progress_client_reads.back())));
}


int ECCommon::ReadPipeline::send_all_remaining_reads(
  const hobject_t &hoid,
  ReadOp &rop)
{
  set<int> already_read;
  const set<pg_shard_t>& ots = rop.obj_to_source[hoid];
  for (set<pg_shard_t>::iterator i = ots.begin(); i != ots.end(); ++i)
    already_read.insert(i->shard);
  dout(10) << __func__ << " have/error shards=" << already_read << dendl;
  map<pg_shard_t, vector<pair<int, int>>> shards;
  int r = get_remaining_shards(hoid, already_read, rop.want_to_read[hoid],
			       rop.complete[hoid], &shards, rop.for_recovery);
  if (r)
    return r;

  list<boost::tuple<uint64_t, uint64_t, uint32_t> > offsets =
    rop.to_read.find(hoid)->second.to_read;

  // (Note cuixf) If we need to read attrs and we read failed, try to read again.
  bool want_attrs =
    rop.to_read.find(hoid)->second.want_attrs &&
    (!rop.complete[hoid].attrs || rop.complete[hoid].attrs->empty());
  if (want_attrs) {
    dout(10) << __func__ << " want attrs again" << dendl;
  }

  rop.to_read.erase(hoid);
  rop.to_read.insert(make_pair(
      hoid,
      read_request_t(
	offsets,
	shards,
	want_attrs)));
  return 0;
}

void ECCommon::ReadPipeline::kick_reads()
{
  while (in_progress_client_reads.size() &&
         in_progress_client_reads.front().is_complete()) {
    in_progress_client_reads.front().run();
    in_progress_client_reads.pop_front();
  }
}


void ECCommon::RMWPipeline::start_rmw(OpRef op)
{
  ceph_assert(op);
  dout(10) << __func__ << ": " << *op << dendl;

  ceph_assert(!tid_to_op_map.count(op->tid));
  waiting_state.push_back(*op);
  tid_to_op_map[op->tid] = std::move(op);
  check_ops();
}

bool ECCommon::RMWPipeline::try_state_to_reads()
{
  if (waiting_state.empty())
    return false;

  Op *op = &(waiting_state.front());
  if (op->requires_rmw() && pipeline_state.cache_invalid()) {
    ceph_assert(get_parent()->get_pool().allows_ecoverwrites());
    dout(20) << __func__ << ": blocking " << *op
	     << " because it requires an rmw and the cache is invalid "
	     << pipeline_state
	     << dendl;
    return false;
  }

  if (!pipeline_state.caching_enabled()) {
    op->using_cache = false;
  } else if (op->invalidates_cache()) {
    dout(20) << __func__ << ": invalidating cache after this op"
	     << dendl;
    pipeline_state.invalidate();
  }

  waiting_state.pop_front();
  waiting_reads.push_back(*op);

  if (op->using_cache) {
    cache.open_write_pin(op->pin);

    extent_set empty;
    for (auto &&hpair: op->plan.will_write) {
      auto to_read_plan_iter = op->plan.to_read.find(hpair.first);
      const extent_set &to_read_plan =
	to_read_plan_iter == op->plan.to_read.end() ?
	empty :
	to_read_plan_iter->second;

      extent_set remote_read = cache.reserve_extents_for_rmw(
	hpair.first,
	op->pin,
	hpair.second,
	to_read_plan);

      extent_set pending_read = to_read_plan;
      pending_read.subtract(remote_read);

      if (!remote_read.empty()) {
	op->remote_read[hpair.first] = std::move(remote_read);
      }
      if (!pending_read.empty()) {
	op->pending_read[hpair.first] = std::move(pending_read);
      }
    }
  } else {
    op->remote_read = op->plan.to_read;
  }

  dout(10) << __func__ << ": " << *op << dendl;

  if (!op->remote_read.empty()) {
    ceph_assert(get_parent()->get_pool().allows_ecoverwrites());
    objects_read_async_no_cache(
      op->remote_read,
      [op, this](map<hobject_t,pair<int, extent_map> > &&results) {
	for (auto &&i: results) {
	  op->remote_read_result.emplace(i.first, i.second.second);
	}
	check_ops();
      });
  }

  return true;
}

bool ECCommon::RMWPipeline::try_reads_to_commit()
{
  if (waiting_reads.empty())
    return false;
  Op *op = &(waiting_reads.front());
  if (op->read_in_progress())
    return false;
  waiting_reads.pop_front();
  waiting_commit.push_back(*op);

  dout(10) << __func__ << ": starting commit on " << *op << dendl;
  dout(20) << __func__ << ": " << cache << dendl;

  get_parent()->apply_stats(
    op->hoid,
    op->delta_stats);

  if (op->using_cache) {
    for (auto &&hpair: op->pending_read) {
      op->remote_read_result[hpair.first].insert(
	cache.get_remaining_extents_for_rmw(
	  hpair.first,
	  op->pin,
	  hpair.second));
    }
    op->pending_read.clear();
  } else {
    ceph_assert(op->pending_read.empty());
  }

  map<shard_id_t, ObjectStore::Transaction> trans;
  for (set<pg_shard_t>::const_iterator i =
	 get_parent()->get_acting_recovery_backfill_shards().begin();
       i != get_parent()->get_acting_recovery_backfill_shards().end();
       ++i) {
    trans[i->shard];
  }

  op->trace.event("start ec write");

  map<hobject_t,extent_map> written;
  op->generate_transactions(
    ec_impl,
    get_parent()->get_info().pgid.pgid,
    sinfo,
    &written,
    &trans,
    get_parent()->get_dpp(),
    get_osdmap()->require_osd_release);

  dout(20) << __func__ << ": " << cache << dendl;
  dout(20) << __func__ << ": written: " << written << dendl;
  dout(20) << __func__ << ": op: " << *op << dendl;

  if (!get_parent()->get_pool().allows_ecoverwrites()) {
    for (auto &&i: op->log_entries) {
      if (i.requires_kraken()) {
	derr << __func__ << ": log entry " << i << " requires kraken"
	     << " but overwrites are not enabled!" << dendl;
	ceph_abort();
      }
    }
  }

  map<hobject_t,extent_set> written_set;
  for (auto &&i: written) {
    written_set[i.first] = i.second.get_interval_set();
  }
  dout(20) << __func__ << ": written_set: " << written_set << dendl;
  ceph_assert(written_set == op->plan.will_write);

  if (op->using_cache) {
    for (auto &&hpair: written) {
      dout(20) << __func__ << ": " << hpair << dendl;
      cache.present_rmw_update(hpair.first, op->pin, hpair.second);
    }
  }
  op->remote_read.clear();
  op->remote_read_result.clear();

  ObjectStore::Transaction empty;
  bool should_write_local = false;
  ECSubWrite local_write_op;
  std::vector<std::pair<int, Message*>> messages;
  messages.reserve(get_parent()->get_acting_recovery_backfill_shards().size());
  set<pg_shard_t> backfill_shards = get_parent()->get_backfill_shards();
  for (set<pg_shard_t>::const_iterator i =
	 get_parent()->get_acting_recovery_backfill_shards().begin();
       i != get_parent()->get_acting_recovery_backfill_shards().end();
       ++i) {
    op->pending_apply.insert(*i);
    op->pending_commit.insert(*i);
    map<shard_id_t, ObjectStore::Transaction>::iterator iter =
      trans.find(i->shard);
    ceph_assert(iter != trans.end());
    bool should_send = get_parent()->should_send_op(*i, op->hoid);
    const pg_stat_t &stats =
      (should_send || !backfill_shards.count(*i)) ?
      get_info().stats :
      get_parent()->get_shard_info().find(*i)->second.stats;

    ECSubWrite sop(
      get_parent()->whoami_shard(),
      op->tid,
      op->reqid,
      op->hoid,
      stats,
      should_send ? iter->second : empty,
      op->version,
      op->trim_to,
      op->roll_forward_to,
      op->log_entries,
      op->updated_hit_set_history,
      op->temp_added,
      op->temp_cleared,
      !should_send);

    ZTracer::Trace trace;
    if (op->trace) {
      // initialize a child span for this shard
      trace.init("ec sub write", nullptr, &op->trace);
      trace.keyval("shard", i->shard.id);
    }

    if (*i == get_parent()->whoami_shard()) {
      should_write_local = true;
      local_write_op.claim(sop);
    } else {
      MOSDECSubOpWrite *r = new MOSDECSubOpWrite(sop);
      r->pgid = spg_t(get_parent()->primary_spg_t().pgid, i->shard);
      r->map_epoch = get_osdmap_epoch();
      r->min_epoch = get_parent()->get_interval_start_epoch();
      r->trace = trace;
      messages.push_back(std::make_pair(i->osd, r));
    }
  }

  if (!messages.empty()) {
    get_parent()->send_message_osd_cluster(messages, get_osdmap_epoch());
  }

  if (should_write_local) {
    handle_sub_write(
      get_parent()->whoami_shard(),
      op->client_op,
      local_write_op,
      op->trace);
  }

  for (auto i = op->on_write.begin();
       i != op->on_write.end();
       op->on_write.erase(i++)) {
    (*i)();
  }

  return true;
}

struct ECDummyOp : ECCommon::RMWPipeline::Op {
  void generate_transactions(
      ceph::ErasureCodeInterfaceRef &ecimpl,
      pg_t pgid,
      const ECUtil::stripe_info_t &sinfo,
      std::map<hobject_t,extent_map> *written,
      std::map<shard_id_t, ObjectStore::Transaction> *transactions,
      DoutPrefixProvider *dpp,
      const ceph_release_t require_osd_release) final
  {
    // NOP, as -- in constrast to ECClassicalOp -- there is no
    // transaction involved
  }
};

bool ECCommon::RMWPipeline::try_finish_rmw()
{
  if (waiting_commit.empty())
    return false;
  Op *op = &(waiting_commit.front());
  if (op->write_in_progress())
    return false;
  waiting_commit.pop_front();

  dout(10) << __func__ << ": " << *op << dendl;
  dout(20) << __func__ << ": " << cache << dendl;

  if (op->roll_forward_to > completed_to)
    completed_to = op->roll_forward_to;
  if (op->version > committed_to)
    committed_to = op->version;

  if (get_osdmap()->require_osd_release >= ceph_release_t::kraken) {
    if (op->version > get_parent()->get_log().get_can_rollback_to() &&
	waiting_reads.empty() &&
	waiting_commit.empty()) {
      // submit a dummy, transaction-empty op to kick the rollforward
      auto tid = get_parent()->get_tid();
      auto nop = std::make_unique<ECDummyOp>();
      nop->hoid = op->hoid;
      nop->trim_to = op->trim_to;
      nop->roll_forward_to = op->version;
      nop->tid = tid;
      nop->reqid = op->reqid;
      waiting_reads.push_back(*nop);
      tid_to_op_map[tid] = std::move(nop);
    }
  }

  if (op->using_cache) {
    cache.release_write_pin(op->pin);
  }
  tid_to_op_map.erase(op->tid);

  if (waiting_reads.empty() &&
      waiting_commit.empty()) {
    pipeline_state.clear();
    dout(20) << __func__ << ": clearing pipeline_state "
	     << pipeline_state
	     << dendl;
  }
  return true;
}

void ECCommon::RMWPipeline::check_ops()
{
  while (try_state_to_reads() ||
	 try_reads_to_commit() ||
	 try_finish_rmw());
}

void ECCommon::RMWPipeline::on_change()
{
  dout(10) << __func__ << dendl;

  completed_to = eversion_t();
  committed_to = eversion_t();
  pipeline_state.clear();
  waiting_reads.clear();
  waiting_state.clear();
  waiting_commit.clear();
  for (auto &&op: tid_to_op_map) {
    cache.release_write_pin(op.second->pin);
  }
  tid_to_op_map.clear();
}

void ECCommon::RMWPipeline::call_write_ordered(std::function<void(void)> &&cb) {
  if (!waiting_state.empty()) {
    waiting_state.back().on_write.emplace_back(std::move(cb));
  } else if (!waiting_reads.empty()) {
    waiting_reads.back().on_write.emplace_back(std::move(cb));
  } else {
    // Nothing earlier in the pipeline, just call it
    cb();
  }
}

ECUtil::HashInfoRef ECCommon::UnstableHashInfoRegistry::maybe_put_hash_info(
  const hobject_t &hoid,
  ECUtil::HashInfo &&hinfo)
{
  return registry.lookup_or_create(hoid, hinfo);
}

ECUtil::HashInfoRef ECCommon::UnstableHashInfoRegistry::get_hash_info(
  const hobject_t &hoid,
  bool create,
  const map<string, bufferlist, less<>>& attrs,
  uint64_t size)
{
  dout(10) << __func__ << ": Getting attr on " << hoid << dendl;
  ECUtil::HashInfoRef ref = registry.lookup(hoid);
  if (!ref) {
    dout(10) << __func__ << ": not in cache " << hoid << dendl;
    ECUtil::HashInfo hinfo(ec_impl->get_chunk_count());
    bufferlist bl;
    map<string, bufferlist>::const_iterator k = attrs.find(ECUtil::get_hinfo_key());
    if (k == attrs.end()) {
      dout(5) << __func__ << " " << hoid << " missing hinfo attr" << dendl;
    } else {
      bl = k->second;
    }
    if (bl.length() > 0) {
      auto bp = bl.cbegin();
      try {
        decode(hinfo, bp);
      } catch(...) {
        dout(0) << __func__ << ": Can't decode hinfo for " << hoid << dendl;
        return ECUtil::HashInfoRef();
      }
      if (hinfo.get_total_chunk_size() != size) {
        dout(0) << __func__ << ": Mismatch of total_chunk_size "
      		       << hinfo.get_total_chunk_size() << dendl;
        return ECUtil::HashInfoRef();
      } else {
        create = true;
      }
    } else if (size == 0) { // If empty object and no hinfo, create it
      create = true;
    }
    if (create) {
      ref = registry.lookup_or_create(hoid, hinfo);
    }
  }
  return ref;
}
