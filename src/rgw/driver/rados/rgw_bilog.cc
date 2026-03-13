// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include "rgw_bilog.h"

#include <span>
#include <string>

#include <boost/system/system_error.hpp>

#include "common/async/blocked_completion.h"
#include "common/dout.h"
#include "rgw_asio_thread.h"
#include "services/svc_bi_rados.h"

#define dout_subsys ceph_subsys_rgw

using ceph::containers::tiny_vector;

RGWBILogFIFO::RGWBILogFIFO(neorados::RADOS rados,
                             const neorados::IOContext& loc,
                             std::span<const std::string> shard_oids)
  : fifos(shard_oids.size(),
          [&rados, &loc, &shard_oids](std::size_t i, auto emplacer) {
            emplacer.emplace(rados, bilog_fifo_oid(shard_oids[i]), loc);
          })
{}

asio::awaitable<void>
RGWBILogFIFO::push(const DoutPrefixProvider* dpp,
                   int shard_id,
                   ceph::buffer::list entry)
{
  co_return co_await fifos[shard_id].push(dpp, std::move(entry));
}

void RGWBILogFIFO::push(const DoutPrefixProvider* dpp,
                   int shard_id,
                   ceph::buffer::list entry,
                   asio::yield_context y)
{
  fifos[shard_id].push(dpp, std::move(entry), y);
}

asio::awaitable<std::tuple<std::span<fifo::entry>, std::optional<std::string>>>
RGWBILogFIFO::list(const DoutPrefixProvider* dpp,
                   int shard_id,
                   std::string marker,
                   std::span<fifo::entry> entries)
{
  co_return co_await fifos[shard_id].list(dpp, std::move(marker), entries);
}

std::tuple<std::span<fifo::entry>, std::optional<std::string>>
RGWBILogFIFO::list(const DoutPrefixProvider* dpp,
                   int shard_id,
                   std::string marker,
                   std::span<fifo::entry> entries,
                   asio::yield_context y)
{
  return fifos[shard_id].list(dpp, std::move(marker), entries, y);
}

asio::awaitable<void>
RGWBILogFIFO::trim(const DoutPrefixProvider* dpp,
                   int shard_id,
                   std::string marker,
                   bool exclusive)
{
  co_return co_await fifos[shard_id].trim(dpp, std::move(marker), exclusive);
}

void RGWBILogFIFO::trim(const DoutPrefixProvider* dpp,
                   int shard_id,
                   std::string marker,
                   bool exclusive,
                   asio::yield_context y)
{
  fifos[shard_id].trim(dpp, std::move(marker), exclusive, y);
}

std::string_view RGWBILogFIFO::max_marker()
{
  static const std::string s = fifo::FIFO::max_marker();
  return std::string_view(s);
}


RGWBILogUpdateBatch::RGWBILogUpdateBatch(const DoutPrefixProvider* dpp,
                                         neorados::RADOS r,
                                         std::shared_ptr<RGWBILogFIFO> fifo)
  : dpp(dpp),
    rados_(std::move(r)),
    fifo_(std::move(fifo))
{}

int RGWBILogUpdateBatch::shard_of(const cls_rgw_obj_key& key) const
{
  const int n = fifo_ ? fifo_->num_shards() : 0;
  if (n <= 1) {
    return 0;
  }
  return RGWSI_BucketIndex_RADOS::bucket_shard_index(key, n);
}

void RGWBILogUpdateBatch::stage(int shard, rgw_bi_log_entry entry)
{
  pending.push_back(Pending{shard, std::move(entry)});
}

void RGWBILogUpdateBatch::add_maybe_flush(uint64_t olh_epoch,
                                          ceph::real_time mtime,
                                          const cls_rgw_bi_log_related_op& op_info)
{
  ldpp_dout(dpp, 20) << __func__ << ": key=" << op_info.key
                     << " op=" << (int)op_info.op << dendl;
  rgw_bi_log_entry entry;
  entry.object = op_info.key.name;
  entry.instance = op_info.key.instance;
  entry.timestamp = mtime;
  entry.op = op_info.op;
  entry.state = CLS_RGW_STATE_COMPLETE;
  entry.tag = op_info.op_tag;
  entry.bilog_flags = op_info.bilog_flags;
  entry.zones_trace = op_info.zones_trace;
  entry.ver.epoch = olh_epoch;
  stage(shard_of(op_info.key), std::move(entry));
}

void RGWBILogUpdateBatch::add_maybe_flush(uint64_t olh_epoch,
                                          const cls_rgw_obj_key& key,
                                          const std::string& op_tag,
                                          bool delete_marker,
                                          ceph::real_time mtime,
                                          const rgw_zone_set& zones_trace)
{
  ldpp_dout(dpp, 20) << __func__ << ": key=" << key
                     << " delete_marker=" << delete_marker << dendl;
  rgw_bi_log_entry entry;
  entry.object = key.name;
  entry.instance = key.instance;
  entry.timestamp = mtime;
  entry.op = delete_marker ? CLS_RGW_OP_LINK_OLH_DM : CLS_RGW_OP_LINK_OLH;
  entry.state = CLS_RGW_STATE_COMPLETE;
  entry.tag = op_tag;
  entry.bilog_flags = RGW_BILOG_FLAG_VERSIONED_OP;
  entry.zones_trace = zones_trace;
  entry.ver.epoch = olh_epoch;
  stage(shard_of(key), std::move(entry));
}

void RGWBILogUpdateBatch::add_maybe_flush(RGWModifyOp op,
                                          const rgw_bucket_dir_entry& list_state,
                                          rgw_zone_set zones_trace)
{
  ldpp_dout(dpp, 20) << __func__ << ": key=" << list_state.key
                     << " op=" << (int)op << dendl;
  rgw_bi_log_entry entry;
  entry.object = list_state.key.name;
  entry.instance = list_state.key.instance;
  entry.timestamp = list_state.meta.mtime;
  entry.op = op;
  entry.state = CLS_RGW_STATE_COMPLETE;
  entry.tag = list_state.tag;
  entry.zones_trace = std::move(zones_trace);
  stage(shard_of(list_state.key), std::move(entry));
}

void RGWBILogUpdateBatch::do_flush(asio::yield_context y)
{
  if (!fifo_) {
    pending.clear(); 
    return;
  }
  for (auto& [shard, entry] : pending) {
    ceph::buffer::list bl;
    encode(entry, bl);
    try {
      fifo_->push(dpp, shard, std::move(bl), y);
    } catch (const boost::system::system_error& e) {
      ldpp_dout(dpp, 5) << __func__ << ": failed to push bilog entry for "
                       << entry.object << "/" << entry.instance
                       << " shard=" << shard
                       << ": " << e.what() << dendl;
    }
  }
  pending.clear();
}

void RGWBILogUpdateBatch::do_flush()
{
  if (!fifo_) {
    pending.clear();
    return;
  }
  maybe_warn_about_blocking(dpp);
  asio::spawn(rados_.get_executor(),
              [this](asio::yield_context y) { do_flush(y); },
              ceph::async::use_blocked);
}

void RGWBILogUpdateBatch::flush(asio::yield_context y)
{
  if (!pending.empty()) {
    do_flush(y);
  }
}

asio::awaitable<void> RGWBILogUpdateBatch::co_flush()
{
  if (pending.empty()) {
    co_return;
  }
  if (!fifo_) {
    pending.clear();
    co_return;
  }
  for (auto& [shard, entry] : pending) {
    ceph::buffer::list bl;
    encode(entry, bl);
    try {
      co_await fifo_->push(dpp, shard, std::move(bl));
    } catch (const boost::system::system_error& e) {
      ldpp_dout(dpp, 5) << __func__ << ": failed to push bilog entry for "
                       << entry.object << "/" << entry.instance
                       << " shard=" << shard
                       << ": " << e.what() << dendl;
    }
  }
  pending.clear();
}

void RGWBILogUpdateBatch::flush()
{
  if (!pending.empty()) {
    do_flush();
  }
}

RGWBILogUpdateBatch::~RGWBILogUpdateBatch()
{
  if (!pending.empty()) {
    try {
      do_flush();
    } catch (const std::exception& e) {
      ldpp_dout(dpp, 0) << "ERROR: " << __func__
                        << ": failed to flush pending bilog entries: "
                        << e.what() << dendl;
    }
  }
}
