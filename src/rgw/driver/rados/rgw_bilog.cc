// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include "cls/rgw/cls_rgw_ops.h"
#include "rgw_bilog_fifo.h"
#include "rgw_log_backing.h"
#include "rgw_bucket.h"
#include "cls/rgw/cls_rgw_types.h"
#include "common/dout.h"

#define dout_subsys ceph_subsys_rgw

asio::awaitable<void> RGWBILogFIFO::push(const DoutPrefixProvider *dpp,
                                          const rgw_bi_log_entry& entry) {
  ceph::buffer::list bl;
  encode(entry, bl);
  co_return co_await fifo.push(dpp, std::move(bl), asio::use_awaitable);
}

asio::awaitable<void> RGWBILogFIFO::push(const DoutPrefixProvider *dpp,
                                          const std::vector<rgw_bi_log_entry>& entries) {
  if (entries.empty()) {
    co_return;
  }
  
  std::deque<ceph::buffer::list> items;
  items.reserve(entries.size());
  for (const auto& entry : entries) {
    ceph::buffer::list bl;
    encode(entry, bl);
    items.push_back(std::move(bl));
  }
  
  co_return co_await fifo.push(dpp, std::move(items), asio::use_awaitable);
}

asio::awaitable<std::tuple<std::vector<rgw_bi_log_entry>, std::string, bool>>
RGWBILogFIFO::list(const DoutPrefixProvider *dpp, std::string marker, uint32_t max_entries) {
  std::vector<fifo::entry> raw_entries(max_entries);
  auto [entries_span, next_marker] = co_await fifo.list(dpp, marker, raw_entries, asio::use_awaitable);
  
  std::vector<rgw_bi_log_entry> result;
  result.reserve(entries_span.size());
  
  for (const auto& raw_entry : entries_span) {
    rgw_bi_log_entry entry;
    auto iter = raw_entry.data.cbegin();
    try {
      decode(entry, iter);
      result.push_back(std::move(entry));
    } catch (const ceph::buffer::error& e) {
      // log error?
      continue;
    }
  }
  
  bool truncated = next_marker.has_value();
  std::string out_marker = next_marker ? *next_marker : std::string{};
  
  co_return std::make_tuple(std::move(result), std::move(out_marker), truncated);
}

asio::awaitable<void> RGWBILogFIFO::trim(const DoutPrefixProvider *dpp,
                                          std::string_view marker) {
  co_return co_await fifo.trim(dpp, std::string(marker), false, asio::use_awaitable);
}

asio::awaitable<std::string> RGWBILogFIFO::get_max_marker(const DoutPrefixProvider *dpp) {
  auto [marker, timestamp] = co_await fifo.last_entry_info(dpp, asio::use_awaitable);
  co_return marker;
}

asio::awaitable<bool> RGWBILogFIFO::is_empty(const DoutPrefixProvider *dpp) {
  std::vector<fifo::entry> entries(1); // only need to check for one entry
  auto [entries_span, marker] = co_await fifo.list(dpp, {}, entries, asio::use_awaitable);
  co_return entries_span.empty();
}

void RGWBILogFIFO::push(const DoutPrefixProvider *dpp,
                        const rgw_bi_log_entry& entry,
                        asio::yield_context y) {
  ceph::buffer::list bl;
  encode(entry, bl);
  fifo.push(dpp, std::move(bl), y);
}

void RGWBILogFIFO::push(const DoutPrefixProvider *dpp,
                        const std::vector<rgw_bi_log_entry>& entries,
                        asio::yield_context y) {
  if (entries.empty()) {
    return;
  }
  
  std::deque<ceph::buffer::list> items;
  items.reserve(entries.size());
  for (const auto& entry : entries) {
    ceph::buffer::list bl;
    encode(entry, bl);
    items.push_back(std::move(bl));
  }
  
  fifo.push(dpp, std::move(items), y);
}

std::tuple<std::vector<rgw_bi_log_entry>, std::string, bool>
RGWBILogFIFO::list(const DoutPrefixProvider *dpp, std::string marker, 
                   uint32_t max_entries, asio::yield_context y) {
  std::vector<fifo::entry> raw_entries(max_entries);
  auto [entries_span, next_marker] = fifo.list(dpp, marker, raw_entries, y);
  
  std::vector<rgw_bi_log_entry> result;
  result.reserve(entries_span.size());
  
  for (const auto& raw_entry : entries_span) {
    rgw_bi_log_entry entry;
    auto iter = raw_entry.data.cbegin();
    try {
      decode(entry, iter);
      result.push_back(std::move(entry));
    } catch (const ceph::buffer::error& e) {
      continue;
    }
  }
  
  bool truncated = next_marker.has_value();
  std::string out_marker = next_marker ? *next_marker : std::string{};
  
  return std::make_tuple(std::move(result), std::move(out_marker), truncated);
}

void RGWBILogFIFO::trim(const DoutPrefixProvider *dpp,
                        std::string_view marker, 
                        asio::yield_context y) {
  fifo.trim(dpp, std::string(marker), false, y);
}

std::string RGWBILogFIFO::get_max_marker(const DoutPrefixProvider *dpp,
                                          asio::yield_context y) {
  auto [marker, timestamp] = fifo.last_entry_info(dpp, y);
  return marker;
}

bool RGWBILogFIFO::is_empty(const DoutPrefixProvider *dpp, asio::yield_context y) {
  std::vector<fifo::entry> entries(1);
  auto [entries_span, marker] = fifo.list(dpp, {}, entries, y);
  return entries_span.empty();
}

std::string_view RGWBILogFIFO::max_marker() {
  static const auto max_mark = fifo::FIFO::max_marker();
  return std::string_view(max_mark);
}

RGWBILogUpdateBatch::RGWBILogUpdateBatch(const DoutPrefixProvider *dpp, 
                                          neorados::RADOS r,
                                          neorados::IOContext loc, 
                                          const RGWBucketInfo& bucket_info,
                                          size_t max_batch_size, /* should be configurable. set to 1 for now*/ )
  : dpp(dpp), bilog_fifo(std::move(r), std::move(loc), bucket_info),
    max_batch_size(max_batch_size) {
  entries.reserve(max_batch_size);
}

void RGWBILogUpdateBatch::add_entry(const rgw_bi_log_entry& entry) {
  entries.push_back(entry);
  
  if (entries.size() >= max_batch_size) {
    flush();
  }
}

void RGWBILogUpdateBatch::add_maybe_flush(const uint64_t olh_epoch, 
                                          const ceph::real_time set_mtime,
                                          const cls_rgw_bi_log_related_op& bi_log_client_info) {
  ldpp_dout(dpp, 20) << __PRETTY_FUNCTION__ 
                     << ": adding bilog entry" << dendl;
  
  rgw_bi_log_entry entry;
  entry.object = bi_log_client_info.key.name;
  entry.instance = bi_log_client_info.key.instance;
  entry.timestamp = set_mtime;
  entry.op = bi_log_client_info.op;
  
  // set olh epoch
  rgw_bucket_entry_ver ver;
  ver.epoch = olh_epoch;
  entry.ver = std::move(ver);
  
  entry.state = CLS_RGW_STATE_COMPLETE;
  entry.tag = bi_log_client_info.op_tag;
  entry.bilog_flags = bi_log_client_info.bilog_flags;
  entry.owner = decltype(entry.owner){};
  entry.owner_display_name = decltype(entry.owner_display_name){};
  entry.zones_trace = bi_log_client_info.zones_trace;

  #if 0
    // TODO: handle the entry.id
    bi_log_index_key(hctx, key, entry.id, index_ver);
    if (entry.id > max_marker)
      max_marker = entry.id;
  #endif
  
  add_entry(entry);
}

void RGWBILogUpdateBatch::add_maybe_flush(const uint64_t olh_epoch,
                                          const cls_rgw_obj_key& key,
                                          const std::string& op_tag,
                                          const bool delete_marker,
                                          const rgw_bucket_olh_log_bi_log_entry& bi_log_replay_data) {
  ldpp_dout(dpp, 20) << __PRETTY_FUNCTION__ << ": the OLH-specific variant" << dendl;
  
  rgw_bi_log_entry entry;
  entry.object = key.name;
  entry.instance = key.instance;
  entry.timestamp = bi_log_replay_data.timestamp;
  entry.op = CLSRGWLinkOLHBase::get_bilog_op_type(delete_marker);
  
  // olh epoch
  rgw_bucket_entry_ver ver;
  ver.epoch = olh_epoch;
  entry.ver = std::move(ver);
  
  entry.state = CLS_RGW_STATE_COMPLETE;
  entry.tag = op_tag;
  entry.bilog_flags = get_olh_op_bilog_flags() | RGW_BILOG_FLAG_VERSIONED_OP;
  
  if (delete_marker) {
    entry.owner = bi_log_replay_data.owner;
    entry.owner_display_name = bi_log_replay_data.owner_display_name;
  }
  entry.zones_trace = bi_log_replay_data.zones_trace;

  #if 0
    // TODO: handle the entry.id
    bi_log_index_key(hctx, key, entry.id, index_ver);
    if (entry.id > max_marker)
      max_marker = entry.id;
  #endif

  add_entry(entry);
}

void RGWBILogUpdateBatch::add_maybe_flush(RGWModifyOp op,
                                          const rgw_bucket_dir_entry& list_state,
                                          rgw_zone_set zones_trace) {
  rgw_bi_log_entry entry;
  entry.object = list_state.key.name;
  entry.instance = list_state.key.instance;
  entry.timestamp = list_state.meta.mtime;
  entry.op = op;
  entry.state = CLS_RGW_STATE_COMPLETE;
  entry.tag = list_state.tag;
  entry.zones_trace = std::move(zones_trace);
  
  add_entry(entry);
}

asio::awaitable<void> RGWBILogUpdateBatch::flush_async() {
  if (!entries.empty()) {
    co_await bilog_fifo.push(dpp, entries);
    entries.clear();
    entries.reserve(max_batch_size);
  }
}

void RGWBILogUpdateBatch::flush() {
  if (!entries.empty()) {
    bilog_fifo.push(dpp, entries, ceph::async::use_blocked);
    entries.clear();
    entries.reserve(max_batch_size);
  }
}

void RGWBILogUpdateBatch::flush(asio::yield_context y) {
  if (!entries.empty()) {
    bilog_fifo.push(dpp, entries, y);
    entries.clear();
    entries.reserve(max_batch_size);
  }
}

// flush on destruction
RGWBILogUpdateBatch::~RGWBILogUpdateBatch() {
  if (!entries.empty()) {
    try {
      flush();
    } catch (const std::exception& e) {
      ldpp_dout(dpp, 0) << "ERROR: failed to flush pending bilog entries: "
                        << e.what() << dendl;
    }
  }
}

size_t RGWBILogUpdateBatch::size() const {
  return entries.size();
}

bool RGWBILogUpdateBatch::empty() const {
  return entries.empty();
}

void RGWBILogUpdateBatch::set_max_batch_size(size_t size) {
  max_batch_size = size;
  if (entries.capacity() < size) {
    entries.reserve(size);
  }
}

RGWBILogFIFO& RGWBILogUpdateBatch::get_fifo() {
  return bilog_fifo;
}