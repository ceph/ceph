// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#pragma once

#include<vector>
#include <string>
#include <tuple>

#include "cls/rgw/cls_rgw_ops.h"
#include "include/neorados/RADOS.hpp"
#include "neorados/cls/fifo.h"
#include "rgw_common.h"

namespace asio = boost::asio;
namespace fifo = neorados::cls::fifo;

class LazyFIFO;
class RGWBucketInfo;
class DoutPrefixProvider;

class RGWBILogFIFO {
  LazyFIFO fifo;
  static std::string get_bilog_oid(const RGWBucketInfo& bucket_info) {
    return bucket_info.bucket.bucket_id + ".bilog.fifo";
  }

public:
  RGWBILogFIFO(neorados::RADOS r, neorados::IOContext loc, 
               const RGWBucketInfo& bucket_info)
    : fifo(std::move(r), get_bilog_oid(bucket_info), std::move(loc)) {}

  asio::awaitable<void> push(const DoutPrefixProvider *dpp,
                             const rgw_bi_log_entry& entry);

  asio::awaitable<void> push(const DoutPrefixProvider *dpp,
                             const std::vector<rgw_bi_log_entry>& entries);

  asio::awaitable<std::tuple<std::vector<rgw_bi_log_entry>, std::string, bool>>
  list(const DoutPrefixProvider *dpp, std::string marker, uint32_t max_entries);

  asio::awaitable<void> trim(const DoutPrefixProvider *dpp,
                             std::string_view marker);

  asio::awaitable<std::string> get_max_marker(const DoutPrefixProvider *dpp);

  asio::awaitable<bool> is_empty(const DoutPrefixProvider *dpp);

  void push(const DoutPrefixProvider *dpp,
            const rgw_bi_log_entry& entry,
            asio::yield_context y);

  void push(const DoutPrefixProvider *dpp,
            const std::vector<rgw_bi_log_entry>& entries,
            asio::yield_context y);

  std::tuple<std::vector<rgw_bi_log_entry>, std::string, bool>
  list(const DoutPrefixProvider *dpp, std::string marker, 
       uint32_t max_entries, asio::yield_context y);

  void trim(const DoutPrefixProvider *dpp,
            std::string_view marker, 
            asio::yield_context y);

  std::string get_max_marker(const DoutPrefixProvider *dpp,
                             asio::yield_context y);

  bool is_empty(const DoutPrefixProvider *dpp, asio::yield_context y);
  
  static std::string_view max_marker();
};

// batch update helper
class RGWBILogUpdateBatch {
  const DoutPrefixProvider *dpp;
  RGWBILogFIFO bilog_fifo;
  std::vector<rgw_bi_log_entry> entries;
  size_t max_batch_size;
  bool auto_flush;

public:
  RGWBILogUpdateBatch(const DoutPrefixProvider *dpp, 
                      neorados::RADOS r,
                      neorados::IOContext loc, 
                      const RGWBucketInfo& bucket_info,
                      size_t max_batch_size = 1,
                      bool auto_flush = true);

  void add_entry(const rgw_bi_log_entry& entry);

  void add_maybe_flush(const uint64_t olh_epoch, 
                       const ceph::real_time set_mtime,
                       const cls_rgw_bi_log_related_op& bi_log_client_info);

  void add_maybe_flush(const uint64_t olh_epoch,
                       const cls_rgw_obj_key& key,
                       const std::string& op_tag,
                       const bool delete_marker,
                       const rgw_bucket_olh_log_bi_log_entry& bi_log_replay_data);

  void add_maybe_flush(RGWModifyOp op,
                       const rgw_bucket_dir_entry& list_state,
                       rgw_zone_set zones_trace);

  asio::awaitable<void> flush_async();
 
  void flush();

  void flush(asio::yield_context y);

  // flush on destruction
  ~RGWBILogUpdateBatch();

  size_t size() const;

  bool empty() const;

  void set_auto_flush(bool enable);

  void set_max_batch_size(size_t size);

  RGWBILogFIFO& get_fifo();
};