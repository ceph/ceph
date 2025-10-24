// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2019 Red Hat, Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 *
 */


#pragma once

#include "rgw_service.h"
#include "svc_bilog_rados.h"
#include "rgw_log_backing.h"
#include "rgw_asio_thread.h"
#include "rgw_bilog.h"
#include "neorados/cls/fifo.h"
#include <boost/asio/awaitable.hpp>
#include "common/async/yield_context.h"
#include "common/async/blocked_completion.h"

class RGWSI_BILog_RADOS : public RGWServiceInstance
{

  // helper function to handle optional_yield blocking/nonblocking
  template<typename Func>
  auto with_optional_yield(const DoutPrefixProvider *dpp, optional_yield y, Func&& sync_func)
      -> decltype(sync_func(y.get_yield_context()))
  {
    if (y) {
      return sync_func(y.get_yield_context());
    } else {
      maybe_warn_about_blocking(dpp);
      return sync_func(ceph::async::use_blocked);
    }
  }

public:
  virtual ~RGWSI_BILog_RADOS() {}

  RGWSI_BILog_RADOS(CephContext *cct);

  virtual void init(RGWSI_BucketIndex_RADOS *bi_rados_svc) = 0;

  virtual int log_start(const DoutPrefixProvider *dpp, optional_yield y,
                const RGWBucketInfo& bucket_info,
                const rgw::bucket_log_layout_generation& log_layout,
                int shard_id) = 0;
  virtual int log_stop(const DoutPrefixProvider *dpp, optional_yield y,
               const RGWBucketInfo& bucket_info,
               const rgw::bucket_log_layout_generation& log_layout,
               int shard_id) = 0;

  virtual int log_trim(const DoutPrefixProvider *dpp, optional_yield y,
               const RGWBucketInfo& bucket_info,
               const rgw::bucket_log_layout_generation& log_layout,
               int shard_id,
               std::string_view marker) = 0;
  virtual int log_list(const DoutPrefixProvider *dpp, optional_yield y,
               const RGWBucketInfo& bucket_info,
               const rgw::bucket_log_layout_generation& log_layout,
               int shard_id,
               std::string& marker,
               uint32_t max,
               std::list<rgw_bi_log_entry>& result,
               bool *truncated) = 0;

  virtual int log_get_max_marker(const DoutPrefixProvider *dpp,
                     const RGWBucketInfo& bucket_info,
                     const std::map<int, rgw_bucket_dir_header>& headers,
                     const int shard_id,
                     std::map<int, std::string> *max_markers,
                     optional_yield y) = 0;

  virtual int log_get_max_marker(const DoutPrefixProvider *dpp,
                     const RGWBucketInfo& bucket_info,
                     const std::map<int, rgw_bucket_dir_header>& headers,
                     const int shard_id,
                     std::string *max_marker,
                     optional_yield y) = 0;
};

class RGWSI_BILog_RADOS_InIndex : public RGWSI_BILog_RADOS
{
public:
  struct Svc {
    RGWSI_BucketIndex_RADOS *bi{nullptr};
  } svc;

  using RGWSI_BILog_RADOS::RGWSI_BILog_RADOS;

  void init(RGWSI_BucketIndex_RADOS *bi_rados_svc) override;

  int log_start(const DoutPrefixProvider *dpp, optional_yield y,
                const RGWBucketInfo& bucket_info,
                const rgw::bucket_log_layout_generation& log_layout,
                int shard_id) override;
  int log_stop(const DoutPrefixProvider *dpp, optional_yield y,
               const RGWBucketInfo& bucket_info,
               const rgw::bucket_log_layout_generation& log_layout,
               int shard_id) override;

  int log_trim(const DoutPrefixProvider *dpp, optional_yield y,
               const RGWBucketInfo& bucket_info,
               const rgw::bucket_log_layout_generation& log_layout,
               int shard_id,
               std::string_view marker) override;
  int log_list(const DoutPrefixProvider *dpp, optional_yield y,
               const RGWBucketInfo& bucket_info,
               const rgw::bucket_log_layout_generation& log_layout,
               int shard_id,
               std::string& marker,
               uint32_t max,
               std::list<rgw_bi_log_entry>& result,
               bool *truncated) override;

  int log_get_max_marker(const DoutPrefixProvider *dpp,
                     const RGWBucketInfo& bucket_info,
                     const std::map<int, rgw_bucket_dir_header>& headers,
                     const int shard_id,
                     std::map<int, std::string> *max_markers,
                     optional_yield y) override;

int log_get_max_marker(const DoutPrefixProvider *dpp,
                     const RGWBucketInfo& bucket_info,
                     const std::map<int, rgw_bucket_dir_header>& headers,
                     const int shard_id,
                     std::string *max_marker,
                     optional_yield y) override;
};

// RGWSI_BILog_RADOS_FIFO -- the reader part of the cls_fifo-based backend
// for BIlog.
//
// Responsibilities:
//   * reading and trimming entries,
//   * discovery of `max_marker` (imporant for our incremental sync feature),
//   * managing the logging state (on/off).

class RGWSI_BILog_RADOS_FIFO : public RGWSI_BILog_RADOS
{
  neorados::RADOS rados;
  struct {
    RGWSI_BucketIndex_RADOS *bi{nullptr};
  } svc;

  // helper method to create FIFO wrapprt
  std::unique_ptr<RGWBILogFIFO> create_bilog_fifo(const DoutPrefixProvider *dpp, 
                                                   const RGWBucketInfo& bucket_info) const;

public:
  RGWSI_BILog_RADOS_FIFO(CephContext *cct, neorados::RADOS r) : RGWSI_BILog_RADOS(cct), rados(std::move(r)) {}

  void init(RGWSI_BucketIndex_RADOS *bi_rados_svc) override;

  int log_start(const DoutPrefixProvider *dpp, optional_yield y,
                const RGWBucketInfo& bucket_info,
                const rgw::bucket_log_layout_generation& log_layout,
                int shard_id) override;

  int log_stop(const DoutPrefixProvider *dpp, optional_yield y,
               const RGWBucketInfo& bucket_info,
               const rgw::bucket_log_layout_generation& log_layout,
               int shard_id) override;

  int log_trim(const DoutPrefixProvider *dpp, optional_yield y,
               const RGWBucketInfo& bucket_info,
               const rgw::bucket_log_layout_generation& log_layout,
               int shard_id,
               std::string_view marker) override;

  int log_list(const DoutPrefixProvider *dpp, optional_yield y,
               const RGWBucketInfo& bucket_info,
               const rgw::bucket_log_layout_generation& log_layout,
               int shard_id,
               std::string& marker,
               uint32_t max,
               std::list<rgw_bi_log_entry>& result,
               bool *truncated) override;

  int log_get_max_marker(const DoutPrefixProvider *dpp,
                         const RGWBucketInfo& bucket_info,
                         const std::map<int, rgw_bucket_dir_header>& headers,
                         const int shard_id,
                         std::string *max_marker,
                         optional_yield y) override;
  
  int log_get_max_marker(const DoutPrefixProvider *dpp,
                         const RGWBucketInfo& bucket_info,
                         const std::map<int, rgw_bucket_dir_header>& headers,
                         const int shard_id,
                         std::map<int, std::string> *max_markers,
                         optional_yield y) override;

  asio::awaitable<void>log_start(const DoutPrefixProvider *dpp,
                                 const RGWBucketInfo& bucket_info,
                                 const rgw::bucket_log_layout_generation& log_layout,
                                 int shard_id);

  asio::awaitable<void> log_stop(const DoutPrefixProvider *dpp,
                                 const RGWBucketInfo& bucket_info,
                                 const rgw::bucket_log_layout_generation& log_layout,
                                 int shard_id);

  asio::awaitable<void> log_trim(const DoutPrefixProvider *dpp,
                                 const RGWBucketInfo& bucket_info,
                                 const rgw::bucket_log_layout_generation& log_layout,
                                 int shard_id, std::string_view marker);

  asio::awaitable<std::tuple<std::vector<rgw_bi_log_entry>, std::string, bool>>
  log_list(const DoutPrefixProvider *dpp,
           const RGWBucketInfo& bucket_info,
           const rgw::bucket_log_layout_generation& log_layout,
           int shard_id, std::string marker, uint32_t max_entries);

  asio::awaitable<std::string>
  log_get_max_marker(const DoutPrefixProvider *dpp,
                     const RGWBucketInfo& bucket_info,
                     const std::map<int, rgw_bucket_dir_header>& headers,
                     int shard_id);
};



// BackendDispatcher has a single responsibility: redirect the calls
// to concrete implementation of the `RGWSI_BILog_RADOS` interface
// (at the time of writing we have InIndex and CLSFIFO) depending on
// BILog layout decription which should be available as a part of
// the RGWBucketInfo.
//
// It's worth to commment on the life-time of `RGWSI_BILog_RADOS`
// instances. This service is created early, around the initialization
// of `RGWRados`; single instance handles many requests.
class RGWSI_BILog_RADOS_BackendDispatcher : public RGWSI_BILog_RADOS
{
  RGWSI_BILog_RADOS_InIndex backend_inindex;
  RGWSI_BILog_RADOS_FIFO backend_fifo;
  RGWSI_BILog_RADOS& get_backend(const RGWBucketInfo& bucket_info);

public:
  RGWSI_BILog_RADOS_BackendDispatcher(CephContext* cct, neorados::RADOS rados);

  void init(RGWSI_BucketIndex_RADOS *bi_rados_svc) override;

  int log_start(const DoutPrefixProvider *dpp, optional_yield y,
                const RGWBucketInfo& bucket_info,
                const rgw::bucket_log_layout_generation& log_layout,
                int shard_id) override {
    return get_backend(bucket_info).log_start(dpp, y, bucket_info,
                                              log_layout, shard_id);
  }
  int log_stop(const DoutPrefixProvider *dpp, optional_yield y,
               const RGWBucketInfo& bucket_info,
               const rgw::bucket_log_layout_generation& log_layout,
               int shard_id) override {
    return get_backend(bucket_info).log_stop(dpp, y, bucket_info,
                                             log_layout, shard_id);
  }

  int log_trim(const DoutPrefixProvider *dpp, optional_yield y,
               const RGWBucketInfo& bucket_info,
               const rgw::bucket_log_layout_generation& log_layout,
               int shard_id,
               std::string_view marker) override {
    return get_backend(bucket_info).log_trim(dpp, y, bucket_info,
                                             log_layout, shard_id,
                                             marker);
  }
  int log_list(const DoutPrefixProvider *dpp, optional_yield y,
               const RGWBucketInfo& bucket_info,
               const rgw::bucket_log_layout_generation& log_layout,
               int shard_id,
               std::string& marker,
               uint32_t max,
               std::list<rgw_bi_log_entry>& result,
               bool *truncated) override {
    return get_backend(bucket_info).log_list(dpp, y, bucket_info,
                                             log_layout, shard_id,
                                             marker, max, result,
                                             truncated);
  }

  int log_get_max_marker(const DoutPrefixProvider *dpp,
                         const RGWBucketInfo& bucket_info,
                         const std::map<int, rgw_bucket_dir_header>& headers,
                         const int shard_id,
                         std::map<int, std::string>* max_markers,
                         optional_yield y) override {
    return get_backend(bucket_info).log_get_max_marker(dpp, bucket_info,
                                                       headers,
                                                       shard_id,
                                                       max_markers, y);
  }

  int log_get_max_marker(const DoutPrefixProvider *dpp,
                       const RGWBucketInfo& bucket_info,
                       const std::map<int, rgw_bucket_dir_header>& headers,
                       const int shard_id,
                       std::string *max_marker,
                       optional_yield y) override {
    return get_backend(bucket_info).log_get_max_marker(dpp, bucket_info,
                                                       headers,
                                                       shard_id,
                                                       max_marker, y);
  }
};
