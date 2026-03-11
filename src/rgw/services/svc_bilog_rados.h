// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

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

#include <map>
#include <optional>
#include <string>
#include <string_view>

#include "include/neorados/RADOS.hpp"
#include "driver/rados/rgw_service.h"
#include "rgw_bucket_layout.h"

class RGWSI_BILog_RADOS : public RGWServiceInstance
{
protected:
  std::optional<neorados::RADOS> rados_neo;

public:

  explicit RGWSI_BILog_RADOS(CephContext *cct) : RGWServiceInstance(cct) {}
  virtual ~RGWSI_BILog_RADOS() = default;

  neorados::RADOS& get_rados_neo() { return *rados_neo; }

  virtual void init(RGWSI_BucketIndex_RADOS *bi_rados_svc,
                    neorados::RADOS rados_neo) = 0;

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
                       std::string_view start_marker,
                       std::string_view end_marker) = 0;
  virtual int log_list(const DoutPrefixProvider *dpp, optional_yield y,
                       const RGWBucketInfo& bucket_info,
                       const rgw::bucket_log_layout_generation& log_layout,
                       int shard_id,
                       std::string& marker,
                       uint32_t max,
                       std::list<rgw_bi_log_entry>& result,
                       bool *truncated) = 0;
  virtual int get_log_status(const DoutPrefixProvider *dpp,
                             const RGWBucketInfo& bucket_info,
                             const rgw::bucket_log_layout_generation& log_layout,
                             int shard_id,
                             std::map<int, std::string> *markers,
                             optional_yield y) = 0;
};

// In-index backend.
class RGWSI_BILog_RADOS_InIndex : public RGWSI_BILog_RADOS
{
  struct Svc {
    RGWSI_BucketIndex_RADOS *bi{nullptr};
  } svc;

public:
  using RGWSI_BILog_RADOS::RGWSI_BILog_RADOS;

  void init(RGWSI_BucketIndex_RADOS *bi_rados_svc,
            neorados::RADOS rados_neo) override;

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
               std::string_view start_marker,
               std::string_view end_marker) override;
  int log_list(const DoutPrefixProvider *dpp, optional_yield y,
               const RGWBucketInfo& bucket_info,
               const rgw::bucket_log_layout_generation& log_layout,
               int shard_id,
               std::string& marker,
               uint32_t max,
               std::list<rgw_bi_log_entry>& result,
               bool *truncated) override;
  int get_log_status(const DoutPrefixProvider *dpp,
                     const RGWBucketInfo& bucket_info,
                     const rgw::bucket_log_layout_generation& log_layout,
                     int shard_id,
                     std::map<int, std::string> *markers,
                     optional_yield y) override;
};

// FIFO (cls_fifo) backend.
class RGWSI_BILog_RADOS_FIFO : public RGWSI_BILog_RADOS
{
  RGWSI_BucketIndex_RADOS *bi_{nullptr};
public:
  using RGWSI_BILog_RADOS::RGWSI_BILog_RADOS;

  void init(RGWSI_BucketIndex_RADOS *bi_rados_svc,
            neorados::RADOS rados_neo) override;

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
               std::string_view start_marker,
               std::string_view end_marker) override;
  int log_list(const DoutPrefixProvider *dpp, optional_yield y,
               const RGWBucketInfo& bucket_info,
               const rgw::bucket_log_layout_generation& log_layout,
               int shard_id,
               std::string& marker,
               uint32_t max,
               std::list<rgw_bi_log_entry>& result,
               bool *truncated) override;
  int get_log_status(const DoutPrefixProvider *dpp,
                     const RGWBucketInfo& bucket_info,
                     const rgw::bucket_log_layout_generation& log_layout,
                     int shard_id,
                     std::map<int, std::string> *markers,
                     optional_yield y) override;
};

// BackendDispatcher: responsibility is to route calls to the correct
// concrete backend based on log_layout.layout.type
class RGWSI_BILog_RADOS_BackendDispatcher : public RGWSI_BILog_RADOS
{
  RGWSI_BILog_RADOS_InIndex backend_inindex;
  RGWSI_BILog_RADOS_FIFO   backend_fifo;

  RGWSI_BILog_RADOS& get_backend(
      const rgw::bucket_log_layout_generation& log_layout);

public:
  explicit RGWSI_BILog_RADOS_BackendDispatcher(CephContext *cct);

  void init(RGWSI_BucketIndex_RADOS *bi_rados_svc,
            neorados::RADOS rados_neo) override;

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
               std::string_view start_marker,
               std::string_view end_marker) override;
  int log_list(const DoutPrefixProvider *dpp, optional_yield y,
               const RGWBucketInfo& bucket_info,
               const rgw::bucket_log_layout_generation& log_layout,
               int shard_id,
               std::string& marker,
               uint32_t max,
               std::list<rgw_bi_log_entry>& result,
               bool *truncated) override;
  int get_log_status(const DoutPrefixProvider *dpp,
                     const RGWBucketInfo& bucket_info,
                     const rgw::bucket_log_layout_generation& log_layout,
                     int shard_id,
                     std::map<int, std::string> *markers,
                     optional_yield y) override;
};
