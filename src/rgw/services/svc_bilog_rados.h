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

#include "rgw/rgw_service.h"

#include "svc_rados.h"


class RGWSI_BILog_RADOS : public RGWServiceInstance
{
public:
  struct Svc {
    RGWSI_RADOS *rados{nullptr};
    RGWSI_BucketIndex_RADOS *bi{nullptr};
  } svc;

  RGWSI_BILog_RADOS(CephContext *cct, boost::asio::io_context& ioc);

  void init(RGWSI_RADOS *rados, RGWSI_BucketIndex_RADOS *bi_rados_svc);

  boost::system::error_code
  log_start(const RGWBucketInfo& bucket_info, int shard_id, optional_yield y);
  boost::system::error_code
  log_stop(const RGWBucketInfo& bucket_info, int shard_id, optional_yield y);

  boost::system::error_code log_trim(const RGWBucketInfo& bucket_info,
				     int shard_id,
				     std::string_view start_marker,
				     std::string_view end_marker,
				     optional_yield y);

  tl::expected<std::pair<std::vector<rgw_bi_log_entry>, bool>,
               boost::system::error_code>
  log_list(const RGWBucketInfo& bucket_info, int shard_id,
           std::string_view marker, uint32_t max, optional_yield y);

  tl::expected<boost::container::flat_map<int, std::string>,
               boost::system::error_code>
  get_log_status(const RGWBucketInfo& bucket_info, int shard_id,
                 optional_yield y);
};
