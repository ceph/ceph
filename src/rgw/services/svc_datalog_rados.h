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


class RGWDataChangesLog;
class RGWDataChangesLogInfo;
struct RGWDataChangesLogMarker;
struct rgw_data_change_log_entry;

namespace rgw {
  class BucketChangeObserver;
}

class RGWSI_DataLog_RADOS : public RGWServiceInstance
{
  std::unique_ptr<RGWDataChangesLog> log;
  R::RADOS* rados;

public:
  RGWSI_DataLog_RADOS(CephContext *cct);
  virtual ~RGWSI_DataLog_RADOS();

  struct Svc {
    RGWSI_Zone *zone{nullptr};
    RGWSI_Cls *cls{nullptr};
  } svc;

  int init(RGWSI_Zone *_zone_svc,
           RGWSI_Cls *_cls_svc,
	   R::RADOS* r);

  int do_start() override;
  void shutdown() override;

  RGWDataChangesLog *get_log() {
    return log.get();
  }

  void set_observer(rgw::BucketChangeObserver *observer);

  int get_log_shard_id(rgw_bucket& bucket, int shard_id);
  const std::string& get_oid(int shard_id) const;

  int get_info(int shard_id, RGWDataChangesLogInfo *info);

  int add_entry(const RGWBucketInfo& bucket_info, int shard_id);
  int list_entries(int shard, const real_time& start_time, const real_time& end_time, int max_entries,
		   list<rgw_data_change_log_entry>& entries,
		   const string& marker,
		   string *out_marker,
		   bool *truncated);
  int list_entries(const real_time& start_time, const real_time& end_time, int max_entries,
		   list<rgw_data_change_log_entry>& entries, RGWDataChangesLogMarker& marker, bool *ptruncated);
  int trim_entries(int shard_id, const real_time& start_time, const real_time& end_time,
                   const string& start_marker, const string& end_marker);
};

