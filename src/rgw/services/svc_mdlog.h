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

#include "rgw/rgw_rados.h"
#include "rgw/rgw_service.h"
#include "rgw/rgw_period_history.h"
#include "rgw/rgw_period_puller.h"

#include "svc_meta_be.h"


class RGWMetadataLog;
class RGWMetadataLogHistory;
class RGWCoroutine;

class RGWSI_Zone;
class RGWSI_SysObj;
class RGWSI_RADOS;

namespace mdlog {
  class ReadHistoryCR;
  class WriteHistoryCR;
}

class RGWSI_MDLog : public RGWServiceInstance
{
  friend class mdlog::ReadHistoryCR;
  friend class mdlog::WriteHistoryCR;

  // maintain a separate metadata log for each period
  std::map<std::string, RGWMetadataLog> md_logs;

  // use the current period's log for mutating operations
  RGWMetadataLog* current_log{nullptr};

  bool run_sync;

  // pulls missing periods for period_history
  std::unique_ptr<RGWPeriodPuller> period_puller;
  // maintains a connected history of periods
  std::unique_ptr<RGWPeriodHistory> period_history;

public:
  RGWSI_MDLog(CephContext *cct, boost::asio::io_context& ioctx, bool run_sync);
  virtual ~RGWSI_MDLog();

  RGWRados* rr;
  struct Svc {
    RGWAsyncRadosProcessor *async{nullptr};
    RGWSI_RADOS *rados{nullptr};
    RGWSI_Zone *zone{nullptr};
    RGWSI_SysObj *sysobj{nullptr};
    RGWSI_MDLog *mdlog{nullptr};
    RGWSI_Cls *cls{nullptr};
  } svc;

  boost::system::error_code init(
    RGWAsyncRadosProcessor *_async,
    RGWRados* _rr,
    RGWSI_RADOS *_rados_svc,
    RGWSI_Zone *_zone_svc,
    RGWSI_SysObj *_sysobj_svc,
    RGWSI_Cls *_cls_svc);

  boost::system::error_code do_start() override;

  // traverse all the way back to the beginning of the period history, and
  // return a cursor to the first period in a fully attached history
  RGWPeriodHistory::Cursor find_oldest_period();

  /// initialize the oldest log period if it doesn't exist, and attach it to
  /// our current history
  RGWPeriodHistory::Cursor init_oldest_log_period();

  /// read the oldest log period, and return a cursor to it in our existing
  /// period history
  RGWPeriodHistory::Cursor read_oldest_log_period() const;

  /// read the oldest log period asynchronously and write its result to the
  /// given cursor pointer
  RGWCoroutine* read_oldest_log_period_cr(RGWPeriodHistory::Cursor *period,
                                          RGWObjVersionTracker *objv) const;

  /// try to advance the oldest log period when the given period is trimmed,
  /// using a rados lock to provide atomicity
  RGWCoroutine* trim_log_period_cr(RGWPeriodHistory::Cursor period,
                                   RGWObjVersionTracker *objv) const;
  boost::system::error_code read_history(RGWMetadataLogHistory *state,
		   RGWObjVersionTracker *objv_tracker) const;

  boost::system::error_code write_history(const RGWMetadataLogHistory& state,
                    RGWObjVersionTracker *objv_tracker,
                    bool exclusive = false);

  boost::system::error_code add_entry(const string& hash_key, const string& section, const string& key, bufferlist& bl);

  int get_shard_id(const string& hash_key);

  RGWPeriodHistory *get_period_history() {
    return period_history.get();
  }

  boost::system::error_code pull_period(const std::string& period_id, RGWPeriod& period);

  /// find or create the metadata log for the given period
  RGWMetadataLog* get_log(const std::string& period);
};
