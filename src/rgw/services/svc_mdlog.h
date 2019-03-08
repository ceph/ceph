// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

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

#include "common/static_ptr.h"

#include "rgw/rgw_service.h"
#include "rgw/rgw_period_history.h"
#include "rgw/rgw_period_puller.h"

#include "svc_meta_be.h"


class RGWMetadataLog;
class RGWMetadataLogHistory;
class RGWCoroutine;

class RGWSI_Zone;
class RGWSI_SysObj;


class RGWSI_MDLog : public RGWServiceInstance
{
  RGWSI_Zone *zone_svc{nullptr};
  RGWSI_SysObj *sysobj_svc{nullptr};

  // maintain a separate metadata log for each period
  std::map<std::string, RGWMetadataLog> md_logs;

  // use the current period's log for mutating operations
  RGWMetadataLog* current_log{nullptr};

  /// find or create the metadata log for the given period
  RGWMetadataLog* get_log(const std::string& period);

  // pulls missing periods for period_history
  std::unique_ptr<RGWPeriodPuller> period_puller;
  // maintains a connected history of periods
  std::unique_ptr<RGWPeriodHistory> period_history;

public:
  RGWSI_MDLog(CephContext *cct) : RGWServiceInstance(cct) {}
  virtual ~RGWSI_MDLog() {}

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

  int init(RGWSI_Zone *_zone_svc, RGWSI_SysObj *_sysobj_svc,
           const std::string& current_period);

  int read_history(RGWMetadataLogHistory *state, RGWObjVersionTracker *objv_tracker);
  int write_history(const RGWMetadataLogHistory& state,
                    RGWObjVersionTracker *objv_tracker,
                    bool exclusive = false);

  int add_entry(RGWSI_MetaBackend::Module *module, const string& section, const string& key, bufferlist& bl);
};

