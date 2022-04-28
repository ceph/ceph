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
public:
  RGWSI_MDLog(CephContext *cct) : RGWServiceInstance(cct) {}
  virtual ~RGWSI_MDLog() {}

  // traverse all the way back to the beginning of the period history, and
  // return a cursor to the first period in a fully attached history
  virtual RGWPeriodHistory::Cursor find_oldest_period(const DoutPrefixProvider *dpp, optional_yield y) = 0;

  /// initialize the oldest log period if it doesn't exist, and attach it to
  /// our current history
  virtual RGWPeriodHistory::Cursor init_oldest_log_period(optional_yield y, const DoutPrefixProvider *dpp) = 0;

  /// read the oldest log period, and return a cursor to it in our existing
  /// period history
  virtual RGWPeriodHistory::Cursor read_oldest_log_period(optional_yield y, const DoutPrefixProvider *dpp) const = 0;

  /// read the oldest log period asynchronously and write its result to the
  /// given cursor pointer
  virtual RGWCoroutine* read_oldest_log_period_cr(const DoutPrefixProvider *dpp, 
                                          RGWPeriodHistory::Cursor *period,
                                          RGWObjVersionTracker *objv) const = 0;

  /// try to advance the oldest log period when the given period is trimmed,
  /// using a rados lock to provide atomicity
  virtual RGWCoroutine* trim_log_period_cr(const DoutPrefixProvider *dpp, 
                                           RGWPeriodHistory::Cursor period,
                                           RGWObjVersionTracker *objv) const = 0;

  virtual int add_entry(const DoutPrefixProvider *dpp, const std::string& hash_key, const std::string& section, const std::string& key, bufferlist& bl) = 0;

  virtual int get_shard_id(const std::string& hash_key, int *shard_id) = 0;

  virtual RGWPeriodHistory *get_period_history() = 0;

  virtual int pull_period(const DoutPrefixProvider *dpp, const std::string& period_id, RGWPeriod& period, optional_yield y) = 0;

  /// find or create the metadata log for the given period
  virtual RGWMetadataLog* get_log(const std::string& period) = 0;
};

