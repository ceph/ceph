
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

#include <string>
#include <vector>
#include "include/encoding.h"
#include "common/ceph_json.h"
#include "common/ceph_time.h"
#include "cls/version/cls_version_types.h"

enum RGWMDLogSyncType {
  APPLY_ALWAYS,
  APPLY_UPDATES,
  APPLY_NEWER,
  APPLY_EXCLUSIVE
};

enum RGWMDLogStatus {
  MDLOG_STATUS_UNKNOWN,
  MDLOG_STATUS_WRITE,
  MDLOG_STATUS_SETATTRS,
  MDLOG_STATUS_REMOVE,
  MDLOG_STATUS_COMPLETE,
  MDLOG_STATUS_ABORT,
};

struct RGWMetadataLogInfo {
  std::string marker;
  real_time last_update;

  void dump(Formatter *f) const;
  void decode_json(JSONObj *obj);
};

struct RGWMetadataLogData {
  obj_version read_version;
  obj_version write_version;
  RGWMDLogStatus status;

  RGWMetadataLogData() : status(MDLOG_STATUS_UNKNOWN) {}

  void encode(bufferlist& bl) const;
  void decode(bufferlist::const_iterator& bl);
  void dump(Formatter *f) const;
  void decode_json(JSONObj *obj);
  static std::list<RGWMetadataLogData> generate_test_instances();
};
WRITE_CLASS_ENCODER(RGWMetadataLogData)


struct rgw_mdlog_info {
  uint32_t num_shards;
  std::string period; //< period id of the master's oldest metadata log
  epoch_t realm_epoch; //< realm epoch of oldest metadata log

  rgw_mdlog_info() : num_shards(0), realm_epoch(0) {}

  void decode_json(JSONObj *obj);
};

struct rgw_mdlog_entry {
  std::string id;
  std::string section;
  std::string name;
  ceph::real_time timestamp;
  RGWMetadataLogData log_data;

  void decode_json(JSONObj *obj);
};

struct rgw_mdlog_shard_data {
  std::string marker;
  bool truncated;
  std::vector<rgw_mdlog_entry> entries;

  void decode_json(JSONObj *obj);
};
