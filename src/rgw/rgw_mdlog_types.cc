
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

#include "rgw_mdlog_types.h"
#include "include/utime.h"

void RGWMetadataLogInfo::dump(Formatter *f) const
{
  encode_json("marker", marker, f);
  utime_t ut(last_update);
  encode_json("last_update", ut, f);
}

void RGWMetadataLogInfo::decode_json(JSONObj *obj)
{
  JSONDecoder::decode_json("marker", marker, obj);
  utime_t ut;
  JSONDecoder::decode_json("last_update", ut, obj);
  last_update = ut.to_real_time();
}

struct LogStatusDump {
  RGWMDLogStatus status;

  explicit LogStatusDump(RGWMDLogStatus _status) : status(_status) {}
  void dump(Formatter *f) const;
};

void LogStatusDump::dump(Formatter *f) const {
  std::string_view s;
  switch (status) {
    case MDLOG_STATUS_WRITE:
      s = "write";
      break;
    case MDLOG_STATUS_SETATTRS:
      s = "set_attrs";
      break;
    case MDLOG_STATUS_REMOVE:
      s = "remove";
      break;
    case MDLOG_STATUS_COMPLETE:
      s = "complete";
      break;
    case MDLOG_STATUS_ABORT:
      s = "abort";
      break;
    default:
      s = "unknown";
      break;
  }
  encode_json("status", s, f);
}

void encode_json(const char *name, const obj_version& v, Formatter *f)
{
  f->open_object_section(name);
  f->dump_string("tag", v.tag);
  f->dump_unsigned("ver", v.ver);
  f->close_section();
}

void decode_json_obj(obj_version& v, JSONObj *obj)
{
  JSONDecoder::decode_json("tag", v.tag, obj);
  JSONDecoder::decode_json("ver", v.ver, obj);
}

void RGWMetadataLogData::encode(bufferlist& bl) const {
  ENCODE_START(1, 1, bl);
  encode(read_version, bl);
  encode(write_version, bl);
  uint32_t s = (uint32_t)status;
  encode(s, bl);
  ENCODE_FINISH(bl);
}

void RGWMetadataLogData::decode(bufferlist::const_iterator& bl) {
   DECODE_START(1, bl);
   decode(read_version, bl);
   decode(write_version, bl);
   uint32_t s;
   decode(s, bl);
   status = (RGWMDLogStatus)s;
   DECODE_FINISH(bl);
}

void RGWMetadataLogData::dump(Formatter *f) const {
  encode_json("read_version", read_version, f);
  encode_json("write_version", write_version, f);
  encode_json("status", LogStatusDump(status), f);
}

void decode_json_obj(RGWMDLogStatus& status, JSONObj *obj) {
  std::string s;
  JSONDecoder::decode_json("status", s, obj);
  if (s == "complete") {
    status = MDLOG_STATUS_COMPLETE;
  } else if (s == "write") {
    status = MDLOG_STATUS_WRITE;
  } else if (s == "remove") {
    status = MDLOG_STATUS_REMOVE;
  } else if (s == "set_attrs") {
    status = MDLOG_STATUS_SETATTRS;
  } else if (s == "abort") {
    status = MDLOG_STATUS_ABORT;
  } else {
    status = MDLOG_STATUS_UNKNOWN;
  }
}

void RGWMetadataLogData::decode_json(JSONObj *obj) {
  JSONDecoder::decode_json("read_version", read_version, obj);
  JSONDecoder::decode_json("write_version", write_version, obj);
  JSONDecoder::decode_json("status", status, obj);
}

std::list<RGWMetadataLogData> RGWMetadataLogData::generate_test_instances() {
  std::list<RGWMetadataLogData> l;
  l.emplace_back();
  l.emplace_back();
  l.back().read_version = obj_version();
  l.back().read_version.tag = "read_tag";
  l.back().write_version = obj_version();
  l.back().write_version.tag = "write_tag";
  l.back().status = MDLOG_STATUS_WRITE;
  return l;
}

void rgw_mdlog_info::decode_json(JSONObj *obj) {
  JSONDecoder::decode_json("num_objects", num_shards, obj);
  JSONDecoder::decode_json("period", period, obj);
  JSONDecoder::decode_json("realm_epoch", realm_epoch, obj);
}

void rgw_mdlog_entry::decode_json(JSONObj *obj) {
  JSONDecoder::decode_json("id", id, obj);
  JSONDecoder::decode_json("section", section, obj);
  JSONDecoder::decode_json("name", name, obj);
  utime_t ut;
  JSONDecoder::decode_json("timestamp", ut, obj);
  timestamp = ut.to_real_time();
  JSONDecoder::decode_json("data", log_data, obj);
}

void rgw_mdlog_shard_data::decode_json(JSONObj *obj) {
  JSONDecoder::decode_json("marker", marker, obj);
  JSONDecoder::decode_json("truncated", truncated, obj);
  JSONDecoder::decode_json("entries", entries, obj);
};

