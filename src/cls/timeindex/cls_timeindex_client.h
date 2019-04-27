// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_CLS_TIMEINDEX_CLIENT_H
#define CEPH_CLS_TIMEINDEX_CLIENT_H

#include "include/rados/librados.hpp"

#include "cls_timeindex_ops.h"

/**
 * timeindex objclass
 */
class TimeindexListCtx : public librados::ObjectOperationCompletion {
  std::list<cls_timeindex_entry> *entries;
  std::string *marker;
  bool *truncated;

public:
  ///* ctor
  TimeindexListCtx(
    std::list<cls_timeindex_entry> *_entries,
    std::string *_marker,
    bool *_truncated)
    : entries(_entries), marker(_marker), truncated(_truncated) {}

  ///* dtor
  ~TimeindexListCtx() {}

  void handle_completion(int r, bufferlist& bl) override {
    if (r >= 0) {
      cls_timeindex_list_ret ret;
      try {
        auto iter = bl.cbegin();
        decode(ret, iter);
        if (entries)
          *entries = ret.entries;
        if (truncated)
          *truncated = ret.truncated;
        if (marker)
          *marker = ret.marker;
      } catch (buffer::error& err) {
        // nothing we can do about it atm
      }
    }
  }
};

void cls_timeindex_add_prepare_entry(
  cls_timeindex_entry& entry,
  const utime_t& key_timestamp,
  const std::string& key_ext,
  bufferlist& bl);

void cls_timeindex_add(
  librados::ObjectWriteOperation& op,
  const std::list<cls_timeindex_entry>& entry);

void cls_timeindex_add(
  librados::ObjectWriteOperation& op,
  const cls_timeindex_entry& entry);

void cls_timeindex_add(
  librados::ObjectWriteOperation& op,
  const utime_t& timestamp,
  const std::string& name,
  const bufferlist& bl);

void cls_timeindex_list(
  librados::ObjectReadOperation& op,
  const utime_t& from,
  const utime_t& to,
  const std::string& in_marker,
  const int max_entries,
  std::list<cls_timeindex_entry>& entries,
  std::string *out_marker,
  bool *truncated);

void cls_timeindex_trim(
  librados::ObjectWriteOperation& op,
  const utime_t& from_time,
  const utime_t& to_time,
  const std::string& from_marker = std::string(),
  const std::string& to_marker = std::string());

int cls_timeindex_trim(
  librados::IoCtx& io_ctx,
  const std::string& oid,
  const utime_t& from_time,
  const utime_t& to_time,
  const std::string& from_marker = std::string(),
  const std::string& to_marker = std::string());
#endif
