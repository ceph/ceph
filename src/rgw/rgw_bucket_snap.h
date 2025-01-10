// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#pragma once

#include <string>
#include <map>
#include "include/types.h"
#include "common/Formatter.h"
#include "common/ceph_time.h"

#include "rgw_bucket_snap_types.h"


class RGWBucketSnapMgr
{
  bool enabled = false;
  rgw_bucket_snap_id cur_snap = RGW_BUCKET_SNAP_NOSNAP;

  std::map<rgw_bucket_snap_id, rgw_bucket_snap> snaps;

  std::map<std::string, rgw_bucket_snap_id> names_to_ids;

public:
  RGWBucketSnapMgr();

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    encode(enabled, bl);
    encode(cur_snap, bl);
    encode(snaps, bl);
    encode(names_to_ids, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(enabled, bl);
    decode(cur_snap, bl);
    decode(snaps, bl);
    decode(names_to_ids, bl);
    DECODE_FINISH(bl);
  }

  void dump(Formatter *f) const;

  rgw_bucket_snap_id get_cur_snap_id() const {
    return cur_snap;
  }

  int create_snap(const rgw_bucket_snap_info& info);

  const std::map<rgw_bucket_snap_id, rgw_bucket_snap>& get_snaps() const {
    return snaps;
  }

  bool is_enabled() const {
    return enabled;
  }

  void set_enabled(bool flag) {
    enabled = flag;

    if (enabled && cur_snap == RGW_BUCKET_SNAP_NOSNAP) {
      cur_snap = RGW_BUCKET_SNAP_START;
    }
  }
};
WRITE_CLASS_ENCODER(RGWBucketSnapMgr)
