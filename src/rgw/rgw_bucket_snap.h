// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#pragma once

#include <string>
#include <map>
#include "include/types.h"
#include "common/Formatter.h"
#include "common/ceph_time.h"

using rgw_bucket_snap_id = uint64_t;

struct rgw_bucket_snap {
  rgw_bucket_snap_id id;
  std::string name;
  std::string description;
  ceph::real_time creation_time;

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    encode(id, bl);
    encode(name, bl);
    encode(description, bl);
    encode(creation_time, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(id, bl);
    decode(name, bl);
    decode(description, bl);
    decode(creation_time, bl);
    DECODE_FINISH(bl);
  }

  void dump(Formatter *f) const;
};
WRITE_CLASS_ENCODER(rgw_bucket_snap)

class RGWBucketSnapMgr
{
  bool enabled = false;
  rgw_bucket_snap_id cur_snap;

  std::map<rgw_bucket_snap_id, rgw_bucket_snap> snaps;

public:
  RGWBucketSnapMgr() {}

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    encode(enabled, bl);
    encode(cur_snap, bl);
    encode(snaps, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(enabled, bl);
    decode(cur_snap, bl);
    decode(snaps, bl);
    DECODE_FINISH(bl);
  }

  void dump(Formatter *f) const;
  static void generate_test_instances(std::list<RGWBucketSnapMgr*>& o);

  rgw_bucket_snap_id get_cur_snap() const {
    return cur_snap;
  }
};
WRITE_CLASS_ENCODER(RGWBucketSnapMgr)
