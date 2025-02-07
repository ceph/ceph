// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2013 Inktank, Inc
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

/* N.B., this header defines fundamental serialized types.  Do not
 * introduce changes or include files which can only be compiled in
 * radosgw or OSD contexts (e.g., rgw_sal.h, rgw_common.h)
 */

#pragma once

static inline int64_t rgw_rounded_kb(int64_t bytes)
{
  return (bytes + 1023) / 1024;
}

class JSONObj;

struct RGWQuotaInfo {
  int64_t max_size;
  int64_t max_objects;
  bool enabled;
  /* Do we want to compare with raw, not rounded RGWStorageStats::size (true)
   * or maybe rounded-to-4KiB RGWStorageStats::size_rounded (false)? */
  bool check_on_raw;

  RGWQuotaInfo()
    : max_size(-1),
      max_objects(-1),
      enabled(false),
      check_on_raw(false) {
  }

  void encode(bufferlist& bl) const {
    ENCODE_START(3, 1, bl);
    if (max_size < 0) {
      encode(-rgw_rounded_kb(abs(max_size)), bl);
    } else {
      encode(rgw_rounded_kb(max_size), bl);
    }
    encode(max_objects, bl);
    encode(enabled, bl);
    encode(max_size, bl);
    encode(check_on_raw, bl);
    ENCODE_FINISH(bl);
  }
  void decode(bufferlist::const_iterator& bl) {
    DECODE_START_LEGACY_COMPAT_LEN(3, 1, 1, bl);
    int64_t max_size_kb;
    decode(max_size_kb, bl);
    decode(max_objects, bl);
    decode(enabled, bl);
    if (struct_v < 2) {
      max_size = max_size_kb * 1024;
    } else {
      decode(max_size, bl);
    }
    if (struct_v >= 3) {
      decode(check_on_raw, bl);
    }
    DECODE_FINISH(bl);
  }

  void dump(Formatter *f) const;
  static void generate_test_instances(std::list<RGWQuotaInfo*>& o);
  void decode_json(JSONObj *obj);

};
WRITE_CLASS_ENCODER(RGWQuotaInfo)

struct RGWQuota {
    RGWQuotaInfo user_quota;
    RGWQuotaInfo bucket_quota;
};
