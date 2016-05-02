// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
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

#ifndef CEPH_RGW_QUOTA_H
#define CEPH_RGW_QUOTA_H


#include "include/utime.h"
#include "include/atomic.h"
#include "common/lru_map.h"

class RGWRados;
class JSONObj;

struct RGWQuotaInfo {
  template<class T> friend class RGWQuotaCache;
protected:
  /* The quota thresholds after which comparing against cached storage stats
   * is disallowed. Those fields may be accessed only by the RGWQuotaCache.
   * They are not intended as tunables but rather as a mean to store results
   * of repeating calculations in the quota cache subsystem. */
  int64_t max_size_soft_threshold;
  int64_t max_objs_soft_threshold;

public:
  int64_t max_size_kb;
  int64_t max_objects;
  bool enabled;

  RGWQuotaInfo()
    : max_size_soft_threshold(-1),
      max_objs_soft_threshold(-1),
      max_size_kb(-1),
      max_objects(-1),
      enabled(false) {
  }

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    ::encode(max_size_kb, bl);
    ::encode(max_objects, bl);
    ::encode(enabled, bl);
    ENCODE_FINISH(bl);
  }
  void decode(bufferlist::iterator& bl) {
    DECODE_START(1, bl);
    ::decode(max_size_kb, bl);
    ::decode(max_objects, bl);
    ::decode(enabled, bl);
    DECODE_FINISH(bl);
  }

  void dump(Formatter *f) const;

  void decode_json(JSONObj *obj);

};
WRITE_CLASS_ENCODER(RGWQuotaInfo)

struct rgw_bucket;

class RGWQuotaHandler {
public:
  RGWQuotaHandler() {}
  virtual ~RGWQuotaHandler() {
  }
  virtual int check_quota(const rgw_user& bucket_owner, rgw_bucket& bucket,
                          RGWQuotaInfo& user_quota, RGWQuotaInfo& bucket_quota,
			  uint64_t num_objs, uint64_t size) = 0;

  virtual void update_stats(const rgw_user& bucket_owner, rgw_bucket& bucket, int obj_delta, uint64_t added_bytes, uint64_t removed_bytes) = 0;

  static RGWQuotaHandler *generate_handler(RGWRados *store, bool quota_threads);
  static void free_handler(RGWQuotaHandler *handler);
};

#endif
