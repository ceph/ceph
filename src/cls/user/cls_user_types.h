// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_CLS_USER_TYPES_H
#define CEPH_CLS_USER_TYPES_H

#include "include/encoding.h"
#include "include/types.h"
#include "include/utime.h"
#include "common/ceph_time.h"

/*
 * this needs to be compatible with rgw_bucket, as it replaces it
 */
struct cls_user_bucket {
  std::string name;
  std::string marker;
  std::string bucket_id;
  std::string placement_id;
  struct {
    std::string data_pool;
    std::string index_pool;
    std::string data_extra_pool;
  } explicit_placement;

  void encode(ceph::buffer::list& bl) const {
    /* since new version of this structure is not backward compatible,
     * we have older rgw running against newer osd if we encode it
     * in the new way. Only encode newer version if placement_id is
     * not empty, otherwise keep handling it as before
     */
    if (!placement_id.empty()) {
      ENCODE_START(9, 8, bl);
      encode(name, bl);
      encode(marker, bl);
      encode(bucket_id, bl);
      encode(placement_id, bl);
      ENCODE_FINISH(bl);
    } else {
      ENCODE_START(7, 3, bl);
      encode(name, bl);
      encode(explicit_placement.data_pool, bl);
      encode(marker, bl);
      encode(bucket_id, bl);
      encode(explicit_placement.index_pool, bl);
      encode(explicit_placement.data_extra_pool, bl);
      ENCODE_FINISH(bl);
    }
  }
  void decode(ceph::buffer::list::const_iterator& bl) {
    DECODE_START_LEGACY_COMPAT_LEN(8, 3, 3, bl);
    decode(name, bl);
    if (struct_v < 8) {
      decode(explicit_placement.data_pool, bl);
    }
    if (struct_v >= 2) {
      decode(marker, bl);
      if (struct_v <= 3) {
        uint64_t id;
        decode(id, bl);
        char buf[16];
        snprintf(buf, sizeof(buf), "%llu", (long long)id);
        bucket_id = buf;
      } else {
        decode(bucket_id, bl);
      }
    }
    if (struct_v < 8) {
      if (struct_v >= 5) {
        decode(explicit_placement.index_pool, bl);
      } else {
        explicit_placement.index_pool = explicit_placement.data_pool;
      }
      if (struct_v >= 7) {
        decode(explicit_placement.data_extra_pool, bl);
      }
    } else {
      decode(placement_id, bl);
      if (struct_v == 8 && placement_id.empty()) {
        decode(explicit_placement.data_pool, bl);
        decode(explicit_placement.index_pool, bl);
        decode(explicit_placement.data_extra_pool, bl);
      }
    }
    DECODE_FINISH(bl);
  }

  bool operator<(const cls_user_bucket& b) const {
    return name.compare(b.name) < 0;
  }

  void dump(ceph::Formatter *f) const;
  static void generate_test_instances(std::list<cls_user_bucket*>& ls);
};
WRITE_CLASS_ENCODER(cls_user_bucket)

/*
 * this structure overrides RGWBucketEnt
 */
struct cls_user_bucket_entry {
  cls_user_bucket bucket;
  size_t size;
  size_t size_rounded;
  ceph::real_time creation_time;
  uint64_t count;
  bool user_stats_sync;

  cls_user_bucket_entry() : size(0), size_rounded(0), count(0), user_stats_sync(false) {}

  void encode(ceph::buffer::list& bl) const {
    ENCODE_START(9, 5, bl);
    uint64_t s = size;
    __u32 mt = ceph::real_clock::to_time_t(creation_time);
    std::string empty_str;  // originally had the bucket name here, but we encode bucket later
    encode(empty_str, bl);
    encode(s, bl);
    encode(mt, bl);
    encode(count, bl);
    encode(bucket, bl);
    s = size_rounded;
    encode(s, bl);
    encode(user_stats_sync, bl);
    encode(creation_time, bl);
    //::encode(placement_rule, bl); removed in v9
    ENCODE_FINISH(bl);
  }
  void decode(ceph::buffer::list::const_iterator& bl) {
    DECODE_START_LEGACY_COMPAT_LEN(9, 5, 5, bl);
    __u32 mt;
    uint64_t s;
    std::string empty_str;  // backward compatibility
    decode(empty_str, bl);
    decode(s, bl);
    decode(mt, bl);
    size = s;
    if (struct_v < 7) {
      creation_time = ceph::real_clock::from_time_t(mt);
    }
    if (struct_v >= 2)
      decode(count, bl);
    if (struct_v >= 3)
      decode(bucket, bl);
    if (struct_v >= 4)
      decode(s, bl);
    size_rounded = s;
    if (struct_v >= 6)
      decode(user_stats_sync, bl);
    if (struct_v >= 7)
      decode(creation_time, bl);
    if (struct_v == 8) { // added in v8, removed in v9
      std::string placement_rule;
      decode(placement_rule, bl);
    }
    DECODE_FINISH(bl);
  }
  void dump(ceph::Formatter *f) const;
  static void generate_test_instances(std::list<cls_user_bucket_entry*>& ls);
};
WRITE_CLASS_ENCODER(cls_user_bucket_entry)

struct cls_user_stats {
  uint64_t total_entries;
  uint64_t total_bytes;
  uint64_t total_bytes_rounded;

  cls_user_stats()
    : total_entries(0),
      total_bytes(0),
      total_bytes_rounded(0) {}

  void encode(ceph::buffer::list& bl) const {
     ENCODE_START(1, 1, bl);
    encode(total_entries, bl);
    encode(total_bytes, bl);
    encode(total_bytes_rounded, bl);
    ENCODE_FINISH(bl);
  }
  void decode(ceph::buffer::list::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(total_entries, bl);
    decode(total_bytes, bl);
    decode(total_bytes_rounded, bl);
    DECODE_FINISH(bl);
  }

  void dump(ceph::Formatter *f) const;
  static void generate_test_instances(std::list<cls_user_stats*>& ls);
};
WRITE_CLASS_ENCODER(cls_user_stats)

/*
 * this needs to be compatible with rgw_bucket, as it replaces it
 */
struct cls_user_header {
  cls_user_stats stats;
  ceph::real_time last_stats_sync;     /* last time a full stats sync completed */
  ceph::real_time last_stats_update;   /* last time a stats update was done */

  void encode(ceph::buffer::list& bl) const {
     ENCODE_START(1, 1, bl);
    encode(stats, bl);
    encode(last_stats_sync, bl);
    encode(last_stats_update, bl);
    ENCODE_FINISH(bl);
  }
  void decode(ceph::buffer::list::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(stats, bl);
    decode(last_stats_sync, bl);
    decode(last_stats_update, bl);
    DECODE_FINISH(bl);
  }

  void dump(ceph::Formatter *f) const;
  static void generate_test_instances(std::list<cls_user_header*>& ls);
};
WRITE_CLASS_ENCODER(cls_user_header)

// omap header for an account index object
struct cls_user_account_header {
  uint32_t count = 0;

  void encode(ceph::buffer::list& bl) const {
    ENCODE_START(1, 1, bl);
    encode(count, bl);
    ENCODE_FINISH(bl);
  }
  void decode(ceph::buffer::list::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(count, bl);
    DECODE_FINISH(bl);
  }
  void dump(ceph::Formatter* f) const;
  static void generate_test_instances(std::list<cls_user_account_header*>& ls);
};
WRITE_CLASS_ENCODER(cls_user_account_header)

// account resource entry
struct cls_user_account_resource {
  // index by name for put/delete
  std::string name;
  // index by path for listing by PathPrefix
  std::string path;
  // additional opaque metadata depending on resource type
  ceph::buffer::list metadata;

  void encode(ceph::buffer::list& bl) const {
    ENCODE_START(1, 1, bl);
    encode(name, bl);
    encode(path, bl);
    encode(metadata, bl);
    ENCODE_FINISH(bl);
  }
  void decode(ceph::buffer::list::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(name, bl);
    decode(path, bl);
    decode(metadata, bl);
    DECODE_FINISH(bl);
  }
  void dump(ceph::Formatter* f) const;
  static void generate_test_instances(std::list<cls_user_account_resource*>& ls);
};
WRITE_CLASS_ENCODER(cls_user_account_resource)

void cls_user_gen_test_bucket(cls_user_bucket *bucket, int i);
void cls_user_gen_test_bucket_entry(cls_user_bucket_entry *entry, int i);
void cls_user_gen_test_stats(cls_user_stats *stats);
void cls_user_gen_test_header(cls_user_header *h);
void cls_user_gen_test_resource(cls_user_account_resource& r);

#endif
