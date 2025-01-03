// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_CLS_USER_OPS_H
#define CEPH_CLS_USER_OPS_H

#include "cls_user_types.h"

struct cls_user_set_buckets_op {
  std::list<cls_user_bucket_entry> entries;
  bool add;
  ceph::real_time time; /* op time */

  cls_user_set_buckets_op() : add(false) {}

  void encode(ceph::buffer::list& bl) const {
    ENCODE_START(1, 1, bl);
    encode(entries, bl);
    encode(add, bl);
    encode(time, bl);
    ENCODE_FINISH(bl);
  }

  void decode(ceph::buffer::list::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(entries, bl);
    decode(add, bl);
    decode(time, bl);
    DECODE_FINISH(bl);
  }

  void dump(ceph::Formatter *f) const;
  static void generate_test_instances(std::list<cls_user_set_buckets_op*>& ls);
};
WRITE_CLASS_ENCODER(cls_user_set_buckets_op)

struct cls_user_remove_bucket_op {
  cls_user_bucket bucket;

  cls_user_remove_bucket_op() {}

  void encode(ceph::buffer::list& bl) const {
    ENCODE_START(1, 1, bl);
    encode(bucket, bl);
    ENCODE_FINISH(bl);
  }

  void decode(ceph::buffer::list::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(bucket, bl);
    DECODE_FINISH(bl);
  }

  void dump(ceph::Formatter *f) const;
  static void generate_test_instances(std::list<cls_user_remove_bucket_op*>& ls);
};
WRITE_CLASS_ENCODER(cls_user_remove_bucket_op)

struct cls_user_list_buckets_op {
  std::string marker;
  std::string end_marker;
  int max_entries; /* upperbound to returned num of entries
                      might return less than that and still be truncated */

  cls_user_list_buckets_op()
    : max_entries(0) {}

  void encode(ceph::buffer::list& bl) const {
    ENCODE_START(2, 1, bl);
    encode(marker, bl);
    encode(max_entries, bl);
    encode(end_marker, bl);
    ENCODE_FINISH(bl);
  }

  void decode(ceph::buffer::list::const_iterator& bl) {
    DECODE_START(2, bl);
    decode(marker, bl);
    decode(max_entries, bl);
    if (struct_v >= 2) {
      decode(end_marker, bl);
    }
    DECODE_FINISH(bl);
  }

  void dump(ceph::Formatter *f) const;
  static void generate_test_instances(std::list<cls_user_list_buckets_op*>& ls);
};
WRITE_CLASS_ENCODER(cls_user_list_buckets_op)

struct cls_user_list_buckets_ret {
  std::list<cls_user_bucket_entry> entries;
  std::string marker;
  bool truncated;

  cls_user_list_buckets_ret() : truncated(false) {}

  void encode(ceph::buffer::list& bl) const {
    ENCODE_START(1, 1, bl);
    encode(entries, bl);
    encode(marker, bl);
    encode(truncated, bl);
    ENCODE_FINISH(bl);
  }

  void decode(ceph::buffer::list::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(entries, bl);
    decode(marker, bl);
    decode(truncated, bl);
    DECODE_FINISH(bl);
  }

  void dump(ceph::Formatter *f) const;
  static void generate_test_instances(std::list<cls_user_list_buckets_ret*>& ls);
};
WRITE_CLASS_ENCODER(cls_user_list_buckets_ret)


struct cls_user_get_header_op {
  cls_user_get_header_op() {}

  void encode(ceph::buffer::list& bl) const {
    ENCODE_START(1, 1, bl);
    ENCODE_FINISH(bl);
  }

  void decode(ceph::buffer::list::const_iterator& bl) {
    DECODE_START(1, bl);
    DECODE_FINISH(bl);
  }

  void dump(ceph::Formatter *f) const;
  static void generate_test_instances(std::list<cls_user_get_header_op*>& ls);
};
WRITE_CLASS_ENCODER(cls_user_get_header_op)

struct cls_user_reset_stats_op {
  ceph::real_time time;
  cls_user_reset_stats_op() {}

  void encode(ceph::buffer::list& bl) const {
    ENCODE_START(1, 1, bl);
    encode(time, bl);
    ENCODE_FINISH(bl);
  }

  void decode(ceph::buffer::list::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(time, bl);
    DECODE_FINISH(bl);
  }

  void dump(ceph::Formatter *f) const;
  static void generate_test_instances(std::list<cls_user_reset_stats_op*>& ls);
};
WRITE_CLASS_ENCODER(cls_user_reset_stats_op);

struct cls_user_reset_stats2_op {
  ceph::real_time time;
  std::string marker;
  cls_user_stats acc_stats;

  cls_user_reset_stats2_op() {}

  void encode(ceph::buffer::list& bl) const {
    ENCODE_START(1, 1, bl);
    encode(time, bl);
    encode(marker, bl);
    encode(acc_stats, bl);
    ENCODE_FINISH(bl);
  }

  void decode(ceph::buffer::list::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(time, bl);
    decode(marker, bl);
    decode(acc_stats, bl);
    DECODE_FINISH(bl);
  }

  void dump(ceph::Formatter *f) const;
  static void generate_test_instances(std::list<cls_user_reset_stats2_op*>& ls);
};
WRITE_CLASS_ENCODER(cls_user_reset_stats2_op);

struct cls_user_reset_stats2_ret {
  std::string marker;
  cls_user_stats acc_stats; /* 0-initialized */
  bool truncated;

  cls_user_reset_stats2_ret()
    : truncated(false) {}

  void update_call(cls_user_reset_stats2_op& call) {
    call.marker = marker;
    call.acc_stats = acc_stats;
  }

  void encode(ceph::buffer::list& bl) const {
    ENCODE_START(1, 1, bl);
    encode(marker, bl);
    encode(acc_stats, bl);
    encode(truncated, bl);
    ENCODE_FINISH(bl);
  }

  void decode(ceph::buffer::list::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(marker, bl);
    decode(acc_stats, bl);
    decode(truncated, bl);
    DECODE_FINISH(bl);
  }

  void dump(ceph::Formatter *f) const;
  static void generate_test_instances(
    std::list<cls_user_reset_stats2_ret*>& ls);
};
WRITE_CLASS_ENCODER(cls_user_reset_stats2_ret);

struct cls_user_get_header_ret {
  cls_user_header header;

  cls_user_get_header_ret() {}

  void encode(ceph::buffer::list& bl) const {
    ENCODE_START(1, 1, bl);
    encode(header, bl);
    ENCODE_FINISH(bl);
  }

  void decode(ceph::buffer::list::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(header, bl);
    DECODE_FINISH(bl);
  }

  void dump(ceph::Formatter *f) const;
  static void generate_test_instances(std::list<cls_user_get_header_ret*>& ls);
};
WRITE_CLASS_ENCODER(cls_user_get_header_ret)

struct cls_user_complete_stats_sync_op {
  ceph::real_time time;

  cls_user_complete_stats_sync_op() {}

  void encode(ceph::buffer::list& bl) const {
    ENCODE_START(1, 1, bl);
    encode(time, bl);
    ENCODE_FINISH(bl);
  }

  void decode(ceph::buffer::list::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(time, bl);
    DECODE_FINISH(bl);
  }

  void dump(ceph::Formatter *f) const;
  static void generate_test_instances(std::list<cls_user_complete_stats_sync_op*>& ls);
};
WRITE_CLASS_ENCODER(cls_user_complete_stats_sync_op)


struct cls_user_account_resource_add_op {
  cls_user_account_resource entry;
  bool exclusive = false;
  uint32_t limit = 0;

  void encode(ceph::buffer::list& bl) const {
    ENCODE_START(1, 1, bl);
    encode(entry, bl);
    encode(exclusive, bl);
    encode(limit, bl);
    ENCODE_FINISH(bl);
  }
  void decode(ceph::buffer::list::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(entry, bl);
    decode(exclusive, bl);
    decode(limit, bl);
    DECODE_FINISH(bl);
  }

  void dump(ceph::Formatter* f) const;
  static void generate_test_instances(std::list<cls_user_account_resource_add_op*>& ls);
};
WRITE_CLASS_ENCODER(cls_user_account_resource_add_op)

struct cls_user_account_resource_get_op {
  std::string name;

  void encode(ceph::buffer::list& bl) const {
    ENCODE_START(1, 1, bl);
    encode(name, bl);
    ENCODE_FINISH(bl);
  }
  void decode(ceph::buffer::list::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(name, bl);
    DECODE_FINISH(bl);
  }

  void dump(ceph::Formatter* f) const;
  static void generate_test_instances(std::list<cls_user_account_resource_get_op*>& ls);
};
WRITE_CLASS_ENCODER(cls_user_account_resource_get_op)

struct cls_user_account_resource_get_ret {
  cls_user_account_resource entry;

  void encode(ceph::buffer::list& bl) const {
    ENCODE_START(1, 1, bl);
    encode(entry, bl);
    ENCODE_FINISH(bl);
  }
  void decode(ceph::buffer::list::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(entry, bl);
    DECODE_FINISH(bl);
  }

  void dump(ceph::Formatter* f) const;
  static void generate_test_instances(std::list<cls_user_account_resource_get_ret*>& ls);
};
WRITE_CLASS_ENCODER(cls_user_account_resource_get_ret)

struct cls_user_account_resource_rm_op {
  std::string name;

  void encode(ceph::buffer::list& bl) const {
    ENCODE_START(1, 1, bl);
    encode(name, bl);
    ENCODE_FINISH(bl);
  }
  void decode(ceph::buffer::list::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(name, bl);
    DECODE_FINISH(bl);
  }

  void dump(ceph::Formatter* f) const;
  static void generate_test_instances(std::list<cls_user_account_resource_rm_op*>& ls);
};
WRITE_CLASS_ENCODER(cls_user_account_resource_rm_op)

struct cls_user_account_resource_list_op {
  std::string marker;
  std::string path_prefix;
  uint32_t max_entries = 0;

  void encode(ceph::buffer::list& bl) const {
    ENCODE_START(1, 1, bl);
    encode(marker, bl);
    encode(path_prefix, bl);
    encode(max_entries, bl);
    ENCODE_FINISH(bl);
  }
  void decode(ceph::buffer::list::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(marker, bl);
    decode(path_prefix, bl);
    decode(max_entries, bl);
    DECODE_FINISH(bl);
  }

  void dump(ceph::Formatter* f) const;
  static void generate_test_instances(std::list<cls_user_account_resource_list_op*>& ls);
};
WRITE_CLASS_ENCODER(cls_user_account_resource_list_op)

struct cls_user_account_resource_list_ret {
  std::vector<cls_user_account_resource> entries;
  bool truncated = false;
  std::string marker;

  void encode(ceph::buffer::list& bl) const {
    ENCODE_START(1, 1, bl);
    encode(entries, bl);
    encode(truncated, bl);
    encode(marker, bl);
    ENCODE_FINISH(bl);
  }
  void decode(ceph::buffer::list::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(entries, bl);
    decode(truncated, bl);
    decode(marker, bl);
    DECODE_FINISH(bl);
  }

  void dump(ceph::Formatter* f) const;
  static void generate_test_instances(std::list<cls_user_account_resource_list_ret*>& ls);
};
WRITE_CLASS_ENCODER(cls_user_account_resource_list_ret)

#endif
