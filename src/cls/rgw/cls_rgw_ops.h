// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_CLS_RGW_OPS_H
#define CEPH_CLS_RGW_OPS_H

#include <cstdint>
#include <list>
#include <map>
#include <string>
#include <vector>

#include "common/ceph_time.h"
#include "common/Formatter.h"

#include "cls/rgw/cls_rgw_types.h"

struct rgw_cls_tag_timeout_op
{
  std::uint64_t tag_timeout = 0;

  rgw_cls_tag_timeout_op() = default;

  void encode(ceph::buffer::list& bl) const {
    ENCODE_START(1, 1, bl);
    encode(tag_timeout, bl);
    ENCODE_FINISH(bl);
  }
  void decode(ceph::buffer::list::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(tag_timeout, bl);
    DECODE_FINISH(bl);
  }
  void dump(ceph::Formatter* f) const;
  static void generate_test_instances(std::list<rgw_cls_tag_timeout_op*>& ls);
};
WRITE_CLASS_ENCODER(rgw_cls_tag_timeout_op)

struct rgw_cls_obj_prepare_op
{
  RGWModifyOp op = CLS_RGW_OP_UNKNOWN;
  cls_rgw_obj_key key;
  std::string tag;
  std::string locator;
  bool log_op = false;
  std::uint16_t bilog_flags = 0;
  rgw_zone_set zones_trace;

  rgw_cls_obj_prepare_op() = default;

  void encode(ceph::buffer::list& bl) const {
    ENCODE_START(7, 5, bl);
    auto c = static_cast<std::uint8_t>(op);
    encode(c, bl);
    encode(tag, bl);
    encode(locator, bl);
    encode(log_op, bl);
    encode(key, bl);
    encode(bilog_flags, bl);
    encode(zones_trace, bl);
    ENCODE_FINISH(bl);
  }
  void decode(ceph::buffer::list::const_iterator& bl) {
    DECODE_START_LEGACY_COMPAT_LEN(7, 3, 3, bl);
    std::uint8_t c;
    decode(c, bl);
    op = static_cast<RGWModifyOp>(c);
    if (struct_v < 5) {
      decode(key.name, bl);
    }
    decode(tag, bl);
    if (struct_v >= 2) {
      decode(locator, bl);
    }
    if (struct_v >= 4) {
      decode(log_op, bl);
    }
    if (struct_v >= 5) {
      decode(key, bl);
    }
    if (struct_v >= 6) {
      decode(bilog_flags, bl);
    }
    if (struct_v >= 7) {
      decode(zones_trace, bl);
    }
    DECODE_FINISH(bl);
  }
  void dump(ceph::Formatter* f) const;
  static void generate_test_instances(std::list<rgw_cls_obj_prepare_op*>& o);
};
WRITE_CLASS_ENCODER(rgw_cls_obj_prepare_op)

struct rgw_cls_obj_complete_op
{
  RGWModifyOp op = CLS_RGW_OP_ADD;
  cls_rgw_obj_key key;
  std::string locator;
  rgw_bucket_entry_ver ver;
  rgw_bucket_dir_entry_meta meta;
  std::string tag;
  bool log_op = false;
  std::uint16_t bilog_flags = 0;

  std::vector<cls_rgw_obj_key> remove_objs;
  rgw_zone_set zones_trace;

  rgw_cls_obj_complete_op() = default;

  void encode(ceph::buffer::list& bl) const {
    ENCODE_START(9, 7, bl);
    auto c = static_cast<std::uint8_t>(op);
    encode(c, bl);
    encode(ver.epoch, bl);
    encode(meta, bl);
    encode(tag, bl);
    encode(locator, bl);
    encode(remove_objs, bl);
    encode(ver, bl);
    encode(log_op, bl);
    encode(key, bl);
    encode(bilog_flags, bl);
    encode(zones_trace, bl);
    ENCODE_FINISH(bl);
 }
  void decode(ceph::buffer::list::const_iterator& bl) {
    DECODE_START_LEGACY_COMPAT_LEN(9, 3, 3, bl);
    std::uint8_t c;
    decode(c, bl);
    op = static_cast<RGWModifyOp>(c);
    if (struct_v < 7) {
      decode(key.name, bl);
    }
    decode(ver.epoch, bl);
    decode(meta, bl);
    decode(tag, bl);
    if (struct_v >= 2) {
      decode(locator, bl);
    }
    if (struct_v >= 4 && struct_v < 7) {
      std::vector<string> old_remove_objs;
      decode(old_remove_objs, bl);

      for (auto iter = old_remove_objs.begin();
           iter != old_remove_objs.end(); ++iter) {
        cls_rgw_obj_key k;
        k.name = *iter;
        remove_objs.push_back(k);
      }
    } else {
      decode(remove_objs, bl);
    }
    if (struct_v >= 5) {
      decode(ver, bl);
    } else {
      ver.pool = -1;
    }
    if (struct_v >= 6) {
      decode(log_op, bl);
    }
    if (struct_v >= 7) {
      decode(key, bl);
    }
    if (struct_v >= 8) {
      decode(bilog_flags, bl);
    }
    if (struct_v >= 9) {
      decode(zones_trace, bl);
    }
    DECODE_FINISH(bl);
  }
  void dump(ceph::Formatter* f) const;
  static void generate_test_instances(std::list<rgw_cls_obj_complete_op*>& o);
};
WRITE_CLASS_ENCODER(rgw_cls_obj_complete_op)

struct rgw_cls_link_olh_op {
  cls_rgw_obj_key key;
  std::string olh_tag;
  bool delete_marker = false;
  std::string op_tag;
  rgw_bucket_dir_entry_meta meta;
  std::uint64_t olh_epoch = 0;
  bool log_op = false;
  std::uint16_t bilog_flags = 0;
  ceph::real_time unmod_since; /* only create delete marker if newer then this */
  bool high_precision_time = false;
  rgw_zone_set zones_trace;

  rgw_cls_link_olh_op() = default;

  void encode(ceph::buffer::list& bl) const {
    ENCODE_START(5, 1, bl);
    encode(key, bl);
    encode(olh_tag, bl);
    encode(delete_marker, bl);
    encode(op_tag, bl);
    encode(meta, bl);
    encode(olh_epoch, bl);
    encode(log_op, bl);
    encode(bilog_flags, bl);
    std::uint64_t t = ceph::real_clock::to_time_t(unmod_since);
    encode(t, bl);
    encode(unmod_since, bl);
    encode(high_precision_time, bl);
    encode(zones_trace, bl);
    ENCODE_FINISH(bl);
  }

  void decode(ceph::buffer::list::const_iterator& bl) {
    DECODE_START(5, bl);
    decode(key, bl);
    decode(olh_tag, bl);
    decode(delete_marker, bl);
    decode(op_tag, bl);
    decode(meta, bl);
    decode(olh_epoch, bl);
    decode(log_op, bl);
    decode(bilog_flags, bl);
    if (struct_v == 2) {
      std::uint64_t t;
      decode(t, bl);
      unmod_since = ceph::real_clock::from_time_t(static_cast<time_t>(t));
    }
    if (struct_v >= 3) {
      std::uint64_t t;
      decode(t, bl);
      decode(unmod_since, bl);
    }
    if (struct_v >= 4) {
      decode(high_precision_time, bl);
    }
    if (struct_v >= 5) {
      decode(zones_trace, bl);
    }
    DECODE_FINISH(bl);
  }

  static void generate_test_instances(std::list<rgw_cls_link_olh_op*>& o);
  void dump(ceph::Formatter* f) const;
};
WRITE_CLASS_ENCODER(rgw_cls_link_olh_op)

struct rgw_cls_unlink_instance_op {
  cls_rgw_obj_key key;
  std::string op_tag;
  std::uint64_t olh_epoch = 0;
  bool log_op = false;
  std::uint16_t bilog_flags = 0;
  std::string olh_tag;
  rgw_zone_set zones_trace;

  rgw_cls_unlink_instance_op() = default;

  void encode(ceph::buffer::list& bl) const {
    ENCODE_START(3, 1, bl);
    encode(key, bl);
    encode(op_tag, bl);
    encode(olh_epoch, bl);
    encode(log_op, bl);
    encode(bilog_flags, bl);
    encode(olh_tag, bl);
    encode(zones_trace, bl);
    ENCODE_FINISH(bl);
  }

  void decode(ceph::buffer::list::const_iterator& bl) {
    DECODE_START(3, bl);
    decode(key, bl);
    decode(op_tag, bl);
    decode(olh_epoch, bl);
    decode(log_op, bl);
    decode(bilog_flags, bl);
    if (struct_v >= 2) {
      decode(olh_tag, bl);
    }
    if (struct_v >= 3) {
      decode(zones_trace, bl);
    }
    DECODE_FINISH(bl);
  }

  static void generate_test_instances(
    std::list<rgw_cls_unlink_instance_op*>& o);
  void dump(ceph::Formatter* f) const;
};
WRITE_CLASS_ENCODER(rgw_cls_unlink_instance_op)

struct rgw_cls_read_olh_log_op
{
  cls_rgw_obj_key olh;
  std::uint64_t ver_marker = 0;
  std::string olh_tag;

  rgw_cls_read_olh_log_op() = default;

  void encode(ceph::buffer::list& bl) const {
    ENCODE_START(1, 1, bl);
    encode(olh, bl);
    encode(ver_marker, bl);
    encode(olh_tag, bl);
    ENCODE_FINISH(bl);
  }
  void decode(ceph::buffer::list::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(olh, bl);
    decode(ver_marker, bl);
    decode(olh_tag, bl);
    DECODE_FINISH(bl);
  }
  static void generate_test_instances(std::list<rgw_cls_read_olh_log_op*>& o);
  void dump(ceph::Formatter* f) const;
};
WRITE_CLASS_ENCODER(rgw_cls_read_olh_log_op)

struct rgw_cls_read_olh_log_ret
{
  std::map<uint64_t, std::vector<rgw_bucket_olh_log_entry>> log;
  bool is_truncated = false;

  rgw_cls_read_olh_log_ret() = default;

  void encode(ceph::buffer::list& bl) const {
    ENCODE_START(1, 1, bl);
    encode(log, bl);
    encode(is_truncated, bl);
    ENCODE_FINISH(bl);
  }
  void decode(ceph::buffer::list::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(log, bl);
    decode(is_truncated, bl);
    DECODE_FINISH(bl);
  }
  static void generate_test_instances(std::list<rgw_cls_read_olh_log_ret*>& o);
  void dump(ceph::Formatter* f) const;
};
WRITE_CLASS_ENCODER(rgw_cls_read_olh_log_ret)

struct rgw_cls_trim_olh_log_op
{
  cls_rgw_obj_key olh;
  std::uint64_t ver = 0;
  std::string olh_tag;

  rgw_cls_trim_olh_log_op() = default;

  void encode(ceph::buffer::list& bl) const {
    ENCODE_START(1, 1, bl);
    encode(olh, bl);
    encode(ver, bl);
    encode(olh_tag, bl);
    ENCODE_FINISH(bl);
  }
  void decode(ceph::buffer::list::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(olh, bl);
    decode(ver, bl);
    decode(olh_tag, bl);
    DECODE_FINISH(bl);
  }
  static void generate_test_instances(std::list<rgw_cls_trim_olh_log_op*>& o);
  void dump(ceph::Formatter* f) const;
};
WRITE_CLASS_ENCODER(rgw_cls_trim_olh_log_op)

struct rgw_cls_bucket_clear_olh_op {
  cls_rgw_obj_key key;
  std::string olh_tag;

  rgw_cls_bucket_clear_olh_op() = default;

  void encode(ceph::buffer::list& bl) const {
    ENCODE_START(1, 1, bl);
    encode(key, bl);
    encode(olh_tag, bl);
    ENCODE_FINISH(bl);
  }

  void decode(ceph::buffer::list::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(key, bl);
    decode(olh_tag, bl);
    DECODE_FINISH(bl);
  }

  static void generate_test_instances(
    std::list<rgw_cls_bucket_clear_olh_op*>& o);
  void dump(ceph::Formatter* f) const;
};
WRITE_CLASS_ENCODER(rgw_cls_bucket_clear_olh_op)

struct rgw_cls_list_op
{
  cls_rgw_obj_key start_obj;
  std::uint32_t num_entries = 0;
  std::string filter_prefix;
  bool list_versions = false;

  rgw_cls_list_op() = default;

  void encode(ceph::buffer::list& bl) const {
    ENCODE_START(5, 4, bl);
    encode(num_entries, bl);
    encode(filter_prefix, bl);
    encode(start_obj, bl);
    encode(list_versions, bl);
    ENCODE_FINISH(bl);
  }
  void decode(ceph::buffer::list::const_iterator& bl) {
    DECODE_START_LEGACY_COMPAT_LEN(5, 2, 2, bl);
    if (struct_v < 4) {
      decode(start_obj.name, bl);
    }
    decode(num_entries, bl);
    if (struct_v >= 3)
      decode(filter_prefix, bl);
    if (struct_v >= 4)
      decode(start_obj, bl);
    if (struct_v >= 5)
      decode(list_versions, bl);
    DECODE_FINISH(bl);
  }
  void dump(ceph::Formatter* f) const;
  static void generate_test_instances(std::list<rgw_cls_list_op*>& o);
};
WRITE_CLASS_ENCODER(rgw_cls_list_op)

struct rgw_cls_list_ret {
  rgw_bucket_dir dir;
  bool is_truncated = false;

  rgw_cls_list_ret() = default;

  void encode(ceph::buffer::list& bl) const {
    ENCODE_START(2, 2, bl);
    encode(dir, bl);
    encode(is_truncated, bl);
    ENCODE_FINISH(bl);
  }
  void decode(ceph::buffer::list::const_iterator& bl) {
    DECODE_START_LEGACY_COMPAT_LEN(2, 2, 2, bl);
    decode(dir, bl);
    decode(is_truncated, bl);
    DECODE_FINISH(bl);
  }
  void dump(ceph::Formatter* f) const;
  static void generate_test_instances(std::list<rgw_cls_list_ret*>& o);
};
WRITE_CLASS_ENCODER(rgw_cls_list_ret)

struct rgw_cls_check_index_ret
{
  rgw_bucket_dir_header existing_header;
  rgw_bucket_dir_header calculated_header;

  rgw_cls_check_index_ret() = default;

  void encode(ceph::buffer::list& bl) const {
    ENCODE_START(1, 1, bl);
    encode(existing_header, bl);
    encode(calculated_header, bl);
    ENCODE_FINISH(bl);
  }
  void decode(ceph::buffer::list::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(existing_header, bl);
    decode(calculated_header, bl);
    DECODE_FINISH(bl);
  }
  void dump(ceph::Formatter* f) const;
  static void generate_test_instances(std::list<rgw_cls_check_index_ret*>& o);
};
WRITE_CLASS_ENCODER(rgw_cls_check_index_ret)

struct rgw_cls_bucket_update_stats_op
{
  bool absolute{false};
  std::map<RGWObjCategory, rgw_bucket_category_stats> stats;

  rgw_cls_bucket_update_stats_op() = default;

  void encode(ceph::buffer::list& bl) const {
    ENCODE_START(1, 1, bl);
    encode(absolute, bl);
    encode(stats, bl);
    ENCODE_FINISH(bl);
  }
  void decode(ceph::buffer::list::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(absolute, bl);
    decode(stats, bl);
    DECODE_FINISH(bl);
  }
  void dump(ceph::Formatter* f) const;
  static void generate_test_instances(
    std::list<rgw_cls_bucket_update_stats_op*>& o);
};
WRITE_CLASS_ENCODER(rgw_cls_bucket_update_stats_op)

struct rgw_cls_obj_remove_op {
  std::vector<std::string> keep_attr_prefixes;

  void encode(ceph::buffer::list& bl) const {
    ENCODE_START(1, 1, bl);
    encode(keep_attr_prefixes, bl);
    ENCODE_FINISH(bl);
  }

  void decode(ceph::buffer::list::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(keep_attr_prefixes, bl);
    DECODE_FINISH(bl);
  }
};
WRITE_CLASS_ENCODER(rgw_cls_obj_remove_op)

struct rgw_cls_obj_store_pg_ver_op {
  std::string attr;

  void encode(ceph::buffer::list& bl) const {
    ENCODE_START(1, 1, bl);
    encode(attr, bl);
    ENCODE_FINISH(bl);
  }

  void decode(ceph::buffer::list::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(attr, bl);
    DECODE_FINISH(bl);
  }
};
WRITE_CLASS_ENCODER(rgw_cls_obj_store_pg_ver_op)

struct rgw_cls_obj_check_attrs_prefix {
  std::string check_prefix;
  bool fail_if_exist = false;

  rgw_cls_obj_check_attrs_prefix() = default;

  void encode(ceph::buffer::list& bl) const {
    ENCODE_START(1, 1, bl);
    encode(check_prefix, bl);
    encode(fail_if_exist, bl);
    ENCODE_FINISH(bl);
  }

  void decode(ceph::buffer::list::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(check_prefix, bl);
    decode(fail_if_exist, bl);
    DECODE_FINISH(bl);
  }
};
WRITE_CLASS_ENCODER(rgw_cls_obj_check_attrs_prefix)

struct rgw_cls_obj_check_mtime {
  ceph::real_time mtime;
  RGWCheckMTimeType type = CLS_RGW_CHECK_TIME_MTIME_EQ;
  bool high_precision_time = false;

  rgw_cls_obj_check_mtime() = default;

  void encode(ceph::buffer::list& bl) const {
    ENCODE_START(2, 1, bl);
    encode(mtime, bl);
    encode(static_cast<uint8_t>(type), bl);
    encode(high_precision_time, bl);
    ENCODE_FINISH(bl);
  }

  void decode(ceph::buffer::list::const_iterator& bl) {
    DECODE_START(2, bl);
    decode(mtime, bl);
    std::uint8_t c;
    decode(c, bl);
    type = static_cast<RGWCheckMTimeType>(c);
    if (struct_v >= 2) {
      decode(high_precision_time, bl);
    }
    DECODE_FINISH(bl);
  }
};
WRITE_CLASS_ENCODER(rgw_cls_obj_check_mtime)

struct rgw_cls_usage_log_add_op {
  rgw_usage_log_info info;
  rgw_user user;

  void encode(ceph::buffer::list& bl) const {
    ENCODE_START(2, 1, bl);
    encode(info, bl);
    encode(user.to_str(), bl);
    ENCODE_FINISH(bl);
  }

  void decode(ceph::buffer::list::const_iterator& bl) {
    DECODE_START(2, bl);
    decode(info, bl);
    if (struct_v >= 2) {
      std::string s;
      decode(s, bl);
      user.from_str(s);
    }
    DECODE_FINISH(bl);
  }
};
WRITE_CLASS_ENCODER(rgw_cls_usage_log_add_op)

struct rgw_cls_bi_get_op {
  cls_rgw_obj_key key;
  BIIndexType type = BIIndexType::Plain; /* namespace: plain, instance, olh */

  rgw_cls_bi_get_op() = default;

  void encode(ceph::buffer::list& bl) const {
    ENCODE_START(1, 1, bl);
    encode(key, bl);
    encode(static_cast<std::uint8_t>(type), bl);
    ENCODE_FINISH(bl);
  }

  void decode(ceph::buffer::list::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(key, bl);
    std::uint8_t c;
    decode(c, bl);
    type = static_cast<BIIndexType>(c);
    DECODE_FINISH(bl);
  }
};
WRITE_CLASS_ENCODER(rgw_cls_bi_get_op)

struct rgw_cls_bi_get_ret {
  rgw_cls_bi_entry entry;

  rgw_cls_bi_get_ret() = default;

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
};
WRITE_CLASS_ENCODER(rgw_cls_bi_get_ret)

struct rgw_cls_bi_put_op {
  rgw_cls_bi_entry entry;

  rgw_cls_bi_put_op() = default;

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
};
WRITE_CLASS_ENCODER(rgw_cls_bi_put_op)

struct rgw_cls_bi_list_op {
  std::uint32_t max = 0;
  std::string name;
  std::string marker;

  rgw_cls_bi_list_op() = default;

  void encode(ceph::buffer::list& bl) const {
    ENCODE_START(1, 1, bl);
    encode(max, bl);
    encode(name, bl);
    encode(marker, bl);
    ENCODE_FINISH(bl);
  }

  void decode(ceph::buffer::list::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(max, bl);
    decode(name, bl);
    decode(marker, bl);
    DECODE_FINISH(bl);
  }
};
WRITE_CLASS_ENCODER(rgw_cls_bi_list_op)

struct rgw_cls_bi_list_ret {
  std::vector<rgw_cls_bi_entry> entries;
  bool is_truncated = false;

  rgw_cls_bi_list_ret() = default;

  void encode(ceph::buffer::list& bl) const {
    ENCODE_START(1, 1, bl);
    encode(entries, bl);
    encode(is_truncated, bl);
    ENCODE_FINISH(bl);
  }

  void decode(ceph::buffer::list::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(entries, bl);
    decode(is_truncated, bl);
    DECODE_FINISH(bl);
  }
};
WRITE_CLASS_ENCODER(rgw_cls_bi_list_ret)

struct rgw_cls_usage_log_read_op {
  std::uint64_t start_epoch;
  std::uint64_t end_epoch;
  std::string owner;
  std::string bucket;

  std::string iter;  // should be empty for the first call, non empty for subsequent calls
  std::uint32_t max_entries;

  void encode(ceph::buffer::list& bl) const {
    ENCODE_START(2, 1, bl);
    encode(start_epoch, bl);
    encode(end_epoch, bl);
    encode(owner, bl);
    encode(iter, bl);
    encode(max_entries, bl);
    encode(bucket, bl);
    ENCODE_FINISH(bl);
  }

  void decode(ceph::buffer::list::const_iterator& bl) {
    DECODE_START(2, bl);
    decode(start_epoch, bl);
    decode(end_epoch, bl);
    decode(owner, bl);
    decode(iter, bl);
    decode(max_entries, bl);
    if (struct_v >= 2) {
      decode(bucket, bl);
    }
    DECODE_FINISH(bl);
  }
};
WRITE_CLASS_ENCODER(rgw_cls_usage_log_read_op)

struct rgw_cls_usage_log_read_ret {
  std::map<rgw_user_bucket, rgw_usage_log_entry> usage;
  bool truncated;
  std::string next_iter;

  void encode(ceph::buffer::list& bl) const {
    ENCODE_START(1, 1, bl);
    encode(usage, bl);
    encode(truncated, bl);
    encode(next_iter, bl);
    ENCODE_FINISH(bl);
  }

  void decode(ceph::buffer::list::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(usage, bl);
    decode(truncated, bl);
    decode(next_iter, bl);
    DECODE_FINISH(bl);
  }
};
WRITE_CLASS_ENCODER(rgw_cls_usage_log_read_ret)

struct rgw_cls_usage_log_trim_op {
  std::uint64_t start_epoch;
  std::uint64_t end_epoch;
  std::string user;
  std::string bucket;

  void encode(ceph::buffer::list& bl) const {
    ENCODE_START(3, 2, bl);
    encode(start_epoch, bl);
    encode(end_epoch, bl);
    encode(user, bl);
    encode(bucket, bl);
    ENCODE_FINISH(bl);
  }

  void decode(ceph::buffer::list::const_iterator& bl) {
    DECODE_START(3, bl);
    decode(start_epoch, bl);
    decode(end_epoch, bl);
    decode(user, bl);
    if (struct_v >= 3) {
      decode(bucket, bl);
    }
    DECODE_FINISH(bl);
  }
};
WRITE_CLASS_ENCODER(rgw_cls_usage_log_trim_op)

struct cls_rgw_gc_set_entry_op {
  std::uint32_t expiration_secs = 0;
  cls_rgw_gc_obj_info info;
  cls_rgw_gc_set_entry_op() = default;

  void encode(ceph::buffer::list& bl) const {
    ENCODE_START(1, 1, bl);
    encode(expiration_secs, bl);
    encode(info, bl);
    ENCODE_FINISH(bl);
  }

  void decode(ceph::buffer::list::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(expiration_secs, bl);
    decode(info, bl);
    DECODE_FINISH(bl);
  }

  void dump(ceph::Formatter* f) const;
  static void generate_test_instances(std::list<cls_rgw_gc_set_entry_op*>& ls);
};
WRITE_CLASS_ENCODER(cls_rgw_gc_set_entry_op)

struct cls_rgw_gc_defer_entry_op {
  std::uint32_t expiration_secs = 0;
  std::string tag;
  cls_rgw_gc_defer_entry_op() = default;

  void encode(ceph::buffer::list& bl) const {
    ENCODE_START(1, 1, bl);
    encode(expiration_secs, bl);
    encode(tag, bl);
    ENCODE_FINISH(bl);
  }

  void decode(ceph::buffer::list::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(expiration_secs, bl);
    decode(tag, bl);
    DECODE_FINISH(bl);
  }

  void dump(ceph::Formatter* f) const;
  static void generate_test_instances(std::list<cls_rgw_gc_defer_entry_op*>& ls);
};
WRITE_CLASS_ENCODER(cls_rgw_gc_defer_entry_op)

struct cls_rgw_gc_list_op {
  std::string marker;
  std::uint32_t max = 0;
  bool expired_only = true;

  cls_rgw_gc_list_op() = default;

  void encode(ceph::buffer::list& bl) const {
    ENCODE_START(2, 1, bl);
    encode(marker, bl);
    encode(max, bl);
    encode(expired_only, bl);
    ENCODE_FINISH(bl);
  }

  void decode(ceph::buffer::list::const_iterator& bl) {
    DECODE_START(2, bl);
    decode(marker, bl);
    decode(max, bl);
    if (struct_v >= 2) {
      decode(expired_only, bl);
    }
    DECODE_FINISH(bl);
  }

  void dump(ceph::Formatter* f) const;
  static void generate_test_instances(std::list<cls_rgw_gc_list_op*>& ls);
};
WRITE_CLASS_ENCODER(cls_rgw_gc_list_op)

struct cls_rgw_gc_list_ret {
  std::vector<cls_rgw_gc_obj_info> entries;
  std::string next_marker;
  bool truncated = false;

  cls_rgw_gc_list_ret() = default;

  void encode(ceph::buffer::list& bl) const {
    ENCODE_START(2, 1, bl);
    encode(entries, bl);
    encode(next_marker, bl);
    encode(truncated, bl);
    ENCODE_FINISH(bl);
  }

  void decode(ceph::buffer::list::const_iterator& bl) {
    DECODE_START(2, bl);
    decode(entries, bl);
    if (struct_v >= 2)
      decode(next_marker, bl);
    decode(truncated, bl);
    DECODE_FINISH(bl);
  }

  void dump(ceph::Formatter* f) const;
  static void generate_test_instances(std::list<cls_rgw_gc_list_ret*>& ls);
};
WRITE_CLASS_ENCODER(cls_rgw_gc_list_ret)

struct cls_rgw_gc_remove_op {
  std::vector<string> tags;

  cls_rgw_gc_remove_op() = default;

  void encode(ceph::buffer::list& bl) const {
    ENCODE_START(1, 1, bl);
    encode(tags, bl);
    ENCODE_FINISH(bl);
  }

  void decode(ceph::buffer::list::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(tags, bl);
    DECODE_FINISH(bl);
  }

  void dump(ceph::Formatter* f) const;
  static void generate_test_instances(std::list<cls_rgw_gc_remove_op*>& ls);
};
WRITE_CLASS_ENCODER(cls_rgw_gc_remove_op)

struct cls_rgw_bi_log_list_op {
  std::string marker;
  std::uint32_t max = 0;

  cls_rgw_bi_log_list_op() = default;

  void encode(ceph::buffer::list& bl) const {
    ENCODE_START(1, 1, bl);
    encode(marker, bl);
    encode(max, bl);
    ENCODE_FINISH(bl);
  }

  void decode(ceph::buffer::list::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(marker, bl);
    decode(max, bl);
    DECODE_FINISH(bl);
  }

  void dump(ceph::Formatter* f) const;
  static void generate_test_instances(std::list<cls_rgw_bi_log_list_op*>& ls);
};
WRITE_CLASS_ENCODER(cls_rgw_bi_log_list_op)

struct cls_rgw_bi_log_trim_op {
  std::string start_marker;
  std::string end_marker;

  cls_rgw_bi_log_trim_op() = default;

  void encode(ceph::buffer::list& bl) const {
    ENCODE_START(1, 1, bl);
    encode(start_marker, bl);
    encode(end_marker, bl);
    ENCODE_FINISH(bl);
  }

  void decode(ceph::buffer::list::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(start_marker, bl);
    decode(end_marker, bl);
    DECODE_FINISH(bl);
  }

  void dump(ceph::Formatter* f) const;
  static void generate_test_instances(std::list<cls_rgw_bi_log_trim_op*>& ls);
};
WRITE_CLASS_ENCODER(cls_rgw_bi_log_trim_op)

struct cls_rgw_bi_log_list_ret {
  std::vector<rgw_bi_log_entry> entries;
  bool truncated = false;;

  cls_rgw_bi_log_list_ret() = default;

  void encode(ceph::buffer::list& bl) const {
    ENCODE_START(1, 1, bl);
    encode(entries, bl);
    encode(truncated, bl);
    ENCODE_FINISH(bl);
  }

  void decode(ceph::buffer::list::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(entries, bl);
    decode(truncated, bl);
    DECODE_FINISH(bl);
  }

  void dump(ceph::Formatter* f) const;
  static void generate_test_instances(std::list<cls_rgw_bi_log_list_ret*>& ls);
};
WRITE_CLASS_ENCODER(cls_rgw_bi_log_list_ret)

struct cls_rgw_lc_get_next_entry_op {
  std::string marker;
  cls_rgw_lc_get_next_entry_op() = default;

  void encode(ceph::buffer::list& bl) const {
    ENCODE_START(1, 1, bl);
    encode(marker, bl);
    ENCODE_FINISH(bl);
  }

  void decode(ceph::buffer::list::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(marker, bl);
    DECODE_FINISH(bl);
  }
};
WRITE_CLASS_ENCODER(cls_rgw_lc_get_next_entry_op)

using rgw_lc_entry_t = std::pair<std::string, int>;

struct cls_rgw_lc_get_next_entry_ret {
  rgw_lc_entry_t entry;
  cls_rgw_lc_get_next_entry_ret() = default;

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

};
WRITE_CLASS_ENCODER(cls_rgw_lc_get_next_entry_ret)

struct cls_rgw_lc_get_entry_op {
  std::string marker;
  cls_rgw_lc_get_entry_op() = default;
  cls_rgw_lc_get_entry_op(std::string_view marker) : marker(marker) {}

  void encode(ceph::buffer::list& bl) const {
    ENCODE_START(1, 1, bl);
    encode(marker, bl);
    ENCODE_FINISH(bl);
  }

  void decode(ceph::buffer::list::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(marker, bl);
    DECODE_FINISH(bl);
  }
};
WRITE_CLASS_ENCODER(cls_rgw_lc_get_entry_op)

struct cls_rgw_lc_get_entry_ret {
  rgw_lc_entry_t entry;
  cls_rgw_lc_get_entry_ret() = default;
  cls_rgw_lc_get_entry_ret(rgw_lc_entry_t&& entry) : entry(std::move(entry)) {}

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

};
WRITE_CLASS_ENCODER(cls_rgw_lc_get_entry_ret)


struct cls_rgw_lc_rm_entry_op {
  rgw_lc_entry_t entry;
  cls_rgw_lc_rm_entry_op() = default;

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
};
WRITE_CLASS_ENCODER(cls_rgw_lc_rm_entry_op)

struct cls_rgw_lc_set_entry_op {
  rgw_lc_entry_t entry;
  cls_rgw_lc_set_entry_op() = default;

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
};
WRITE_CLASS_ENCODER(cls_rgw_lc_set_entry_op)

struct cls_rgw_lc_put_head_op {
  cls_rgw_lc_obj_head head;

  cls_rgw_lc_put_head_op() = default;

  void encode(ceph::buffer::list& bl) const {
    ENCODE_START(1, 1, bl);
    encode(head, bl);
    ENCODE_FINISH(bl);
  }

  void decode(ceph::buffer::list::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(head, bl);
    DECODE_FINISH(bl);
  }

};
WRITE_CLASS_ENCODER(cls_rgw_lc_put_head_op)

struct cls_rgw_lc_get_head_ret {
  cls_rgw_lc_obj_head head;

  cls_rgw_lc_get_head_ret() = default;

  void encode(ceph::buffer::list& bl) const {
    ENCODE_START(1, 1, bl);
    encode(head, bl);
    ENCODE_FINISH(bl);
  }

  void decode(ceph::buffer::list::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(head, bl);
    DECODE_FINISH(bl);
  }

};
WRITE_CLASS_ENCODER(cls_rgw_lc_get_head_ret)

struct cls_rgw_lc_list_entries_op {
  std::string marker;
  std::uint32_t max_entries = 0;

  cls_rgw_lc_list_entries_op() = default;

  void encode(ceph::buffer::list& bl) const {
    ENCODE_START(1, 1, bl);
    encode(marker, bl);
    encode(max_entries, bl);
    ENCODE_FINISH(bl);
  }

  void decode(ceph::buffer::list::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(marker, bl);
    decode(max_entries, bl);
    DECODE_FINISH(bl);
  }

};
WRITE_CLASS_ENCODER(cls_rgw_lc_list_entries_op)

struct cls_rgw_lc_list_entries_ret {
  std::map<std::string, int> entries;
  bool is_truncated{false};

  cls_rgw_lc_list_entries_ret() = default;

  void encode(ceph::buffer::list& bl) const {
    ENCODE_START(2, 1, bl);
    encode(entries, bl);
    encode(is_truncated, bl);
    ENCODE_FINISH(bl);
  }

  void decode(ceph::buffer::list::const_iterator& bl) {
    DECODE_START(2, bl);
    decode(entries, bl);
    if (struct_v >= 2) {
      decode(is_truncated, bl);
    }
    DECODE_FINISH(bl);
  }

};
WRITE_CLASS_ENCODER(cls_rgw_lc_list_entries_ret)

struct cls_rgw_reshard_add_op {
 cls_rgw_reshard_entry entry;

  cls_rgw_reshard_add_op() = default;

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
  static void generate_test_instances(std::list<cls_rgw_reshard_add_op*>& o);
  void dump(ceph::Formatter* f) const;
};
WRITE_CLASS_ENCODER(cls_rgw_reshard_add_op)

struct cls_rgw_reshard_list_op {
  std::uint32_t max{0};
  std::string marker;

  cls_rgw_reshard_list_op() = default;

  void encode(ceph::buffer::list& bl) const {
    ENCODE_START(1, 1, bl);
    encode(max, bl);
    encode(marker, bl);
    ENCODE_FINISH(bl);
  }

  void decode(ceph::buffer::list::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(max, bl);
    decode(marker, bl);
    DECODE_FINISH(bl);
  }
  static void generate_test_instances(std::list<cls_rgw_reshard_list_op*>& o);
  void dump(ceph::Formatter* f) const;
};
WRITE_CLASS_ENCODER(cls_rgw_reshard_list_op)


struct cls_rgw_reshard_list_ret {
  std::vector<cls_rgw_reshard_entry> entries;
  bool is_truncated{false};

  cls_rgw_reshard_list_ret() = default;

  void encode(ceph::buffer::list& bl) const {
    ENCODE_START(1, 1, bl);
    encode(entries, bl);
    encode(is_truncated, bl);
    ENCODE_FINISH(bl);
  }

  void decode(ceph::buffer::list::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(entries, bl);
    decode(is_truncated, bl);
    DECODE_FINISH(bl);
  }
  static void generate_test_instances(std::list<cls_rgw_reshard_list_ret*>& o);
  void dump(ceph::Formatter* f) const;
};
WRITE_CLASS_ENCODER(cls_rgw_reshard_list_ret)

struct cls_rgw_reshard_get_op {
  cls_rgw_reshard_entry entry;

  cls_rgw_reshard_get_op() = default;

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
  static void generate_test_instances(std::list<cls_rgw_reshard_get_op*>& o);
  void dump(ceph::Formatter* f) const;
};
WRITE_CLASS_ENCODER(cls_rgw_reshard_get_op)

struct cls_rgw_reshard_get_ret {
  cls_rgw_reshard_entry entry;

  cls_rgw_reshard_get_ret() = default;

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
  static void generate_test_instances(std::list<cls_rgw_reshard_get_ret*>& o);
  void dump(ceph::Formatter* f) const;
};
WRITE_CLASS_ENCODER(cls_rgw_reshard_get_ret)

struct cls_rgw_reshard_remove_op {
  std::string tenant;
  std::string bucket_name;
  std::string bucket_id;

  cls_rgw_reshard_remove_op() = default;

  void encode(ceph::buffer::list& bl) const {
    ENCODE_START(1, 1, bl);
    encode(tenant, bl);
    encode(bucket_name, bl);
    encode(bucket_id, bl);
    ENCODE_FINISH(bl);
  }

  void decode(ceph::buffer::list::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(tenant, bl);
    decode(bucket_name, bl);
    decode(bucket_id, bl);
    DECODE_FINISH(bl);
  }
  static void generate_test_instances(std::list<cls_rgw_reshard_remove_op*>& o);
  void dump(ceph::Formatter* f) const;
};
WRITE_CLASS_ENCODER(cls_rgw_reshard_remove_op)

struct cls_rgw_set_bucket_resharding_op  {
  cls_rgw_bucket_instance_entry entry;

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
  static void generate_test_instances(
    std::list<cls_rgw_set_bucket_resharding_op*>& o);
  void dump(ceph::Formatter* f) const;
};
WRITE_CLASS_ENCODER(cls_rgw_set_bucket_resharding_op)

struct cls_rgw_clear_bucket_resharding_op {
  void encode(ceph::buffer::list& bl) const {
    ENCODE_START(1, 1, bl);
    ENCODE_FINISH(bl);
  }

  void decode(ceph::buffer::list::const_iterator& bl) {
    DECODE_START(1, bl);
    DECODE_FINISH(bl);
  }
  static void generate_test_instances(
    std::list<cls_rgw_clear_bucket_resharding_op*>& o);
  void dump(ceph::Formatter* f) const;
};
WRITE_CLASS_ENCODER(cls_rgw_clear_bucket_resharding_op)

struct cls_rgw_guard_bucket_resharding_op  {
  int ret_err{0};

  void encode(ceph::buffer::list& bl) const {
    ENCODE_START(1, 1, bl);
    encode(ret_err, bl);
    ENCODE_FINISH(bl);
  }

  void decode(ceph::buffer::list::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(ret_err, bl);
    DECODE_FINISH(bl);
  }

  static void generate_test_instances(
    std::list<cls_rgw_guard_bucket_resharding_op*>& o);
  void dump(ceph::Formatter* f) const;
};
WRITE_CLASS_ENCODER(cls_rgw_guard_bucket_resharding_op)

struct cls_rgw_get_bucket_resharding_op  {
  void encode(ceph::buffer::list& bl) const {
    ENCODE_START(1, 1, bl);
    ENCODE_FINISH(bl);
  }

  void decode(ceph::buffer::list::const_iterator& bl) {
    DECODE_START(1, bl);
    DECODE_FINISH(bl);
  }

  static void generate_test_instances(
    std::list<cls_rgw_get_bucket_resharding_op*>& o);
  void dump(ceph::Formatter* f) const;
};
WRITE_CLASS_ENCODER(cls_rgw_get_bucket_resharding_op)

struct cls_rgw_get_bucket_resharding_ret  {
  cls_rgw_bucket_instance_entry new_instance;

  void encode(ceph::buffer::list& bl) const {
    ENCODE_START(1, 1, bl);
    encode(new_instance, bl);
    ENCODE_FINISH(bl);
  }

  void decode(ceph::buffer::list::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(new_instance, bl);
    DECODE_FINISH(bl);
  }

  static void generate_test_instances(
    std::list<cls_rgw_get_bucket_resharding_ret*>& o);
  void dump(ceph::Formatter*f) const;
};
WRITE_CLASS_ENCODER(cls_rgw_get_bucket_resharding_ret)

#endif /* CEPH_CLS_RGW_OPS_H */
