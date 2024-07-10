// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include "cls/rgw/cls_rgw_types.h"

struct rgw_cls_tag_timeout_op
{
  uint64_t tag_timeout;

  rgw_cls_tag_timeout_op() : tag_timeout(0) {}

  void encode(ceph::buffer::list &bl) const {
    ENCODE_START(1, 1, bl);
    encode(tag_timeout, bl);
    ENCODE_FINISH(bl);
  }
  void decode(ceph::buffer::list::const_iterator &bl) {
    DECODE_START(1, bl);
    decode(tag_timeout, bl);
    DECODE_FINISH(bl);
  }
  void dump(ceph::Formatter *f) const;
  static void generate_test_instances(std::list<rgw_cls_tag_timeout_op*>& ls);
};
WRITE_CLASS_ENCODER(rgw_cls_tag_timeout_op)

struct rgw_cls_obj_prepare_op
{
  RGWModifyOp op;
  cls_rgw_obj_key key;
  std::string tag;
  std::string locator;
  bool log_op;
  uint16_t bilog_flags;
  rgw_zone_set zones_trace;

  rgw_cls_obj_prepare_op() : op(CLS_RGW_OP_UNKNOWN), log_op(false), bilog_flags(0) {}

  void encode(ceph::buffer::list &bl) const {
    ENCODE_START(7, 5, bl);
    uint8_t c = (uint8_t)op;
    encode(c, bl);
    encode(tag, bl);
    encode(locator, bl);
    encode(log_op, bl);
    encode(key, bl);
    encode(bilog_flags, bl);
    encode(zones_trace, bl);
    ENCODE_FINISH(bl);
  }
  void decode(ceph::buffer::list::const_iterator &bl) {
    DECODE_START_LEGACY_COMPAT_LEN(7, 3, 3, bl);
    uint8_t c;
    decode(c, bl);
    op = (RGWModifyOp)c;
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
  void dump(ceph::Formatter *f) const;
  static void generate_test_instances(std::list<rgw_cls_obj_prepare_op*>& o);
};
WRITE_CLASS_ENCODER(rgw_cls_obj_prepare_op)

struct rgw_cls_obj_complete_op
{
  RGWModifyOp op;
  cls_rgw_obj_key key;
  std::string locator;
  rgw_bucket_entry_ver ver;
  rgw_bucket_dir_entry_meta meta;
  std::string tag;
  bool log_op;
  uint16_t bilog_flags;

  std::list<cls_rgw_obj_key> remove_objs;
  rgw_zone_set zones_trace;

  rgw_cls_obj_complete_op() : op(CLS_RGW_OP_ADD), log_op(false), bilog_flags(0) {}

  void encode(ceph::buffer::list &bl) const {
    ENCODE_START(9, 7, bl);
    uint8_t c = (uint8_t)op;
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
  void decode(ceph::buffer::list::const_iterator &bl) {
    DECODE_START_LEGACY_COMPAT_LEN(9, 3, 3, bl);
    uint8_t c;
    decode(c, bl);
    op = (RGWModifyOp)c;
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
      std::list<std::string> old_remove_objs;
      decode(old_remove_objs, bl);

      for (auto  iter = old_remove_objs.begin();
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
  void dump(ceph::Formatter *f) const;
  static void generate_test_instances(std::list<rgw_cls_obj_complete_op*>& o);
};
WRITE_CLASS_ENCODER(rgw_cls_obj_complete_op)

struct rgw_cls_link_olh_op {
  cls_rgw_obj_key key;
  std::string olh_tag;
  bool delete_marker;
  std::string op_tag;
  rgw_bucket_dir_entry_meta meta;
  uint64_t olh_epoch;
  bool log_op;
  uint16_t bilog_flags;
  ceph::real_time unmod_since; /* only create delete marker if newer then this */
  bool high_precision_time;
  rgw_zone_set zones_trace;

  rgw_cls_link_olh_op() : delete_marker(false), olh_epoch(0), log_op(false), bilog_flags(0), high_precision_time(false) {}

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
    uint64_t t = ceph::real_clock::to_time_t(unmod_since);
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
      uint64_t t;
      decode(t, bl);
      unmod_since = ceph::real_clock::from_time_t(static_cast<time_t>(t));
    }
    if (struct_v >= 3) {
      uint64_t t;
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

  static void generate_test_instances(std::list<rgw_cls_link_olh_op *>& o);
  void dump(ceph::Formatter *f) const;
};
WRITE_CLASS_ENCODER(rgw_cls_link_olh_op)

struct rgw_cls_unlink_instance_op {
  cls_rgw_obj_key key;
  std::string op_tag;
  uint64_t olh_epoch;
  bool log_op;
  uint16_t bilog_flags;
  std::string olh_tag;
  rgw_zone_set zones_trace;

  rgw_cls_unlink_instance_op() : olh_epoch(0), log_op(false), bilog_flags(0) {}

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

  static void generate_test_instances(std::list<rgw_cls_unlink_instance_op *>& o);
  void dump(ceph::Formatter *f) const;
};
WRITE_CLASS_ENCODER(rgw_cls_unlink_instance_op)

struct rgw_cls_read_olh_log_op
{
  cls_rgw_obj_key olh;
  uint64_t ver_marker;
  std::string olh_tag;

  rgw_cls_read_olh_log_op() : ver_marker(0) {}

  void encode(ceph::buffer::list &bl) const {
    ENCODE_START(1, 1, bl);
    encode(olh, bl);
    encode(ver_marker, bl);
    encode(olh_tag, bl);
    ENCODE_FINISH(bl);
  }
  void decode(ceph::buffer::list::const_iterator &bl) {
    DECODE_START(1, bl);
    decode(olh, bl);
    decode(ver_marker, bl);
    decode(olh_tag, bl);
    DECODE_FINISH(bl);
  }
  static void generate_test_instances(std::list<rgw_cls_read_olh_log_op *>& o);
  void dump(ceph::Formatter *f) const;
};
WRITE_CLASS_ENCODER(rgw_cls_read_olh_log_op)


struct rgw_cls_read_olh_log_ret
{
  std::map<uint64_t, std::vector<rgw_bucket_olh_log_entry> > log;
  bool is_truncated;

  rgw_cls_read_olh_log_ret() : is_truncated(false) {}

  void encode(ceph::buffer::list &bl) const {
    ENCODE_START(1, 1, bl);
    encode(log, bl);
    encode(is_truncated, bl);
    ENCODE_FINISH(bl);
  }
  void decode(ceph::buffer::list::const_iterator &bl) {
    DECODE_START(1, bl);
    decode(log, bl);
    decode(is_truncated, bl);
    DECODE_FINISH(bl);
  }
  static void generate_test_instances(std::list<rgw_cls_read_olh_log_ret *>& o);
  void dump(ceph::Formatter *f) const;
};
WRITE_CLASS_ENCODER(rgw_cls_read_olh_log_ret)

struct rgw_cls_trim_olh_log_op
{
  cls_rgw_obj_key olh;
  uint64_t ver;
  std::string olh_tag;

  rgw_cls_trim_olh_log_op() : ver(0) {}

  void encode(ceph::buffer::list &bl) const {
    ENCODE_START(1, 1, bl);
    encode(olh, bl);
    encode(ver, bl);
    encode(olh_tag, bl);
    ENCODE_FINISH(bl);
  }
  void decode(ceph::buffer::list::const_iterator &bl) {
    DECODE_START(1, bl);
    decode(olh, bl);
    decode(ver, bl);
    decode(olh_tag, bl);
    DECODE_FINISH(bl);
  }
  static void generate_test_instances(std::list<rgw_cls_trim_olh_log_op *>& o);
  void dump(ceph::Formatter *f) const;
};
WRITE_CLASS_ENCODER(rgw_cls_trim_olh_log_op)

struct rgw_cls_bucket_clear_olh_op {
  cls_rgw_obj_key key;
  std::string olh_tag;

  rgw_cls_bucket_clear_olh_op() {}

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

  static void generate_test_instances(std::list<rgw_cls_bucket_clear_olh_op *>& o);
  void dump(ceph::Formatter *f) const;
};
WRITE_CLASS_ENCODER(rgw_cls_bucket_clear_olh_op)

struct rgw_cls_list_op
{
  cls_rgw_obj_key start_obj;
  uint32_t num_entries;
  std::string filter_prefix;
  bool list_versions;
  std::string delimiter;

  rgw_cls_list_op() : num_entries(0), list_versions(false) {}

  void encode(ceph::buffer::list &bl) const {
    ENCODE_START(6, 4, bl);
    encode(num_entries, bl);
    encode(filter_prefix, bl);
    encode(start_obj, bl);
    encode(list_versions, bl);
    encode(delimiter, bl);
    ENCODE_FINISH(bl);
  }
  void decode(ceph::buffer::list::const_iterator &bl) {
    DECODE_START_LEGACY_COMPAT_LEN(6, 2, 2, bl);
    if (struct_v < 4) {
      decode(start_obj.name, bl);
    }
    decode(num_entries, bl);
    if (struct_v >= 3) {
      decode(filter_prefix, bl);
    }
    if (struct_v >= 4) {
      decode(start_obj, bl);
    }
    if (struct_v >= 5) {
      decode(list_versions, bl);
    }
    if (struct_v >= 6) {
      decode(delimiter, bl);
    }
    DECODE_FINISH(bl);
  }
  void dump(ceph::Formatter *f) const;
  static void generate_test_instances(std::list<rgw_cls_list_op*>& o);
};
WRITE_CLASS_ENCODER(rgw_cls_list_op)

struct rgw_cls_list_ret {
  rgw_bucket_dir dir;
  bool is_truncated;

  // if is_truncated is true, starting marker for next iteration; this
  // is necessary as it's possible after maximum number of tries we
  // still might have zero entries to return, in which case we have to
  // at least move the ball forward
  cls_rgw_obj_key marker;

  // cls_filtered is not transmitted; it is assumed true for versions
  // on/after 3 and false for prior versions; this allows the rgw
  // layer to know when an older osd (cls) does not do the filtering
  bool cls_filtered;

  rgw_cls_list_ret() :
    is_truncated(false),
    cls_filtered(true)
  {}

  void encode(ceph::buffer::list &bl) const {
    ENCODE_START(4, 2, bl);
    encode(dir, bl);
    encode(is_truncated, bl);
    encode(marker, bl);
    ENCODE_FINISH(bl);
  }
  void decode(ceph::buffer::list::const_iterator &bl) {
    DECODE_START_LEGACY_COMPAT_LEN(4, 2, 2, bl);
    decode(dir, bl);
    decode(is_truncated, bl);
    cls_filtered = struct_v >= 3;
    if (struct_v >= 4) {
      decode(marker, bl);
    }
    DECODE_FINISH(bl);
  }
  void dump(ceph::Formatter *f) const;
  static void generate_test_instances(std::list<rgw_cls_list_ret*>& o);
};
WRITE_CLASS_ENCODER(rgw_cls_list_ret)

struct rgw_cls_check_index_ret
{
  rgw_bucket_dir_header existing_header;
  rgw_bucket_dir_header calculated_header;

  rgw_cls_check_index_ret() {}

  void encode(ceph::buffer::list &bl) const {
    ENCODE_START(1, 1, bl);
    encode(existing_header, bl);
    encode(calculated_header, bl);
    ENCODE_FINISH(bl);
  }
  void decode(ceph::buffer::list::const_iterator &bl) {
    DECODE_START(1, bl);
    decode(existing_header, bl);
    decode(calculated_header, bl);
    DECODE_FINISH(bl);
  }
  void dump(ceph::Formatter *f) const;
  static void generate_test_instances(std::list<rgw_cls_check_index_ret *>& o);
};
WRITE_CLASS_ENCODER(rgw_cls_check_index_ret)

struct rgw_cls_bucket_update_stats_op
{
  bool absolute{false};
  std::map<RGWObjCategory, rgw_bucket_category_stats> stats;

  rgw_cls_bucket_update_stats_op() {}

  void encode(ceph::buffer::list &bl) const {
    ENCODE_START(1, 1, bl);
    encode(absolute, bl);
    encode(stats, bl);
    ENCODE_FINISH(bl);
  }
  void decode(ceph::buffer::list::const_iterator &bl) {
    DECODE_START(1, bl);
    decode(absolute, bl);
    decode(stats, bl);
    DECODE_FINISH(bl);
  }
  void dump(ceph::Formatter *f) const;
  static void generate_test_instances(std::list<rgw_cls_bucket_update_stats_op *>& o);
};
WRITE_CLASS_ENCODER(rgw_cls_bucket_update_stats_op)

struct rgw_cls_obj_remove_op {
  std::list<std::string> keep_attr_prefixes;

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

  void dump(ceph::Formatter *f) const {
    encode_json("keep_attr_prefixes", keep_attr_prefixes, f);
  }

  static void generate_test_instances(std::list<rgw_cls_obj_remove_op*>& o) {
    o.push_back(new rgw_cls_obj_remove_op);
    o.back()->keep_attr_prefixes.push_back("keep_attr_prefixes1");
    o.back()->keep_attr_prefixes.push_back("keep_attr_prefixes2");
    o.back()->keep_attr_prefixes.push_back("keep_attr_prefixes3");
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

  void dump(ceph::Formatter *f) const {
    f->dump_string("attr", attr);
  }

  static void generate_test_instances(std::list<rgw_cls_obj_store_pg_ver_op*>& o) {
    o.push_back(new rgw_cls_obj_store_pg_ver_op);
    o.back()->attr = "attr";
  }
};
WRITE_CLASS_ENCODER(rgw_cls_obj_store_pg_ver_op)

struct rgw_cls_obj_check_attrs_prefix {
  std::string check_prefix;
  bool fail_if_exist;

  rgw_cls_obj_check_attrs_prefix() : fail_if_exist(false) {}

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

  void dump(ceph::Formatter *f) const {
    f->dump_string("check_prefix", check_prefix);
    f->dump_bool("fail_if_exist", fail_if_exist);
  }

  static void generate_test_instances(std::list<rgw_cls_obj_check_attrs_prefix*>& o) {
    o.push_back(new rgw_cls_obj_check_attrs_prefix);
    o.back()->check_prefix = "prefix";
    o.back()->fail_if_exist = true;
  }
};
WRITE_CLASS_ENCODER(rgw_cls_obj_check_attrs_prefix)

struct rgw_cls_obj_check_mtime {
  ceph::real_time mtime;
  RGWCheckMTimeType type;
  bool high_precision_time;

  rgw_cls_obj_check_mtime() : type(CLS_RGW_CHECK_TIME_MTIME_EQ), high_precision_time(false) {}

  void encode(ceph::buffer::list& bl) const {
    ENCODE_START(2, 1, bl);
    encode(mtime, bl);
    encode((uint8_t)type, bl);
    encode(high_precision_time, bl);
    ENCODE_FINISH(bl);
  }

  void decode(ceph::buffer::list::const_iterator& bl) {
    DECODE_START(2, bl);
    decode(mtime, bl);
    uint8_t c;
    decode(c, bl);
    type = (RGWCheckMTimeType)c;
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

  void dump(ceph::Formatter *f) const {
    f->dump_object("info", info);
    f->dump_string("user", user.to_str());
  }

  static void generate_test_instances(std::list<rgw_cls_usage_log_add_op*>& o) {
    o.push_back(new rgw_cls_usage_log_add_op);
  }
};
WRITE_CLASS_ENCODER(rgw_cls_usage_log_add_op)

struct rgw_cls_bi_get_op {
  cls_rgw_obj_key key;
  BIIndexType type; /* namespace: plain, instance, olh */

  rgw_cls_bi_get_op() : type(BIIndexType::Plain) {}

  void encode(ceph::buffer::list& bl) const {
    ENCODE_START(1, 1, bl);
    encode(key, bl);
    encode((uint8_t)type, bl);
    ENCODE_FINISH(bl);
  }

  void decode(ceph::buffer::list::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(key, bl);
    uint8_t c;
    decode(c, bl);
    type = (BIIndexType)c;
    DECODE_FINISH(bl);
  }

  void dump(ceph::Formatter *f) const {
    f->dump_stream("key") << key;
    f->dump_int("type", (int)type);
  }

  static void generate_test_instances(std::list<rgw_cls_bi_get_op*>& o) {
    o.push_back(new rgw_cls_bi_get_op);
    o.push_back(new rgw_cls_bi_get_op);
    o.back()->key.name = "key";
    o.back()->key.instance = "instance";
    o.back()->type = BIIndexType::Plain;
  }
};
WRITE_CLASS_ENCODER(rgw_cls_bi_get_op)

struct rgw_cls_bi_get_ret {
  rgw_cls_bi_entry entry;

  rgw_cls_bi_get_ret() {}

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

  void dump(ceph::Formatter *f) const {
    f->dump_string("entry", entry.idx);
  }

  static void generate_test_instances(std::list<rgw_cls_bi_get_ret*>& o) {
    o.push_back(new rgw_cls_bi_get_ret);
    o.back()->entry.idx = "entry";
  }
};
WRITE_CLASS_ENCODER(rgw_cls_bi_get_ret)

struct rgw_cls_bi_put_op {
  rgw_cls_bi_entry entry;

  rgw_cls_bi_put_op() {}

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

  void dump(ceph::Formatter *f) const {
    f->dump_string("entry", entry.idx);
  }

  static void generate_test_instances(std::list<rgw_cls_bi_put_op*>& o) {
    o.push_back(new rgw_cls_bi_put_op);
    o.push_back(new rgw_cls_bi_put_op);
    o.back()->entry.idx = "entry";
  }
};
WRITE_CLASS_ENCODER(rgw_cls_bi_put_op)

struct rgw_cls_bi_list_op {
  uint32_t max;
  std::string name_filter; // limit result to one object and its instances
  std::string marker;

  rgw_cls_bi_list_op() : max(0) {}

  void encode(ceph::buffer::list& bl) const {
    ENCODE_START(1, 1, bl);
    encode(max, bl);
    encode(name_filter, bl);
    encode(marker, bl);
    ENCODE_FINISH(bl);
  }

  void decode(ceph::buffer::list::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(max, bl);
    decode(name_filter, bl);
    decode(marker, bl);
    DECODE_FINISH(bl);
  }

  void dump(ceph::Formatter *f) const {
    f->dump_unsigned("max", max);
    f->dump_string("name_filter", name_filter);
    f->dump_string("marker", marker);
  }

  static void generate_test_instances(std::list<rgw_cls_bi_list_op*>& o) {
    o.push_back(new rgw_cls_bi_list_op);
    o.push_back(new rgw_cls_bi_list_op);
    o.back()->max = 100;
    o.back()->name_filter = "name_filter";
    o.back()->marker = "marker";
  }
};
WRITE_CLASS_ENCODER(rgw_cls_bi_list_op)

struct rgw_cls_bi_list_ret {
  std::list<rgw_cls_bi_entry> entries;
  bool is_truncated;

  rgw_cls_bi_list_ret() : is_truncated(false) {}

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

  void dump(ceph::Formatter *f) const {
    f->dump_bool("is_truncated", is_truncated);
    encode_json("entries", entries, f);
  }

  static void generate_test_instances(std::list<rgw_cls_bi_list_ret*>& o) {
    o.push_back(new rgw_cls_bi_list_ret);
    o.push_back(new rgw_cls_bi_list_ret);
    o.back()->entries.push_back(rgw_cls_bi_entry());
    o.back()->entries.push_back(rgw_cls_bi_entry());
    o.back()->entries.back().idx = "entry";
    o.back()->is_truncated = true;
  }
};
WRITE_CLASS_ENCODER(rgw_cls_bi_list_ret)

struct rgw_cls_usage_log_read_op {
  uint64_t start_epoch;
  uint64_t end_epoch;
  std::string owner;
  std::string bucket;

  std::string iter;  // should be empty for the first call, non empty for subsequent calls
  uint32_t max_entries;

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

  void dump(ceph::Formatter *f) const {
    f->dump_unsigned("start_epoch", start_epoch);
    f->dump_unsigned("end_epoch", end_epoch);
    f->dump_string("owner", owner);
    f->dump_string("bucket", bucket);
    f->dump_string("iter", iter);
    f->dump_unsigned("max_entries", max_entries);
  }

  static void generate_test_instances(std::list<rgw_cls_usage_log_read_op*>& o) {
    o.push_back(new rgw_cls_usage_log_read_op);
    o.back()->start_epoch = 1;
    o.back()->end_epoch = 2;
    o.back()->owner = "owner";
    o.back()->bucket = "bucket";
    o.back()->iter = "iter";
    o.back()->max_entries = 100;
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

  void dump(ceph::Formatter *f) const {
    f->dump_bool("truncated", truncated);
    f->dump_string("next_iter", next_iter);
    encode_json("usage", usage, f);
  }

  static void generate_test_instances(std::list<rgw_cls_usage_log_read_ret*>& o) {
    o.push_back(new rgw_cls_usage_log_read_ret);
    o.back()->next_iter = "123";
    o.back()->truncated = true;
    o.back()->usage.clear();
    o.push_back(new rgw_cls_usage_log_read_ret);
    o.back()->usage[rgw_user_bucket("user1", "bucket1")] = rgw_usage_log_entry();
    o.back()->usage[rgw_user_bucket("user2", "bucket2")] = rgw_usage_log_entry();
    o.back()->truncated = true;
    o.back()->next_iter = "next_iter";
  }
};
WRITE_CLASS_ENCODER(rgw_cls_usage_log_read_ret)

struct rgw_cls_usage_log_trim_op {
  uint64_t start_epoch;
  uint64_t end_epoch;
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

  void dump(ceph::Formatter *f) const {
    f->dump_unsigned("start_epoch", start_epoch);
    f->dump_unsigned("end_epoch", end_epoch);
    f->dump_string("user", user);
    f->dump_string("bucket", bucket);
  }

  static void generate_test_instances(std::list<rgw_cls_usage_log_trim_op*>& ls) {
    rgw_cls_usage_log_trim_op *m = new rgw_cls_usage_log_trim_op;
    m->start_epoch = 1;
    m->end_epoch = 2;
    m->user = "user";
    m->bucket = "bucket";
    ls.push_back(m);
  }
};
WRITE_CLASS_ENCODER(rgw_cls_usage_log_trim_op)

struct cls_rgw_gc_set_entry_op {
  uint32_t expiration_secs;
  cls_rgw_gc_obj_info info;
  cls_rgw_gc_set_entry_op() : expiration_secs(0) {}

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

  void dump(ceph::Formatter *f) const;
  static void generate_test_instances(std::list<cls_rgw_gc_set_entry_op*>& ls);

  size_t estimate_encoded_size() const {
    constexpr size_t start_overhead = sizeof(__u8) + sizeof(__u8) + sizeof(ceph_le32); // version and length prefix
    constexpr size_t expr_secs_overhead = sizeof(__u32); // expiration_seconds_overhead
    return start_overhead + expr_secs_overhead + info.estimate_encoded_size();
  }
};
WRITE_CLASS_ENCODER(cls_rgw_gc_set_entry_op)

struct cls_rgw_gc_defer_entry_op {
  uint32_t expiration_secs;
  std::string tag;
  cls_rgw_gc_defer_entry_op() : expiration_secs(0) {}

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

  void dump(ceph::Formatter *f) const;
  static void generate_test_instances(std::list<cls_rgw_gc_defer_entry_op*>& ls);
};
WRITE_CLASS_ENCODER(cls_rgw_gc_defer_entry_op)

struct cls_rgw_gc_list_op {
  std::string marker;
  uint32_t max;
  bool expired_only;

  cls_rgw_gc_list_op() : max(0), expired_only(true) {}

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

  void dump(ceph::Formatter *f) const;
  static void generate_test_instances(std::list<cls_rgw_gc_list_op*>& ls);
};
WRITE_CLASS_ENCODER(cls_rgw_gc_list_op)

struct cls_rgw_gc_list_ret {
  std::list<cls_rgw_gc_obj_info> entries;
  std::string next_marker;
  bool truncated;

  cls_rgw_gc_list_ret() : truncated(false) {}

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

  void dump(ceph::Formatter *f) const;
  static void generate_test_instances(std::list<cls_rgw_gc_list_ret*>& ls);
};
WRITE_CLASS_ENCODER(cls_rgw_gc_list_ret)

struct cls_rgw_gc_remove_op {
  std::vector<std::string> tags;

  cls_rgw_gc_remove_op() {}

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

  void dump(ceph::Formatter *f) const;
  static void generate_test_instances(std::list<cls_rgw_gc_remove_op*>& ls);
};
WRITE_CLASS_ENCODER(cls_rgw_gc_remove_op)

struct cls_rgw_bi_log_list_op {
  std::string marker;
  uint32_t max;

  cls_rgw_bi_log_list_op() : max(0) {}

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

  void dump(ceph::Formatter *f) const;
  static void generate_test_instances(std::list<cls_rgw_bi_log_list_op*>& ls);
};
WRITE_CLASS_ENCODER(cls_rgw_bi_log_list_op)

struct cls_rgw_bi_log_trim_op {
  std::string start_marker;
  std::string end_marker;

  cls_rgw_bi_log_trim_op() {}

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

  void dump(ceph::Formatter *f) const;
  static void generate_test_instances(std::list<cls_rgw_bi_log_trim_op*>& ls);
};
WRITE_CLASS_ENCODER(cls_rgw_bi_log_trim_op)

struct cls_rgw_bi_log_list_ret {
  std::list<rgw_bi_log_entry> entries;
  bool truncated;

  cls_rgw_bi_log_list_ret() : truncated(false) {}

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

  void dump(ceph::Formatter *f) const;
  static void generate_test_instances(std::list<cls_rgw_bi_log_list_ret*>& ls);
};
WRITE_CLASS_ENCODER(cls_rgw_bi_log_list_ret)

struct cls_rgw_lc_get_next_entry_op {
  std::string marker;
  cls_rgw_lc_get_next_entry_op() {}

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

struct cls_rgw_lc_get_next_entry_ret {
  cls_rgw_lc_entry entry;

  cls_rgw_lc_get_next_entry_ret() {}

  void encode(ceph::buffer::list& bl) const {
    ENCODE_START(2, 2, bl);
    encode(entry, bl);
    ENCODE_FINISH(bl);
  }

  void decode(ceph::buffer::list::const_iterator& bl) {
    DECODE_START(2, bl);
    if (struct_v < 2) {
      std::pair<std::string, int> oe;
      decode(oe, bl);
      entry = {oe.first, 0 /* start */, uint32_t(oe.second)};
    } else {
      decode(entry, bl);
    }
    DECODE_FINISH(bl);
  }

};
WRITE_CLASS_ENCODER(cls_rgw_lc_get_next_entry_ret)

struct cls_rgw_lc_get_entry_op {
  std::string marker;
  cls_rgw_lc_get_entry_op() {}
  cls_rgw_lc_get_entry_op(const std::string& _marker) : marker(_marker) {}

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
  cls_rgw_lc_entry entry;

  cls_rgw_lc_get_entry_ret() {}
  cls_rgw_lc_get_entry_ret(cls_rgw_lc_entry&& _entry)
    : entry(std::move(_entry)) {}

  void encode(ceph::buffer::list& bl) const {
    ENCODE_START(2, 2, bl);
    encode(entry, bl);
    ENCODE_FINISH(bl);
  }

  void decode(ceph::buffer::list::const_iterator& bl) {
    DECODE_START(2, bl);
    if (struct_v < 2) {
      /* there was an unmarked change in the encoding during v1, so
       * if the sender version is v1, try decoding both ways (sorry) */
      ceph::buffer::list::const_iterator save_bl = bl;
      try {
	decode(entry, bl);
      } catch (ceph::buffer::error& e) {
	std::pair<std::string, int> oe;
	bl = save_bl;
	decode(oe, bl);
	entry.bucket = oe.first;
	entry.start_time = 0;
	entry.status = oe.second;
      }
    } else {
      decode(entry, bl);
    }
    DECODE_FINISH(bl);
  }
  void dump(ceph::Formatter *f) const;
  static void generate_test_instances(std::list<cls_rgw_lc_get_entry_ret*>& ls);
};
WRITE_CLASS_ENCODER(cls_rgw_lc_get_entry_ret)

struct cls_rgw_lc_rm_entry_op {
  cls_rgw_lc_entry entry;
  cls_rgw_lc_rm_entry_op() {}

  void encode(ceph::buffer::list& bl) const {
    ENCODE_START(2, 2, bl);
    encode(entry, bl);
    ENCODE_FINISH(bl);
  }

  void decode(ceph::buffer::list::const_iterator& bl) {
    DECODE_START(2, bl);
    if (struct_v < 2) {
      std::pair<std::string, int> oe;
      decode(oe, bl);
      entry = {oe.first, 0 /* start */, uint32_t(oe.second)};
    } else {
      decode(entry, bl);
    }
    DECODE_FINISH(bl);
  }
};
WRITE_CLASS_ENCODER(cls_rgw_lc_rm_entry_op)

struct cls_rgw_lc_set_entry_op {
  cls_rgw_lc_entry entry;
  cls_rgw_lc_set_entry_op() {}

  void encode(ceph::buffer::list& bl) const {
    ENCODE_START(2, 2, bl);
    encode(entry, bl);
    ENCODE_FINISH(bl);
  }

  void decode(ceph::buffer::list::const_iterator& bl) {
    DECODE_START(2, bl);
    if (struct_v < 2) {
      std::pair<std::string, int> oe;
      decode(oe, bl);
      entry = {oe.first, 0 /* start */, uint32_t(oe.second)};
    } else {
      decode(entry, bl);
    }
    DECODE_FINISH(bl);
  }

  void dump(ceph::Formatter *f) const {
    f->dump_string("bucket", entry.bucket);
    f->dump_int("start_time", entry.start_time);
    f->dump_int("status", entry.status);
  }

  static void generate_test_instances(std::list<cls_rgw_lc_set_entry_op*>& ls) {
    ls.push_back(new cls_rgw_lc_set_entry_op);
    ls.push_back(new cls_rgw_lc_set_entry_op);
    ls.back()->entry.bucket = "foo";
    ls.back()->entry.start_time = 123;
    ls.back()->entry.status = 456;
  }
};
WRITE_CLASS_ENCODER(cls_rgw_lc_set_entry_op)

struct cls_rgw_lc_put_head_op {
  cls_rgw_lc_obj_head head;


  cls_rgw_lc_put_head_op() {}

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

  cls_rgw_lc_get_head_ret() {}

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
  uint32_t max_entries = 0;
  uint8_t compat_v{0};

  cls_rgw_lc_list_entries_op() {}

  void encode(ceph::buffer::list& bl) const {
    ENCODE_START(3, 1, bl);
    encode(marker, bl);
    encode(max_entries, bl);
    ENCODE_FINISH(bl);
  }

  void decode(ceph::buffer::list::const_iterator& bl) {
    DECODE_START(3, bl);
    compat_v = struct_v;
    decode(marker, bl);
    decode(max_entries, bl);
    DECODE_FINISH(bl);
  }

};
WRITE_CLASS_ENCODER(cls_rgw_lc_list_entries_op)

struct cls_rgw_lc_list_entries_ret {
  std::vector<cls_rgw_lc_entry> entries;
  bool is_truncated{false};
  uint8_t compat_v;

cls_rgw_lc_list_entries_ret(uint8_t compat_v = 3)
  : compat_v(compat_v) {}

  void encode(ceph::buffer::list& bl) const {
    ENCODE_START(compat_v, 1, bl);
    if (compat_v <= 2) {
      std::map<std::string, int> oes;
      std::for_each(entries.begin(), entries.end(),
                   [&oes](const cls_rgw_lc_entry& elt)
                     {oes.insert({elt.bucket, elt.status});});
      encode(oes, bl);
    } else {
      encode(entries, bl);
    }
    encode(is_truncated, bl);
    ENCODE_FINISH(bl);
  }

  void decode(ceph::buffer::list::const_iterator& bl) {
    DECODE_START(3, bl);
    compat_v = struct_v;
    if (struct_v <= 2) {
      std::map<std::string, int> oes;
      decode(oes, bl);
      std::for_each(oes.begin(), oes.end(),
		    [this](const std::pair<std::string, int>& oe)
		      {entries.push_back({oe.first, 0 /* start */,
					  uint32_t(oe.second)});});
    } else {
      decode(entries, bl);
    }
    if (struct_v >= 2) {
      decode(is_truncated, bl);
    }
    DECODE_FINISH(bl);
  }
};
WRITE_CLASS_ENCODER(cls_rgw_lc_list_entries_ret)

struct cls_rgw_mp_upload_part_info_update_op {
  std::string part_key;
  RGWUploadPartInfo info;

  cls_rgw_mp_upload_part_info_update_op() {}

  void encode(buffer::list& bl) const {
    ENCODE_START(1, 1, bl);
    encode(part_key, bl);
    encode(info, bl);
    ENCODE_FINISH(bl);
  }

  void decode(buffer::list::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(part_key, bl);
    decode(info, bl);
    DECODE_FINISH(bl);
  }

  static void generate_test_instances(std::list<cls_rgw_mp_upload_part_info_update_op*>& ls);
  void dump(Formatter* f) const;
};
WRITE_CLASS_ENCODER(cls_rgw_mp_upload_part_info_update_op)

struct cls_rgw_reshard_add_op {
  cls_rgw_reshard_entry entry;

  // true -> will not overwrite existing entry
  bool create_only {false};

  cls_rgw_reshard_add_op() {}

  void encode(ceph::buffer::list& bl) const {
    ENCODE_START(2, 1, bl);
    encode(entry, bl);
    encode(create_only, bl);
    ENCODE_FINISH(bl);
  }

  void decode(ceph::buffer::list::const_iterator& bl) {
    DECODE_START(2, bl);
    decode(entry, bl);
    create_only = false;
    if (struct_v >= 2) {
      decode(create_only, bl);
    }
    DECODE_FINISH(bl);
  }
  static void generate_test_instances(std::list<cls_rgw_reshard_add_op*>& o);
  void dump(ceph::Formatter *f) const;
};
WRITE_CLASS_ENCODER(cls_rgw_reshard_add_op)

struct cls_rgw_reshard_list_op {
  uint32_t max{0};
  std::string marker;

  cls_rgw_reshard_list_op() {}

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
  void dump(ceph::Formatter *f) const;
};
WRITE_CLASS_ENCODER(cls_rgw_reshard_list_op)


struct cls_rgw_reshard_list_ret {
  std::list<cls_rgw_reshard_entry> entries;
  bool is_truncated{false};

  cls_rgw_reshard_list_ret() {}

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
  void dump(ceph::Formatter *f) const;
};
WRITE_CLASS_ENCODER(cls_rgw_reshard_list_ret)

struct cls_rgw_reshard_get_op {
  cls_rgw_reshard_entry entry;

  cls_rgw_reshard_get_op() {}

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
  void dump(ceph::Formatter *f) const;
};
WRITE_CLASS_ENCODER(cls_rgw_reshard_get_op)

struct cls_rgw_reshard_get_ret {
  cls_rgw_reshard_entry entry;

  cls_rgw_reshard_get_ret() {}

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
  void dump(ceph::Formatter *f) const;
};
WRITE_CLASS_ENCODER(cls_rgw_reshard_get_ret)

struct cls_rgw_reshard_remove_op {
  std::string tenant;
  std::string bucket_name;
  std::string bucket_id;

  cls_rgw_reshard_remove_op() {}

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
  void dump(ceph::Formatter *f) const;
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
  static void generate_test_instances(std::list<cls_rgw_set_bucket_resharding_op*>& o);
  void dump(ceph::Formatter *f) const;
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
  static void generate_test_instances(std::list<cls_rgw_clear_bucket_resharding_op*>& o);
  void dump(ceph::Formatter *f) const;
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

  static void generate_test_instances(std::list<cls_rgw_guard_bucket_resharding_op*>& o);
  void dump(ceph::Formatter *f) const;
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

  static void generate_test_instances(std::list<cls_rgw_get_bucket_resharding_op*>& o);
  void dump(ceph::Formatter *f) const;
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

  static void generate_test_instances(std::list<cls_rgw_get_bucket_resharding_ret*>& o);
  void dump(ceph::Formatter *f) const;
};
WRITE_CLASS_ENCODER(cls_rgw_get_bucket_resharding_ret)
