#ifndef CEPH_CLS_RGW_OPS_H
#define CEPH_CLS_RGW_OPS_H

#include <map>

#include "include/types.h"
#include "cls/rgw/cls_rgw_types.h"

struct rgw_cls_tag_timeout_op
{
  uint64_t tag_timeout;

  rgw_cls_tag_timeout_op() : tag_timeout(0) {}

  void encode(bufferlist &bl) const {
    ENCODE_START(1, 1, bl);
    ::encode(tag_timeout, bl);
    ENCODE_FINISH(bl);
  }
  void decode(bufferlist::iterator &bl) {
    DECODE_START(1, bl);
    ::decode(tag_timeout, bl);
    DECODE_FINISH(bl);
  }
  void dump(Formatter *f) const;
  static void generate_test_instances(list<rgw_cls_tag_timeout_op*>& ls);
};
WRITE_CLASS_ENCODER(rgw_cls_tag_timeout_op)

struct rgw_cls_obj_prepare_op
{
  RGWModifyOp op;
  cls_rgw_obj_key key;
  string tag;
  string locator;
  bool log_op;
  uint16_t bilog_flags;

  rgw_cls_obj_prepare_op() : op(CLS_RGW_OP_UNKNOWN), log_op(false), bilog_flags(0) {}

  void encode(bufferlist &bl) const {
    ENCODE_START(6, 5, bl);
    uint8_t c = (uint8_t)op;
    ::encode(c, bl);
    ::encode(tag, bl);
    ::encode(locator, bl);
    ::encode(log_op, bl);
    ::encode(key, bl);
    ::encode(bilog_flags, bl);
    ENCODE_FINISH(bl);
  }
  void decode(bufferlist::iterator &bl) {
    DECODE_START_LEGACY_COMPAT_LEN(6, 3, 3, bl);
    uint8_t c;
    ::decode(c, bl);
    op = (RGWModifyOp)c;
    if (struct_v < 5) {
      ::decode(key.name, bl);
    }
    ::decode(tag, bl);
    if (struct_v >= 2) {
      ::decode(locator, bl);
    }
    if (struct_v >= 4) {
      ::decode(log_op, bl);
    }
    if (struct_v >= 5) {
      ::decode(key, bl);
    }
    if (struct_v >= 6) {
      ::decode(bilog_flags, bl);
    }
    DECODE_FINISH(bl);
  }
  void dump(Formatter *f) const;
  static void generate_test_instances(list<rgw_cls_obj_prepare_op*>& o);
};
WRITE_CLASS_ENCODER(rgw_cls_obj_prepare_op)

struct rgw_cls_obj_complete_op
{
  RGWModifyOp op;
  cls_rgw_obj_key key;
  string locator;
  rgw_bucket_entry_ver ver;
  struct rgw_bucket_dir_entry_meta meta;
  string tag;
  bool log_op;
  uint16_t bilog_flags;

  list<cls_rgw_obj_key> remove_objs;

  rgw_cls_obj_complete_op() : op(CLS_RGW_OP_ADD), log_op(false), bilog_flags(0) {}

  void encode(bufferlist &bl) const {
    ENCODE_START(8, 7, bl);
    uint8_t c = (uint8_t)op;
    ::encode(c, bl);
    ::encode(ver.epoch, bl);
    ::encode(meta, bl);
    ::encode(tag, bl);
    ::encode(locator, bl);
    ::encode(remove_objs, bl);
    ::encode(ver, bl);
    ::encode(log_op, bl);
    ::encode(key, bl);
    ::encode(bilog_flags, bl);
    ENCODE_FINISH(bl);
 }
  void decode(bufferlist::iterator &bl) {
    DECODE_START_LEGACY_COMPAT_LEN(8, 3, 3, bl);
    uint8_t c;
    ::decode(c, bl);
    op = (RGWModifyOp)c;
    if (struct_v < 7) {
      ::decode(key.name, bl);
    }
    ::decode(ver.epoch, bl);
    ::decode(meta, bl);
    ::decode(tag, bl);
    if (struct_v >= 2) {
      ::decode(locator, bl);
    }
    if (struct_v >= 4 && struct_v < 7) {
      list<string> old_remove_objs;
      ::decode(old_remove_objs, bl);

      for (list<string>::iterator iter = old_remove_objs.begin();
           iter != old_remove_objs.end(); ++iter) {
        cls_rgw_obj_key k;
        k.name = *iter;
        remove_objs.push_back(k);
      }
    } else {
      ::decode(remove_objs, bl);
    }
    if (struct_v >= 5) {
      ::decode(ver, bl);
    } else {
      ver.pool = -1;
    }
    if (struct_v >= 6) {
      ::decode(log_op, bl);
    }
    if (struct_v >= 7) {
      ::decode(key, bl);
    }
    if (struct_v >= 8) {
      ::decode(bilog_flags, bl);
    }
    DECODE_FINISH(bl);
  }
  void dump(Formatter *f) const;
  static void generate_test_instances(list<rgw_cls_obj_complete_op*>& o);
};
WRITE_CLASS_ENCODER(rgw_cls_obj_complete_op)

struct rgw_cls_link_olh_op {
  cls_rgw_obj_key key;
  string olh_tag;
  bool delete_marker;
  string op_tag;
  struct rgw_bucket_dir_entry_meta meta;
  uint64_t olh_epoch;
  bool log_op;
  uint16_t bilog_flags;

  rgw_cls_link_olh_op() : delete_marker(false), olh_epoch(0), log_op(false), bilog_flags(0) {}

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    ::encode(key, bl);
    ::encode(olh_tag, bl);
    ::encode(delete_marker, bl);
    ::encode(op_tag, bl);
    ::encode(meta, bl);
    ::encode(olh_epoch, bl);
    ::encode(log_op, bl);
    ::encode(bilog_flags, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::iterator& bl) {
    DECODE_START(1, bl);
    ::decode(key, bl);
    ::decode(olh_tag, bl);
    ::decode(delete_marker, bl);
    ::decode(op_tag, bl);
    ::decode(meta, bl);
    ::decode(olh_epoch, bl);
    ::decode(log_op, bl);
    ::decode(bilog_flags, bl);
    DECODE_FINISH(bl);
  }

  static void generate_test_instances(list<rgw_cls_link_olh_op *>& o);
  void dump(Formatter *f) const;
};
WRITE_CLASS_ENCODER(rgw_cls_link_olh_op)

struct rgw_cls_unlink_instance_op {
  cls_rgw_obj_key key;
  string op_tag;
  uint64_t olh_epoch;
  bool log_op;
  uint16_t bilog_flags;
  string olh_tag;

  rgw_cls_unlink_instance_op() : olh_epoch(0), log_op(false), bilog_flags(0) {}

  void encode(bufferlist& bl) const {
    ENCODE_START(2, 1, bl);
    ::encode(key, bl);
    ::encode(op_tag, bl);
    ::encode(olh_epoch, bl);
    ::encode(log_op, bl);
    ::encode(bilog_flags, bl);
    ::encode(olh_tag, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::iterator& bl) {
    DECODE_START(2, bl);
    ::decode(key, bl);
    ::decode(op_tag, bl);
    ::decode(olh_epoch, bl);
    ::decode(log_op, bl);
    ::decode(bilog_flags, bl);
    if (struct_v >= 2) {
      ::decode(olh_tag, bl);
    }
    DECODE_FINISH(bl);
  }

  static void generate_test_instances(list<rgw_cls_unlink_instance_op *>& o);
  void dump(Formatter *f) const;
};
WRITE_CLASS_ENCODER(rgw_cls_unlink_instance_op)

struct rgw_cls_read_olh_log_op
{
  cls_rgw_obj_key olh;
  uint64_t ver_marker;
  string olh_tag;

  rgw_cls_read_olh_log_op() : ver_marker(0) {}

  void encode(bufferlist &bl) const {
    ENCODE_START(1, 1, bl);
    ::encode(olh, bl);
    ::encode(ver_marker, bl);
    ::encode(olh_tag, bl);
    ENCODE_FINISH(bl);
  }
  void decode(bufferlist::iterator &bl) {
    DECODE_START(1, bl);
    ::decode(olh, bl);
    ::decode(ver_marker, bl);
    ::decode(olh_tag, bl);
    DECODE_FINISH(bl);
  }
  static void generate_test_instances(list<rgw_cls_read_olh_log_op *>& o);
  void dump(Formatter *f) const;
};
WRITE_CLASS_ENCODER(rgw_cls_read_olh_log_op)


struct rgw_cls_read_olh_log_ret
{
  map<uint64_t, vector<struct rgw_bucket_olh_log_entry> > log;
  bool is_truncated;

  rgw_cls_read_olh_log_ret() : is_truncated(false) {}

  void encode(bufferlist &bl) const {
    ENCODE_START(1, 1, bl);
    ::encode(log, bl);
    ::encode(is_truncated, bl);
    ENCODE_FINISH(bl);
  }
  void decode(bufferlist::iterator &bl) {
    DECODE_START(1, bl);
    ::decode(log, bl);
    ::decode(is_truncated, bl);
    DECODE_FINISH(bl);
  }
  static void generate_test_instances(list<rgw_cls_read_olh_log_ret *>& o);
  void dump(Formatter *f) const;
};
WRITE_CLASS_ENCODER(rgw_cls_read_olh_log_ret)

struct rgw_cls_trim_olh_log_op
{
  cls_rgw_obj_key olh;
  uint64_t ver;
  string olh_tag;

  rgw_cls_trim_olh_log_op() : ver(0) {}

  void encode(bufferlist &bl) const {
    ENCODE_START(1, 1, bl);
    ::encode(olh, bl);
    ::encode(ver, bl);
    ::encode(olh_tag, bl);
    ENCODE_FINISH(bl);
  }
  void decode(bufferlist::iterator &bl) {
    DECODE_START(1, bl);
    ::decode(olh, bl);
    ::decode(ver, bl);
    ::decode(olh_tag, bl);
    DECODE_FINISH(bl);
  }
  static void generate_test_instances(list<rgw_cls_trim_olh_log_op *>& o);
  void dump(Formatter *f) const;
};
WRITE_CLASS_ENCODER(rgw_cls_trim_olh_log_op)

struct rgw_cls_bucket_clear_olh_op {
  cls_rgw_obj_key key;
  string olh_tag;

  rgw_cls_bucket_clear_olh_op() {}

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    ::encode(key, bl);
    ::encode(olh_tag, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::iterator& bl) {
    DECODE_START(1, bl);
    ::decode(key, bl);
    ::decode(olh_tag, bl);
    DECODE_FINISH(bl);
  }

  static void generate_test_instances(list<rgw_cls_bucket_clear_olh_op *>& o);
  void dump(Formatter *f) const;
};
WRITE_CLASS_ENCODER(rgw_cls_bucket_clear_olh_op)

struct rgw_cls_list_op
{
  cls_rgw_obj_key start_obj;
  uint32_t num_entries;
  string filter_prefix;
  bool list_versions;

  rgw_cls_list_op() : num_entries(0), list_versions(false) {}

  void encode(bufferlist &bl) const {
    ENCODE_START(5, 4, bl);
    ::encode(num_entries, bl);
    ::encode(filter_prefix, bl);
    ::encode(start_obj, bl);
    ::encode(list_versions, bl);
    ENCODE_FINISH(bl);
  }
  void decode(bufferlist::iterator &bl) {
    DECODE_START_LEGACY_COMPAT_LEN(5, 2, 2, bl);
    if (struct_v < 4) {
      ::decode(start_obj.name, bl);
    }
    ::decode(num_entries, bl);
    if (struct_v >= 3)
      ::decode(filter_prefix, bl);
    if (struct_v >= 4)
      ::decode(start_obj, bl);
    if (struct_v >= 5)
      ::decode(list_versions, bl);
    DECODE_FINISH(bl);
  }
  void dump(Formatter *f) const;
  static void generate_test_instances(list<rgw_cls_list_op*>& o);
};
WRITE_CLASS_ENCODER(rgw_cls_list_op)

struct rgw_cls_list_ret
{
  rgw_bucket_dir dir;
  bool is_truncated;

  rgw_cls_list_ret() : is_truncated(false) {}

  void encode(bufferlist &bl) const {
    ENCODE_START(2, 2, bl);
    ::encode(dir, bl);
    ::encode(is_truncated, bl);
    ENCODE_FINISH(bl);
  }
  void decode(bufferlist::iterator &bl) {
    DECODE_START_LEGACY_COMPAT_LEN(2, 2, 2, bl);
    ::decode(dir, bl);
    ::decode(is_truncated, bl);
    DECODE_FINISH(bl);
  }
  void dump(Formatter *f) const;
  static void generate_test_instances(list<rgw_cls_list_ret*>& o);
};
WRITE_CLASS_ENCODER(rgw_cls_list_ret)

struct rgw_cls_check_index_ret
{
  rgw_bucket_dir_header existing_header;
  rgw_bucket_dir_header calculated_header;

  rgw_cls_check_index_ret() {}

  void encode(bufferlist &bl) const {
    ENCODE_START(1, 1, bl);
    ::encode(existing_header, bl);
    ::encode(calculated_header, bl);
    ENCODE_FINISH(bl);
  }
  void decode(bufferlist::iterator &bl) {
    DECODE_START(1, bl);
    ::decode(existing_header, bl);
    ::decode(calculated_header, bl);
    DECODE_FINISH(bl);
  }
  void dump(Formatter *f) const;
  static void generate_test_instances(list<rgw_cls_check_index_ret *>& o);
};
WRITE_CLASS_ENCODER(rgw_cls_check_index_ret)

struct rgw_cls_obj_remove_op {
  list<string> keep_attr_prefixes;

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    ::encode(keep_attr_prefixes, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::iterator& bl) {
    DECODE_START(1, bl);
    ::decode(keep_attr_prefixes, bl);
    DECODE_FINISH(bl);
  }
};
WRITE_CLASS_ENCODER(rgw_cls_obj_remove_op)

struct rgw_cls_obj_check_attrs_prefix {
  string check_prefix;
  bool fail_if_exist;

  rgw_cls_obj_check_attrs_prefix() : fail_if_exist(false) {}

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    ::encode(check_prefix, bl);
    ::encode(fail_if_exist, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::iterator& bl) {
    DECODE_START(1, bl);
    ::decode(check_prefix, bl);
    ::decode(fail_if_exist, bl);
    DECODE_FINISH(bl);
  }
};
WRITE_CLASS_ENCODER(rgw_cls_obj_check_attrs_prefix)

struct rgw_cls_usage_log_add_op {
  rgw_usage_log_info info;

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    ::encode(info, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::iterator& bl) {
    DECODE_START(1, bl);
    ::decode(info, bl);
    DECODE_FINISH(bl);
  }
};
WRITE_CLASS_ENCODER(rgw_cls_usage_log_add_op)

struct rgw_cls_bi_get_op {
  cls_rgw_obj_key key;
  BIIndexType type; /* namespace: plain, instance, olh */

  rgw_cls_bi_get_op() : type(PlainIdx) {}

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    ::encode(key, bl);
    ::encode((uint8_t)type, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::iterator& bl) {
    DECODE_START(1, bl);
    ::decode(key, bl);
    uint8_t c;
    ::decode(c, bl);
    type = (BIIndexType)c;
    DECODE_FINISH(bl);
  }
};
WRITE_CLASS_ENCODER(rgw_cls_bi_get_op)

struct rgw_cls_bi_get_ret {
  rgw_cls_bi_entry entry;

  rgw_cls_bi_get_ret() {}

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    ::encode(entry, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::iterator& bl) {
    DECODE_START(1, bl);
    ::decode(entry, bl);
    DECODE_FINISH(bl);
  }
};
WRITE_CLASS_ENCODER(rgw_cls_bi_get_ret)

struct rgw_cls_bi_put_op {
  rgw_cls_bi_entry entry;

  rgw_cls_bi_put_op() {}

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    ::encode(entry, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::iterator& bl) {
    DECODE_START(1, bl);
    ::decode(entry, bl);
    DECODE_FINISH(bl);
  }
};
WRITE_CLASS_ENCODER(rgw_cls_bi_put_op)

struct rgw_cls_bi_list_op {
  uint32_t max;
  string name;
  string marker;

  rgw_cls_bi_list_op() : max(0) {}

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    ::encode(max, bl);
    ::encode(name, bl);
    ::encode(marker, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::iterator& bl) {
    DECODE_START(1, bl);
    ::decode(max, bl);
    ::decode(name, bl);
    ::decode(marker, bl);
    DECODE_FINISH(bl);
  }
};
WRITE_CLASS_ENCODER(rgw_cls_bi_list_op)

struct rgw_cls_bi_list_ret {
  list<rgw_cls_bi_entry> entries;
  bool is_truncated;

  rgw_cls_bi_list_ret() : is_truncated(false) {}

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    ::encode(entries, bl);
    ::encode(is_truncated, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::iterator& bl) {
    DECODE_START(1, bl);
    ::decode(entries, bl);
    ::decode(is_truncated, bl);
    DECODE_FINISH(bl);
  }
};
WRITE_CLASS_ENCODER(rgw_cls_bi_list_ret)

struct rgw_cls_usage_log_read_op {
  uint64_t start_epoch;
  uint64_t end_epoch;
  string owner;

  string iter;  // should be empty for the first call, non empty for subsequent calls
  uint32_t max_entries;

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    ::encode(start_epoch, bl);
    ::encode(end_epoch, bl);
    ::encode(owner, bl);
    ::encode(iter, bl);
    ::encode(max_entries, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::iterator& bl) {
    DECODE_START(1, bl);
    ::decode(start_epoch, bl);
    ::decode(end_epoch, bl);
    ::decode(owner, bl);
    ::decode(iter, bl);
    ::decode(max_entries, bl);
    DECODE_FINISH(bl);
  }
};
WRITE_CLASS_ENCODER(rgw_cls_usage_log_read_op)

struct rgw_cls_usage_log_read_ret {
  map<rgw_user_bucket, rgw_usage_log_entry> usage;
  bool truncated;
  string next_iter;

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    ::encode(usage, bl);
    ::encode(truncated, bl);
    ::encode(next_iter, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::iterator& bl) {
    DECODE_START(1, bl);
    ::decode(usage, bl);
    ::decode(truncated, bl);
    ::decode(next_iter, bl);
    DECODE_FINISH(bl);
  }
};
WRITE_CLASS_ENCODER(rgw_cls_usage_log_read_ret)

struct rgw_cls_usage_log_trim_op {
  uint64_t start_epoch;
  uint64_t end_epoch;
  string user;

  void encode(bufferlist& bl) const {
    ENCODE_START(2, 2, bl);
    ::encode(start_epoch, bl);
    ::encode(end_epoch, bl);
    ::encode(user, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::iterator& bl) {
    DECODE_START(2, bl);
    ::decode(start_epoch, bl);
    ::decode(end_epoch, bl);
    ::decode(user, bl);
    DECODE_FINISH(bl);
  }
};
WRITE_CLASS_ENCODER(rgw_cls_usage_log_trim_op)

struct cls_rgw_gc_set_entry_op {
  uint32_t expiration_secs;
  cls_rgw_gc_obj_info info;
  cls_rgw_gc_set_entry_op() : expiration_secs(0) {}

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    ::encode(expiration_secs, bl);
    ::encode(info, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::iterator& bl) {
    DECODE_START(1, bl);
    ::decode(expiration_secs, bl);
    ::decode(info, bl);
    DECODE_FINISH(bl);
  }

  void dump(Formatter *f) const;
  static void generate_test_instances(list<cls_rgw_gc_set_entry_op*>& ls);
};
WRITE_CLASS_ENCODER(cls_rgw_gc_set_entry_op)

struct cls_rgw_gc_defer_entry_op {
  uint32_t expiration_secs;
  string tag;
  cls_rgw_gc_defer_entry_op() : expiration_secs(0) {}

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    ::encode(expiration_secs, bl);
    ::encode(tag, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::iterator& bl) {
    DECODE_START(1, bl);
    ::decode(expiration_secs, bl);
    ::decode(tag, bl);
    DECODE_FINISH(bl);
  }

  void dump(Formatter *f) const;
  static void generate_test_instances(list<cls_rgw_gc_defer_entry_op*>& ls);
};
WRITE_CLASS_ENCODER(cls_rgw_gc_defer_entry_op)

struct cls_rgw_gc_list_op {
  string marker;
  uint32_t max;
  bool expired_only;

  cls_rgw_gc_list_op() : max(0), expired_only(true) {}

  void encode(bufferlist& bl) const {
    ENCODE_START(2, 1, bl);
    ::encode(marker, bl);
    ::encode(max, bl);
    ::encode(expired_only, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::iterator& bl) {
    DECODE_START(2, bl);
    ::decode(marker, bl);
    ::decode(max, bl);
    if (struct_v >= 2) {
      ::decode(expired_only, bl);
    }
    DECODE_FINISH(bl);
  }

  void dump(Formatter *f) const;
  static void generate_test_instances(list<cls_rgw_gc_list_op*>& ls);
};
WRITE_CLASS_ENCODER(cls_rgw_gc_list_op)

struct cls_rgw_gc_list_ret {
  list<cls_rgw_gc_obj_info> entries;
  bool truncated;

  cls_rgw_gc_list_ret() : truncated(false) {}

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    ::encode(entries, bl);
    ::encode(truncated, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::iterator& bl) {
    DECODE_START(1, bl);
    ::decode(entries, bl);
    ::decode(truncated, bl);
    DECODE_FINISH(bl);
  }

  void dump(Formatter *f) const;
  static void generate_test_instances(list<cls_rgw_gc_list_ret*>& ls);
};
WRITE_CLASS_ENCODER(cls_rgw_gc_list_ret)

struct cls_rgw_gc_remove_op {
  list<string> tags;

  cls_rgw_gc_remove_op() {}

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    ::encode(tags, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::iterator& bl) {
    DECODE_START(1, bl);
    ::decode(tags, bl);
    DECODE_FINISH(bl);
  }

  void dump(Formatter *f) const;
  static void generate_test_instances(list<cls_rgw_gc_remove_op*>& ls);
};
WRITE_CLASS_ENCODER(cls_rgw_gc_remove_op)

struct cls_rgw_bi_log_list_op {
  string marker;
  uint32_t max;

  cls_rgw_bi_log_list_op() : max(0) {}

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    ::encode(marker, bl);
    ::encode(max, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::iterator& bl) {
    DECODE_START(1, bl);
    ::decode(marker, bl);
    ::decode(max, bl);
    DECODE_FINISH(bl);
  }

  void dump(Formatter *f) const;
  static void generate_test_instances(list<cls_rgw_bi_log_list_op*>& ls);
};
WRITE_CLASS_ENCODER(cls_rgw_bi_log_list_op)

struct cls_rgw_bi_log_trim_op {
  string start_marker;
  string end_marker;

  cls_rgw_bi_log_trim_op() {}

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    ::encode(start_marker, bl);
    ::encode(end_marker, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::iterator& bl) {
    DECODE_START(1, bl);
    ::decode(start_marker, bl);
    ::decode(end_marker, bl);
    DECODE_FINISH(bl);
  }

  void dump(Formatter *f) const;
  static void generate_test_instances(list<cls_rgw_bi_log_trim_op*>& ls);
};
WRITE_CLASS_ENCODER(cls_rgw_bi_log_trim_op)

struct cls_rgw_bi_log_list_ret {
  list<rgw_bi_log_entry> entries;
  bool truncated;

  cls_rgw_bi_log_list_ret() : truncated(false) {}

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    ::encode(entries, bl);
    ::encode(truncated, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::iterator& bl) {
    DECODE_START(1, bl);
    ::decode(entries, bl);
    ::decode(truncated, bl);
    DECODE_FINISH(bl);
  }

  void dump(Formatter *f) const;
  static void generate_test_instances(list<cls_rgw_bi_log_list_ret*>& ls);
};
WRITE_CLASS_ENCODER(cls_rgw_bi_log_list_ret)


#endif
