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
  string name;
  string tag;
  string locator;
  bool log_op;

  rgw_cls_obj_prepare_op() : op(CLS_RGW_OP_UNKNOWN), log_op(false) {}

  void encode(bufferlist &bl) const {
    ENCODE_START(4, 3, bl);
    uint8_t c = (uint8_t)op;
    ::encode(c, bl);
    ::encode(name, bl);
    ::encode(tag, bl);
    ::encode(locator, bl);
    ::encode(log_op, bl);
    ENCODE_FINISH(bl);
  }
  void decode(bufferlist::iterator &bl) {
    DECODE_START_LEGACY_COMPAT_LEN(4, 3, 3, bl);
    uint8_t c;
    ::decode(c, bl);
    op = (RGWModifyOp)c;
    ::decode(name, bl);
    ::decode(tag, bl);
    if (struct_v >= 2) {
      ::decode(locator, bl);
    }
    if (struct_v >= 4) {
      ::decode(log_op, bl);
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
  string name;
  string locator;
  rgw_bucket_entry_ver ver;
  struct rgw_bucket_dir_entry_meta meta;
  string tag;
  bool log_op;

  list<string> remove_objs;

  rgw_cls_obj_complete_op() : op(CLS_RGW_OP_ADD), log_op(false) {}

  void encode(bufferlist &bl) const {
    ENCODE_START(6, 3, bl);
    uint8_t c = (uint8_t)op;
    ::encode(c, bl);
    ::encode(name, bl);
    ::encode(ver.epoch, bl);
    ::encode(meta, bl);
    ::encode(tag, bl);
    ::encode(locator, bl);
    ::encode(remove_objs, bl);
    ::encode(ver, bl);
    ::encode(log_op, bl);
    ENCODE_FINISH(bl);
 }
  void decode(bufferlist::iterator &bl) {
    DECODE_START_LEGACY_COMPAT_LEN(6, 3, 3, bl);
    uint8_t c;
    ::decode(c, bl);
    op = (RGWModifyOp)c;
    ::decode(name, bl);
    ::decode(ver.epoch, bl);
    ::decode(meta, bl);
    ::decode(tag, bl);
    if (struct_v >= 2) {
      ::decode(locator, bl);
    }
    if (struct_v >= 4) {
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
    DECODE_FINISH(bl);
  }
  void dump(Formatter *f) const;
  static void generate_test_instances(list<rgw_cls_obj_complete_op*>& o);
};
WRITE_CLASS_ENCODER(rgw_cls_obj_complete_op)

struct rgw_cls_list_op
{
  string start_obj;
  uint32_t num_entries;
  string filter_prefix;

  rgw_cls_list_op() : num_entries(0) {}

  void encode(bufferlist &bl) const {
    ENCODE_START(3, 2, bl);
    ::encode(start_obj, bl);
    ::encode(num_entries, bl);
    ::encode(filter_prefix, bl);
    ENCODE_FINISH(bl);
  }
  void decode(bufferlist::iterator &bl) {
    DECODE_START_LEGACY_COMPAT_LEN(3, 2, 2, bl);
    ::decode(start_obj, bl);
    ::decode(num_entries, bl);
    if (struct_v >= 3)
      ::decode(filter_prefix, bl);
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
  static void generate_test_instances(list<rgw_cls_list_ret*>& o);
};
WRITE_CLASS_ENCODER(rgw_cls_check_index_ret)

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
