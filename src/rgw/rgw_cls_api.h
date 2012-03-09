#ifndef CEPH_RGW_CLS_API_H
#define CEPH_RGW_CLS_API_H

#include <map>

#include "include/types.h"
#include "include/utime.h"


#define CEPH_RGW_REMOVE 'r'
#define CEPH_RGW_UPDATE 'u'
#define CEPH_RGW_TAG_TIMEOUT 60*60*24

enum RGWPendingState {
  CLS_RGW_STATE_PENDING_MODIFY,
  CLS_RGW_STATE_COMPLETE,
};

enum RGWModifyOp {
  CLS_RGW_OP_ADD    = 0,
  CLS_RGW_OP_DEL    = 1,
  CLS_RGW_OP_CANCEL = 2,
};

struct rgw_bucket_pending_info {
  RGWPendingState state;
  utime_t timestamp;
  uint8_t op;

  void encode(bufferlist &bl) const {
    __u8 struct_v = 1;
    ::encode(struct_v, bl);
    uint8_t s = (uint8_t)state;
    ::encode(s, bl);
    ::encode(timestamp, bl);
    ::encode(op, bl);
  }
  void decode(bufferlist::iterator &bl) {
    __u8 struct_v;
    ::decode(struct_v, bl);
    uint8_t s;
    ::decode(s, bl);
    state = (RGWPendingState)s;
    ::decode(timestamp, bl);
    ::decode(op, bl);
  }
};
WRITE_CLASS_ENCODER(rgw_bucket_pending_info)

struct rgw_bucket_dir_entry_meta {
  uint8_t category;
  uint64_t size;
  utime_t mtime;
  string etag;
  string owner;
  string owner_display_name;
  string tag;
  string content_type;

  rgw_bucket_dir_entry_meta() :
  category(0), size(0) { mtime.set_from_double(0); }

  void encode(bufferlist &bl) const {
    __u8 struct_v = 2;
    ::encode(struct_v, bl);
    ::encode(category, bl);
    ::encode(size, bl);
    ::encode(mtime, bl);
    ::encode(etag, bl);
    ::encode(owner, bl);
    ::encode(owner_display_name, bl);
    ::encode(content_type, bl);
  }
  void decode(bufferlist::iterator &bl) {
    __u8 struct_v;
    ::decode(struct_v, bl);
    ::decode(category, bl);
    ::decode(size, bl);
    ::decode(mtime, bl);
    ::decode(etag, bl);
    ::decode(owner, bl);
    ::decode(owner_display_name, bl);
    if (struct_v >= 2)
      ::decode(content_type, bl);
  }
};
WRITE_CLASS_ENCODER(rgw_bucket_dir_entry_meta)

struct rgw_bucket_dir_entry {
  std::string name;
  uint64_t epoch;
  std::string locator;
  bool exists;
  struct rgw_bucket_dir_entry_meta meta;
  map<string, struct rgw_bucket_pending_info> pending_map;

  rgw_bucket_dir_entry() :
    epoch(0), exists(false) {}

  void encode(bufferlist &bl) const {
    __u8 struct_v = 2;
    if (!locator.size()) {
      struct_v = 1; // don't waste space encoding it
    }
    ::encode(struct_v, bl);
    ::encode(name, bl);
    ::encode(epoch, bl);
    ::encode(exists, bl);
    ::encode(meta, bl);
    ::encode(pending_map, bl);
    if (locator.size()) {
      ::encode(locator, bl);
    }
  }
  void decode(bufferlist::iterator &bl) {
    __u8 struct_v;
    ::decode(struct_v, bl);
    ::decode(name, bl);
    ::decode(epoch, bl);
    ::decode(exists, bl);
    ::decode(meta, bl);
    ::decode(pending_map, bl);
    if (struct_v >= 2) {
      ::decode(locator, bl);
    }
  }
};
WRITE_CLASS_ENCODER(rgw_bucket_dir_entry)

struct rgw_bucket_category_stats {
  uint64_t total_size;
  uint64_t total_size_rounded;
  uint64_t num_entries;

  void encode(bufferlist &bl) const {
    __u8 struct_v = 1;
    ::encode(struct_v, bl);
    ::encode(total_size, bl);
    ::encode(total_size_rounded, bl);
    ::encode(num_entries, bl);
  }
  void decode(bufferlist::iterator &bl) {
    __u8 struct_v;
    ::decode(struct_v, bl);
    ::decode(total_size, bl);
    ::decode(total_size_rounded, bl);
    ::decode(num_entries, bl);
  }
};
WRITE_CLASS_ENCODER(rgw_bucket_category_stats)

struct rgw_bucket_dir_header {
  map<uint8_t, rgw_bucket_category_stats> stats;

  void encode(bufferlist &bl) const {
    __u8 struct_v = 1;
    ::encode(struct_v, bl);
    ::encode(stats, bl);
  }
  void decode(bufferlist::iterator &bl) {
    __u8 struct_v;
    ::decode(struct_v, bl);
    ::decode(stats, bl);
  }
};
WRITE_CLASS_ENCODER(rgw_bucket_dir_header)

struct rgw_bucket_dir {
  struct rgw_bucket_dir_header header;
  std::map<string, struct rgw_bucket_dir_entry> m;

  void encode(bufferlist &bl) const {
    __u8 struct_v = 1;
    ::encode(struct_v, bl);
    ::encode(header, bl);
    ::encode(m, bl);
  }
  void decode(bufferlist::iterator &bl) {
    __u8 struct_v;
    ::decode(struct_v, bl);
    ::decode(header, bl);
    ::decode(m, bl);
  }
};
WRITE_CLASS_ENCODER(rgw_bucket_dir)

struct rgw_cls_obj_prepare_op
{
  uint8_t op;
  string name;
  string tag;
  string locator;

  void encode(bufferlist &bl) const {
    __u8 struct_v = 2;
    if (!locator.size()) {
      struct_v = 1; // don't waste the encoding space
    }
    ::encode(struct_v, bl);
    ::encode(op, bl);
    ::encode(name, bl);
    ::encode(tag, bl);
    if (locator.size()) {
      ::encode(locator, bl);
    }
  }
  void decode(bufferlist::iterator &bl) {
    __u8 struct_v;
    ::decode(struct_v, bl);
    ::decode(op, bl);
    ::decode(name, bl);
    ::decode(tag, bl);
    if (struct_v >= 2) {
      ::decode(locator, bl);
    }
  }
};
WRITE_CLASS_ENCODER(rgw_cls_obj_prepare_op)

struct rgw_cls_obj_complete_op
{
  uint8_t op;
  string name;
  string locator;
  uint64_t epoch;
  struct rgw_bucket_dir_entry_meta meta;
  string tag;

  void encode(bufferlist &bl) const {
    __u8 struct_v = 2;
    if (!locator.size()) {
      struct_v = 1; // don't waste the encoding space
    }
    ::encode(struct_v, bl);
    ::encode(op, bl);
    ::encode(name, bl);
    ::encode(epoch, bl);
    ::encode(meta, bl);
    ::encode(tag, bl);
    if (locator.size()) {
      ::encode(locator, bl);
    }
 }
  void decode(bufferlist::iterator &bl) {
    __u8 struct_v;
    ::decode(struct_v, bl);
    ::decode(op, bl);
    ::decode(name, bl);
    ::decode(epoch, bl);
    ::decode(meta, bl);
    ::decode(tag, bl);
    if (struct_v >= 2) {
      ::decode(locator, bl);
    }
  }
};
WRITE_CLASS_ENCODER(rgw_cls_obj_complete_op)

struct rgw_cls_list_op
{
  string start_obj;
  uint32_t num_entries;
  string filter_prefix;

  void encode(bufferlist &bl) const {
    __u8 struct_v = 2;
    ::encode(struct_v, bl);
    ::encode(start_obj, bl);
    ::encode(num_entries, bl);
    ::encode(filter_prefix, bl);
  }
  void decode(bufferlist::iterator &bl) {
    __u8 struct_v;
    ::decode(struct_v, bl);
    ::decode(start_obj, bl);
    ::decode(num_entries, bl);
    if (struct_v >= 2)
      ::decode(filter_prefix, bl);
  }
};
WRITE_CLASS_ENCODER(rgw_cls_list_op)

struct rgw_cls_list_ret
{
  rgw_bucket_dir dir;
  bool is_truncated;

  void encode(bufferlist &bl) const {
    __u8 struct_v = 1;
    ::encode(struct_v, bl);
    ::encode(dir, bl);
    ::encode(is_truncated, bl);
  }
  void decode(bufferlist::iterator &bl) {
    __u8 struct_v;
    ::decode(struct_v, bl);
    ::decode(dir, bl);
    ::decode(is_truncated, bl);
  }
};
WRITE_CLASS_ENCODER(rgw_cls_list_ret)

#endif
