#ifndef CEPH_CLS_RGW_TYPES_H
#define CEPH_CLS_RGW_TYPES_H

#include <map>

#include "include/types.h"
#include "include/utime.h"

#define CEPH_RGW_REMOVE 'r'
#define CEPH_RGW_UPDATE 'u'
#define CEPH_RGW_TAG_TIMEOUT 60*60*24

namespace ceph {
  class Formatter;
}

enum RGWPendingState {
  CLS_RGW_STATE_PENDING_MODIFY = 0,
  CLS_RGW_STATE_COMPLETE       = 1,
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

  rgw_bucket_pending_info() : state(CLS_RGW_STATE_PENDING_MODIFY), op(0) {}

  void encode(bufferlist &bl) const {
    ENCODE_START(2, 2, bl);
    uint8_t s = (uint8_t)state;
    ::encode(s, bl);
    ::encode(timestamp, bl);
    ::encode(op, bl);
    ENCODE_FINISH(bl);
  }
  void decode(bufferlist::iterator &bl) {
    DECODE_START_LEGACY_COMPAT_LEN(2, 2, 2, bl);
    uint8_t s;
    ::decode(s, bl);
    state = (RGWPendingState)s;
    ::decode(timestamp, bl);
    ::decode(op, bl);
    DECODE_FINISH(bl);
  }
  void dump(Formatter *f) const;
  static void generate_test_instances(list<rgw_bucket_pending_info*>& o);
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
    ENCODE_START(3, 3, bl);
    ::encode(category, bl);
    ::encode(size, bl);
    ::encode(mtime, bl);
    ::encode(etag, bl);
    ::encode(owner, bl);
    ::encode(owner_display_name, bl);
    ::encode(content_type, bl);
    ENCODE_FINISH(bl);
  }
  void decode(bufferlist::iterator &bl) {
    DECODE_START_LEGACY_COMPAT_LEN(3, 3, 3, bl);
    ::decode(category, bl);
    ::decode(size, bl);
    ::decode(mtime, bl);
    ::decode(etag, bl);
    ::decode(owner, bl);
    ::decode(owner_display_name, bl);
    if (struct_v >= 2)
      ::decode(content_type, bl);
    DECODE_FINISH(bl);
  }
  void dump(Formatter *f) const;
  static void generate_test_instances(list<rgw_bucket_dir_entry_meta*>& o);
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
    ENCODE_START(3, 3, bl);
    ::encode(name, bl);
    ::encode(epoch, bl);
    ::encode(exists, bl);
    ::encode(meta, bl);
    ::encode(pending_map, bl);
    ::encode(locator, bl);
    ENCODE_FINISH(bl);
  }
  void decode(bufferlist::iterator &bl) {
    DECODE_START_LEGACY_COMPAT_LEN(3, 3, 3, bl);
    ::decode(name, bl);
    ::decode(epoch, bl);
    ::decode(exists, bl);
    ::decode(meta, bl);
    ::decode(pending_map, bl);
    if (struct_v >= 2) {
      ::decode(locator, bl);
    }
    DECODE_FINISH(bl);
  }
  void dump(Formatter *f) const;
  static void generate_test_instances(list<rgw_bucket_dir_entry*>& o);
};
WRITE_CLASS_ENCODER(rgw_bucket_dir_entry)

struct rgw_bucket_category_stats {
  uint64_t total_size;
  uint64_t total_size_rounded;
  uint64_t num_entries;

  rgw_bucket_category_stats() : total_size(0), total_size_rounded(0), num_entries(0) {}

  void encode(bufferlist &bl) const {
    ENCODE_START(2, 2, bl);
    ::encode(total_size, bl);
    ::encode(total_size_rounded, bl);
    ::encode(num_entries, bl);
    ENCODE_FINISH(bl);
  }
  void decode(bufferlist::iterator &bl) {
    DECODE_START_LEGACY_COMPAT_LEN(2, 2, 2, bl);
    ::decode(total_size, bl);
    ::decode(total_size_rounded, bl);
    ::decode(num_entries, bl);
    DECODE_FINISH(bl);
  }
  void dump(Formatter *f) const;
  static void generate_test_instances(list<rgw_bucket_category_stats*>& o);
};
WRITE_CLASS_ENCODER(rgw_bucket_category_stats)

struct rgw_bucket_dir_header {
  map<uint8_t, rgw_bucket_category_stats> stats;

  void encode(bufferlist &bl) const {
    ENCODE_START(2, 2, bl);
    ::encode(stats, bl);
    ENCODE_FINISH(bl);
  }
  void decode(bufferlist::iterator &bl) {
    DECODE_START_LEGACY_COMPAT_LEN(2, 2, 2, bl);
    ::decode(stats, bl);
    DECODE_FINISH(bl);
  }
  void dump(Formatter *f) const;
  static void generate_test_instances(list<rgw_bucket_dir_header*>& o);
};
WRITE_CLASS_ENCODER(rgw_bucket_dir_header)

struct rgw_bucket_dir {
  struct rgw_bucket_dir_header header;
  std::map<string, struct rgw_bucket_dir_entry> m;

  void encode(bufferlist &bl) const {
    ENCODE_START(2, 2, bl);
    ::encode(header, bl);
    ::encode(m, bl);
    ENCODE_FINISH(bl);
  }
  void decode(bufferlist::iterator &bl) {
    DECODE_START_LEGACY_COMPAT_LEN(2, 2, 2, bl);
    ::decode(header, bl);
    ::decode(m, bl);
    DECODE_FINISH(bl);
  }
  void dump(Formatter *f) const;
  static void generate_test_instances(list<rgw_bucket_dir*>& o);
};
WRITE_CLASS_ENCODER(rgw_bucket_dir)


struct rgw_usage_log_entry {
  string owner;
  string bucket;
  uint64_t epoch;
  uint64_t bytes_sent;
  uint64_t bytes_received;
  uint64_t ops;
  uint64_t successful_ops;

  rgw_usage_log_entry() : bytes_sent(0), bytes_received(0), ops(0), successful_ops(0) {}
  rgw_usage_log_entry(string& o, string& b, uint64_t s, uint64_t r) : owner(o), bucket(b), bytes_sent(s), bytes_received(r), ops(0), successful_ops(0) {}

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    ::encode(owner, bl);
    ::encode(bucket, bl);
    ::encode(epoch, bl);
    ::encode(bytes_sent, bl);
    ::encode(bytes_received, bl);
    ::encode(ops, bl);
    ::encode(successful_ops, bl);
    ENCODE_FINISH(bl);
  }


   void decode(bufferlist::iterator& bl) {
    DECODE_START(1, bl);
    ::decode(owner, bl);
    ::decode(bucket, bl);
    ::decode(epoch, bl);
    ::decode(bytes_sent, bl);
    ::decode(bytes_received, bl);
    ::decode(ops, bl);
    ::decode(successful_ops, bl);
    DECODE_FINISH(bl);
  }

  void aggregate(const rgw_usage_log_entry& e) {
    if (owner.empty()) {
      owner = e.owner;
      bucket = e.bucket;
      epoch = e.epoch;
    }
    bytes_sent += e.bytes_sent;
    bytes_received += e.bytes_received;
    ops += e.ops;
    successful_ops += e.successful_ops;
  }
};
WRITE_CLASS_ENCODER(rgw_usage_log_entry)

struct rgw_usage_log_info {
  vector<rgw_usage_log_entry> entries;

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    ::encode(entries, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::iterator& bl) {
    DECODE_START(1, bl);
    ::decode(entries, bl);
    DECODE_FINISH(bl);
  }

  rgw_usage_log_info() {}
};
WRITE_CLASS_ENCODER(rgw_usage_log_info)

struct rgw_user_bucket {
  string user;
  string bucket;

  rgw_user_bucket() {}
  rgw_user_bucket(string &u, string& b) : user(u), bucket(b) {}

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    ::encode(user, bl);
    ::encode(bucket, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::iterator& bl) {
    DECODE_START(1, bl);
    ::decode(user, bl);
    ::decode(bucket, bl);
    DECODE_FINISH(bl);
  }

  bool operator<(const rgw_user_bucket& ub2) const {
    int comp = user.compare(ub2.user);
    if (comp < 0)
      return true;
    else if (!comp)
      return bucket.compare(ub2.bucket) < 0;
  
    return false;
  }
};
WRITE_CLASS_ENCODER(rgw_user_bucket)

#endif
