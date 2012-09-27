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
  uint64_t tag_timeout;

  rgw_bucket_dir_header() : tag_timeout(0) {}

  void encode(bufferlist &bl) const {
    ENCODE_START(3, 2, bl);
    ::encode(stats, bl);
    ::encode(tag_timeout, bl);
    ENCODE_FINISH(bl);
  }
  void decode(bufferlist::iterator &bl) {
    DECODE_START_LEGACY_COMPAT_LEN(3, 2, 2, bl);
    ::decode(stats, bl);
    if (struct_v > 2) {
      ::decode(tag_timeout, bl);
    } else {
      tag_timeout = 0;
    }
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

struct rgw_usage_data {
  uint64_t bytes_sent;
  uint64_t bytes_received;
  uint64_t ops;
  uint64_t successful_ops;

  rgw_usage_data() : bytes_sent(0), bytes_received(0), ops(0), successful_ops(0) {}
  rgw_usage_data(uint64_t sent, uint64_t received) : bytes_sent(sent), bytes_received(received), ops(0), successful_ops(0) {}

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    ::encode(bytes_sent, bl);
    ::encode(bytes_received, bl);
    ::encode(ops, bl);
    ::encode(successful_ops, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::iterator& bl) {
    DECODE_START(1, bl);
    ::decode(bytes_sent, bl);
    ::decode(bytes_received, bl);
    ::decode(ops, bl);
    ::decode(successful_ops, bl);
    DECODE_FINISH(bl);
  }

  void aggregate(const rgw_usage_data& usage) {
    bytes_sent += usage.bytes_sent;
    bytes_received += usage.bytes_received;
    ops += usage.ops;
    successful_ops += usage.successful_ops;
  }
};
WRITE_CLASS_ENCODER(rgw_usage_data)


struct rgw_usage_log_entry {
  string owner;
  string bucket;
  uint64_t epoch;
  rgw_usage_data total_usage; /* this one is kept for backwards compatibility */
  map<string, rgw_usage_data> usage_map;

  rgw_usage_log_entry() : epoch(0) {}
  rgw_usage_log_entry(string& o, string& b) : owner(o), bucket(b), epoch(0) {}

  void encode(bufferlist& bl) const {
    ENCODE_START(2, 1, bl);
    ::encode(owner, bl);
    ::encode(bucket, bl);
    ::encode(epoch, bl);
    ::encode(total_usage.bytes_sent, bl);
    ::encode(total_usage.bytes_received, bl);
    ::encode(total_usage.ops, bl);
    ::encode(total_usage.successful_ops, bl);
    ::encode(usage_map, bl);
    ENCODE_FINISH(bl);
  }


   void decode(bufferlist::iterator& bl) {
    DECODE_START(2, bl);
    ::decode(owner, bl);
    ::decode(bucket, bl);
    ::decode(epoch, bl);
    ::decode(total_usage.bytes_sent, bl);
    ::decode(total_usage.bytes_received, bl);
    ::decode(total_usage.ops, bl);
    ::decode(total_usage.successful_ops, bl);
    if (struct_v < 2) {
      usage_map[""] = total_usage;
    } else {
      ::decode(usage_map, bl);
    }
    DECODE_FINISH(bl);
  }

  void aggregate(const rgw_usage_log_entry& e, map<string, bool> *categories = NULL) {
    if (owner.empty()) {
      owner = e.owner;
      bucket = e.bucket;
      epoch = e.epoch;
    }
    map<string, rgw_usage_data>::const_iterator iter;
    for (iter = e.usage_map.begin(); iter != e.usage_map.end(); ++iter) {
      if (!categories || !categories->size() || categories->count(iter->first)) {
        add(iter->first, iter->second);
      }
    }
  }

  void sum(rgw_usage_data& usage, map<string, bool>& categories) const {
    usage = rgw_usage_data();
    for (map<string, rgw_usage_data>::const_iterator iter = usage_map.begin(); iter != usage_map.end(); ++iter) {
      if (!categories.size() || categories.count(iter->first)) {
        usage.aggregate(iter->second);
      }
    }
  }

  void add(const string& category, const rgw_usage_data& data) {
    usage_map[category].aggregate(data);
    total_usage.aggregate(data);
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

enum cls_rgw_gc_op {
  CLS_RGW_GC_DEL_OBJ,
  CLS_RGW_GC_DEL_BUCKET,
};

struct cls_rgw_obj {
  string pool;
  string oid;
  string key;

  cls_rgw_obj() {}
  cls_rgw_obj(string& _p, string& _o) : pool(_p), oid(_o) {}

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    ::encode(pool, bl);
    ::encode(oid, bl);
    ::encode(key, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::iterator& bl) {
    DECODE_START(1, bl);
    ::decode(pool, bl);
    ::decode(oid, bl);
    ::decode(key, bl);
    DECODE_FINISH(bl);
  }
};
WRITE_CLASS_ENCODER(cls_rgw_obj)

struct cls_rgw_obj_chain {
  list<cls_rgw_obj> objs;

  cls_rgw_obj_chain() {}

  void push_obj(string& pool, string& oid, string& key) {
    cls_rgw_obj obj;
    obj.pool = pool;
    obj.oid = oid;
    obj.key = key;
    objs.push_back(obj);
  }

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    ::encode(objs, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::iterator& bl) {
    DECODE_START(1, bl);
    ::decode(objs, bl);
    DECODE_FINISH(bl);
  }
};
WRITE_CLASS_ENCODER(cls_rgw_obj_chain)

struct cls_rgw_gc_obj_info
{
  string tag;
  cls_rgw_obj_chain chain;
  utime_t time;

  cls_rgw_gc_obj_info() {}


  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    ::encode(tag, bl);
    ::encode(chain, bl);
    ::encode(time, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::iterator& bl) {
    DECODE_START(1, bl);
    ::decode(tag, bl);
    ::decode(chain, bl);
    ::decode(time, bl);
    DECODE_FINISH(bl);
  }
};
WRITE_CLASS_ENCODER(cls_rgw_gc_obj_info)

#endif
