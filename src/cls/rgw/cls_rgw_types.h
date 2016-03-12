#ifndef CEPH_CLS_RGW_TYPES_H
#define CEPH_CLS_RGW_TYPES_H

#include <map>

#include "include/types.h"
#include "include/utime.h"
#include "common/Formatter.h"

#include "rgw/rgw_basic_types.h"

#define CEPH_RGW_REMOVE 'r'
#define CEPH_RGW_UPDATE 'u'
#define CEPH_RGW_TAG_TIMEOUT 60*60*24

class JSONObj;

namespace ceph {
  class Formatter;
}

enum RGWPendingState {
  CLS_RGW_STATE_PENDING_MODIFY = 0,
  CLS_RGW_STATE_COMPLETE       = 1,
  CLS_RGW_STATE_UNKNOWN        = 2,
};

enum RGWModifyOp {
  CLS_RGW_OP_ADD     = 0,
  CLS_RGW_OP_DEL     = 1,
  CLS_RGW_OP_CANCEL  = 2,
  CLS_RGW_OP_UNKNOWN = 3,
  CLS_RGW_OP_LINK_OLH        = 4,
  CLS_RGW_OP_LINK_OLH_DM     = 5, /* creation of delete marker */
  CLS_RGW_OP_UNLINK_INSTANCE = 6,
};

enum RGWBILogFlags {
  RGW_BILOG_FLAG_VERSIONED_OP = 0x1,
};

enum RGWCheckMTimeType {
  CLS_RGW_CHECK_TIME_MTIME_EQ = 0,
  CLS_RGW_CHECK_TIME_MTIME_LT = 1,
  CLS_RGW_CHECK_TIME_MTIME_LE = 2,
  CLS_RGW_CHECK_TIME_MTIME_GT = 3,
  CLS_RGW_CHECK_TIME_MTIME_GE = 4,
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
  void decode_json(JSONObj *obj);
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
  string content_type;
  uint64_t accounted_size;

  rgw_bucket_dir_entry_meta() :
  category(0), size(0), accounted_size(0) { mtime.set_from_double(0); }

  void encode(bufferlist &bl) const {
    ENCODE_START(4, 3, bl);
    ::encode(category, bl);
    ::encode(size, bl);
    ::encode(mtime, bl);
    ::encode(etag, bl);
    ::encode(owner, bl);
    ::encode(owner_display_name, bl);
    ::encode(content_type, bl);
    ::encode(accounted_size, bl);
    ENCODE_FINISH(bl);
  }
  void decode(bufferlist::iterator &bl) {
    DECODE_START_LEGACY_COMPAT_LEN(4, 3, 3, bl);
    ::decode(category, bl);
    ::decode(size, bl);
    ::decode(mtime, bl);
    ::decode(etag, bl);
    ::decode(owner, bl);
    ::decode(owner_display_name, bl);
    if (struct_v >= 2)
      ::decode(content_type, bl);
    if (struct_v >= 4)
      ::decode(accounted_size, bl);
    else
      accounted_size = size;
    DECODE_FINISH(bl);
  }
  void dump(Formatter *f) const;
  void decode_json(JSONObj *obj);
  static void generate_test_instances(list<rgw_bucket_dir_entry_meta*>& o);
};
WRITE_CLASS_ENCODER(rgw_bucket_dir_entry_meta)

template<class T>
void encode_packed_val(T val, bufferlist& bl)
{
  if ((uint64_t)val < 0x80) {
    ::encode((uint8_t)val, bl);
  } else {
    unsigned char c = 0x80;

    if ((uint64_t)val < 0x100) {
      c |= 1;
      ::encode(c, bl);
      ::encode((uint8_t)val, bl);
    } else if ((uint64_t)val <= 0x10000) {
      c |= 2;
      ::encode(c, bl);
      ::encode((uint16_t)val, bl);
    } else if ((uint64_t)val <= 0x1000000) {
      c |= 4;
      ::encode(c, bl);
      ::encode((uint32_t)val, bl);
    } else {
      c |= 8;
      ::encode(c, bl);
      ::encode((uint64_t)val, bl);
    }
  }
}

template<class T>
void decode_packed_val(T& val, bufferlist::iterator& bl)
{
  unsigned char c;
  ::decode(c, bl);
  if (c < 0x80) {
    val = c;
    return;
  }

  c &= ~0x80;

  switch (c) {
    case 1:
      {
        uint8_t v;
        ::decode(v, bl);
        val = v;
      }
      break;
    case 2:
      {
        uint16_t v;
        ::decode(v, bl);
        val = v;
      }
      break;
    case 4:
      {
        uint32_t v;
        ::decode(v, bl);
        val = v;
      }
      break;
    case 8:
      {
        uint64_t v;
        ::decode(v, bl);
        val = v;
      }
      break;
    default:
      throw buffer::error();
  }
}

struct rgw_bucket_entry_ver {
  int64_t pool;
  uint64_t epoch;

  rgw_bucket_entry_ver() : pool(-1), epoch(0) {}

  void encode(bufferlist &bl) const {
    ENCODE_START(1, 1, bl);
    ::encode_packed_val(pool, bl);
    ::encode_packed_val(epoch, bl);
    ENCODE_FINISH(bl);
  }
  void decode(bufferlist::iterator &bl) {
    DECODE_START(1, bl);
    ::decode_packed_val(pool, bl);
    ::decode_packed_val(epoch, bl);
    DECODE_FINISH(bl);
  }
  void dump(Formatter *f) const;
  void decode_json(JSONObj *obj);
  static void generate_test_instances(list<rgw_bucket_entry_ver*>& o);
};
WRITE_CLASS_ENCODER(rgw_bucket_entry_ver)

struct cls_rgw_obj_key {
  string name;
  string instance;

  cls_rgw_obj_key() {}
  cls_rgw_obj_key(const string &_name) : name(_name) {}
  cls_rgw_obj_key(const string& n, const string& i) : name(n), instance(i) {}

  bool operator==(const cls_rgw_obj_key& k) const {
    return (name.compare(k.name) == 0) &&
           (instance.compare(k.instance) == 0);
  }
  bool operator<(const cls_rgw_obj_key& k) const {
    int r = name.compare(k.name);
    if (r == 0) {
      r = instance.compare(k.instance);
    }
    return (r < 0);
  }
  void encode(bufferlist &bl) const {
    ENCODE_START(1, 1, bl);
    ::encode(name, bl);
    ::encode(instance, bl);
    ENCODE_FINISH(bl);
  }
  void decode(bufferlist::iterator &bl) {
    DECODE_START(1, bl);
    ::decode(name, bl);
    ::decode(instance, bl);
    DECODE_FINISH(bl);
  }
  void dump(Formatter *f) const {
    f->dump_string("name", name);
    f->dump_string("instance", instance);
  }
  void decode_json(JSONObj *obj);
  static void generate_test_instances(list<cls_rgw_obj_key*>& ls) {
    ls.push_back(new cls_rgw_obj_key);
    ls.push_back(new cls_rgw_obj_key);
    ls.back()->name = "name";
    ls.back()->instance = "instance";
  }
};
WRITE_CLASS_ENCODER(cls_rgw_obj_key)


#define RGW_BUCKET_DIRENT_FLAG_VER           0x1    /* a versioned object instance */
#define RGW_BUCKET_DIRENT_FLAG_CURRENT       0x2    /* the last object instance of a versioned object */
#define RGW_BUCKET_DIRENT_FLAG_DELETE_MARKER 0x4    /* delete marker */
#define RGW_BUCKET_DIRENT_FLAG_VER_MARKER    0x8    /* object is versioned, a placeholder for the plain entry */

struct rgw_bucket_dir_entry {
  cls_rgw_obj_key key;
  rgw_bucket_entry_ver ver;
  std::string locator;
  bool exists;
  struct rgw_bucket_dir_entry_meta meta;
  multimap<string, struct rgw_bucket_pending_info> pending_map;
  uint64_t index_ver;
  string tag;
  uint16_t flags;
  uint64_t versioned_epoch;

  rgw_bucket_dir_entry() :
    exists(false), index_ver(0), flags(0), versioned_epoch(0) {}

  void encode(bufferlist &bl) const {
    ENCODE_START(8, 3, bl);
    ::encode(key.name, bl);
    ::encode(ver.epoch, bl);
    ::encode(exists, bl);
    ::encode(meta, bl);
    ::encode(pending_map, bl);
    ::encode(locator, bl);
    ::encode(ver, bl);
    ::encode_packed_val(index_ver, bl);
    ::encode(tag, bl);
    ::encode(key.instance, bl);
    ::encode(flags, bl);
    ::encode(versioned_epoch, bl);
    ENCODE_FINISH(bl);
  }
  void decode(bufferlist::iterator &bl) {
    DECODE_START_LEGACY_COMPAT_LEN(6, 3, 3, bl);
    ::decode(key.name, bl);
    ::decode(ver.epoch, bl);
    ::decode(exists, bl);
    ::decode(meta, bl);
    ::decode(pending_map, bl);
    if (struct_v >= 2) {
      ::decode(locator, bl);
    }
    if (struct_v >= 4) {
      ::decode(ver, bl);
    } else {
      ver.pool = -1;
    }
    if (struct_v >= 5) {
      ::decode_packed_val(index_ver, bl);
      ::decode(tag, bl);
    }
    if (struct_v >= 6) {
      ::decode(key.instance, bl);
    }
    if (struct_v >= 7) {
      ::decode(flags, bl);
    }
    if (struct_v >= 8) {
      ::decode(versioned_epoch, bl);
    }
    DECODE_FINISH(bl);
  }

  bool is_current() {
    int test_flags = RGW_BUCKET_DIRENT_FLAG_VER | RGW_BUCKET_DIRENT_FLAG_CURRENT;
    return (flags & RGW_BUCKET_DIRENT_FLAG_VER) == 0 ||
           (flags & test_flags) == test_flags;
  }
  bool is_delete_marker() { return (flags & RGW_BUCKET_DIRENT_FLAG_DELETE_MARKER) != 0; }
  bool is_visible() {
    return is_current() && !is_delete_marker();
  }
  bool is_valid() { return (flags & RGW_BUCKET_DIRENT_FLAG_VER_MARKER) == 0; }

  void dump(Formatter *f) const;
  void decode_json(JSONObj *obj);
  static void generate_test_instances(list<rgw_bucket_dir_entry*>& o);
};
WRITE_CLASS_ENCODER(rgw_bucket_dir_entry)

enum BIIndexType {
  InvalidIdx    = 0,
  PlainIdx      = 1,
  InstanceIdx   = 2,
  OLHIdx        = 3,
};

struct rgw_cls_bi_entry {
  BIIndexType type;
  string idx;
  bufferlist data;

  rgw_cls_bi_entry() : type(InvalidIdx) {}

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    ::encode((uint8_t)type, bl);
    ::encode(idx, bl);
    ::encode(data, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::iterator& bl) {
    DECODE_START(1, bl);
    uint8_t c;
    ::decode(c, bl);
    type = (BIIndexType)c;
    ::decode(idx, bl);
    ::decode(data, bl);
    DECODE_FINISH(bl);
  }

  void dump(Formatter *f) const;
  void decode_json(JSONObj *obj, cls_rgw_obj_key *effective_key = NULL);
};
WRITE_CLASS_ENCODER(rgw_cls_bi_entry)

enum OLHLogOp {
  CLS_RGW_OLH_OP_UNKNOWN         = 0,
  CLS_RGW_OLH_OP_LINK_OLH        = 1,
  CLS_RGW_OLH_OP_UNLINK_OLH      = 2, /* object does not exist */
  CLS_RGW_OLH_OP_REMOVE_INSTANCE = 3,
};

struct rgw_bucket_olh_log_entry {
  uint64_t epoch;
  OLHLogOp op;
  string op_tag;
  cls_rgw_obj_key key;
  bool delete_marker;

  rgw_bucket_olh_log_entry() : epoch(0), op(CLS_RGW_OLH_OP_UNKNOWN), delete_marker(false) {}


  void encode(bufferlist &bl) const {
    ENCODE_START(1, 1, bl);
    ::encode(epoch, bl);
    ::encode((__u8)op, bl);
    ::encode(op_tag, bl);
    ::encode(key, bl);
    ::encode(delete_marker, bl);
    ENCODE_FINISH(bl);
  }
  void decode(bufferlist::iterator &bl) {
    DECODE_START(1, bl);
    ::decode(epoch, bl);
    uint8_t c;
    ::decode(c, bl);
    op = (OLHLogOp)c;
    ::decode(op_tag, bl);
    ::decode(key, bl);
    ::decode(delete_marker, bl);
    DECODE_FINISH(bl);
  }
  static void generate_test_instances(list<rgw_bucket_olh_log_entry*>& o);
  void dump(Formatter *f) const;
  void decode_json(JSONObj *obj);
};
WRITE_CLASS_ENCODER(rgw_bucket_olh_log_entry)

struct rgw_bucket_olh_entry {
  cls_rgw_obj_key key;
  bool delete_marker;
  uint64_t epoch;
  map<uint64_t, vector<struct rgw_bucket_olh_log_entry> > pending_log;
  string tag;
  bool exists;
  bool pending_removal;

  rgw_bucket_olh_entry() : delete_marker(false), epoch(0), exists(false), pending_removal(false) {}

  void encode(bufferlist &bl) const {
    ENCODE_START(1, 1, bl);
    ::encode(key, bl);
    ::encode(delete_marker, bl);
    ::encode(epoch, bl);
    ::encode(pending_log, bl);
    ::encode(tag, bl);
    ::encode(exists, bl);
    ::encode(pending_removal, bl);
    ENCODE_FINISH(bl);
  }
  void decode(bufferlist::iterator &bl) {
    DECODE_START(1, bl);
    ::decode(key, bl);
    ::decode(delete_marker, bl);
    ::decode(epoch, bl);
    ::decode(pending_log, bl);
    ::decode(tag, bl);
    ::decode(exists, bl);
    ::decode(pending_removal, bl);
    DECODE_FINISH(bl);
  }
  void dump(Formatter *f) const;
  void decode_json(JSONObj *obj);
};
WRITE_CLASS_ENCODER(rgw_bucket_olh_entry)

struct rgw_bi_log_entry {
  string id;
  string object;
  string instance;
  utime_t timestamp;
  rgw_bucket_entry_ver ver;
  RGWModifyOp op;
  RGWPendingState state;
  uint64_t index_ver;
  string tag;
  uint16_t bilog_flags;
  string owner; /* only being set if it's a delete marker */
  string owner_display_name; /* only being set if it's a delete marker */

  rgw_bi_log_entry() : op(CLS_RGW_OP_UNKNOWN), state(CLS_RGW_STATE_PENDING_MODIFY), index_ver(0), bilog_flags(0) {}

  void encode(bufferlist &bl) const {
    ENCODE_START(3, 1, bl);
    ::encode(id, bl);
    ::encode(object, bl);
    ::encode(timestamp, bl);
    ::encode(ver, bl);
    ::encode(tag, bl);
    uint8_t c = (uint8_t)op;
    ::encode(c, bl);
    c = (uint8_t)state;
    ::encode(c, bl);
    encode_packed_val(index_ver, bl);
    ::encode(instance, bl);
    ::encode(bilog_flags, bl);
    ::encode(owner, bl);
    ::encode(owner_display_name, bl);
    ENCODE_FINISH(bl);
  }
  void decode(bufferlist::iterator &bl) {
    DECODE_START(2, bl);
    ::decode(id, bl);
    ::decode(object, bl);
    ::decode(timestamp, bl);
    ::decode(ver, bl);
    ::decode(tag, bl);
    uint8_t c;
    ::decode(c, bl);
    op = (RGWModifyOp)c;
    ::decode(c, bl);
    state = (RGWPendingState)c;
    decode_packed_val(index_ver, bl);
    if (struct_v >= 2) {
      ::decode(instance, bl);
      ::decode(bilog_flags, bl);
    }
    if (struct_v >= 3) {
      ::decode(owner, bl);
      ::decode(owner_display_name, bl);
    }
    DECODE_FINISH(bl);
  }
  void dump(Formatter *f) const;
  void decode_json(JSONObj *obj);
  static void generate_test_instances(list<rgw_bi_log_entry*>& o);
  
  bool is_versioned() {
    return ((bilog_flags & RGW_BILOG_FLAG_VERSIONED_OP) != 0);
  }
};
WRITE_CLASS_ENCODER(rgw_bi_log_entry)

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
  uint64_t ver;
  uint64_t master_ver;
  string max_marker;

  rgw_bucket_dir_header() : tag_timeout(0), ver(0), master_ver(0) {}

  void encode(bufferlist &bl) const {
    ENCODE_START(5, 2, bl);
    ::encode(stats, bl);
    ::encode(tag_timeout, bl);
    ::encode(ver, bl);
    ::encode(master_ver, bl);
    ::encode(max_marker, bl);
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
    if (struct_v >= 4) {
      ::decode(ver, bl);
      ::decode(master_ver, bl);
    } else {
      ver = 0;
    }
    if (struct_v >= 5) {
      ::decode(max_marker, bl);
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
  rgw_user owner;
  rgw_user payer; /* if empty, same as owner */
  string bucket;
  uint64_t epoch;
  rgw_usage_data total_usage; /* this one is kept for backwards compatibility */
  map<string, rgw_usage_data> usage_map;

  rgw_usage_log_entry() : epoch(0) {}
  rgw_usage_log_entry(string& o, string& b) : owner(o), bucket(b), epoch(0) {}
  rgw_usage_log_entry(string& o, string& p, string& b) : owner(o), payer(p), bucket(b), epoch(0) {}

  void encode(bufferlist& bl) const {
    ENCODE_START(3, 1, bl);
    ::encode(owner.to_str(), bl);
    ::encode(bucket, bl);
    ::encode(epoch, bl);
    ::encode(total_usage.bytes_sent, bl);
    ::encode(total_usage.bytes_received, bl);
    ::encode(total_usage.ops, bl);
    ::encode(total_usage.successful_ops, bl);
    ::encode(usage_map, bl);
    ::encode(payer.to_str(), bl);
    ENCODE_FINISH(bl);
  }


   void decode(bufferlist::iterator& bl) {
    DECODE_START(3, bl);
    string s;
    ::decode(s, bl);
    owner.from_str(s);
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
    if (struct_v >= 3) {
      string p;
      ::decode(p, bl);
      payer.from_str(p);
    }
    DECODE_FINISH(bl);
  }

  void aggregate(const rgw_usage_log_entry& e, map<string, bool> *categories = NULL) {
    if (owner.empty()) {
      owner = e.owner;
      bucket = e.bucket;
      epoch = e.epoch;
      payer = e.payer;
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
  rgw_user_bucket(const string& u, const string& b) : user(u), bucket(b) {}

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
  cls_rgw_obj_key key;
  string loc;

  cls_rgw_obj() {}
  cls_rgw_obj(string& _p, cls_rgw_obj_key& _k) : pool(_p), key(_k) {}

  void encode(bufferlist& bl) const {
    ENCODE_START(2, 1, bl);
    ::encode(pool, bl);
    ::encode(key.name, bl);
    ::encode(loc, bl);
    ::encode(key, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::iterator& bl) {
    DECODE_START(2, bl);
    ::decode(pool, bl);
    ::decode(key.name, bl);
    ::decode(loc, bl);
    if (struct_v >= 2) {
      ::decode(key, bl);
    }
    DECODE_FINISH(bl);
  }

  void dump(Formatter *f) const {
    f->dump_string("pool", pool);
    f->dump_string("oid", key.name);
    f->dump_string("key", loc);
    f->dump_string("instance", key.instance);
  }
  static void generate_test_instances(list<cls_rgw_obj*>& ls) {
    ls.push_back(new cls_rgw_obj);
    ls.push_back(new cls_rgw_obj);
    ls.back()->pool = "mypool";
    ls.back()->key.name = "myoid";
    ls.back()->loc = "mykey";
  }
};
WRITE_CLASS_ENCODER(cls_rgw_obj)

struct cls_rgw_obj_chain {
  list<cls_rgw_obj> objs;

  cls_rgw_obj_chain() {}

  void push_obj(string& pool, cls_rgw_obj_key& key, string& loc) {
    cls_rgw_obj obj;
    obj.pool = pool;
    obj.key = key;
    obj.loc = loc;
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

  void dump(Formatter *f) const {
    f->open_array_section("objs");
    for (list<cls_rgw_obj>::const_iterator p = objs.begin(); p != objs.end(); ++p) {
      f->open_object_section("obj");
      p->dump(f);
      f->close_section();
    }
    f->close_section();
  }
  static void generate_test_instances(list<cls_rgw_obj_chain*>& ls) {
    ls.push_back(new cls_rgw_obj_chain);
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

  void dump(Formatter *f) const {
    f->dump_string("tag", tag);
    f->open_object_section("chain");
    chain.dump(f);
    f->close_section();
    f->dump_stream("time") << time;
  }
  static void generate_test_instances(list<cls_rgw_gc_obj_info*>& ls) {
    ls.push_back(new cls_rgw_gc_obj_info);
    ls.push_back(new cls_rgw_gc_obj_info);
    ls.back()->tag = "footag";
    ls.back()->time = utime_t(21, 32);
  }
};
WRITE_CLASS_ENCODER(cls_rgw_gc_obj_info)

#endif
