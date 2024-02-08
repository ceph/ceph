// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <string>
#include <list>
#include <boost/container/flat_map.hpp>
#include "common/ceph_time.h"
#include "common/Formatter.h"

#include <fmt/format.h>

#include "rgw/rgw_basic_types.h"

#define CEPH_RGW_REMOVE 'r' // value 114
#define CEPH_RGW_UPDATE 'u' // value 117
#define CEPH_RGW_DIR_SUGGEST_LOG_OP  0x80
#define CEPH_RGW_DIR_SUGGEST_OP_MASK 0x7f

constexpr uint64_t CEPH_RGW_DEFAULT_TAG_TIMEOUT = 120; // in seconds

class JSONObj;

using ceph::operator <<;

struct rgw_zone_set_entry {
  std::string zone;
  std::optional<std::string> location_key;

  bool operator<(const rgw_zone_set_entry& e) const {
    if (zone < e.zone) {
      return true;
    }
    if (zone > e.zone) {
      return false;
    }
    return (location_key < e.location_key);
  }

  bool operator==(const rgw_zone_set_entry& e) const {
    return zone == e.zone && location_key == e.location_key;
  }

  rgw_zone_set_entry() {}
  rgw_zone_set_entry(const std::string& _zone,
                     std::optional<std::string> _location_key) : zone(_zone),
                                                                location_key(_location_key) {}
  rgw_zone_set_entry(const std::string& s) {
    from_str(s);
  }

  void from_str(const std::string& s);
  std::string to_str() const;

  void encode(ceph::buffer::list &bl) const;
  void decode(ceph::buffer::list::const_iterator &bl);

  void dump(ceph::Formatter *f) const;
  void decode_json(JSONObj *obj);
};
WRITE_CLASS_ENCODER(rgw_zone_set_entry)

struct rgw_zone_set {
  std::set<rgw_zone_set_entry> entries;

  void encode(ceph::buffer::list &bl) const {
    /* no ENCODE_START, ENCODE_END for backward compatibility */
    ceph::encode(entries, bl);
  }
  void decode(ceph::buffer::list::const_iterator &bl) {
    /* no DECODE_START, DECODE_END for backward compatibility */
    ceph::decode(entries, bl);
  }
  void dump(ceph::Formatter *f) const;
  static void generate_test_instances(std::list<rgw_zone_set*>& o);
  void insert(const std::string& zone, std::optional<std::string> location_key);
  bool exists(const std::string& zone, std::optional<std::string> location_key) const;
};
WRITE_CLASS_ENCODER(rgw_zone_set)

/* backward compatibility, rgw_zone_set needs to encode/decode the same as std::set */
void encode_json(const char *name, const rgw_zone_set& zs, ceph::Formatter *f);
void decode_json_obj(rgw_zone_set& zs, JSONObj *obj);


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
  CLS_RGW_OP_SYNCSTOP        = 7,
  CLS_RGW_OP_RESYNC          = 8,
};

std::string_view to_string(RGWModifyOp op);
RGWModifyOp parse_modify_op(std::string_view name);

inline std::ostream& operator<<(std::ostream& out, RGWModifyOp op) {
  return out << to_string(op);
}

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

#define ROUND_BLOCK_SIZE 4096

inline uint64_t cls_rgw_get_rounded_size(uint64_t size) {
  return (size + ROUND_BLOCK_SIZE - 1) & ~(ROUND_BLOCK_SIZE - 1);
}

/*
 * This takes a std::string that either wholly contains a delimiter or is a
 * path that ends with a delimiter and appends a new character to the
 * end such that when a we request bucket-index entries *after* this,
 * we'll get the next object after the "subdirectory". This works
 * because we append a '\xFF' character, and no valid UTF-8 character
 * can contain that byte, so no valid entries can be skipped.
 */
inline std::string cls_rgw_after_delim(const std::string& path) {
  // assert: ! path.empty()
  return path + '\xFF';
}

struct rgw_bucket_pending_info {
  RGWPendingState state;
  ceph::real_time timestamp;
  uint8_t op;

  rgw_bucket_pending_info() : state(CLS_RGW_STATE_PENDING_MODIFY), op(0) {}

  void encode(ceph::buffer::list &bl) const {
    ENCODE_START(2, 2, bl);
    uint8_t s = (uint8_t)state;
    encode(s, bl);
    encode(timestamp, bl);
    encode(op, bl);
    ENCODE_FINISH(bl);
  }
  void decode(ceph::buffer::list::const_iterator &bl) {
    DECODE_START_LEGACY_COMPAT_LEN(2, 2, 2, bl);
    uint8_t s;
    decode(s, bl);
    state = (RGWPendingState)s;
    decode(timestamp, bl);
    decode(op, bl);
    DECODE_FINISH(bl);
  }
  void dump(ceph::Formatter *f) const;
  void decode_json(JSONObj *obj);
  static void generate_test_instances(std::list<rgw_bucket_pending_info*>& o);
};
WRITE_CLASS_ENCODER(rgw_bucket_pending_info)


// categories of objects stored in a bucket index (b-i) and used to
// differentiate their associated statistics (bucket stats, and in
// some cases user stats)
enum class RGWObjCategory : uint8_t {
  None      = 0,  // b-i entries for delete markers; also used in
                  // testing and for default values in default
                  // constructors

  Main      = 1,  // b-i entries for standard objs

  Shadow    = 2,  // presumably intended for multipart shadow
                  // uploads; not currently used in the codebase

  MultiMeta = 3,  // b-i entries for multipart upload metadata objs

  CloudTiered = 4, // b-i entries which are tiered to external cloud
};

std::string_view to_string(RGWObjCategory c);

inline std::ostream& operator<<(std::ostream& out, RGWObjCategory c) {
  return out << to_string(c);
}

struct rgw_bucket_dir_entry_meta {
  RGWObjCategory category;
  uint64_t size;
  ceph::real_time mtime;
  std::string etag;
  std::string owner;
  std::string owner_display_name;
  std::string content_type;
  uint64_t accounted_size;
  std::string user_data;
  std::string storage_class;
  bool appendable;

  rgw_bucket_dir_entry_meta() :
    category(RGWObjCategory::None), size(0), accounted_size(0), appendable(false) { }

  void encode(ceph::buffer::list &bl) const {
    ENCODE_START(7, 3, bl);
    encode(category, bl);
    encode(size, bl);
    encode(mtime, bl);
    encode(etag, bl);
    encode(owner, bl);
    encode(owner_display_name, bl);
    encode(content_type, bl);
    encode(accounted_size, bl);
    encode(user_data, bl);
    encode(storage_class, bl);
    encode(appendable, bl);
    ENCODE_FINISH(bl);
  }

  void decode(ceph::buffer::list::const_iterator &bl) {
    DECODE_START_LEGACY_COMPAT_LEN(6, 3, 3, bl);
    decode(category, bl);
    decode(size, bl);
    decode(mtime, bl);
    decode(etag, bl);
    decode(owner, bl);
    decode(owner_display_name, bl);
    if (struct_v >= 2)
      decode(content_type, bl);
    if (struct_v >= 4)
      decode(accounted_size, bl);
    else
      accounted_size = size;
    if (struct_v >= 5)
      decode(user_data, bl);
    if (struct_v >= 6)
      decode(storage_class, bl);
    if (struct_v >= 7)
      decode(appendable, bl);
    DECODE_FINISH(bl);
  }
  void dump(ceph::Formatter *f) const;
  void decode_json(JSONObj *obj);
  static void generate_test_instances(std::list<rgw_bucket_dir_entry_meta*>& o);
};
WRITE_CLASS_ENCODER(rgw_bucket_dir_entry_meta)

template<class T>
void encode_packed_val(T val, ceph::buffer::list& bl)
{
  using ceph::encode;
  if ((uint64_t)val < 0x80) {
    encode((uint8_t)val, bl);
  } else {
    unsigned char c = 0x80;

    if ((uint64_t)val < 0x100) {
      c |= 1;
      encode(c, bl);
      encode((uint8_t)val, bl);
    } else if ((uint64_t)val <= 0x10000) {
      c |= 2;
      encode(c, bl);
      encode((uint16_t)val, bl);
    } else if ((uint64_t)val <= 0x1000000) {
      c |= 4;
      encode(c, bl);
      encode((uint32_t)val, bl);
    } else {
      c |= 8;
      encode(c, bl);
      encode((uint64_t)val, bl);
    }
  }
}

template<class T>
void decode_packed_val(T& val, ceph::buffer::list::const_iterator& bl)
{
  using ceph::decode;
  unsigned char c;
  decode(c, bl);
  if (c < 0x80) {
    val = c;
    return;
  }

  c &= ~0x80;

  switch (c) {
    case 1:
      {
        uint8_t v;
        decode(v, bl);
        val = v;
      }
      break;
    case 2:
      {
        uint16_t v;
        decode(v, bl);
        val = v;
      }
      break;
    case 4:
      {
        uint32_t v;
        decode(v, bl);
        val = v;
      }
      break;
    case 8:
      {
        uint64_t v;
        decode(v, bl);
        val = v;
      }
      break;
    default:
      throw ceph::buffer::malformed_input();
  }
}

struct rgw_bucket_entry_ver {
  int64_t pool;
  uint64_t epoch;

  rgw_bucket_entry_ver() : pool(-1), epoch(0) {}

  void encode(ceph::buffer::list &bl) const {
    ENCODE_START(1, 1, bl);
    encode_packed_val(pool, bl);
    encode_packed_val(epoch, bl);
    ENCODE_FINISH(bl);
  }
  void decode(ceph::buffer::list::const_iterator &bl) {
    DECODE_START(1, bl);
    decode_packed_val(pool, bl);
    decode_packed_val(epoch, bl);
    DECODE_FINISH(bl);
  }
  void dump(ceph::Formatter *f) const;
  void decode_json(JSONObj *obj);
  static void generate_test_instances(std::list<rgw_bucket_entry_ver*>& o);
};
WRITE_CLASS_ENCODER(rgw_bucket_entry_ver)

typedef rgw_obj_index_key cls_rgw_obj_key;

inline std::ostream& operator<<(std::ostream& out, const cls_rgw_obj_key& o) {
  out << o.name;
  if (!o.instance.empty()) {
    out << '[' << o.instance << ']';
  }
  return out;
}

struct rgw_bucket_dir_entry {
  /* a versioned object instance */
  static constexpr uint16_t FLAG_VER =                0x1;
  /* the last object instance of a versioned object */
  static constexpr uint16_t FLAG_CURRENT =            0x2;
  /* delete marker */
  static constexpr uint16_t FLAG_DELETE_MARKER =      0x4;
  /* object is versioned, a placeholder for the plain entry */
  static constexpr uint16_t FLAG_VER_MARKER =         0x8;
  /* object is a proxy; it is not listed in the bucket index but is a
   * prefix ending with a delimiter, perhaps common to multiple
   * entries; it is only useful when a delimiter is used and
   * represents a "subdirectory" (again, ending in a delimiter) that
   * may contain one or more actual entries/objects */
  static constexpr uint16_t FLAG_COMMON_PREFIX =   0x8000;

  cls_rgw_obj_key key;
  rgw_bucket_entry_ver ver;
  std::string locator;
  bool exists;
  rgw_bucket_dir_entry_meta meta;
  std::multimap<std::string, rgw_bucket_pending_info> pending_map;
  uint64_t index_ver;
  std::string tag;
  uint16_t flags;
  uint64_t versioned_epoch;

  rgw_bucket_dir_entry() :
    exists(false), index_ver(0), flags(0), versioned_epoch(0) {}

  void encode(ceph::buffer::list &bl) const {
    ENCODE_START(8, 3, bl);
    encode(key.name, bl);
    encode(ver.epoch, bl);
    encode(exists, bl);
    encode(meta, bl);
    encode(pending_map, bl);
    encode(locator, bl);
    encode(ver, bl);
    encode_packed_val(index_ver, bl);
    encode(tag, bl);
    encode(key.instance, bl);
    encode(flags, bl);
    encode(versioned_epoch, bl);
    ENCODE_FINISH(bl);
  }
  void decode(ceph::buffer::list::const_iterator &bl) {
    DECODE_START_LEGACY_COMPAT_LEN(8, 3, 3, bl);
    decode(key.name, bl);
    decode(ver.epoch, bl);
    decode(exists, bl);
    decode(meta, bl);
    decode(pending_map, bl);
    if (struct_v >= 2) {
      decode(locator, bl);
    }
    if (struct_v >= 4) {
      decode(ver, bl);
    } else {
      ver.pool = -1;
    }
    if (struct_v >= 5) {
      decode_packed_val(index_ver, bl);
      decode(tag, bl);
    }
    if (struct_v >= 6) {
      decode(key.instance, bl);
    }
    if (struct_v >= 7) {
      decode(flags, bl);
    }
    if (struct_v >= 8) {
      decode(versioned_epoch, bl);
    }
    DECODE_FINISH(bl);
  }

  bool is_current() const {
    int test_flags =
      rgw_bucket_dir_entry::FLAG_VER | rgw_bucket_dir_entry::FLAG_CURRENT;
    return (flags & rgw_bucket_dir_entry::FLAG_VER) == 0 ||
           (flags & test_flags) == test_flags;
  }
  bool is_delete_marker() const {
    return (flags & rgw_bucket_dir_entry::FLAG_DELETE_MARKER) != 0;
  }
  bool is_visible() const {
    return is_current() && !is_delete_marker();
  }
  bool is_valid() const {
    return (flags & rgw_bucket_dir_entry::FLAG_VER_MARKER) == 0;
  }
  bool is_common_prefix() const {
    return flags & rgw_bucket_dir_entry::FLAG_COMMON_PREFIX;
  }

  void dump(ceph::Formatter *f) const;
  void decode_json(JSONObj *obj);
  static void generate_test_instances(std::list<rgw_bucket_dir_entry*>& o);
};
WRITE_CLASS_ENCODER(rgw_bucket_dir_entry)

enum class BIIndexType : uint8_t {
  Invalid    = 0,
  Plain      = 1,
  Instance   = 2,
  OLH        = 3,
};

struct rgw_bucket_category_stats;

struct rgw_cls_bi_entry {
  BIIndexType type;
  std::string idx;
  ceph::buffer::list data;

  rgw_cls_bi_entry() : type(BIIndexType::Invalid) {}

  void encode(ceph::buffer::list& bl) const {
    ENCODE_START(1, 1, bl);
    encode(type, bl);
    encode(idx, bl);
    encode(data, bl);
    ENCODE_FINISH(bl);
  }

  void decode(ceph::buffer::list::const_iterator& bl) {
    DECODE_START(1, bl);
    uint8_t c;
    decode(c, bl);
    type = (BIIndexType)c;
    decode(idx, bl);
    decode(data, bl);
    DECODE_FINISH(bl);
  }

  void dump(ceph::Formatter *f) const;
  void decode_json(JSONObj *obj, cls_rgw_obj_key *effective_key = NULL);
  static void generate_test_instances(std::list<rgw_cls_bi_entry*>& o);
  bool get_info(cls_rgw_obj_key *key, RGWObjCategory *category,
		rgw_bucket_category_stats *accounted_stats);
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
  std::string op_tag;
  cls_rgw_obj_key key;
  bool delete_marker;

  rgw_bucket_olh_log_entry() : epoch(0), op(CLS_RGW_OLH_OP_UNKNOWN), delete_marker(false) {}


  void encode(ceph::buffer::list &bl) const {
    ENCODE_START(1, 1, bl);
    encode(epoch, bl);
    encode((__u8)op, bl);
    encode(op_tag, bl);
    encode(key, bl);
    encode(delete_marker, bl);
    ENCODE_FINISH(bl);
  }
  void decode(ceph::buffer::list::const_iterator &bl) {
    DECODE_START(1, bl);
    decode(epoch, bl);
    uint8_t c;
    decode(c, bl);
    op = (OLHLogOp)c;
    decode(op_tag, bl);
    decode(key, bl);
    decode(delete_marker, bl);
    DECODE_FINISH(bl);
  }
  static void generate_test_instances(std::list<rgw_bucket_olh_log_entry*>& o);
  void dump(ceph::Formatter *f) const;
  void decode_json(JSONObj *obj);
};
WRITE_CLASS_ENCODER(rgw_bucket_olh_log_entry)

struct rgw_bucket_olh_entry {
  cls_rgw_obj_key key;
  bool delete_marker;
  uint64_t epoch;
  std::map<uint64_t, std::vector<struct rgw_bucket_olh_log_entry> > pending_log;
  std::string tag;
  bool exists;
  bool pending_removal;

  rgw_bucket_olh_entry() : delete_marker(false), epoch(0), exists(false), pending_removal(false) {}

  void encode(ceph::buffer::list &bl) const {
    ENCODE_START(1, 1, bl);
    encode(key, bl);
    encode(delete_marker, bl);
    encode(epoch, bl);
    encode(pending_log, bl);
    encode(tag, bl);
    encode(exists, bl);
    encode(pending_removal, bl);
    ENCODE_FINISH(bl);
  }
  void decode(ceph::buffer::list::const_iterator &bl) {
    DECODE_START(1, bl);
    decode(key, bl);
    decode(delete_marker, bl);
    decode(epoch, bl);
    decode(pending_log, bl);
    decode(tag, bl);
    decode(exists, bl);
    decode(pending_removal, bl);
    DECODE_FINISH(bl);
  }
  void dump(ceph::Formatter *f) const;
  void decode_json(JSONObj *obj);
  static void generate_test_instances(std::list<rgw_bucket_olh_entry*>& o);
};
WRITE_CLASS_ENCODER(rgw_bucket_olh_entry)

struct rgw_bi_log_entry {
  std::string id;
  std::string object;
  std::string instance;
  ceph::real_time timestamp;
  rgw_bucket_entry_ver ver;
  RGWModifyOp op;
  RGWPendingState state;
  uint64_t index_ver;
  std::string tag;
  uint16_t bilog_flags;
  std::string owner; /* only being set if it's a delete marker */
  std::string owner_display_name; /* only being set if it's a delete marker */
  rgw_zone_set zones_trace;

  rgw_bi_log_entry() : op(CLS_RGW_OP_UNKNOWN), state(CLS_RGW_STATE_PENDING_MODIFY), index_ver(0), bilog_flags(0) {}

  void encode(ceph::buffer::list &bl) const {
    ENCODE_START(4, 1, bl);
    encode(id, bl);
    encode(object, bl);
    encode(timestamp, bl);
    encode(ver, bl);
    encode(tag, bl);
    uint8_t c = (uint8_t)op;
    encode(c, bl);
    c = (uint8_t)state;
    encode(c, bl);
    encode_packed_val(index_ver, bl);
    encode(instance, bl);
    encode(bilog_flags, bl);
    encode(owner, bl);
    encode(owner_display_name, bl);
    encode(zones_trace, bl);
    ENCODE_FINISH(bl);
  }
  void decode(ceph::buffer::list::const_iterator &bl) {
    DECODE_START(4, bl);
    decode(id, bl);
    decode(object, bl);
    decode(timestamp, bl);
    decode(ver, bl);
    decode(tag, bl);
    uint8_t c;
    decode(c, bl);
    op = (RGWModifyOp)c;
    decode(c, bl);
    state = (RGWPendingState)c;
    decode_packed_val(index_ver, bl);
    if (struct_v >= 2) {
      decode(instance, bl);
      decode(bilog_flags, bl);
    }
    if (struct_v >= 3) {
      decode(owner, bl);
      decode(owner_display_name, bl);
    }
    if (struct_v >= 4) {
      decode(zones_trace, bl);
    }
    DECODE_FINISH(bl);
  }
  void dump(ceph::Formatter *f) const;
  void decode_json(JSONObj *obj);
  static void generate_test_instances(std::list<rgw_bi_log_entry*>& o);

  bool is_versioned() {
    return ((bilog_flags & RGW_BILOG_FLAG_VERSIONED_OP) != 0);
  }
};
WRITE_CLASS_ENCODER(rgw_bi_log_entry)

struct rgw_bucket_category_stats {
  uint64_t total_size;
  uint64_t total_size_rounded;
  uint64_t num_entries;
  uint64_t actual_size{0}; //< account for compression, encryption

  rgw_bucket_category_stats() : total_size(0), total_size_rounded(0), num_entries(0) {}

  void encode(ceph::buffer::list &bl) const {
    ENCODE_START(3, 2, bl);
    encode(total_size, bl);
    encode(total_size_rounded, bl);
    encode(num_entries, bl);
    encode(actual_size, bl);
    ENCODE_FINISH(bl);
  }
  void decode(ceph::buffer::list::const_iterator &bl) {
    DECODE_START_LEGACY_COMPAT_LEN(3, 2, 2, bl);
    decode(total_size, bl);
    decode(total_size_rounded, bl);
    decode(num_entries, bl);
    if (struct_v >= 3) {
      decode(actual_size, bl);
    } else {
      actual_size = total_size;
    }
    DECODE_FINISH(bl);
  }
  void dump(ceph::Formatter *f) const;
  static void generate_test_instances(std::list<rgw_bucket_category_stats*>& o);
};
WRITE_CLASS_ENCODER(rgw_bucket_category_stats)

inline bool operator==(const rgw_bucket_category_stats& lhs,
                       const rgw_bucket_category_stats& rhs) {
  return lhs.total_size == rhs.total_size
      && lhs.total_size_rounded == rhs.total_size_rounded
      && lhs.num_entries == rhs.num_entries
      && lhs.actual_size == rhs.actual_size;
}
inline bool operator!=(const rgw_bucket_category_stats& lhs,
                       const rgw_bucket_category_stats& rhs) {
  return !(lhs == rhs);
}

enum class cls_rgw_reshard_status : uint8_t {
  NOT_RESHARDING  = 0,
  IN_PROGRESS     = 1,
  DONE            = 2
};
std::ostream& operator<<(std::ostream&, cls_rgw_reshard_status);

inline std::string to_string(const cls_rgw_reshard_status status)
{
  switch (status) {
  case cls_rgw_reshard_status::NOT_RESHARDING:
    return "not-resharding";
  case cls_rgw_reshard_status::IN_PROGRESS:
    return "in-progress";
  case cls_rgw_reshard_status::DONE:
    return "done";
  };
  return "Unknown reshard status";
}

struct cls_rgw_bucket_instance_entry {
  using RESHARD_STATUS = cls_rgw_reshard_status;
  
  cls_rgw_reshard_status reshard_status{RESHARD_STATUS::NOT_RESHARDING};

  void encode(ceph::buffer::list& bl) const {
    ENCODE_START(3, 1, bl);
    encode((uint8_t)reshard_status, bl);
    { // fields removed in v2 but added back as empty in v3
      std::string bucket_instance_id;
      encode(bucket_instance_id, bl);
      int32_t num_shards{-1};
      encode(num_shards, bl);
    }
    ENCODE_FINISH(bl);
  }

  void decode(ceph::buffer::list::const_iterator& bl) {
    DECODE_START(3, bl);
    uint8_t s;
    decode(s, bl);
    reshard_status = (cls_rgw_reshard_status)s;
    if (struct_v != 2) { // fields removed from v2, added back in v3
      std::string bucket_instance_id;
      decode(bucket_instance_id, bl);
      int32_t num_shards{-1};
      decode(num_shards, bl);
    }
    DECODE_FINISH(bl);
  }

  void dump(ceph::Formatter *f) const;
  static void generate_test_instances(std::list<cls_rgw_bucket_instance_entry*>& o);

  void clear() {
    reshard_status = RESHARD_STATUS::NOT_RESHARDING;
  }

  void set_status(cls_rgw_reshard_status s) {
    reshard_status = s;
  }

  bool resharding() const {
    return reshard_status != RESHARD_STATUS::NOT_RESHARDING;
  }

  bool resharding_in_progress() const {
    return reshard_status == RESHARD_STATUS::IN_PROGRESS;
  }

  friend std::ostream& operator<<(std::ostream& out, const cls_rgw_bucket_instance_entry& v) {
    out << "instance entry reshard status: " << v.reshard_status;
    return out;
  }
};
WRITE_CLASS_ENCODER(cls_rgw_bucket_instance_entry)

using rgw_bucket_dir_stats = std::map<RGWObjCategory, rgw_bucket_category_stats>;

struct rgw_bucket_dir_header {
  rgw_bucket_dir_stats stats;
  uint64_t tag_timeout;
  uint64_t ver;
  uint64_t master_ver;
  std::string max_marker;
  cls_rgw_bucket_instance_entry new_instance;
  bool syncstopped;

  rgw_bucket_dir_header() : tag_timeout(0), ver(0), master_ver(0), syncstopped(false) {}

  void encode(ceph::buffer::list &bl) const {
    ENCODE_START(7, 2, bl);
    encode(stats, bl);
    encode(tag_timeout, bl);
    encode(ver, bl);
    encode(master_ver, bl);
    encode(max_marker, bl);
    encode(new_instance, bl);
    encode(syncstopped,bl);
    ENCODE_FINISH(bl);
  }
  void decode(ceph::buffer::list::const_iterator &bl) {
    DECODE_START_LEGACY_COMPAT_LEN(6, 2, 2, bl);
    decode(stats, bl);
    if (struct_v > 2) {
      decode(tag_timeout, bl);
    } else {
      tag_timeout = 0;
    }
    if (struct_v >= 4) {
      decode(ver, bl);
      decode(master_ver, bl);
    } else {
      ver = 0;
    }
    if (struct_v >= 5) {
      decode(max_marker, bl);
    }
    if (struct_v >= 6) {
      decode(new_instance, bl);
    } else {
      new_instance = cls_rgw_bucket_instance_entry();
    }
    if (struct_v >= 7) {
      decode(syncstopped,bl);
    }
    DECODE_FINISH(bl);
  }
  void dump(ceph::Formatter *f) const;
  static void generate_test_instances(std::list<rgw_bucket_dir_header*>& o);

  bool resharding() const {
    return new_instance.resharding();
  }
  bool resharding_in_progress() const {
    return new_instance.resharding_in_progress();
  }
};
WRITE_CLASS_ENCODER(rgw_bucket_dir_header)

struct rgw_bucket_dir {
  rgw_bucket_dir_header header;
  boost::container::flat_map<std::string, rgw_bucket_dir_entry> m;

  void encode(ceph::buffer::list &bl) const {
    ENCODE_START(2, 2, bl);
    encode(header, bl);
    encode(m, bl);
    ENCODE_FINISH(bl);
  }
  void decode(ceph::buffer::list::const_iterator &bl) {
    DECODE_START_LEGACY_COMPAT_LEN(2, 2, 2, bl);
    decode(header, bl);
    decode(m, bl);
    DECODE_FINISH(bl);
  }
  void dump(ceph::Formatter *f) const;
  static void generate_test_instances(std::list<rgw_bucket_dir*>& o);
};
WRITE_CLASS_ENCODER(rgw_bucket_dir)

struct rgw_usage_data {
  uint64_t bytes_sent;
  uint64_t bytes_received;
  uint64_t ops;
  uint64_t successful_ops;

  rgw_usage_data() : bytes_sent(0), bytes_received(0), ops(0), successful_ops(0) {}
  rgw_usage_data(uint64_t sent, uint64_t received) : bytes_sent(sent), bytes_received(received), ops(0), successful_ops(0) {}

  void encode(ceph::buffer::list& bl) const {
    ENCODE_START(1, 1, bl);
    encode(bytes_sent, bl);
    encode(bytes_received, bl);
    encode(ops, bl);
    encode(successful_ops, bl);
    ENCODE_FINISH(bl);
  }

  void decode(ceph::buffer::list::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(bytes_sent, bl);
    decode(bytes_received, bl);
    decode(ops, bl);
    decode(successful_ops, bl);
    DECODE_FINISH(bl);
  }

  void aggregate(const rgw_usage_data& usage) {
    bytes_sent += usage.bytes_sent;
    bytes_received += usage.bytes_received;
    ops += usage.ops;
    successful_ops += usage.successful_ops;
  }
  void dump(ceph::Formatter *f) const;
  static void generate_test_instances(std::list<rgw_usage_data*>& o);
};
WRITE_CLASS_ENCODER(rgw_usage_data)


struct rgw_usage_log_entry {
  rgw_user owner;
  rgw_user payer; /* if empty, same as owner */
  std::string bucket;
  uint64_t epoch;
  rgw_usage_data total_usage; /* this one is kept for backwards compatibility */
  std::map<std::string, rgw_usage_data> usage_map;

  rgw_usage_log_entry() : epoch(0) {}
  rgw_usage_log_entry(std::string& o, std::string& b) : owner(o), bucket(b), epoch(0) {}
  rgw_usage_log_entry(std::string& o, std::string& p, std::string& b) : owner(o), payer(p), bucket(b), epoch(0) {}

  void encode(ceph::buffer::list& bl) const {
    ENCODE_START(3, 1, bl);
    encode(owner.to_str(), bl);
    encode(bucket, bl);
    encode(epoch, bl);
    encode(total_usage.bytes_sent, bl);
    encode(total_usage.bytes_received, bl);
    encode(total_usage.ops, bl);
    encode(total_usage.successful_ops, bl);
    encode(usage_map, bl);
    encode(payer.to_str(), bl);
    ENCODE_FINISH(bl);
  }


   void decode(ceph::buffer::list::const_iterator& bl) {
    DECODE_START(3, bl);
    std::string s;
    decode(s, bl);
    owner.from_str(s);
    decode(bucket, bl);
    decode(epoch, bl);
    decode(total_usage.bytes_sent, bl);
    decode(total_usage.bytes_received, bl);
    decode(total_usage.ops, bl);
    decode(total_usage.successful_ops, bl);
    if (struct_v < 2) {
      usage_map[""] = total_usage;
    } else {
      decode(usage_map, bl);
    }
    if (struct_v >= 3) {
      std::string p;
      decode(p, bl);
      payer.from_str(p);
    }
    DECODE_FINISH(bl);
  }

  void aggregate(const rgw_usage_log_entry& e,
		 std::map<std::string, bool> *categories = NULL) {
    if (owner.empty()) {
      owner = e.owner;
      bucket = e.bucket;
      epoch = e.epoch;
      payer = e.payer;
    }

    for (auto iter = e.usage_map.begin(); iter != e.usage_map.end(); ++iter) {
      if (!categories || !categories->size() || categories->count(iter->first)) {
        add(iter->first, iter->second);
      }
    }
  }

  void sum(rgw_usage_data& usage,
	   std::map<std::string, bool>& categories) const {
    usage = rgw_usage_data();
    for (auto iter = usage_map.begin(); iter != usage_map.end(); ++iter) {
      if (!categories.size() || categories.count(iter->first)) {
        usage.aggregate(iter->second);
      }
    }
  }

  void add(const std::string& category, const rgw_usage_data& data) {
    usage_map[category].aggregate(data);
    total_usage.aggregate(data);
  }

  void dump(ceph::Formatter* f) const;
  static void generate_test_instances(std::list<rgw_usage_log_entry*>& o);

};
WRITE_CLASS_ENCODER(rgw_usage_log_entry)

struct rgw_usage_log_info {
  std::vector<rgw_usage_log_entry> entries;

  void encode(ceph::buffer::list& bl) const {
    ENCODE_START(1, 1, bl);
    encode(entries, bl);
    ENCODE_FINISH(bl);
  }

  void decode(ceph::buffer::list::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(entries, bl);
    DECODE_FINISH(bl);
  }
  void dump(ceph::Formatter* f) const;
  static void generate_test_instances(std::list<rgw_usage_log_info*>& o);

  rgw_usage_log_info() {}
};
WRITE_CLASS_ENCODER(rgw_usage_log_info)

struct rgw_user_bucket {
  std::string user;
  std::string bucket;

  rgw_user_bucket() {}
  rgw_user_bucket(const std::string& u, const std::string& b) : user(u), bucket(b) {}

  void encode(ceph::buffer::list& bl) const {
    ENCODE_START(1, 1, bl);
    encode(user, bl);
    encode(bucket, bl);
    ENCODE_FINISH(bl);
  }

  void decode(ceph::buffer::list::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(user, bl);
    decode(bucket, bl);
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
  void dump(ceph::Formatter* f) const;
  static void generate_test_instances(std::list<rgw_user_bucket*>& o);
};
WRITE_CLASS_ENCODER(rgw_user_bucket)

enum cls_rgw_gc_op {
  CLS_RGW_GC_DEL_OBJ,
  CLS_RGW_GC_DEL_BUCKET,
};

struct cls_rgw_obj {
  std::string pool;
  cls_rgw_obj_key key;
  std::string loc;

  cls_rgw_obj() {}
  cls_rgw_obj(std::string& _p, cls_rgw_obj_key& _k) : pool(_p), key(_k) {}

  void encode(ceph::buffer::list& bl) const {
    ENCODE_START(2, 1, bl);
    encode(pool, bl);
    encode(key.name, bl);
    encode(loc, bl);
    encode(key, bl);
    ENCODE_FINISH(bl);
  }

  void decode(ceph::buffer::list::const_iterator& bl) {
    DECODE_START(2, bl);
    decode(pool, bl);
    decode(key.name, bl);
    decode(loc, bl);
    if (struct_v >= 2) {
      decode(key, bl);
    }
    DECODE_FINISH(bl);
  }

  void dump(ceph::Formatter *f) const {
    f->dump_string("pool", pool);
    f->dump_string("oid", key.name);
    f->dump_string("key", loc);
    f->dump_string("instance", key.instance);
  }
  static void generate_test_instances(std::list<cls_rgw_obj*>& ls) {
    ls.push_back(new cls_rgw_obj);
    ls.push_back(new cls_rgw_obj);
    ls.back()->pool = "mypool";
    ls.back()->key.name = "myoid";
    ls.back()->loc = "mykey";
  }

  size_t estimate_encoded_size() const {
    constexpr size_t start_overhead = sizeof(__u8) + sizeof(__u8) + sizeof(ceph_le32); // version and length prefix
    constexpr size_t string_overhead = sizeof(__u32); // strings are encoded with 32-bit length prefix
    return start_overhead +
        string_overhead + pool.size() +
        string_overhead + key.name.size() +
        string_overhead + loc.size() +
        key.estimate_encoded_size();
  }
};
WRITE_CLASS_ENCODER(cls_rgw_obj)

struct cls_rgw_obj_chain {
  std::list<cls_rgw_obj> objs;

  cls_rgw_obj_chain() {}

  void push_obj(const std::string& pool, const cls_rgw_obj_key& key, const std::string& loc) {
    cls_rgw_obj obj;
    obj.pool = pool;
    obj.key = key;
    obj.loc = loc;
    objs.push_back(obj);
  }

  void encode(ceph::buffer::list& bl) const {
    ENCODE_START(1, 1, bl);
    encode(objs, bl);
    ENCODE_FINISH(bl);
  }

  void decode(ceph::buffer::list::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(objs, bl);
    DECODE_FINISH(bl);
  }

  void dump(ceph::Formatter *f) const {
    f->open_array_section("objs");
    for (std::list<cls_rgw_obj>::const_iterator p = objs.begin(); p != objs.end(); ++p) {
      f->open_object_section("obj");
      p->dump(f);
      f->close_section();
    }
    f->close_section();
  }
  static void generate_test_instances(std::list<cls_rgw_obj_chain*>& ls) {
    ls.push_back(new cls_rgw_obj_chain);
  }

  bool empty() {
    return objs.empty();
  }

  size_t estimate_encoded_size() const {
    constexpr size_t start_overhead = sizeof(__u8) + sizeof(__u8) + sizeof(ceph_le32);
    constexpr size_t size_overhead = sizeof(__u32); // size of the chain
    size_t chain_overhead = 0;
    for (auto& it : objs) {
      chain_overhead += it.estimate_encoded_size();
    }
    return (start_overhead + size_overhead + chain_overhead);
  }
};
WRITE_CLASS_ENCODER(cls_rgw_obj_chain)

struct cls_rgw_gc_obj_info
{
  std::string tag;
  cls_rgw_obj_chain chain;
  ceph::real_time time;

  cls_rgw_gc_obj_info() {}

  void encode(ceph::buffer::list& bl) const {
    ENCODE_START(1, 1, bl);
    encode(tag, bl);
    encode(chain, bl);
    encode(time, bl);
    ENCODE_FINISH(bl);
  }

  void decode(ceph::buffer::list::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(tag, bl);
    decode(chain, bl);
    decode(time, bl);
    DECODE_FINISH(bl);
  }

  void dump(ceph::Formatter *f) const {
    f->dump_string("tag", tag);
    f->open_object_section("chain");
    chain.dump(f);
    f->close_section();
    f->dump_stream("time") << time;
  }
  static void generate_test_instances(std::list<cls_rgw_gc_obj_info*>& ls) {
    ls.push_back(new cls_rgw_gc_obj_info);
    ls.push_back(new cls_rgw_gc_obj_info);
    ls.back()->tag = "footag";
    ceph_timespec ts{ceph_le32(21), ceph_le32(32)};
    ls.back()->time = ceph::real_clock::from_ceph_timespec(ts);
  }

  size_t estimate_encoded_size() const {
    constexpr size_t start_overhead = sizeof(__u8) + sizeof(__u8) + sizeof(ceph_le32); // version and length prefix
    constexpr size_t string_overhead = sizeof(__u32); // strings are encoded with 32-bit length prefix
    constexpr size_t time_overhead = 2 * sizeof(ceph_le32); // time is stored as tv_sec and tv_nsec
    return start_overhead + string_overhead + tag.size() +
            time_overhead + chain.estimate_encoded_size();
  }
};
WRITE_CLASS_ENCODER(cls_rgw_gc_obj_info)

struct cls_rgw_lc_obj_head
{
  time_t start_date = 0;
  std::string marker;
  time_t shard_rollover_date = 0;

  cls_rgw_lc_obj_head() {}

  void encode(ceph::buffer::list& bl) const {
    ENCODE_START(2, 2, bl);
    uint64_t t = start_date;
    encode(t, bl);
    encode(marker, bl);
    encode(shard_rollover_date, bl);
    ENCODE_FINISH(bl);
  }

  void decode(ceph::buffer::list::const_iterator& bl) {
    DECODE_START(2, bl);
    uint64_t t;
    decode(t, bl);
    start_date = static_cast<time_t>(t);
    decode(marker, bl);
    if (struct_v < 2) {
      shard_rollover_date = 0;
    } else {
      decode(t, bl);
      shard_rollover_date = static_cast<time_t>(t);
    }
    DECODE_FINISH(bl);
  }

  void dump(ceph::Formatter *f) const;
  static void generate_test_instances(std::list<cls_rgw_lc_obj_head*>& ls);
};
WRITE_CLASS_ENCODER(cls_rgw_lc_obj_head)

struct cls_rgw_lc_entry {
  std::string bucket;
  uint64_t start_time; // if in_progress
  uint32_t status;

  cls_rgw_lc_entry()
    : start_time(0), status(0) {}

  cls_rgw_lc_entry(const cls_rgw_lc_entry& rhs) = default;

  cls_rgw_lc_entry(const std::string& b, uint64_t t, uint32_t s)
    : bucket(b), start_time(t), status(s) {};

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    encode(bucket, bl);
    encode(start_time, bl);
    encode(status, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(bucket, bl);
    decode(start_time, bl);
    decode(status, bl);
    DECODE_FINISH(bl);
  }
  void dump(Formatter *f) const;
  static void generate_test_instances(std::list<cls_rgw_lc_entry*>& ls);
};
WRITE_CLASS_ENCODER(cls_rgw_lc_entry);

struct cls_rgw_reshard_entry
{
  ceph::real_time time;
  std::string tenant;
  std::string bucket_name;
  std::string bucket_id;
  uint32_t old_num_shards{0};
  uint32_t new_num_shards{0};

  cls_rgw_reshard_entry() {}

  void encode(ceph::buffer::list& bl) const {
    ENCODE_START(2, 1, bl);
    encode(time, bl);
    encode(tenant, bl);
    encode(bucket_name, bl);
    encode(bucket_id, bl);
    encode(old_num_shards, bl);
    encode(new_num_shards, bl);
    ENCODE_FINISH(bl);
  }

  void decode(ceph::buffer::list::const_iterator& bl) {
    DECODE_START(2, bl);
    decode(time, bl);
    decode(tenant, bl);
    decode(bucket_name, bl);
    decode(bucket_id, bl);
    if (struct_v < 2) {
      std::string new_instance_id; // removed in v2
      decode(new_instance_id, bl);
    }
    decode(old_num_shards, bl);
    decode(new_num_shards, bl);
    DECODE_FINISH(bl);
  }

  void dump(ceph::Formatter *f) const;
  static void generate_test_instances(std::list<cls_rgw_reshard_entry*>& o);

  static void generate_key(const std::string& tenant, const std::string& bucket_name, std::string *key);
  void get_key(std::string *key) const;
};
WRITE_CLASS_ENCODER(cls_rgw_reshard_entry)
