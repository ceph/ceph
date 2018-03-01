// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_RGWRADOS_H
#define CEPH_RGWRADOS_H

#include <functional>

#include "include/rados/librados.hpp"
#include "include/Context.h"
#include "common/admin_socket.h"
#include "common/RefCountedObj.h"
#include "common/RWLock.h"
#include "common/ceph_time.h"
#include "common/lru_map.h"
#include "rgw_common.h"
#include "cls/rgw/cls_rgw_types.h"
#include "cls/version/cls_version_types.h"
#include "cls/log/cls_log_types.h"
#include "cls/statelog/cls_statelog_types.h"
#include "cls/timeindex/cls_timeindex_types.h"
#include "rgw_log.h"
#include "rgw_metadata.h"
#include "rgw_meta_sync_status.h"
#include "rgw_period_puller.h"
#include "rgw_sync_module.h"
#include "rgw_sync_log_trim.h"

class RGWWatcher;
class SafeTimer;
class ACLOwner;
class RGWGC;
class RGWMetaNotifier;
class RGWDataNotifier;
class RGWLC;
class RGWObjectExpirer;
class RGWMetaSyncProcessorThread;
class RGWDataSyncProcessorThread;
class RGWSyncLogTrimThread;
class RGWSyncTraceManager;
class RGWRESTConn;
struct RGWZoneGroup;
struct RGWZoneParams;
class RGWReshard;
class RGWReshardWait;

/* flags for put_obj_meta() */
#define PUT_OBJ_CREATE      0x01
#define PUT_OBJ_EXCL        0x02
#define PUT_OBJ_CREATE_EXCL (PUT_OBJ_CREATE | PUT_OBJ_EXCL)

#define RGW_OBJ_NS_MULTIPART "multipart"
#define RGW_OBJ_NS_SHADOW    "shadow"

#define RGW_BUCKET_INSTANCE_MD_PREFIX ".bucket.meta."

#define RGW_NO_SHARD -1

#define RGW_SHARDS_PRIME_0 7877
#define RGW_SHARDS_PRIME_1 65521

// only called by rgw_shard_id and rgw_bucket_shard_index
static inline int rgw_shards_mod(unsigned hval, int max_shards)
{
  if (max_shards <= RGW_SHARDS_PRIME_0) {
    return hval % RGW_SHARDS_PRIME_0 % max_shards;
  }
  return hval % RGW_SHARDS_PRIME_1 % max_shards;
}

// used for logging and tagging
static inline int rgw_shard_id(const string& key, int max_shards)
{
  return rgw_shards_mod(ceph_str_hash_linux(key.c_str(), key.size()),
			max_shards);
}

// used for bucket indices
static inline uint32_t rgw_bucket_shard_index(const std::string& key,
					      int num_shards) {
  uint32_t sid = ceph_str_hash_linux(key.c_str(), key.size());
  uint32_t sid2 = sid ^ ((sid & 0xFF) << 24);
  return rgw_shards_mod(sid2, num_shards);
}

static inline int rgw_shards_max()
{
  return RGW_SHARDS_PRIME_1;
}

static inline void prepend_bucket_marker(const rgw_bucket& bucket, const string& orig_oid, string& oid)
{
  if (bucket.marker.empty() || orig_oid.empty()) {
    oid = orig_oid;
  } else {
    oid = bucket.marker;
    oid.append("_");
    oid.append(orig_oid);
  }
}

static inline void get_obj_bucket_and_oid_loc(const rgw_obj& obj, string& oid, string& locator)
{
  const rgw_bucket& bucket = obj.bucket;
  prepend_bucket_marker(bucket, obj.get_oid(), oid);
  const string& loc = obj.key.get_loc();
  if (!loc.empty()) {
    prepend_bucket_marker(bucket, loc, locator);
  } else {
    locator.clear();
  }
}

int rgw_init_ioctx(librados::Rados *rados, const rgw_pool& pool, librados::IoCtx& ioctx, bool create = false);

int rgw_policy_from_attrset(CephContext *cct, map<string, bufferlist>& attrset, RGWAccessControlPolicy *policy);

static inline bool rgw_raw_obj_to_obj(const rgw_bucket& bucket, const rgw_raw_obj& raw_obj, rgw_obj *obj)
{
  ssize_t pos = raw_obj.oid.find('_');
  if (pos < 0) {
    return false;
  }

  if (!rgw_obj_key::parse_raw_oid(raw_obj.oid.substr(pos + 1), &obj->key)) {
    return false;
  }
  obj->bucket = bucket;

  return true;
}

struct rgw_bucket_placement {
  string placement_rule;
  rgw_bucket bucket;

  void dump(Formatter *f) const;
};

class rgw_obj_select {
  string placement_rule;
  rgw_obj obj;
  rgw_raw_obj raw_obj;
  bool is_raw;

public:
  rgw_obj_select() : is_raw(false) {}
  rgw_obj_select(const rgw_obj& _obj) : obj(_obj), is_raw(false) {}
  rgw_obj_select(const rgw_raw_obj& _raw_obj) : raw_obj(_raw_obj), is_raw(true) {}
  rgw_obj_select(const rgw_obj_select& rhs) {
    placement_rule = rhs.placement_rule;
    is_raw = rhs.is_raw;
    if (is_raw) {
      raw_obj = rhs.raw_obj;
    } else {
      obj = rhs.obj;
    }
  }

  rgw_raw_obj get_raw_obj(const RGWZoneGroup& zonegroup, const RGWZoneParams& zone_params) const;
  rgw_raw_obj get_raw_obj(RGWRados *store) const;

  rgw_obj_select& operator=(const rgw_obj& rhs) {
    obj = rhs;
    is_raw = false;
    return *this;
  }

  rgw_obj_select& operator=(const rgw_raw_obj& rhs) {
    raw_obj = rhs;
    is_raw = true;
    return *this;
  }

  void set_placement_rule(const string& rule) {
    placement_rule = rule;
  }
};

struct compression_block {
  uint64_t old_ofs;
  uint64_t new_ofs;
  uint64_t len;

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    encode(old_ofs, bl);
    encode(new_ofs, bl);
    encode(len, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::iterator& bl) {
     DECODE_START(1, bl);
     decode(old_ofs, bl);
     decode(new_ofs, bl);
     decode(len, bl);
     DECODE_FINISH(bl);
  }
  void dump(Formatter *f) const;
};
WRITE_CLASS_ENCODER(compression_block)

struct RGWCompressionInfo {
  string compression_type;
  uint64_t orig_size;
  vector<compression_block> blocks;

  RGWCompressionInfo() : compression_type("none"), orig_size(0) {}
  RGWCompressionInfo(const RGWCompressionInfo& cs_info) : compression_type(cs_info.compression_type),
                                                          orig_size(cs_info.orig_size),
                                                          blocks(cs_info.blocks) {}

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    encode(compression_type, bl);
    encode(orig_size, bl);
    encode(blocks, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::iterator& bl) {
     DECODE_START(1, bl);
     decode(compression_type, bl);
     decode(orig_size, bl);
     decode(blocks, bl);
     DECODE_FINISH(bl);
  } 
  void dump(Formatter *f) const;
};
WRITE_CLASS_ENCODER(RGWCompressionInfo)

int rgw_compression_info_from_attrset(map<string, bufferlist>& attrs, bool& need_decompress, RGWCompressionInfo& cs_info);

struct RGWOLHInfo {
  rgw_obj target;
  bool removed;

  RGWOLHInfo() : removed(false) {}

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    encode(target, bl);
    encode(removed, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::iterator& bl) {
     DECODE_START(1, bl);
     decode(target, bl);
     decode(removed, bl);
     DECODE_FINISH(bl);
  }
  static void generate_test_instances(list<RGWOLHInfo*>& o);
  void dump(Formatter *f) const;
};
WRITE_CLASS_ENCODER(RGWOLHInfo)

struct RGWOLHPendingInfo {
  ceph::real_time time;

  RGWOLHPendingInfo() {}

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    encode(time, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::iterator& bl) {
     DECODE_START(1, bl);
     decode(time, bl);
     DECODE_FINISH(bl);
  }

  void dump(Formatter *f) const;
};
WRITE_CLASS_ENCODER(RGWOLHPendingInfo)

struct RGWUsageBatch {
  map<ceph::real_time, rgw_usage_log_entry> m;

  void insert(ceph::real_time& t, rgw_usage_log_entry& entry, bool *account) {
    bool exists = m.find(t) != m.end();
    *account = !exists;
    m[t].aggregate(entry);
  }
};

struct RGWUsageIter {
  string read_iter;
  uint32_t index;

  RGWUsageIter() : index(0) {}
};

class RGWGetDataCB {
protected:
  uint64_t extra_data_len;
public:
  virtual int handle_data(bufferlist& bl, off_t bl_ofs, off_t bl_len) = 0;
  RGWGetDataCB() : extra_data_len(0) {}
  virtual ~RGWGetDataCB() {}
  virtual void set_extra_data_len(uint64_t len) {
    extra_data_len = len;
  }
  /**
   * Flushes any cached data. Used by RGWGetObjFilter.
   * Return logic same as handle_data.
   */
  virtual int flush() {
    return 0;
  }
  /**
   * Allows to extend fetch range of RGW object. Used by RGWGetObjFilter.
   */
  virtual int fixup_range(off_t& bl_ofs, off_t& bl_end) {
    return 0;
  }
};

class RGWAccessListFilter {
public:
  virtual ~RGWAccessListFilter() {}
  virtual bool filter(string& name, string& key) = 0;
};

struct RGWCloneRangeInfo {
  rgw_obj src;
  off_t src_ofs;
  off_t dst_ofs;
  uint64_t len;
};

struct RGWObjManifestPart {
  rgw_obj loc;   /* the object where the data is located */
  uint64_t loc_ofs;  /* the offset at that object where the data is located */
  uint64_t size;     /* the part size */

  RGWObjManifestPart() : loc_ofs(0), size(0) {}

  void encode(bufferlist& bl) const {
    ENCODE_START(2, 2, bl);
    encode(loc, bl);
    encode(loc_ofs, bl);
    encode(size, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::iterator& bl) {
     DECODE_START_LEGACY_COMPAT_LEN_32(2, 2, 2, bl);
     decode(loc, bl);
     decode(loc_ofs, bl);
     decode(size, bl);
     DECODE_FINISH(bl);
  }

  void dump(Formatter *f) const;
  static void generate_test_instances(list<RGWObjManifestPart*>& o);
};
WRITE_CLASS_ENCODER(RGWObjManifestPart)

/*
 The manifest defines a set of rules for structuring the object parts.
 There are a few terms to note:
     - head: the head part of the object, which is the part that contains
       the first chunk of data. An object might not have a head (as in the
       case of multipart-part objects).
     - stripe: data portion of a single rgw object that resides on a single
       rados object.
     - part: a collection of stripes that make a contiguous part of an
       object. A regular object will only have one part (although might have
       many stripes), a multipart object might have many parts. Each part
       has a fixed stripe size, although the last stripe of a part might
       be smaller than that. Consecutive parts may be merged if their stripe
       value is the same.
*/

struct RGWObjManifestRule {
  uint32_t start_part_num;
  uint64_t start_ofs;
  uint64_t part_size; /* each part size, 0 if there's no part size, meaning it's unlimited */
  uint64_t stripe_max_size; /* underlying obj max size */
  string override_prefix;

  RGWObjManifestRule() : start_part_num(0), start_ofs(0), part_size(0), stripe_max_size(0) {}
  RGWObjManifestRule(uint32_t _start_part_num, uint64_t _start_ofs, uint64_t _part_size, uint64_t _stripe_max_size) :
                       start_part_num(_start_part_num), start_ofs(_start_ofs), part_size(_part_size), stripe_max_size(_stripe_max_size) {}

  void encode(bufferlist& bl) const {
    ENCODE_START(2, 1, bl);
    encode(start_part_num, bl);
    encode(start_ofs, bl);
    encode(part_size, bl);
    encode(stripe_max_size, bl);
    encode(override_prefix, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::iterator& bl) {
    DECODE_START(2, bl);
    decode(start_part_num, bl);
    decode(start_ofs, bl);
    decode(part_size, bl);
    decode(stripe_max_size, bl);
    if (struct_v >= 2)
      decode(override_prefix, bl);
    DECODE_FINISH(bl);
  }
  void dump(Formatter *f) const;
};
WRITE_CLASS_ENCODER(RGWObjManifestRule)

class RGWObjManifest {
protected:
  bool explicit_objs; /* old manifest? */
  map<uint64_t, RGWObjManifestPart> objs;

  uint64_t obj_size;

  rgw_obj obj;
  uint64_t head_size;
  string head_placement_rule;

  uint64_t max_head_size;
  string prefix;
  rgw_bucket_placement tail_placement; /* might be different than the original bucket,
                                       as object might have been copied across pools */
  map<uint64_t, RGWObjManifestRule> rules;

  string tail_instance; /* tail object's instance */

  void convert_to_explicit(const RGWZoneGroup& zonegroup, const RGWZoneParams& zone_params);
  int append_explicit(RGWObjManifest& m, const RGWZoneGroup& zonegroup, const RGWZoneParams& zone_params);
  void append_rules(RGWObjManifest& m, map<uint64_t, RGWObjManifestRule>::iterator& iter, string *override_prefix);

  void update_iterators() {
    begin_iter.seek(0);
    end_iter.seek(obj_size);
  }
public:

  RGWObjManifest() : explicit_objs(false), obj_size(0), head_size(0), max_head_size(0),
                     begin_iter(this), end_iter(this) {}
  RGWObjManifest(const RGWObjManifest& rhs) {
    *this = rhs;
  }
  RGWObjManifest& operator=(const RGWObjManifest& rhs) {
    explicit_objs = rhs.explicit_objs;
    objs = rhs.objs;
    obj_size = rhs.obj_size;
    obj = rhs.obj;
    head_size = rhs.head_size;
    max_head_size = rhs.max_head_size;
    prefix = rhs.prefix;
    tail_placement = rhs.tail_placement;
    rules = rhs.rules;
    tail_instance = rhs.tail_instance;

    begin_iter.set_manifest(this);
    end_iter.set_manifest(this);

    begin_iter.seek(rhs.begin_iter.get_ofs());
    end_iter.seek(rhs.end_iter.get_ofs());

    return *this;
  }

  map<uint64_t, RGWObjManifestPart>& get_explicit_objs() {
    return objs;
  }


  void set_explicit(uint64_t _size, map<uint64_t, RGWObjManifestPart>& _objs) {
    explicit_objs = true;
    obj_size = _size;
    objs.swap(_objs);
  }

  void get_implicit_location(uint64_t cur_part_id, uint64_t cur_stripe, uint64_t ofs, string *override_prefix, rgw_obj_select *location);

  void set_trivial_rule(uint64_t tail_ofs, uint64_t stripe_max_size) {
    RGWObjManifestRule rule(0, tail_ofs, 0, stripe_max_size);
    rules[0] = rule;
    max_head_size = tail_ofs;
  }

  void set_multipart_part_rule(uint64_t stripe_max_size, uint64_t part_num) {
    RGWObjManifestRule rule(0, 0, 0, stripe_max_size);
    rule.start_part_num = part_num;
    rules[0] = rule;
    max_head_size = 0;
  }

  void encode(bufferlist& bl) const {
    ENCODE_START(7, 6, bl);
    encode(obj_size, bl);
    encode(objs, bl);
    encode(explicit_objs, bl);
    encode(obj, bl);
    encode(head_size, bl);
    encode(max_head_size, bl);
    encode(prefix, bl);
    encode(rules, bl);
    bool encode_tail_bucket = !(tail_placement.bucket == obj.bucket);
    encode(encode_tail_bucket, bl);
    if (encode_tail_bucket) {
      encode(tail_placement.bucket, bl);
    }
    bool encode_tail_instance = (tail_instance != obj.key.instance);
    encode(encode_tail_instance, bl);
    if (encode_tail_instance) {
      encode(tail_instance, bl);
    }
    encode(head_placement_rule, bl);
    encode(tail_placement.placement_rule, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::iterator& bl) {
    DECODE_START_LEGACY_COMPAT_LEN_32(7, 2, 2, bl);
    decode(obj_size, bl);
    decode(objs, bl);
    if (struct_v >= 3) {
      decode(explicit_objs, bl);
      decode(obj, bl);
      decode(head_size, bl);
      decode(max_head_size, bl);
      decode(prefix, bl);
      decode(rules, bl);
    } else {
      explicit_objs = true;
      if (!objs.empty()) {
        map<uint64_t, RGWObjManifestPart>::iterator iter = objs.begin();
        obj = iter->second.loc;
        head_size = iter->second.size;
        max_head_size = head_size;
      }
    }

    if (explicit_objs && head_size > 0 && !objs.empty()) {
      /* patch up manifest due to issue 16435:
       * the first object in the explicit objs list might not be the one we need to access, use the
       * head object instead if set. This would happen if we had an old object that was created
       * when the explicit objs manifest was around, and it got copied.
       */
      rgw_obj& obj_0 = objs[0].loc;
      if (!obj_0.get_oid().empty() && obj_0.key.ns.empty()) {
        objs[0].loc = obj;
        objs[0].size = head_size;
      }
    }

    if (struct_v >= 4) {
      if (struct_v < 6) {
        decode(tail_placement.bucket, bl);
      } else {
        bool need_to_decode;
        decode(need_to_decode, bl);
        if (need_to_decode) {
          decode(tail_placement.bucket, bl);
        } else {
          tail_placement.bucket = obj.bucket;
        }
      }
    }

    if (struct_v >= 5) {
      if (struct_v < 6) {
        decode(tail_instance, bl);
      } else {
        bool need_to_decode;
        decode(need_to_decode, bl);
        if (need_to_decode) {
          decode(tail_instance, bl);
        } else {
          tail_instance = obj.key.instance;
        }
      }
    } else { // old object created before 'tail_instance' field added to manifest
      tail_instance = obj.key.instance;
    }

    if (struct_v >= 7) {
      decode(head_placement_rule, bl);
      decode(tail_placement.placement_rule, bl);
    }

    update_iterators();
    DECODE_FINISH(bl);
  }

  void dump(Formatter *f) const;
  static void generate_test_instances(list<RGWObjManifest*>& o);

  int append(RGWObjManifest& m, RGWZoneGroup& zonegroup, RGWZoneParams& zone_params);
  int append(RGWObjManifest& m, RGWRados *store);

  bool get_rule(uint64_t ofs, RGWObjManifestRule *rule);

  bool empty() {
    if (explicit_objs)
      return objs.empty();
    return rules.empty();
  }

  bool has_explicit_objs() {
    return explicit_objs;
  }

  bool has_tail() {
    if (explicit_objs) {
      if (objs.size() == 1) {
        map<uint64_t, RGWObjManifestPart>::iterator iter = objs.begin();
        rgw_obj& o = iter->second.loc;
        return !(obj == o);
      }
      return (objs.size() >= 2);
    }
    return (obj_size > head_size);
  }

  void set_head(const string& placement_rule, const rgw_obj& _o, uint64_t _s) {
    head_placement_rule = placement_rule;
    obj = _o;
    head_size = _s;

    if (explicit_objs && head_size > 0) {
      objs[0].loc = obj;
      objs[0].size = head_size;
    }
  }

  const rgw_obj& get_obj() {
    return obj;
  }

  void set_tail_placement(const string& placement_rule, const rgw_bucket& _b) {
    tail_placement.placement_rule = placement_rule;
    tail_placement.bucket = _b;
  }

  const rgw_bucket_placement& get_tail_placement() {
    return tail_placement;
  }

  const string& get_head_placement_rule() {
    return head_placement_rule;
  }

  void set_prefix(const string& _p) {
    prefix = _p;
  }

  const string& get_prefix() {
    return prefix;
  }

  void set_tail_instance(const string& _ti) {
    tail_instance = _ti;
  }

  const string& get_tail_instance() {
    return tail_instance;
  }

  void set_head_size(uint64_t _s) {
    head_size = _s;
  }

  void set_obj_size(uint64_t s) {
    obj_size = s;

    update_iterators();
  }

  uint64_t get_obj_size() {
    return obj_size;
  }

  uint64_t get_head_size() {
    return head_size;
  }

  void set_max_head_size(uint64_t s) {
    max_head_size = s;
  }

  uint64_t get_max_head_size() {
    return max_head_size;
  }

  class obj_iterator {
    RGWObjManifest *manifest;
    uint64_t part_ofs; /* where current part starts */
    uint64_t stripe_ofs; /* where current stripe starts */
    uint64_t ofs;       /* current position within the object */
    uint64_t stripe_size;      /* current part size */

    int cur_part_id;
    int cur_stripe;
    string cur_override_prefix;

    rgw_obj_select location;

    map<uint64_t, RGWObjManifestRule>::iterator rule_iter;
    map<uint64_t, RGWObjManifestRule>::iterator next_rule_iter;

    map<uint64_t, RGWObjManifestPart>::iterator explicit_iter;

    void init() {
      part_ofs = 0;
      stripe_ofs = 0;
      ofs = 0;
      stripe_size = 0;
      cur_part_id = 0;
      cur_stripe = 0;
    }

    void update_explicit_pos();


  protected:

    void set_manifest(RGWObjManifest *m) {
      manifest = m;
    }

  public:
    obj_iterator() : manifest(NULL) {
      init();
    }
    explicit obj_iterator(RGWObjManifest *_m) : manifest(_m) {
      init();
      if (!manifest->empty()) {
        seek(0);
      }
    }
    obj_iterator(RGWObjManifest *_m, uint64_t _ofs) : manifest(_m) {
      init();
      if (!manifest->empty()) {
        seek(_ofs);
      }
    }
    void seek(uint64_t ofs);

    void operator++();
    bool operator==(const obj_iterator& rhs) {
      return (ofs == rhs.ofs);
    }
    bool operator!=(const obj_iterator& rhs) {
      return (ofs != rhs.ofs);
    }
    const rgw_obj_select& get_location() {
      return location;
    }

    /* start of current stripe */
    uint64_t get_stripe_ofs() {
      if (manifest->explicit_objs) {
        return explicit_iter->first;
      }
      return stripe_ofs;
    }

    /* current ofs relative to start of rgw object */
    uint64_t get_ofs() const {
      return ofs;
    }

    /* stripe number */
    int get_cur_stripe() const {
      return cur_stripe;
    }

    /* current stripe size */
    uint64_t get_stripe_size() {
      if (manifest->explicit_objs) {
        return explicit_iter->second.size;
      }
      return stripe_size;
    }

    /* offset where data starts within current stripe */
    uint64_t location_ofs() {
      if (manifest->explicit_objs) {
        return explicit_iter->second.loc_ofs;
      }
      return 0; /* all stripes start at zero offset */
    }

    void update_location();

    friend class RGWObjManifest;
  };

  const obj_iterator& obj_begin();
  const obj_iterator& obj_end();
  obj_iterator obj_find(uint64_t ofs);

  obj_iterator begin_iter;
  obj_iterator end_iter;

  /*
   * simple object generator. Using a simple single rule manifest.
   */
  class generator {
    RGWObjManifest *manifest;
    uint64_t last_ofs;
    uint64_t cur_part_ofs;
    int cur_part_id;
    int cur_stripe;
    uint64_t cur_stripe_size;
    string cur_oid;
    
    string oid_prefix;

    rgw_obj_select cur_obj;

    RGWObjManifestRule rule;

  public:
    generator() : manifest(NULL), last_ofs(0), cur_part_ofs(0), cur_part_id(0), 
		  cur_stripe(0), cur_stripe_size(0) {}
    int create_begin(CephContext *cct, RGWObjManifest *manifest, const string& placement_rule, rgw_bucket& bucket, rgw_obj& obj);

    int create_next(uint64_t ofs);

    rgw_raw_obj get_cur_obj(RGWZoneGroup& zonegroup, RGWZoneParams& zone_params) { return cur_obj.get_raw_obj(zonegroup, zone_params); }
    rgw_raw_obj get_cur_obj(RGWRados *store) { return cur_obj.get_raw_obj(store); }

    /* total max size of current stripe (including head obj) */
    uint64_t cur_stripe_max_size() {
      return cur_stripe_size;
    }
  };
};
WRITE_CLASS_ENCODER(RGWObjManifest)

struct RGWUploadPartInfo {
  uint32_t num;
  uint64_t size;
  uint64_t accounted_size{0};
  string etag;
  ceph::real_time modified;
  RGWObjManifest manifest;
  RGWCompressionInfo cs_info;

  RGWUploadPartInfo() : num(0), size(0) {}

  void encode(bufferlist& bl) const {
    ENCODE_START(4, 2, bl);
    encode(num, bl);
    encode(size, bl);
    encode(etag, bl);
    encode(modified, bl);
    encode(manifest, bl);
    encode(cs_info, bl);
    encode(accounted_size, bl);
    ENCODE_FINISH(bl);
  }
  void decode(bufferlist::iterator& bl) {
    DECODE_START_LEGACY_COMPAT_LEN(4, 2, 2, bl);
    decode(num, bl);
    decode(size, bl);
    decode(etag, bl);
    decode(modified, bl);
    if (struct_v >= 3)
      decode(manifest, bl);
    if (struct_v >= 4) {
      decode(cs_info, bl);
      decode(accounted_size, bl);
    } else {
      accounted_size = size;
    }
    DECODE_FINISH(bl);
  }
  void dump(Formatter *f) const;
  static void generate_test_instances(list<RGWUploadPartInfo*>& o);
};
WRITE_CLASS_ENCODER(RGWUploadPartInfo)

struct RGWObjState {
  rgw_obj obj;
  bool is_atomic;
  bool has_attrs;
  bool exists;
  uint64_t size; //< size of raw object
  uint64_t accounted_size{0}; //< size before compression, encryption
  ceph::real_time mtime;
  uint64_t epoch;
  bufferlist obj_tag;
  bufferlist tail_tag;
  string write_tag;
  bool fake_tag;
  RGWObjManifest manifest;
  bool has_manifest;
  string shadow_obj;
  bool has_data;
  bufferlist data;
  bool prefetch_data;
  bool keep_tail;
  bool is_olh;
  bufferlist olh_tag;
  uint64_t pg_ver;
  uint32_t zone_short_id;

  /* important! don't forget to update copy constructor */

  RGWObjVersionTracker objv_tracker;

  map<string, bufferlist> attrset;
  RGWObjState() : is_atomic(false), has_attrs(0), exists(false),
                  size(0), epoch(0), fake_tag(false), has_manifest(false),
                  has_data(false), prefetch_data(false), keep_tail(false), is_olh(false),
                  pg_ver(0), zone_short_id(0) {}
  RGWObjState(const RGWObjState& rhs) : obj (rhs.obj) {
    is_atomic = rhs.is_atomic;
    has_attrs = rhs.has_attrs;
    exists = rhs.exists;
    size = rhs.size;
    accounted_size = rhs.accounted_size;
    mtime = rhs.mtime;
    epoch = rhs.epoch;
    if (rhs.obj_tag.length()) {
      obj_tag = rhs.obj_tag;
    }
    if (rhs.tail_tag.length()) {
      tail_tag = rhs.tail_tag;
    }
    write_tag = rhs.write_tag;
    fake_tag = rhs.fake_tag;
    if (rhs.has_manifest) {
      manifest = rhs.manifest;
    }
    has_manifest = rhs.has_manifest;
    shadow_obj = rhs.shadow_obj;
    has_data = rhs.has_data;
    if (rhs.data.length()) {
      data = rhs.data;
    }
    prefetch_data = rhs.prefetch_data;
    keep_tail = rhs.keep_tail;
    is_olh = rhs.is_olh;
    objv_tracker = rhs.objv_tracker;
    pg_ver = rhs.pg_ver;
  }

  bool get_attr(string name, bufferlist& dest) {
    map<string, bufferlist>::iterator iter = attrset.find(name);
    if (iter != attrset.end()) {
      dest = iter->second;
      return true;
    }
    return false;
  }
};

struct RGWRawObjState {
  rgw_raw_obj obj;
  bool has_attrs{false};
  bool exists{false};
  uint64_t size{0};
  ceph::real_time mtime;
  uint64_t epoch{0};
  bufferlist obj_tag;
  bool has_data{false};
  bufferlist data;
  bool prefetch_data{false};
  uint64_t pg_ver{0};

  /* important! don't forget to update copy constructor */

  RGWObjVersionTracker objv_tracker;

  map<string, bufferlist> attrset;
  RGWRawObjState() {}
  RGWRawObjState(const RGWRawObjState& rhs) : obj (rhs.obj) {
    has_attrs = rhs.has_attrs;
    exists = rhs.exists;
    size = rhs.size;
    mtime = rhs.mtime;
    epoch = rhs.epoch;
    if (rhs.obj_tag.length()) {
      obj_tag = rhs.obj_tag;
    }
    has_data = rhs.has_data;
    if (rhs.data.length()) {
      data = rhs.data;
    }
    prefetch_data = rhs.prefetch_data;
    pg_ver = rhs.pg_ver;
    objv_tracker = rhs.objv_tracker;
  }
};

struct RGWPoolIterCtx {
  librados::IoCtx io_ctx;
  librados::NObjectIterator iter;
};

struct RGWListRawObjsCtx {
  bool initialized;
  RGWPoolIterCtx iter_ctx;

  RGWListRawObjsCtx() : initialized(false) {}
};

struct RGWDefaultSystemMetaObjInfo {
  string default_id;

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    encode(default_id, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::iterator& bl) {
    DECODE_START(1, bl);
    decode(default_id, bl);
    DECODE_FINISH(bl);
  }

  void dump(Formatter *f) const;
  void decode_json(JSONObj *obj);
};
WRITE_CLASS_ENCODER(RGWDefaultSystemMetaObjInfo)

struct RGWNameToId {
  string obj_id;

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    encode(obj_id, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::iterator& bl) {
    DECODE_START(1, bl);
    decode(obj_id, bl);
    DECODE_FINISH(bl);
  }

  void dump(Formatter *f) const;
  void decode_json(JSONObj *obj);
};
WRITE_CLASS_ENCODER(RGWNameToId)

class RGWSystemMetaObj {
protected:
  string id;
  string name;

  CephContext *cct;
  RGWRados *store;

  int store_name(bool exclusive);
  int store_info(bool exclusive);
  int read_info(const string& obj_id, bool old_format = false);
  int read_id(const string& obj_name, string& obj_id);
  int read_default(RGWDefaultSystemMetaObjInfo& default_info,
		   const string& oid);
  /* read and use default id */
  int use_default(bool old_format = false);

public:
  RGWSystemMetaObj() : cct(NULL), store(NULL) {}
  RGWSystemMetaObj(const string& _name): name(_name), cct(NULL), store(NULL)  {}
  RGWSystemMetaObj(const string& _id, const string& _name) : id(_id), name(_name), cct(NULL), store(NULL) {}
  RGWSystemMetaObj(CephContext *_cct, RGWRados *_store): cct(_cct), store(_store){}
  RGWSystemMetaObj(const string& _name, CephContext *_cct, RGWRados *_store): name(_name), cct(_cct), store(_store){}
  const string& get_name() const { return name; }
  const string& get_id() const { return id; }

  void set_name(const string& _name) { name = _name;}
  void set_id(const string& _id) { id = _id;}
  void clear_id() { id.clear(); }

  virtual ~RGWSystemMetaObj() {}

  virtual void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    encode(id, bl);
    encode(name, bl);
    ENCODE_FINISH(bl);
  }

  virtual void decode(bufferlist::iterator& bl) {
    DECODE_START(1, bl);
    decode(id, bl);
    decode(name, bl);
    DECODE_FINISH(bl);
  }

  void reinit_instance(CephContext *_cct, RGWRados *_store) {
    cct = _cct;
    store = _store;
  }
  int init(CephContext *_cct, RGWRados *_store, bool setup_obj = true, bool old_format = false);
  virtual int read_default_id(string& default_id, bool old_format = false);
  virtual int set_as_default(bool exclusive = false);
  int delete_default();
  virtual int create(bool exclusive = true);
  int delete_obj(bool old_format = false);
  int rename(const string& new_name);
  int update() { return store_info(false);}
  int update_name() { return store_name(false);}
  int read();
  int write(bool exclusive);

  virtual rgw_pool get_pool(CephContext *cct) = 0;
  virtual const string get_default_oid(bool old_format = false) = 0;
  virtual const string& get_names_oid_prefix() = 0;
  virtual const string& get_info_oid_prefix(bool old_format = false) = 0;
  virtual const string& get_predefined_name(CephContext *cct) = 0;

  void dump(Formatter *f) const;
  void decode_json(JSONObj *obj);
};
WRITE_CLASS_ENCODER(RGWSystemMetaObj)

struct RGWZonePlacementInfo {
  rgw_pool index_pool;
  rgw_pool data_pool;
  rgw_pool data_extra_pool; /* if not set we should use data_pool */
  RGWBucketIndexType index_type;
  std::string compression_type;

  RGWZonePlacementInfo() : index_type(RGWBIType_Normal) {}

  void encode(bufferlist& bl) const {
    ENCODE_START(6, 1, bl);
    encode(index_pool.to_str(), bl);
    encode(data_pool.to_str(), bl);
    encode(data_extra_pool.to_str(), bl);
    encode((uint32_t)index_type, bl);
    encode(compression_type, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::iterator& bl) {
    DECODE_START(6, bl);
    string index_pool_str;
    string data_pool_str;
    decode(index_pool_str, bl);
    index_pool = rgw_pool(index_pool_str);
    decode(data_pool_str, bl);
    data_pool = rgw_pool(data_pool_str);
    if (struct_v >= 4) {
      string data_extra_pool_str;
      decode(data_extra_pool_str, bl);
      data_extra_pool = rgw_pool(data_extra_pool_str);
    }
    if (struct_v >= 5) {
      uint32_t it;
      decode(it, bl);
      index_type = (RGWBucketIndexType)it;
    }
    if (struct_v >= 6) {
      decode(compression_type, bl);
    }
    DECODE_FINISH(bl);
  }
  const rgw_pool& get_data_extra_pool() const {
    if (data_extra_pool.empty()) {
      return data_pool;
    }
    return data_extra_pool;
  }
  void dump(Formatter *f) const;
  void decode_json(JSONObj *obj);
};
WRITE_CLASS_ENCODER(RGWZonePlacementInfo)

struct RGWZoneParams : RGWSystemMetaObj {
  rgw_pool domain_root;
  rgw_pool metadata_heap;
  rgw_pool control_pool;
  rgw_pool gc_pool;
  rgw_pool lc_pool;
  rgw_pool log_pool;
  rgw_pool intent_log_pool;
  rgw_pool usage_log_pool;

  rgw_pool user_keys_pool;
  rgw_pool user_email_pool;
  rgw_pool user_swift_pool;
  rgw_pool user_uid_pool;
  rgw_pool roles_pool;
  rgw_pool reshard_pool;

  RGWAccessKey system_key;

  map<string, RGWZonePlacementInfo> placement_pools;

  string realm_id;

  map<string, string, ltstr_nocase> tier_config;

  RGWZoneParams() : RGWSystemMetaObj() {}
  RGWZoneParams(const string& name) : RGWSystemMetaObj(name){}
  RGWZoneParams(const string& id, const string& name) : RGWSystemMetaObj(id, name) {}
  RGWZoneParams(const string& id, const string& name, const string& _realm_id)
    : RGWSystemMetaObj(id, name), realm_id(_realm_id) {}

  rgw_pool get_pool(CephContext *cct);
  const string get_default_oid(bool old_format = false) override;
  const string& get_names_oid_prefix() override;
  const string& get_info_oid_prefix(bool old_format = false) override;
  const string& get_predefined_name(CephContext *cct) override;

  int init(CephContext *_cct, RGWRados *_store, bool setup_obj = true,
	   bool old_format = false);
  using RGWSystemMetaObj::init;
  int read_default_id(string& default_id, bool old_format = false) override;
  int set_as_default(bool exclusive = false) override;
  int create_default(bool old_format = false);
  int create(bool exclusive = true) override;
  int fix_pool_names();

  const string& get_compression_type(const string& placement_rule) const;
  
  void encode(bufferlist& bl) const override {
    ENCODE_START(10, 1, bl);
    encode(domain_root, bl);
    encode(control_pool, bl);
    encode(gc_pool, bl);
    encode(log_pool, bl);
    encode(intent_log_pool, bl);
    encode(usage_log_pool, bl);
    encode(user_keys_pool, bl);
    encode(user_email_pool, bl);
    encode(user_swift_pool, bl);
    encode(user_uid_pool, bl);
    RGWSystemMetaObj::encode(bl);
    encode(system_key, bl);
    encode(placement_pools, bl);
    encode(metadata_heap, bl);
    encode(realm_id, bl);
    encode(lc_pool, bl);
    encode(tier_config, bl);
    encode(roles_pool, bl);
    encode(reshard_pool, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::iterator& bl) override {
    DECODE_START(10, bl);
    decode(domain_root, bl);
    decode(control_pool, bl);
    decode(gc_pool, bl);
    decode(log_pool, bl);
    decode(intent_log_pool, bl);
    decode(usage_log_pool, bl);
    decode(user_keys_pool, bl);
    decode(user_email_pool, bl);
    decode(user_swift_pool, bl);
    decode(user_uid_pool, bl);
    if (struct_v >= 6) {
      RGWSystemMetaObj::decode(bl);
    } else if (struct_v >= 2) {
      decode(name, bl);
      id = name;
    }
    if (struct_v >= 3)
      decode(system_key, bl);
    if (struct_v >= 4)
      decode(placement_pools, bl);
    if (struct_v >= 5)
      decode(metadata_heap, bl);
    if (struct_v >= 6) {
      decode(realm_id, bl);
    }
    if (struct_v >= 7) {
      decode(lc_pool, bl);
    } else {
      lc_pool = log_pool.name + ":lc";
    }
    if (struct_v >= 8) {
      decode(tier_config, bl);
    }
    if (struct_v >= 9) {
      decode(roles_pool, bl);
    } else {
      roles_pool = name + ".rgw.meta:roles";
    }
    if (struct_v >= 10) {
      decode(reshard_pool, bl);
    } else {
      reshard_pool = log_pool.name + ":reshard";
    }
    DECODE_FINISH(bl);
  }
  void dump(Formatter *f) const;
  void decode_json(JSONObj *obj);
  static void generate_test_instances(list<RGWZoneParams*>& o);

  bool get_placement(const string& placement_id, RGWZonePlacementInfo *placement) const {
    auto iter = placement_pools.find(placement_id);
    if (iter == placement_pools.end()) {
      return false;
    }
    *placement = iter->second;
    return true;
  }

  /*
   * return data pool of the head object
   */
  bool get_head_data_pool(const string& placement_id, const rgw_obj& obj, rgw_pool *pool) const {
    const rgw_data_placement_target& explicit_placement = obj.bucket.explicit_placement;
    if (!explicit_placement.data_pool.empty()) {
      if (!obj.in_extra_data) {
        *pool = explicit_placement.data_pool;
      } else {
        *pool = explicit_placement.get_data_extra_pool();
      }
      return true;
    }
    if (placement_id.empty()) {
      return false;
    }
    auto iter = placement_pools.find(placement_id);
    if (iter == placement_pools.end()) {
      return false;
    }
    if (!obj.in_extra_data) {
      *pool = iter->second.data_pool;
    } else {
      *pool = iter->second.get_data_extra_pool();
    }
    return true;
  }
};
WRITE_CLASS_ENCODER(RGWZoneParams)

struct RGWZone {
  string id;
  string name;
  list<string> endpoints;
  bool log_meta;
  bool log_data;
  bool read_only;
  string tier_type;

  string redirect_zone;

/**
 * Represents the number of shards for the bucket index object, a value of zero
 * indicates there is no sharding. By default (no sharding, the name of the object
 * is '.dir.{marker}', with sharding, the name is '.dir.{marker}.{sharding_id}',
 * sharding_id is zero-based value. It is not recommended to set a too large value
 * (e.g. thousand) as it increases the cost for bucket listing.
 */
  uint32_t bucket_index_max_shards;

  bool sync_from_all;
  set<string> sync_from; /* list of zones to sync from */

  RGWZone() : log_meta(false), log_data(false), read_only(false), bucket_index_max_shards(0),
              sync_from_all(true) {}

  void encode(bufferlist& bl) const {
    ENCODE_START(7, 1, bl);
    encode(name, bl);
    encode(endpoints, bl);
    encode(log_meta, bl);
    encode(log_data, bl);
    encode(bucket_index_max_shards, bl);
    encode(id, bl);
    encode(read_only, bl);
    encode(tier_type, bl);
    encode(sync_from_all, bl);
    encode(sync_from, bl);
    encode(redirect_zone, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::iterator& bl) {
    DECODE_START(7, bl);
    decode(name, bl);
    if (struct_v < 4) {
      id = name;
    }
    decode(endpoints, bl);
    if (struct_v >= 2) {
      decode(log_meta, bl);
      decode(log_data, bl);
    }
    if (struct_v >= 3) {
      decode(bucket_index_max_shards, bl);
    }
    if (struct_v >= 4) {
      decode(id, bl);
      decode(read_only, bl);
    }
    if (struct_v >= 5) {
      decode(tier_type, bl);
    }
    if (struct_v >= 6) {
      decode(sync_from_all, bl);
      decode(sync_from, bl);
    }
    if (struct_v >= 7) {
      decode(redirect_zone, bl);
    }
    DECODE_FINISH(bl);
  }
  void dump(Formatter *f) const;
  void decode_json(JSONObj *obj);
  static void generate_test_instances(list<RGWZone*>& o);

  bool is_read_only() { return read_only; }

  bool syncs_from(const string& zone_id) {
    return (sync_from_all || sync_from.find(zone_id) != sync_from.end());
  }
};
WRITE_CLASS_ENCODER(RGWZone)

struct RGWDefaultZoneGroupInfo {
  string default_zonegroup;

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    encode(default_zonegroup, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::iterator& bl) {
    DECODE_START(1, bl);
    decode(default_zonegroup, bl);
    DECODE_FINISH(bl);
  }
  void dump(Formatter *f) const;
  void decode_json(JSONObj *obj);
  //todo: implement ceph-dencoder
};
WRITE_CLASS_ENCODER(RGWDefaultZoneGroupInfo)

struct RGWZoneGroupPlacementTarget {
  string name;
  set<string> tags;

  bool user_permitted(list<string>& user_tags) const {
    if (tags.empty()) {
      return true;
    }
    for (auto& rule : user_tags) {
      if (tags.find(rule) != tags.end()) {
        return true;
      }
    }
    return false;
  }

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    encode(name, bl);
    encode(tags, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::iterator& bl) {
    DECODE_START(1, bl);
    decode(name, bl);
    decode(tags, bl);
    DECODE_FINISH(bl);
  }
  void dump(Formatter *f) const;
  void decode_json(JSONObj *obj);
};
WRITE_CLASS_ENCODER(RGWZoneGroupPlacementTarget)


struct RGWZoneGroup : public RGWSystemMetaObj {
  string api_name;
  list<string> endpoints;
  bool is_master = false;

  string master_zone;
  map<string, RGWZone> zones;

  map<string, RGWZoneGroupPlacementTarget> placement_targets;
  string default_placement;

  list<string> hostnames;
  list<string> hostnames_s3website;
  // TODO: Maybe convert hostnames to a map<string,list<string>> for
  // endpoint_type->hostnames
/*
20:05 < _robbat21irssi> maybe I do someting like: if (hostname_map.empty()) { populate all map keys from hostnames; };
20:05 < _robbat21irssi> but that's a later compatability migration planning bit
20:06 < yehudasa> more like if (!hostnames.empty()) {
20:06 < yehudasa> for (list<string>::iterator iter = hostnames.begin(); iter != hostnames.end(); ++iter) {
20:06 < yehudasa> hostname_map["s3"].append(iter->second);
20:07 < yehudasa> hostname_map["s3website"].append(iter->second);
20:07 < yehudasa> s/append/push_back/g
20:08 < _robbat21irssi> inner loop over APIs
20:08 < yehudasa> yeah, probably
20:08 < _robbat21irssi> s3, s3website, swift, swith_auth, swift_website
*/
  map<string, list<string> > api_hostname_map;
  map<string, list<string> > api_endpoints_map;

  string realm_id;

  RGWZoneGroup(): is_master(false){}
  RGWZoneGroup(const std::string &id, const std::string &name):RGWSystemMetaObj(id, name) {}
  RGWZoneGroup(const std::string &_name):RGWSystemMetaObj(_name) {}
  RGWZoneGroup(const std::string &_name, bool _is_master, CephContext *cct, RGWRados* store,
	       const string& _realm_id, const list<string>& _endpoints)
    : RGWSystemMetaObj(_name, cct , store), endpoints(_endpoints), is_master(_is_master),
      realm_id(_realm_id) {}

  bool is_master_zonegroup() const { return is_master;}
  void update_master(bool _is_master) {
    is_master = _is_master;
    post_process_params();
  }
  void post_process_params();

  void encode(bufferlist& bl) const override {
    ENCODE_START(4, 1, bl);
    encode(name, bl);
    encode(api_name, bl);
    encode(is_master, bl);
    encode(endpoints, bl);
    encode(master_zone, bl);
    encode(zones, bl);
    encode(placement_targets, bl);
    encode(default_placement, bl);
    encode(hostnames, bl);
    encode(hostnames_s3website, bl);
    RGWSystemMetaObj::encode(bl);
    encode(realm_id, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::iterator& bl) override {
    DECODE_START(4, bl);
    decode(name, bl);
    decode(api_name, bl);
    decode(is_master, bl);
    decode(endpoints, bl);
    decode(master_zone, bl);
    decode(zones, bl);
    decode(placement_targets, bl);
    decode(default_placement, bl);
    if (struct_v >= 2) {
      decode(hostnames, bl);
    }
    if (struct_v >= 3) {
      decode(hostnames_s3website, bl);
    }
    if (struct_v >= 4) {
      RGWSystemMetaObj::decode(bl);
      decode(realm_id, bl);
    } else {
      id = name;
    }
    DECODE_FINISH(bl);
  }

  int read_default_id(string& default_id, bool old_format = false) override;
  int set_as_default(bool exclusive = false) override;
  int create_default(bool old_format = false);
  int equals(const string& other_zonegroup) const;
  int add_zone(const RGWZoneParams& zone_params, bool *is_master, bool *read_only,
               const list<string>& endpoints, const string *ptier_type,
               bool *psync_from_all, list<string>& sync_from, list<string>& sync_from_rm,
               string *predirect_zone);
  int remove_zone(const std::string& zone_id);
  int rename_zone(const RGWZoneParams& zone_params);
  rgw_pool get_pool(CephContext *cct);
  const string get_default_oid(bool old_region_format = false) override;
  const string& get_info_oid_prefix(bool old_region_format = false) override;
  const string& get_names_oid_prefix() override;
  const string& get_predefined_name(CephContext *cct) override;

  void dump(Formatter *f) const;
  void decode_json(JSONObj *obj);
  static void generate_test_instances(list<RGWZoneGroup*>& o);
};
WRITE_CLASS_ENCODER(RGWZoneGroup)

struct RGWPeriodMap
{
  string id;
  map<string, RGWZoneGroup> zonegroups;
  map<string, RGWZoneGroup> zonegroups_by_api;
  map<string, uint32_t> short_zone_ids;

  string master_zonegroup;

  void encode(bufferlist& bl) const;
  void decode(bufferlist::iterator& bl);

  int update(const RGWZoneGroup& zonegroup, CephContext *cct);

  void dump(Formatter *f) const;
  void decode_json(JSONObj *obj);

  void reset() {
    zonegroups.clear();
    zonegroups_by_api.clear();
    master_zonegroup.clear();
  }

  uint32_t get_zone_short_id(const string& zone_id) const;
};
WRITE_CLASS_ENCODER(RGWPeriodMap)

struct RGWPeriodConfig
{
  RGWQuotaInfo bucket_quota;
  RGWQuotaInfo user_quota;

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    encode(bucket_quota, bl);
    encode(user_quota, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::iterator& bl) {
    DECODE_START(1, bl);
    decode(bucket_quota, bl);
    decode(user_quota, bl);
    DECODE_FINISH(bl);
  }

  void dump(Formatter *f) const;
  void decode_json(JSONObj *obj);

  // the period config must be stored in a local object outside of the period,
  // so that it can be used in a default configuration where no realm/period
  // exists
  int read(RGWRados *store, const std::string& realm_id);
  int write(RGWRados *store, const std::string& realm_id);

  static std::string get_oid(const std::string& realm_id);
  static rgw_pool get_pool(CephContext *cct);
};
WRITE_CLASS_ENCODER(RGWPeriodConfig)

/* for backward comaptability */
struct RGWRegionMap {

  map<string, RGWZoneGroup> regions;

  string master_region;

  RGWQuotaInfo bucket_quota;
  RGWQuotaInfo user_quota;

  void encode(bufferlist& bl) const;
  void decode(bufferlist::iterator& bl);

  void dump(Formatter *f) const;
  void decode_json(JSONObj *obj);
};
WRITE_CLASS_ENCODER(RGWRegionMap)

struct RGWZoneGroupMap {

  map<string, RGWZoneGroup> zonegroups;
  map<string, RGWZoneGroup> zonegroups_by_api;

  string master_zonegroup;

  RGWQuotaInfo bucket_quota;
  RGWQuotaInfo user_quota;

  /* constract the map */
  int read(CephContext *cct, RGWRados *store);

  void encode(bufferlist& bl) const;
  void decode(bufferlist::iterator& bl);

  void dump(Formatter *f) const;
  void decode_json(JSONObj *obj);
};
WRITE_CLASS_ENCODER(RGWZoneGroupMap)

class RGWRealm;

struct objexp_hint_entry {
  string tenant;
  string bucket_name;
  string bucket_id;
  rgw_obj_key obj_key;
  ceph::real_time exp_time;

  void encode(bufferlist& bl) const {
    ENCODE_START(2, 1, bl);
    encode(bucket_name, bl);
    encode(bucket_id, bl);
    encode(obj_key, bl);
    encode(exp_time, bl);
    encode(tenant, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::iterator& bl) {
    // XXX Do we want DECODE_START_LEGACY_COMPAT_LEN(2, 1, 1, bl); ?
    DECODE_START(2, bl);
    decode(bucket_name, bl);
    decode(bucket_id, bl);
    decode(obj_key, bl);
    decode(exp_time, bl);
    if (struct_v >= 2) {
      decode(tenant, bl);
    } else {
      tenant.clear();
    }
    DECODE_FINISH(bl);
  }
};
WRITE_CLASS_ENCODER(objexp_hint_entry)

class RGWPeriod;

class RGWRealm : public RGWSystemMetaObj
{
  string current_period;
  epoch_t epoch{0}; //< realm epoch, incremented for each new period

  int create_control(bool exclusive);
  int delete_control();
public:
  RGWRealm() {}
  RGWRealm(const string& _id, const string& _name = "") : RGWSystemMetaObj(_id, _name) {}
  RGWRealm(CephContext *_cct, RGWRados *_store): RGWSystemMetaObj(_cct, _store) {}
  RGWRealm(const string& _name, CephContext *_cct, RGWRados *_store): RGWSystemMetaObj(_name, _cct, _store){}

  void encode(bufferlist& bl) const override {
    ENCODE_START(1, 1, bl);
    RGWSystemMetaObj::encode(bl);
    encode(current_period, bl);
    encode(epoch, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::iterator& bl) override {
    DECODE_START(1, bl);
    RGWSystemMetaObj::decode(bl);
    decode(current_period, bl);
    decode(epoch, bl);
    DECODE_FINISH(bl);
  }

  int create(bool exclusive = true) override;
  int delete_obj();
  rgw_pool get_pool(CephContext *cct);
  const string get_default_oid(bool old_format = false) override;
  const string& get_names_oid_prefix() override;
  const string& get_info_oid_prefix(bool old_format = false) override;
  const string& get_predefined_name(CephContext *cct) override;

  using RGWSystemMetaObj::read_id; // expose as public for radosgw-admin

  void dump(Formatter *f) const;
  void decode_json(JSONObj *obj);

  const string& get_current_period() const {
    return current_period;
  }
  int set_current_period(RGWPeriod& period);
  void clear_current_period_and_epoch() {
    current_period.clear();
    epoch = 0;
  }
  epoch_t get_epoch() const { return epoch; }

  string get_control_oid();
  /// send a notify on the realm control object
  int notify_zone(bufferlist& bl);
  /// notify the zone of a new period
  int notify_new_period(const RGWPeriod& period);
};
WRITE_CLASS_ENCODER(RGWRealm)

struct RGWPeriodLatestEpochInfo {
  epoch_t epoch;

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    encode(epoch, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::iterator& bl) {
    DECODE_START(1, bl);
    decode(epoch, bl);
    DECODE_FINISH(bl);
  }

  void dump(Formatter *f) const;
  void decode_json(JSONObj *obj);
};
WRITE_CLASS_ENCODER(RGWPeriodLatestEpochInfo)

class RGWPeriod
{
  string id;
  epoch_t epoch;
  string predecessor_uuid;
  std::vector<std::string> sync_status;
  RGWPeriodMap period_map;
  RGWPeriodConfig period_config;
  string master_zonegroup;
  string master_zone;

  string realm_id;
  string realm_name;
  epoch_t realm_epoch{1}; //< realm epoch when period was made current

  CephContext *cct;
  RGWRados *store;

  int read_info();
  int read_latest_epoch(RGWPeriodLatestEpochInfo& epoch_info,
                        RGWObjVersionTracker *objv = nullptr);
  int use_latest_epoch();
  int use_current_period();

  const string get_period_oid();
  const string get_period_oid_prefix();

  // gather the metadata sync status for each shard; only for use on master zone
  int update_sync_status(const RGWPeriod &current_period,
                         std::ostream& error_stream, bool force_if_stale);

public:
  RGWPeriod() : epoch(0), cct(NULL), store(NULL) {}

  RGWPeriod(const string& period_id, epoch_t _epoch = 0)
    : id(period_id), epoch(_epoch),
      cct(NULL), store(NULL) {}

  const string& get_id() const { return id; }
  epoch_t get_epoch() const { return epoch; }
  epoch_t get_realm_epoch() const { return realm_epoch; }
  const string& get_predecessor() const { return predecessor_uuid; }
  const string& get_master_zone() const { return master_zone; }
  const string& get_master_zonegroup() const { return master_zonegroup; }
  const string& get_realm() const { return realm_id; }
  const RGWPeriodMap& get_map() const { return period_map; }
  RGWPeriodConfig& get_config() { return period_config; }
  const RGWPeriodConfig& get_config() const { return period_config; }
  const std::vector<std::string>& get_sync_status() const { return sync_status; }
  rgw_pool get_pool(CephContext *cct);
  const string& get_latest_epoch_oid();
  const string& get_info_oid_prefix();

  void set_user_quota(RGWQuotaInfo& user_quota) {
    period_config.user_quota = user_quota;
  }

  void set_bucket_quota(RGWQuotaInfo& bucket_quota) {
    period_config.bucket_quota = bucket_quota;
  }

  void set_id(const string& id) {
    this->id = id;
    period_map.id = id;
  }
  void set_epoch(epoch_t epoch) { this->epoch = epoch; }
  void set_realm_epoch(epoch_t epoch) { realm_epoch = epoch; }

  void set_predecessor(const string& predecessor)
  {
    predecessor_uuid = predecessor;
  }

  void set_realm_id(const string& _realm_id) {
    realm_id = _realm_id;
  }

  int reflect();

  int get_zonegroup(RGWZoneGroup& zonegroup,
		    const string& zonegroup_id);

  bool is_single_zonegroup() const
  {
      return (period_map.zonegroups.size() == 1);
  }

  /*
    returns true if there are several zone groups with a least one zone
   */
  bool is_multi_zonegroups_with_zones()
  {
    int count = 0;
    for (const auto& zg:  period_map.zonegroups) {
      if (zg.second.zones.size() > 0) {
	if (count++ > 0) {
	  return true;
	}
      }
    }
    return false;
  }

  int get_latest_epoch(epoch_t& epoch);
  int set_latest_epoch(epoch_t epoch, bool exclusive = false,
                       RGWObjVersionTracker *objv = nullptr);
  // update latest_epoch if the given epoch is higher, else return -EEXIST
  int update_latest_epoch(epoch_t epoch);

  int init(CephContext *_cct, RGWRados *_store, const string &period_realm_id, const string &period_realm_name = "",
	   bool setup_obj = true);
  int init(CephContext *_cct, RGWRados *_store, bool setup_obj = true);  

  int create(bool exclusive = true);
  int delete_obj();
  int store_info(bool exclusive);
  int add_zonegroup(const RGWZoneGroup& zonegroup);

  void fork();
  int update();

  // commit a staging period; only for use on master zone
  int commit(RGWRealm& realm, const RGWPeriod &current_period,
             std::ostream& error_stream, bool force_if_stale = false);

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    encode(id, bl);
    encode(epoch, bl);
    encode(realm_epoch, bl);
    encode(predecessor_uuid, bl);
    encode(sync_status, bl);
    encode(period_map, bl);
    encode(master_zone, bl);
    encode(master_zonegroup, bl);
    encode(period_config, bl);
    encode(realm_id, bl);
    encode(realm_name, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::iterator& bl) {
    DECODE_START(1, bl);
    decode(id, bl);
    decode(epoch, bl);
    decode(realm_epoch, bl);
    decode(predecessor_uuid, bl);
    decode(sync_status, bl);
    decode(period_map, bl);
    decode(master_zone, bl);
    decode(master_zonegroup, bl);
    decode(period_config, bl);
    decode(realm_id, bl);
    decode(realm_name, bl);
    DECODE_FINISH(bl);
  }
  void dump(Formatter *f) const;
  void decode_json(JSONObj *obj);

  static string get_staging_id(const string& realm_id) {
    return realm_id + ":staging";
  }
};
WRITE_CLASS_ENCODER(RGWPeriod)

class RGWDataChangesLog;
class RGWMetaSyncStatusManager;
class RGWDataSyncStatusManager;
class RGWReplicaLogger;
class RGWCoroutinesManagerRegistry;
  
class RGWStateLog {
  RGWRados *store;
  int num_shards;
  string module_name;

  void oid_str(int shard, string& oid);
  int get_shard_num(const string& object);
  string get_oid(const string& object);
  int open_ioctx(librados::IoCtx& ioctx);

  struct list_state {
    int cur_shard;
    int max_shard;
    string marker;
    string client_id;
    string op_id;
    string object;

    list_state() : cur_shard(0), max_shard(0) {}
  };

protected:
  virtual bool dump_entry_internal(const cls_statelog_entry& entry, Formatter *f) {
    return false;
  }

public:
  RGWStateLog(RGWRados *_store, int _num_shards, const string& _module_name) :
              store(_store), num_shards(_num_shards), module_name(_module_name) {}
  virtual ~RGWStateLog() {}

  int store_entry(const string& client_id, const string& op_id, const string& object,
                  uint32_t state, bufferlist *bl, uint32_t *check_state);

  int remove_entry(const string& client_id, const string& op_id, const string& object);

  void init_list_entries(const string& client_id, const string& op_id, const string& object,
                         void **handle);

  int list_entries(void *handle, int max_entries, list<cls_statelog_entry>& entries, bool *done);

  void finish_list_entries(void *handle);

  virtual void dump_entry(const cls_statelog_entry& entry, Formatter *f);
};

/*
 * state transitions:
 *
 * unknown -> in-progress -> complete
 *                        -> error
 *
 * user can try setting the 'abort' state, and it can only succeed if state is
 * in-progress.
 *
 * state renewal cannot switch state (stays in the same state)
 *
 * rgw can switch from in-progress to complete
 * rgw can switch from in-progress to error
 *
 * rgw can switch from abort to cancelled
 *
 */

class RGWOpState : public RGWStateLog {
protected:
  bool dump_entry_internal(const cls_statelog_entry& entry, Formatter *f) override;
public:

  enum OpState {
    OPSTATE_UNKNOWN     = 0,
    OPSTATE_IN_PROGRESS = 1,
    OPSTATE_COMPLETE    = 2,
    OPSTATE_ERROR       = 3,
    OPSTATE_ABORT       = 4,
    OPSTATE_CANCELLED   = 5,
  };

  explicit RGWOpState(RGWRados *_store);

  int state_from_str(const string& s, OpState *state);
  int set_state(const string& client_id, const string& op_id, const string& object, OpState state);
  int renew_state(const string& client_id, const string& op_id, const string& object, OpState state);
};

class RGWOpStateSingleOp
{
  RGWOpState os;
  string client_id;
  string op_id;
  string object;

  CephContext *cct;

  RGWOpState::OpState cur_state;
  ceph::real_time last_update;

public:
  RGWOpStateSingleOp(RGWRados *store, const string& cid, const string& oid, const string& obj);

  int set_state(RGWOpState::OpState state);
  int renew_state();
};

class RGWGetBucketStats_CB : public RefCountedObject {
protected:
  rgw_bucket bucket;
  map<RGWObjCategory, RGWStorageStats> *stats;
public:
  explicit RGWGetBucketStats_CB(const rgw_bucket& _bucket) : bucket(_bucket), stats(NULL) {}
  ~RGWGetBucketStats_CB() override {}
  virtual void handle_response(int r) = 0;
  virtual void set_response(map<RGWObjCategory, RGWStorageStats> *_stats) {
    stats = _stats;
  }
};

class RGWGetUserStats_CB : public RefCountedObject {
protected:
  rgw_user user;
  RGWStorageStats stats;
public:
  explicit RGWGetUserStats_CB(const rgw_user& _user) : user(_user) {}
  ~RGWGetUserStats_CB() override {}
  virtual void handle_response(int r) = 0;
  virtual void set_response(RGWStorageStats& _stats) {
    stats = _stats;
  }
};

class RGWGetDirHeader_CB;
class RGWGetUserHeader_CB;

struct rgw_rados_ref {
  rgw_pool pool;
  string oid;
  string key;
  librados::IoCtx ioctx;
};

class RGWChainedCache {
public:
  virtual ~RGWChainedCache() {}
  virtual void chain_cb(const string& key, void *data) = 0;
  virtual void invalidate(const string& key) = 0;
  virtual void invalidate_all() = 0;

  struct Entry {
    RGWChainedCache *cache;
    const string& key;
    void *data;

    Entry(RGWChainedCache *_c, const string& _k, void *_d) : cache(_c), key(_k), data(_d) {}
  };
};

template <class T, class S>
class RGWObjectCtxImpl {
  RGWRados *store;
  std::map<T, S> objs_state;
  RWLock lock;

public:
  RGWObjectCtxImpl(RGWRados *_store) : store(_store), lock("RGWObjectCtxImpl") {}

  S *get_state(const T& obj) {
    S *result;
    typename std::map<T, S>::iterator iter;
    lock.get_read();
    assert (!obj.empty());
    iter = objs_state.find(obj);
    if (iter != objs_state.end()) {
      result = &iter->second;
      lock.unlock();
    } else {
      lock.unlock();
      lock.get_write();
      result = &objs_state[obj];
      lock.unlock();
    }
    return result;
  }

  void set_atomic(T& obj) {
    RWLock::WLocker wl(lock);
    assert (!obj.empty());
    objs_state[obj].is_atomic = true;
  }
  void set_prefetch_data(T& obj) {
    RWLock::WLocker wl(lock);
    assert (!obj.empty());
    objs_state[obj].prefetch_data = true;
  }
  void invalidate(T& obj) {
    RWLock::WLocker wl(lock);
    auto iter = objs_state.find(obj);
    if (iter == objs_state.end()) {
      return;
    }
    bool is_atomic = iter->second.is_atomic;
    bool prefetch_data = iter->second.prefetch_data;
  
    objs_state.erase(iter);

    if (is_atomic || prefetch_data) {
      auto& s = objs_state[obj];
      s.is_atomic = is_atomic;
      s.prefetch_data = prefetch_data;
    }
  }
};

template<>
void RGWObjectCtxImpl<rgw_obj, RGWObjState>::invalidate(rgw_obj& obj);

template<>
void RGWObjectCtxImpl<rgw_raw_obj, RGWRawObjState>::invalidate(rgw_raw_obj& obj);

struct RGWObjectCtx {
  RGWRados *store;
  void *user_ctx;

  RGWObjectCtxImpl<rgw_obj, RGWObjState> obj;
  RGWObjectCtxImpl<rgw_raw_obj, RGWRawObjState> raw;

  explicit RGWObjectCtx(RGWRados *_store) : store(_store), user_ctx(NULL), obj(store), raw(store) { }
  RGWObjectCtx(RGWRados *_store, void *_user_ctx) : store(_store), user_ctx(_user_ctx), obj(store), raw(store) { }
};

class Finisher;
class RGWAsyncRadosProcessor;

template <class T>
class RGWChainedCacheImpl;

struct bucket_info_entry {
  RGWBucketInfo info;
  real_time mtime;
  map<string, bufferlist> attrs;
};

struct tombstone_entry {
  ceph::real_time mtime;
  uint32_t zone_short_id;
  uint64_t pg_ver;

  tombstone_entry() = default;
  tombstone_entry(const RGWObjState& state)
    : mtime(state.mtime), zone_short_id(state.zone_short_id),
      pg_ver(state.pg_ver) {}
};

class RGWIndexCompletionManager;

class RGWRados : public AdminSocketHook
{
  friend class RGWGC;
  friend class RGWMetaNotifier;
  friend class RGWDataNotifier;
  friend class RGWLC;
  friend class RGWObjectExpirer;
  friend class RGWMetaSyncProcessorThread;
  friend class RGWDataSyncProcessorThread;
  friend class RGWStateLog;
  friend class RGWReplicaLogger;
  friend class RGWReshard;
  friend class RGWBucketReshard;
  friend class BucketIndexLockGuard;
  friend class RGWCompleteMultipart;

  static constexpr const char* admin_commands[4][3] = {
    { "cache list",
      "cache list name=filter,type=CephString,req=false",
      "cache list [filter_str]: list object cache, possibly matching substrings" },
    { "cache inspect",
      "cache inspect name=target,type=CephString,req=true",
      "cache inspect target: print cache element" },
    { "cache erase",
      "cache erase name=target,type=CephString,req=true",
      "cache erase target: erase element from cache" },
    { "cache zap",
      "cache zap",
      "cache zap: erase all elements from cache" }
  };

  /** Open the pool used as root for this gateway */
  int open_root_pool_ctx();
  int open_gc_pool_ctx();
  int open_lc_pool_ctx();
  int open_objexp_pool_ctx();
  int open_reshard_pool_ctx();

  int open_pool_ctx(const rgw_pool& pool, librados::IoCtx&  io_ctx);
  int open_bucket_index_ctx(const RGWBucketInfo& bucket_info, librados::IoCtx& index_ctx);
  int open_bucket_index(const RGWBucketInfo& bucket_info, librados::IoCtx&  index_ctx, string& bucket_oid);
  int open_bucket_index_base(const RGWBucketInfo& bucket_info, librados::IoCtx&  index_ctx,
      string& bucket_oid_base);
  int open_bucket_index_shard(const RGWBucketInfo& bucket_info, librados::IoCtx& index_ctx,
      const string& obj_key, string *bucket_obj, int *shard_id);
  int open_bucket_index_shard(const RGWBucketInfo& bucket_info, librados::IoCtx& index_ctx,
                              int shard_id, string *bucket_obj);
  int open_bucket_index(const RGWBucketInfo& bucket_info, librados::IoCtx& index_ctx,
      map<int, string>& bucket_objs, int shard_id = -1, map<int, string> *bucket_instance_ids = NULL);
  template<typename T>
  int open_bucket_index(const RGWBucketInfo& bucket_info, librados::IoCtx& index_ctx,
                        map<int, string>& oids, map<int, T>& bucket_objs,
                        int shard_id = -1, map<int, string> *bucket_instance_ids = NULL);
  void build_bucket_index_marker(const string& shard_id_str, const string& shard_marker,
      string *marker);

  void get_bucket_instance_ids(const RGWBucketInfo& bucket_info, int shard_id, map<int, string> *result);

  std::atomic<int64_t> max_req_id = { 0 };
  Mutex lock;
  Mutex watchers_lock;
  SafeTimer *timer;

  RGWGC *gc;
  RGWLC *lc;
  RGWObjectExpirer *obj_expirer;
  bool use_gc_thread;
  bool use_lc_thread;
  bool quota_threads;
  bool run_sync_thread;
  bool run_reshard_thread;

  RGWAsyncRadosProcessor* async_rados;

  RGWMetaNotifier *meta_notifier;
  RGWDataNotifier *data_notifier;
  RGWMetaSyncProcessorThread *meta_sync_processor_thread;
  RGWSyncTraceManager *sync_tracer = nullptr;
  map<string, RGWDataSyncProcessorThread *> data_sync_processor_threads;

  boost::optional<rgw::BucketTrimManager> bucket_trim;
  RGWSyncLogTrimThread *sync_log_trimmer{nullptr};

  Mutex meta_sync_thread_lock;
  Mutex data_sync_thread_lock;

  int num_watchers;
  RGWWatcher **watchers;
  std::set<int> watchers_set;
  librados::IoCtx root_pool_ctx;      // .rgw
  librados::IoCtx control_pool_ctx;   // .rgw.control
  bool watch_initialized;

  friend class RGWWatcher;

  Mutex bucket_id_lock;

  // This field represents the number of bucket index object shards
  uint32_t bucket_index_max_shards;

  int get_obj_head_ioctx(const RGWBucketInfo& bucket_info, const rgw_obj& obj, librados::IoCtx *ioctx);
  int get_obj_head_ref(const RGWBucketInfo& bucket_info, const rgw_obj& obj, rgw_rados_ref *ref);
  int get_system_obj_ref(const rgw_raw_obj& obj, rgw_rados_ref *ref);
  uint64_t max_bucket_id;

  int get_olh_target_state(RGWObjectCtx& rctx, const RGWBucketInfo& bucket_info, const rgw_obj& obj,
                           RGWObjState *olh_state, RGWObjState **target_state);
  int get_system_obj_state_impl(RGWObjectCtx *rctx, rgw_raw_obj& obj, RGWRawObjState **state, RGWObjVersionTracker *objv_tracker);
  int get_obj_state_impl(RGWObjectCtx *rctx, const RGWBucketInfo& bucket_info, const rgw_obj& obj, RGWObjState **state,
                         bool follow_olh, bool assume_noent = false);
  int append_atomic_test(RGWObjectCtx *rctx, const RGWBucketInfo& bucket_info, const rgw_obj& obj,
                         librados::ObjectOperation& op, RGWObjState **state);

  int update_placement_map();
  int store_bucket_info(RGWBucketInfo& info, map<string, bufferlist> *pattrs, RGWObjVersionTracker *objv_tracker, bool exclusive);

  void remove_rgw_head_obj(librados::ObjectWriteOperation& op);
  void cls_obj_check_prefix_exist(librados::ObjectOperation& op, const string& prefix, bool fail_if_exist);
  void cls_obj_check_mtime(librados::ObjectOperation& op, const real_time& mtime, bool high_precision_time, RGWCheckMTimeType type);
protected:
  CephContext *cct;

  std::vector<librados::Rados> rados;
  uint32_t next_rados_handle;
  RWLock handle_lock;
  std::map<pthread_t, int> rados_map;

  using RGWChainedCacheImpl_bucket_info_entry = RGWChainedCacheImpl<bucket_info_entry>;
  RGWChainedCacheImpl_bucket_info_entry *binfo_cache;

  using tombstone_cache_t = lru_map<rgw_obj, tombstone_entry>;
  tombstone_cache_t *obj_tombstone_cache;

  librados::IoCtx gc_pool_ctx;        // .rgw.gc
  librados::IoCtx lc_pool_ctx;        // .rgw.lc
  librados::IoCtx objexp_pool_ctx;
  librados::IoCtx reshard_pool_ctx;

  bool pools_initialized;

  string trans_id_suffix;

  RGWQuotaHandler *quota_handler;

  Finisher *finisher;

  RGWCoroutinesManagerRegistry *cr_registry;

  RGWSyncModulesManager *sync_modules_manager{nullptr};
  RGWSyncModuleInstanceRef sync_module;
  bool writeable_zone{false};

  RGWZoneGroup zonegroup;
  RGWZone zone_public_config; /* external zone params, e.g., entrypoints, log flags, etc. */  
  RGWZoneParams zone_params; /* internal zone params, e.g., rados pools */
  uint32_t zone_short_id;

  RGWPeriod current_period;

  RGWIndexCompletionManager *index_completion_manager{nullptr};
public:
  RGWRados() : lock("rados_timer_lock"), watchers_lock("watchers_lock"), timer(NULL),
               gc(NULL), lc(NULL), obj_expirer(NULL), use_gc_thread(false), use_lc_thread(false), quota_threads(false),
               run_sync_thread(false), run_reshard_thread(false), async_rados(nullptr), meta_notifier(NULL),
               data_notifier(NULL), meta_sync_processor_thread(NULL),
               meta_sync_thread_lock("meta_sync_thread_lock"), data_sync_thread_lock("data_sync_thread_lock"),
               num_watchers(0), watchers(NULL),
               watch_initialized(false),
               bucket_id_lock("rados_bucket_id"),
               bucket_index_max_shards(0),
               max_bucket_id(0), cct(NULL),
               next_rados_handle(0),
               handle_lock("rados_handle_lock"),
               binfo_cache(NULL), obj_tombstone_cache(nullptr),
               pools_initialized(false),
               quota_handler(NULL),
               finisher(NULL),
               cr_registry(NULL),
               zone_short_id(0),
               rest_master_conn(NULL),
               meta_mgr(NULL), data_log(NULL), reshard(NULL) {}

  uint64_t get_new_req_id() {
    return ++max_req_id;
  }

  librados::IoCtx* get_lc_pool_ctx() {
    return &lc_pool_ctx;
  }
  void set_context(CephContext *_cct) {
    cct = _cct;
  }

  /**
   * AmazonS3 errors contain a HostId string, but is an opaque base64 blob; we
   * try to be more transparent. This has a wrapper so we can update it when zonegroup/zone are changed.
   */
  void init_host_id() {
    /* uint64_t needs 16, two '-' separators and a trailing null */
    const string& zone_name = get_zone().name;
    const string& zonegroup_name = zonegroup.get_name();
    char charbuf[16 + zone_name.size() + zonegroup_name.size() + 2 + 1];
    snprintf(charbuf, sizeof(charbuf), "%llx-%s-%s", (unsigned long long)instance_id(), zone_name.c_str(), zonegroup_name.c_str());
    string s(charbuf);
    host_id = s;
  }

  string host_id;

  RGWRealm realm;

  RGWRESTConn *rest_master_conn;
  map<string, RGWRESTConn *> zone_conn_map;
  map<string, RGWRESTConn *> zone_data_sync_from_map;
  map<string, RGWRESTConn *> zone_data_notify_to_map;
  map<string, RGWRESTConn *> zonegroup_conn_map;

  map<string, string> zone_id_by_name;
  map<string, RGWZone> zone_by_id;

  RGWRESTConn *get_zone_conn_by_id(const string& id) {
    auto citer = zone_conn_map.find(id);
    if (citer == zone_conn_map.end()) {
      return NULL;
    }

    return citer->second;
  }

  RGWRESTConn *get_zone_conn_by_name(const string& name) {
    auto i = zone_id_by_name.find(name);
    if (i == zone_id_by_name.end()) {
      return NULL;
    }

    return get_zone_conn_by_id(i->second);
  }

  bool find_zone_id_by_name(const string& name, string *id) {
    auto i = zone_id_by_name.find(name);
    if (i == zone_id_by_name.end()) {
      return false;
    }
    *id = i->second; 
    return true;
  }

  int get_zonegroup(const string& id, RGWZoneGroup& zonegroup) {
    int ret = 0;
    if (id == get_zonegroup().get_id()) {
      zonegroup = get_zonegroup();
    } else if (!current_period.get_id().empty()) {
      ret = current_period.get_zonegroup(zonegroup, id);
    }
    return ret;
  }

  RGWRealm& get_realm() {
    return realm;
  }

  RGWZoneParams& get_zone_params() { return zone_params; }
  RGWZoneGroup& get_zonegroup() {
    return zonegroup;
  }
  RGWZone& get_zone() {
    return zone_public_config;
  }

  bool zone_is_writeable() {
    return writeable_zone && !get_zone().is_read_only();
  }

  uint32_t get_zone_short_id() const {
    return zone_short_id;
  }

  bool zone_syncs_from(RGWZone& target_zone, RGWZone& source_zone);

  bool get_redirect_zone_endpoint(string *endpoint);

  const RGWQuotaInfo& get_bucket_quota() {
    return current_period.get_config().bucket_quota;
  }

  const RGWQuotaInfo& get_user_quota() {
    return current_period.get_config().user_quota;
  }

  const string& get_current_period_id() {
    return current_period.get_id();
  }

  bool has_zonegroup_api(const std::string& api) const {
    if (!current_period.get_id().empty()) {
      const auto& zonegroups_by_api = current_period.get_map().zonegroups_by_api;
      if (zonegroups_by_api.find(api) != zonegroups_by_api.end())
        return true;
    }
    return false;
  }

  // pulls missing periods for period_history
  std::unique_ptr<RGWPeriodPuller> period_puller;
  // maintains a connected history of periods
  std::unique_ptr<RGWPeriodHistory> period_history;

  RGWAsyncRadosProcessor* get_async_rados() const { return async_rados; };

  RGWMetadataManager *meta_mgr;

  RGWDataChangesLog *data_log;

  RGWReshard *reshard;
  std::shared_ptr<RGWReshardWait> reshard_wait;

  virtual ~RGWRados() = default;

  tombstone_cache_t *get_tombstone_cache() {
    return obj_tombstone_cache;
  }

  RGWSyncModulesManager *get_sync_modules_manager() {
    return sync_modules_manager;
  }
  const RGWSyncModuleInstanceRef& get_sync_module() {
    return sync_module;
  }
  RGWSyncTraceManager *get_sync_tracer() {
    return sync_tracer;
  }

  int get_required_alignment(const rgw_pool& pool, uint64_t *alignment);
  int get_max_chunk_size(const rgw_pool& pool, uint64_t *max_chunk_size);
  int get_max_chunk_size(const string& placement_rule, const rgw_obj& obj, uint64_t *max_chunk_size);

  uint32_t get_max_bucket_shards() {
    return rgw_shards_max();
  }


  int get_raw_obj_ref(const rgw_raw_obj& obj, rgw_rados_ref *ref);

  int list_raw_objects_init(const rgw_pool& pool, const string& marker, RGWListRawObjsCtx *ctx);
  int list_raw_objects_next(const string& prefix_filter, int max,
                            RGWListRawObjsCtx& ctx, list<string>& oids,
                            bool *is_truncated);
  int list_raw_objects(const rgw_pool& pool, const string& prefix_filter, int max,
                       RGWListRawObjsCtx& ctx, list<string>& oids,
                       bool *is_truncated);
  string list_raw_objs_get_cursor(RGWListRawObjsCtx& ctx);

  int list_raw_prefixed_objs(const rgw_pool& pool, const string& prefix, list<string>& result);
  int list_zonegroups(list<string>& zonegroups);
  int list_regions(list<string>& regions);
  int list_zones(list<string>& zones);
  int list_realms(list<string>& realms);
  int list_periods(list<string>& periods);
  int list_periods(const string& current_period, list<string>& periods);
  void tick();

  CephContext *ctx() { return cct; }
  /** do all necessary setup of the storage device */
  int initialize(CephContext *_cct, bool _use_gc_thread, bool _use_lc_thread, bool _quota_threads, bool _run_sync_thread, bool _run_reshard_thread) {
    set_context(_cct);
    use_gc_thread = _use_gc_thread;
    use_lc_thread = _use_lc_thread;
    quota_threads = _quota_threads;
    run_sync_thread = _run_sync_thread;
    run_reshard_thread = _run_reshard_thread;
    return initialize();
  }
  /** Initialize the RADOS instance and prepare to do other ops */
  virtual int init_rados();
  int init_zg_from_period(bool *initialized);
  int init_zg_from_local(bool *creating_defaults);
  int init_complete();
  int replace_region_with_zonegroup();
  int convert_regionmap();
  int initialize();
  void finalize();

  int register_to_service_map(const string& daemon_type, const map<string, string>& meta);
  int update_service_map(std::map<std::string, std::string>&& status);

  void schedule_context(Context *c);

  /** set up a bucket listing. handle is filled in. */
  int list_buckets_init(RGWAccessHandle *handle);
  /** 
   * get the next bucket in the listing. obj is filled in,
   * handle is updated.
   */
  int list_buckets_next(rgw_bucket_dir_entry& obj, RGWAccessHandle *handle);

  /// list logs
  int log_list_init(const string& prefix, RGWAccessHandle *handle);
  int log_list_next(RGWAccessHandle handle, string *name);

  /// remove log
  int log_remove(const string& name);

  /// show log
  int log_show_init(const string& name, RGWAccessHandle *handle);
  int log_show_next(RGWAccessHandle handle, rgw_log_entry *entry);

  // log bandwidth info
  int log_usage(map<rgw_user_bucket, RGWUsageBatch>& usage_info);
  int read_usage(const rgw_user& user, uint64_t start_epoch, uint64_t end_epoch, uint32_t max_entries,
                 bool *is_truncated, RGWUsageIter& read_iter, map<rgw_user_bucket, rgw_usage_log_entry>& usage);
  int trim_usage(rgw_user& user, uint64_t start_epoch, uint64_t end_epoch);
  int clear_usage();

  int create_pool(const rgw_pool& pool);

  int init_bucket_index(RGWBucketInfo& bucket_info, int num_shards);
  int select_bucket_placement(RGWUserInfo& user_info, const string& zonegroup_id, const string& rule,
                              string *pselected_rule_name, RGWZonePlacementInfo *rule_info);
  int select_legacy_bucket_placement(RGWZonePlacementInfo *rule_info);
  int select_new_bucket_location(RGWUserInfo& user_info, const string& zonegroup_id, const string& rule,
                                 string *pselected_rule_name, RGWZonePlacementInfo *rule_info);
  int select_bucket_location_by_rule(const string& location_rule, RGWZonePlacementInfo *rule_info);
  void create_bucket_id(string *bucket_id);

  bool get_obj_data_pool(const string& placement_rule, const rgw_obj& obj, rgw_pool *pool);
  bool obj_to_raw(const string& placement_rule, const rgw_obj& obj, rgw_raw_obj *raw_obj);

  int create_bucket(RGWUserInfo& owner, rgw_bucket& bucket,
                            const string& zonegroup_id,
                            const string& placement_rule,
                            const string& swift_ver_location,
                            const RGWQuotaInfo * pquota_info,
                            map<std::string,bufferlist>& attrs,
                            RGWBucketInfo& bucket_info,
                            obj_version *pobjv,
                            obj_version *pep_objv,
                            ceph::real_time creation_time,
                            rgw_bucket *master_bucket,
                            uint32_t *master_num_shards,
                            bool exclusive = true);
  int add_bucket_placement(const rgw_pool& new_pool);
  int remove_bucket_placement(const rgw_pool& new_pool);
  int list_placement_set(set<rgw_pool>& names);
  int create_pools(vector<rgw_pool>& pools, vector<int>& retcodes);

  RGWCoroutinesManagerRegistry *get_cr_registry() { return cr_registry; }

  class SystemObject {
    RGWRados *store;
    RGWObjectCtx& ctx;
    rgw_raw_obj obj;

  public:

    SystemObject(RGWRados *_store, RGWObjectCtx& _ctx, rgw_raw_obj& _obj) :
      store(_store), ctx(_ctx), obj(_obj)
    {}

    void invalidate_state();

    RGWRados *get_store() { return store; }
    rgw_raw_obj& get_obj() { return obj; }
    RGWObjectCtx& get_ctx() { return ctx; }

    struct Read {
      RGWRados::SystemObject *source;

      struct GetObjState {
        rgw_rados_ref ref;
        bool has_ref{false};
        uint64_t last_ver{0};

        GetObjState() {}

        int get_ref(RGWRados *store, rgw_raw_obj& obj, rgw_rados_ref **pref);
      } state;
      
      struct StatParams {
        ceph::real_time *lastmod;
        uint64_t *obj_size;
        map<string, bufferlist> *attrs;

        StatParams() : lastmod(NULL), obj_size(NULL), attrs(NULL) {}
      } stat_params;

      struct ReadParams {
        rgw_cache_entry_info *cache_info{nullptr};
        map<string, bufferlist> *attrs;

        ReadParams() : attrs(NULL) {}
      } read_params;

      explicit Read(RGWRados::SystemObject *_source) : source(_source) {}

      int stat(RGWObjVersionTracker *objv_tracker);
      int read(int64_t ofs, int64_t end, bufferlist& bl, RGWObjVersionTracker *objv_tracker,
	       boost::optional<obj_version> refresh_version = boost::none);
      int get_attr(const char *name, bufferlist& dest);
    };
  };

  struct BucketShard {
    RGWRados *store;
    rgw_bucket bucket;
    int shard_id;
    librados::IoCtx index_ctx;
    string bucket_obj;

    explicit BucketShard(RGWRados *_store) : store(_store), shard_id(-1) {}
    int init(const rgw_bucket& _bucket, const rgw_obj& obj);
    int init(const rgw_bucket& _bucket, int sid);
    int init(const RGWBucketInfo& bucket_info, int sid);
  };

  class Object {
    RGWRados *store;
    RGWBucketInfo bucket_info;
    RGWObjectCtx& ctx;
    rgw_obj obj;

    BucketShard bs;

    RGWObjState *state;

    bool versioning_disabled;

    bool bs_initialized;

  protected:
    int get_state(RGWObjState **pstate, bool follow_olh, bool assume_noent = false);
    void invalidate_state();

    int prepare_atomic_modification(librados::ObjectWriteOperation& op, bool reset_obj, const string *ptag,
                                    const char *ifmatch, const char *ifnomatch, bool removal_op, bool modify_tail);
    int complete_atomic_modification();

  public:
    Object(RGWRados *_store, const RGWBucketInfo& _bucket_info, RGWObjectCtx& _ctx, const rgw_obj& _obj) : store(_store), bucket_info(_bucket_info),
                                                                                               ctx(_ctx), obj(_obj), bs(store),
                                                                                               state(NULL), versioning_disabled(false),
                                                                                               bs_initialized(false) {}

    RGWRados *get_store() { return store; }
    rgw_obj& get_obj() { return obj; }
    RGWObjectCtx& get_ctx() { return ctx; }
    RGWBucketInfo& get_bucket_info() { return bucket_info; }
    int get_manifest(RGWObjManifest **pmanifest);

    int get_bucket_shard(BucketShard **pbs) {
      if (!bs_initialized) {
        int r = bs.init(bucket_info.bucket, obj);
        if (r < 0) {
          return r;
        }
        bs_initialized = true;
      }
      *pbs = &bs;
      return 0;
    }

    void set_versioning_disabled(bool status) {
      versioning_disabled = status;
    }

    bool versioning_enabled() {
      return (!versioning_disabled && bucket_info.versioning_enabled());
    }

    struct Read {
      RGWRados::Object *source;

      struct GetObjState {
        librados::IoCtx io_ctx;
        rgw_obj obj;
        rgw_raw_obj head_obj;
      } state;
      
      struct ConditionParams {
        const ceph::real_time *mod_ptr;
        const ceph::real_time *unmod_ptr;
        bool high_precision_time;
        uint32_t mod_zone_id;
        uint64_t mod_pg_ver;
        const char *if_match;
        const char *if_nomatch;
        
        ConditionParams() : 
                 mod_ptr(NULL), unmod_ptr(NULL), high_precision_time(false), mod_zone_id(0), mod_pg_ver(0),
                 if_match(NULL), if_nomatch(NULL) {}
      } conds;

      struct Params {
        ceph::real_time *lastmod;
        uint64_t *obj_size;
        map<string, bufferlist> *attrs;

        Params() : lastmod(NULL), obj_size(NULL), attrs(NULL) {}
      } params;

      explicit Read(RGWRados::Object *_source) : source(_source) {}

      int prepare();
      static int range_to_ofs(uint64_t obj_size, int64_t &ofs, int64_t &end);
      int read(int64_t ofs, int64_t end, bufferlist& bl);
      int iterate(int64_t ofs, int64_t end, RGWGetDataCB *cb);
      int get_attr(const char *name, bufferlist& dest);
    };

    struct Write {
      RGWRados::Object *target;
      
      struct MetaParams {
        ceph::real_time *mtime;
        map<std::string, bufferlist>* rmattrs;
        const bufferlist *data;
        RGWObjManifest *manifest;
        const string *ptag;
        list<rgw_obj_index_key> *remove_objs;
        ceph::real_time set_mtime;
        rgw_user owner;
        RGWObjCategory category;
        int flags;
        const char *if_match;
        const char *if_nomatch;
        uint64_t olh_epoch;
        ceph::real_time delete_at;
        bool canceled;
        const string *user_data;
        rgw_zone_set *zones_trace;
        bool modify_tail;
        bool completeMultipart;

        MetaParams() : mtime(NULL), rmattrs(NULL), data(NULL), manifest(NULL), ptag(NULL),
                 remove_objs(NULL), category(RGW_OBJ_CATEGORY_MAIN), flags(0),
                 if_match(NULL), if_nomatch(NULL), olh_epoch(0), canceled(false), user_data(nullptr), zones_trace(nullptr),
                 modify_tail(false),  completeMultipart(false) {}
      } meta;

      explicit Write(RGWRados::Object *_target) : target(_target) {}

      int _do_write_meta(uint64_t size, uint64_t accounted_size,
                     map<std::string, bufferlist>& attrs,
                     bool modify_tail, bool assume_noent,
                     void *index_op);
      int write_meta(uint64_t size, uint64_t accounted_size,
                     map<std::string, bufferlist>& attrs);
      int write_data(const char *data, uint64_t ofs, uint64_t len, bool exclusive);
    };

    struct Delete {
      RGWRados::Object *target;

      struct DeleteParams {
        rgw_user bucket_owner;
        int versioning_status;
        ACLOwner obj_owner; /* needed for creation of deletion marker */
        uint64_t olh_epoch;
        string marker_version_id;
        uint32_t bilog_flags;
        list<rgw_obj_index_key> *remove_objs;
        ceph::real_time expiration_time;
        ceph::real_time unmod_since;
        ceph::real_time mtime; /* for setting delete marker mtime */
        bool high_precision_time;
        rgw_zone_set *zones_trace;

        DeleteParams() : versioning_status(0), olh_epoch(0), bilog_flags(0), remove_objs(NULL), high_precision_time(false), zones_trace(nullptr) {}
      } params;

      struct DeleteResult {
        bool delete_marker;
        string version_id;

        DeleteResult() : delete_marker(false) {}
      } result;
      
      explicit Delete(RGWRados::Object *_target) : target(_target) {}

      int delete_obj();
    };

    struct Stat {
      RGWRados::Object *source;

      struct Result {
        rgw_obj obj;
        RGWObjManifest manifest;
        bool has_manifest;
        uint64_t size;
	struct timespec mtime;
        map<string, bufferlist> attrs;

        Result() : has_manifest(false), size(0) {}
      } result;

      struct State {
        librados::IoCtx io_ctx;
        librados::AioCompletion *completion;
        int ret;

        State() : completion(NULL), ret(0) {}
      } state;


      explicit Stat(RGWRados::Object *_source) : source(_source) {}

      int stat_async();
      int wait();
      int stat();
    private:
      int finish();
    };
  };

  class Bucket {
    RGWRados *store;
    RGWBucketInfo bucket_info;
    rgw_bucket& bucket;
    int shard_id;

  public:
    Bucket(RGWRados *_store, const RGWBucketInfo& _bucket_info) : store(_store), bucket_info(_bucket_info), bucket(bucket_info.bucket),
                                                            shard_id(RGW_NO_SHARD) {}
    RGWRados *get_store() { return store; }
    rgw_bucket& get_bucket() { return bucket; }
    RGWBucketInfo& get_bucket_info() { return bucket_info; }

    int update_bucket_id(const string& new_bucket_id);

    int get_shard_id() { return shard_id; }
    void set_shard_id(int id) {
      shard_id = id;
    }

    class UpdateIndex {
      RGWRados::Bucket *target;
      string optag;
      rgw_obj obj;
      uint16_t bilog_flags{0};
      BucketShard bs;
      bool bs_initialized{false};
      bool blind;
      bool prepared{false};
      rgw_zone_set *zones_trace{nullptr};

      int init_bs() {
        int r = bs.init(target->get_bucket(), obj);
        if (r < 0) {
          return r;
        }
        bs_initialized = true;
        return 0;
      }

      void invalidate_bs() {
        bs_initialized = false;
      }

      int guard_reshard(BucketShard **pbs, std::function<int(BucketShard *)> call);
    public:

      UpdateIndex(RGWRados::Bucket *_target, const rgw_obj& _obj) : target(_target), obj(_obj),
                                                              bs(target->get_store()) {
                                                                blind = (target->get_bucket_info().index_type == RGWBIType_Indexless);
                                                              }

      int get_bucket_shard(BucketShard **pbs) {
        if (!bs_initialized) {
          int r = init_bs();
          if (r < 0) {
            return r;
          }
        }
        *pbs = &bs;
        return 0;
      }

      void set_bilog_flags(uint16_t flags) {
        bilog_flags = flags;
      }
      
      void set_zones_trace(rgw_zone_set *_zones_trace) {
        zones_trace = _zones_trace;
      }

      int prepare(RGWModifyOp, const string *write_tag);
      int complete(int64_t poolid, uint64_t epoch, uint64_t size,
                   uint64_t accounted_size, ceph::real_time& ut,
                   const string& etag, const string& content_type,
                   bufferlist *acl_bl, RGWObjCategory category,
		   list<rgw_obj_index_key> *remove_objs, const string *user_data = nullptr);
      int complete_del(int64_t poolid, uint64_t epoch,
                       ceph::real_time& removed_mtime, /* mtime of removed object */
                       list<rgw_obj_index_key> *remove_objs);
      int cancel();

      const string *get_optag() { return &optag; }

      bool is_prepared() { return prepared; }
    };

    struct List {
      RGWRados::Bucket *target;
      rgw_obj_key next_marker;

      struct Params {
        string prefix;
        string delim;
        rgw_obj_key marker;
        rgw_obj_key end_marker;
        string ns;
        bool enforce_ns;
        RGWAccessListFilter *filter;
        bool list_versions;

        Params() : enforce_ns(true), filter(NULL), list_versions(false) {}
      } params;

    public:
      explicit List(RGWRados::Bucket *_target) : target(_target) {}

      int list_objects(int64_t max, vector<rgw_bucket_dir_entry> *result, map<string, bool> *common_prefixes, bool *is_truncated);
      rgw_obj_key& get_next_marker() {
        return next_marker;
      }
    };
  };

  /** Write/overwrite an object to the bucket storage. */
  virtual int put_system_obj_impl(rgw_raw_obj& obj, uint64_t size, ceph::real_time *mtime,
              map<std::string, bufferlist>& attrs, int flags,
              const bufferlist& data,
              RGWObjVersionTracker *objv_tracker,
              ceph::real_time set_mtime /* 0 for don't set */);

  virtual int put_system_obj_data(void *ctx, rgw_raw_obj& obj,
              const bufferlist& bl, off_t ofs, bool exclusive,
              RGWObjVersionTracker *objv_tracker = nullptr);
  int aio_put_obj_data(void *ctx, rgw_raw_obj& obj, bufferlist& bl,
                        off_t ofs, bool exclusive, void **handle);

  int put_system_obj(void *ctx, rgw_raw_obj& obj, const bufferlist& data, bool exclusive,
              ceph::real_time *mtime, map<std::string, bufferlist>& attrs, RGWObjVersionTracker *objv_tracker,
              ceph::real_time set_mtime) {
    int flags = PUT_OBJ_CREATE;
    if (exclusive)
      flags |= PUT_OBJ_EXCL;

    return put_system_obj_impl(obj, data.length(), mtime, attrs, flags, data, objv_tracker, set_mtime);
  }
  int aio_wait(void *handle);
  bool aio_completed(void *handle);

  int on_last_entry_in_listing(RGWBucketInfo& bucket_info,
                               const std::string& obj_prefix,
                               const std::string& obj_delim,
                               std::function<int(const rgw_bucket_dir_entry&)> handler);

  bool swift_versioning_enabled(const RGWBucketInfo& bucket_info) const {
    return bucket_info.has_swift_versioning() &&
        bucket_info.swift_ver_location.size();
  }

  int swift_versioning_copy(RGWObjectCtx& obj_ctx,              /* in/out */
                            const rgw_user& user,               /* in */
                            RGWBucketInfo& bucket_info,         /* in */
                            rgw_obj& obj);                      /* in */
  int swift_versioning_restore(RGWObjectCtx& obj_ctx,           /* in/out */
                               const rgw_user& user,            /* in */
                               RGWBucketInfo& bucket_info,      /* in */
                               rgw_obj& obj,                    /* in */
                               bool& restored);                 /* out */
  int copy_obj_to_remote_dest(RGWObjState *astate,
                              map<string, bufferlist>& src_attrs,
                              RGWRados::Object::Read& read_op,
                              const rgw_user& user_id,
                              rgw_obj& dest_obj,
                              ceph::real_time *mtime);

  enum AttrsMod {
    ATTRSMOD_NONE    = 0,
    ATTRSMOD_REPLACE = 1,
    ATTRSMOD_MERGE   = 2
  };

  int rewrite_obj(RGWBucketInfo& dest_bucket_info, rgw_obj& obj);

  int stat_remote_obj(RGWObjectCtx& obj_ctx,
               const rgw_user& user_id,
               const string& client_id,
               req_info *info,
               const string& source_zone,
               rgw_obj& src_obj,
               RGWBucketInfo& src_bucket_info,
               real_time *src_mtime,
               uint64_t *psize,
               const real_time *mod_ptr,
               const real_time *unmod_ptr,
               bool high_precision_time,
               const char *if_match,
               const char *if_nomatch,
               map<string, bufferlist> *pattrs,
               string *version_id,
               string *ptag,
               string *petag);

  int fetch_remote_obj(RGWObjectCtx& obj_ctx,
                       const rgw_user& user_id,
                       const string& client_id,
                       const string& op_id,
                       bool record_op_state,
                       req_info *info,
                       const string& source_zone,
                       rgw_obj& dest_obj,
                       rgw_obj& src_obj,
                       RGWBucketInfo& dest_bucket_info,
                       RGWBucketInfo& src_bucket_info,
                       ceph::real_time *src_mtime,
                       ceph::real_time *mtime,
                       const ceph::real_time *mod_ptr,
                       const ceph::real_time *unmod_ptr,
                       bool high_precision_time,
                       const char *if_match,
                       const char *if_nomatch,
                       AttrsMod attrs_mod,
                       bool copy_if_newer,
                       map<string, bufferlist>& attrs,
                       RGWObjCategory category,
                       uint64_t olh_epoch,
		       ceph::real_time delete_at,
                       string *version_id,
                       string *ptag,
                       ceph::buffer::list *petag,
                       void (*progress_cb)(off_t, void *),
                       void *progress_data,
                       rgw_zone_set *zones_trace= nullptr);
  /**
   * Copy an object.
   * dest_obj: the object to copy into
   * src_obj: the object to copy from
   * attrs: usage depends on attrs_mod parameter
   * attrs_mod: the modification mode of the attrs, may have the following values:
   *            ATTRSMOD_NONE - the attributes of the source object will be
   *                            copied without modifications, attrs parameter is ignored;
   *            ATTRSMOD_REPLACE - new object will have the attributes provided by attrs
   *                               parameter, source object attributes are not copied;
   *            ATTRSMOD_MERGE - any conflicting meta keys on the source object's attributes
   *                             are overwritten by values contained in attrs parameter.
   * Returns: 0 on success, -ERR# otherwise.
   */
  int copy_obj(RGWObjectCtx& obj_ctx,
               const rgw_user& user_id,
               const string& client_id,
               const string& op_id,
               req_info *info,
               const string& source_zone,
               rgw_obj& dest_obj,
               rgw_obj& src_obj,
               RGWBucketInfo& dest_bucket_info,
               RGWBucketInfo& src_bucket_info,
               ceph::real_time *src_mtime,
               ceph::real_time *mtime,
               const ceph::real_time *mod_ptr,
               const ceph::real_time *unmod_ptr,
               bool high_precision_time,
               const char *if_match,
               const char *if_nomatch,
               AttrsMod attrs_mod,
               bool copy_if_newer,
               map<std::string, bufferlist>& attrs,
               RGWObjCategory category,
               uint64_t olh_epoch,
	       ceph::real_time delete_at,
               string *version_id,
               string *ptag,
               ceph::buffer::list *petag,
               void (*progress_cb)(off_t, void *),
               void *progress_data);

  int copy_obj_data(RGWObjectCtx& obj_ctx,
               RGWBucketInfo& dest_bucket_info,
	       RGWRados::Object::Read& read_op, off_t end,
               rgw_obj& dest_obj,
	       ceph::real_time *mtime,
	       ceph::real_time set_mtime,
               map<string, bufferlist>& attrs,
               uint64_t olh_epoch,
	       ceph::real_time delete_at,
               string *version_id,
               ceph::buffer::list *petag);
  
  int check_bucket_empty(RGWBucketInfo& bucket_info);

  /**
   * Delete a bucket.
   * bucket: the name of the bucket to delete
   * Returns 0 on success, -ERR# otherwise.
   */
  int delete_bucket(RGWBucketInfo& bucket_info, RGWObjVersionTracker& objv_tracker, bool check_empty = true);

  bool is_meta_master();

  /**
   * Check to see if the bucket metadata is synced
   */
  bool is_syncing_bucket_meta(const rgw_bucket& bucket);
  void wakeup_meta_sync_shards(set<int>& shard_ids);
  void wakeup_data_sync_shards(const string& source_zone, map<int, set<string> >& shard_ids);

  RGWMetaSyncStatusManager* get_meta_sync_manager();
  RGWDataSyncStatusManager* get_data_sync_manager(const std::string& source_zone);

  int set_bucket_owner(rgw_bucket& bucket, ACLOwner& owner);
  int set_buckets_enabled(std::vector<rgw_bucket>& buckets, bool enabled);
  int bucket_suspended(rgw_bucket& bucket, bool *suspended);

  /** Delete an object.*/
  int delete_obj(RGWObjectCtx& obj_ctx,
                         const RGWBucketInfo& bucket_owner,
                         const rgw_obj& src_obj,
                         int versioning_status,
                         uint16_t bilog_flags = 0,
                         const ceph::real_time& expiration_time = ceph::real_time(),
                         rgw_zone_set *zones_trace = nullptr);

  /** Delete a raw object.*/
  int delete_raw_obj(const rgw_raw_obj& obj);

  /* Delete a system object */
  virtual int delete_system_obj(rgw_raw_obj& src_obj, RGWObjVersionTracker *objv_tracker = NULL);

  /** Remove an object from the bucket index */
  int delete_obj_index(const rgw_obj& obj);

  /**
   * Get an attribute for a system object.
   * obj: the object to get attr
   * name: name of the attr to retrieve
   * dest: bufferlist to store the result in
   * Returns: 0 on success, -ERR# otherwise.
   */
  virtual int system_obj_get_attr(rgw_raw_obj& obj, const char *name, bufferlist& dest);

  int system_obj_set_attr(void *ctx, rgw_raw_obj& obj, const char *name, bufferlist& bl,
                          RGWObjVersionTracker *objv_tracker);
  virtual int system_obj_set_attrs(void *ctx, rgw_raw_obj& obj,
                                   map<string, bufferlist>& attrs,
                                   map<string, bufferlist>* rmattrs,
                                   RGWObjVersionTracker *objv_tracker);

  /**
   * Set an attr on an object.
   * bucket: name of the bucket holding the object
   * obj: name of the object to set the attr on
   * name: the attr to set
   * bl: the contents of the attr
   * Returns: 0 on success, -ERR# otherwise.
   */
  int set_attr(void *ctx, const RGWBucketInfo& bucket_info, rgw_obj& obj, const char *name, bufferlist& bl);

  int set_attrs(void *ctx, const RGWBucketInfo& bucket_info, rgw_obj& obj,
                        map<string, bufferlist>& attrs,
                        map<string, bufferlist>* rmattrs);

  int get_system_obj_state(RGWObjectCtx *rctx, rgw_raw_obj& obj, RGWRawObjState **state, RGWObjVersionTracker *objv_tracker);
  int get_obj_state(RGWObjectCtx *rctx, const RGWBucketInfo& bucket_info, const rgw_obj& obj, RGWObjState **state,
                    bool follow_olh, bool assume_noent = false);
  int get_obj_state(RGWObjectCtx *rctx, const RGWBucketInfo& bucket_info, const rgw_obj& obj, RGWObjState **state) {
    return get_obj_state(rctx, bucket_info, obj, state, true);
  }

  virtual int stat_system_obj(RGWObjectCtx& obj_ctx,
                              RGWRados::SystemObject::Read::GetObjState& state,
                              rgw_raw_obj& obj,
                              map<string, bufferlist> *attrs,
                              ceph::real_time *lastmod,
                              uint64_t *obj_size,
                              RGWObjVersionTracker *objv_tracker);

  virtual int get_system_obj(RGWObjectCtx& obj_ctx, RGWRados::SystemObject::Read::GetObjState& read_state,
                             RGWObjVersionTracker *objv_tracker, rgw_raw_obj& obj,
                             bufferlist& bl, off_t ofs, off_t end,
                             map<string, bufferlist> *attrs,
                             rgw_cache_entry_info *cache_info,
			     boost::optional<obj_version> refresh_version =
			       boost::none);

  virtual void register_chained_cache(RGWChainedCache *cache) {}
  virtual bool chain_cache_entry(std::initializer_list<rgw_cache_entry_info*> cache_info_entries,
				 RGWChainedCache::Entry *chained_entry) { return false; }

  int iterate_obj(RGWObjectCtx& ctx,
                  const RGWBucketInfo& bucket_info, const rgw_obj& obj,
                  off_t ofs, off_t end,
                  uint64_t max_chunk_size,
                  int (*iterate_obj_cb)(const RGWBucketInfo& bucket_info, const rgw_obj& obj, const rgw_raw_obj&, off_t, off_t, off_t, bool, RGWObjState *, void *),
                  void *arg);

  int flush_read_list(struct get_obj_data *d);

  int get_obj_iterate_cb(RGWObjectCtx *ctx, RGWObjState *astate,
                         const RGWBucketInfo& bucket_info, const rgw_obj& obj,
                         const rgw_raw_obj& read_obj,
                         off_t obj_ofs, off_t read_ofs, off_t len,
                         bool is_head_obj, void *arg);

  void get_obj_aio_completion_cb(librados::completion_t cb, void *arg);

  /**
   * a simple object read without keeping state
   */

  virtual int raw_obj_stat(rgw_raw_obj& obj, uint64_t *psize, ceph::real_time *pmtime, uint64_t *epoch,
                       map<string, bufferlist> *attrs, bufferlist *first_chunk,
                       RGWObjVersionTracker *objv_tracker);

  int obj_operate(const RGWBucketInfo& bucket_info, const rgw_obj& obj, librados::ObjectWriteOperation *op);
  int obj_operate(const RGWBucketInfo& bucket_info, const rgw_obj& obj, librados::ObjectReadOperation *op);

  int guard_reshard(BucketShard *bs, const rgw_obj& obj_instance, std::function<int(BucketShard *)> call);
  int block_while_resharding(RGWRados::BucketShard *bs, string *new_bucket_id);

  void bucket_index_guard_olh_op(RGWObjState& olh_state, librados::ObjectOperation& op);
  int olh_init_modification(const RGWBucketInfo& bucket_info, RGWObjState& state, const rgw_obj& olh_obj, string *op_tag);
  int olh_init_modification_impl(const RGWBucketInfo& bucket_info, RGWObjState& state, const rgw_obj& olh_obj, string *op_tag);
  int bucket_index_link_olh(const RGWBucketInfo& bucket_info, RGWObjState& olh_state,
                            const rgw_obj& obj_instance, bool delete_marker,
                            const string& op_tag, struct rgw_bucket_dir_entry_meta *meta,
                            uint64_t olh_epoch,
                            ceph::real_time unmod_since, bool high_precision_time, rgw_zone_set *zones_trace = nullptr);
  int bucket_index_unlink_instance(const RGWBucketInfo& bucket_info, const rgw_obj& obj_instance, const string& op_tag, const string& olh_tag, uint64_t olh_epoch, rgw_zone_set *zones_trace = nullptr);
  int bucket_index_read_olh_log(const RGWBucketInfo& bucket_info, RGWObjState& state, const rgw_obj& obj_instance, uint64_t ver_marker,
                                map<uint64_t, vector<rgw_bucket_olh_log_entry> > *log, bool *is_truncated);
  int bucket_index_trim_olh_log(const RGWBucketInfo& bucket_info, RGWObjState& obj_state, const rgw_obj& obj_instance, uint64_t ver);
  int bucket_index_clear_olh(const RGWBucketInfo& bucket_info, RGWObjState& state, const rgw_obj& obj_instance);
  int apply_olh_log(RGWObjectCtx& ctx, RGWObjState& obj_state, const RGWBucketInfo& bucket_info, const rgw_obj& obj,
                    bufferlist& obj_tag, map<uint64_t, vector<rgw_bucket_olh_log_entry> >& log,
                    uint64_t *plast_ver, rgw_zone_set *zones_trace = nullptr);
  int update_olh(RGWObjectCtx& obj_ctx, RGWObjState *state, const RGWBucketInfo& bucket_info, const rgw_obj& obj, rgw_zone_set *zones_trace = nullptr);
  int set_olh(RGWObjectCtx& obj_ctx, RGWBucketInfo& bucket_info, const rgw_obj& target_obj, bool delete_marker, rgw_bucket_dir_entry_meta *meta,
              uint64_t olh_epoch, ceph::real_time unmod_since, bool high_precision_time, rgw_zone_set *zones_trace = nullptr);
  int unlink_obj_instance(RGWObjectCtx& obj_ctx, RGWBucketInfo& bucket_info, const rgw_obj& target_obj,
                          uint64_t olh_epoch, rgw_zone_set *zones_trace = nullptr);

  void check_pending_olh_entries(map<string, bufferlist>& pending_entries, map<string, bufferlist> *rm_pending_entries);
  int remove_olh_pending_entries(const RGWBucketInfo& bucket_info, RGWObjState& state, const rgw_obj& olh_obj, map<string, bufferlist>& pending_attrs);
  int follow_olh(const RGWBucketInfo& bucket_info, RGWObjectCtx& ctx, RGWObjState *state, const rgw_obj& olh_obj, rgw_obj *target);
  int get_olh(const RGWBucketInfo& bucket_info, const rgw_obj& obj, RGWOLHInfo *olh);

  void gen_rand_obj_instance_name(rgw_obj *target);

  int omap_get_vals(rgw_raw_obj& obj, bufferlist& header, const std::string& marker, uint64_t count, std::map<string, bufferlist>& m);
  int omap_get_all(rgw_raw_obj& obj, bufferlist& header, std::map<string, bufferlist>& m);
  int omap_set(rgw_raw_obj& obj, const std::string& key, bufferlist& bl, bool must_exist = false);
  int omap_set(rgw_raw_obj& obj, map<std::string, bufferlist>& m, bool must_exist = false);
  int omap_del(rgw_raw_obj& obj, const std::string& key);
  int update_containers_stats(map<string, RGWBucketEnt>& m);
  int append_async(rgw_raw_obj& obj, size_t size, bufferlist& bl);

  int watch(const string& oid, uint64_t *watch_handle, librados::WatchCtx2 *ctx);
  int unwatch(uint64_t watch_handle);
  void add_watcher(int i);
  void remove_watcher(int i);
  virtual bool need_watch_notify() { return false; }
  int init_watch();
  void finalize_watch();
  int distribute(const string& key, bufferlist& bl);
  virtual int watch_cb(uint64_t notify_id,
		       uint64_t cookie,
		       uint64_t notifier_id,
		       bufferlist& bl) { return 0; }
  void pick_control_oid(const string& key, string& notify_oid);

  virtual void set_cache_enabled(bool state) {}

  void set_atomic(void *ctx, rgw_obj& obj) {
    RGWObjectCtx *rctx = static_cast<RGWObjectCtx *>(ctx);
    rctx->obj.set_atomic(obj);
  }
  void set_prefetch_data(void *ctx, rgw_obj& obj) {
    RGWObjectCtx *rctx = static_cast<RGWObjectCtx *>(ctx);
    rctx->obj.set_prefetch_data(obj);
  }
  void set_prefetch_data(void *ctx, rgw_raw_obj& obj) {
    RGWObjectCtx *rctx = static_cast<RGWObjectCtx *>(ctx);
    rctx->raw.set_prefetch_data(obj);
  }

  int decode_policy(bufferlist& bl, ACLOwner *owner);
  int get_bucket_stats(RGWBucketInfo& bucket_info, int shard_id, string *bucket_ver, string *master_ver,
      map<RGWObjCategory, RGWStorageStats>& stats, string *max_marker, bool* syncstopped = NULL);
  int get_bucket_stats_async(RGWBucketInfo& bucket_info, int shard_id, RGWGetBucketStats_CB *cb);
  int get_user_stats(const rgw_user& user, RGWStorageStats& stats);
  int get_user_stats_async(const rgw_user& user, RGWGetUserStats_CB *cb);
  void get_bucket_instance_obj(const rgw_bucket& bucket, rgw_raw_obj& obj);
  void get_bucket_meta_oid(const rgw_bucket& bucket, string& oid);

  int put_bucket_entrypoint_info(const string& tenant_name, const string& bucket_name, RGWBucketEntryPoint& entry_point,
                                 bool exclusive, RGWObjVersionTracker& objv_tracker, ceph::real_time mtime,
                                 map<string, bufferlist> *pattrs);
  int put_bucket_instance_info(RGWBucketInfo& info, bool exclusive, ceph::real_time mtime, map<string, bufferlist> *pattrs);
  int get_bucket_entrypoint_info(RGWObjectCtx& obj_ctx, const string& tenant_name, const string& bucket_name,
                                 RGWBucketEntryPoint& entry_point, RGWObjVersionTracker *objv_tracker,
                                 ceph::real_time *pmtime, map<string, bufferlist> *pattrs, rgw_cache_entry_info *cache_info = NULL,
				 boost::optional<obj_version> refresh_version = boost::none);
  int get_bucket_instance_info(RGWObjectCtx& obj_ctx, const string& meta_key, RGWBucketInfo& info, ceph::real_time *pmtime, map<string, bufferlist> *pattrs);
  int get_bucket_instance_info(RGWObjectCtx& obj_ctx, const rgw_bucket& bucket, RGWBucketInfo& info, ceph::real_time *pmtime, map<string, bufferlist> *pattrs);
  int get_bucket_instance_from_oid(RGWObjectCtx& obj_ctx, const string& oid, RGWBucketInfo& info, ceph::real_time *pmtime, map<string, bufferlist> *pattrs,
                                   rgw_cache_entry_info *cache_info = NULL,
				   boost::optional<obj_version> refresh_version = boost::none);

  int convert_old_bucket_info(RGWObjectCtx& obj_ctx, const string& tenant_name, const string& bucket_name);
  static void make_bucket_entry_name(const string& tenant_name, const string& bucket_name, string& bucket_entry);


private:
  int _get_bucket_info(RGWObjectCtx& obj_ctx, const string& tenant,
		       const string& bucket_name, RGWBucketInfo& info,
		       real_time *pmtime,
		       map<string, bufferlist> *pattrs,
		       boost::optional<obj_version> refresh_version);
public:

  bool call(std::string_view command, const cmdmap_t& cmdmap,
	    std::string_view format,
	    bufferlist& out) override final;

protected:
  void cache_list_dump_helper(Formatter* f,
			      const std::string& name,
			      const ceph::real_time mtime,
			      const std::uint64_t size) {
    f->dump_string("name", name);
    f->dump_string("mtime", ceph::to_iso_8601(mtime));
    f->dump_unsigned("size", size);
  }

  // `call_list` must iterate over all cache entries and call
  // `cache_list_dump_helper` with the supplied Formatter on any that
  // include `filter` as a substring.
  //
  virtual void call_list(const std::optional<std::string>& filter,
			 Formatter* format);
  // `call_inspect` must look up the requested target and, if found,
  // dump it to the supplied Formatter and return true. If not found,
  // it must return false.
  //
  virtual bool call_inspect(const std::string& target, Formatter* format);

  // `call_erase` must erase the requested target and return true. If
  // the requested target does not exist, it should return false.
  virtual bool call_erase(const std::string& target);

  // `call_zap` must erase the cache.
  virtual void call_zap();
public:

  int get_bucket_info(RGWObjectCtx& obj_ctx,
		      const string& tenant_name, const string& bucket_name,
		      RGWBucketInfo& info,
		      ceph::real_time *pmtime, map<string, bufferlist> *pattrs = NULL);

  // Returns true on successful refresh. Returns false if there was an
  // error or the version stored on the OSD is the same as that
  // presented in the BucketInfo structure.
  //
  int try_refresh_bucket_info(RGWBucketInfo& info,
			      ceph::real_time *pmtime,
			      map<string, bufferlist> *pattrs = nullptr);

  int put_linked_bucket_info(RGWBucketInfo& info, bool exclusive, ceph::real_time mtime, obj_version *pep_objv,
			     map<string, bufferlist> *pattrs, bool create_entry_point);

  int cls_rgw_init_index(librados::IoCtx& io_ctx, librados::ObjectWriteOperation& op, string& oid);
  int cls_obj_prepare_op(BucketShard& bs, RGWModifyOp op, string& tag, rgw_obj& obj, uint16_t bilog_flags, rgw_zone_set *zones_trace = nullptr);
  int cls_obj_complete_op(BucketShard& bs, const rgw_obj& obj, RGWModifyOp op, string& tag, int64_t pool, uint64_t epoch,
                          rgw_bucket_dir_entry& ent, RGWObjCategory category, list<rgw_obj_index_key> *remove_objs, uint16_t bilog_flags, rgw_zone_set *zones_trace = nullptr);
  int cls_obj_complete_add(BucketShard& bs, const rgw_obj& obj, string& tag, int64_t pool, uint64_t epoch, rgw_bucket_dir_entry& ent,
                           RGWObjCategory category, list<rgw_obj_index_key> *remove_objs, uint16_t bilog_flags, rgw_zone_set *zones_trace = nullptr);
  int cls_obj_complete_del(BucketShard& bs, string& tag, int64_t pool, uint64_t epoch, rgw_obj& obj,
                           ceph::real_time& removed_mtime, list<rgw_obj_index_key> *remove_objs, uint16_t bilog_flags, rgw_zone_set *zones_trace = nullptr);
  int cls_obj_complete_cancel(BucketShard& bs, string& tag, rgw_obj& obj, uint16_t bilog_flags, rgw_zone_set *zones_trace = nullptr);
  int cls_obj_set_bucket_tag_timeout(RGWBucketInfo& bucket_info, uint64_t timeout);
  int cls_bucket_list(RGWBucketInfo& bucket_info, int shard_id, rgw_obj_index_key& start, const string& prefix,
                      uint32_t num_entries, bool list_versions, map<string, rgw_bucket_dir_entry>& m,
                      bool *is_truncated, rgw_obj_index_key *last_entry,
                      bool (*force_check_filter)(const string&  name) = NULL);
  int cls_bucket_head(const RGWBucketInfo& bucket_info, int shard_id, vector<rgw_bucket_dir_header>& headers, map<int, string> *bucket_instance_ids = NULL);
  int cls_bucket_head_async(const RGWBucketInfo& bucket_info, int shard_id, RGWGetDirHeader_CB *ctx, int *num_aio);
  int list_bi_log_entries(RGWBucketInfo& bucket_info, int shard_id, string& marker, uint32_t max, std::list<rgw_bi_log_entry>& result, bool *truncated);
  int trim_bi_log_entries(RGWBucketInfo& bucket_info, int shard_id, string& marker, string& end_marker);
  int resync_bi_log_entries(RGWBucketInfo& bucket_info, int shard_id);
  int stop_bi_log_entries(RGWBucketInfo& bucket_info, int shard_id);
  int get_bi_log_status(RGWBucketInfo& bucket_info, int shard_id, map<int, string>& max_marker);

  int bi_get_instance(const RGWBucketInfo& bucket_info, rgw_obj& obj, rgw_bucket_dir_entry *dirent);
  int bi_get(rgw_bucket& bucket, rgw_obj& obj, BIIndexType index_type, rgw_cls_bi_entry *entry);
  void bi_put(librados::ObjectWriteOperation& op, BucketShard& bs, rgw_cls_bi_entry& entry);
  int bi_put(BucketShard& bs, rgw_cls_bi_entry& entry);
  int bi_put(rgw_bucket& bucket, rgw_obj& obj, rgw_cls_bi_entry& entry);
  int bi_list(rgw_bucket& bucket, int shard_id, const string& filter_obj, const string& marker, uint32_t max, list<rgw_cls_bi_entry> *entries, bool *is_truncated);
  int bi_list(BucketShard& bs, const string& filter_obj, const string& marker, uint32_t max, list<rgw_cls_bi_entry> *entries, bool *is_truncated);
  int bi_list(rgw_bucket& bucket, const string& obj_name, const string& marker, uint32_t max,
              list<rgw_cls_bi_entry> *entries, bool *is_truncated);
  int bi_remove(BucketShard& bs);

  int cls_obj_usage_log_add(const string& oid, rgw_usage_log_info& info);
  int cls_obj_usage_log_read(string& oid, string& user, uint64_t start_epoch, uint64_t end_epoch, uint32_t max_entries,
                             string& read_iter, map<rgw_user_bucket, rgw_usage_log_entry>& usage, bool *is_truncated);
  int cls_obj_usage_log_trim(string& oid, string& user, uint64_t start_epoch, uint64_t end_epoch);
  int cls_obj_usage_log_clear(string& oid);

  int key_to_shard_id(const string& key, int max_shards);
  void shard_name(const string& prefix, unsigned max_shards, const string& key, string& name, int *shard_id);
  void shard_name(const string& prefix, unsigned max_shards, const string& section, const string& key, string& name);
  void shard_name(const string& prefix, unsigned shard_id, string& name);
  int get_target_shard_id(const RGWBucketInfo& bucket_info, const string& obj_key, int *shard_id);
  void time_log_prepare_entry(cls_log_entry& entry, const ceph::real_time& ut, const string& section, const string& key, bufferlist& bl);
  int time_log_add_init(librados::IoCtx& io_ctx);
  int time_log_add(const string& oid, list<cls_log_entry>& entries,
		   librados::AioCompletion *completion, bool monotonic_inc = true);
  int time_log_add(const string& oid, const ceph::real_time& ut, const string& section, const string& key, bufferlist& bl);
  int time_log_list(const string& oid, const ceph::real_time& start_time, const ceph::real_time& end_time,
                    int max_entries, list<cls_log_entry>& entries,
		    const string& marker, string *out_marker, bool *truncated);
  int time_log_info(const string& oid, cls_log_header *header);
  int time_log_info_async(librados::IoCtx& io_ctx, const string& oid, cls_log_header *header, librados::AioCompletion *completion);
  int time_log_trim(const string& oid, const ceph::real_time& start_time, const ceph::real_time& end_time,
                    const string& from_marker, const string& to_marker,
                    librados::AioCompletion *completion = nullptr);

  string objexp_hint_get_shardname(int shard_num);
  int objexp_key_shard(const rgw_obj_index_key& key);
  void objexp_get_shard(int shard_num,
                        string& shard);                       /* out */
  int objexp_hint_add(const ceph::real_time& delete_at,
                      const string& tenant_name,
                      const string& bucket_name,
                      const string& bucket_id,
                      const rgw_obj_index_key& obj_key);
  int objexp_hint_list(const string& oid,
                       const ceph::real_time& start_time,
                       const ceph::real_time& end_time,
                       const int max_entries,
                       const string& marker,
                       list<cls_timeindex_entry>& entries, /* out */
                       string *out_marker,                 /* out */
                       bool *truncated);                   /* out */
  int objexp_hint_parse(cls_timeindex_entry &ti_entry,
                        objexp_hint_entry& hint_entry);    /* out */
  int objexp_hint_trim(const string& oid,
                       const ceph::real_time& start_time,
                       const ceph::real_time& end_time,
                       const string& from_marker = std::string(),
                       const string& to_marker   = std::string());

  int lock_exclusive(rgw_pool& pool, const string& oid, ceph::timespan& duration, string& zone_id, string& owner_id);
  int unlock(rgw_pool& pool, const string& oid, string& zone_id, string& owner_id);

  void update_gc_chain(rgw_obj& head_obj, RGWObjManifest& manifest, cls_rgw_obj_chain *chain);
  int send_chain_to_gc(cls_rgw_obj_chain& chain, const string& tag, bool sync);
  int gc_operate(string& oid, librados::ObjectWriteOperation *op);
  int gc_aio_operate(string& oid, librados::ObjectWriteOperation *op, librados::AioCompletion **pc = nullptr);
  int gc_operate(string& oid, librados::ObjectReadOperation *op, bufferlist *pbl);

  int list_gc_objs(int *index, string& marker, uint32_t max, bool expired_only, std::list<cls_rgw_gc_obj_info>& result, bool *truncated);
  int process_gc(bool expired_only);
  int process_expire_objects();
  int defer_gc(void *ctx, const RGWBucketInfo& bucket_info, const rgw_obj& obj);

  int process_lc();
  int list_lc_progress(const string& marker, uint32_t max_entries, map<string, int> *progress_map);
  
  int bucket_check_index(RGWBucketInfo& bucket_info,
                         map<RGWObjCategory, RGWStorageStats> *existing_stats,
                         map<RGWObjCategory, RGWStorageStats> *calculated_stats);
  int bucket_rebuild_index(RGWBucketInfo& bucket_info);
  int bucket_set_reshard(RGWBucketInfo& bucket_info, const cls_rgw_bucket_instance_entry& entry);
  int remove_objs_from_index(RGWBucketInfo& bucket_info, list<rgw_obj_index_key>& oid_list);
  int move_rados_obj(librados::IoCtx& src_ioctx,
		     const string& src_oid, const string& src_locator,
	             librados::IoCtx& dst_ioctx,
		     const string& dst_oid, const string& dst_locator);
  int fix_head_obj_locator(const RGWBucketInfo& bucket_info, bool copy_obj, bool remove_bad, rgw_obj_key& key);
  int fix_tail_obj_locator(const RGWBucketInfo& bucket_info, rgw_obj_key& key, bool fix, bool *need_fix);

  int cls_user_get_header(const string& user_id, cls_user_header *header);
  int cls_user_get_header_async(const string& user_id, RGWGetUserHeader_CB *ctx);
  int cls_user_sync_bucket_stats(rgw_raw_obj& user_obj, const RGWBucketInfo& bucket_info);
  int cls_user_list_buckets(rgw_raw_obj& obj,
                            const string& in_marker,
                            const string& end_marker,
                            int max_entries,
                            list<cls_user_bucket_entry>& entries,
                            string *out_marker,
                            bool *truncated);
  int cls_user_add_bucket(rgw_raw_obj& obj, const cls_user_bucket_entry& entry);
  int cls_user_update_buckets(rgw_raw_obj& obj, list<cls_user_bucket_entry>& entries, bool add);
  int cls_user_complete_stats_sync(rgw_raw_obj& obj);
  int complete_sync_user_stats(const rgw_user& user_id);
  int cls_user_remove_bucket(rgw_raw_obj& obj, const cls_user_bucket& bucket);
  int cls_user_get_bucket_stats(const rgw_bucket& bucket, cls_user_bucket_entry& entry);

  int check_quota(const rgw_user& bucket_owner, rgw_bucket& bucket,
                  RGWQuotaInfo& user_quota, RGWQuotaInfo& bucket_quota, uint64_t obj_size);

  int check_bucket_shards(const RGWBucketInfo& bucket_info, const rgw_bucket& bucket,
			  RGWQuotaInfo& bucket_quota);

  int add_bucket_to_reshard(const RGWBucketInfo& bucket_info, uint32_t new_num_shards);

  uint64_t instance_id();
  const string& zone_name() {
    return get_zone_params().get_name();
  }
  const string& zone_id() {
    return get_zone_params().get_id();
  }
  string unique_id(uint64_t unique_num) {
    char buf[32];
    snprintf(buf, sizeof(buf), ".%llu.%llu", (unsigned long long)instance_id(), (unsigned long long)unique_num);
    string s = get_zone_params().get_id() + buf;
    return s;
  }

  void init_unique_trans_id_deps() {
    char buf[16 + 2 + 1]; /* uint64_t needs 16, 2 hyphens add further 2 */

    snprintf(buf, sizeof(buf), "-%llx-", (unsigned long long)instance_id());
    url_encode(string(buf) + get_zone_params().get_name(), trans_id_suffix);
  }

  /* In order to preserve compability with Swift API, transaction ID
   * should contain at least 32 characters satisfying following spec:
   *  - first 21 chars must be in range [0-9a-f]. Swift uses this
   *    space for storing fragment of UUID obtained through a call to
   *    uuid4() function of Python's uuid module;
   *  - char no. 22 must be a hyphen;
   *  - at least 10 next characters constitute hex-formatted timestamp
   *    padded with zeroes if necessary. All bytes must be in [0-9a-f]
   *    range;
   *  - last, optional part of transaction ID is any url-encoded string
   *    without restriction on length. */
  string unique_trans_id(const uint64_t unique_num) {
    char buf[41]; /* 2 + 21 + 1 + 16 (timestamp can consume up to 16) + 1 */
    time_t timestamp = time(NULL);

    snprintf(buf, sizeof(buf), "tx%021llx-%010llx",
             (unsigned long long)unique_num,
             (unsigned long long)timestamp);

    return string(buf) + trans_id_suffix;
  }

  void get_log_pool(rgw_pool& pool) {
    pool = get_zone_params().log_pool;
  }

  bool need_to_log_data() {
    return get_zone().log_data;
  }

  bool need_to_log_metadata() {
    return is_meta_master() &&
      (get_zonegroup().zones.size() > 1 || current_period.is_multi_zonegroups_with_zones());
  }

  bool can_reshard() const {
    return current_period.get_id().empty() ||
      (zonegroup.zones.size() == 1 && current_period.is_single_zonegroup());
  }

  librados::Rados* get_rados_handle();

  int delete_raw_obj_aio(const rgw_raw_obj& obj, list<librados::AioCompletion *>& handles);
  int delete_obj_aio(const rgw_obj& obj, RGWBucketInfo& info, RGWObjState *astate,
                     list<librados::AioCompletion *>& handles, bool keep_index_consistent);
 private:
  /**
   * This is a helper method, it generates a list of bucket index objects with the given
   * bucket base oid and number of shards.
   *
   * bucket_oid_base [in] - base name of the bucket index object;
   * num_shards [in] - number of bucket index object shards.
   * bucket_objs [out] - filled by this method, a list of bucket index objects.
   */
  void get_bucket_index_objects(const string& bucket_oid_base, uint32_t num_shards,
      map<int, string>& bucket_objs, int shard_id = -1);

  /**
   * Get the bucket index object with the given base bucket index object and object key,
   * and the number of bucket index shards.
   *
   * bucket_oid_base [in] - bucket object base name.
   * obj_key [in] - object key.
   * num_shards [in] - number of bucket index shards.
   * hash_type [in] - type of hash to find the shard ID.
   * bucket_obj [out] - the bucket index object for the given object.
   *
   * Return 0 on success, a failure code otherwise.
   */
  int get_bucket_index_object(const string& bucket_oid_base, const string& obj_key,
      uint32_t num_shards, RGWBucketInfo::BIShardsHashType hash_type, string *bucket_obj, int *shard);

  void get_bucket_index_object(const string& bucket_oid_base, uint32_t num_shards,
                               int shard_id, string *bucket_obj);

  /**
   * Check the actual on-disk state of the object specified
   * by list_state, and fill in the time and size of object.
   * Then append any changes to suggested_updates for
   * the rgw class' dir_suggest_changes function.
   *
   * Note that this can maul list_state; don't use it afterwards. Also
   * it expects object to already be filled in from list_state; it only
   * sets the size and mtime.
   *
   * Returns 0 on success, -ENOENT if the object doesn't exist on disk,
   * and -errno on other failures. (-ENOENT is not a failure, and it
   * will encode that info as a suggested update.)
   */
  int check_disk_state(librados::IoCtx io_ctx,
                       const RGWBucketInfo& bucket_info,
                       rgw_bucket_dir_entry& list_state,
                       rgw_bucket_dir_entry& object,
                       bufferlist& suggested_updates);

  /**
   * Init pool iteration
   * pool: pool to use for the ctx initialization
   * ctx: context object to use for the iteration
   * Returns: 0 on success, -ERR# otherwise.
   */
  int pool_iterate_begin(const rgw_pool& pool, RGWPoolIterCtx& ctx);

  /**
   * Init pool iteration
   * pool: pool to use
   * cursor: position to start iteration
   * ctx: context object to use for the iteration
   * Returns: 0 on success, -ERR# otherwise.
   */
  int pool_iterate_begin(const rgw_pool& pool, const string& cursor, RGWPoolIterCtx& ctx);

  /**
   * Get pool iteration position
   * ctx: context object to use for the iteration
   * Returns: string representation of position
   */
  string pool_iterate_get_cursor(RGWPoolIterCtx& ctx);

  /**
   * Iterate over pool return object names, use optional filter
   * ctx: iteration context, initialized with pool_iterate_begin()
   * num: max number of objects to return
   * objs: a vector that the results will append into
   * is_truncated: if not NULL, will hold true iff iteration is complete
   * filter: if not NULL, will be used to filter returned objects
   * Returns: 0 on success, -ERR# otherwise.
   */
  int pool_iterate(RGWPoolIterCtx& ctx, uint32_t num, vector<rgw_bucket_dir_entry>& objs,
                   bool *is_truncated, RGWAccessListFilter *filter);

  uint64_t next_bucket_id();
};

class RGWStoreManager {
public:
  RGWStoreManager() {}
  static RGWRados *get_storage(CephContext *cct, bool use_gc_thread, bool use_lc_thread, bool quota_threads, bool run_sync_thread, bool run_reshard_thread) {
    RGWRados *store = init_storage_provider(cct, use_gc_thread, use_lc_thread, quota_threads, run_sync_thread,
					    run_reshard_thread);
    return store;
  }
  static RGWRados *get_raw_storage(CephContext *cct) {
    RGWRados *store = init_raw_storage_provider(cct);
    return store;
  }
  static RGWRados *init_storage_provider(CephContext *cct, bool use_gc_thread, bool use_lc_thread, bool quota_threads, bool run_sync_thread, bool run_reshard_thread);
  static RGWRados *init_raw_storage_provider(CephContext *cct);
  static void close_storage(RGWRados *store);

};

template <class T>
class RGWChainedCacheImpl : public RGWChainedCache {
  ceph::timespan expiry;
  RWLock lock;

  std::unordered_map<std::string, std::pair<T, ceph::coarse_mono_time>> entries;

public:
  RGWChainedCacheImpl() : lock("RGWChainedCacheImpl::lock") {}

  void init(RGWRados *store) {
    store->register_chained_cache(this);
    expiry = std::chrono::seconds(store->ctx()->_conf->get_val<uint64_t>(
				    "rgw_cache_expiry_interval"));
  }

  boost::optional<T> find(const string& key) {
    RWLock::RLocker rl(lock);
    auto iter = entries.find(key);
    if (iter == entries.end()) {
      return boost::none;
    }
    if (expiry.count() &&
	(ceph::coarse_mono_clock::now() - iter->second.second) > expiry) {
      return boost::none;
    }

    return iter->second.first;
  }

  bool put(RGWRados *store, const string& key, T *entry,
	   std::initializer_list<rgw_cache_entry_info *> cache_info_entries) {
    Entry chain_entry(this, key, entry);

    /* we need the store cache to call us under its lock to maintain lock ordering */
    return store->chain_cache_entry(cache_info_entries, &chain_entry);
  }

  void chain_cb(const string& key, void *data) override {
    T *entry = static_cast<T *>(data);
    RWLock::WLocker wl(lock);
    entries[key].first = *entry;
    if (expiry.count() > 0) {
      entries[key].second = ceph::coarse_mono_clock::now();
    }
  }

  void invalidate(const string& key) override {
    RWLock::WLocker wl(lock);
    entries.erase(key);
  }

  void invalidate_all() override {
    RWLock::WLocker wl(lock);
    entries.clear();
  }
}; /* RGWChainedCacheImpl */

/**
 * Base of PUT operation.
 * Allow to create chained data transformers like compresors and encryptors.
 */
class RGWPutObjDataProcessor
{
public:
  RGWPutObjDataProcessor(){}
  virtual ~RGWPutObjDataProcessor(){}
  virtual int handle_data(bufferlist& bl, off_t ofs, void **phandle, rgw_raw_obj *pobj, bool *again) = 0;
  virtual int throttle_data(void *handle, const rgw_raw_obj& obj, uint64_t size, bool need_to_wait) = 0;
}; /* RGWPutObjDataProcessor */


class RGWPutObjProcessor : public RGWPutObjDataProcessor
{
protected:
  RGWRados *store;
  RGWObjectCtx& obj_ctx;
  bool is_complete;
  RGWBucketInfo bucket_info;
  bool canceled;

  virtual int do_complete(size_t accounted_size, const string& etag,
                          ceph::real_time *mtime, ceph::real_time set_mtime,
                          map<string, bufferlist>& attrs, ceph::real_time delete_at,
                          const char *if_match, const char *if_nomatch, const string *user_data,
                          rgw_zone_set* zones_trace = nullptr) = 0;

public:
  RGWPutObjProcessor(RGWObjectCtx& _obj_ctx, RGWBucketInfo& _bi) : store(NULL), 
                                                                   obj_ctx(_obj_ctx), 
                                                                   is_complete(false), 
                                                                   bucket_info(_bi), 
                                                                   canceled(false) {}
  ~RGWPutObjProcessor() override {}
  virtual int prepare(RGWRados *_store, string *oid_rand) {
    store = _store;
    return 0;
  }

  int complete(size_t accounted_size, const string& etag, 
               ceph::real_time *mtime, ceph::real_time set_mtime,
               map<string, bufferlist>& attrs, ceph::real_time delete_at,
               const char *if_match = NULL, const char *if_nomatch = NULL, const string *user_data = nullptr,
               rgw_zone_set *zones_trace = nullptr);

  CephContext *ctx();

  bool is_canceled() { return canceled; }
}; /* RGWPutObjProcessor */

struct put_obj_aio_info {
  void *handle;
  rgw_raw_obj obj;
  uint64_t size;
};

#define RGW_PUT_OBJ_MIN_WINDOW_SIZE_DEFAULT (16 * 1024 * 1024)

class RGWPutObjProcessor_Aio : public RGWPutObjProcessor
{
  list<struct put_obj_aio_info> pending;
  uint64_t window_size{RGW_PUT_OBJ_MIN_WINDOW_SIZE_DEFAULT};
  uint64_t pending_size{0};

  struct put_obj_aio_info pop_pending();
  int wait_pending_front();
  bool pending_has_completed();

  rgw_raw_obj last_written_obj;

protected:
  uint64_t obj_len{0};

  set<rgw_raw_obj> written_objs;
  rgw_obj head_obj;

  void add_written_obj(const rgw_raw_obj& obj) {
    written_objs.insert(obj);
  }

  int drain_pending();
  int handle_obj_data(rgw_raw_obj& obj, bufferlist& bl, off_t ofs, off_t abs_ofs, void **phandle, bool exclusive);

public:
  int prepare(RGWRados *store, string *oid_rand) override;
  int throttle_data(void *handle, const rgw_raw_obj& obj, uint64_t size, bool need_to_wait) override;

  RGWPutObjProcessor_Aio(RGWObjectCtx& obj_ctx, RGWBucketInfo& bucket_info) : RGWPutObjProcessor(obj_ctx, bucket_info) {}
  ~RGWPutObjProcessor_Aio() override;
}; /* RGWPutObjProcessor_Aio */

class RGWPutObjProcessor_Atomic : public RGWPutObjProcessor_Aio
{
  bufferlist first_chunk;
  uint64_t part_size;
  off_t cur_part_ofs;
  off_t next_part_ofs;
  int cur_part_id;
  off_t data_ofs;

  bufferlist pending_data_bl;
  uint64_t max_chunk_size;

  bool versioned_object;
  uint64_t olh_epoch;
  string version_id;

protected:
  rgw_bucket bucket;
  string obj_str;

  string unique_tag;

  rgw_raw_obj cur_obj;
  RGWObjManifest manifest;
  RGWObjManifest::generator manifest_gen;

  int write_data(bufferlist& bl, off_t ofs, void **phandle, rgw_raw_obj *pobj, bool exclusive);
  int do_complete(size_t accounted_size, const string& etag,
                  ceph::real_time *mtime, ceph::real_time set_mtime,
                  map<string, bufferlist>& attrs, ceph::real_time delete_at,
                  const char *if_match, const char *if_nomatch, const string *user_data, rgw_zone_set *zones_trace) override;

  int prepare_next_part(off_t ofs);
  int complete_parts();
  int complete_writing_data();

  int prepare_init(RGWRados *store, string *oid_rand);

public:
  ~RGWPutObjProcessor_Atomic() override {}
  RGWPutObjProcessor_Atomic(RGWObjectCtx& obj_ctx, RGWBucketInfo& bucket_info,
                            rgw_bucket& _b, const string& _o, uint64_t _p, const string& _t, bool versioned) :
                                RGWPutObjProcessor_Aio(obj_ctx, bucket_info),
                                part_size(_p),
                                cur_part_ofs(0),
                                next_part_ofs(_p),
                                cur_part_id(0),
                                data_ofs(0),
                                max_chunk_size(0),
                                versioned_object(versioned),
                                olh_epoch(0),
                                bucket(_b),
                                obj_str(_o),
                                unique_tag(_t) {}
  int prepare(RGWRados *store, string *oid_rand) override;
  virtual bool immutable_head() { return false; }
  int handle_data(bufferlist& bl, off_t ofs, void **phandle, rgw_raw_obj *pobj, bool *again) override;

  void set_olh_epoch(uint64_t epoch) {
    olh_epoch = epoch;
  }

  void set_version_id(const string& vid) {
    version_id = vid;
  }

  const string& get_version_id() const {
    return version_id;
  }
}; /* RGWPutObjProcessor_Atomic */

#define MP_META_SUFFIX ".meta"

class RGWMPObj {
  string oid;
  string prefix;
  string meta;
  string upload_id;
public:
  RGWMPObj() {}
  RGWMPObj(const string& _oid, const string& _upload_id) {
    init(_oid, _upload_id, _upload_id);
  }
  void init(const string& _oid, const string& _upload_id) {
    init(_oid, _upload_id, _upload_id);
  }
  void init(const string& _oid, const string& _upload_id, const string& part_unique_str) {
    if (_oid.empty()) {
      clear();
      return;
    }
    oid = _oid;
    upload_id = _upload_id;
    prefix = oid + ".";
    meta = prefix + upload_id + MP_META_SUFFIX;
    prefix.append(part_unique_str);
  }
  string& get_meta() { return meta; }
  string get_part(int num) {
    char buf[16];
    snprintf(buf, 16, ".%d", num);
    string s = prefix;
    s.append(buf);
    return s;
  }
  string get_part(string& part) {
    string s = prefix;
    s.append(".");
    s.append(part);
    return s;
  }
  string& get_upload_id() {
    return upload_id;
  }
  string& get_key() {
    return oid;
  }
  bool from_meta(string& meta) {
    int end_pos = meta.rfind('.'); // search for ".meta"
    if (end_pos < 0)
      return false;
    int mid_pos = meta.rfind('.', end_pos - 1); // <key>.<upload_id>
    if (mid_pos < 0)
      return false;
    oid = meta.substr(0, mid_pos);
    upload_id = meta.substr(mid_pos + 1, end_pos - mid_pos - 1);
    init(oid, upload_id, upload_id);
    return true;
  }
  void clear() {
    oid = "";
    prefix = "";
    meta = "";
    upload_id = "";
  }
};

class RGWPutObjProcessor_Multipart : public RGWPutObjProcessor_Atomic
{
  string part_num;
  RGWMPObj mp;
  req_state *s;
  string upload_id;

protected:
  int prepare(RGWRados *store, string *oid_rand);
  int do_complete(size_t accounted_size, const string& etag,
                  ceph::real_time *mtime, ceph::real_time set_mtime,
                  map<string, bufferlist>& attrs, ceph::real_time delete_at,
                  const char *if_match, const char *if_nomatch, const string *user_data,
                  rgw_zone_set *zones_trace) override;
public:
  bool immutable_head() { return true; }
  RGWPutObjProcessor_Multipart(RGWObjectCtx& obj_ctx, RGWBucketInfo& bucket_info, uint64_t _p, req_state *_s) :
                   RGWPutObjProcessor_Atomic(obj_ctx, bucket_info, _s->bucket, _s->object.name, _p, _s->req_id, false), s(_s) {}
  void get_mp(RGWMPObj** _mp);
}; /* RGWPutObjProcessor_Multipart */

class RGWRadosThread {
  class Worker : public Thread {
    CephContext *cct;
    RGWRadosThread *processor;
    Mutex lock;
    Cond cond;

    void wait() {
      Mutex::Locker l(lock);
      cond.Wait(lock);
    };

    void wait_interval(const utime_t& wait_time) {
      Mutex::Locker l(lock);
      cond.WaitInterval(lock, wait_time);
    }

  public:
    Worker(CephContext *_cct, RGWRadosThread *_p) : cct(_cct), processor(_p), lock("RGWRadosThread::Worker") {}
    void *entry() override;
    void signal() {
      Mutex::Locker l(lock);
      cond.Signal();
    }
  };

  Worker *worker;

protected:
  CephContext *cct;
  RGWRados *store;

  std::atomic<bool> down_flag = { false };

  string thread_name;

  virtual uint64_t interval_msec() = 0;
  virtual void stop_process() {}
public:
  RGWRadosThread(RGWRados *_store, const string& thread_name = "radosgw") 
    : worker(NULL), cct(_store->ctx()), store(_store), thread_name(thread_name) {}
  virtual ~RGWRadosThread() {
    stop();
  }

  virtual int init() { return 0; }
  virtual int process() = 0;

  bool going_down() { return down_flag; }

  void start();
  void stop();

  void signal() {
    if (worker) {
      worker->signal();
    }
  }
};

#endif
