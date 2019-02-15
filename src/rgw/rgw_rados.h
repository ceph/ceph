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
#include "common/ceph_json.h"
#include "rgw_common.h"
#include "cls/rgw/cls_rgw_types.h"
#include "cls/version/cls_version_types.h"
#include "cls/log/cls_log_types.h"
#include "cls/timeindex/cls_timeindex_types.h"
#include "cls/otp/cls_otp_types.h"
#include "rgw_log.h"
#include "rgw_metadata.h"
#include "rgw_meta_sync_status.h"
#include "rgw_period_puller.h"
#include "rgw_sync_module.h"
#include "rgw_sync_log_trim.h"
#include "rgw_service.h"

#include "services/svc_rados.h"
#include "services/svc_zone.h"

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
struct RGWZoneGroup;
struct RGWZoneParams;
class RGWReshard;
class RGWReshardWait;

class RGWSysObjectCtx;

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
  rgw_placement_rule placement_rule;
  rgw_bucket bucket;

  void dump(Formatter *f) const;
};

class rgw_obj_select {
  rgw_placement_rule placement_rule;
  rgw_obj obj;
  rgw_raw_obj raw_obj;
  bool is_raw;

public:
  rgw_obj_select() : is_raw(false) {}
  explicit rgw_obj_select(const rgw_obj& _obj) : obj(_obj), is_raw(false) {}
  explicit rgw_obj_select(const rgw_raw_obj& _raw_obj) : raw_obj(_raw_obj), is_raw(true) {}
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

  void set_placement_rule(const rgw_placement_rule& rule) {
    placement_rule = rule;
  }
  void dump(Formatter *f) const;
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

  void decode(bufferlist::const_iterator& bl) {
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

  void decode(bufferlist::const_iterator& bl) {
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

  void decode(bufferlist::const_iterator& bl) {
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

  void decode(bufferlist::const_iterator& bl) {
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
public:
  virtual int handle_data(bufferlist& bl, off_t bl_ofs, off_t bl_len) = 0;
  RGWGetDataCB() {}
  virtual ~RGWGetDataCB() {}
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

  void decode(bufferlist::const_iterator& bl) {
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

  void decode(bufferlist::const_iterator& bl) {
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
  rgw_placement_rule head_placement_rule;

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

  void decode(bufferlist::const_iterator& bl) {
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

  int append(RGWObjManifest& m, const RGWZoneGroup& zonegroup,
             const RGWZoneParams& zone_params);
  int append(RGWObjManifest& m, RGWSI_Zone *zone_svc);

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

  void set_head(const rgw_placement_rule& placement_rule, const rgw_obj& _o, uint64_t _s) {
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

  void set_tail_placement(const rgw_placement_rule& placement_rule, const rgw_bucket& _b) {
    tail_placement.placement_rule = placement_rule;
    tail_placement.bucket = _b;
  }

  const rgw_bucket_placement& get_tail_placement() {
    return tail_placement;
  }

  const rgw_placement_rule& get_head_placement_rule() {
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
    void dump(Formatter *f) const;
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
    int create_begin(CephContext *cct, RGWObjManifest *manifest,
                     const rgw_placement_rule& head_placement_rule,
                     const rgw_placement_rule *tail_placement_rule,
                     const rgw_bucket& bucket,
                     const rgw_obj& obj);

    int create_next(uint64_t ofs);

    rgw_raw_obj get_cur_obj(RGWZoneGroup& zonegroup, RGWZoneParams& zone_params) { return cur_obj.get_raw_obj(zonegroup, zone_params); }
    rgw_raw_obj get_cur_obj(RGWRados *store) const { return cur_obj.get_raw_obj(store); }

    /* total max size of current stripe (including head obj) */
    uint64_t cur_stripe_max_size() const {
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
  void decode(bufferlist::const_iterator& bl) {
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

  void decode(bufferlist::const_iterator& bl) {
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

class RGWDataChangesLog;
class RGWMetaSyncStatusManager;
class RGWDataSyncStatusManager;
class RGWCoroutinesManagerRegistry;

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

class RGWObjectCtx {
  RGWRados *store;
  RWLock lock{"RGWObjectCtx"};
  void *s{nullptr};

  std::map<rgw_obj, RGWObjState> objs_state;
public:
  explicit RGWObjectCtx(RGWRados *_store) : store(_store) {}
  explicit RGWObjectCtx(RGWRados *_store, void *_s) : store(_store), s(_s) {}

  void *get_private() {
    return s;
  }

  RGWRados *get_store() {
    return store;
  }

  RGWObjState *get_state(const rgw_obj& obj) {
    RGWObjState *result;
    typename std::map<rgw_obj, RGWObjState>::iterator iter;
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

  void set_atomic(rgw_obj& obj) {
    RWLock::WLocker wl(lock);
    assert (!obj.empty());
    objs_state[obj].is_atomic = true;
  }
  void set_prefetch_data(const rgw_obj& obj) {
    RWLock::WLocker wl(lock);
    assert (!obj.empty());
    objs_state[obj].prefetch_data = true;
  }

  void invalidate(const rgw_obj& obj) {
    RWLock::WLocker wl(lock);
    auto iter = objs_state.find(obj);
    if (iter == objs_state.end()) {
      return;
    }
    bool is_atomic = iter->second.is_atomic;
    bool prefetch_data = iter->second.prefetch_data;
  
    objs_state.erase(iter);

    if (is_atomic || prefetch_data) {
      auto& state = objs_state[obj];
      state.is_atomic = is_atomic;
      state.prefetch_data = prefetch_data;
    }
  }
};

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
  explicit tombstone_entry(const RGWObjState& state)
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
  friend class RGWReshard;
  friend class RGWBucketReshard;
  friend class RGWBucketReshardLock;
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

  librados::IoCtx root_pool_ctx;      // .rgw

  double inject_notify_timeout_probability = 0;
  unsigned max_notify_retries = 0;

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
  int get_obj_state_impl(RGWObjectCtx *rctx, const RGWBucketInfo& bucket_info, const rgw_obj& obj, RGWObjState **state,
                         bool follow_olh, bool assume_noent = false);
  int append_atomic_test(RGWObjectCtx *rctx, const RGWBucketInfo& bucket_info, const rgw_obj& obj,
                         librados::ObjectOperation& op, RGWObjState **state);
  int append_atomic_test(const RGWObjState* astate, librados::ObjectOperation& op);

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

  RGWQuotaHandler *quota_handler;

  RGWCoroutinesManagerRegistry *cr_registry;

  RGWSyncModuleInstanceRef sync_module;
  bool writeable_zone{false};

  RGWIndexCompletionManager *index_completion_manager{nullptr};

  bool use_cache{false};
public:
  RGWRados(): lock("rados_timer_lock"), timer(NULL),
               gc(NULL), lc(NULL), obj_expirer(NULL), use_gc_thread(false), use_lc_thread(false), quota_threads(false),
               run_sync_thread(false), run_reshard_thread(false), async_rados(nullptr), meta_notifier(NULL),
               data_notifier(NULL), meta_sync_processor_thread(NULL),
               meta_sync_thread_lock("meta_sync_thread_lock"), data_sync_thread_lock("data_sync_thread_lock"),
               bucket_id_lock("rados_bucket_id"),
               bucket_index_max_shards(0),
               max_bucket_id(0), cct(NULL),
               next_rados_handle(0),
               handle_lock("rados_handle_lock"),
               binfo_cache(NULL), obj_tombstone_cache(nullptr),
               pools_initialized(false),
               quota_handler(NULL),
               cr_registry(NULL),
               meta_mgr(NULL), data_log(NULL), reshard(NULL) {}

  RGWRados& set_use_cache(bool status) {
    use_cache = status;
    return *this;
  }

  RGWLC *get_lc() {
    return lc;
  }

  RGWRados& set_run_gc_thread(bool _use_gc_thread) {
    use_gc_thread = _use_gc_thread;
    return *this;
  }

  RGWRados& set_run_lc_thread(bool _use_lc_thread) {
    use_lc_thread = _use_lc_thread;
    return *this;
  }

  RGWRados& set_run_quota_threads(bool _run_quota_threads) {
    quota_threads = _run_quota_threads;
    return *this;
  }

  RGWRados& set_run_sync_thread(bool _run_sync_thread) {
    run_sync_thread = _run_sync_thread;
    return *this;
  }

  RGWRados& set_run_reshard_thread(bool _run_reshard_thread) {
    run_reshard_thread = _run_reshard_thread;
    return *this;
  }

  uint64_t get_new_req_id() {
    return ++max_req_id;
  }

  librados::IoCtx* get_lc_pool_ctx() {
    return &lc_pool_ctx;
  }
  void set_context(CephContext *_cct) {
    cct = _cct;
  }

  RGWServices svc;

  /**
   * AmazonS3 errors contain a HostId string, but is an opaque base64 blob; we
   * try to be more transparent. This has a wrapper so we can update it when zonegroup/zone are changed.
   */
  string host_id;

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
  const RGWSyncModuleInstanceRef& get_sync_module() {
    return sync_module;
  }
  RGWSyncTraceManager *get_sync_tracer() {
    return sync_tracer;
  }

  int get_required_alignment(const rgw_pool& pool, uint64_t *alignment);
  void get_max_aligned_size(uint64_t size, uint64_t alignment, uint64_t *max_size);
  int get_max_chunk_size(const rgw_pool& pool, uint64_t *max_chunk_size, uint64_t *palignment = nullptr);
  int get_max_chunk_size(const rgw_placement_rule& placement_rule, const rgw_obj& obj, uint64_t *max_chunk_size, uint64_t *palignment = nullptr);

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

  CephContext *ctx() { return cct; }
  /** do all necessary setup of the storage device */
  int initialize(CephContext *_cct) {
    set_context(_cct);
    return initialize();
  }
  /** Initialize the RADOS instance and prepare to do other ops */
  int init_svc(bool raw);
  int init_rados();
  int init_complete();
  int initialize();
  void finalize();

  int register_to_service_map(const string& daemon_type, const map<string, string>& meta);
  int update_service_map(std::map<std::string, std::string>&& status);

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
  int read_usage(const rgw_user& user, const string& bucket_name, uint64_t start_epoch, uint64_t end_epoch,
                 uint32_t max_entries, bool *is_truncated, RGWUsageIter& read_iter, map<rgw_user_bucket,
		 rgw_usage_log_entry>& usage);
  int trim_usage(const rgw_user& user, const string& bucket_name, uint64_t start_epoch, uint64_t end_epoch);
  int clear_usage();

  int create_pool(const rgw_pool& pool);

  int init_bucket_index(RGWBucketInfo& bucket_info, int num_shards);
  int clean_bucket_index(RGWBucketInfo& bucket_info, int num_shards);
  void create_bucket_id(string *bucket_id);

  bool get_obj_data_pool(const rgw_placement_rule& placement_rule, const rgw_obj& obj, rgw_pool *pool);
  bool obj_to_raw(const rgw_placement_rule& placement_rule, const rgw_obj& obj, rgw_raw_obj *raw_obj);

  int create_bucket(const RGWUserInfo& owner, rgw_bucket& bucket,
                            const string& zonegroup_id,
                            const rgw_placement_rule& placement_rule,
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

  RGWCoroutinesManagerRegistry *get_cr_registry() { return cr_registry; }

  struct BucketShard {
    RGWRados *store;
    rgw_bucket bucket;
    int shard_id;
    librados::IoCtx index_ctx;
    string bucket_obj;

    explicit BucketShard(RGWRados *_store) : store(_store), shard_id(-1) {}
    int init(const rgw_bucket& _bucket, const rgw_obj& obj, RGWBucketInfo* out);
    int init(const rgw_bucket& _bucket, int sid, RGWBucketInfo* out);
    int init(const RGWBucketInfo& bucket_info, const rgw_obj& obj);
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
        int r =
	  bs.init(bucket_info.bucket, obj, nullptr /* no RGWBucketInfo */);
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
        map<rgw_pool, librados::IoCtx> io_ctxs;
        rgw_pool cur_pool;
        librados::IoCtx *cur_ioctx{nullptr};
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
        std::optional<uint64_t> olh_epoch;
        ceph::real_time delete_at;
        bool canceled;
        const string *user_data;
        rgw_zone_set *zones_trace;
        bool modify_tail;
        bool completeMultipart;

        MetaParams() : mtime(NULL), rmattrs(NULL), data(NULL), manifest(NULL), ptag(NULL),
                 remove_objs(NULL), category(RGWObjCategory::Main), flags(0),
                 if_match(NULL), if_nomatch(NULL), canceled(false), user_data(nullptr), zones_trace(nullptr),
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
      const req_state* get_req_state() {
        return (req_state *)target->get_ctx().get_private();
      }
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
        int r =
	  bs.init(target->get_bucket(), obj, nullptr /* no RGWBucketInfo */);
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
                   const string& storage_class,
                   bufferlist *acl_bl, RGWObjCategory category,
		   list<rgw_obj_index_key> *remove_objs, const string *user_data = nullptr);
      int complete_del(int64_t poolid, uint64_t epoch,
                       ceph::real_time& removed_mtime, /* mtime of removed object */
                       list<rgw_obj_index_key> *remove_objs);
      int cancel();

      const string *get_optag() { return &optag; }

      bool is_prepared() { return prepared; }
    }; // class UpdateIndex

    class List {
    protected:

      RGWRados::Bucket *target;
      rgw_obj_key next_marker;

      int list_objects_ordered(int64_t max,
			       vector<rgw_bucket_dir_entry> *result,
			       map<string, bool> *common_prefixes,
			       bool *is_truncated);
      int list_objects_unordered(int64_t max,
				 vector<rgw_bucket_dir_entry> *result,
				 map<string, bool> *common_prefixes,
				 bool *is_truncated);

    public:

      struct Params {
        string prefix;
        string delim;
        rgw_obj_key marker;
        rgw_obj_key end_marker;
        string ns;
        bool enforce_ns;
        RGWAccessListFilter *filter;
        bool list_versions;
	bool allow_unordered;

        Params() :
	  enforce_ns(true),
	  filter(NULL),
	  list_versions(false),
	  allow_unordered(false)
	{}
      } params;

      explicit List(RGWRados::Bucket *_target) : target(_target) {}

      int list_objects(int64_t max,
		       vector<rgw_bucket_dir_entry> *result,
		       map<string, bool> *common_prefixes,
		       bool *is_truncated) {
	if (params.allow_unordered) {
	  return list_objects_unordered(max, result, common_prefixes,
					is_truncated);
	} else {
	  return list_objects_ordered(max, result, common_prefixes,
				      is_truncated);
	}
      }
      rgw_obj_key& get_next_marker() {
        return next_marker;
      }
    }; // class List
  }; // class Bucket

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
  int swift_versioning_restore(RGWSysObjectCtx& sysobj_ctx,
                               RGWObjectCtx& obj_ctx,           /* in/out */
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

  int rewrite_obj(RGWBucketInfo& dest_bucket_info, const rgw_obj& obj);

  int stat_remote_obj(RGWObjectCtx& obj_ctx,
               const rgw_user& user_id,
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
               map<string, string> *pheaders,
               string *version_id,
               string *ptag,
               string *petag);

  int fetch_remote_obj(RGWObjectCtx& obj_ctx,
                       const rgw_user& user_id,
                       req_info *info,
                       const string& source_zone,
                       const rgw_obj& dest_obj,
                       const rgw_obj& src_obj,
                       RGWBucketInfo& dest_bucket_info,
                       RGWBucketInfo& src_bucket_info,
		       std::optional<rgw_placement_rule> dest_placement,
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
                       std::optional<uint64_t> olh_epoch,
		       ceph::real_time delete_at,
                       string *ptag,
                       string *petag,
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
               req_info *info,
               const string& source_zone,
               rgw_obj& dest_obj,
               rgw_obj& src_obj,
               RGWBucketInfo& dest_bucket_info,
               RGWBucketInfo& src_bucket_info,
               const rgw_placement_rule& dest_placement,
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
               string *petag,
               void (*progress_cb)(off_t, void *),
               void *progress_data);

  int copy_obj_data(RGWObjectCtx& obj_ctx,
               RGWBucketInfo& dest_bucket_info,
               const rgw_placement_rule& dest_placement,
	       RGWRados::Object::Read& read_op, off_t end,
               const rgw_obj& dest_obj,
	       ceph::real_time *mtime,
	       ceph::real_time set_mtime,
               map<string, bufferlist>& attrs,
               uint64_t olh_epoch,
	       ceph::real_time delete_at,
               string *petag);
  
  int transition_obj(RGWObjectCtx& obj_ctx,
                     RGWBucketInfo& bucket_info,
                     rgw_obj& obj,
                     const rgw_placement_rule& placement_rule,
                     const real_time& mtime,
                     uint64_t olh_epoch);

  int check_bucket_empty(RGWBucketInfo& bucket_info);

  /**
   * Delete a bucket.
   * bucket: the name of the bucket to delete
   * Returns 0 on success, -ERR# otherwise.
   */
  int delete_bucket(RGWBucketInfo& bucket_info, RGWObjVersionTracker& objv_tracker, bool check_empty = true);

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

  int delete_raw_obj(const rgw_raw_obj& obj);

  /** Remove an object from the bucket index */
  int delete_obj_index(const rgw_obj& obj);

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

  int get_obj_state(RGWObjectCtx *rctx, const RGWBucketInfo& bucket_info, const rgw_obj& obj, RGWObjState **state,
                    bool follow_olh, bool assume_noent = false);
  int get_obj_state(RGWObjectCtx *rctx, const RGWBucketInfo& bucket_info, const rgw_obj& obj, RGWObjState **state) {
    return get_obj_state(rctx, bucket_info, obj, state, true);
  }

  using iterate_obj_cb = int (*)(const rgw_raw_obj&, off_t, off_t,
                                 off_t, bool, RGWObjState*, void*);

  int iterate_obj(RGWObjectCtx& ctx, const RGWBucketInfo& bucket_info,
                  const rgw_obj& obj, off_t ofs, off_t end,
                  uint64_t max_chunk_size, iterate_obj_cb cb, void *arg);

  int flush_read_list(struct get_obj_data *d);

  int get_obj_iterate_cb(const rgw_raw_obj& read_obj, off_t obj_ofs,
                         off_t read_ofs, off_t len, bool is_head_obj,
                         RGWObjState *astate, void *arg);

  void get_obj_aio_completion_cb(librados::completion_t cb, void *arg);

  /**
   * a simple object read without keeping state
   */

  int raw_obj_stat(rgw_raw_obj& obj, uint64_t *psize, ceph::real_time *pmtime, uint64_t *epoch,
                   map<string, bufferlist> *attrs, bufferlist *first_chunk,
                   RGWObjVersionTracker *objv_tracker);

  int obj_operate(const RGWBucketInfo& bucket_info, const rgw_obj& obj, librados::ObjectWriteOperation *op);
  int obj_operate(const RGWBucketInfo& bucket_info, const rgw_obj& obj, librados::ObjectReadOperation *op);

  int guard_reshard(BucketShard *bs,
		    const rgw_obj& obj_instance,
		    const RGWBucketInfo& bucket_info,
		    std::function<int(BucketShard *)> call);
  int block_while_resharding(RGWRados::BucketShard *bs,
			     string *new_bucket_id,
			     const RGWBucketInfo& bucket_info,
                             optional_yield y);

  void bucket_index_guard_olh_op(RGWObjState& olh_state, librados::ObjectOperation& op);
  int olh_init_modification(const RGWBucketInfo& bucket_info, RGWObjState& state, const rgw_obj& olh_obj, string *op_tag);
  int olh_init_modification_impl(const RGWBucketInfo& bucket_info, RGWObjState& state, const rgw_obj& olh_obj, string *op_tag);
  int bucket_index_link_olh(const RGWBucketInfo& bucket_info, RGWObjState& olh_state,
                            const rgw_obj& obj_instance, bool delete_marker,
                            const string& op_tag, struct rgw_bucket_dir_entry_meta *meta,
                            uint64_t olh_epoch,
                            ceph::real_time unmod_since, bool high_precision_time,
                            rgw_zone_set *zones_trace = nullptr,
                            bool log_data_change = false);
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
              uint64_t olh_epoch, ceph::real_time unmod_since, bool high_precision_time,
              rgw_zone_set *zones_trace = nullptr, bool log_data_change = false);
  int repair_olh(RGWObjState* state, const RGWBucketInfo& bucket_info,
                 const rgw_obj& obj);
  int unlink_obj_instance(RGWObjectCtx& obj_ctx, RGWBucketInfo& bucket_info, const rgw_obj& target_obj,
                          uint64_t olh_epoch, rgw_zone_set *zones_trace = nullptr);

  void check_pending_olh_entries(map<string, bufferlist>& pending_entries, map<string, bufferlist> *rm_pending_entries);
  int remove_olh_pending_entries(const RGWBucketInfo& bucket_info, RGWObjState& state, const rgw_obj& olh_obj, map<string, bufferlist>& pending_attrs);
  int follow_olh(const RGWBucketInfo& bucket_info, RGWObjectCtx& ctx, RGWObjState *state, const rgw_obj& olh_obj, rgw_obj *target);
  int get_olh(const RGWBucketInfo& bucket_info, const rgw_obj& obj, RGWOLHInfo *olh);

  void gen_rand_obj_instance_name(rgw_obj_key *target_key);
  void gen_rand_obj_instance_name(rgw_obj *target);

  int update_containers_stats(map<string, RGWBucketEnt>& m);
  int append_async(rgw_raw_obj& obj, size_t size, bufferlist& bl);

public:
  void set_atomic(void *ctx, rgw_obj& obj) {
    RGWObjectCtx *rctx = static_cast<RGWObjectCtx *>(ctx);
    rctx->set_atomic(obj);
  }
  void set_prefetch_data(void *ctx, const rgw_obj& obj) {
    RGWObjectCtx *rctx = static_cast<RGWObjectCtx *>(ctx);
    rctx->set_prefetch_data(obj);
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
  int get_bucket_entrypoint_info(RGWSysObjectCtx& obj_ctx, const string& tenant_name, const string& bucket_name,
                                 RGWBucketEntryPoint& entry_point, RGWObjVersionTracker *objv_tracker,
                                 ceph::real_time *pmtime, map<string, bufferlist> *pattrs, rgw_cache_entry_info *cache_info = NULL,
				 boost::optional<obj_version> refresh_version = boost::none);
  int get_bucket_instance_info(RGWSysObjectCtx& obj_ctx, const string& meta_key, RGWBucketInfo& info, ceph::real_time *pmtime, map<string, bufferlist> *pattrs);
  int get_bucket_instance_info(RGWSysObjectCtx& obj_ctx, const rgw_bucket& bucket, RGWBucketInfo& info, ceph::real_time *pmtime, map<string, bufferlist> *pattrs);
  int get_bucket_instance_from_oid(RGWSysObjectCtx& obj_ctx, const string& oid, RGWBucketInfo& info, ceph::real_time *pmtime, map<string, bufferlist> *pattrs,
                                   rgw_cache_entry_info *cache_info = NULL,
				   boost::optional<obj_version> refresh_version = boost::none);

  int convert_old_bucket_info(RGWSysObjectCtx& obj_ctx, const string& tenant_name, const string& bucket_name);
  static void make_bucket_entry_name(const string& tenant_name, const string& bucket_name, string& bucket_entry);


private:
  int _get_bucket_info(RGWSysObjectCtx& obj_ctx, const string& tenant,
		       const string& bucket_name, RGWBucketInfo& info,
		       real_time *pmtime,
		       map<string, bufferlist> *pattrs,
		       boost::optional<obj_version> refresh_version);
public:

  bool call(std::string_view command, const cmdmap_t& cmdmap,
	    std::string_view format,
	    bufferlist& out) override final;

protected:
  // `call_list` must iterate over all cache entries and call
  // `cache_list_dump_helper` with the supplied Formatter on any that
  // include `filter` as a substring.
  //
  void call_list(const std::optional<std::string>& filter,
			 Formatter* format);
  // `call_inspect` must look up the requested target and, if found,
  // dump it to the supplied Formatter and return true. If not found,
  // it must return false.
  //
  bool call_inspect(const std::string& target, Formatter* format);

  // `call_erase` must erase the requested target and return true. If
  // the requested target does not exist, it should return false.
  bool call_erase(const std::string& target);

  // `call_zap` must erase the cache.
  void call_zap();
public:

  int get_bucket_info(RGWSysObjectCtx& obj_ctx,
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

  int cls_obj_prepare_op(BucketShard& bs, RGWModifyOp op, string& tag, rgw_obj& obj, uint16_t bilog_flags, rgw_zone_set *zones_trace = nullptr);
  int cls_obj_complete_op(BucketShard& bs, const rgw_obj& obj, RGWModifyOp op, string& tag, int64_t pool, uint64_t epoch,
                          rgw_bucket_dir_entry& ent, RGWObjCategory category, list<rgw_obj_index_key> *remove_objs, uint16_t bilog_flags, rgw_zone_set *zones_trace = nullptr);
  int cls_obj_complete_add(BucketShard& bs, const rgw_obj& obj, string& tag, int64_t pool, uint64_t epoch, rgw_bucket_dir_entry& ent,
                           RGWObjCategory category, list<rgw_obj_index_key> *remove_objs, uint16_t bilog_flags, rgw_zone_set *zones_trace = nullptr);
  int cls_obj_complete_del(BucketShard& bs, string& tag, int64_t pool, uint64_t epoch, rgw_obj& obj,
                           ceph::real_time& removed_mtime, list<rgw_obj_index_key> *remove_objs, uint16_t bilog_flags, rgw_zone_set *zones_trace = nullptr);
  int cls_obj_complete_cancel(BucketShard& bs, string& tag, rgw_obj& obj, uint16_t bilog_flags, rgw_zone_set *zones_trace = nullptr);
  int cls_obj_set_bucket_tag_timeout(RGWBucketInfo& bucket_info, uint64_t timeout);
  int cls_bucket_list_ordered(RGWBucketInfo& bucket_info, int shard_id,
			      rgw_obj_index_key& start, const string& prefix,
			      uint32_t num_entries, bool list_versions,
			      map<string, rgw_bucket_dir_entry>& m,
			      bool *is_truncated,
			      rgw_obj_index_key *last_entry,
			      bool (*force_check_filter)(const string& name) = nullptr);
  int cls_bucket_list_unordered(RGWBucketInfo& bucket_info, int shard_id,
				rgw_obj_index_key& start, const string& prefix,
				uint32_t num_entries, bool list_versions,
				vector<rgw_bucket_dir_entry>& ent_list,
				bool *is_truncated, rgw_obj_index_key *last_entry,
				bool (*force_check_filter)(const string& name) = nullptr);
  int cls_bucket_head(const RGWBucketInfo& bucket_info, int shard_id, vector<rgw_bucket_dir_header>& headers, map<int, string> *bucket_instance_ids = NULL);
  int cls_bucket_head_async(const RGWBucketInfo& bucket_info, int shard_id, RGWGetDirHeader_CB *ctx, int *num_aio);
  int list_bi_log_entries(RGWBucketInfo& bucket_info, int shard_id, string& marker, uint32_t max, std::list<rgw_bi_log_entry>& result, bool *truncated);
  int trim_bi_log_entries(RGWBucketInfo& bucket_info, int shard_id, string& marker, string& end_marker);
  int resync_bi_log_entries(RGWBucketInfo& bucket_info, int shard_id);
  int stop_bi_log_entries(RGWBucketInfo& bucket_info, int shard_id);
  int get_bi_log_status(RGWBucketInfo& bucket_info, int shard_id, map<int, string>& max_marker);

  int bi_get_instance(const RGWBucketInfo& bucket_info, const rgw_obj& obj, rgw_bucket_dir_entry *dirent);
  int bi_get_olh(const RGWBucketInfo& bucket_info, const rgw_obj& obj, rgw_bucket_olh_entry *olh);
  int bi_get(const RGWBucketInfo& bucket_info, const rgw_obj& obj, BIIndexType index_type, rgw_cls_bi_entry *entry);
  void bi_put(librados::ObjectWriteOperation& op, BucketShard& bs, rgw_cls_bi_entry& entry);
  int bi_put(BucketShard& bs, rgw_cls_bi_entry& entry);
  int bi_put(rgw_bucket& bucket, rgw_obj& obj, rgw_cls_bi_entry& entry);
  int bi_list(rgw_bucket& bucket, int shard_id, const string& filter_obj, const string& marker, uint32_t max, list<rgw_cls_bi_entry> *entries, bool *is_truncated);
  int bi_list(BucketShard& bs, const string& filter_obj, const string& marker, uint32_t max, list<rgw_cls_bi_entry> *entries, bool *is_truncated);
  int bi_list(rgw_bucket& bucket, const string& obj_name, const string& marker, uint32_t max,
              list<rgw_cls_bi_entry> *entries, bool *is_truncated);
  int bi_remove(BucketShard& bs);

  int cls_obj_usage_log_add(const string& oid, rgw_usage_log_info& info);
  int cls_obj_usage_log_read(const string& oid, const string& user, const string& bucket, uint64_t start_epoch,
                             uint64_t end_epoch, uint32_t max_entries, string& read_iter, map<rgw_user_bucket,
			     rgw_usage_log_entry>& usage, bool *is_truncated);
  int cls_obj_usage_log_trim(const string& oid, const string& user, const string& bucket, uint64_t start_epoch,
                             uint64_t end_epoch);
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

  int lock_exclusive(const rgw_pool& pool, const string& oid, ceph::timespan& duration, string& zone_id, string& owner_id);
  int unlock(const rgw_pool& pool, const string& oid, string& zone_id, string& owner_id);

  void update_gc_chain(rgw_obj& head_obj, RGWObjManifest& manifest, cls_rgw_obj_chain *chain);
  int send_chain_to_gc(cls_rgw_obj_chain& chain, const string& tag, bool sync);
  int gc_operate(string& oid, librados::ObjectWriteOperation *op);
  int gc_aio_operate(string& oid, librados::ObjectWriteOperation *op, librados::AioCompletion **pc = nullptr);
  int gc_operate(string& oid, librados::ObjectReadOperation *op, bufferlist *pbl);

  int list_gc_objs(int *index, string& marker, uint32_t max, bool expired_only, std::list<cls_rgw_gc_obj_info>& result, bool *truncated);
  int process_gc(bool expired_only);
  bool process_expire_objects();
  int defer_gc(void *ctx, const RGWBucketInfo& bucket_info, const rgw_obj& obj);

  int process_lc();
  int list_lc_progress(const string& marker, uint32_t max_entries, map<string, int> *progress_map);
  
  int bucket_check_index(RGWBucketInfo& bucket_info,
                         map<RGWObjCategory, RGWStorageStats> *existing_stats,
                         map<RGWObjCategory, RGWStorageStats> *calculated_stats);
  int bucket_rebuild_index(RGWBucketInfo& bucket_info);
  int bucket_set_reshard(const RGWBucketInfo& bucket_info, const cls_rgw_bucket_instance_entry& entry);
  int remove_objs_from_index(RGWBucketInfo& bucket_info, list<rgw_obj_index_key>& oid_list);
  int move_rados_obj(librados::IoCtx& src_ioctx,
		     const string& src_oid, const string& src_locator,
	             librados::IoCtx& dst_ioctx,
		     const string& dst_oid, const string& dst_locator);
  int fix_head_obj_locator(const RGWBucketInfo& bucket_info, bool copy_obj, bool remove_bad, rgw_obj_key& key);
  int fix_tail_obj_locator(const RGWBucketInfo& bucket_info, rgw_obj_key& key, bool fix, bool *need_fix);

  int cls_user_get_header(const string& user_id, cls_user_header *header);
  int cls_user_reset_stats(const string& user_id);
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
                  RGWQuotaInfo& user_quota, RGWQuotaInfo& bucket_quota, uint64_t obj_size, bool check_size_only = false);

  int check_bucket_shards(const RGWBucketInfo& bucket_info, const rgw_bucket& bucket,
			  RGWQuotaInfo& bucket_quota);

  int add_bucket_to_reshard(const RGWBucketInfo& bucket_info, uint32_t new_num_shards);

  uint64_t instance_id();

  librados::Rados* get_rados_handle();

  int delete_raw_obj_aio(const rgw_raw_obj& obj, list<librados::AioCompletion *>& handles);
  int delete_obj_aio(const rgw_obj& obj, RGWBucketInfo& info, RGWObjState *astate,
                     list<librados::AioCompletion *>& handles, bool keep_index_consistent);

  /* mfa/totp stuff */
 private:
  void prepare_mfa_write(librados::ObjectWriteOperation *op,
                         RGWObjVersionTracker *objv_tracker,
                         const ceph::real_time& mtime);
 public:
  string get_mfa_oid(const rgw_user& user);
  int get_mfa_ref(const rgw_user& user, rgw_rados_ref *ref);
  int check_mfa(const rgw_user& user, const string& otp_id, const string& pin);
  int create_mfa(const rgw_user& user, const rados::cls::otp::otp_info_t& config,
                 RGWObjVersionTracker *objv_tracker, const ceph::real_time& mtime);
  int remove_mfa(const rgw_user& user, const string& id,
                 RGWObjVersionTracker *objv_tracker, const ceph::real_time& mtime);
  int get_mfa(const rgw_user& user, const string& id, rados::cls::otp::otp_info_t *result);
  int list_mfa(const rgw_user& user, list<rados::cls::otp::otp_info_t> *result);
  int otp_get_current_time(const rgw_user& user, ceph::real_time *result);

  /* mfa interfaces used by metadata engine */
  int set_mfa(const string& oid, const list<rados::cls::otp::otp_info_t>& entries, bool reset_obj,
              RGWObjVersionTracker *objv_tracker, const ceph::real_time& mtime);
  int list_mfa(const string& oid, list<rados::cls::otp::otp_info_t> *result,
               RGWObjVersionTracker *objv_tracker, ceph::real_time *pmtime);
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
  static RGWRados *get_storage(CephContext *cct, bool use_gc_thread, bool use_lc_thread, bool quota_threads,
			       bool run_sync_thread, bool run_reshard_thread, bool use_cache = true) {
    RGWRados *store = init_storage_provider(cct, use_gc_thread, use_lc_thread, quota_threads, run_sync_thread,
					    run_reshard_thread, use_cache);
    return store;
  }
  static RGWRados *get_raw_storage(CephContext *cct) {
    RGWRados *store = init_raw_storage_provider(cct);
    return store;
  }
  static RGWRados *init_storage_provider(CephContext *cct, bool use_gc_thread, bool use_lc_thread, bool quota_threads, bool run_sync_thread, bool run_reshard_thread, bool use_metadata_cache);
  static RGWRados *init_raw_storage_provider(CephContext *cct);
  static void close_storage(RGWRados *store);

};

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
