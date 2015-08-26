#ifndef CEPH_RGWRADOS_H
#define CEPH_RGWRADOS_H

#include "include/rados/librados.hpp"
#include "include/Context.h"
#include "common/RefCountedObj.h"
#include "rgw_common.h"
#include "cls/rgw/cls_rgw_types.h"
#include "cls/version/cls_version_types.h"
#include "cls/log/cls_log_types.h"
#include "cls/statelog/cls_statelog_types.h"
#include "rgw_log.h"
#include "rgw_metadata.h"
#include "rgw_rest_conn.h"

class RGWWatcher;
class SafeTimer;
class ACLOwner;
class RGWGC;

/* flags for put_obj_meta() */
#define PUT_OBJ_CREATE      0x01
#define PUT_OBJ_EXCL        0x02
#define PUT_OBJ_CREATE_EXCL (PUT_OBJ_CREATE | PUT_OBJ_EXCL)

#define RGW_OBJ_NS_MULTIPART "multipart"
#define RGW_OBJ_NS_SHADOW    "shadow"

#define RGW_BUCKET_INSTANCE_MD_PREFIX ".bucket.meta."

static inline void prepend_bucket_marker(rgw_bucket& bucket, const string& orig_oid, string& oid)
{
  if (bucket.marker.empty() || orig_oid.empty()) {
    oid = orig_oid;
  } else {
    oid = bucket.marker;
    oid.append("_");
    oid.append(orig_oid);
  }
}

static inline void get_obj_bucket_and_oid_key(const rgw_obj& obj, rgw_bucket& bucket, string& oid, string& key)
{
  bucket = obj.bucket;
  prepend_bucket_marker(bucket, obj.object, oid);
  prepend_bucket_marker(bucket, obj.key, key);
}

int rgw_policy_from_attrset(CephContext *cct, map<string, bufferlist>& attrset, RGWAccessControlPolicy *policy);

struct RGWUsageBatch {
  map<utime_t, rgw_usage_log_entry> m;

  void insert(utime_t& t, rgw_usage_log_entry& entry, bool *account) {
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
  rgw_obj loc;       /* the object where the data is located */
  uint64_t loc_ofs;  /* the offset at that object where the data is located */
  uint64_t size;     /* the part size */

  RGWObjManifestPart() : loc_ofs(0), size(0) {}

  void encode(bufferlist& bl) const {
    ENCODE_START(2, 2, bl);
    ::encode(loc, bl);
    ::encode(loc_ofs, bl);
    ::encode(size, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::iterator& bl) {
     DECODE_START_LEGACY_COMPAT_LEN_32(2, 2, 2, bl);
     ::decode(loc, bl);
     ::decode(loc_ofs, bl);
     ::decode(size, bl);
     DECODE_FINISH(bl);
  }

  void dump(Formatter *f) const;
  static void generate_test_instances(list<RGWObjManifestPart*>& o);
};
WRITE_CLASS_ENCODER(RGWObjManifestPart);

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
    ::encode(start_part_num, bl);
    ::encode(start_ofs, bl);
    ::encode(part_size, bl);
    ::encode(stripe_max_size, bl);
    ::encode(override_prefix, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::iterator& bl) {
    DECODE_START(2, bl);
    ::decode(start_part_num, bl);
    ::decode(start_ofs, bl);
    ::decode(part_size, bl);
    ::decode(stripe_max_size, bl);
    if (struct_v >= 2)
      ::decode(override_prefix, bl);
    DECODE_FINISH(bl);
  }
  void dump(Formatter *f) const;
};
WRITE_CLASS_ENCODER(RGWObjManifestRule);

class RGWObjManifest {
protected:
  bool explicit_objs; /* old manifest? */
  map<uint64_t, RGWObjManifestPart> objs;

  uint64_t obj_size;

  rgw_obj head_obj;
  uint64_t head_size;

  uint64_t max_head_size;
  string prefix;
  rgw_bucket tail_bucket; /* might be different than the original bucket,
                             as object might have been copied across buckets */
  map<uint64_t, RGWObjManifestRule> rules;

  void convert_to_explicit();
  int append_explicit(RGWObjManifest& m);
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
    head_obj = rhs.head_obj;
    head_size = rhs.head_size;
    max_head_size = rhs.max_head_size;
    prefix = rhs.prefix;
    tail_bucket = rhs.tail_bucket;
    rules = rhs.rules;

    begin_iter.set_manifest(this);
    end_iter.set_manifest(this);

    begin_iter.seek(rhs.begin_iter.get_ofs());
    end_iter.seek(rhs.end_iter.get_ofs());

    return *this;
  }


  void set_explicit(uint64_t _size, map<uint64_t, RGWObjManifestPart>& _objs) {
    explicit_objs = true;
    obj_size = _size;
    objs.swap(_objs);
  }

  void get_implicit_location(uint64_t cur_part_id, uint64_t cur_stripe, uint64_t ofs, string *override_prefix, rgw_obj *location);

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
    ENCODE_START(4, 3, bl);
    ::encode(obj_size, bl);
    ::encode(objs, bl);
    ::encode(explicit_objs, bl);
    ::encode(head_obj, bl);
    ::encode(head_size, bl);
    ::encode(max_head_size, bl);
    ::encode(prefix, bl);
    ::encode(rules, bl);
    ::encode(tail_bucket, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::iterator& bl) {
    DECODE_START_LEGACY_COMPAT_LEN_32(4, 2, 2, bl);
    ::decode(obj_size, bl);
    ::decode(objs, bl);
    if (struct_v >= 3) {
      ::decode(explicit_objs, bl);
      ::decode(head_obj, bl);
      ::decode(head_size, bl);
      ::decode(max_head_size, bl);
      ::decode(prefix, bl);
      ::decode(rules, bl);
    } else {
      explicit_objs = true;
      if (!objs.empty()) {
        map<uint64_t, RGWObjManifestPart>::iterator iter = objs.begin();
        head_obj = iter->second.loc;
        head_size = iter->second.size;
        max_head_size = head_size;
      }
    }

    if (struct_v >= 4) {
      ::decode(tail_bucket, bl);
    }

    update_iterators();
    DECODE_FINISH(bl);
  }

  void dump(Formatter *f) const;
  static void generate_test_instances(list<RGWObjManifest*>& o);

  int append(RGWObjManifest& m);

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
        rgw_obj& obj = iter->second.loc;
        return head_obj.object != obj.object;
      }
      return (objs.size() >= 2);
    }
    return (obj_size > head_size);
  }

  void set_head(const rgw_obj& _o) {
    head_obj = _o;
  }

  const rgw_obj& get_head() {
    return head_obj;
  }

  void set_tail_bucket(const rgw_bucket& _b) {
    tail_bucket = _b;
  }

  rgw_bucket& get_tail_bucket() {
    return tail_bucket;
  }

  void set_prefix(const string& _p) {
    prefix = _p;
  }

  const string& get_prefix() {
    return prefix;
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

    rgw_obj location;

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
    obj_iterator(RGWObjManifest *_m) : manifest(_m) {
      init();
      seek(0);
    }
    obj_iterator(RGWObjManifest *_m, uint64_t _ofs) : manifest(_m) {
      init();
      seek(_ofs);
    }
    void seek(uint64_t ofs);

    void operator++();
    bool operator==(const obj_iterator& rhs) {
      return (ofs == rhs.ofs);
    }
    bool operator!=(const obj_iterator& rhs) {
      return (ofs != rhs.ofs);
    }
    const rgw_obj& get_location() {
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

    rgw_obj cur_obj;
    rgw_bucket bucket;


    RGWObjManifestRule rule;

  public:
    generator() : manifest(NULL), last_ofs(0), cur_part_ofs(0), cur_part_id(0), 
		  cur_stripe(0), cur_stripe_size(0) {}
    int create_begin(CephContext *cct, RGWObjManifest *manifest, rgw_bucket& bucket, rgw_obj& head);

    int create_next(uint64_t ofs);

    const rgw_obj& get_cur_obj() { return cur_obj; }

    /* total max size of current stripe (including head obj) */
    uint64_t cur_stripe_max_size() {
      return cur_stripe_size;
    }
  };
};
WRITE_CLASS_ENCODER(RGWObjManifest);

struct RGWUploadPartInfo {
  uint32_t num;
  uint64_t size;
  string etag;
  utime_t modified;
  RGWObjManifest manifest;

  RGWUploadPartInfo() : num(0), size(0) {}

  void encode(bufferlist& bl) const {
    ENCODE_START(3, 2, bl);
    ::encode(num, bl);
    ::encode(size, bl);
    ::encode(etag, bl);
    ::encode(modified, bl);
    ::encode(manifest, bl);
    ENCODE_FINISH(bl);
  }
  void decode(bufferlist::iterator& bl) {
    DECODE_START_LEGACY_COMPAT_LEN(3, 2, 2, bl);
    ::decode(num, bl);
    ::decode(size, bl);
    ::decode(etag, bl);
    ::decode(modified, bl);
    if (struct_v >= 3)
      ::decode(manifest, bl);
    DECODE_FINISH(bl);
  }
  void dump(Formatter *f) const;
  static void generate_test_instances(list<RGWUploadPartInfo*>& o);
};
WRITE_CLASS_ENCODER(RGWUploadPartInfo)

class RGWPutObjProcessor
{
protected:
  RGWRados *store;
  void *obj_ctx;
  bool is_complete;
  string bucket_owner;

  virtual int do_complete(string& etag, time_t *mtime, time_t set_mtime, map<string, bufferlist>& attrs) = 0;

  list<rgw_obj> objs;

  void add_obj(const rgw_obj& obj) {
    objs.push_back(obj);
  }
public:
  RGWPutObjProcessor(const string& _bo) : store(NULL), obj_ctx(NULL), is_complete(false), bucket_owner(_bo) {}
  virtual ~RGWPutObjProcessor();
  virtual int prepare(RGWRados *_store, void *_o, string *oid_rand) {
    store = _store;
    obj_ctx = _o;
    return 0;
  };
  virtual int handle_data(bufferlist& bl, off_t ofs, MD5 *hash, void **phandle, bool *again) = 0;
  virtual int throttle_data(void *handle, bool need_to_wait) = 0;
  virtual void complete_hash(MD5 *hash) {
    assert(0);
  }
  virtual int complete(string& etag, time_t *mtime, time_t set_mtime, map<string, bufferlist>& attrs);

  CephContext *ctx();
};

class RGWPutObjProcessor_Plain : public RGWPutObjProcessor
{
  rgw_bucket bucket;
  string obj_str;

  bufferlist data;
  rgw_obj obj;
  off_t ofs;

protected:
  int prepare(RGWRados *store, void *obj_ctx, string *oid_rand);
  int handle_data(bufferlist& bl, off_t ofs, MD5 *hash /* NULL expected */, void **phandle, bool *again);
  int do_complete(string& etag, time_t *mtime, time_t set_mtime, map<string, bufferlist>& attrs);

public:
  int throttle_data(void *handle, bool need_to_wait) { return 0; }
  RGWPutObjProcessor_Plain(const string& bucket_owner, rgw_bucket& b, const string& o) : RGWPutObjProcessor(bucket_owner),
                                                                                         bucket(b), obj_str(o), ofs(0) {}
};

struct put_obj_aio_info {
  void *handle;
};

class RGWPutObjProcessor_Aio : public RGWPutObjProcessor
{
  list<struct put_obj_aio_info> pending;
  size_t max_chunks;

  struct put_obj_aio_info pop_pending();
  int wait_pending_front();
  bool pending_has_completed();

protected:
  uint64_t obj_len;

  int drain_pending();
  int handle_obj_data(rgw_obj& obj, bufferlist& bl, off_t ofs, off_t abs_ofs, void **phandle, bool exclusive);

public:
  int throttle_data(void *handle, bool need_to_wait);

  RGWPutObjProcessor_Aio(const string& bucket_owner) : RGWPutObjProcessor(bucket_owner), max_chunks(RGW_MAX_PENDING_CHUNKS), obj_len(0) {}
  virtual ~RGWPutObjProcessor_Aio() {
    drain_pending();
  }
};

class RGWPutObjProcessor_Atomic : public RGWPutObjProcessor_Aio
{
  bufferlist first_chunk;
  uint64_t part_size;
  off_t cur_part_ofs;
  off_t next_part_ofs;
  int cur_part_id;
  off_t data_ofs;

  uint64_t extra_data_len;
  bufferlist extra_data_bl;
  bufferlist pending_data_bl;
  uint64_t max_chunk_size;

protected:
  rgw_bucket bucket;
  string obj_str;

  string unique_tag;

  rgw_obj head_obj;
  rgw_obj cur_obj;
  RGWObjManifest manifest;
  RGWObjManifest::generator manifest_gen;

  int write_data(bufferlist& bl, off_t ofs, void **phandle, bool exclusive);
  virtual int do_complete(string& etag, time_t *mtime, time_t set_mtime, map<string, bufferlist>& attrs);

  int prepare_next_part(off_t ofs);
  int complete_parts();
  int complete_writing_data();

  int prepare_init(RGWRados *store, void *obj_ctx, string *oid_rand);

public:
  ~RGWPutObjProcessor_Atomic() {}
  RGWPutObjProcessor_Atomic(const string& bucket_owner, rgw_bucket& _b, const string& _o, uint64_t _p, const string& _t) :
                                RGWPutObjProcessor_Aio(bucket_owner),
                                part_size(_p),
                                cur_part_ofs(0),
                                next_part_ofs(_p),
                                cur_part_id(0),
                                data_ofs(0),
                                extra_data_len(0),
                                max_chunk_size(0),
                                bucket(_b),
                                obj_str(_o),
                                unique_tag(_t) {}
  int prepare(RGWRados *store, void *obj_ctx, string *oid_rand);
  virtual bool immutable_head() { return false; }
  void set_extra_data_len(uint64_t len) {
    extra_data_len = len;
  }
  virtual int handle_data(bufferlist& bl, off_t ofs, MD5 *hash, void **phandle, bool *again);
  virtual void complete_hash(MD5 *hash);
  bufferlist& get_extra_data() { return extra_data_bl; }
};


struct RGWObjState {
  bool is_atomic;
  bool has_attrs;
  bool exists;
  uint64_t size;
  time_t mtime;
  uint64_t epoch;
  bufferlist obj_tag;
  string write_tag;
  bool fake_tag;
  RGWObjManifest manifest;
  bool has_manifest;
  string shadow_obj;
  bool has_data;
  bufferlist data;
  bool prefetch_data;
  bool keep_tail;
  RGWObjVersionTracker objv_tracker;

  map<string, bufferlist> attrset;
  RGWObjState() : is_atomic(false), has_attrs(0), exists(false),
                  size(0), mtime(0), epoch(0), fake_tag(false), has_manifest(false),
                  has_data(false), prefetch_data(false), keep_tail(false) {}

  bool get_attr(string name, bufferlist& dest) {
    map<string, bufferlist>::iterator iter = attrset.find(name);
    if (iter != attrset.end()) {
      dest = iter->second;
      return true;
    }
    return false;
  }

  void clear() {
    has_attrs = false;
    exists = false;
    fake_tag = false;
    epoch = 0;
    size = 0;
    mtime = 0;
    obj_tag.clear();
    shadow_obj.clear();
    attrset.clear();
    data.clear();
  }
};

struct RGWRadosCtx {
  RGWRados *store;
  map<rgw_obj, RGWObjState> objs_state;
  int (*intent_cb)(RGWRados *store, void *user_ctx, rgw_obj& obj, RGWIntentEvent intent);
  void *user_ctx;

  RGWRadosCtx(RGWRados *_store) : store(_store), intent_cb(NULL), user_ctx(NULL) { }
  RGWRadosCtx(RGWRados *_store, void *_user_ctx) : store(_store), intent_cb(NULL), user_ctx(_user_ctx) { }

  RGWObjState *get_state(rgw_obj& obj);
  void set_atomic(rgw_obj& obj);
  void set_prefetch_data(rgw_obj& obj);

  void set_intent_cb(int (*cb)(RGWRados *store, void *user_ctx, rgw_obj& obj, RGWIntentEvent intent)) {
    intent_cb = cb;
  }

  int notify_intent(RGWRados *store, rgw_obj& obj, RGWIntentEvent intent) {
    if (intent_cb) {
      return intent_cb(store, user_ctx, obj, intent);
    }
    return 0;
  }
};

struct RGWPoolIterCtx {
  librados::IoCtx io_ctx;
  librados::ObjectIterator iter;
};

struct RGWListRawObjsCtx {
  bool initialized;
  RGWPoolIterCtx iter_ctx;

  RGWListRawObjsCtx() : initialized(false) {}
};

struct RGWRegion;


struct RGWZonePlacementInfo {
  string index_pool;
  string data_pool;
  string data_extra_pool; /* if not set we should use data_pool */

  void encode(bufferlist& bl) const {
    ENCODE_START(4, 1, bl);
    ::encode(index_pool, bl);
    ::encode(data_pool, bl);
    ::encode(data_extra_pool, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::iterator& bl) {
    DECODE_START(4, bl);
    ::decode(index_pool, bl);
    ::decode(data_pool, bl);
    if (struct_v >= 4) {
      ::decode(data_extra_pool, bl);
    }
    DECODE_FINISH(bl);
  }
  const string& get_data_extra_pool() {
    if (data_extra_pool.empty()) {
      return data_pool;
    }
    return data_extra_pool;
  }
  void dump(Formatter *f) const;
  void decode_json(JSONObj *obj);
};
WRITE_CLASS_ENCODER(RGWZonePlacementInfo);

struct RGWZoneParams {
  rgw_bucket domain_root;
  rgw_bucket control_pool;
  rgw_bucket gc_pool;
  rgw_bucket log_pool;
  rgw_bucket intent_log_pool;
  rgw_bucket usage_log_pool;

  rgw_bucket user_keys_pool;
  rgw_bucket user_email_pool;
  rgw_bucket user_swift_pool;
  rgw_bucket user_uid_pool;

  string name;
  bool is_master;

  RGWAccessKey system_key;

  map<string, RGWZonePlacementInfo> placement_pools;

  RGWZoneParams() : is_master(false) {}

  static int get_pool_name(CephContext *cct, string *pool_name);
  void init_name(CephContext *cct, RGWRegion& region);
  int init(CephContext *cct, RGWRados *store, RGWRegion& region);
  void init_default(RGWRados *store);
  int store_info(CephContext *cct, RGWRados *store, RGWRegion& region);

  void encode(bufferlist& bl) const {
    ENCODE_START(4, 1, bl);
    ::encode(domain_root, bl);
    ::encode(control_pool, bl);
    ::encode(gc_pool, bl);
    ::encode(log_pool, bl);
    ::encode(intent_log_pool, bl);
    ::encode(usage_log_pool, bl);
    ::encode(user_keys_pool, bl);
    ::encode(user_email_pool, bl);
    ::encode(user_swift_pool, bl);
    ::encode(user_uid_pool, bl);
    ::encode(name, bl);
    ::encode(system_key, bl);
    ::encode(placement_pools, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::iterator& bl) {
     DECODE_START(4, bl);
    ::decode(domain_root, bl);
    ::decode(control_pool, bl);
    ::decode(gc_pool, bl);
    ::decode(log_pool, bl);
    ::decode(intent_log_pool, bl);
    ::decode(usage_log_pool, bl);
    ::decode(user_keys_pool, bl);
    ::decode(user_email_pool, bl);
    ::decode(user_swift_pool, bl);
    ::decode(user_uid_pool, bl);
    if (struct_v >= 2)
      ::decode(name, bl);
    if (struct_v >= 3)
      ::decode(system_key, bl);
    if (struct_v >= 4)
      ::decode(placement_pools, bl);
    DECODE_FINISH(bl);
  }
  void dump(Formatter *f) const;
  void decode_json(JSONObj *obj);
};
WRITE_CLASS_ENCODER(RGWZoneParams);

struct RGWZone {
  string name;
  list<string> endpoints;
  bool log_meta;
  bool log_data;

  RGWZone() : log_meta(false), log_data(false) {}

  void encode(bufferlist& bl) const {
    ENCODE_START(2, 1, bl);
    ::encode(name, bl);
    ::encode(endpoints, bl);
    ::encode(log_meta, bl);
    ::encode(log_data, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::iterator& bl) {
    DECODE_START(2, bl);
    ::decode(name, bl);
    ::decode(endpoints, bl);
    if (struct_v >= 2) {
      ::decode(log_meta, bl);
      ::decode(log_data, bl);
    }
    DECODE_FINISH(bl);
  }
  void dump(Formatter *f) const;
  void decode_json(JSONObj *obj);
};
WRITE_CLASS_ENCODER(RGWZone);

struct RGWDefaultRegionInfo {
  string default_region;

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    ::encode(default_region, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::iterator& bl) {
    DECODE_START(1, bl);
    ::decode(default_region, bl);
    DECODE_FINISH(bl);
  }
  void dump(Formatter *f) const;
  void decode_json(JSONObj *obj);
};
WRITE_CLASS_ENCODER(RGWDefaultRegionInfo);

struct RGWRegionPlacementTarget {
  string name;
  list<string> tags;

  bool user_permitted(list<string>& user_tags) {
    if (tags.empty()) {
      return true;
    }
    for (list<string>::iterator uiter = user_tags.begin(); uiter != user_tags.end(); ++uiter) { /* we don't expect many of either, so we can handle this kind of lookup */
      string& rule = *uiter;
      for (list<string>::iterator iter = tags.begin(); iter != tags.end(); ++iter) {
        if (rule == *iter) {
          return true;
        }
      }
    }
    return false;
  }

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    ::encode(name, bl);
    ::encode(tags, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::iterator& bl) {
    DECODE_START(1, bl);
    ::decode(name, bl);
    ::decode(tags, bl);
    DECODE_FINISH(bl);
  }
  void dump(Formatter *f) const;
  void decode_json(JSONObj *obj);
};
WRITE_CLASS_ENCODER(RGWRegionPlacementTarget);


struct RGWRegion {
  string name;
  string api_name;
  list<string> endpoints;
  bool is_master;

  string master_zone;
  map<string, RGWZone> zones;

  map<string, RGWRegionPlacementTarget> placement_targets;
  string default_placement;

  CephContext *cct;
  RGWRados *store;

  RGWRegion() : is_master(false), cct(NULL), store(NULL) {}

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    ::encode(name, bl);
    ::encode(api_name, bl);
    ::encode(is_master, bl);
    ::encode(endpoints, bl);
    ::encode(master_zone, bl);
    ::encode(zones, bl);
    ::encode(placement_targets, bl);
    ::encode(default_placement, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::iterator& bl) {
    DECODE_START(1, bl);
    ::decode(name, bl);
    ::decode(api_name, bl);
    ::decode(is_master, bl);
    ::decode(endpoints, bl);
    ::decode(master_zone, bl);
    ::decode(zones, bl);
    ::decode(placement_targets, bl);
    ::decode(default_placement, bl);
    DECODE_FINISH(bl);
  }

  int init(CephContext *_cct, RGWRados *_store, bool setup_region = true);
  int create_default();
  int store_info(bool exclusive);
  int read_info(const string& region_name);
  int read_default(RGWDefaultRegionInfo& default_region);
  int set_as_default();
  int equals(const string& other_region);

  static int get_pool_name(CephContext *cct, string *pool_name);

  void dump(Formatter *f) const;
  void decode_json(JSONObj *obj);
};
WRITE_CLASS_ENCODER(RGWRegion);

struct RGWRegionMap {
  Mutex lock;
  map<string, RGWRegion> regions;
  map<string, RGWRegion> regions_by_api;

  string master_region;

  RGWQuotaInfo bucket_quota;
  RGWQuotaInfo user_quota;

  RGWRegionMap() : lock("RGWRegionMap") {}

  void encode(bufferlist& bl) const;
  void decode(bufferlist::iterator& bl);

  void get_params(CephContext *cct, string& pool_name, string& oid);
  int read(CephContext *cct, RGWRados *store);
  int store(CephContext *cct, RGWRados *store);

  int update(RGWRegion& region);

  void dump(Formatter *f) const;
  void decode_json(JSONObj *obj);
};
WRITE_CLASS_ENCODER(RGWRegionMap);

class RGWDataChangesLog;
class RGWReplicaLogger;
  
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
  bool dump_entry_internal(const cls_statelog_entry& entry, Formatter *f);
public:

  enum OpState {
    OPSTATE_UNKNOWN     = 0,
    OPSTATE_IN_PROGRESS = 1,
    OPSTATE_COMPLETE    = 2,
    OPSTATE_ERROR       = 3,
    OPSTATE_ABORT       = 4,
    OPSTATE_CANCELLED   = 5,
  };

  RGWOpState(RGWRados *_store);

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
  utime_t last_update;

public:
  RGWOpStateSingleOp(RGWRados *store, const string& cid, const string& oid, const string& obj);

  int set_state(RGWOpState::OpState state);
  int renew_state();
};

class RGWGetBucketStats_CB : public RefCountedObject {
protected:
  rgw_bucket bucket;
  uint64_t bucket_ver;
  uint64_t master_ver;
  map<RGWObjCategory, RGWStorageStats> *stats;
  string max_marker;
public:
  RGWGetBucketStats_CB(rgw_bucket& _bucket) : bucket(_bucket), stats(NULL) {}
  virtual ~RGWGetBucketStats_CB() {}
  virtual void handle_response(int r) = 0;
  virtual void set_response(uint64_t _bucket_ver, uint64_t _master_ver,
                            map<RGWObjCategory, RGWStorageStats> *_stats,
                            const string &_max_marker) {
    bucket_ver = _bucket_ver;
    master_ver = _master_ver;
    stats = _stats;
    max_marker = _max_marker;
  }
};

class RGWGetUserStats_CB : public RefCountedObject {
protected:
  string user;
  RGWStorageStats stats;
public:
  RGWGetUserStats_CB(const string& _user) : user(_user) {}
  virtual ~RGWGetUserStats_CB() {}
  virtual void handle_response(int r) = 0;
  virtual void set_response(RGWStorageStats& _stats) {
    stats = _stats;
  }
};

class RGWGetDirHeader_CB;
class RGWGetUserHeader_CB;

struct rgw_rados_ref {
  string oid;
  string key;
  librados::IoCtx ioctx;
};


class RGWRados
{
  friend class RGWGC;
  friend class RGWStateLog;
  friend class RGWReplicaLogger;

  /** Open the pool used as root for this gateway */
  int open_root_pool_ctx();
  int open_gc_pool_ctx();

  int open_bucket_pool_ctx(const string& bucket_name, const string& pool, librados::IoCtx&  io_ctx);
  int open_bucket_index_ctx(rgw_bucket& bucket, librados::IoCtx&  index_ctx);
  int open_bucket_data_ctx(rgw_bucket& bucket, librados::IoCtx&  io_ctx);
  int open_bucket_data_extra_ctx(rgw_bucket& bucket, librados::IoCtx&  io_ctx);
  int open_bucket_index(rgw_bucket& bucket, librados::IoCtx&  index_ctx, string& bucket_oid);

  struct GetObjState {
    librados::IoCtx io_ctx;
    bool sent_data;

    GetObjState() : sent_data(false) {}
  };

  Mutex lock;
  SafeTimer *timer;

  class C_Tick : public Context {
    RGWRados *rados;
  public:
    C_Tick(RGWRados *_r) : rados(_r) {}
    void finish(int r) {
      rados->tick();
    }
  };

  RGWGC *gc;
  bool use_gc_thread;
  bool quota_threads;

  int num_watchers;
  RGWWatcher **watchers;
  uint64_t *watch_handles;
  librados::IoCtx root_pool_ctx;      // .rgw
  librados::IoCtx control_pool_ctx;   // .rgw.control
  bool watch_initialized;

  Mutex bucket_id_lock;

  int get_obj_ioctx(const rgw_obj& obj, librados::IoCtx *ioctx);
  int get_obj_ref(const rgw_obj& obj, rgw_rados_ref *ref, rgw_bucket *bucket, bool ref_system_obj = false);
  uint64_t max_bucket_id;

  int get_obj_state(RGWRadosCtx *rctx, rgw_obj& obj, RGWObjState **state, RGWObjVersionTracker *objv_tracker);
  int append_atomic_test(RGWRadosCtx *rctx, rgw_obj& obj,
                         librados::ObjectOperation& op, RGWObjState **state);
  int prepare_atomic_for_write_impl(RGWRadosCtx *rctx, rgw_obj& obj,
                         librados::ObjectWriteOperation& op, RGWObjState **pstate,
			 bool reset_obj, const string *ptag);
  int prepare_atomic_for_write(RGWRadosCtx *rctx, rgw_obj& obj,
                         librados::ObjectWriteOperation& op, RGWObjState **pstate,
			 bool reset_obj, const string *ptag);

  void atomic_write_finish(RGWObjState *state, int r) {
    if (state && r == -ECANCELED) {
      state->clear();
    }
  }

  int clone_objs_impl(void *ctx, rgw_obj& dst_obj, 
                 vector<RGWCloneRangeInfo>& ranges,
                 map<string, bufferlist> attrs,
                 RGWObjCategory category,
                 time_t *pmtime,
                 bool truncate_dest,
                 bool exclusive,
                 pair<string, bufferlist> *cmp_xattr);

  virtual int clone_obj(void *ctx, rgw_obj& dst_obj, off_t dst_ofs,
                          rgw_obj& src_obj, off_t src_ofs,
                          uint64_t size, time_t *pmtime,
                          map<string, bufferlist> attrs,
                          RGWObjCategory category) {
    RGWCloneRangeInfo info;
    vector<RGWCloneRangeInfo> v;
    info.src = src_obj;
    info.src_ofs = src_ofs;
    info.dst_ofs = dst_ofs;
    info.len = size;
    v.push_back(info);
    return clone_objs(ctx, dst_obj, v, attrs, category, pmtime, true, false);
  }
  int complete_atomic_overwrite(RGWRadosCtx *rctx, RGWObjState *state, rgw_obj& obj);

  int update_placement_map();
  int store_bucket_info(RGWBucketInfo& info, map<string, bufferlist> *pattrs, RGWObjVersionTracker *objv_tracker, bool exclusive);

protected:
  virtual int delete_obj_impl(void *ctx, const string& bucket_owner, rgw_obj& src_obj, RGWObjVersionTracker *objv_tracker);

  CephContext *cct;
  librados::Rados *rados;
  librados::IoCtx gc_pool_ctx;        // .rgw.gc

  bool pools_initialized;

  string region_name;
  string zone_name;

  RGWQuotaHandler *quota_handler;

public:
  RGWRados() : lock("rados_timer_lock"), timer(NULL),
               gc(NULL), use_gc_thread(false), quota_threads(false),
               num_watchers(0), watchers(NULL), watch_handles(NULL),
               watch_initialized(false),
               bucket_id_lock("rados_bucket_id"), max_bucket_id(0),
               cct(NULL), rados(NULL),
               pools_initialized(false),
               quota_handler(NULL),
               rest_master_conn(NULL),
               meta_mgr(NULL), data_log(NULL) {}

  void set_context(CephContext *_cct) {
    cct = _cct;
  }

  void set_region(const string& name) {
    region_name = name;
  }

  void set_zone(const string& name) {
    zone_name = name;
  }

  RGWRegion region;
  RGWZoneParams zone; /* internal zone params, e.g., rados pools */
  RGWZone zone_public_config; /* external zone params, e.g., entrypoints, log flags, etc. */
  RGWRegionMap region_map;
  RGWRESTConn *rest_master_conn;
  map<string, RGWRESTConn *> zone_conn_map;
  map<string, RGWRESTConn *> region_conn_map;

  RGWMetadataManager *meta_mgr;

  RGWDataChangesLog *data_log;

  virtual ~RGWRados() {
    if (rados) {
      rados->shutdown();
      delete rados;
    }
  }

  int get_required_alignment(rgw_bucket& bucket, uint64_t *alignment);
  int get_max_chunk_size(rgw_bucket& bucket, uint64_t *max_chunk_size);

  int list_raw_objects(rgw_bucket& pool, const string& prefix_filter, int max,
                       RGWListRawObjsCtx& ctx, list<string>& oids,
                       bool *is_truncated);

  int list_raw_prefixed_objs(string pool_name, const string& prefix, list<string>& result);
  int list_regions(list<string>& regions);
  int list_zones(list<string>& zones);

  void tick();

  CephContext *ctx() { return cct; }
  /** do all necessary setup of the storage device */
  int initialize(CephContext *_cct, bool _use_gc_thread, bool _quota_threads) {
    set_context(_cct);
    use_gc_thread = _use_gc_thread;
    quota_threads = _quota_threads;
    return initialize();
  }
  /** Initialize the RADOS instance and prepare to do other ops */
  virtual int init_rados();
  int init_complete();
  virtual int initialize();
  virtual void finalize();

  /** set up a bucket listing. handle is filled in. */
  virtual int list_buckets_init(RGWAccessHandle *handle);
  /** 
   * get the next bucket in the listing. obj is filled in,
   * handle is updated.
   */
  virtual int list_buckets_next(RGWObjEnt& obj, RGWAccessHandle *handle);

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
  int read_usage(string& user, uint64_t start_epoch, uint64_t end_epoch, uint32_t max_entries,
                 bool *is_truncated, RGWUsageIter& read_iter, map<rgw_user_bucket, rgw_usage_log_entry>& usage);
  int trim_usage(string& user, uint64_t start_epoch, uint64_t end_epoch);

  /**
   * get listing of the objects in a bucket.
   * bucket: bucket to list contents of
   * max: maximum number of results to return
   * prefix: only return results that match this prefix
   * delim: do not include results that match this string.
   *     Any skipped results will have the matching portion of their name
   *     inserted in common_prefixes with a "true" mark.
   * marker: if filled in, begin the listing with this object.
   * result: the objects are put in here.
   * common_prefixes: if delim is filled in, any matching prefixes are placed
   *     here.
   */
  virtual int list_objects(rgw_bucket& bucket, int max, std::string& prefix, std::string& delim,
                   std::string& marker, std::string *next_marker, std::vector<RGWObjEnt>& result,
                   map<string, bool>& common_prefixes, bool get_content_type, string& ns, bool enforce_ns,
                   bool *is_truncated, RGWAccessListFilter *filter);

  virtual int create_pool(rgw_bucket& bucket);

  /**
   * create a bucket with name bucket and the given list of attrs
   * returns 0 on success, -ERR# otherwise.
   */
  virtual int init_bucket_index(rgw_bucket& bucket);
  int select_bucket_placement(RGWUserInfo& user_info, const string& region_name, const std::string& rule,
                              const std::string& bucket_name, rgw_bucket& bucket, string *pselected_rule);
  int select_legacy_bucket_placement(const string& bucket_name, rgw_bucket& bucket);
  int select_new_bucket_location(RGWUserInfo& user_info, const string& region_name, const string& rule,
                                 const std::string& bucket_name, rgw_bucket& bucket, string *pselected_rule);
  int set_bucket_location_by_rule(const string& location_rule, const std::string& bucket_name, rgw_bucket& bucket);
  virtual int create_bucket(RGWUserInfo& owner, rgw_bucket& bucket,
                            const string& region_name,
                            const string& placement_rule,
                            map<std::string,bufferlist>& attrs,
                            RGWBucketInfo& bucket_info,
                            obj_version *pobjv,
                            obj_version *pep_objv,
                            time_t creation_time,
                            rgw_bucket *master_bucket,
                            bool exclusive = true);
  virtual int add_bucket_placement(std::string& new_pool);
  virtual int remove_bucket_placement(std::string& new_pool);
  virtual int list_placement_set(set<string>& names);
  virtual int create_pools(vector<string>& names, vector<int>& retcodes);

  struct PutObjMetaExtraParams {
    time_t *mtime;
    map<std::string, bufferlist>* rmattrs;
    const bufferlist *data;
    RGWObjManifest *manifest;
    const string *ptag;
    list<string> *remove_objs;
    bool modify_version;
    RGWObjVersionTracker *objv_tracker;
    time_t set_mtime;
    string owner;

    PutObjMetaExtraParams() : mtime(NULL), rmattrs(NULL),
                     data(NULL), manifest(NULL), ptag(NULL),
                     remove_objs(NULL), modify_version(false),
                     objv_tracker(NULL), set_mtime(0) {}
  };

  /** Write/overwrite an object to the bucket storage. */
  virtual int put_obj_meta_impl(void *ctx, rgw_obj& obj, uint64_t size, time_t *mtime,
              map<std::string, bufferlist>& attrs, RGWObjCategory category, int flags,
              map<std::string, bufferlist>* rmattrs, const bufferlist *data,
              RGWObjManifest *manifest, const string *ptag, list<string> *remove_objs,
              bool modify_version, RGWObjVersionTracker *objv_tracker,
              time_t set_mtime /* 0 for don't set */,
              const string& owner);

  virtual int put_obj_meta(void *ctx, rgw_obj& obj, uint64_t size, time_t *mtime,
              map<std::string, bufferlist>& attrs, RGWObjCategory category, int flags,
              const string& owner, const bufferlist *data = NULL) {
    return put_obj_meta_impl(ctx, obj, size, mtime, attrs, category, flags,
                        NULL, data, NULL, NULL, NULL,
                        false, NULL, 0, owner);
  }

  virtual int put_obj_meta(void *ctx, rgw_obj& obj, uint64_t size,  map<std::string, bufferlist>& attrs,
                           RGWObjCategory category, int flags, PutObjMetaExtraParams& params) {
    return put_obj_meta_impl(ctx, obj, size, params.mtime, attrs, category, flags,
                        params.rmattrs, params.data, params.manifest, params.ptag, params.remove_objs,
                        params.modify_version, params.objv_tracker, params.set_mtime, params.owner);
  }

  virtual int put_obj_data(void *ctx, rgw_obj& obj, const char *data,
              off_t ofs, size_t len, bool exclusive);
  virtual int aio_put_obj_data(void *ctx, rgw_obj& obj, bufferlist& bl,
                               off_t ofs, bool exclusive, void **handle);
  /* note that put_obj doesn't set category on an object, only use it for none user objects */
  int put_system_obj(void *ctx, rgw_obj& obj, const char *data, size_t len, bool exclusive,
              time_t *mtime, map<std::string, bufferlist>& attrs, RGWObjVersionTracker *objv_tracker,
              time_t set_mtime) {
    bufferlist bl;
    bl.append(data, len);
    int flags = PUT_OBJ_CREATE;
    if (exclusive)
      flags |= PUT_OBJ_EXCL;

    PutObjMetaExtraParams ep;
    ep.mtime = mtime;
    ep.data = &bl;
    ep.modify_version = true;
    ep.objv_tracker = objv_tracker;
    ep.set_mtime = set_mtime;

    int ret = put_obj_meta(ctx, obj, len, attrs, RGW_OBJ_CATEGORY_NONE, flags, ep);
    return ret;
  }
  virtual int aio_wait(void *handle);
  virtual bool aio_completed(void *handle);
  virtual int clone_objs(void *ctx, rgw_obj& dst_obj, 
                         vector<RGWCloneRangeInfo>& ranges,
                         map<string, bufferlist> attrs,
                         RGWObjCategory category,
                         time_t *pmtime, bool truncate_dest, bool exclusive) {
    return clone_objs(ctx, dst_obj, ranges, attrs, category, pmtime, truncate_dest, exclusive, NULL);
  }

  int clone_objs(void *ctx, rgw_obj& dst_obj, 
                 vector<RGWCloneRangeInfo>& ranges,
                 map<string, bufferlist> attrs,
                 RGWObjCategory category,
                 time_t *pmtime,
                 bool truncate_dest,
                 bool exclusive,
                 pair<string, bufferlist> *cmp_xattr);

  int clone_obj_cond(void *ctx, rgw_obj& dst_obj, off_t dst_ofs,
                rgw_obj& src_obj, off_t src_ofs,
                uint64_t size, map<string, bufferlist> attrs,
                RGWObjCategory category,
                time_t *pmtime,
                bool truncate_dest,
                bool exclusive,
                pair<string, bufferlist> *xattr_cond) {
    RGWCloneRangeInfo info;
    vector<RGWCloneRangeInfo> v;
    info.src = src_obj;
    info.src_ofs = src_ofs;
    info.dst_ofs = dst_ofs;
    info.len = size;
    v.push_back(info);
    return clone_objs(ctx, dst_obj, v, attrs, category, pmtime, truncate_dest, exclusive, xattr_cond);
  }

  /**
   * Copy an object.
   * dest_obj: the object to copy into
   * src_obj: the object to copy from
   * attrs: if replace_attrs is set then these are placed on the new object
   * err: stores any errors resulting from the get of the original object
   * Returns: 0 on success, -ERR# otherwise.
   */
  virtual int copy_obj(void *ctx,
               const string& user_id,
               const string& client_id,
               const string& op_id,
               req_info *info,
               const string& source_zone,
               rgw_obj& dest_obj,
               rgw_obj& src_obj,
               RGWBucketInfo& dest_bucket_info,
               RGWBucketInfo& src_bucket_info,
               time_t *mtime,
               const time_t *mod_ptr,
               const time_t *unmod_ptr,
               const char *if_match,
               const char *if_nomatch,
               bool replace_attrs,
               map<std::string, bufferlist>& attrs,
               RGWObjCategory category,
               string *ptag,
               struct rgw_err *err,
               void (*progress_cb)(off_t, void *),
               void *progress_data);

  int copy_obj_data(void *ctx,
               const string& owner,
	       void **handle, off_t end,
               rgw_obj& dest_obj,
               rgw_obj& src_obj,
               uint64_t max_chunk_size,
	       time_t *mtime,
               map<string, bufferlist>& attrs,
               RGWObjCategory category,
               string *ptag,
               struct rgw_err *err);
  /**
   * Delete a bucket.
   * bucket: the name of the bucket to delete
   * Returns 0 on success, -ERR# otherwise.
   */
  virtual int delete_bucket(rgw_bucket& bucket, RGWObjVersionTracker& objv_tracker);

  /**
   * Check to see if the bucket metadata is synced
   */
  bool is_syncing_bucket_meta(rgw_bucket& bucket);
  
  int set_bucket_owner(rgw_bucket& bucket, ACLOwner& owner);
  int set_buckets_enabled(std::vector<rgw_bucket>& buckets, bool enabled);
  int bucket_suspended(rgw_bucket& bucket, bool *suspended);

  /** Delete an object.*/
  virtual int delete_obj(void *ctx, const string& bucket_owner, rgw_obj& src_obj, RGWObjVersionTracker *objv_tracker = NULL);
  virtual int delete_system_obj(void *ctx, rgw_obj& src_obj, RGWObjVersionTracker *objv_tracker = NULL);

  /** Remove an object from the bucket index */
  int delete_obj_index(rgw_obj& obj);

  /**
   * Get the attributes for an object.
   * bucket: name of the bucket holding the object.
   * obj: name of the object
   * name: name of the attr to retrieve
   * dest: bufferlist to store the result in
   * Returns: 0 on success, -ERR# otherwise.
   */
  virtual int get_attr(void *ctx, rgw_obj& obj, const char *name, bufferlist& dest);

  /**
   * Set an attr on an object.
   * bucket: name of the bucket holding the object
   * obj: name of the object to set the attr on
   * name: the attr to set
   * bl: the contents of the attr
   * Returns: 0 on success, -ERR# otherwise.
   */
  virtual int set_attr(void *ctx, rgw_obj& obj, const char *name, bufferlist& bl,
                       RGWObjVersionTracker *objv_tracker);

  virtual int set_attrs(void *ctx, rgw_obj& obj,
                        map<string, bufferlist>& attrs,
                        map<string, bufferlist>* rmattrs,
                        RGWObjVersionTracker *objv_tracker);

/**
 * Get data about an object out of RADOS and into memory.
 * bucket: name of the bucket the object is in.
 * obj: name/key of the object to read
 * data: if get_data==true, this pointer will be set
 *    to an address containing the object's data/value
 * ofs: the offset of the object to read from
 * end: the point in the object to stop reading
 * attrs: if non-NULL, the pointed-to map will contain
 *    all the attrs of the object when this function returns
 * mod_ptr: if non-NULL, compares the object's mtime to *mod_ptr,
 *    and if mtime is smaller it fails.
 * unmod_ptr: if non-NULL, compares the object's mtime to *unmod_ptr,
 *    and if mtime is >= it fails.
 * if_match/nomatch: if non-NULL, compares the object's etag attr
 *    to the string and, if it doesn't/does match, fails out.
 * err: Many errors will result in this structure being filled
 *    with extra informatin on the error.
 * Returns: -ERR# on failure, otherwise
 *          (if get_data==true) length of read data,
 *          (if get_data==false) length of the object
 */
  virtual int prepare_get_obj(void *ctx, rgw_obj& obj,
            off_t *ofs, off_t *end,
            map<string, bufferlist> *attrs,
            const time_t *mod_ptr,
            const time_t *unmod_ptr,
            time_t *lastmod,
            const char *if_match,
            const char *if_nomatch,
            uint64_t *total_size,
            uint64_t *obj_size,
            RGWObjVersionTracker *objv_tracker,
            void **handle,
            struct rgw_err *err);

  virtual int get_obj(void *ctx, RGWObjVersionTracker *objv_tracker, void **handle, rgw_obj& obj,
                      bufferlist& bl, off_t ofs, off_t end);

  virtual void finish_get_obj(void **handle);

  int iterate_obj(void *ctx, rgw_obj& obj,
                  off_t ofs, off_t end,
                  uint64_t max_chunk_size,
                  int (*iterate_obj_cb)(rgw_obj&, off_t, off_t, off_t, bool, RGWObjState *, void *),
                  void *arg);

  int get_obj_iterate(void *ctx, void **handle, rgw_obj& obj,
                      off_t ofs, off_t end,
	              RGWGetDataCB *cb);

  int flush_read_list(struct get_obj_data *d);

  int get_obj_iterate_cb(void *ctx, RGWObjState *astate,
                         rgw_obj& obj,
                         off_t obj_ofs, off_t read_ofs, off_t len,
                         bool is_head_obj, void *arg);

  void get_obj_aio_completion_cb(librados::completion_t cb, void *arg);

  /**
   * a simple object read without keeping state
   */
  virtual int read(void *ctx, rgw_obj& obj, off_t ofs, size_t size, bufferlist& bl);

  virtual int obj_stat(void *ctx, rgw_obj& obj, uint64_t *psize, time_t *pmtime,
                       uint64_t *epoch, map<string, bufferlist> *attrs, bufferlist *first_chunk,
                       RGWObjVersionTracker *objv_tracker);

  virtual bool supports_omap() { return true; }
  int omap_get_vals(rgw_obj& obj, bufferlist& header, const std::string& marker, uint64_t count, std::map<string, bufferlist>& m);
  virtual int omap_get_all(rgw_obj& obj, bufferlist& header, std::map<string, bufferlist>& m);
  virtual int omap_set(rgw_obj& obj, std::string& key, bufferlist& bl);
  virtual int omap_set(rgw_obj& obj, map<std::string, bufferlist>& m);
  virtual int omap_del(rgw_obj& obj, const std::string& key);
  virtual int update_containers_stats(map<string, RGWBucketEnt>& m);
  virtual int append_async(rgw_obj& obj, size_t size, bufferlist& bl);

  virtual bool need_watch_notify() { return false; }
  virtual int init_watch();
  virtual void finalize_watch();
  virtual int distribute(const string& key, bufferlist& bl);
  virtual int watch_cb(int opcode, uint64_t ver, bufferlist& bl) { return 0; }
  void pick_control_oid(const string& key, string& notify_oid);

  void *create_context(void *user_ctx) {
    RGWRadosCtx *rctx = new RGWRadosCtx(this);
    rctx->user_ctx = user_ctx;
    return rctx;
  }
  void destroy_context(void *ctx) {
    delete static_cast<RGWRadosCtx *>(ctx);
  }
  void set_atomic(void *ctx, rgw_obj& obj) {
    RGWRadosCtx *rctx = static_cast<RGWRadosCtx *>(ctx);
    rctx->set_atomic(obj);
  }
  void set_prefetch_data(void *ctx, rgw_obj& obj) {
    RGWRadosCtx *rctx = static_cast<RGWRadosCtx *>(ctx);
    rctx->set_prefetch_data(obj);
  }
  // to notify upper layer that we need to do some operation on an object, and it's up to
  // the upper layer to schedule this operation.. e.g., log intent in intent log
  void set_intent_cb(void *ctx, int (*cb)(RGWRados *store, void *user_ctx, rgw_obj& obj, RGWIntentEvent intent)) {
    RGWRadosCtx *rctx = static_cast<RGWRadosCtx *>(ctx);
    rctx->set_intent_cb(cb);
  }

  int decode_policy(bufferlist& bl, ACLOwner *owner);
  int get_bucket_stats(rgw_bucket& bucket, uint64_t *bucket_ver, uint64_t *master_ver, map<RGWObjCategory, RGWStorageStats>& stats,
                       string *max_marker);
  int get_bucket_stats_async(rgw_bucket& bucket, RGWGetBucketStats_CB *cb);
  int get_user_stats(const string& user, RGWStorageStats& stats);
  int get_user_stats_async(const string& user, RGWGetUserStats_CB *cb);
  void get_bucket_instance_obj(rgw_bucket& bucket, rgw_obj& obj);
  void get_bucket_instance_entry(rgw_bucket& bucket, string& entry);
  void get_bucket_meta_oid(rgw_bucket& bucket, string& oid);

  int put_bucket_entrypoint_info(const string& bucket_name, RGWBucketEntryPoint& entry_point, bool exclusive, RGWObjVersionTracker& objv_tracker, time_t mtime,
                                 map<string, bufferlist> *pattrs);
  int put_bucket_instance_info(RGWBucketInfo& info, bool exclusive, time_t mtime, map<string, bufferlist> *pattrs);
  int get_bucket_entrypoint_info(void *ctx, const string& bucket_name, RGWBucketEntryPoint& entry_point, RGWObjVersionTracker *objv_tracker, time_t *pmtime,
                                 map<string, bufferlist> *pattrs);
  int get_bucket_instance_info(void *ctx, const string& meta_key, RGWBucketInfo& info, time_t *pmtime, map<string, bufferlist> *pattrs);
  int get_bucket_instance_info(void *ctx, rgw_bucket& bucket, RGWBucketInfo& info, time_t *pmtime, map<string, bufferlist> *pattrs);
  int get_bucket_instance_from_oid(void *ctx, string& oid, RGWBucketInfo& info, time_t *pmtime, map<string, bufferlist> *pattrs);

  int convert_old_bucket_info(void *ctx, string& bucket_name);
  virtual int get_bucket_info(void *ctx, const string& bucket_name, RGWBucketInfo& info,
                              time_t *pmtime, map<string, bufferlist> *pattrs = NULL);
  virtual int put_linked_bucket_info(RGWBucketInfo& info, bool exclusive, time_t mtime, obj_version *pep_objv,
                                     map<string, bufferlist> *pattrs, bool create_entry_point);

  int cls_rgw_init_index(librados::IoCtx& io_ctx, librados::ObjectWriteOperation& op, string& oid);
  int cls_obj_prepare_op(rgw_bucket& bucket, RGWModifyOp op, string& tag,
                         string& name, string& locator);
  int cls_obj_complete_op(rgw_bucket& bucket, RGWModifyOp op, string& tag, int64_t pool, uint64_t epoch,
                          RGWObjEnt& ent, RGWObjCategory category, list<string> *remove_objs);
  int cls_obj_complete_add(rgw_bucket& bucket, string& tag, int64_t pool, uint64_t epoch, RGWObjEnt& ent, RGWObjCategory category, list<string> *remove_objs);
  int cls_obj_complete_del(rgw_bucket& bucket, string& tag, int64_t pool, uint64_t epoch, string& name);
  int cls_obj_complete_cancel(rgw_bucket& bucket, string& tag, string& name);
  int cls_obj_set_bucket_tag_timeout(rgw_bucket& bucket, uint64_t timeout);
  int cls_bucket_list(rgw_bucket& bucket, string start, string prefix, uint32_t num,
                      map<string, RGWObjEnt>& m, bool *is_truncated,
                      string *last_entry, bool (*force_check_filter)(const string&  name) = NULL);
  int cls_bucket_head(rgw_bucket& bucket, struct rgw_bucket_dir_header& header);
  int cls_bucket_head_async(rgw_bucket& bucket, RGWGetDirHeader_CB *ctx);
  int prepare_update_index(RGWObjState *state, rgw_bucket& bucket,
                           RGWModifyOp op, rgw_obj& oid, string& tag);
  int complete_update_index(rgw_bucket& bucket, string& oid, string& tag, int64_t poolid, uint64_t epoch, uint64_t size,
                            utime_t& ut, string& etag, string& content_type, bufferlist *acl_bl, RGWObjCategory category,
			    list<string> *remove_objs);
  int complete_update_index_del(rgw_bucket& bucket, string& oid, string& tag, int64_t pool, uint64_t epoch) {
    if (bucket_is_system(bucket))
      return 0;

    return cls_obj_complete_del(bucket, tag, pool, epoch, oid);
  }
  int complete_update_index_cancel(rgw_bucket& bucket, string& oid, string& tag) {
    if (bucket_is_system(bucket))
      return 0;

    return cls_obj_complete_cancel(bucket, tag, oid);
  }
  int list_bi_log_entries(rgw_bucket& bucket, string& marker, uint32_t max, std::list<rgw_bi_log_entry>& result, bool *truncated);
  int trim_bi_log_entries(rgw_bucket& bucket, string& marker, string& end_marker);

  int cls_obj_usage_log_add(const string& oid, rgw_usage_log_info& info);
  int cls_obj_usage_log_read(string& oid, string& user, uint64_t start_epoch, uint64_t end_epoch, uint32_t max_entries,
                             string& read_iter, map<rgw_user_bucket, rgw_usage_log_entry>& usage, bool *is_truncated);
  int cls_obj_usage_log_trim(string& oid, string& user, uint64_t start_epoch, uint64_t end_epoch);

  void shard_name(const string& prefix, unsigned max_shards, const string& key, string& name);
  void shard_name(const string& prefix, unsigned max_shards, const string& section, const string& key, string& name);
  void time_log_prepare_entry(cls_log_entry& entry, const utime_t& ut, string& section, string& key, bufferlist& bl);
  int time_log_add(const string& oid, list<cls_log_entry>& entries);
  int time_log_add(const string& oid, const utime_t& ut, const string& section, const string& key, bufferlist& bl);
  int time_log_list(const string& oid, utime_t& start_time, utime_t& end_time,
                    int max_entries, list<cls_log_entry>& entries,
		    const string& marker, string *out_marker, bool *truncated);
  int time_log_info(const string& oid, cls_log_header *header);
  int time_log_trim(const string& oid, const utime_t& start_time, const utime_t& end_time,
                    const string& from_marker, const string& to_marker);
  int lock_exclusive(rgw_bucket& pool, const string& oid, utime_t& duration, string& zone_id, string& owner_id);
  int unlock(rgw_bucket& pool, const string& oid, string& zone_id, string& owner_id);

  /// clean up/process any temporary objects older than given date[/time]
  int remove_temp_objects(string date, string time);

  int gc_operate(string& oid, librados::ObjectWriteOperation *op);
  int gc_aio_operate(string& oid, librados::ObjectWriteOperation *op);
  int gc_operate(string& oid, librados::ObjectReadOperation *op, bufferlist *pbl);

  int list_gc_objs(int *index, string& marker, uint32_t max, bool expired_only, std::list<cls_rgw_gc_obj_info>& result, bool *truncated);
  int process_gc();
  int defer_gc(void *ctx, rgw_obj& obj);

  int bucket_check_index(rgw_bucket& bucket,
                         map<RGWObjCategory, RGWStorageStats> *existing_stats,
                         map<RGWObjCategory, RGWStorageStats> *calculated_stats);
  int bucket_rebuild_index(rgw_bucket& bucket);
  int remove_objs_from_index(rgw_bucket& bucket, list<string>& oid_list);

  int cls_user_get_header(const string& user_id, cls_user_header *header);
  int cls_user_get_header_async(const string& user_id, RGWGetUserHeader_CB *ctx);
  int cls_user_sync_bucket_stats(rgw_obj& user_obj, rgw_bucket& bucket);
  int update_user_bucket_stats(const string& user_id, rgw_bucket& bucket, RGWStorageStats& stats);
  int cls_user_list_buckets(rgw_obj& obj,
                            const string& in_marker, int max_entries,
                            list<cls_user_bucket_entry>& entries,
                            string *out_marker, bool *truncated);
  int cls_user_add_bucket(rgw_obj& obj, const cls_user_bucket_entry& entry);
  int cls_user_update_buckets(rgw_obj& obj, list<cls_user_bucket_entry>& entries, bool add);
  int cls_user_complete_stats_sync(rgw_obj& obj);
  int complete_sync_user_stats(const string& user_id);
  int cls_user_add_bucket(rgw_obj& obj, list<cls_user_bucket_entry>& entries);
  int cls_user_remove_bucket(rgw_obj& obj, const cls_user_bucket& bucket);

  int check_quota(const string& bucket_owner, rgw_bucket& bucket,
                  RGWQuotaInfo& user_quota, RGWQuotaInfo& bucket_quota, uint64_t obj_size);

  string unique_id(uint64_t unique_num) {
    char buf[32];
    snprintf(buf, sizeof(buf), ".%llu.%llu", (unsigned long long)instance_id(), (unsigned long long)unique_num);
    string s = zone.name + buf;
    return s;
  }


  void get_log_pool_name(string& name) {
    name = zone.log_pool.name;
  }

  bool need_to_log_data() {
    return zone_public_config.log_data;
  }

  bool need_to_log_metadata() {
    return zone_public_config.log_meta;
  }

 private:
  int process_intent_log(rgw_bucket& bucket, string& oid,
			 time_t epoch, int flags, bool purge);
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
                       rgw_bucket& bucket,
                       rgw_bucket_dir_entry& list_state,
                       RGWObjEnt& object,
                       bufferlist& suggested_updates);

  bool bucket_is_system(rgw_bucket& bucket) {
    return (bucket.name[0] == '.');
  }

  /**
   * Init pool iteration
   * bucket: pool name in a bucket object
   * ctx: context object to use for the iteration
   * Returns: 0 on success, -ERR# otherwise.
   */
  int pool_iterate_begin(rgw_bucket& bucket, RGWPoolIterCtx& ctx);
  /**
   * Iterate over pool return object names, use optional filter
   * ctx: iteration context, initialized with pool_iterate_begin()
   * num: max number of objects to return
   * objs: a vector that the results will append into
   * is_truncated: if not NULL, will hold true iff iteration is complete
   * filter: if not NULL, will be used to filter returned objects
   * Returns: 0 on success, -ERR# otherwise.
   */
  int pool_iterate(RGWPoolIterCtx& ctx, uint32_t num, vector<RGWObjEnt>& objs,
                   bool *is_truncated, RGWAccessListFilter *filter);

  uint64_t instance_id();
  uint64_t next_bucket_id();
};

class RGWStoreManager {
public:
  RGWStoreManager() {}
  static RGWRados *get_storage(CephContext *cct, bool use_gc_thread, bool quota_threads) {
    RGWRados *store = init_storage_provider(cct, use_gc_thread, quota_threads);
    return store;
  }
  static RGWRados *get_raw_storage(CephContext *cct) {
    RGWRados *store = init_raw_storage_provider(cct);
    return store;
  }
  static RGWRados *init_storage_provider(CephContext *cct, bool use_gc_thread, bool quota_threads);
  static RGWRados *init_raw_storage_provider(CephContext *cct);
  static void close_storage(RGWRados *store);

};



#endif
