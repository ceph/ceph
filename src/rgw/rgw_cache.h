// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#ifndef CEPH_RGWCACHE_H
#define CEPH_RGWCACHE_H

#include <string>
#include <map>
#include <unordered_map>
#include "include/types.h"
#include "include/utime.h"
#include "include/ceph_assert.h"
#include "common/ceph_mutex.h"

#include "cls/version/cls_version_types.h"
#include "rgw_common.h"

enum {
  UPDATE_OBJ,
  INVALIDATE_OBJ,
};

#define CACHE_FLAG_DATA           0x01
#define CACHE_FLAG_XATTRS         0x02
#define CACHE_FLAG_META           0x04
#define CACHE_FLAG_MODIFY_XATTRS  0x08
#define CACHE_FLAG_OBJV           0x10

struct ObjectMetaInfo {
  uint64_t size;
  real_time mtime;

  ObjectMetaInfo() : size(0) {}

  void encode(bufferlist& bl) const {
    ENCODE_START(2, 2, bl);
    encode(size, bl);
    encode(mtime, bl);
    ENCODE_FINISH(bl);
  }
  void decode(bufferlist::const_iterator& bl) {
    DECODE_START_LEGACY_COMPAT_LEN(2, 2, 2, bl);
    decode(size, bl);
    decode(mtime, bl);
    DECODE_FINISH(bl);
  }
  void dump(Formatter *f) const;
  static void generate_test_instances(list<ObjectMetaInfo*>& o);
};
WRITE_CLASS_ENCODER(ObjectMetaInfo)

struct ObjectCacheInfo {
  int status = 0;
  uint32_t flags = 0;
  uint64_t epoch = 0;
  bufferlist data;
  map<string, bufferlist> xattrs;
  map<string, bufferlist> rm_xattrs;
  ObjectMetaInfo meta;
  obj_version version = {};
  ceph::coarse_mono_time time_added;

  ObjectCacheInfo() = default;

  void encode(bufferlist& bl) const {
    ENCODE_START(5, 3, bl);
    encode(status, bl);
    encode(flags, bl);
    encode(data, bl);
    encode(xattrs, bl);
    encode(meta, bl);
    encode(rm_xattrs, bl);
    encode(epoch, bl);
    encode(version, bl);
    ENCODE_FINISH(bl);
  }
  void decode(bufferlist::const_iterator& bl) {
    DECODE_START_LEGACY_COMPAT_LEN(5, 3, 3, bl);
    decode(status, bl);
    decode(flags, bl);
    decode(data, bl);
    decode(xattrs, bl);
    decode(meta, bl);
    if (struct_v >= 2)
      decode(rm_xattrs, bl);
    if (struct_v >= 4)
      decode(epoch, bl);
    if (struct_v >= 5)
      decode(version, bl);
    DECODE_FINISH(bl);
  }
  void dump(Formatter *f) const;
  static void generate_test_instances(list<ObjectCacheInfo*>& o);
};
WRITE_CLASS_ENCODER(ObjectCacheInfo)

struct RGWCacheNotifyInfo {
  uint32_t op;
  rgw_raw_obj obj;
  ObjectCacheInfo obj_info;
  off_t ofs;
  string ns;

  RGWCacheNotifyInfo() : op(0), ofs(0) {}

  void encode(bufferlist& obl) const {
    ENCODE_START(2, 2, obl);
    encode(op, obl);
    encode(obj, obl);
    encode(obj_info, obl);
    encode(ofs, obl);
    encode(ns, obl);
    ENCODE_FINISH(obl);
  }
  void decode(bufferlist::const_iterator& ibl) {
    DECODE_START_LEGACY_COMPAT_LEN(2, 2, 2, ibl);
    decode(op, ibl);
    decode(obj, ibl);
    decode(obj_info, ibl);
    decode(ofs, ibl);
    decode(ns, ibl);
    DECODE_FINISH(ibl);
  }
  void dump(Formatter *f) const;
  static void generate_test_instances(list<RGWCacheNotifyInfo*>& o);
};
WRITE_CLASS_ENCODER(RGWCacheNotifyInfo)
inline std::ostream& operator <<(std::ostream& m, const RGWCacheNotifyInfo& cni) {
  return m << "[op: " << cni.op << ", obj: " << cni.obj
	   << ", ofs" << cni.ofs << ", ns" << cni.ns << "]";
}


class RGWChainedCache {
public:
  virtual ~RGWChainedCache() {}
  virtual void chain_cb(const string& key, void *data) = 0;
  virtual void invalidate(const string& key) = 0;
  virtual void invalidate_all() = 0;
  virtual void unregistered() {}

  struct Entry {
    RGWChainedCache *cache;
    const string& key;
    void *data;

    Entry(RGWChainedCache *_c, const string& _k, void *_d) : cache(_c), key(_k), data(_d) {}
  };
};


struct ObjectCacheEntry {
  ObjectCacheInfo info;
  std::list<string>::iterator lru_iter;
  uint64_t lru_promotion_ts;
  uint64_t gen;
  std::vector<pair<RGWChainedCache *, string> > chained_entries;

  ObjectCacheEntry() : lru_promotion_ts(0), gen(0) {}
};

class ObjectCache {
  std::unordered_map<string, ObjectCacheEntry> cache_map;
  std::list<string> lru;
  unsigned long lru_size;
  unsigned long lru_counter;
  unsigned long lru_window;
  ceph::shared_mutex lock = ceph::make_shared_mutex("ObjectCache");
  CephContext *cct;

  vector<RGWChainedCache *> chained_cache;

  bool enabled;
  ceph::timespan expiry;

  void touch_lru(const DoutPrefixProvider *dpp, const string& name, ObjectCacheEntry& entry,
		 std::list<string>::iterator& lru_iter);
  void remove_lru(const string& name, std::list<string>::iterator& lru_iter);
  void invalidate_lru(ObjectCacheEntry& entry);

  void do_invalidate_all();

public:
  ObjectCache() : lru_size(0), lru_counter(0), lru_window(0), cct(NULL), enabled(false) { }
  ~ObjectCache();
  int get(const DoutPrefixProvider *dpp, const std::string& name, ObjectCacheInfo& bl, uint32_t mask, rgw_cache_entry_info *cache_info);
  std::optional<ObjectCacheInfo> get(const DoutPrefixProvider *dpp, const std::string& name) {
    std::optional<ObjectCacheInfo> info{std::in_place};
    auto r = get(dpp, name, *info, 0, nullptr);
    return r < 0 ? std::nullopt : info;
  }

  template<typename F>
  void for_each(const F& f) {
    std::shared_lock l{lock};
    if (enabled) {
      auto now  = ceph::coarse_mono_clock::now();
      for (const auto& [name, entry] : cache_map) {
        if (expiry.count() && (now - entry.info.time_added) < expiry) {
          f(name, entry);
        }
      }
    }
  }

  void put(const DoutPrefixProvider *dpp, const std::string& name, ObjectCacheInfo& bl, rgw_cache_entry_info *cache_info);
  bool invalidate_remove(const DoutPrefixProvider *dpp, const std::string& name);
  void set_ctx(CephContext *_cct) {
    cct = _cct;
    lru_window = cct->_conf->rgw_cache_lru_size / 2;
    expiry = std::chrono::seconds(cct->_conf.get_val<uint64_t>(
						"rgw_cache_expiry_interval"));
  }
  bool chain_cache_entry(const DoutPrefixProvider *dpp,
                         std::initializer_list<rgw_cache_entry_info*> cache_info_entries,
			 RGWChainedCache::Entry *chained_entry);

  void set_enabled(bool status);

  void chain_cache(RGWChainedCache *cache);
  void unchain_cache(RGWChainedCache *cache);
  void invalidate_all();
};

#endif
