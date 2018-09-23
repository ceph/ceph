// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_RGWCACHE_H
#define CEPH_RGWCACHE_H

#include "rgw_rados.h"
#include <string>
#include <map>
#include <curl/curl.h>
#include <unordered_map>

#include <unistd.h> /*engage1*/
#include <signal.h> /*engage1*/
#include "include/Context.h" /*engage1*/

#include "include/types.h"
#include "include/utime.h"
#include "include/ceph_assert.h"
#include "common/RWLock.h"


#include "include/lru.h" /*engage1*/
#include "rgw_threadpool.h"

enum {
  UPDATE_OBJ,
  REMOVE_OBJ,
};

#define CACHE_FLAG_DATA           0x01
#define CACHE_FLAG_XATTRS         0x02
#define CACHE_FLAG_META           0x04
#define CACHE_FLAG_MODIFY_XATTRS  0x08
#define CACHE_FLAG_OBJV           0x10

#define mydout(v) lsubdout(T::cct, rgw, v)

/*engage1*/
struct DataCache;
class L2CacheThreadPool;
class HttpL2Request;

struct ChunkDataInfo : public LRUObject {
	CephContext *cct;
	uint64_t size;
	time_t access_time;
	string address;
	string oid;
	bool complete;
	struct ChunkDataInfo *lru_prev;
	struct ChunkDataInfo *lru_next;	

	ChunkDataInfo(): size(0) {}

	void set_ctx(CephContext *_cct) {
		cct = _cct;
	}

	void dump(Formatter *f) const;
	static void generate_test_instances(list<ChunkDataInfo*>& o);
};

struct cacheAioWriteRequest{
	string oid;
	void *data;
	int fd;
	struct aiocb *cb;
	DataCache *priv_data;
	CephContext *cct;
	
	cacheAioWriteRequest(CephContext *_cct) : cct(_cct) {}
	int create_io(bufferlist& bl, unsigned int len, string oid);
	
	void release() {
		::close(fd);
		cb->aio_buf = NULL;
		free(data);
		data = NULL;
		free(cb);
		free(this);
	}
};

struct DataCache {

private:
  std::map<string, ChunkDataInfo*> cache_map;
  std::list<string> outstanding_write_list;
  int index;
  RWLock lock;
  Mutex  cache_lock;
  Mutex  req_lock;
  Mutex  eviction_lock;
  CephContext *cct;
  enum _io_type { 
    SYNC_IO = 1,
    ASYNC_IO = 2,
    SEND_FILE = 3
  } io_type;

  struct sigaction action;
  long long free_data_cache_size = 0;
  long long outstanding_write_size;
  L2CacheThreadPool *tp;
  struct ChunkDataInfo *head;
  struct ChunkDataInfo *tail;

private:
  void add_io();

public:
  DataCache();
  ~DataCache() {}

  bool get(string oid);
  void put(bufferlist& bl, unsigned int len, string obj_key);
  int io_write(bufferlist& bl, unsigned int len, std::string oid);
  int create_aio_write_request(bufferlist& bl, unsigned int len, std::string oid);
  void cache_aio_write_completion_cb(cacheAioWriteRequest *c);
  size_t random_eviction();
  size_t lru_eviction();
  std::string hash_uri(std::string dest);
  std::string deterministic_hash(std::string oid);
  void remote_io(struct librados::L2CacheRequest *l2request); 
  void init_l2_request_cb(librados::completion_t c, void *arg);
  void push_l2_request(librados::L2CacheRequest*l2request);
  void l2_http_request(off_t ofs , off_t len, std::string oid);
  void init(CephContext *_cct) {
    cct = _cct;
    free_data_cache_size = cct->_conf->rgw_datacache_size;
    head = NULL;
    tail = NULL;
  }

  void lru_insert_head(struct ChunkDataInfo *o) {
    o->lru_next = head;
    o->lru_prev = NULL;
    if (head) {
      head->lru_prev = o;
    } else {
      tail = o;
    }
    head = o;
  }
  void lru_insert_tail(struct ChunkDataInfo *o) {
    o->lru_next = NULL;
    o->lru_prev = tail;
    if (tail) {
      tail->lru_next = o;
    } else {
      head = o;
    }
    tail = o;
  }

  void lru_remove(struct ChunkDataInfo *o) {
    if (o->lru_next)
      o->lru_next->lru_prev = o->lru_prev;
    else
      tail = o->lru_prev;
    if (o->lru_prev)
      o->lru_prev->lru_next = o->lru_next;
    else
      head = o->lru_next;
    o->lru_next = o->lru_prev = NULL;
  }
};

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
  RWLock lock;
  CephContext *cct;

  vector<RGWChainedCache *> chained_cache;

  bool enabled;
  ceph::timespan expiry;

  void touch_lru(const string& name, ObjectCacheEntry& entry,
		 std::list<string>::iterator& lru_iter);
  void remove_lru(const string& name, std::list<string>::iterator& lru_iter);
  void invalidate_lru(ObjectCacheEntry& entry);

  void do_invalidate_all();
public:
  ObjectCache() : lru_size(0), lru_counter(0), lru_window(0), lock("ObjectCache"), cct(NULL), enabled(false) { }
  int get(const std::string& name, ObjectCacheInfo& bl, uint32_t mask, rgw_cache_entry_info *cache_info);
  std::optional<ObjectCacheInfo> get(const std::string& name) {
    std::optional<ObjectCacheInfo> info{std::in_place};
    auto r = get(name, *info, 0, nullptr);
    return r < 0 ? std::nullopt : info;
  }

  template<typename F>
  void for_each(const F& f) {
    RWLock::RLocker l(lock);
    if (enabled) {
      auto now  = ceph::coarse_mono_clock::now();
      for (const auto& [name, entry] : cache_map) {
        if (expiry.count() && (now - entry.info.time_added) < expiry) {
          f(name, entry);
        }
      }
    }
  }

  void put(const std::string& name, ObjectCacheInfo& bl, rgw_cache_entry_info *cache_info);
  bool remove(const std::string& name);
  void set_ctx(CephContext *_cct) {
    cct = _cct;
    lru_window = cct->_conf->rgw_cache_lru_size / 2;
    expiry = std::chrono::seconds(cct->_conf.get_val<uint64_t>(
						"rgw_cache_expiry_interval"));
  }
  bool chain_cache_entry(std::initializer_list<rgw_cache_entry_info*> cache_info_entries,
			 RGWChainedCache::Entry *chained_entry);

  void set_enabled(bool status);

  void chain_cache(RGWChainedCache *cache);
  void invalidate_all();
};

template <class T>
class RGWCache  : public T
{
  ObjectCache cache;

  int list_objects_raw_init(rgw_pool& pool, RGWAccessHandle *handle) {
    return T::list_objects_raw_init(pool, handle);
  }
  int list_objects_raw_next(rgw_bucket_dir_entry& obj, RGWAccessHandle *handle) {
    return T::list_objects_raw_next(obj, handle);
  }

  string normal_name(rgw_pool& pool, const std::string& oid) {
    std::string buf;
    buf.reserve(pool.name.size() + pool.ns.size() + oid.size() + 2);
    buf.append(pool.name).append("+").append(pool.ns).append("+").append(oid);
    return buf;
  }

  void normalize_pool_and_obj(rgw_pool& src_pool, const string& src_obj, rgw_pool& dst_pool, string& dst_obj);

  bool need_watch_notify() override {
    return true;
  }

  int distribute_cache(const string& normal_name, rgw_raw_obj& obj, ObjectCacheInfo& obj_info, int op);
  int watch_cb(uint64_t notify_id,
	       uint64_t cookie,
	       uint64_t notifier_id,
	       bufferlist& bl) override;

  void set_cache_enabled(bool state) override {
    cache.set_enabled(state);
  }
public:
  RGWCache() {}

  void register_chained_cache(RGWChainedCache *cc) override {
    cache.chain_cache(cc);
  }

  int init_rados() override {
    int ret;
    cache.set_ctx(T::cct);
    ret = T::init_rados();
    if (ret < 0)
      return ret;

    return 0;
  }


  int system_obj_set_attrs(void *ctx, rgw_raw_obj& obj, 
                map<string, bufferlist>& attrs,
                map<string, bufferlist>* rmattrs,
                RGWObjVersionTracker *objv_tracker);
  int put_system_obj_impl(rgw_raw_obj& obj, uint64_t size, real_time *mtime,
              map<std::string, bufferlist>& attrs, int flags,
              const bufferlist& data,
              RGWObjVersionTracker *objv_tracker,
              real_time set_mtime) override;
  int put_system_obj_data(void *ctx, rgw_raw_obj& obj, const bufferlist& bl, off_t ofs, bool exclusive,
                          RGWObjVersionTracker *objv_tracker = nullptr) override;

  int get_system_obj(RGWObjectCtx& obj_ctx, RGWRados::SystemObject::Read::GetObjState& read_state,
                     RGWObjVersionTracker *objv_tracker, rgw_raw_obj& obj,
                     bufferlist& bl, off_t ofs, off_t end,
                     map<string, bufferlist> *attrs,
                     rgw_cache_entry_info *cache_info,
		     boost::optional<obj_version> refresh_version = boost::none) override;

  int raw_obj_stat(rgw_raw_obj& obj, uint64_t *psize, real_time *pmtime, uint64_t *epoch, map<string, bufferlist> *attrs,
                   bufferlist *first_chunk, RGWObjVersionTracker *objv_tracker) override;

  int delete_system_obj(rgw_raw_obj& obj, RGWObjVersionTracker *objv_tracker) override;

  bool chain_cache_entry(std::initializer_list<rgw_cache_entry_info *> cache_info_entries, RGWChainedCache::Entry *chained_entry) override {
    return cache.chain_cache_entry(cache_info_entries, chained_entry);
  }
  void call_list(const std::optional<std::string>& filter,
		 Formatter* format) override;
  bool call_inspect(const std::string& target, Formatter* format) override;
  bool call_erase(const std::string& target) override;
  void call_zap() override;
};

template <class T>
void RGWCache<T>::normalize_pool_and_obj(rgw_pool& src_pool, const string& src_obj, rgw_pool& dst_pool, string& dst_obj)
{
  if (src_obj.size()) {
    dst_pool = src_pool;
    dst_obj = src_obj;
  } else {
    dst_pool = T::get_zone_params().domain_root;
    dst_obj = src_pool.name;
  }
}

template <class T>
int RGWCache<T>::delete_system_obj(rgw_raw_obj& obj, RGWObjVersionTracker *objv_tracker)
{
  rgw_pool pool;
  string oid;
  normalize_pool_and_obj(obj.pool, obj.oid, pool, oid);

  string name = normal_name(pool, oid);
  cache.remove(name);

  ObjectCacheInfo info;
  distribute_cache(name, obj, info, REMOVE_OBJ);

  return T::delete_system_obj(obj, objv_tracker);
}

template <class T>
int RGWCache<T>::get_system_obj(RGWObjectCtx& obj_ctx, RGWRados::SystemObject::Read::GetObjState& read_state,
                     RGWObjVersionTracker *objv_tracker, rgw_raw_obj& obj,
                     bufferlist& obl, off_t ofs, off_t end,
                     map<string, bufferlist> *attrs,
		     rgw_cache_entry_info *cache_info,
		     boost::optional<obj_version> refresh_version)
{
  rgw_pool pool;
  string oid;
  normalize_pool_and_obj(obj.pool, obj.oid, pool, oid);
  if (ofs != 0)
    return T::get_system_obj(obj_ctx, read_state, objv_tracker, obj, obl, ofs, end, attrs, cache_info);

  string name = normal_name(pool, oid);

  ObjectCacheInfo info;

  uint32_t flags = CACHE_FLAG_DATA;
  if (objv_tracker)
    flags |= CACHE_FLAG_OBJV;
  if (attrs)
    flags |= CACHE_FLAG_XATTRS;

  if ((cache.get(name, info, flags, cache_info) == 0) &&
      (!refresh_version || !info.version.compare(&(*refresh_version)))) {
    if (info.status < 0)
      return info.status;

    bufferlist& bl = info.data;

    bufferlist::iterator i = bl.begin();

    obl.clear();

    i.copy_all(obl);
    if (objv_tracker)
      objv_tracker->read_version = info.version;
    if (attrs)
      *attrs = info.xattrs;
    return bl.length();
  }
  int r = T::get_system_obj(obj_ctx, read_state, objv_tracker, obj, obl, ofs, end, attrs, cache_info);
  if (r < 0) {
    if (r == -ENOENT) { // only update ENOENT, we'd rather retry other errors
      info.status = r;
      cache.put(name, info, cache_info);
    }
    return r;
  }

  if (obl.length() == end + 1) {
    /* in this case, most likely object contains more data, we can't cache it */
    return r;
  }

  bufferptr p(r);
  bufferlist& bl = info.data;
  bl.clear();
  bufferlist::iterator o = obl.begin();
  o.copy_all(bl);
  info.status = 0;
  info.flags = flags;
  if (objv_tracker) {
    info.version = objv_tracker->read_version;
  }
  if (attrs) {
    info.xattrs = *attrs;
  }
  cache.put(name, info, cache_info);
  return r;
}

template <class T>
int RGWCache<T>::system_obj_set_attrs(void *ctx, rgw_raw_obj& obj, 
                           map<string, bufferlist>& attrs,
                           map<string, bufferlist>* rmattrs,
                           RGWObjVersionTracker *objv_tracker) 
{
  rgw_pool pool;
  string oid;
  normalize_pool_and_obj(obj.pool, obj.oid, pool, oid);
  ObjectCacheInfo info;
  info.xattrs = attrs;
  if (rmattrs)
    info.rm_xattrs = *rmattrs;
  info.status = 0;
  info.flags = CACHE_FLAG_MODIFY_XATTRS;
  if (objv_tracker) {
    info.version = objv_tracker->write_version;
    info.flags |= CACHE_FLAG_OBJV;
  }
  int ret = T::system_obj_set_attrs(ctx, obj, attrs, rmattrs, objv_tracker);
  string name = normal_name(pool, oid);
  if (ret >= 0) {
    cache.put(name, info, NULL);
    int r = distribute_cache(name, obj, info, UPDATE_OBJ);
    if (r < 0)
      mydout(0) << "ERROR: failed to distribute cache for " << obj << dendl;
  } else {
    cache.remove(name);
  }

  return ret;
}

template <class T>
int RGWCache<T>::put_system_obj_impl(rgw_raw_obj& obj, uint64_t size, real_time *mtime,
              map<std::string, bufferlist>& attrs, int flags,
              const bufferlist& data,
              RGWObjVersionTracker *objv_tracker,
              real_time set_mtime)
{
  rgw_pool pool;
  string oid;
  normalize_pool_and_obj(obj.pool, obj.oid, pool, oid);
  ObjectCacheInfo info;
  info.xattrs = attrs;
  info.status = 0;
  info.data = data;
  info.flags = CACHE_FLAG_XATTRS | CACHE_FLAG_DATA | CACHE_FLAG_META;
  if (objv_tracker) {
    info.version = objv_tracker->write_version;
    info.flags |= CACHE_FLAG_OBJV;
  }
  ceph::real_time result_mtime;
  int ret = T::put_system_obj_impl(obj, size, &result_mtime, attrs, flags, data,
				   objv_tracker, set_mtime);
  if (mtime) {
    *mtime = result_mtime;
  }
  info.meta.mtime = result_mtime;
  info.meta.size = size;
  string name = normal_name(pool, oid);
  if (ret >= 0) {
    cache.put(name, info, NULL);
    // Only distribute the cache information if we did not just create
    // the object with the exclusive flag. Note: PUT_OBJ_EXCL implies
    // PUT_OBJ_CREATE. Generally speaking, when successfully creating
    // a system object with the exclusive flag it is not necessary to
    // call distribute_cache, as a) it's unclear whether other RGWs
    // will need that system object in the near-term and b) it
    // generates additional network traffic.
    if (!(flags & PUT_OBJ_EXCL)) {
      int r = distribute_cache(name, obj, info, UPDATE_OBJ);
      if (r < 0)
	mydout(0) << "ERROR: failed to distribute cache for " << obj << dendl;
    }
  } else {
    cache.remove(name);
  }

  return ret;
}

template <class T>
int RGWCache<T>::put_system_obj_data(void *ctx, rgw_raw_obj& obj, const bufferlist& data, off_t ofs, bool exclusive,
                                     RGWObjVersionTracker *objv_tracker)
{
  rgw_pool pool;
  string oid;
  normalize_pool_and_obj(obj.pool, obj.oid, pool, oid);
  ObjectCacheInfo info;
  bool cacheable = false;
  if ((ofs == 0) || (ofs == -1)) {
    cacheable = true;
    info.data = data;
    info.meta.size = data.length();
    info.status = 0;
    info.flags = CACHE_FLAG_DATA;
  }
  if (objv_tracker) {
    info.version = objv_tracker->write_version;
    info.flags |= CACHE_FLAG_OBJV;
  }
  int ret = T::put_system_obj_data(ctx, obj, data, ofs, exclusive, objv_tracker);
  if (cacheable) {
    string name = normal_name(pool, oid);
    if (ret >= 0) {
      cache.put(name, info, NULL);
      int r = distribute_cache(name, obj, info, UPDATE_OBJ);
      if (r < 0)
        mydout(0) << "ERROR: failed to distribute cache for " << obj << dendl;
    } else {
      cache.remove(name);
    }
  }

  return ret;
}

template <class T>
int RGWCache<T>::raw_obj_stat(rgw_raw_obj& obj, uint64_t *psize, real_time *pmtime,
                          uint64_t *pepoch, map<string, bufferlist> *attrs,
                          bufferlist *first_chunk, RGWObjVersionTracker *objv_tracker)
{
  rgw_pool pool;
  string oid;
  normalize_pool_and_obj(obj.pool, obj.oid, pool, oid);

  string name = normal_name(pool, oid);

  uint64_t size;
  real_time mtime;
  uint64_t epoch;

  ObjectCacheInfo info;
  uint32_t flags = CACHE_FLAG_META | CACHE_FLAG_XATTRS;
  if (objv_tracker)
    flags |= CACHE_FLAG_OBJV;
  int r = cache.get(name, info, flags, NULL);
  if (r == 0) {
    if (info.status < 0)
      return info.status;

    size = info.meta.size;
    mtime = info.meta.mtime;
    epoch = info.epoch;
    if (objv_tracker)
      objv_tracker->read_version = info.version;
    goto done;
  }
  r = T::raw_obj_stat(obj, &size, &mtime, &epoch, &info.xattrs, first_chunk, objv_tracker);
  if (r < 0) {
    if (r == -ENOENT) {
      info.status = r;
      cache.put(name, info, NULL);
    }
    return r;
  }
  info.status = 0;
  info.epoch = epoch;
  info.meta.mtime = mtime;
  info.meta.size = size;
  info.flags = CACHE_FLAG_META | CACHE_FLAG_XATTRS;
  if (objv_tracker) {
    info.flags |= CACHE_FLAG_OBJV;
    info.version = objv_tracker->read_version;
  }
  cache.put(name, info, NULL);
done:
  if (psize)
    *psize = size;
  if (pmtime)
    *pmtime = mtime;
  if (pepoch)
    *pepoch = epoch;
  if (attrs)
    *attrs = info.xattrs;
  return 0;
}

template <class T>
int RGWCache<T>::distribute_cache(const string& normal_name, rgw_raw_obj& obj, ObjectCacheInfo& obj_info, int op)
{
  RGWCacheNotifyInfo info;

  info.op = op;

  info.obj_info = obj_info;
  info.obj = obj;
  bufferlist bl;
  encode(info, bl);
  return T::distribute(normal_name, bl);
}

template <class T>
int RGWCache<T>::watch_cb(uint64_t notify_id,
			  uint64_t cookie,
			  uint64_t notifier_id,
			  bufferlist& bl)
{
  RGWCacheNotifyInfo info;

  try {
    auto iter = bl.cbegin();
    decode(info, iter);
  } catch (buffer::end_of_buffer& err) {
    mydout(0) << "ERROR: got bad notification" << dendl;
    return -EIO;
  } catch (buffer::error& err) {
    mydout(0) << "ERROR: buffer::error" << dendl;
    return -EIO;
  }

  rgw_pool pool;
  string oid;
  normalize_pool_and_obj(info.obj.pool, info.obj.oid, pool, oid);
  string name = normal_name(pool, oid);
  
  switch (info.op) {
  case UPDATE_OBJ:
    cache.put(name, info.obj_info, NULL);
    break;
  case REMOVE_OBJ:
    cache.remove(name);
    break;
  default:
    mydout(0) << "WARNING: got unknown notification op: " << info.op << dendl;
    return -EINVAL;
  }

  return 0;
}

template<typename T>
void RGWCache<T>::call_list(const std::optional<std::string>& filter,
			    Formatter* f)
{
  cache.for_each(
    [this, &filter, f] (const string& name, const ObjectCacheEntry& entry) {
      if (!filter || name.find(*filter) != name.npos) {
	T::cache_list_dump_helper(f, name, entry.info.meta.mtime,
				  entry.info.meta.size);
      }
    });
}

template<typename T>
bool RGWCache<T>::call_inspect(const std::string& target, Formatter* f)
{
  if (const auto entry = cache.get(target)) {
    f->open_object_section("cache_entry");
    f->dump_string("name", target.c_str());
    entry->dump(f);
    f->close_section();
    return true;
  } else {
    return false;
  }
}

template<typename T>
bool RGWCache<T>::call_erase(const std::string& target)
{
  return cache.remove(target);
}

template<typename T>
void RGWCache<T>::call_zap()
{
  cache.invalidate_all();
}

template <class T>
class RGWDataCache  : public RGWCache<T>
{

  DataCache   data_cache;

public:
  RGWDataCache() {}

  int init_rados() override {
    int ret;
    data_cache.init(T::cct);
    ret = RGWCache<T>::init_rados();
    if (ret < 0)
      return ret;

    return 0;
  }

  int flush_read_list(struct get_obj_data *d);
  int get_obj_iterate_cb(RGWObjectCtx *ctx, RGWObjState *astate,
      const RGWBucketInfo& bucket_info, const rgw_obj& obj,
      const rgw_raw_obj& read_obj,
      off_t obj_ofs, off_t read_ofs, off_t len,
      bool is_head_obj, void *arg);

};

struct get_obj_data;

template<typename T>
int RGWDataCache<T>::flush_read_list(struct get_obj_data *d) {
  d->data_lock.Lock();
  list<bufferlist> l;
  l.swap(d->read_list);
  d->get();
  d->read_list.clear();

  d->data_lock.Unlock();

  int r = 0;

  std::string oid;
  list<bufferlist>::iterator iter;
  for (iter = l.begin(); iter != l.end(); ++iter) {
    bufferlist& bl = *iter;
    oid = d->get_pending_oid();
    if (bl.length() == 0x400000)
      data_cache.put(bl, bl.length(), oid);
    r = d->client_cb->handle_data(bl, 0, bl.length());
    if (r < 0) {
      mydout(0) << "ERROR: flush_read_list(): d->client_cb->handle_data() returned " << r << dendl;
      break;
    }
  }

  d->data_lock.Lock();
  d->put();
  if (r < 0) {
    d->set_cancelled(r);
  }
  d->data_lock.Unlock();
  return r;

}

template<typename T>
int RGWDataCache<T>::get_obj_iterate_cb(RGWObjectCtx *ctx, RGWObjState *astate,
    const RGWBucketInfo& bucket_info, const rgw_obj& obj,
    const rgw_raw_obj& read_obj,
    off_t obj_ofs, off_t read_ofs, off_t len,
    bool is_head_obj, void *arg) {
  
  RGWObjectCtx *rctx = static_cast<RGWObjectCtx *>(ctx);
  librados::ObjectReadOperation op;
  struct get_obj_data *d = (struct get_obj_data *)arg;
  string oid, key;
  bufferlist *pbl;
  librados::AioCompletion *c;

  int r;

  if (is_head_obj) {
    /* only when reading from the head object do we need to do the atomic test */
    r = T::append_atomic_test(rctx, bucket_info, obj, op, &astate);
    if (r < 0)
      return r;

    if (astate &&
        obj_ofs < astate->data.length()) {
      unsigned chunk_len = std::min((uint64_t)astate->data.length() - obj_ofs, (uint64_t)len);

      r = d->client_cb->handle_data(astate->data, obj_ofs, chunk_len);
      if (r < 0)
        return r;

      d->lock.Lock();
      d->total_read += chunk_len;
      d->lock.Unlock();
	
      len -= chunk_len;
      read_ofs += chunk_len;
      obj_ofs += chunk_len;
      if (!len)
	  return 0;
    }
  }

  d->throttle.get(len);
  if (d->is_cancelled()) {
    return d->get_err_code();
  }

  /* add io after we check that we're not cancelled, otherwise we're going to have trouble
   * cleaning up
   */
  d->add_io(obj_ofs, len, &pbl, &c);

  mydout(20) << "rados->get_obj_iterate_cb oid=" << read_obj.oid << " obj-ofs=" << obj_ofs << " read_ofs=" << read_ofs << " len=" << len << dendl;
  op.read(read_ofs, len, pbl, NULL);

  librados::IoCtx io_ctx(d->io_ctx);
  io_ctx.locator_set_key(read_obj.loc);
  d->add_pending_oid(read_obj.oid);

  if (data_cache.get(read_obj.oid)) {
    librados::L1CacheRequest *cc;
    d->add_l1_request(&cc, pbl, read_obj.oid, len, obj_ofs, read_ofs, key, c);
    r = io_ctx.cache_aio_notifier(read_obj.oid, cc);
    r = d->submit_l1_aio_read(cc);
    if (r != 0 ){
      mydout(0) << "Error cache_aio_read failed err=" << r << dendl;
    }
  } else if (d->deterministic_hash_is_local(read_obj.oid)){
    mydout(20) << "rados->get_obj_iterate_cb oid=" << read_obj.oid << " obj-ofs=" << obj_ofs << " read_ofs=" << read_ofs << " len=" << len << dendl;
    op.read(read_ofs, len, pbl, NULL);
    r = io_ctx.aio_operate(read_obj.oid, c, &op, NULL);
    mydout(20) << "rados->aio_operate r=" << r << " bl.length=" << pbl->length() << dendl;
    if (r < 0) {
      mydout(0) << "rados->aio_operate r=" << r << dendl;
      goto done_err;
    }
  } else {
    librados::L2CacheRequest *cc;
    d->add_l2_request(&cc, pbl, read_obj.oid, obj_ofs, read_ofs, len, key, c);
//    r = d->add_cache_notifier(oid, c);
    r = io_ctx.cache_aio_notifier(read_obj.oid, cc); 
    data_cache.push_l2_request(cc);
  }

  // Flush data to client if there is any
  r = flush_read_list(d);
  if (r < 0)
    return r;

  return 0;

done_err:
  mydout(20) << "cancelling io r=" << r << " obj_ofs=" << obj_ofs << dendl;
  d->set_cancelled(r);
  d->cancel_io(obj_ofs);

  return r;
}

class L2CacheThreadPool {
public:
  L2CacheThreadPool(int n) {
    curl_global_init(CURL_GLOBAL_ALL);
    for (int i=0; i<n; ++i) {
      threads.push_back(new PoolWorkerThread(workQueue));
      threads.back()->start();
    }
  }

  ~L2CacheThreadPool() {
    finish();
  }

  void addTask(Task *nt) {
    workQueue.addTask(nt);
  }

  void finish() {
    for (size_t i=0,e=threads.size(); i<e; ++i)
      workQueue.addTask(NULL);
    for (size_t i=0,e=threads.size(); i<e; ++i) {
      threads[i]->join();
      delete threads[i];
    }
    threads.clear();
  }

private:
  std::vector<PoolWorkerThread*> threads;
  WorkQueue workQueue;
};

class HttpL2Request : public Task {
public:
  HttpL2Request(librados::L2CacheRequest *_req, CephContext *_cct) : Task(), req(_req), cct(_cct) {
    pthread_mutex_init(&qmtx,0);
    pthread_cond_init(&wcond, 0);
  }
  ~HttpL2Request() {
    pthread_mutex_destroy(&qmtx);
    pthread_cond_destroy(&wcond);
  }
  virtual void run();
  virtual void set_handler(void *handle) {
    curl_handle = (CURL *)handle;
  }
private:
  int submit_http_request();
  int sign_request(RGWAccessKey& key, RGWEnv& env, req_info& info);
private:
  pthread_mutex_t qmtx;
  pthread_cond_t wcond;
  librados::L2CacheRequest *req;
  CURL *curl_handle;
  CephContext *cct;
};



#endif
