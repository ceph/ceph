// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_RGWCACHE_H
#define CEPH_RGWCACHE_H

#include "rgw_rados.h"
#include <string>
#include <map>
#include "include/types.h"
#include "include/utime.h"
#include "include/assert.h"
#include "common/RWLock.h"
#include "rgw_l2_requester.h"

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

struct get_obj_data;

vector<string> str_split(const string &s, char * delim);

struct ObjectMetaInfo {
  uint64_t size;
  real_time mtime;

  ObjectMetaInfo() : size(0) {}

  void encode(bufferlist& bl) const {
    ENCODE_START(2, 2, bl);
    ::encode(size, bl);
    ::encode(mtime, bl);
    ENCODE_FINISH(bl);
  }
  void decode(bufferlist::iterator& bl) {
    DECODE_START_LEGACY_COMPAT_LEN(2, 2, 2, bl);
    ::decode(size, bl);
    ::decode(mtime, bl);
    DECODE_FINISH(bl);
  }
  void dump(Formatter *f) const;
  static void generate_test_instances(list<ObjectMetaInfo*>& o);
};
WRITE_CLASS_ENCODER(ObjectMetaInfo)

struct ObjectCacheInfo {
  int status;
  uint32_t flags;
  uint64_t epoch;
  bufferlist data;
  map<string, bufferlist> xattrs;
  map<string, bufferlist> rm_xattrs;
  ObjectMetaInfo meta;
  obj_version version;

  ObjectCacheInfo() : status(0), flags(0), epoch(0), version() {}

  void encode(bufferlist& bl) const {
    ENCODE_START(5, 3, bl);
    ::encode(status, bl);
    ::encode(flags, bl);
    ::encode(data, bl);
    ::encode(xattrs, bl);
    ::encode(meta, bl);
    ::encode(rm_xattrs, bl);
    ::encode(epoch, bl);
    ::encode(version, bl);
    ENCODE_FINISH(bl);
  }
  void decode(bufferlist::iterator& bl) {
    DECODE_START_LEGACY_COMPAT_LEN(5, 3, 3, bl);
    ::decode(status, bl);
    ::decode(flags, bl);
    ::decode(data, bl);
    ::decode(xattrs, bl);
    ::decode(meta, bl);
    if (struct_v >= 2)
      ::decode(rm_xattrs, bl);
    if (struct_v >= 4)
      ::decode(epoch, bl);
    if (struct_v >= 5)
      ::decode(version, bl);
    DECODE_FINISH(bl);
  }
  void dump(Formatter *f) const;
  static void generate_test_instances(list<ObjectCacheInfo*>& o);
};
WRITE_CLASS_ENCODER(ObjectCacheInfo)

struct RGWCacheNotifyInfo {
  uint32_t op;
  rgw_obj obj;
  ObjectCacheInfo obj_info;
  off_t ofs;
  string ns;

  RGWCacheNotifyInfo() : op(0), ofs(0) {}

  void encode(bufferlist& obl) const {
    ENCODE_START(2, 2, obl);
    ::encode(op, obl);
    ::encode(obj, obl);
    ::encode(obj_info, obl);
    ::encode(ofs, obl);
    ::encode(ns, obl);
    ENCODE_FINISH(obl);
  }
  void decode(bufferlist::iterator& ibl) {
    DECODE_START_LEGACY_COMPAT_LEN(2, 2, 2, ibl);
    ::decode(op, ibl);
    ::decode(obj, ibl);
    ::decode(obj_info, ibl);
    ::decode(ofs, ibl);
    ::decode(ns, ibl);
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
  std::list<pair<RGWChainedCache *, string> > chained_entries;

  ObjectCacheEntry() : lru_promotion_ts(0), gen(0) {}
};

class ObjectCache {
  std::map<string, ObjectCacheEntry> cache_map;
  std::list<string> lru;
  unsigned long lru_size;
  unsigned long lru_counter;
  unsigned long lru_window;
  RWLock lock;
  CephContext *cct;

  list<RGWChainedCache *> chained_cache;

  bool enabled;

  void touch_lru(string& name, ObjectCacheEntry& entry, std::list<string>::iterator& lru_iter);
  void remove_lru(string& name, std::list<string>::iterator& lru_iter);

  void do_invalidate_all();
public:
  ObjectCache() : lru_size(0), lru_counter(0), lru_window(0), lock("ObjectCache"), cct(NULL), enabled(false) { }
  int get(std::string& name, ObjectCacheInfo& bl, uint32_t mask, rgw_cache_entry_info *cache_info);
  void put(std::string& name, ObjectCacheInfo& bl, rgw_cache_entry_info *cache_info);
  void remove(std::string& name);
  void set_ctx(CephContext *_cct) {
    cct = _cct;
    lru_window = cct->_conf->rgw_cache_lru_size / 2;
  }
  bool chain_cache_entry(list<rgw_cache_entry_info *>& cache_info_entries, RGWChainedCache::Entry *chained_entry);

  void set_enabled(bool status);

  void chain_cache(RGWChainedCache *cache);
  void invalidate_all();
};

template <class T>
class RGWCache  : public T
{
  ObjectCache cache;

  int list_objects_raw_init(rgw_bucket& bucket, RGWAccessHandle *handle) {
    return T::list_objects_raw_init(bucket, handle);
  }
  int list_objects_raw_next(RGWObjEnt& obj, RGWAccessHandle *handle) {
    return T::list_objects_raw_next(obj, handle);
  }

  string normal_name(rgw_bucket& bucket, const std::string& oid) {
    string& bucket_name = bucket.name;
    char buf[bucket_name.size() + 1 + oid.size() + 1];
    const char *bucket_str = bucket_name.c_str();
    const char *oid_str = oid.c_str();
    sprintf(buf, "%s+%s", bucket_str, oid_str);
    return string(buf);
  }

  void normalize_bucket_and_obj(rgw_bucket& src_bucket, const string& src_obj, rgw_bucket& dst_bucket, string& dst_obj);
  string normal_name(rgw_obj& obj) {
    return normal_name(obj.bucket, obj.get_object());
  }

  bool need_watch_notify() {
    return true;
  }

  int distribute_cache(const string& normal_name, rgw_obj& obj, ObjectCacheInfo& obj_info, int op);
  int watch_cb(uint64_t notify_id,
	       uint64_t cookie,
	       uint64_t notifier_id,
	       bufferlist& bl);

  void set_cache_enabled(bool state) {
    cache.set_enabled(state);
  }
public:
  RGWCache() {}

  void register_chained_cache(RGWChainedCache *cc) {
    cache.chain_cache(cc);
  }

 virtual int init_rados() {
    int ret;
    cache.set_ctx(T::cct);
    ret = T::init_rados();
    if (ret < 0)
      return ret;

    return 0;
  }

  int system_obj_set_attrs(void *ctx, rgw_obj& obj, 
                map<string, bufferlist>& attrs,
                map<string, bufferlist>* rmattrs,
                RGWObjVersionTracker *objv_tracker);
  int put_system_obj_impl(rgw_obj& obj, uint64_t size, real_time *mtime,
              map<std::string, bufferlist>& attrs, int flags,
              bufferlist& data,
              RGWObjVersionTracker *objv_tracker,
              real_time set_mtime);
  int put_system_obj_data(void *ctx, rgw_obj& obj, bufferlist& bl, off_t ofs, bool exclusive);

  int get_system_obj(RGWObjectCtx& obj_ctx, RGWRados::SystemObject::Read::GetObjState& read_state,
                     RGWObjVersionTracker *objv_tracker, rgw_obj& obj,
                     bufferlist& bl, off_t ofs, off_t end,
                     map<string, bufferlist> *attrs,
                     rgw_cache_entry_info *cache_info);

  int raw_obj_stat(rgw_obj& obj, uint64_t *psize, real_time *pmtime, uint64_t *epoch, map<string, bufferlist> *attrs,
                   bufferlist *first_chunk, RGWObjVersionTracker *objv_tracker);

  int delete_system_obj(rgw_obj& obj, RGWObjVersionTracker *objv_tracker);

  bool chain_cache_entry(list<rgw_cache_entry_info *>& cache_info_entries, RGWChainedCache::Entry *chained_entry) {
    return cache.chain_cache_entry(cache_info_entries, chained_entry);
  }
};

template <class T>
void RGWCache<T>::normalize_bucket_and_obj(rgw_bucket& src_bucket, const string& src_obj, rgw_bucket& dst_bucket, string& dst_obj)
{
  if (src_obj.size()) {
    dst_bucket = src_bucket;
    dst_obj = src_obj;
  } else {
    dst_bucket = T::get_zone_params().domain_root;
    dst_obj = src_bucket.name;
  }
}

template <class T>
int RGWCache<T>::delete_system_obj(rgw_obj& obj, RGWObjVersionTracker *objv_tracker)
{
  rgw_bucket bucket;
  string oid;
  normalize_bucket_and_obj(obj.bucket, obj.get_object(), bucket, oid);

  string name = normal_name(obj);
  cache.remove(name);

  ObjectCacheInfo info;
  distribute_cache(name, obj, info, REMOVE_OBJ);

  return T::delete_system_obj(obj, objv_tracker);
}

template <class T>
int RGWCache<T>::get_system_obj(RGWObjectCtx& obj_ctx, RGWRados::SystemObject::Read::GetObjState& read_state,
                     RGWObjVersionTracker *objv_tracker, rgw_obj& obj,
                     bufferlist& obl, off_t ofs, off_t end,
                     map<string, bufferlist> *attrs,
                     rgw_cache_entry_info *cache_info)
{
  rgw_bucket bucket;
  string oid;
  normalize_bucket_and_obj(obj.bucket, obj.get_object(), bucket, oid);
  if (ofs != 0)
    return T::get_system_obj(obj_ctx, read_state, objv_tracker, obj, obl, ofs, end, attrs, cache_info);

  string name = normal_name(obj.bucket, oid);

  ObjectCacheInfo info;

  uint32_t flags = CACHE_FLAG_DATA;
  if (objv_tracker)
    flags |= CACHE_FLAG_OBJV;
  if (attrs)
    flags |= CACHE_FLAG_XATTRS;
  
  if (cache.get(name, info, flags, cache_info) == 0) {
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
int RGWCache<T>::system_obj_set_attrs(void *ctx, rgw_obj& obj, 
                           map<string, bufferlist>& attrs,
                           map<string, bufferlist>* rmattrs,
                           RGWObjVersionTracker *objv_tracker) 
{
  rgw_bucket bucket;
  string oid;
  normalize_bucket_and_obj(obj.bucket, obj.get_object(), bucket, oid);
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
  string name = normal_name(bucket, oid);
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
int RGWCache<T>::put_system_obj_impl(rgw_obj& obj, uint64_t size, real_time *mtime,
              map<std::string, bufferlist>& attrs, int flags,
              bufferlist& data,
              RGWObjVersionTracker *objv_tracker,
              real_time set_mtime)
{
  rgw_bucket bucket;
  string oid;
  normalize_bucket_and_obj(obj.bucket, obj.get_object(), bucket, oid);
  ObjectCacheInfo info;
  info.xattrs = attrs;
  info.status = 0;
  info.flags = CACHE_FLAG_XATTRS;
  info.data = data;
  info.flags |= CACHE_FLAG_DATA | CACHE_FLAG_META;
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
  string name = normal_name(bucket, oid);
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
int RGWCache<T>::put_system_obj_data(void *ctx, rgw_obj& obj, bufferlist& data, off_t ofs, bool exclusive)
{
  rgw_bucket bucket;
  string oid;
  normalize_bucket_and_obj(obj.bucket, obj.get_object(), bucket, oid);
  ObjectCacheInfo info;
  bool cacheable = false;
  if ((ofs == 0) || (ofs == -1)) {
    cacheable = true;
    info.data = data;
    info.meta.size = data.length();
    info.status = 0;
    info.flags = CACHE_FLAG_DATA;
  }
  int ret = T::put_system_obj_data(ctx, obj, data, ofs, exclusive);
  if (cacheable) {
    string name = normal_name(bucket, oid);
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
int RGWCache<T>::raw_obj_stat(rgw_obj& obj, uint64_t *psize, real_time *pmtime,
                          uint64_t *pepoch, map<string, bufferlist> *attrs,
                          bufferlist *first_chunk, RGWObjVersionTracker *objv_tracker)
{
  rgw_bucket bucket;
  string oid;
  normalize_bucket_and_obj(obj.bucket, obj.get_object(), bucket, oid);

  string name = normal_name(bucket, oid);

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
int RGWCache<T>::distribute_cache(const string& normal_name, rgw_obj& obj, ObjectCacheInfo& obj_info, int op)
{
  RGWCacheNotifyInfo info;

  info.op = op;

  info.obj_info = obj_info;
  info.obj = obj;
  bufferlist bl;
  ::encode(info, bl);
  int ret = T::distribute(normal_name, bl);
  return ret;
}

template <class T>
int RGWCache<T>::watch_cb(uint64_t notify_id,
			  uint64_t cookie,
			  uint64_t notifier_id,
			  bufferlist& bl)
{
  RGWCacheNotifyInfo info;

  try {
    bufferlist::iterator iter = bl.begin();
    ::decode(info, iter);
  } catch (buffer::end_of_buffer& err) {
    mydout(0) << "ERROR: got bad notification" << dendl;
    return -EIO;
  } catch (buffer::error& err) {
    mydout(0) << "ERROR: buffer::error" << dendl;
    return -EIO;
  }

  rgw_bucket bucket;
  string oid;
  normalize_bucket_and_obj(info.obj.bucket, info.obj.get_object(), bucket, oid);
  string name = normal_name(bucket, oid);
  
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

/* dataCache */
struct BlockInfo {
  CephContext *cct;
  uint64_t size;
  time_t access_time;
  string address;
  string oid;
  atomic_t ref_cnt;
  BlockInfo(CephContext *_cct): cct(_cct), size(0) {}

  void get () {
    ref_cnt.inc();
  }

  void put () {
    if (!ref_cnt.dec()) {
      delete this;
    }
  }
};

struct cacheAioWriteRequest{
  string oid;
  struct aiocb *cb;
  void *priv;
  CephContext *cct;
  atomic_t ref_cnt;

  cacheAioWriteRequest(CephContext *_cct) : cct(_cct) {}
  int prep_write(const char *data, size_t len, const char *fname);
  
  void get () {
    ref_cnt.inc();
  }

  void put () {
    if (!ref_cnt.dec()) {
      delete this;
    }
  }

  void release() {
    ::close(cb->aio_fildes);
    free((void *)cb->aio_buf);
    free(cb);
    put();
  }
};

class DataCache {
private:
  CephContext *cct;
  Mutex  eviction_lock;
  Mutex  cache_lock;
  string cache_location;
  std::map<string, BlockInfo*> cache_map;
  std::list<string> outstanding_write_list;
  L2CacheThreadPool *tp;
  std::vector<string> dist_hosts;
  
  struct {
    size_t free_sz;
    size_t pending_sz;
    size_t size;
  } metadata;

public:
  struct {
    unsigned char l1: 4;
    unsigned char l2: 4;
  } enable;

  string localhost;

public:
  DataCache();
  ~DataCache() {}

  bool get (string oid);
  void put (const char *, size_t len, string oid);
  
  string persistent_hash(string oid);

  // L1 Functions
  int  io_write (const char *data, size_t len, string oid);
  int  aio_write (const char *data, size_t len, string oid);
  void aio_write_cb (cacheAioWriteRequest *c);
  size_t random_eviction();
  
  // L2 Functions
  void submit_l2_read(struct L2CacheRequest *l2request);
  void init_l2_request_cb(librados::completion_t c, void *arg);
  string validate_location(string location);
  void init(CephContext *_cct) {
    char dilm = ';';
    cct = _cct;
    metadata.free_sz = cct->_conf->rgw_data_cache_size;
    localhost = cct->_conf->host;
    dist_hosts = str_split(cct->_conf->rgw_l2_host, &dilm);
    cache_location = validate_location(cct->_conf->rgw_data_cache_location);
    enable.l1 = cct->_conf->rgw_enable_l1;
    enable.l2 = cct->_conf->rgw_enable_l2;
  }
  void add_io();
};        

template <class T>
class RGWDataCache  : public RGWCache<T>
{
  DataCache dcache;
  
public:
  RGWDataCache(); 
  ~RGWDataCache();

  int init_rados(); 
  int get_obj_iterate_cb(RGWObjectCtx *ctx, RGWObjState *astate,
                        rgw_obj& obj,
                        off_t obj_ofs, off_t read_ofs, off_t len,
                        bool is_head_obj, void *arg);
  int flush_read_list(struct get_obj_data *d);
  void get_obj_aio_completion_cb(librados::completion_t cb, void *arg);
};

template <class T>
RGWDataCache<T>::RGWDataCache() {

}

template <class T>
RGWDataCache<T>::~RGWDataCache() {

}

template <class T>
int RGWDataCache<T>::init_rados() {
  int ret = 0;
  dcache.init(T::cct);
  ret = T::init_rados();
  if (ret < 0)
    return ret;

  return 0;
}

template <class T>
int RGWDataCache<T>::get_obj_iterate_cb(RGWObjectCtx *ctx, RGWObjState *astate,
                rgw_obj& obj,
                off_t obj_ofs, off_t read_ofs, off_t len,
                bool is_head_obj, void *arg){

  RGWObjectCtx *rctx = static_cast<RGWObjectCtx *>(ctx);
  librados::ObjectReadOperation op;
  struct get_obj_data *d = (struct get_obj_data *)arg;
  string oid, key;
  rgw_bucket bucket;
  bufferlist *pbl;
  librados::AioCompletion *c;
  string dest = ""; 
  int r = 0;

  if (is_head_obj) {
    /* only when reading from the head object do we need to do the atomic test */
    r = T::append_atomic_test(rctx, obj, op, &astate);
    if (r < 0)
      return r;

    if (astate &&
        obj_ofs < astate->data.length()) {
      unsigned chunk_len = min((uint64_t)astate->data.length() - obj_ofs, (uint64_t)len);
      d->data_lock.Lock();
      r = d->client_cb->handle_data(astate->data, obj_ofs, chunk_len);
      d->data_lock.Unlock();
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

 get_obj_bucket_and_oid_loc(obj, bucket, oid, key);

 d->throttle.get(len);
 if (d->is_cancelled()) {
   return d->get_err_code();
 }

 /* add io after we check that we're not cancelled, otherwise we're going to have trouble
  * cleaning up
 */
  d->add_io(obj_ofs, len, oid, &pbl, &c);
  mydout(20) << "rados->get_obj_iterate_cb oid=" << oid << " obj-ofs=" << obj_ofs << " read_ofs=" << read_ofs << " len=" << len << dendl;
  librados::IoCtx io_ctx(d->io_ctx); // Get a pointer to Librados
  io_ctx.locator_set_key(key);
  
  if (dcache.get(oid)) {
    L1CacheRequest *cc;
    d->add_l1_request(&cc, pbl, oid, len, obj_ofs, read_ofs, key, c);
    //r = io_ctx.cache_aio_operate(oid, cc);
    r = d->submit_l1_aio_read(cc);
    if (r < 0) {
      op.read(read_ofs, len, pbl, NULL);     
      r = io_ctx.aio_operate(oid, c, &op, NULL);            
      if (r < 0) 
        goto done_err;
    }
  }
  else if ((dcache.enable.l2) && ((dest=dcache.persistent_hash(oid)).compare(dcache.localhost) != 0)){
    L2CacheRequest *cc;
    d->add_l2_request(&cc, pbl,  oid, dest, obj_ofs, read_ofs, len, key, c);
    // r = io_ctx.cache_aio_operate(oid, cc);
    dcache.submit_l2_read(cc);
  }
  else{
    op.read(read_ofs, len, pbl, NULL); 
    r = io_ctx.aio_operate(oid, c, &op, NULL);  
    if (r < 0) 
       goto done_err;
  }
                 
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

template <class T>
int RGWDataCache<T>::flush_read_list(struct get_obj_data *d) {

  d->data_lock.Lock();
  list<bufferlist> l;
  l.swap(d->read_list);
  d->get();
  d->read_list.clear();
  d->data_lock.Unlock();
  string oid;
  int r = 0;
  list<bufferlist>::iterator iter;
  for (iter = l.begin(); iter != l.end(); ++iter) {
    bufferlist& bl = *iter;
    
    oid = d->get_pending_oid();
    if ((bl.length() == 0x400000))
      dcache.put(bl.c_str(), bl.length(), oid);

    r = d->client_cb->handle_data(bl, 0, bl.length());
    if (r < 0) {
      mydout(0) << "ERROR: flush_read_list(): d->client_c->handle_data() returned " << r << dendl;
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

template <class T>
void RGWDataCache<T>::get_obj_aio_completion_cb(librados::completion_t cb, void *arg) {
  
  struct get_obj_aio_data *aio_data = (struct get_obj_aio_data *)arg;
  struct get_obj_data *d = aio_data->op_data;
  off_t ofs = aio_data->ofs;
  off_t len = aio_data->len;

  list<bufferlist> bl_list;
  list<bufferlist>::iterator iter;
  int r;

  mydout(20) << "get_obj_aio_completion_cb: io completion ofs=" << ofs << " len=" << len << dendl;
  d->throttle.put(len);

  if (d->is_cancelled()) {
    goto done;
  }

  d->data_lock.Lock();

  r = d->get_complete_ios(ofs, bl_list);
  if (r < 0) {
    goto done_unlock;
  }
  d->read_list.splice(d->read_list.end(), bl_list);

done_unlock:
  d->data_lock.Unlock();
done:
  d->put();
  return;
}
/* dataCache */
#endif
