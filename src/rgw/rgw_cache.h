// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#ifndef CEPH_RGWCACHE_H
#define CEPH_RGWCACHE_H

#include "rgw_rados.h"
#include <string>
#include <map>
#include <curl/curl.h>
#include <unordered_map>


#include "include/types.h"
#include "include/utime.h"
#include "include/ceph_assert.h"
#include "common/ceph_mutex.h"
#include "rgw_common.h"

#include "cls/version/cls_version_types.h"

#include <unistd.h> /*engage1*/
#include <signal.h> /*engage1*/
#include "include/Context.h" /*engage1*/
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
  ceph::mutex cache_lock;
  ceph::mutex req_lock;
  ceph::mutex eviction_lock;
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

  void touch_lru(const string& name, ObjectCacheEntry& entry,
		 std::list<string>::iterator& lru_iter);
  void remove_lru(const string& name, std::list<string>::iterator& lru_iter);
  void invalidate_lru(ObjectCacheEntry& entry);

  void do_invalidate_all();

public:
  ObjectCache() : lru_size(0), lru_counter(0), lru_window(0), cct(NULL), enabled(false) { }
  ~ObjectCache();
  int get(const std::string& name, ObjectCacheInfo& bl, uint32_t mask, rgw_cache_entry_info *cache_info);
  std::optional<ObjectCacheInfo> get(const std::string& name) {
    std::optional<ObjectCacheInfo> info{std::in_place};
    auto r = get(name, *info, 0, nullptr);
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
  void unchain_cache(RGWChainedCache *cache);
  void invalidate_all();
};

// FIXME: #CACHEREBASE // : public RGWSI_SysObj_Cache
/*
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
    // only when reading from the head object do we need to do the atomic test
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

  // add io after we check that we're not cancelled, otherwise we're going to have trouble
  // cleaning up
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
*/
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
