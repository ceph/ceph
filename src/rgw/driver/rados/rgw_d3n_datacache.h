// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#pragma once

#include "rgw_rados.h"
#include <curl/curl.h>

#include "rgw_common.h"

#include <unistd.h>
#include <signal.h>
#include "include/Context.h"
#include "include/lru.h"
#include "rgw_d3n_cacherequest.h"


/*D3nDataCache*/
struct D3nDataCache;


struct D3nChunkDataInfo : public LRUObject {
	CephContext *cct;
	uint64_t size;
	time_t access_time;
	std::string address;
	std::string oid;
	bool complete;
	struct D3nChunkDataInfo* lru_prev;
	struct D3nChunkDataInfo* lru_next;

	D3nChunkDataInfo(): size(0) {}

	void set_ctx(CephContext *_cct) {
		cct = _cct;
	}

	void dump(Formatter *f) const;
	static void generate_test_instances(std::list<D3nChunkDataInfo*>& o);
};

struct D3nCacheAioWriteRequest {
	std::string oid;
	void *data = nullptr;
	int fd = -1;
	struct aiocb *cb = nullptr;
	D3nDataCache *priv_data = nullptr;
	CephContext *cct = nullptr;

	D3nCacheAioWriteRequest(CephContext *_cct) : cct(_cct) {}
	int d3n_libaio_prepare_write_op(bufferlist& bl, unsigned int len, std::string oid, std::string cache_location);

  ~D3nCacheAioWriteRequest() {
    ::close(fd);
    free(data);
    cb->aio_buf = nullptr;
		delete(cb);
  }
};

struct D3nDataCache {

private:
  std::unordered_map<std::string, D3nChunkDataInfo*> d3n_cache_map;
  std::set<std::string> d3n_outstanding_write_list;
  std::mutex d3n_cache_lock;
  std::mutex d3n_eviction_lock;

  CephContext *cct;
  enum class _io_type {
    SYNC_IO = 1,
    ASYNC_IO = 2,
    SEND_FILE = 3
  } io_type;
  enum class _eviction_policy {
    LRU=0, RANDOM=1
  } eviction_policy;

  struct sigaction action;
  uint64_t free_data_cache_size = 0;
  uint64_t outstanding_write_size = 0;
  struct D3nChunkDataInfo* head;
  struct D3nChunkDataInfo* tail;

private:
  void add_io();

public:
  D3nDataCache();
  ~D3nDataCache() {
    while (lru_eviction() > 0);
  }

  std::string cache_location;

  bool get(const std::string& oid, const off_t len);
  void put(bufferlist& bl, unsigned int len, std::string& obj_key);
  int d3n_io_write(bufferlist& bl, unsigned int len, std::string oid);
  int d3n_libaio_create_write_request(bufferlist& bl, unsigned int len, std::string oid);
  void d3n_libaio_write_completion_cb(D3nCacheAioWriteRequest* c);
  size_t random_eviction();
  size_t lru_eviction();

  void init(CephContext *_cct);

  void lru_insert_head(struct D3nChunkDataInfo* o) {
    lsubdout(g_ceph_context, rgw_datacache, 30) << "D3nDataCache: " << __func__ << "()" << dendl;
    o->lru_next = head;
    o->lru_prev = nullptr;
    if (head) {
      head->lru_prev = o;
    } else {
      tail = o;
    }
    head = o;
  }

  void lru_insert_tail(struct D3nChunkDataInfo* o) {
    lsubdout(g_ceph_context, rgw_datacache, 30) << "D3nDataCache: " << __func__ << "()" << dendl;
    o->lru_next = nullptr;
    o->lru_prev = tail;
    if (tail) {
      tail->lru_next = o;
    } else {
      head = o;
    }
    tail = o;
  }

  void lru_remove(struct D3nChunkDataInfo* o) {
    lsubdout(g_ceph_context, rgw_datacache, 30) << "D3nDataCache: " << __func__ << "()" << dendl;
    if (o->lru_next)
      o->lru_next->lru_prev = o->lru_prev;
    else
      tail = o->lru_prev;
    if (o->lru_prev)
      o->lru_prev->lru_next = o->lru_next;
    else
      head = o->lru_next;
    o->lru_next = o->lru_prev = nullptr;
  }
};


template <class T>
class D3nRGWDataCache : public T {

public:
  D3nRGWDataCache() {}

  int init_rados() override {
    int ret;
    ret = T::init_rados();
    if (ret < 0)
      return ret;

    return 0;
  }

  int get_obj_iterate_cb(const DoutPrefixProvider *dpp, const rgw_raw_obj& read_obj, off_t obj_ofs,
                         off_t read_ofs, off_t len, bool is_head_obj,
                         RGWObjState *astate, void *arg) override;
};

template<typename T>
int D3nRGWDataCache<T>::get_obj_iterate_cb(const DoutPrefixProvider *dpp, const rgw_raw_obj& read_obj, off_t obj_ofs,
                                 off_t read_ofs, off_t len, bool is_head_obj,
                                 RGWObjState *astate, void *arg) {
  lsubdout(g_ceph_context, rgw_datacache, 30) << "D3nDataCache::" << __func__ << "(): is head object : " << is_head_obj << dendl;
  librados::ObjectReadOperation op;
  struct get_obj_data* d = static_cast<struct get_obj_data*>(arg);
  std::string oid, key;

  if (is_head_obj) {
    // only when reading from the head object do we need to do the atomic test
    int r = T::append_atomic_test(dpp, astate, op);
    if (r < 0)
      return r;

    // astate can be modified by append_atomic_test
    // coverity[check_after_deref:SUPPRESS]
    if (astate &&
        obj_ofs < astate->data.length()) {
      unsigned chunk_len = std::min((uint64_t)astate->data.length() - obj_ofs, (uint64_t)len);

      r = d->client_cb->handle_data(astate->data, obj_ofs, chunk_len);
      if (r < 0)
        return r;

      len -= chunk_len;
      d->offset += chunk_len;
      read_ofs += chunk_len;
      obj_ofs += chunk_len;
      if (!len)
        return 0;
    }

    rgw_rados_ref ref;
    r = rgw_get_rados_ref(dpp, d->rgwrados->get_rados_handle(), read_obj, &ref);
    if (r < 0) {
      ldpp_dout(dpp, 4) << "failed to open rados context for " << read_obj << dendl;
      return r;
    }

    ldpp_dout(dpp, 20) << "D3nDataCache::" << __func__ << "(): oid=" << read_obj.oid << " obj-ofs=" << obj_ofs << " read_ofs=" << read_ofs << " len=" << len << dendl;
    op.read(read_ofs, len, nullptr, nullptr);

    const uint64_t cost = len;
    const uint64_t id = obj_ofs; // use logical object offset for sorting replies

    auto completed = d->aio->get(ref.obj, rgw::Aio::librados_op(ref.ioctx, std::move(op), d->yield), cost, id);
    return d->flush(std::move(completed));
  } else {
    ldpp_dout(dpp, 20) << "D3nDataCache::" << __func__ << "(): oid=" << read_obj.oid << ", is_head_obj=" << is_head_obj << ", obj-ofs=" << obj_ofs << ", read_ofs=" << read_ofs << ", len=" << len << dendl;
    int r;

    op.read(read_ofs, len, nullptr, nullptr);

    const uint64_t cost = len;
    const uint64_t id = obj_ofs; // use logical object offset for sorting replies
    oid = read_obj.oid;

    rgw_rados_ref ref;
    r = rgw_get_rados_ref(dpp, d->rgwrados->get_rados_handle(), read_obj, &ref);
    if (r < 0) {
      ldpp_dout(dpp, 4) << "failed to open rados context for " << read_obj << dendl;
      return r;
    }

    const bool is_compressed = (astate->attrset.find(RGW_ATTR_COMPRESSION) != astate->attrset.end());
    const bool is_encrypted = (astate->attrset.find(RGW_ATTR_CRYPT_MODE) != astate->attrset.end());
    if (read_ofs != 0 || astate->size != astate->accounted_size || is_compressed || is_encrypted) {
      d->d3n_bypass_cache_write = true;
      lsubdout(g_ceph_context, rgw, 5) << "D3nDataCache: " << __func__ << "(): Note - bypassing datacache: oid=" << read_obj.oid << ", read_ofs!=0 = " << read_ofs << ", size=" << astate->size << " != accounted_size=" << astate->accounted_size << ", is_compressed=" << is_compressed << ", is_encrypted=" << is_encrypted  << dendl;
      auto completed = d->aio->get(ref.obj, rgw::Aio::librados_op(ref.ioctx, std::move(op), d->yield), cost, id);
      r = d->flush(std::move(completed));
      return r;
    }

    if (d->rgwrados->d3n_data_cache->get(oid, len)) {
      // Read From Cache
      ldpp_dout(dpp, 20) << "D3nDataCache: " << __func__ << "(): READ FROM CACHE: oid=" << read_obj.oid << ", obj-ofs=" << obj_ofs << ", read_ofs=" << read_ofs << ", len=" << len << dendl;
      auto completed = d->aio->get(ref.obj, rgw::Aio::d3n_cache_op(dpp, d->yield, read_ofs, len, d->rgwrados->d3n_data_cache->cache_location), cost, id);
      r = d->flush(std::move(completed));
      if (r < 0) {
        lsubdout(g_ceph_context, rgw, 0) << "D3nDataCache: " << __func__ << "(): Error: failed to drain/flush, r= " << r << dendl;
      }
      return r;
    } else {
      // Write To Cache
      ldpp_dout(dpp, 20) << "D3nDataCache: " << __func__ << "(): WRITE TO CACHE: oid=" << read_obj.oid << ", obj-ofs=" << obj_ofs << ", read_ofs=" << read_ofs << " len=" << len << dendl;
      auto completed = d->aio->get(ref.obj, rgw::Aio::librados_op(ref.ioctx, std::move(op), d->yield), cost, id);
      return d->flush(std::move(completed));
    }
  }
  lsubdout(g_ceph_context, rgw, 1) << "D3nDataCache: " << __func__ << "(): Warning: Check head object cache handling flow, oid=" << read_obj.oid << dendl;

  return 0;
}

