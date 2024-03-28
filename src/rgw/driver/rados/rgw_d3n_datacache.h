// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#pragma once

#include <aio.h>
#include <curl/curl.h>

#include "rgw_common.h"
#include "rgw_rados.h"

#include <unistd.h>
#include <signal.h>
#include "include/Context.h"
#include "include/lru.h"


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

