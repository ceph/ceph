// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include "rgw_d3n_datacache.h"
#include "rgw_rest_client.h"
#include "rgw_auth_s3.h"
#include "rgw_op.h"
#include "rgw_common.h"
#include "rgw_auth_s3.h"
#include "rgw_op.h"
#include "rgw_crypt_sanitize.h"
#if defined(__linux__)
#include <features.h>
#endif

#if __has_include(<filesystem>)
#include <filesystem>
namespace efs = std::filesystem;
#else
#include <experimental/filesystem>
namespace efs = std::experimental::filesystem;
#endif

#define dout_subsys ceph_subsys_rgw

using namespace std;

int D3nCacheAioWriteRequest::d3n_libaio_prepare_write_op(bufferlist& bl, unsigned int len, string oid, string cache_location)
{
  std::string location = cache_location + url_encode(oid, true);
  int r = 0;

  lsubdout(g_ceph_context, rgw_datacache, 20) << "D3nDataCache: " << __func__ << "(): Write To Cache, location=" << location << dendl;
  cb = new struct aiocb;
  mode_t mode = S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH;
  memset(cb, 0, sizeof(struct aiocb));
  r = fd = ::open(location.c_str(), O_WRONLY | O_CREAT | O_TRUNC, mode);
  if (fd < 0) {
    ldout(cct, 0) << "ERROR: D3nCacheAioWriteRequest::create_io: open file failed, errno=" << errno << ", location='" << location.c_str() << "'" << dendl;
    return r;
  }
  r = 0;

  if (g_conf()->rgw_d3n_l1_fadvise != POSIX_FADV_NORMAL)
    posix_fadvise(fd, 0, 0, g_conf()->rgw_d3n_l1_fadvise);
  cb->aio_fildes = fd;

  data = malloc(len);
  if (!data) {
    ldout(cct, 0) << "ERROR: D3nCacheAioWriteRequest::create_io: memory allocation failed" << dendl;
    return -1;
  }
  cb->aio_buf = data;
  memcpy((void*)data, bl.c_str(), len);
  cb->aio_nbytes = len;

  return r;
}

D3nDataCache::D3nDataCache()
  : cct(nullptr), io_type(_io_type::ASYNC_IO), free_data_cache_size(0), outstanding_write_size(0)
{
  lsubdout(g_ceph_context, rgw_datacache, 5) << "D3nDataCache: " << __func__ << "()" << dendl;
}

void D3nDataCache::init(CephContext *_cct) {
  cct = _cct;
  // coverity[missing_lock:SUPPRESS]
  free_data_cache_size = cct->_conf->rgw_d3n_l1_datacache_size;
  head = nullptr;
  tail = nullptr;
  cache_location = cct->_conf->rgw_d3n_l1_datacache_persistent_path;
  if(cache_location.back() != '/') {
      cache_location += "/";
  }
  try {
    if (efs::exists(cache_location)) {
      // d3n: evict the cache storage directory
      if (g_conf()->rgw_d3n_l1_evict_cache_on_start) {
        lsubdout(g_ceph_context, rgw, 5) << "D3nDataCache: init: evicting the persistent storage directory on start" << dendl;
        for (auto& p : efs::directory_iterator(cache_location)) {
          efs::remove_all(p.path());
        }
      }
    } else {
      // create the cache storage directory
      lsubdout(g_ceph_context, rgw, 5) << "D3nDataCache: init: creating the persistent storage directory on start" << dendl;
      efs::create_directories(cache_location);
    }
  } catch (const efs::filesystem_error& e) {
    lderr(g_ceph_context) << "D3nDataCache: init: ERROR initializing the cache storage directory '" << cache_location <<
                              "' : " << e.what() << dendl;
  }

  auto conf_eviction_policy = cct->_conf.get_val<std::string>("rgw_d3n_l1_eviction_policy");
  ceph_assert(conf_eviction_policy == "lru" || conf_eviction_policy == "random");
  if (conf_eviction_policy == "lru")
    eviction_policy = _eviction_policy::LRU;
  if (conf_eviction_policy == "random")
    eviction_policy = _eviction_policy::RANDOM;

#if defined(HAVE_LIBAIO) && defined(__GLIBC__)
  // libaio setup
  struct aioinit ainit{0};
  ainit.aio_threads = cct->_conf.get_val<int64_t>("rgw_d3n_libaio_aio_threads");
  ainit.aio_num = cct->_conf.get_val<int64_t>("rgw_d3n_libaio_aio_num");
  ainit.aio_idle_time = 5;
  aio_init(&ainit);
#endif
}

int D3nDataCache::d3n_io_write(bufferlist& bl, unsigned int len, std::string oid)
{
  D3nChunkDataInfo* chunk_info{nullptr};
  std::string location = cache_location + url_encode(oid, true);
  int r = 0;

  lsubdout(g_ceph_context, rgw_datacache, 20) << "D3nDataCache: " << __func__ << "(): location=" << location << dendl;

  {
    size_t nbytes = 0;
    auto file_closer = [&r](FILE* fd) {
      if (fd != nullptr) {
        r = std::fclose(fd);
      }
      fd = nullptr;
    };

    std::unique_ptr<FILE, decltype(file_closer)> cache_file(
      fopen(location.c_str(), "w+"), file_closer);

    if (cache_file == nullptr) {
      ldout(cct, 0) << "ERROR: D3nDataCache::fopen file has return error, errno=" << errno << dendl;
      return -errno;
    }

    nbytes = fwrite(bl.c_str(), 1, len, cache_file.get());
    if (nbytes != len) {
      ldout(cct, 0) << "ERROR: D3nDataCache::io_write: fwrite has returned error: nbytes!=len, nbytes=" << nbytes << ", len=" << len << dendl;
      return -EIO;
    }
  }

  // Check whether fclose returned an error
  if (r != 0) {
    ldout(cct, 0) << "ERROR: D3nDataCache::fclose file has return error, errno=" << errno << dendl;
    return -errno;
  }

  { // update cache_map entries for new chunk in cache
    const std::lock_guard l(d3n_cache_lock);
    chunk_info = new D3nChunkDataInfo;
    chunk_info->oid = oid;
    chunk_info->set_ctx(cct);
    chunk_info->size = len;
    d3n_cache_map.insert(pair<string, D3nChunkDataInfo*>(oid, chunk_info));
  }

  return r;
}

void d3n_libaio_write_cb(sigval sigval)
{
  lsubdout(g_ceph_context, rgw_datacache, 30) << "D3nDataCache: " << __func__ << "()" << dendl;
  D3nCacheAioWriteRequest* c = static_cast<D3nCacheAioWriteRequest*>(sigval.sival_ptr);
  c->priv_data->d3n_libaio_write_completion_cb(c);
}


void D3nDataCache::d3n_libaio_write_completion_cb(D3nCacheAioWriteRequest* c)
{
  D3nChunkDataInfo* chunk_info{nullptr};

  ldout(cct, 5) << "D3nDataCache: " << __func__ << "(): oid=" << c->oid << dendl;

  { // update cache_map entries for new chunk in cache
    const std::lock_guard l(d3n_cache_lock);
    d3n_outstanding_write_list.erase(c->oid);
    chunk_info = new D3nChunkDataInfo;
    chunk_info->oid = c->oid;
    chunk_info->set_ctx(cct);
    chunk_info->size = c->cb->aio_nbytes;
    d3n_cache_map.insert(pair<string, D3nChunkDataInfo*>(c->oid, chunk_info));
  }

  { // update free size
    const std::lock_guard l(d3n_eviction_lock);
    free_data_cache_size -= c->cb->aio_nbytes;
    outstanding_write_size -= c->cb->aio_nbytes;
    lru_insert_head(chunk_info);
  }

  delete c;
  c = nullptr;
}

int D3nDataCache::d3n_libaio_create_write_request(bufferlist& bl, unsigned int len, std::string oid)
{
  lsubdout(g_ceph_context, rgw_datacache, 30) << "D3nDataCache: " << __func__ << "(): Write To Cache, oid=" << oid << ", len=" << len << dendl;
  auto wr = std::make_unique<struct D3nCacheAioWriteRequest>(cct);
  int r = 0;

  if ((r = wr->d3n_libaio_prepare_write_op(bl, len, oid, cache_location)) < 0) {
    ldout(cct, 0) << "ERROR: D3nDataCache: " << __func__ << "() prepare libaio write op r=" << r << dendl;
    return r;
  }
  wr->cb->aio_sigevent.sigev_notify = SIGEV_THREAD;
  wr->cb->aio_sigevent.sigev_notify_function = d3n_libaio_write_cb;
  wr->cb->aio_sigevent.sigev_notify_attributes = nullptr;
  wr->cb->aio_sigevent.sigev_value.sival_ptr = (void*)(wr.get());
  wr->oid = oid;
  wr->priv_data = this;

  if ((r = ::aio_write(wr->cb)) != 0) {
    ldout(cct, 0) << "ERROR: D3nDataCache: " << __func__ << "() aio_write r=" << r << dendl;
    return r;
  }

  // wr will be deleted when the write is successful and d3n_libaio_write_completion_cb gets called
  // coverity[RESOURCE_LEAK:FALSE]
  wr.release();

  return r;
}

void D3nDataCache::put(bufferlist& bl, unsigned int len, std::string& oid)
{
  size_t sr = 0;
  uint64_t freed_size = 0, _free_data_cache_size = 0, _outstanding_write_size = 0;

  ldout(cct, 10) << "D3nDataCache::" << __func__ << "(): oid=" << oid << ", len=" << len << dendl;
  {
    const std::lock_guard l(d3n_cache_lock);
    std::unordered_map<string, D3nChunkDataInfo*>::iterator iter = d3n_cache_map.find(oid);
    if (iter != d3n_cache_map.end()) {
      ldout(cct, 10) << "D3nDataCache::" << __func__ << "(): data already cached, no rewrite" << dendl;
      return;
    }
    auto it = d3n_outstanding_write_list.find(oid);
    if (it != d3n_outstanding_write_list.end()) {
      ldout(cct, 10) << "D3nDataCache: NOTE: data put in cache already issued, no rewrite" << dendl;
      return;
    }
    d3n_outstanding_write_list.insert(oid);
  }
  {
    const std::lock_guard l(d3n_eviction_lock);
    _free_data_cache_size = free_data_cache_size;
    _outstanding_write_size = outstanding_write_size;
  }
  ldout(cct, 20) << "D3nDataCache: Before eviction _free_data_cache_size:" << _free_data_cache_size << ", _outstanding_write_size:" << _outstanding_write_size << ", freed_size:" << freed_size << dendl;
  while (len > (_free_data_cache_size - _outstanding_write_size + freed_size)) {
    ldout(cct, 20) << "D3nDataCache: enter eviction" << dendl;
    if (eviction_policy == _eviction_policy::LRU) {
      sr = lru_eviction();
    } else if (eviction_policy == _eviction_policy::RANDOM) {
      sr = random_eviction();
    } else {
      ldout(cct, 0) << "D3nDataCache: Warning: unknown cache eviction policy, defaulting to lru eviction" << dendl;
      sr = lru_eviction();
    }
    if (sr == 0) {
      ldout(cct, 2) << "D3nDataCache: Warning: eviction was not able to free disk space, not writing to cache" << dendl;
      d3n_outstanding_write_list.erase(oid);
      return;
    }
    ldout(cct, 20) << "D3nDataCache: completed eviction of " << sr << " bytes" << dendl;
    freed_size += sr;
  }
  int r = 0;
  r = d3n_libaio_create_write_request(bl, len, oid);
  if (r < 0) {
    const std::lock_guard l(d3n_cache_lock);
    d3n_outstanding_write_list.erase(oid);
    ldout(cct, 1) << "D3nDataCache: create_aio_write_request fail, r=" << r << dendl;
    return;
  }

  const std::lock_guard l(d3n_eviction_lock);
  free_data_cache_size += freed_size;
  outstanding_write_size += len;
}

bool D3nDataCache::get(const string& oid, const off_t len)
{
  const std::lock_guard l(d3n_cache_lock);
  bool exist = false;
  string location = cache_location + url_encode(oid, true);

  lsubdout(g_ceph_context, rgw_datacache, 20) << "D3nDataCache: " << __func__ << "(): location=" << location << dendl;
  std::unordered_map<string, D3nChunkDataInfo*>::iterator iter = d3n_cache_map.find(oid);
  if (!(iter == d3n_cache_map.end())) {
    // check inside cache whether file exists or not!!!! then make exist true;
    struct D3nChunkDataInfo* chdo = iter->second;
    struct stat st;
    int r = stat(location.c_str(), &st);
    if ( r != -1 && st.st_size == len) { // file exists and contains required data range length
      exist = true;
      /*LRU*/
      /*get D3nChunkDataInfo*/
      const std::lock_guard l(d3n_eviction_lock);
      lru_remove(chdo);
      lru_insert_head(chdo);
      lsubdout(g_ceph_context, rgw_datacache, 20) << "D3nDataCache: " << __func__ << "(): file found of required size =" << st.st_size << dendl;
    } else {
      d3n_cache_map.erase(oid);
      const std::lock_guard l(d3n_eviction_lock);
      lru_remove(chdo);
      delete chdo;
      exist = false;
      lsubdout(g_ceph_context, rgw_datacache, 20) << "D3nDataCache: " << __func__ << "(): file found of but of different size =" << st.st_size << dendl;
      lsubdout(g_ceph_context, rgw_datacache, 20) << "D3nDataCache: " << __func__ << "(): removing oid from d3n cache map =" << oid << dendl;
    }
  }
  return exist;
}

size_t D3nDataCache::random_eviction()
{
  lsubdout(g_ceph_context, rgw_datacache, 20) << "D3nDataCache: " << __func__ << "()" << dendl;
  int n_entries = 0;
  int random_index = 0;
  size_t freed_size = 0;
  D3nChunkDataInfo* del_entry;
  string del_oid, location;
  {
    const std::lock_guard l(d3n_cache_lock);
    n_entries = d3n_cache_map.size();
    if (n_entries <= 0) {
      return -1;
    }
    srand (time(NULL));
    random_index = ceph::util::generate_random_number<int>(0, n_entries-1);
    std::unordered_map<string, D3nChunkDataInfo*>::iterator iter = d3n_cache_map.begin();
    std::advance(iter, random_index);
    del_oid = iter->first;
    del_entry =  iter->second;
    ldout(cct, 20) << "D3nDataCache: random_eviction: index:" << random_index << ", free size: " << del_entry->size << dendl;
    freed_size = del_entry->size;
    delete del_entry;
    del_entry = nullptr;
    d3n_cache_map.erase(del_oid); // oid
  }

  location = cache_location + url_encode(del_oid, true);
  ::remove(location.c_str());
  return freed_size;
}

size_t D3nDataCache::lru_eviction()
{
  lsubdout(g_ceph_context, rgw_datacache, 20) << "D3nDataCache: " << __func__ << "()" << dendl;
  int n_entries = 0;
  size_t freed_size = 0;
  D3nChunkDataInfo* del_entry;
  string del_oid, location;

  {
    const std::lock_guard l(d3n_eviction_lock);
    del_entry = tail;
    if (del_entry == nullptr) {
      ldout(cct, 2) << "D3nDataCache: lru_eviction: del_entry=null_ptr" << dendl;
      return 0;
    }
    lru_remove(del_entry);
  }

  {
    const std::lock_guard l(d3n_cache_lock);
    n_entries = d3n_cache_map.size();
    if (n_entries <= 0) {
      ldout(cct, 2) << "D3nDataCache: lru_eviction: cache_map.size<=0" << dendl;
      return -1;
    }
    del_oid = del_entry->oid;
    ldout(cct, 20) << "D3nDataCache: lru_eviction: oid to remove: " << del_oid << dendl;
    d3n_cache_map.erase(del_oid); // oid
  }
  freed_size = del_entry->size;
  delete del_entry;
  location = cache_location + url_encode(del_oid, true);
  ::remove(location.c_str());
  return freed_size;
}
