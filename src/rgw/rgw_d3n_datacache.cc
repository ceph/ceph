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


#define dout_subsys ceph_subsys_rgw


class RGWGetObj_CB;


int D3nCacheAioWriteRequest::create_io(bufferlist& bl, unsigned int len, string oid, string cache_location)
{
  lsubdout(g_ceph_context, rgw_datacache, 20) << "D3nDataCache: " << __func__ << "()" << dendl;
  std::string location = cache_location + oid;
  int r = 0;

  cb = new struct aiocb;
  mode_t mode = S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH;
  memset(cb, 0, sizeof(struct aiocb));
  r = fd = ::open(location.c_str(), O_WRONLY | O_CREAT | O_TRUNC, mode);
  if (fd < 0) {
    ldout(cct, 0) << "ERROR: create_aio_write_request: open file failed, " << errno << "\tlocation: " << location.c_str() <<dendl;
    goto done;
  }
  posix_fadvise(fd, 0, 0, g_conf()->rgw_d3n_l1_fadvise);
  cb->aio_fildes = fd;

  data = malloc(len);
  if (!data) {
    ldout(cct, 0) << "ERROR: create_aio_write_request: memory allocation failed" << dendl;
    goto close_file;
  }
  cb->aio_buf = data;
  memcpy((void*)data, bl.c_str(), len);
  cb->aio_nbytes = len;
  goto done;

close_file:
  ::close(fd);
done:
  return r;
}

D3nDataCache::D3nDataCache()
  : index(0), cct(NULL), io_type(ASYNC_IO), free_data_cache_size(0), outstanding_write_size(0)
{
  lsubdout(g_ceph_context, rgw_datacache, 5) << "D3nDataCache: " << __func__ << "()" << dendl;
}

int D3nDataCache::io_write(bufferlist& bl, unsigned int len, std::string oid)
{
  lsubdout(g_ceph_context, rgw_datacache, 30) << "D3nDataCache: " << __func__ << "()" << dendl;
  D3nChunkDataInfo* chunk_info = new D3nChunkDataInfo;

  std::string location = cache_location + oid; /* replace tmp with the correct path from config file*/
  FILE *cache_file = 0;
  int r = 0;

  cache_file = fopen(location.c_str(),"w+");
  if (cache_file != nullptr) {
    ldout(cct, 0) << "ERROR: D3nDataCache::open file has return error " << r << dendl;
    return -1;
  }

  r = fwrite(bl.c_str(), 1, len, cache_file);
  if (r < 0) {
    ldout(cct, 0) << "ERROR: D3nDataCache::write: write in file has return error " << r << dendl;
  }

  fclose(cache_file);

  /*update cahce_map entries for new chunk in cache*/
  cache_lock.lock();
  chunk_info->oid = oid;
  chunk_info->set_ctx(cct);
  chunk_info->size = len;
  cache_map.insert(pair<string, D3nChunkDataInfo*>(oid, chunk_info));
  cache_lock.unlock();

  return r;
}

void _d3n_cache_aio_write_completion_cb(sigval_t sigval)
{
  lsubdout(g_ceph_context, rgw_datacache, 30) << "D3nDataCache: " << __func__ << "()" << dendl;
  D3nCacheAioWriteRequest* c = static_cast<D3nCacheAioWriteRequest*>(sigval.sival_ptr);
  c->priv_data->cache_aio_write_completion_cb(c);
}


void D3nDataCache::cache_aio_write_completion_cb(D3nCacheAioWriteRequest* c)
{
  D3nChunkDataInfo* chunk_info{nullptr};

  ldout(cct, 5) << "D3nDataCache: " << __func__ << "(): oid=" << c->oid << dendl;

  /*update cache_map entries for new chunk in cache*/
  cache_lock.lock();
  outstanding_write_list.remove(c->oid);
  chunk_info = new D3nChunkDataInfo;
  chunk_info->oid = c->oid;
  chunk_info->set_ctx(cct);
  chunk_info->size = c->cb->aio_nbytes;
  cache_map.insert(pair<string, D3nChunkDataInfo*>(c->oid, chunk_info));
  cache_lock.unlock();

  /*update free size*/
  eviction_lock.lock();
  free_data_cache_size -= c->cb->aio_nbytes;
  outstanding_write_size -= c->cb->aio_nbytes;
  lru_insert_head(chunk_info);
  eviction_lock.unlock();
  delete c;
  c = nullptr;
}

int D3nDataCache::create_aio_write_request(bufferlist& bl, unsigned int len, std::string oid)
{
  lsubdout(g_ceph_context, rgw_datacache, 30) << "D3nDataCache: " << __func__ << "()" << dendl;
  struct D3nCacheAioWriteRequest* wr = new struct D3nCacheAioWriteRequest(cct);
  int r=0;
  if (wr->create_io(bl, len, oid, cache_location) < 0) {
    ldout(cct, 0) << "D3nDataCache: Error create_aio_write_request" << dendl;
    goto done;
  }
  wr->cb->aio_sigevent.sigev_notify = SIGEV_THREAD;
  wr->cb->aio_sigevent.sigev_notify_function = _d3n_cache_aio_write_completion_cb;
  wr->cb->aio_sigevent.sigev_notify_attributes = nullptr;
  wr->cb->aio_sigevent.sigev_value.sival_ptr = wr;
  wr->oid = oid;
  wr->priv_data = this;

  if ((r= ::aio_write(wr->cb)) != 0) {
    ldout(cct, 0) << "ERROR: aio_write "<< r << dendl;
    goto error;
  }
  return 0;

error:
  delete wr;
done:
  return r;
}

void D3nDataCache::put(bufferlist& bl, unsigned int len, std::string& oid)
{
  lsubdout(g_ceph_context, rgw_datacache, 20) << "D3nDataCache: " << __func__ << "()" << dendl;
  int r = 0;
  uint64_t freed_size = 0, _free_data_cache_size = 0, _outstanding_write_size = 0;

  ldout(cct, 20) << "D3nDataCache: We are in D3nDataCache::put() and oid is: " << oid <<dendl;
  cache_lock.lock();
  map<string, D3nChunkDataInfo*>::iterator iter = cache_map.find(oid);
  if (iter != cache_map.end()) {
    cache_lock.unlock();
    ldout(cct, 10) << "D3nDataCache: Warning: data already cached, no rewrite" << dendl;
    return;
  }
  std::list<std::string>::iterator it = std::find(outstanding_write_list.begin(), outstanding_write_list.end(),oid);
  if (it != outstanding_write_list.end()) {
    cache_lock.unlock();
    ldout(cct, 10) << "D3nDataCache: Warning: data put in cache already issued, no rewrite" << dendl;
    return;
  }
  outstanding_write_list.push_back(oid);
  cache_lock.unlock();

  eviction_lock.lock();
  _free_data_cache_size = free_data_cache_size;
  _outstanding_write_size = outstanding_write_size;
  eviction_lock.unlock();

  ldout(cct, 20) << "D3nDataCache: Before eviction _free_data_cache_size:" << _free_data_cache_size << ", _outstanding_write_size:" << _outstanding_write_size << ", freed_size:" << freed_size << dendl;
  while (len >= (_free_data_cache_size - _outstanding_write_size + freed_size)) {
    ldout(cct, 20) << "D3nDataCache: enter eviction, r=" << r << dendl;
    if (eviction_policy == LRU) {
      r = lru_eviction();
    } else if (eviction_policy == RANDOM) {
      r = random_eviction();
    } else {
      ldout(cct, 0) << "D3nDataCache: Warning: unknown cache eviction policy, defaulting to lru eviction" << dendl;
      r = lru_eviction();
    }
    if (r < 0)
      return;
    freed_size += r;
  }
  r = create_aio_write_request(bl, len, oid);
  if (r < 0) {
    cache_lock.lock();
    outstanding_write_list.remove(oid);
    cache_lock.unlock();
    ldout(cct, 1) << "D3nDataCache: create_aio_write_request fail, r=" << r << dendl;
    return;
  }

  eviction_lock.lock();
  free_data_cache_size += freed_size;
  outstanding_write_size += len;
  eviction_lock.unlock();
}

bool D3nDataCache::get(const string& oid)
{
  lsubdout(g_ceph_context, rgw_datacache, 20) << "D3nDataCache: " << __func__ << "()" << dendl;
  bool exist = false;
  string location = cache_location + oid;
  cache_lock.lock();
  map<string, D3nChunkDataInfo*>::iterator iter = cache_map.find(oid);
  if (!(iter == cache_map.end())) {
    // check inside cache whether file exists or not!!!! then make exist true;
    struct D3nChunkDataInfo* chdo = iter->second;
    if (access(location.c_str(), F_OK ) != -1 ) { // file exists
      exist = true;
      {
        /*LRU*/
        /*get D3nChunkDataInfo*/
        eviction_lock.lock();
        lru_remove(chdo);
        lru_insert_head(chdo);
        eviction_lock.unlock();
      }
    } else {
      cache_map.erase(oid);
      lru_remove(chdo);
      exist = false;
    }
  }
  cache_lock.unlock();
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

  cache_lock.lock();
  n_entries = cache_map.size();
  if (n_entries <= 0) {
    cache_lock.unlock();
    return -1;
  }
  srand (time(NULL));
  random_index = rand()%n_entries;
  map<string, D3nChunkDataInfo*>::iterator iter = cache_map.begin();
  std::advance(iter, random_index);
  del_oid = iter->first;
  del_entry =  iter->second;
  ldout(cct, 20) << "D3nDataCache: random_eviction: index:" << random_index << ", free size:0x" << std::hex << del_entry->size << dendl;
  freed_size = del_entry->size;
  free(del_entry);
  del_entry = nullptr;
  cache_map.erase(del_oid); // oid
  cache_lock.unlock();

  location = cache_location + del_oid; /*replace tmp with the correct path from config file*/
  remove(location.c_str());
  return freed_size;
}

size_t D3nDataCache::lru_eviction()
{
  lsubdout(g_ceph_context, rgw_datacache, 20) << "D3nDataCache: " << __func__ << "()" << dendl;
  int n_entries = 0;
  size_t freed_size = 0;
  D3nChunkDataInfo* del_entry;
  string del_oid, location;

  eviction_lock.lock();
  del_entry = tail;
  if (del_entry == nullptr) {
    ldout(cct, 1) << "D3nDataCache: lru_eviction: error: del_entry=null_ptr" << dendl;
    return 0;
  }
  lru_remove(del_entry);
  eviction_lock.unlock();

  cache_lock.lock();
  n_entries = cache_map.size();
  if (n_entries <= 0) {
    cache_lock.unlock();
    return -1;
  }
  del_oid = del_entry->oid;
  ldout(cct, 20) << "D3nDataCache: lru_eviction: oid to remove" << del_oid << dendl;
  map<string, D3nChunkDataInfo*>::iterator iter = cache_map.find(del_oid);
  if (iter != cache_map.end()) {
    cache_map.erase(iter); // oid
  }
  cache_lock.unlock();
  freed_size = del_entry->size;
  free(del_entry);
  location = cache_location + del_oid; /*replace tmp with the correct path from config file*/
  remove(location.c_str());
  return freed_size;
}


std::vector<string> d3n_split(const std::string &s, char * delim)
{
  stringstream ss(s);
  std::string item;
  std::vector<string> tokens;
  while (getline(ss, item, (*delim))) {
    tokens.push_back(item);
  }
  return tokens;
}
