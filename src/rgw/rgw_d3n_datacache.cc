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
  cb->aio_buf = nullptr;
  free(data);
  data = nullptr;

close_file:
  ::close(fd);
done:
  return r;
}

D3nDataCache::D3nDataCache()
  : index(0), cct(NULL), io_type(ASYNC_IO), free_data_cache_size(0), outstanding_write_size(0)
{
  tp = new D3nL2CacheThreadPool(32);
}

int D3nDataCache::io_write(bufferlist& bl, unsigned int len, std::string oid)
{
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
  D3nCacheAioWriteRequest* c = static_cast<D3nCacheAioWriteRequest*>(sigval.sival_ptr);
  c->priv_data->cache_aio_write_completion_cb(c);
}


void D3nDataCache::cache_aio_write_completion_cb(D3nCacheAioWriteRequest* c)
{
  D3nChunkDataInfo* chunk_info{nullptr};

  ldout(cct, 5) << "D3nDataCache: cache_aio_write_completion_cb oid:" << c->oid <<dendl;

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
  c->release();
}

int D3nDataCache::create_aio_write_request(bufferlist& bl, unsigned int len, std::string oid)
{
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
  wr->release();
done:
  return r;
}

void D3nDataCache::put(bufferlist& bl, unsigned int len, std::string& oid)
{
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

  ldout(cct, 20) << "D3nDataCache: Before eviction _free_data_cache_size:" << _free_data_cache_size << ", _outstanding_write_size:" << _outstanding_write_size << "freed_size:" << freed_size << dendl;
  while (len >= (_free_data_cache_size - _outstanding_write_size + freed_size)) {
    ldout(cct, 20) << "D3nDataCache: enter eviction, r=" << r << dendl;
    r = lru_eviction();
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
  map<string, D3nChunkDataInfo*>::iterator iter = cache_map.find(del_entry->oid);
  if (iter != cache_map.end()) {
    cache_map.erase(del_oid); // oid
  }
  cache_lock.unlock();
  freed_size = del_entry->size;
  free(del_entry);
  location = cache_location + del_oid; /*replace tmp with the correct path from config file*/
  remove(location.c_str());
  return freed_size;
}


void D3nDataCache::remote_io(D3nL2CacheRequest* l2request )
{
  ldout(cct, 20) << "D3nDataCache: Add task to remote IO" << dendl;
  tp->addTask(new D3nHttpL2Request(l2request, cct));
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

void D3nDataCache::push_l2_request(D3nL2CacheRequest* l2request )
{
  tp->addTask(new D3nHttpL2Request(l2request, cct));
}

static size_t _d3n_l2_response_cb(void *ptr, size_t size, size_t nmemb, void* param)
{
  D3nL2CacheRequest* req = static_cast<D3nL2CacheRequest*>(param);
  req->pbl->append((char *)ptr, size*nmemb);
  return size*nmemb;
}

// TODO: complete L2 cache support refactoring
void D3nHttpL2Request::run()
{
/*
  get_obj_data* d = static_cast<get_obj_data*>(req->op_data);
  int n_retries = cct->_conf->rgw_d3n_l2_datacache_request_thread_num;
  int r = 0;

  for (int i=0; i<n_retries; i++ ) {
    if (!(r = submit_http_request())) {
      d->cache_aio_completion_cb(req);
      return;
    }
    if (r == ECANCELED) {
      return;
    }
  }
  */
}
/*
int D3nHttpL2Request::submit_http_request()
{
  CURLcode res;
  string auth_token;
  string range = std::to_string(req->ofs + req->read_ofs)+ "-"+ std::to_string(req->ofs + req->read_ofs + req->len - 1);
  struct curl_slist* header{nullptr};
  get_obj_data* d = static_cast<get_obj_data*>(req->op_data);

  string req_uri;
  string uri,dest;
  (static_cast<RGWGetObj_CB*>(d->client_cb))->get_req_info(dest, req_uri, auth_token);
  uri = "http://" + req->dest + req_uri;

  struct req_state *s;
  ((RGWGetObj_CB *)(d->client_cb))->get_req_info(req->dest, s->info.request_uri, auth_token);

  if (s->dialect == "s3") {
    RGWEnv env;
    req_info info(cct, &env);
    memcpy(&info, &s->info, sizeof(info));
    memcpy(&env, s->info.env, sizeof(env));
    info.env = &env;
    std::string access_key_id;
    const char* http_auth = s->info.env->get("HTTP_AUTHORIZATION");
    if (! http_auth || http_auth[0] == '\0') {
      access_key_id = s->info.args.get("AWSAccessKeyId");
    } else {
      string auth_str("auth");
      int pos = auth_str.rfind(':');
      if (pos < 0)
        return -EINVAL;
      access_key_id = auth_str.substr(0, pos);
    }
    map<string, RGWAccessKey>::iterator iter = s->user->get_info().access_keys.find(access_key_id);
    if (iter == s->user->get_info().access_keys.end()) {
      ldout(cct, 1) << "ERROR: access key not encoded in user info" << dendl;
      return -EPERM;
    }
   RGWAccessKey& key = iter->second;
   sign_request(key, env, info);
  }
  else if (s->dialect == "swift") {
    header = curl_slist_append(header, auth_token.c_str());
  } else {
    ldout(cct, 10) << "D3nDataCache: curl_easy_perform() failed " << dendl;
    return -1;
  }


  if (curl_handle) {
    curl_easy_setopt(curl_handle, CURLOPT_RANGE, range.c_str());
    curl_easy_setopt(curl_handle, CURLOPT_HTTPHEADER, header);
    curl_easy_setopt(curl_handle, CURLOPT_URL, uri.c_str());
    curl_easy_setopt(curl_handle, CURLOPT_FOLLOWLOCATION, 1L);
    curl_easy_setopt(curl_handle, CURLOPT_VERBOSE, 1L);
    curl_easy_setopt(curl_handle, CURLOPT_WRITEFUNCTION, _d3n_l2_response_cb);
    curl_easy_setopt(curl_handle, CURLOPT_WRITEDATA, (void*)req);
    res = curl_easy_perform(curl_handle);
    curl_easy_reset(curl_handle);
    curl_slist_free_all(header);
  }
  if (res != CURLE_OK) {
    ldout(cct, 10) << "D3nDataCache: curl_easy_perform() failed " << curl_easy_strerror(res) << " oid " << req->oid << " offset " << req->ofs + req->read_ofs  <<dendl;
    return -1;
  }
  return 0;
}

int D3nHttpL2Request::sign_request(RGWAccessKey& key, RGWEnv& env, req_info& info)
{
  // don't sign if no key is provided
  if (key.key.empty()) {
    return 0;
  }

  if (cct->_conf->subsys.should_gather(ceph_subsys_rgw, 20)) {
    for (const auto& i: env.get_map()) {
      ldout(cct, 20) << "> " << i.first << " -> " << rgw::crypt_sanitize::x_meta_map{i.first, i.second} << dendl;
    }
  }

  std::string canonical_header;
  if (!rgw_create_s3_canonical_header(info, NULL, canonical_header, false)) {
    ldout(cct, 0) << "failed to create canonical s3 header" << dendl;
    return -EINVAL;
  }

  ldout(cct, 20) << "generated canonical header: " << canonical_header << dendl;

  std::string digest;
  try {
    digest = rgw::auth::s3::get_v2_signature(cct, key.key, canonical_header);
  } catch (int ret) {
    return ret;
  }

  string auth_hdr = "AWS " + key.id + ":" + digest;
  ldout(cct, 15) << "generated auth header: " << auth_hdr << dendl;

  env.set("AUTHORIZATION", auth_hdr);

  return 0;
}
*/

