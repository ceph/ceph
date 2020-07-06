// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include "rgw_cache.h"
uint64_t expected_size = 0;
#include "rgw_perf_counters.h"

#include <errno.h>

#define dout_subsys ceph_subsys_rgw

/*datacache*/
#include <iostream>
#include <fstream> // writing to file
#include <sstream> // writing to memory (a string)
#include <unistd.h>
#include <fcntl.h>
#include <signal.h>
#include <stdlib.h>
#include <stdio.h>
#include "rgw_rest_conn.h"
#include <openssl/hmac.h>
#include <ctime>
#include <mutex>
#include <curl/curl.h>
#include <time.h>
//#include "rgw_cacherequest.h"
static const std::string base64_chars =
"ABCDEFGHIJKLMNOPQRSTUVWXYZ"
"abcdefghijklmnopqrstuvwxyz"
"0123456789+/";


static inline bool is_base64(unsigned char c) {
  return (isalnum(c) || (c == '+') || (c == '/'));
}
std::string base64_encode(unsigned char const* bytes_to_encode, unsigned int in_len) {
  std::string ret;
  int i = 0;
  int j = 0;
  unsigned char char_array_3[3];
  unsigned char char_array_4[4];
  while (in_len--) {
    char_array_3[i++] = *(bytes_to_encode++);
    if (i == 3) {
      char_array_4[0] = (char_array_3[0] & 0xfc) >> 2;
      char_array_4[1] = ((char_array_3[0] & 0x03) << 4) + ((char_array_3[1] & 0xf0) >> 4);
      char_array_4[2] = ((char_array_3[1] & 0x0f) << 2) + ((char_array_3[2] & 0xc0) >> 6);
      char_array_4[3] = char_array_3[2] & 0x3f;

      for(i = 0; (i <4) ; i++)
	ret += base64_chars[char_array_4[i]];
      i = 0;
    }
  }

  if (i)
  {
    for(j = i; j < 3; j++)
      char_array_3[j] = '\0';

    char_array_4[0] = (char_array_3[0] & 0xfc) >> 2;
    char_array_4[1] = ((char_array_3[0] & 0x03) << 4) + ((char_array_3[1] & 0xf0) >> 4);
    char_array_4[2] = ((char_array_3[1] & 0x0f) << 2) + ((char_array_3[2] & 0xc0) >> 6);
    char_array_4[3] = char_array_3[2] & 0x3f;

    for (j = 0; (j < i + 1); j++)
      ret += base64_chars[char_array_4[j]];

    while((i++ < 3))
      ret += '=';
  }
  return ret;
}






int ObjectCache::get(const string& name, ObjectCacheInfo& info, uint32_t mask, rgw_cache_entry_info *cache_info)
{

  std::shared_lock rl{lock};
  if (!enabled) {
    return -ENOENT;
  }
  auto iter = cache_map.find(name);
  if (iter == cache_map.end()) {
    ldout(cct, 10) << "cache get: name=" << name << " : miss" << dendl;
    if (perfcounter) {
      perfcounter->inc(l_rgw_cache_miss);
    }
    return -ENOENT;
  }

  if (expiry.count() &&
      (ceph::coarse_mono_clock::now() - iter->second.info.time_added) > expiry) {
    ldout(cct, 10) << "cache get: name=" << name << " : expiry miss" << dendl;
    rl.unlock();
    std::unique_lock wl{lock};  // write lock for insertion
    // check that wasn't already removed by other thread
    iter = cache_map.find(name);
    if (iter != cache_map.end()) {
      for (auto &kv : iter->second.chained_entries)
	kv.first->invalidate(kv.second);
      remove_lru(name, iter->second.lru_iter);
      cache_map.erase(iter);
    }
    if (perfcounter) {
      perfcounter->inc(l_rgw_cache_miss);
    }
    return -ENOENT;
  }

  ObjectCacheEntry *entry = &iter->second;

  if (lru_counter - entry->lru_promotion_ts > lru_window) {
    ldout(cct, 20) << "cache get: touching lru, lru_counter=" << lru_counter
      << " promotion_ts=" << entry->lru_promotion_ts << dendl;
    rl.unlock();
    std::unique_lock wl{lock};  // write lock for insertion
    /* need to redo this because entry might have dropped off the cache */
    iter = cache_map.find(name);
    if (iter == cache_map.end()) {
      ldout(cct, 10) << "lost race! cache get: name=" << name << " : miss" << dendl;
      if(perfcounter) perfcounter->inc(l_rgw_cache_miss);
      return -ENOENT;
    }

    entry = &iter->second;
    /* check again, we might have lost a race here */
    if (lru_counter - entry->lru_promotion_ts > lru_window) {
      touch_lru(name, *entry, iter->second.lru_iter);
    }
  }

  ObjectCacheInfo& src = iter->second.info;
  if ((src.flags & mask) != mask) {
    ldout(cct, 10) << "cache get: name=" << name << " : type miss (requested=0x"
      << std::hex << mask << ", cached=0x" << src.flags
      << std::dec << ")" << dendl;
    if(perfcounter) perfcounter->inc(l_rgw_cache_miss);
    return -ENOENT;
  }
  ldout(cct, 10) << "cache get: name=" << name << " : hit (requested=0x"
    << std::hex << mask << ", cached=0x" << src.flags
    << std::dec << ")" << dendl;

  info = src;
  if (cache_info) {
    cache_info->cache_locator = name;
    cache_info->gen = entry->gen;
  }
  if(perfcounter) perfcounter->inc(l_rgw_cache_hit);

  return 0;
}

bool ObjectCache::chain_cache_entry(std::initializer_list<rgw_cache_entry_info*> cache_info_entries,
    RGWChainedCache::Entry *chained_entry)
{
  std::unique_lock l{lock};

  if (!enabled) {
    return false;
  }

  std::vector<ObjectCacheEntry*> entries;
  entries.reserve(cache_info_entries.size());
  /* first verify that all entries are still valid */
  for (auto cache_info : cache_info_entries) {
    ldout(cct, 10) << "chain_cache_entry: cache_locator="
      << cache_info->cache_locator << dendl;
    auto iter = cache_map.find(cache_info->cache_locator);
    if (iter == cache_map.end()) {
      ldout(cct, 20) << "chain_cache_entry: couldn't find cache locator" << dendl;
      return false;
    }

    auto entry = &iter->second;

    if (entry->gen != cache_info->gen) {
      ldout(cct, 20) << "chain_cache_entry: entry.gen (" << entry->gen
	<< ") != cache_info.gen (" << cache_info->gen << ")"
	<< dendl;
      return false;
    }
    entries.push_back(entry);
  }


  chained_entry->cache->chain_cb(chained_entry->key, chained_entry->data);

  for (auto entry : entries) {
    entry->chained_entries.push_back(make_pair(chained_entry->cache,
	  chained_entry->key));
  }

  return true;
}

void ObjectCache::put(const string& name, ObjectCacheInfo& info, rgw_cache_entry_info *cache_info)
{
  std::unique_lock l{lock};

  if (!enabled) {
    return;
  }

  ldout(cct, 10) << "cache put: name=" << name << " info.flags=0x"
    << std::hex << info.flags << std::dec << dendl;

  auto [iter, inserted] = cache_map.emplace(name, ObjectCacheEntry{});
  ObjectCacheEntry& entry = iter->second;
  entry.info.time_added = ceph::coarse_mono_clock::now();
  if (inserted) {
    entry.lru_iter = lru.end();
  }
  ObjectCacheInfo& target = entry.info;

  invalidate_lru(entry);

  entry.chained_entries.clear();
  entry.gen++;

  touch_lru(name, entry, entry.lru_iter);

  target.status = info.status;

  if (info.status < 0) {
    target.flags = 0;
    target.xattrs.clear();
    target.data.clear();
    return;
  }

  if (cache_info) {
    cache_info->cache_locator = name;
    cache_info->gen = entry.gen;
  }

  target.flags |= info.flags;

  if (info.flags & CACHE_FLAG_META)
    target.meta = info.meta;
  else if (!(info.flags & CACHE_FLAG_MODIFY_XATTRS))
    target.flags &= ~CACHE_FLAG_META; // non-meta change should reset meta

  if (info.flags & CACHE_FLAG_XATTRS) {
    target.xattrs = info.xattrs;
    map<string, bufferlist>::iterator iter;
    for (iter = target.xattrs.begin(); iter != target.xattrs.end(); ++iter) {
      ldout(cct, 10) << "updating xattr: name=" << iter->first << " bl.length()=" << iter->second.length() << dendl;
    }
  } else if (info.flags & CACHE_FLAG_MODIFY_XATTRS) {
    map<string, bufferlist>::iterator iter;
    for (iter = info.rm_xattrs.begin(); iter != info.rm_xattrs.end(); ++iter) {
      ldout(cct, 10) << "removing xattr: name=" << iter->first << dendl;
      target.xattrs.erase(iter->first);
    }
    for (iter = info.xattrs.begin(); iter != info.xattrs.end(); ++iter) {
      ldout(cct, 10) << "appending xattr: name=" << iter->first << " bl.length()=" << iter->second.length() << dendl;
      target.xattrs[iter->first] = iter->second;
    }
  }

  if (info.flags & CACHE_FLAG_DATA)
    target.data = info.data;

  if (info.flags & CACHE_FLAG_OBJV)
    target.version = info.version;
}

bool ObjectCache::remove(const string& name)
{
  std::unique_lock l{lock};

  if (!enabled) {
    return false;
  }

  auto iter = cache_map.find(name);
  if (iter == cache_map.end())
    return false;

  ldout(cct, 10) << "removing " << name << " from cache" << dendl;
  ObjectCacheEntry& entry = iter->second;

  for (auto& kv : entry.chained_entries) {
    kv.first->invalidate(kv.second);
  }

  remove_lru(name, iter->second.lru_iter);
  cache_map.erase(iter);
  return true;
}

void ObjectCache::touch_lru(const string& name, ObjectCacheEntry& entry,
    std::list<string>::iterator& lru_iter)
{
  while (lru_size > (size_t)cct->_conf->rgw_cache_lru_size) {
    auto iter = lru.begin();
    if ((*iter).compare(name) == 0) {
      /*
       * if the entry we're touching happens to be at the lru end, don't remove it,
       * lru shrinking can wait for next time
       */
      break;
    }
    auto map_iter = cache_map.find(*iter);
    ldout(cct, 10) << "removing entry: name=" << *iter << " from cache LRU" << dendl;
    if (map_iter != cache_map.end()) {
      ObjectCacheEntry& entry = map_iter->second;
      invalidate_lru(entry);
      cache_map.erase(map_iter);
    }
    lru.pop_front();
    lru_size--;
  }

  if (lru_iter == lru.end()) {
    lru.push_back(name);
    lru_size++;
    lru_iter--;
    ldout(cct, 10) << "adding " << name << " to cache LRU end" << dendl;
  } else {
    ldout(cct, 10) << "moving " << name << " to cache LRU end" << dendl;
    lru.erase(lru_iter);
    lru.push_back(name);
    lru_iter = lru.end();
    --lru_iter;
  }

  lru_counter++;
  entry.lru_promotion_ts = lru_counter;
}

void ObjectCache::remove_lru(const string& name,
    std::list<string>::iterator& lru_iter)
{
  if (lru_iter == lru.end())
    return;

  lru.erase(lru_iter);
  lru_size--;
  lru_iter = lru.end();
}

void ObjectCache::invalidate_lru(ObjectCacheEntry& entry)
{
  for (auto iter = entry.chained_entries.begin();
      iter != entry.chained_entries.end(); ++iter) {
    RGWChainedCache *chained_cache = iter->first;
    chained_cache->invalidate(iter->second);
  }
}

void ObjectCache::set_enabled(bool status)
{
  std::unique_lock l{lock};

  enabled = status;

  if (!enabled) {
    do_invalidate_all();
  }
}

void ObjectCache::invalidate_all()
{
  std::unique_lock l{lock};

  do_invalidate_all();
}

void ObjectCache::do_invalidate_all()
{
  cache_map.clear();
  lru.clear();

  lru_size = 0;
  lru_counter = 0;
  lru_window = 0;

  for (auto& cache : chained_cache) {
    cache->invalidate_all();
  }
}

void ObjectCache::chain_cache(RGWChainedCache *cache) {
  std::unique_lock l{lock};
  chained_cache.push_back(cache);
}

void ObjectCache::unchain_cache(RGWChainedCache *cache) {
  std::unique_lock l{lock};

  auto iter = chained_cache.begin();
  for (; iter != chained_cache.end(); ++iter) {
    if (cache == *iter) {
      chained_cache.erase(iter);
      cache->unregistered();
      return;
    }
  }
}

ObjectCache::~ObjectCache()
{
  for (auto cache : chained_cache) {
    cache->unregistered();
  }
}


/* datacache */

DataCache::DataCache() : cct(NULL), capacity(0){}

void DataCache::submit_remote_req(struct RemoteRequest *c){
  ldout(cct, 0) << "submit_remote_req" <<dendl;
  tp->addTask(new RemoteS3Request(c, cct));
}

void DataCache::retrieve_obj_info(cache_obj* c_obj, RGWRados *store){
  ldout(cct, 0) << __func__ <<dendl;
  int ret = store->blkDirectory.getValue(c_obj);
}



void retrieve_aged_objList(RGWRados *store, string start_time, string end_time){
  //  ldout(cct, 0) << __func__ <<dendl;
  vector<vector<string>> object_list;
  //  object_list = store->objDirectory.get_aged_keys(start_time, end_time);
}



size_t DataCache::get_used_pool_capacity(string pool_name, RGWRados *store){

  ldout(cct, 0) << __func__ <<dendl;
  size_t used_capacity = 0;
  librados::Rados *rados = store->get_rados_handle();
  list<string> vec;
  int r = rados->pool_list(vec);
  //  vec.push_back(pool_name);
  map<string,librados::pool_stat_t> stats;
  r = rados->get_pool_stats(vec, stats);
  if (r < 0) {
    ldout(cct, 0) << "error fetching pool stats: " <<dendl;
    return -1;
  }
  for (map<string,librados::pool_stat_t>::iterator i = stats.begin();i != stats.end(); ++i) {
    const char *pool_name = i->first.c_str();
    librados::pool_stat_t& s = i->second;
  }
  return used_capacity; //return used size
}


void timer_start(RGWRados *store, int interval)
{
  time_t rawTime = time(NULL);
  string end_time = asctime(gmtime(&rawTime));
  rawTime = rawTime - (60 * interval);
  string start_time = asctime(gmtime(&rawTime));
  std::thread([store, interval, &start_time, &end_time]() {
      while (true)
      {
      //	    std::vector<std::pair<std::string, std::string>> object_list;
      vector<vector<string>> object_list;
      retrieve_aged_objList(store, start_time, end_time); 
      /*	    for (const auto& c_obj : object_list){ //FIXME : iterate over aged objects
      //	    store->copy_remote(store, c_obj); //aging function
      }
      */          std::this_thread::sleep_for(std::chrono::minutes(interval));
      start_time = end_time;
      time_t rawTime = time(NULL);
      end_time = asctime(gmtime(&rawTime));
      }
      }).detach();
}


void DataCache::start_cache_aging(RGWRados *store){
  ldout(cct, 0) << __func__ <<dendl;
  timer_start(store,cct->_conf->aging_interval);
}

size_t DataCache::remove_read_cache_entry(cache_obj& c_obj){

  string oid = c_obj.bucket_name+"_"+c_obj.obj_name+"_" + std::to_string(c_obj.chunk_id);
  /*
  ChunkDataInfo *del_entry;
  cache_lock.lock();

  int n_entries = cache_map.size();
  if (n_entries <= 0){
    cache_lock.unlock();
    return -1;
  }
  
  map<string, ChunkDataInfo*>::iterator iter = cache_map.begin();
  string del_oid = iter->first;

  cache_map.erase(del_oid); 
  cache_lock.unlock();
*/
  string location = cct->_conf->rgw_datacache_path + "/"+ c_obj.bucket_name+"_"+c_obj.obj_name+"_" + std::to_string(c_obj.chunk_id);
  if(access(location.c_str(), F_OK ) != -1 ) { 
    remove(location.c_str());
    return c_obj.chunk_size_in_bytes;
  }
  return 0;
}



int cacheAioWriteRequest::create_io(bufferlist& bl, uint64_t len, string key) {
  std::string location = cct->_conf->rgw_datacache_path + "/"+ key;
  int ret = 0;
  cb = new struct aiocb;
  mode_t mode = S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH;
  memset(cb, 0, sizeof(struct aiocb));
  ret = fd = ::open(location.c_str(), O_WRONLY | O_CREAT | O_TRUNC, mode);
  if (fd < 0)
  {
    ldout(cct, 0) << "ERROR: create_aio_write_request: open file failed, " << errno << "\tlocation: " << location.c_str() <<dendl;
    goto done;
  }
  cb->aio_fildes = fd;

  data = malloc(len);
  if(!data)
  {
    ldout(cct, 0) << "ERROR: create_aio_write_request: memory allocation failed" << dendl;
    goto close_file;
  }
  cb->aio_buf = data;
  memcpy((void *)data, bl.c_str(), len);
  cb->aio_nbytes = len;
  goto done;	
close_file:
  ::close(fd);
done:
  ldout(cct, 0) << "done" << dendl;
  return ret;
}


void DataCache::cache_aio_write_completion_cb(cacheAioWriteRequest *c){
  ChunkDataInfo  *chunk_info = NULL;
  
  ldout(cct, 20) << __func__ <<dendl;
  cache_lock.lock();
  outstanding_write_list.remove(c->key);
  /*chunk_info = new ChunkDataInfo;
  chunk_info->obj_id = c->key;
  chunk_info->set_ctx(cct);
  chunk_info->size = c->cb->aio_nbytes;
  cache_map.insert(pair<string, ChunkDataInfo*>(c->key, chunk_info));
  */
  cache_lock.unlock();
  c->release(); 

}


void _cache_aio_write_completion_cb(sigval_t sigval) {
  cacheAioWriteRequest *c = (cacheAioWriteRequest *)sigval.sival_ptr;
  c->priv_data->cache_aio_write_completion_cb(c);
}

int DataCache::create_aio_write_request(bufferlist& bl, uint64_t len, std::string key){
  struct cacheAioWriteRequest *wr= new struct cacheAioWriteRequest(cct);
  int ret = 0;
  if (wr->create_io(bl, len, key) < 0) {
    ldout(cct, 0) << "Error: create_io " << dendl;
    goto done;
  }

  wr->cb->aio_sigevent.sigev_notify = SIGEV_THREAD;
  wr->cb->aio_sigevent.sigev_notify_function = _cache_aio_write_completion_cb;
  wr->cb->aio_sigevent.sigev_notify_attributes = NULL;
  wr->cb->aio_sigevent.sigev_value.sival_ptr = (void*)wr;
  wr->key = key;
  wr->priv_data = this;
  if((ret= ::aio_write(wr->cb)) != 0) {
    ldout(cct, 0) << "Error: aio_write failed "<< ret << dendl;
    goto error;
  }
  return 0;

error:
  wr->release();
done:
  return ret;


}
void DataCache::put(bufferlist& bl, uint64_t len, string obj_id){
  ldout(cct, 10) << __func__  << obj_id <<dendl;

  cache_lock.lock(); 
  /*
  map<string, ChunkDataInfo *>::iterator iter = cache_map.find(obj_id);
  if (iter != cache_map.end()) {
    cache_lock.unlock();
    ldout(cct, 10) << "Warning: obj data already is cached, no re-write" << dendl;
    return;
  }*/

  std::list<std::string>::iterator it = std::find(outstanding_write_list.begin(), outstanding_write_list.end(),obj_id);
  if (it != outstanding_write_list.end()) {
    cache_lock.unlock();
    ldout(cct, 5) << "Warning: write is already issued, re-write, obj_id="<< obj_id << dendl;
    return;
  }

  outstanding_write_list.push_back(obj_id);
  cache_lock.unlock();

  int ret = create_aio_write_request(bl, len, obj_id);
  if (ret < 0) {
    cache_lock.lock();
    outstanding_write_list.remove(obj_id);
    cache_lock.unlock();
    ldout(cct, 0) << "Error: create_aio_write_request is failed"  << ret << dendl;
    return;
  }
}

static size_t _remote_req_cb(void *ptr, size_t size, size_t nmemb, void* param) {
  RemoteRequest *req = static_cast<RemoteRequest *>(param);
  req->bl->append((char *)ptr, size*nmemb);
  return size*nmemb;
}

string RemoteS3Request::sign_s3_request(string HTTP_Verb, string uri, string date, string YourSecretAccessKeyID, string AWSAccessKeyId){
  std::string Content_Type = "application/x-www-form-urlencoded; charset=utf-8";
  std::string Content_MD5 ="";
  std::string CanonicalizedResource = uri.c_str();
  std::string StringToSign = HTTP_Verb + "\n" + Content_MD5 + "\n" + Content_Type + "\n" + date + "\n" +CanonicalizedResource;
  char key[YourSecretAccessKeyID.length()+1] ;
  strcpy(key, YourSecretAccessKeyID.c_str());
  const char * data = StringToSign.c_str();
  unsigned char* digest;
  digest = HMAC(EVP_sha1(), key, strlen(key), (unsigned char*)data, strlen(data), NULL, NULL);
  std::string signature = base64_encode(digest, 20);
  return signature;

}

string RemoteS3Request::get_date(){
  std::string zone=" GMT";
  time_t now = time(0);
  char* dt = ctime(&now);
  tm *gmtm = gmtime(&now);
  dt = asctime(gmtm);
  std::string date(dt);
  char buffer[80];
  std::strftime(buffer,80,"%a, %d %b %Y %X %Z",gmtm);
  puts(buffer);
  date = buffer;
  return date;
}

int RemoteS3Request::submit_http_get_request_s3(){
  string range = std::to_string(req->ofs + req->read_ofs)+ "-"+ std::to_string(req->ofs + req->read_ofs + req->read_len - 1);
  ldout(cct, 10) << __func__  << " range " << range << dendl;
  CURLcode res;
  string uri = "/"+req->c_obj->bucket_name + "/" +req->c_obj->obj_name;
  string date = get_date();
  string AWSAccessKeyId=req->c_obj->accesskey.id;
  string YourSecretAccessKeyID=req->c_obj->accesskey.key;
  string signature = sign_s3_request("GET", uri, date, YourSecretAccessKeyID, AWSAccessKeyId);
  string Authorization = "AWS "+ AWSAccessKeyId +":" + signature;
  string loc = req->c_obj->host + uri;
  string auth="Authorization: " + Authorization;
  string timestamp="Date: " + date;
  string user_agent="User-Agent: aws-sdk-java/1.7.4 Linux/3.10.0-514.6.1.el7.x86_64 OpenJDK_64-Bit_Server_VM/24.131-b00/1.7.0_131";
  string content_type="Content-Type: application/x-www-form-urlencoded; charset=utf-8";
  curl_handle = curl_easy_init();
  if(curl_handle) {
    struct curl_slist *chunk = NULL;
    chunk = curl_slist_append(chunk, auth.c_str());
    chunk = curl_slist_append(chunk, timestamp.c_str());
    chunk = curl_slist_append(chunk, user_agent.c_str());
    chunk = curl_slist_append(chunk, content_type.c_str());
    curl_easy_setopt(curl_handle, CURLOPT_RANGE, range.c_str());
    res = curl_easy_setopt(curl_handle, CURLOPT_HTTPHEADER, chunk); //set headers
    curl_easy_setopt(curl_handle, CURLOPT_URL, loc.c_str());
    curl_easy_setopt(curl_handle, CURLOPT_FOLLOWLOCATION, 1L); //for redirection of the url
    curl_easy_setopt(curl_handle, CURLOPT_WRITEFUNCTION, _remote_req_cb);
    curl_easy_setopt(curl_handle, CURLOPT_WRITEDATA, (void*)req);
    res = curl_easy_perform(curl_handle); //run the curl command
    curl_easy_reset(curl_handle);
    curl_slist_free_all(chunk);
  }if(res != CURLE_OK){
    ldout(cct,10) << "__func__ " << curl_easy_strerror(res) << " key " << req->key << dendl;
    return -1;
  }else{
    req->r->result = 0;
    req->aio->put(*(req->r));
    return 0;}

}

/*Remote S3 Request datacacahe*/
int RemoteS3Request::submit_op() {
  return req->submit_op();
}

void RemoteS3Request::run() {

  ldout(cct, 20) << __func__  <<dendl;
  int max_retries = cct->_conf->max_remote_retries;
  int r = 0;
  for (int i=0; i<max_retries; i++ ){
    if(!(r = submit_http_get_request_s3())){
      req->finish();
      return;
    }
    ldout(cct, 0) << "ERROR: " << __func__  << "(): remote s3 request for failed, obj="<<req->key << dendl;
    req->r->result = -1;
    req->aio->put(*(req->r));
  }


}

