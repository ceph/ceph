// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "ObjectCacheStore.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rbd_cache
#undef dout_prefix
#define dout_prefix *_dout << "rbd::cache::ObjectCacheStore: " << this << " " \
                           << __func__ << ": "

namespace rbd {
namespace cache {

ObjectCacheStore::ObjectCacheStore(CephContext *cct, ContextWQ* work_queue)
      : m_cct(cct), m_work_queue(work_queue),
        m_rados(new librados::Rados()) {
  m_policy = new SimplePolicy(4096, 0.9); // TODO
}

ObjectCacheStore::~ObjectCacheStore() {
  delete m_policy;
}

int ObjectCacheStore::init(bool reset) {

  int ret = m_rados->init_with_context(m_cct);
  if(ret < 0) {
    lderr(m_cct) << "fail to init Ceph context" << dendl;
    return ret;
  }

  ret = m_rados->connect();
  if(ret < 0 ) {
    lderr(m_cct) << "fail to conect to cluster" << dendl;
    return ret;
  }
  //TODO(): check existing cache objects
  return ret;
}

int ObjectCacheStore::do_promote(std::string pool_name, std::string object_name) {
  int ret = 0;
  std::string cache_file_name =  pool_name + object_name;

  //TODO(): lock on ioctx map
  if (m_ioctxs.find(pool_name) == m_ioctxs.end()) {
    librados::IoCtx* io_ctx = new librados::IoCtx();
    ret = m_rados->ioctx_create(pool_name.c_str(), *io_ctx);
    if (ret < 0) {
      lderr(m_cct) << "fail to create ioctx" << dendl;
      assert(0);
    }
    m_ioctxs.emplace(pool_name, io_ctx); 
  }

  assert(m_ioctxs.find(pool_name) != m_ioctxs.end());
  
  librados::IoCtx* ioctx = m_ioctxs[pool_name]; 

  //promoting: update metadata 
  m_policy->update_status(cache_file_name, PROMOTING);
  assert(PROMOTING == m_policy->get_status(cache_file_name));

  librados::bufferlist* read_buf = new librados::bufferlist();
  int object_size = 4096*1024; //TODO(): read config from image metadata

  //TODO(): async promote
  ret = promote_object(ioctx, object_name, read_buf, object_size);
  if (ret == -ENOENT) {
    read_buf->append(std::string(object_size, '0'));
    ret = 0;
  }

  if( ret < 0) {
    lderr(m_cct) << "fail to read from rados" << dendl;
    return ret;
  }

  // persistent to cache
  librbd::cache::SyncFile cache_file(m_cct, cache_file_name);
  cache_file.open();
  ret = cache_file.write_object_to_file(*read_buf, object_size);
  
  // update metadata
  assert(PROMOTING == m_policy->get_status(cache_file_name));
  m_policy->update_status(cache_file_name, PROMOTED);
  assert(PROMOTED == m_policy->get_status(cache_file_name));

  return ret;

}
 
// return -1, client need to read data from cluster.
// return 0,  client directly read data from cache.
int ObjectCacheStore::lookup_object(std::string pool_name, std::string object_name) {

  std::string cache_file_name =  pool_name + object_name;

  // TODO lookup and return status;

  CACHESTATUS ret;
  ret = m_policy->lookup_object(cache_file_name);

  switch(ret) {
    case NONE:
      return do_promote(pool_name, object_name);
    case PROMOTING:
      return -1;
    case PROMOTED:
      return 0;
    default:
      return -1;
  }
}

void ObjectCacheStore::evict_thread_body() {
  int ret;
  while(m_evict_go) {
    std::string temp_cache_file;

    ret = m_policy->evict_object(temp_cache_file);
    if(ret == 0) {
      continue;
    }

    // TODO
    // delete temp_cache_file file.

    assert(EVICTING == m_policy->get_status(temp_cache_file));

    m_policy->update_status(temp_cache_file, EVICTED);

    assert(NONE == m_policy->get_status(temp_cache_file));
  }
}


int ObjectCacheStore::shutdown() {
  m_rados->shutdown();
  return 0;
}

int ObjectCacheStore::init_cache(std::string vol_name, uint64_t vol_size) {
  return 0;
}

int ObjectCacheStore::lock_cache(std::string vol_name) {
  return 0;
}

int ObjectCacheStore::promote_object(librados::IoCtx* ioctx, std::string object_name, librados::bufferlist* read_buf, uint64_t read_len) {
  int ret;

  librados::AioCompletion* read_completion = librados::Rados::aio_create_completion(); 

  ret = ioctx->aio_read(object_name, read_completion, read_buf, read_len, 0);
  if(ret < 0) {
    lderr(m_cct) << "fail to read from rados" << dendl;
    return ret;
  }
  read_completion->wait_for_complete();
  ret = read_completion->get_return_value();
  return ret;
  
}

} // namespace cache
} // namespace rbd
