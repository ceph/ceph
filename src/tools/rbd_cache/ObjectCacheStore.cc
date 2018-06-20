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
        m_cache_table_lock("rbd::cache::ObjectCacheStore"),
        m_rados(new librados::Rados()) {
}

ObjectCacheStore::~ObjectCacheStore() {

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
  {
    Mutex::Locker locker(m_cache_table_lock);
    m_cache_table.emplace(cache_file_name, PROMOTING);
  }

  librados::bufferlist read_buf;      
  int object_size = 4096*1024; //TODO(): read config from image metadata

  //TODO(): async promote
  ret = promote_object(ioctx, object_name, read_buf, object_size);
  if (ret == -ENOENT) {
    read_buf.append(std::string(object_size, '0'));
    ret = 0;
  }

  if( ret < 0) {
    lderr(m_cct) << "fail to read from rados" << dendl;
    return ret;
  }

  // persistent to cache
  librbd::cache::SyncFile cache_file(m_cct, cache_file_name);
  cache_file.open();
  ret = cache_file.write_object_to_file(read_buf, object_size);
  
  assert(m_cache_table.find(cache_file_name) != m_cache_table.end()); 

  // update metadata
  {
    Mutex::Locker locker(m_cache_table_lock);
    m_cache_table.emplace(cache_file_name, PROMOTED);
  }

  return ret;

}
 
int ObjectCacheStore::lookup_object(std::string pool_name, std::string object_name) {

  std::string cache_file_name =  pool_name + object_name;
  {
    Mutex::Locker locker(m_cache_table_lock);

    auto it = m_cache_table.find(cache_file_name);
    if (it != m_cache_table.end()) {

      if (it->second == PROMOTING) {
        return -1;
      } else if (it->second == PROMOTED) {
        return 0;
      } else {
        assert(0);
      }
    }
  }

  int ret = do_promote(pool_name, object_name);

  return ret;
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

int ObjectCacheStore::promote_object(librados::IoCtx* ioctx, std::string object_name, librados::bufferlist read_buf, uint64_t read_len) {
  int ret;

  librados::AioCompletion* read_completion = librados::Rados::aio_create_completion(); 

  ret = ioctx->aio_read(object_name, read_completion, &read_buf, read_len, 0);
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
