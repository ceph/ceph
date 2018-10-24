// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "ObjectCacheStore.h"
#include "include/Context.h"
#include "librbd/Utils.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_immutable_obj_cache
#undef dout_prefix
#define dout_prefix *_dout << "ceph::cache::ObjectCacheStore: " << this << " " \
                           << __func__ << ": "

namespace ceph {
namespace immutable_obj_cache {

ObjectCacheStore::ObjectCacheStore(CephContext *cct, ContextWQ* work_queue)
      : m_cct(cct), m_work_queue(work_queue), m_rados(new librados::Rados()),
        m_ioctxs_lock("ceph::cache::ObjectCacheStore::m_ioctxs_lock") {

  uint64_t object_cache_entries =
    cct->_conf.get_val<int64_t>("rbd_shared_cache_entries");

  //TODO(): allow to set level
  m_policy = new SimplePolicy(object_cache_entries, 0.5);
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

  std::string cache_path = m_cct->_conf.get_val<std::string>("rbd_shared_cache_path");
  //TODO(): check and reuse existing cache objects
  if(reset) {
    std::string cmd = "exec rm -rf " + cache_path + "/rbd_cache*; exec mkdir -p " + cache_path;
    //TODO(): to use std::filesystem
    int r = system(cmd.c_str());
  }

  evict_thd = new std::thread([this]{this->evict_thread_body();});
  return ret;
}

int ObjectCacheStore::do_promote(std::string pool_name, std::string object_name) {
  int ret = 0;
  std::string cache_file_name =  pool_name + object_name;

  {
    Mutex::Locker _locker(m_ioctxs_lock);
    if (m_ioctxs.find(pool_name) == m_ioctxs.end()) {
      librados::IoCtx* io_ctx = new librados::IoCtx();
      ret = m_rados->ioctx_create(pool_name.c_str(), *io_ctx);
      if (ret < 0) {
        lderr(m_cct) << "fail to create ioctx" << dendl;
        return ret;
      }
      m_ioctxs.emplace(pool_name, io_ctx);
    }
  }

  assert(m_ioctxs.find(pool_name) != m_ioctxs.end());
  
  librados::IoCtx* ioctx = m_ioctxs[pool_name]; 

  librados::bufferlist* read_buf = new librados::bufferlist();
  uint32_t object_size = 4096*1024; //TODO(): read config from image metadata

  auto ctx = new FunctionContext([this, read_buf, cache_file_name,
    object_size](int ret) {
      handle_promote_callback(ret, read_buf, cache_file_name, object_size);
   });

   return promote_object(ioctx, object_name, read_buf, object_size, ctx);
}

int ObjectCacheStore::handle_promote_callback(int ret, bufferlist* read_buf,
  std::string cache_file_name, uint32_t object_size) {
  ldout(m_cct, 20) << dendl;
  // rados read error
  if(ret != -ENOENT && ret < 0) {
    lderr(m_cct) << "fail to read from rados" << dendl;

    delete read_buf;
    m_policy->update_status(cache_file_name, OBJ_CACHE_NONE);
    return ret;
  }

  if (ret == -ENOENT) {
    // object is empty
    ret = 0;
  }

  if (ret < object_size) {
    // object is partial, fill with '0'
    read_buf->append(std::string(object_size - ret, '0'));
  }

  // write to cache
  librbd::cache::SyncFile cache_file(m_cct, cache_file_name);
  cache_file.create();

  ret = cache_file.write_object_to_file(*read_buf, object_size);
  if (ret < 0) {
    lderr(m_cct) << "fail to write cache file" << dendl;

    m_policy->update_status(cache_file_name, OBJ_CACHE_NONE);
    delete read_buf;
    return ret;
  }

  // update metadata
  assert(OBJ_CACHE_PROMOTING == m_policy->get_status(cache_file_name));
  m_policy->update_status(cache_file_name, OBJ_CACHE_PROMOTED);
  assert(OBJ_CACHE_PROMOTED == m_policy->get_status(cache_file_name));

  delete read_buf;
  return ret;
}
 
int ObjectCacheStore::lookup_object(std::string pool_name, std::string object_name) {

  int pret;
  CACHESTATUS ret;
  ret = m_policy->lookup_object(pool_name + object_name);

  switch(ret) {
    case OBJ_CACHE_NONE:
      evict_objects();
      pret = do_promote(pool_name, object_name);
      if (pret < 0) {
        lderr(m_cct) << "fail to start promoting" << dendl;
      }
      return -1;
    case OBJ_CACHE_PROMOTED:
      return 0;
    case OBJ_CACHE_PROMOTING:
      return -1;
    default:
      lderr(m_cct) << "unrecognized object cache status." << dendl;
      assert(0);
  }
}

void ObjectCacheStore::evict_thread_body() {
  int ret;
  while(m_evict_go) {
    ret = evict_objects();
  }
}

int ObjectCacheStore::shutdown() {
  m_evict_go = false;
  evict_thd->join();
  m_rados->shutdown();
  return 0;
}

int ObjectCacheStore::init_cache(std::string vol_name, uint64_t vol_size) {
  return 0;
}

int ObjectCacheStore::promote_object(librados::IoCtx* ioctx, std::string object_name, 
                                     librados::bufferlist* read_buf, uint64_t read_len,
                                     Context* on_finish) {
  auto ctx = new FunctionContext([on_finish](int ret) {
    on_finish->complete(ret);
  });

  librados::AioCompletion* read_completion = librbd::util::create_rados_callback(ctx);
  int ret = ioctx->aio_read(object_name, read_completion, read_buf, read_len, 0);
  if(ret < 0) {
    lderr(m_cct) << "fail to read from rados" << dendl;
    assert(0);
    return ret;
  }
  return 0;
}

int ObjectCacheStore::evict_objects() {
  std::list<std::string> obj_list;
  m_policy->get_evict_list(&obj_list);
  for (auto& obj: obj_list) {
    do_evict(obj);
  }
}

int ObjectCacheStore::do_evict(std::string cache_file) {
  //TODO(): call SyncFile API
  int ret = std::remove(cache_file.c_str());
   // evict entry in policy
  if (ret == 0) {
    m_policy->evict_entry(cache_file);
  }

  return ret;
}

} // namespace immutable_obj_cache
} // namespace ceph
