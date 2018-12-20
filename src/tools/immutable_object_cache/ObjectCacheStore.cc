// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "include/Context.h"
#include "ObjectCacheStore.h"
#include "Utils.h"
#include <experimental/filesystem>

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_immutable_obj_cache
#undef dout_prefix
#define dout_prefix *_dout << "ceph::cache::ObjectCacheStore: " << this << " " \
                           << __func__ << ": "

namespace efs = std::experimental::filesystem;

namespace ceph {
namespace immutable_obj_cache {

ObjectCacheStore::ObjectCacheStore(CephContext *cct, ContextWQ* work_queue)
      : m_cct(cct), m_work_queue(work_queue), m_rados(new librados::Rados()),
        m_ioctxs_lock("ceph::cache::ObjectCacheStore::m_ioctxs_lock") {

  object_cache_entries =
    m_cct->_conf.get_val<Option::size_t>("immutable_object_cache_max_size");

  std::string cache_path = m_cct->_conf.get_val<std::string>("immutable_object_cache_path");
  m_cache_root_dir = cache_path + "/ceph_immutable_obj_cache/";

  //TODO(): allow to set cache level
  m_policy = new SimplePolicy(m_cct, object_cache_entries, 0.1);
}

ObjectCacheStore::~ObjectCacheStore() {
  delete m_policy;
}

int ObjectCacheStore::init(bool reset) {
  ldout(m_cct, 20) << dendl;

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

  //TODO(): fsck and reuse existing cache objects
  if (reset) {
    if (efs::exists(m_cache_root_dir)) {
      efs::remove_all(m_cache_root_dir);
    }
    efs::create_directories(m_cache_root_dir);
  }

  evict_thd = new std::thread([this]{this->evict_thread_body();});
  return ret;
}

void ObjectCacheStore::evict_thread_body() {
  int ret;
  while(m_evict_go) {
    ret = evict_objects();
  }
}

int ObjectCacheStore::shutdown() {
  ldout(m_cct, 20) << dendl;

  m_evict_go = false;
  evict_thd->join();
  m_rados->shutdown();
  return 0;
}

int ObjectCacheStore::init_cache(std::string pool_name, std::string vol_name, uint64_t vol_size) {
  ldout(m_cct, 20) << "pool name = " << pool_name
                   << " volume name = " << vol_name
                   << " volume size = " << vol_size << dendl;

  std::string vol_cache_dir = m_cache_root_dir + pool_name + "_" + vol_name;

  int dir = m_dir_num - 1;
  while (dir >= 0) {
    efs::create_directories(vol_cache_dir + "/" + std::to_string(dir));
    dir --;
  }
  return 0;
}

int ObjectCacheStore::do_promote(std::string pool_name, std::string vol_name, std::string object_name) {
  ldout(m_cct, 20) << "to promote object = "
                   << object_name << " from pool: "
                   << pool_name << dendl;

  int ret = 0;
  std::string cache_file_name =  pool_name + object_name;
  std::string vol_cache_dir = pool_name + "_" + vol_name;
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

  auto ctx = new FunctionContext([this, read_buf, vol_cache_dir, cache_file_name,
    object_size](int ret) {
      handle_promote_callback(ret, read_buf, vol_cache_dir, cache_file_name, object_size);
   });

   return promote_object(ioctx, object_name, read_buf, object_size, ctx);
}

int ObjectCacheStore::handle_promote_callback(int ret, bufferlist* read_buf,
  std::string cache_dir, std::string cache_file_name, uint32_t object_size) {
  ldout(m_cct, 20) << "cache dir: " << cache_dir
                   << " cache_file_name: " << cache_file_name << dendl;

  // rados read error
  if(ret != -ENOENT && ret < 0) {
    lderr(m_cct) << "fail to read from rados" << dendl;

    m_policy->update_status(cache_file_name, OBJ_CACHE_NONE);
    delete read_buf;
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

  if (m_dir_num > 0) {
    auto const pos = cache_file_name.find_last_of('.');
    cache_dir = cache_dir + "/" + std::to_string(stoul(cache_file_name.substr(pos+1)) % m_dir_num);
  }
  // write to cache
  ObjectCacheFile cache_file(m_cct, cache_dir + "/" + cache_file_name);
  cache_file.create();

  ret = cache_file.write_object_to_file(*read_buf, object_size);
  if (ret < 0) {
    lderr(m_cct) << "fail to write cache file" << dendl;

    m_policy->update_status(cache_file_name, OBJ_CACHE_NONE);
    delete read_buf;
    return ret;
  }

  // update metadata
  assert(OBJ_CACHE_SKIP == m_policy->get_status(cache_file_name));
  m_policy->update_status(cache_file_name, OBJ_CACHE_PROMOTED);
  assert(OBJ_CACHE_PROMOTED == m_policy->get_status(cache_file_name));

  delete read_buf;
  return ret;

  evict_objects();
}

int ObjectCacheStore::lookup_object(std::string pool_name,
    std::string vol_name, std::string object_name) {
  ldout(m_cct, 20) << "object name = " << object_name
                   << " in pool: " << pool_name << dendl;

  int pret = -1;
  cache_status_t ret = m_policy->lookup_object(pool_name + object_name);

  switch(ret) {
    case OBJ_CACHE_NONE: {
      pret = do_promote(pool_name, vol_name, object_name);
      if (pret < 0) {
        lderr(m_cct) << "fail to start promote" << dendl;
      }
      return -1;
    }
    case OBJ_CACHE_PROMOTED:
      return 0;
    case OBJ_CACHE_SKIP:
      return -1;
    default:
      lderr(m_cct) << "unrecognized object cache status." << dendl;
      assert(0);
  }
}

int ObjectCacheStore::promote_object(librados::IoCtx* ioctx, std::string object_name,
                                     librados::bufferlist* read_buf, uint64_t read_len,
                                     Context* on_finish) {
  ldout(m_cct, 20) << "object name = " << object_name
                   << " read len = " << read_len << dendl;

  auto ctx = new FunctionContext([on_finish](int ret) {
    on_finish->complete(ret);
  });

  librados::AioCompletion* read_completion = create_rados_callback(ctx);
  int ret = ioctx->aio_read(object_name, read_completion, read_buf, read_len, 0);
  if(ret < 0) {
    lderr(m_cct) << "failed to read from rados" << dendl;
  }
  read_completion->release();

  return ret;
}

int ObjectCacheStore::evict_objects() {
  ldout(m_cct, 20) << dendl;

  std::list<std::string> obj_list;
  m_policy->get_evict_list(&obj_list);
  for (auto& obj: obj_list) {
    do_evict(obj);
  }
}

int ObjectCacheStore::do_evict(std::string cache_file) {
  ldout(m_cct, 20) << "file = " << cache_file << dendl;

  //TODO(): need a better way to get file path
  std::string pool_name = "rbd";

  size_t pos1 = cache_file.rfind("rbd_data");
  pool_name = cache_file.substr(0, pos1);

  pos1 = cache_file.find_first_of('.');
  size_t pos2 = cache_file.find_last_of('.');
  std::string vol_name = cache_file.substr(pos1+1, pos2-pos1-1);

  std::string cache_dir = m_cache_root_dir + pool_name + "_" + vol_name;

   if (m_dir_num > 0) {
    auto const pos = cache_file.find_last_of('.');
    cache_dir = cache_dir + "/" + std::to_string(stoul(cache_file.substr(pos+1)) % m_dir_num);
  }
  std::string cache_file_path = cache_dir + "/" + cache_file;

  ldout(m_cct, 20) << "delete file: " << cache_file_path << dendl;
  int ret = std::remove(cache_file_path.c_str());
   // evict entry in policy
  if (ret == 0) {
    m_policy->evict_entry(cache_file);
  }

  return ret;
}

} // namespace immutable_obj_cache
} // namespace ceph
