// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

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

ObjectCacheStore::ObjectCacheStore(CephContext *cct)
      : m_cct(cct), m_rados(new librados::Rados()),
        m_ioctxs_lock("ceph::cache::ObjectCacheStore::m_ioctxs_lock") {

  object_cache_max_size =
    m_cct->_conf.get_val<Option::size_t>("immutable_object_cache_max_size");

  std::string cache_path = m_cct->_conf.get_val<std::string>("immutable_object_cache_path");
  m_cache_root_dir = cache_path + "/ceph_immutable_obj_cache/";

  //TODO(): allow to set cache level
  m_policy = new SimplePolicy(m_cct, object_cache_max_size/(4096*1024), 0.1);
}

ObjectCacheStore::~ObjectCacheStore() {
  delete m_policy;
}

int ObjectCacheStore::init(bool reset) {
  ldout(m_cct, 20) << dendl;

  int ret = m_rados->init_with_context(m_cct);
  if (ret < 0) {
    lderr(m_cct) << "fail to init Ceph context" << dendl;
    return ret;
  }

  ret = m_rados->connect();
  if (ret < 0 ) {
    lderr(m_cct) << "fail to connect to cluster" << dendl;
    return ret;
  }

  //TODO(): fsck and reuse existing cache objects
  if (reset) {
    std::error_code ec;
    if (efs::exists(m_cache_root_dir)) {
      if (!efs::remove_all(m_cache_root_dir, ec)) {
        lderr(m_cct) << "fail to remove old cache store: " << ec << dendl;
	return -1;
      }
    }

    if (!efs::create_directories(m_cache_root_dir, ec)) {
        lderr(m_cct) << "fail to create cache store dir: " << ec << dendl;
	return -1;
    }
  }

  return ret;
}

int ObjectCacheStore::shutdown() {
  ldout(m_cct, 20) << dendl;

  m_rados->shutdown();
  return 0;
}

int ObjectCacheStore::init_cache() {
  ldout(m_cct, 20) << dendl;
  std::string cache_dir = m_cache_root_dir;

  int dir = m_dir_num - 1;
  while (dir >= 0) {
    efs::create_directories(cache_dir + "/" + std::to_string(dir));
    dir --;
  }
  return 0;
}

int ObjectCacheStore::do_promote(std::string pool_nspace,
                                  uint64_t pool_id, uint64_t snap_id,
                                  std::string object_name) {
  ldout(m_cct, 20) << "to promote object = "
                   << object_name << " from pool ID : "
                   << pool_id << dendl;

  int ret = 0;
  std::string cache_file_name = std::move(generate_cache_file_name(pool_nspace,
                                          pool_id, snap_id, object_name));
  {
    Mutex::Locker _locker(m_ioctxs_lock);
    if (m_ioctxs.find(pool_id) == m_ioctxs.end()) {
      librados::IoCtx* io_ctx = new librados::IoCtx();
      ret = m_rados->ioctx_create2(pool_id, *io_ctx);
      if (ret < 0) {
        lderr(m_cct) << "fail to create ioctx" << dendl;
        return ret;
      }
      m_ioctxs.emplace(pool_id, io_ctx);
    }
  }

  ceph_assert(m_ioctxs.find(pool_id) != m_ioctxs.end());

  librados::IoCtx* ioctx = m_ioctxs[pool_id];

  librados::bufferlist* read_buf = new librados::bufferlist();

  auto ctx = new FunctionContext([this, read_buf, cache_file_name](int ret) {
      handle_promote_callback(ret, read_buf, cache_file_name);
   });

   return promote_object(ioctx, object_name, read_buf, ctx);
}

int ObjectCacheStore::handle_promote_callback(int ret, bufferlist* read_buf,
  std::string cache_file_name) {
  ldout(m_cct, 20) << " cache_file_name: " << cache_file_name << dendl;

  // rados read error
  if (ret != -ENOENT && ret < 0) {
    lderr(m_cct) << "fail to read from rados" << dendl;

    m_policy->update_status(cache_file_name, OBJ_CACHE_NONE);
    delete read_buf;
    return ret;
  }

  if (ret == -ENOENT) {
    // object is empty
    ret = 0;
  }

  std::string cache_file_path = std::move(generate_cache_file_path(cache_file_name));

  ret = read_buf->write_file(cache_file_path.c_str());
  if (ret < 0) {
    lderr(m_cct) << "fail to write cache file" << dendl;

    m_policy->update_status(cache_file_name, OBJ_CACHE_NONE);
    delete read_buf;
    return ret;
  }

  // update metadata
  ceph_assert(OBJ_CACHE_SKIP == m_policy->get_status(cache_file_name));
  m_policy->update_status(cache_file_name, OBJ_CACHE_PROMOTED);
  ceph_assert(OBJ_CACHE_PROMOTED == m_policy->get_status(cache_file_name));

  delete read_buf;

  evict_objects();

  return ret;
}

int ObjectCacheStore::lookup_object(std::string pool_nspace,
                                    uint64_t pool_id, uint64_t snap_id,
                                    std::string object_name,
                                    std::string& target_cache_file_path) {
  ldout(m_cct, 20) << "object name = " << object_name
                   << " in pool ID : " << pool_id << dendl;

  int pret = -1;
  std::string cache_file_name = std::move(generate_cache_file_name(pool_nspace,
                                            pool_id, snap_id, object_name));

  cache_status_t ret = m_policy->lookup_object(cache_file_name);

  switch(ret) {
    case OBJ_CACHE_NONE: {
      pret = do_promote(pool_nspace, pool_id, snap_id, object_name);
      if (pret < 0) {
        lderr(m_cct) << "fail to start promote" << dendl;
      }
      return -1;
    }
    case OBJ_CACHE_PROMOTED:
      // librbd hook will go to target_cache_file_path to read data
      target_cache_file_path = std::move(generate_cache_file_path(cache_file_name));
      return 0;
    case OBJ_CACHE_SKIP:
      return -1;
    default:
      lderr(m_cct) << "unrecognized object cache status." << dendl;
      ceph_assert(0);
  }
}

int ObjectCacheStore::promote_object(librados::IoCtx* ioctx,
                                     std::string object_name,
                                     librados::bufferlist* read_buf,
                                     Context* on_finish) {
  ldout(m_cct, 20) << "object name = " << object_name << dendl;

  librados::AioCompletion* read_completion = create_rados_callback(on_finish);
  // issue a zero-sized read req to get full obj
  int ret = ioctx->aio_read(object_name, read_completion, read_buf, 0, 0);
  if (ret < 0) {
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

  if (cache_file == "") {
    return 0;
  }

  std::string cache_file_path = std::move(generate_cache_file_path(cache_file));

  ldout(m_cct, 20) << "delete file: " << cache_file_path << dendl;
  int ret = std::remove(cache_file_path.c_str());
   // evict entry in policy
  if (ret == 0) {
    m_policy->update_status(cache_file, OBJ_CACHE_SKIP);
    m_policy->evict_entry(cache_file);
  }

  return ret;
}

std::string ObjectCacheStore::generate_cache_file_name(std::string pool_nspace,
                                                       uint64_t pool_id,
                                                       uint64_t snap_id,
                                                       std::string oid) {
  return pool_nspace + ":" + std::to_string(pool_id) + ":" +
         std::to_string(snap_id) + ":" + oid;
}

std::string ObjectCacheStore::generate_cache_file_path(std::string cache_file_name) {

  std::string cache_file_dir = "";

  if (m_dir_num > 0) {
    auto const pos = cache_file_name.find_last_of('.');
    cache_file_dir = std::to_string(stoul(cache_file_name.substr(pos+1)) % m_dir_num);
  }

  return m_cache_root_dir + cache_file_dir + "/" + cache_file_name;
}

} // namespace immutable_obj_cache
} // namespace ceph
