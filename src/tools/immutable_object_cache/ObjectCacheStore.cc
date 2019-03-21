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
        m_ioctx_map_lock("ceph::cache::ObjectCacheStore::m_ioctx_map_lock") {

  m_cache_root_dir =
    m_cct->_conf.get_val<std::string>("immutable_object_cache_path");

  if (m_cache_root_dir.back() != '/') {
    m_cache_root_dir += "/";
  }

  uint64_t cache_max_size =
    m_cct->_conf.get_val<Option::size_t>("immutable_object_cache_max_size");

  double cache_watermark =
    m_cct->_conf.get_val<double>("immutable_object_cache_watermark");

  uint64_t max_inflight_ops =
    m_cct->_conf.get_val<uint64_t>("immutable_object_cache_max_inflight_ops");

  m_policy = new SimplePolicy(m_cct, cache_max_size, max_inflight_ops,
                              cache_watermark);
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
  if (ret < 0) {
    lderr(m_cct) << "fail to connect to cluster" << dendl;
    return ret;
  }

  // TODO(dehao): fsck and reuse existing cache objects
  if (reset) {
    std::error_code ec;
    if (efs::exists(m_cache_root_dir)) {
      // remove all sub folders
      for (auto& p : efs::directory_iterator(m_cache_root_dir)) {
        efs::remove_all(p.path());
      }
    } else {
      if (!efs::create_directories(m_cache_root_dir, ec)) {
        lderr(m_cct) << "fail to create cache store dir: " << ec << dendl;
        return ec.value();
      }
    }
  }
  return 0;
}

int ObjectCacheStore::shutdown() {
  ldout(m_cct, 20) << dendl;

  m_rados->shutdown();
  return 0;
}

int ObjectCacheStore::init_cache() {
  ldout(m_cct, 20) << dendl;
  std::string cache_dir = m_cache_root_dir;

  return 0;
}

int ObjectCacheStore::do_promote(std::string pool_nspace,
                                  uint64_t pool_id, uint64_t snap_id,
                                  std::string object_name) {
  ldout(m_cct, 20) << "to promote object: " << object_name
                   << " from pool id: " << pool_id
                   << " namespace: " << pool_nspace
                   << " snapshot: " << snap_id << dendl;

  int ret = 0;
  std::string cache_file_name = std::move(get_cache_file_name(pool_nspace,
                                          pool_id, snap_id, object_name));
  librados::IoCtx ioctx;
  {
    Mutex::Locker _locker(m_ioctx_map_lock);
    if (m_ioctx_map.find(pool_id) == m_ioctx_map.end()) {
      ret = m_rados->ioctx_create2(pool_id, ioctx);
      if (ret < 0) {
        lderr(m_cct) << "fail to create ioctx" << dendl;
        return ret;
      }
      m_ioctx_map.emplace(pool_id, ioctx);
    } else {
      ioctx = m_ioctx_map[pool_id];
    }
  }

  ioctx.set_namespace(pool_nspace);
  ioctx.snap_set_read(snap_id);

  librados::bufferlist* read_buf = new librados::bufferlist();

  auto ctx = new FunctionContext([this, read_buf, cache_file_name](int ret) {
    handle_promote_callback(ret, read_buf, cache_file_name);
  });

  return promote_object(&ioctx, object_name, read_buf, ctx);
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

  std::string cache_file_path = std::move(
    get_cache_file_path(cache_file_name, true));

  if (cache_file_path == "") {
    lderr(m_cct) << "fail to write cache file" << dendl;
    m_policy->update_status(cache_file_name, OBJ_CACHE_NONE);
    delete read_buf;
    return -ENOSPC;
  }

  ret = read_buf->write_file(cache_file_path.c_str());
  if (ret < 0) {
    lderr(m_cct) << "fail to write cache file" << dendl;

    m_policy->update_status(cache_file_name, OBJ_CACHE_NONE);
    delete read_buf;
    return ret;
  }

  // update metadata
  ceph_assert(OBJ_CACHE_SKIP == m_policy->get_status(cache_file_name));
  m_policy->update_status(cache_file_name, OBJ_CACHE_PROMOTED, read_buf->length());
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
  std::string cache_file_name = std::move(get_cache_file_name(pool_nspace,
                                            pool_id, snap_id, object_name));

  cache_status_t ret = m_policy->lookup_object(cache_file_name);

  switch (ret) {
    case OBJ_CACHE_NONE: {
      pret = do_promote(pool_nspace, pool_id, snap_id, object_name);
      if (pret < 0) {
        lderr(m_cct) << "fail to start promote" << dendl;
      }
      return ret;
    }
    case OBJ_CACHE_PROMOTED:
      target_cache_file_path = get_cache_file_path(cache_file_name);
      return ret;
    case OBJ_CACHE_SKIP:
      return ret;
    default:
      lderr(m_cct) << "unrecognized object cache status" << dendl;
      ceph_assert(0);
  }
}

int ObjectCacheStore::promote_object(librados::IoCtx* ioctx,
                                     std::string object_name,
                                     librados::bufferlist* read_buf,
                                     Context* on_finish) {
  ldout(m_cct, 20) << "object name = " << object_name << dendl;

  librados::AioCompletion* read_completion = create_rados_callback(on_finish);
  // issue a zero-sized read req to get the entire obj
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
  for (auto& obj : obj_list) {
    do_evict(obj);
  }
  return 0;
}

int ObjectCacheStore::do_evict(std::string cache_file) {
  ldout(m_cct, 20) << "file = " << cache_file << dendl;

  if (cache_file == "") {
    return 0;
  }

  std::string cache_file_path = std::move(get_cache_file_path(cache_file));

  ldout(m_cct, 20) << "evict cache: " << cache_file_path << dendl;

  // TODO(dehao): possible race on read?
  int ret = std::remove(cache_file_path.c_str());
  // evict metadata
  if (ret == 0) {
    m_policy->update_status(cache_file, OBJ_CACHE_SKIP);
    m_policy->evict_entry(cache_file);
  }

  return ret;
}

std::string ObjectCacheStore::get_cache_file_name(std::string pool_nspace,
                                                       uint64_t pool_id,
                                                       uint64_t snap_id,
                                                       std::string oid) {
  return pool_nspace + ":" + std::to_string(pool_id) + ":" +
         std::to_string(snap_id) + ":" + oid;
}

std::string ObjectCacheStore::get_cache_file_path(std::string cache_file_name,
                                                  bool mkdir) {
  ldout(m_cct, 20) << cache_file_name <<dendl;

  uint32_t crc = 0;
  crc = ceph_crc32c(0, (unsigned char *)cache_file_name.c_str(),
                    cache_file_name.length());

  std::string cache_file_dir = std::to_string(crc % 100) + "/";

  if (mkdir) {
    ldout(m_cct, 20) << "creating cache dir: " << cache_file_dir <<dendl;
    std::error_code ec;
    std::string new_dir = m_cache_root_dir + cache_file_dir;
    if (efs::exists(new_dir, ec)) {
      ldout(m_cct, 20) << "cache dir exists: " << cache_file_dir <<dendl;
      return new_dir + cache_file_name;
    }

    if (!efs::create_directories(new_dir, ec)) {
      ldout(m_cct, 5) << "fail to create cache dir: " << new_dir
                      << "error: " << ec.message() << dendl;
      return "";
    }
  }

  return m_cache_root_dir + cache_file_dir + cache_file_name;
}

}  // namespace immutable_obj_cache
}  // namespace ceph
