// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/cache/SharedPersistentObjectCacher.h"
#include "include/buffer.h"
#include "common/dout.h"
#include "librbd/ImageCtx.h"

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::cache::SharedPersistentObjectCacher: " << this \
                           << " " <<  __func__ << ": "

namespace librbd {
namespace cache {

template <typename I>
SharedPersistentObjectCacher<I>::SharedPersistentObjectCacher(I *image_ctx, std::string cache_path)
  : m_image_ctx(image_ctx), m_cache_path(cache_path),
    m_file_map_lock("librbd::cache::SharedObjectCacher::filemaplock") {
  auto *cct = m_image_ctx->cct;
  ldout(cct, 20) << dendl;
}

template <typename I>
SharedPersistentObjectCacher<I>::~SharedPersistentObjectCacher() {
  for(auto &it: file_map) {
    if(it.second) {
      delete it.second;
    }
  }
}

template <typename I>
int SharedPersistentObjectCacher<I>::read_object(std::string oid, ceph::bufferlist* read_data, uint64_t offset, uint64_t length, Context *on_finish) {

  auto *cct = m_image_ctx->cct;
  ldout(cct, 20) << "object: " << oid << dendl;

  //TODO(): do not calculate the path, get from the response from daemon
  int dir_num = 10;
  std::string cache_file_name = m_image_ctx->data_ctx.get_pool_name() + oid;
  std::string cache_dir = m_image_ctx->data_ctx.get_pool_name() + "_" + m_image_ctx->id;

   if (dir_num > 0) {
    auto const pos = oid.find_last_of('.');
    cache_dir = cache_dir + "/" + std::to_string(stoul(oid.substr(pos+1)) % dir_num);
  }
  std::string cache_file_path = cache_dir + "/" + cache_file_name;

  SyncFile* target_cache_file;
  if (file_map.find(cache_file_path) == file_map.end()) {
    target_cache_file = new SyncFile(cct, cache_file_path);
    target_cache_file->open_file();
    file_map[cache_file_path] = target_cache_file;
  } else {
    target_cache_file = file_map[cache_file_path];
  }

  int ret = target_cache_file->read_object_from_file(read_data, offset, length);
  if (ret < 0) {
    ldout(cct, 5) << "read from file return error: " << ret 
                  << "file name= " << cache_file_name
                  << dendl;
  }

  return ret;
}


} // namespace cache
} // namespace librbd

template class librbd::cache::SharedPersistentObjectCacher<librbd::ImageCtx>;
