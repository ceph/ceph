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
}

template <typename I>
int SharedPersistentObjectCacher<I>::read_object(std::string file_path,
        ceph::bufferlist* read_data, uint64_t offset, uint64_t length,
        Context *on_finish) {

  auto *cct = m_image_ctx->cct;
  ldout(cct, 20) << "file path: " << file_path << dendl;

  std::string error;
  int ret = read_data->pread_file(file_path.c_str(), offset, length, &error);
  if (ret < 0) {
    ldout(cct, 5) << "read from file return error: " << error
                  << "file path= " << file_path
                  << dendl;
    return ret;
  }

  if (read_data->length() != length) {
    ceph_assert(ret < length);
    read_data->append("0", length - read_data->length());
  }

  ceph_assert(read_data->length() == length);

  return length;
}


} // namespace cache
} // namespace librbd

template class librbd::cache::SharedPersistentObjectCacher<librbd::ImageCtx>;
