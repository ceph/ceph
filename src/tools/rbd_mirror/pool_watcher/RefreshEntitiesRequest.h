// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_RBD_MIRROR_POOL_WATCHER_REFRESH_IMAGES_REQUEST_H
#define CEPH_RBD_MIRROR_POOL_WATCHER_REFRESH_IMAGES_REQUEST_H

#include "include/buffer.h"
#include "include/rados/librados.hpp"
#include "cls/rbd/cls_rbd_types.h"
#include "tools/rbd_mirror/Types.h"
#include <string>

struct Context;

namespace librbd { struct ImageCtx; }

namespace rbd {
namespace mirror {
namespace pool_watcher {

template <typename ImageCtxT = librbd::ImageCtx>
class RefreshEntitiesRequest {
public:
  static RefreshEntitiesRequest *create(librados::IoCtx &remote_io_ctx,
                                        std::map<MirrorEntity,
                                        std::string> *entities,
                                        Context *on_finish) {
    return new RefreshEntitiesRequest(remote_io_ctx, entities, on_finish);
  }

  RefreshEntitiesRequest(librados::IoCtx &remote_io_ctx,
                         std::map<MirrorEntity,
                         std::string> *entities,
                         Context *on_finish)
    : m_remote_io_ctx(remote_io_ctx), m_entities(entities),
      m_on_finish(on_finish) {
  }

  void send();

private:
  /**
   * @verbatim
   *
   * <start>
   *    |
   *    |   /-------------\
   *    |   |             |
   *    v   v             | (more images)
   * MIRROR_IMAGE_LIST ---/
   *    |
   *    |   /-------------\
   *    |   |             |
   *    v   v             | (more groups)
   * MIRROR_GROUP_LIST ---/
   *    |
   *    |   /-------------\
   *    |   |             |
   *    v   v             | (for every group)
   * GROUP_IMAGE_LIST ----/
   *    |
   *    v
   * <finish>
   *
   * @endverbatim
   */

  librados::IoCtx &m_remote_io_ctx;
  std::map<MirrorEntity, std::string> *m_entities;
  Context *m_on_finish;

  bufferlist m_out_bl;
  std::string m_start_after;

  std::map<std::string, cls::rbd::MirrorGroup> m_groups;
  cls::rbd::GroupImageSpec m_start_group_image_list_after;
  size_t m_group_size = 0;

  void mirror_image_list();
  void handle_mirror_image_list(int r);

  void mirror_group_list();
  void handle_mirror_group_list(int r);

  void group_image_list();
  void handle_group_image_list(int r);

  void finish(int r);

};

} // namespace pool_watcher
} // namespace mirror
} // namespace rbd

extern template class rbd::mirror::pool_watcher::RefreshEntitiesRequest<librbd::ImageCtx>;

#endif // CEPH_RBD_MIRROR_POOL_WATCHER_REFRESH_IMAGES_REQUEST_H
