// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_RBD_MIRROR_IMAGE_MAP_UPDATE_REQUEST_H
#define CEPH_RBD_MIRROR_IMAGE_MAP_UPDATE_REQUEST_H

#include "cls/rbd/cls_rbd_types.h"
#include "include/rados/librados.hpp"

class Context;

namespace librbd { class ImageCtx; }

namespace rbd {
namespace mirror {
namespace image_map {

template<typename ImageCtxT = librbd::ImageCtx>
class UpdateRequest {
public:
  // accepts an image map for updation and a collection of
  // global image ids to purge.
  static UpdateRequest *create(librados::IoCtx &ioctx,
                               std::map<std::string, cls::rbd::MirrorImageMap> &&update_mapping,
                               std::set<std::string> &&remove_global_image_ids, Context *on_finish) {
    return new UpdateRequest(ioctx, std::move(update_mapping), std::move(remove_global_image_ids),
                             on_finish);
  }

  void send();

private:
  /**
   * @verbatim
   *
   *     <start>
   *        |       . . . . . . . .
   *        v       v             . MAX_UPDATE
   *  UPDATE_IMAGE_MAP. . . . . . .
   *        |
   *        v
   *    <finish>
   *
   * @endverbatim
   */
  UpdateRequest(librados::IoCtx &ioctx,
                std::map<std::string, cls::rbd::MirrorImageMap> &&update_mapping,
                std::set<std::string> &&remove_global_image_ids, Context *on_finish);

  librados::IoCtx &m_ioctx;
  std::map<std::string, cls::rbd::MirrorImageMap> m_update_mapping;
  std::set<std::string> m_remove_global_image_ids;
  Context *m_on_finish;

  void update_image_map();
  void handle_update_image_map(int r);

  void finish(int r);
};

} // namespace image_map
} // namespace mirror
} // namespace rbd

#endif // CEPH_RBD_MIRROR_IMAGE_MAP_UPDATE_REQUEST_H
