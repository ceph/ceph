// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_RBD_MIRROR_IMAGE_MAP_LOAD_REQUEST_H
#define CEPH_RBD_MIRROR_IMAGE_MAP_LOAD_REQUEST_H

#include "cls/rbd/cls_rbd_types.h"
#include "include/rados/librados.hpp"

class Context;

namespace librbd { class ImageCtx; }

namespace rbd {
namespace mirror {
namespace image_map {

template<typename ImageCtxT = librbd::ImageCtx>
class LoadRequest {
public:
  static LoadRequest *create(librados::IoCtx &ioctx,
                             std::map<std::string, cls::rbd::MirrorImageMap> *image_mapping,
                             Context *on_finish) {
    return new LoadRequest(ioctx, image_mapping, on_finish);
  }

  void send();

private:
  /**
   * @verbatim
   *
   *     <start>
   *        |     . . . . . . . .
   *        v     v             . MAX_RETURN
   *  IMAGE_MAP_LIST. . . . . . .
   *        |
   *        v
   *  MIRROR_IMAGE_LIST
   *        |
   *        v
   *  CLEANUP_IMAGE_MAP
   *        |
   *        v
   *    <finish>
   *
   * @endverbatim
   */
  LoadRequest(librados::IoCtx &ioctx,
              std::map<std::string, cls::rbd::MirrorImageMap> *image_mapping,
              Context *on_finish);

  librados::IoCtx &m_ioctx;
  std::map<std::string, cls::rbd::MirrorImageMap> *m_image_mapping;
  Context *m_on_finish;

  std::set<std::string> m_global_image_ids;

  bufferlist m_out_bl;
  std::string m_start_after;

  void image_map_list();
  void handle_image_map_list(int r);

  void mirror_image_list();
  void handle_mirror_image_list(int r);

  void cleanup_image_map();

  void finish(int r);
};

} // namespace image_map
} // namespace mirror
} // namespace rbd

#endif // CEPH_RBD_MIRROR_IMAGE_MAP_LOAD_REQUEST_H
