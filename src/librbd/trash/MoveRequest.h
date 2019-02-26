// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_TRASH_MOVE_REQUEST_H
#define CEPH_LIBRBD_TRASH_MOVE_REQUEST_H

#include "include/utime.h"
#include "include/rados/librados.hpp"
#include "cls/rbd/cls_rbd_types.h"
#include <string>

struct CephContext;
struct Context;

namespace librbd {

struct ImageCtx;

namespace trash {

template <typename ImageCtxT = librbd::ImageCtx>
class MoveRequest {
public:
  static MoveRequest* create(librados::IoCtx& io_ctx,
                             const std::string& image_id,
                             const cls::rbd::TrashImageSpec& trash_image_spec,
                             Context* on_finish) {
    return new MoveRequest(io_ctx, image_id, trash_image_spec, on_finish);
  }

  MoveRequest(librados::IoCtx& io_ctx, const std::string& image_id,
              const cls::rbd::TrashImageSpec& trash_image_spec,
              Context* on_finish)
    : m_io_ctx(io_ctx), m_image_id(image_id),
      m_trash_image_spec(trash_image_spec), m_on_finish(on_finish),
      m_cct(reinterpret_cast<CephContext *>(io_ctx.cct())) {
  }

  void send();

private:
  /*
   * @verbatim
   *
   * <start>
   *    |
   *    v
   * TRASH_ADD
   *    |
   *    v
   * REMOVE_ID
   *    |
   *    v
   * DIRECTORY_REMOVE
   *    |
   *    v
   * <finish>
   *
   * @endverbatim
   */

  librados::IoCtx &m_io_ctx;
  std::string m_image_id;
  cls::rbd::TrashImageSpec m_trash_image_spec;
  Context *m_on_finish;

  CephContext *m_cct;

  void trash_add();
  void handle_trash_add(int r);

  void remove_id();
  void handle_remove_id(int r);

  void directory_remove();
  void handle_directory_remove(int r);

  void finish(int r);

};

} // namespace trash
} // namespace librbd

extern template class librbd::trash::MoveRequest<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_TRASH_MOVE_REQUEST_H
