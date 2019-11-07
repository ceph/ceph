// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_IMAGE_REMOVE_REQUEST_H
#define CEPH_LIBRBD_IMAGE_REMOVE_REQUEST_H

#include "include/rados/librados.hpp"
#include "librbd/ImageCtx.h"
#include "librbd/image/TypeTraits.h"

#include <list>

class Context;
class ContextWQ;
class SafeTimer;

namespace librbd {

class ProgressContext;

namespace image {

template<typename ImageCtxT = ImageCtx>
class RemoveRequest {
private:
  // mock unit testing support
  typedef ::librbd::image::TypeTraits<ImageCtxT> TypeTraits;
  typedef typename TypeTraits::ContextWQ ContextWQ;
public:
  static RemoveRequest *create(librados::IoCtx &ioctx,
                               const std::string &image_name,
                               const std::string &image_id,
                               bool force, bool from_trash_remove,
                               ProgressContext &prog_ctx,
                               ContextWQ *op_work_queue,
                               Context *on_finish) {
    return new RemoveRequest(ioctx, image_name, image_id, force,
                             from_trash_remove, prog_ctx, op_work_queue,
                             on_finish);
  }

  static RemoveRequest *create(librados::IoCtx &ioctx, ImageCtxT *image_ctx,
                               bool force, bool from_trash_remove,
                               ProgressContext &prog_ctx,
                               ContextWQ *op_work_queue,
                               Context *on_finish) {
    return new RemoveRequest(ioctx, image_ctx, force, from_trash_remove,
                             prog_ctx, op_work_queue, on_finish);
  }

  void send();

private:
  /**
   * @verbatim
   *
   *                                  <start>
   *                                     |
   *                                     v
   *       (skip if already opened) OPEN IMAGE------------------\
   *                                     |                      |
   *                                     v                      |
   *                              PRE REMOVE IMAGE * * *        |
   *                                     |             *        |
   *                                     v             *        |
   *                                TRIM IMAGE * * * * *        |
   *                                     |             *        |
   *                                     v             *        |
   *                                DETACH CHILD       *        |
   *                                     |             *        |
   *                                     v             *        v
   *                               CLOSE IMAGE < * * * *        |
   *                                     |                      |
   *                               error v                      |
   * /------<--------\              REMOVE HEADER<--------------/
   * |               |                /  |
   * |               |-------<-------/   |
   * |               |                   v
   * |               |              REMOVE JOURNAL
   * |               |                /  |
   * |               |-------<-------/   |
   * |               |                   v
   * v               ^          REMOVE OBJECTMAP
   * |               |                /  |
   * |               |-------<-------/   |
   * |               |                   v
   * |               |    REMOVE MIRROR IMAGE
   * |               |                /  |
   * |               |-------<-------/   |
   * |               |                   v
   * |               |            REMOVE ID OBJECT
   * |               |                /  |
   * |               |-------<-------/   |
   * |               |                   v
   * |               |              REMOVE IMAGE
   * |               |                /  |
   * |               \-------<-------/   |
   * |                                   v
   * \------------------>------------<finish>
   *
   * @endverbatim
   */

  RemoveRequest(librados::IoCtx &ioctx, const std::string &image_name,
                const std::string &image_id, bool force, bool from_trash_remove,
                ProgressContext &prog_ctx, ContextWQ *op_work_queue,
                Context *on_finish);

  RemoveRequest(librados::IoCtx &ioctx, ImageCtxT *image_ctx, bool force,
                bool from_trash_remove, ProgressContext &prog_ctx,
                ContextWQ *op_work_queue, Context *on_finish);

  librados::IoCtx &m_ioctx;
  std::string m_image_name;
  std::string m_image_id;
  ImageCtxT *m_image_ctx = nullptr;
  bool m_force;
  bool m_from_trash_remove;
  ProgressContext &m_prog_ctx;
  ContextWQ *m_op_work_queue;
  Context *m_on_finish;

  CephContext *m_cct;
  std::string m_header_oid;
  bool m_old_format = false;
  bool m_unknown_format = true;

  librados::IoCtx m_parent_io_ctx;

  decltype(m_image_ctx->exclusive_lock) m_exclusive_lock = nullptr;

  int m_ret_val = 0;
  bufferlist m_out_bl;
  std::list<obj_watch_t> m_watchers;

  std::map<uint64_t, SnapInfo> m_snap_infos;

  void open_image();
  void handle_open_image(int r);

  void send_journal_remove();
  void handle_journal_remove(int r);

  void send_object_map_remove();
  void handle_object_map_remove(int r);

  void mirror_image_remove();
  void handle_mirror_image_remove(int r);

  void pre_remove_image();
  void handle_pre_remove_image(int r);

  void trim_image();
  void handle_trim_image(int r);

  void detach_child();
  void handle_detach_child(int r);

  void send_disable_mirror();
  void handle_disable_mirror(int r);

  void send_close_image(int r);
  void handle_send_close_image(int r);

  void remove_header();
  void handle_remove_header(int r);

  void remove_header_v2();
  void handle_remove_header_v2(int r);

  void remove_image();

  void remove_v1_image();
  void handle_remove_v1_image(int r);

  void remove_v2_image();

  void dir_get_image_id();
  void handle_dir_get_image_id(int r);

  void dir_get_image_name();
  void handle_dir_get_image_name(int r);

  void remove_id_object();
  void handle_remove_id_object(int r);

  void dir_remove_image();
  void handle_dir_remove_image(int r);

  void finish(int r);
};

} // namespace image
} // namespace librbd

extern template class librbd::image::RemoveRequest<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_IMAGE_REMOVE_REQUEST_H
