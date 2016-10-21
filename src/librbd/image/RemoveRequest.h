// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_IMAGE_REMOVE_REQUEST_H
#define CEPH_LIBRBD_IMAGE_REMOVE_REQUEST_H

#include "include/int_types.h"
#include "include/rbd/librbd.hpp"
#include "include/rados/librados.hpp"
#include "librbd/ImageCtx.h"
#include "librbd/Journal.h"
#include "librbd/journal/Types.h"
#include "cls/rbd/cls_rbd_types.h"
#include "librbd/journal/TypeTraits.h"
#include "cls/journal/cls_journal_types.h"

using librados::IoCtx;

class Context;
class ContextWQ;
class SafeTimer;

namespace librbd {

class ImageCtx;
class ProgressContext;

namespace image {

template<typename ImageCtxT = ImageCtx>
class RemoveRequest {
public:
  static RemoveRequest *create(IoCtx &ioctx, const std::string &image_name, const std::string &image_id,
                               bool force, ProgressContext &prog_ctx, ContextWQ *op_work_queue,
                               Context *on_finish) {
    return new RemoveRequest(ioctx, image_name, image_id, force, prog_ctx, op_work_queue, on_finish);
  }

  void send();

private:
  RemoveRequest(IoCtx &ioctx, const std::string &image_name, const std::string &image_id,
                bool force, ProgressContext &prog_ctx, ContextWQ *op_work_queue, Context *on_finish);

  IoCtx m_ioctx;
  std::string m_image_name;
  std::string m_image_id;
  bool m_force;
  ProgressContext &m_prog_ctx;
  ContextWQ *m_op_work_queue;
  Context *m_on_finish;

  CephContext *m_cct;
  std::string m_header_oid;
  bool m_old_format = false;
  bool m_unknown_format = true;
  ImageCtxT *m_image_ctx;

  int m_retval = 0;
  bufferlist m_out_bl;
  std::list<obj_watch_t> m_watchers;

  void open_image();
  Context *handle_open_image(int *result);

  void mirror_image_remove();
  Context* handle_mirror_image_remove(int *result);

  void check_exclusive_lock();

  void acquire_exclusive_lock();
  Context *handle_exclusive_lock(int *result);
  Context *handle_exclusive_lock_force(int *result);

  void validate_image_removal();
  void check_image_snaps();

  void check_image_watchers();
  Context *handle_check_image_watchers(int *result);

  void check_image_consistency_group();
  Context *handle_check_image_consistency_group(int *result);

  void disable_features();
  Context *handle_disable_features(int *result);

  void trim_image();
  Context *handle_trim_image(int *result);

  void remove_child();
  Context *handle_remove_child(int *result);

  void send_close_image(int r);
  Context *handle_send_close_image(int *result);

  void switch_thread_context();
  void handle_switch_thread_context(int r);

  void remove_header();
  Context *handle_remove_header(int *result);

  void remove_image();

  void remove_v1_image();
  void handle_remove_v1_image(int r);

  void remove_v2_image();

  void dir_get_image_id();
  Context *handle_dir_get_image_id(int *result);

  void dir_get_image_name();
  Context *handle_dir_get_image_name(int *result);

  void remove_id_object();
  Context *handle_remove_id_object(int *result);

  void dir_remove_image();
  Context *handle_dir_remove_image(int *result);
};

} // namespace image
} // namespace librbd

extern template class librbd::image::RemoveRequest<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_IMAGE_REMOVE_REQUEST_H
