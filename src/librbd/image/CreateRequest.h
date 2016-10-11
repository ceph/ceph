// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_IMAGE_CREATE_REQUEST_H
#define CEPH_LIBRBD_IMAGE_CREATE_REQUEST_H

#include "include/int_types.h"
#include "include/buffer.h"
#include "common/WorkQueue.h"
#include "librbd/ObjectMap.h"
#include "include/rados/librados.hpp"
#include "include/rbd_types.h"
#include "cls/rbd/cls_rbd_types.h"
#include "include/rbd/librbd.hpp"
#include "librbd/ImageCtx.h"
#include "common/Timer.h"
#include "librbd/journal/TypeTraits.h"

class Context;

using librados::IoCtx;

namespace journal {
  class Journaler;
}

namespace librbd {
namespace image {

template <typename ImageCtxT = ImageCtx>
class CreateRequest {
public:
  static CreateRequest *create(IoCtx &ioctx, const std::string &image_name,
                               const std::string &image_id, uint64_t size,
                               const ImageOptions &image_options,
                               const std::string &non_primary_global_image_id,
                               const std::string &primary_mirror_uuid,
                               ContextWQ *op_work_queue, Context *on_finish) {
    return new CreateRequest(ioctx, image_name, image_id, size, image_options,
                             non_primary_global_image_id, primary_mirror_uuid,
                             op_work_queue, on_finish);
  }

  static int validate_order(CephContext *cct, uint8_t order);

  void send();

private:
  /**
   * @verbatim
   *
   *                                  <start> . . . . > . . . . .
   *                                     |                      .
   *                                     v                      .
   *                                VALIDATE POOL               v (pool validation
   *                                     |                      . disabled)
   *                                     v                      .
   * (error: bottom up)           CREATE ID OBJECT. . < . . . . .
   *  _______<_______                    |
   * |               |                   v
   * |               |          ADD IMAGE TO DIRECTORY
   * |               |               /   |
   * |      REMOVE ID OBJECT<-------/    v         (stripingv2 disabled)
   * |               |              CREATE IMAGE. . . . > . . . .
   * v               |               /   |                      .
   * |      REMOVE FROM DIR<--------/    v                      .
   * |               |          SET STRIPE UNIT COUNT           .
   * |               |               /   |  \ . . . . . > . . . .
   * |      REMOVE HEADER OBJ<------/    v                     /. (object-map
   * |               |\           OBJECT MAP RESIZE . . < . . * v  disabled)
   * |               | \              /  |  \ . . . . . > . . . .
   * |               |  *<-----------/   v                     /. (journaling
   * |               |             FETCH MIRROR MODE. . < . . * v  disabled)
   * |               |                /   |                     .
   * |     REMOVE OBJECT MAP<--------/    v                     .
   * |               |\             JOURNAL CREATE              .
   * |               | \               /  |                     .
   * v               |  *<------------/   v                     .
   * |               |           FETCH MIRROR IMAGE             v
   * |               |                /   |                     .
   * |        JOURNAL REMOVE<--------/    v                     .
   * |                \          MIRROR IMAGE ENABLE            .
   * |                 \               /  |                     .
   * |                  *<------------/   v                     .
   * |                              NOTIFY WATCHERS             .
   * |                                    |                     .
   * |                                    v                     .
   * |_____________>___________________<finish> . . . . < . . . .
   *
   * @endverbatim
   */

  CreateRequest(IoCtx &ioctx, const std::string &image_name,
                const std::string &image_id, uint64_t size,
                const ImageOptions &image_options,
                const std::string &non_primary_global_image_id,
                const std::string &primary_mirror_uuid,
                ContextWQ *op_work_queue, Context *on_finish);

  IoCtx m_ioctx;
  std::string m_image_name;
  std::string m_image_id;
  uint64_t m_size;
  uint8_t m_order = 0;
  uint64_t m_features = 0;
  uint64_t m_stripe_unit = 0;
  uint64_t m_stripe_count = 0;
  uint8_t m_journal_order = 0;
  uint8_t m_journal_splay_width = 0;
  std::string m_journal_pool;
  std::string m_data_pool;
  int64_t m_data_pool_id = -1;
  const std::string m_non_primary_global_image_id;
  const std::string m_primary_mirror_uuid;

  ContextWQ *m_op_work_queue;
  Context *m_on_finish;

  CephContext *m_cct;
  int m_r_saved;  // used to return actual error after cleanup
  bool m_force_non_primary;
  file_layout_t m_layout;
  std::string m_id_obj, m_header_obj, m_objmap_name;

  bufferlist m_outbl;
  rbd_mirror_mode_t m_mirror_mode;
  cls::rbd::MirrorImage m_mirror_image_internal;

  void validate_pool();
  Context *handle_validate_pool(int *result);

  void create_id_object();
  Context *handle_create_id_object(int *result);

  void add_image_to_directory();
  Context *handle_add_image_to_directory(int *result);

  void create_image();
  Context *handle_create_image(int *result);

  void set_stripe_unit_count();
  Context *handle_set_stripe_unit_count(int *result);

  void object_map_resize();
  Context *handle_object_map_resize(int *result);

  void fetch_mirror_mode();
  Context *handle_fetch_mirror_mode(int *result);

  void journal_create();
  Context *handle_journal_create(int *result);

  void fetch_mirror_image();
  Context *handle_fetch_mirror_image(int *result);

  void mirror_image_enable();
  Context *handle_mirror_image_enable(int *result);

  void send_watcher_notification();
  void handle_watcher_notify(int r);

  void complete(int r);

  // cleanup
  void journal_remove();
  Context *handle_journal_remove(int *result);

  void remove_object_map();
  Context *handle_remove_object_map(int *result);

  void remove_header_object();
  Context *handle_remove_header_object(int *result);

  void remove_from_dir();
  Context *handle_remove_from_dir(int *result);

  void remove_id_object();
  Context *handle_remove_id_object(int *result);
};

} //namespace image
} //namespace librbd

extern template class librbd::image::CreateRequest<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_IMAGE_CREATE_REQUEST_H
