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
  static CreateRequest *create(IoCtx &ioctx, std::string &imgname, std::string &imageid,
                               uint64_t size, int order, uint64_t features,
                               uint64_t stripe_unit, uint64_t stripe_count,
                               uint8_t journal_order, uint8_t journal_splay_width,
                               const std::string &journal_pool,
                               const std::string &non_primary_global_image_id,
                               const std::string &primary_mirror_uuid,
                               ContextWQ *op_work_queue, Context *on_finish) {
    return new CreateRequest(ioctx, imgname, imageid, size, order, features, stripe_unit,
                             stripe_count, journal_order, journal_splay_width, journal_pool,
                             non_primary_global_image_id, primary_mirror_uuid,
                             op_work_queue, on_finish);
  }

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

  CreateRequest(IoCtx &ioctx, std::string &imgname, std::string &imageid,
                uint64_t size, int order, uint64_t features,
                uint64_t stripe_unit, uint64_t stripe_count, uint8_t journal_order,
                uint8_t journal_splay_width, const std::string &journal_pool,
                const std::string &non_primary_global_image_id,
                const std::string &primary_mirror_uuid,
                ContextWQ *op_work_queue, Context *on_finish);

  IoCtx m_ioctx;
  std::string m_image_name;
  std::string m_image_id;
  uint64_t m_size;
  int m_order;
  uint64_t m_features, m_stripe_unit, m_stripe_count;
  uint8_t m_journal_order, m_journal_splay_width;
  const std::string m_journal_pool;
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
