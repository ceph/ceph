// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_JOURNAL_RESET_REQUEST_H
#define CEPH_LIBRBD_JOURNAL_RESET_REQUEST_H

#include "include/int_types.h"
#include "include/buffer.h"
#include "include/rados/librados.hpp"
#include "include/rbd/librbd.hpp"
#include "common/Mutex.h"
#include "librbd/journal/TypeTraits.h"
#include <string>

class Context;
class ContextWQ;
class SafeTimer;

namespace journal { class Journaler; }

namespace librbd {

class ImageCtx;

namespace journal {

template<typename ImageCtxT = ImageCtx>
class ResetRequest {
public:
  static ResetRequest *create(librados::IoCtx &io_ctx,
                              const std::string &image_id,
                              const std::string &client_id,
                              const std::string &mirror_uuid,
                              ContextWQ *op_work_queue, Context *on_finish) {
    return new ResetRequest(io_ctx, image_id, client_id, mirror_uuid,
                            op_work_queue, on_finish);
  }

  ResetRequest(librados::IoCtx &io_ctx, const std::string &image_id,
               const std::string &client_id, const std::string &mirror_uuid,
               ContextWQ *op_work_queue, Context *on_finish)
    : m_io_ctx(io_ctx), m_image_id(image_id), m_client_id(client_id),
      m_mirror_uuid(mirror_uuid), m_op_work_queue(op_work_queue),
      m_on_finish(on_finish),
      m_cct(reinterpret_cast<CephContext *>(m_io_ctx.cct())) {
  }

  void send();

private:
  /**
   * @verbatim
   *
   * <start>
   *    |
   *    v
   * INIT_JOURNALER
   *    |
   *    v
   * SHUT_DOWN_JOURNALER
   *    |
   *    v
   * REMOVE_JOURNAL
   *    |
   *    v
   * CREATE_JOURNAL
   *    |
   *    v
   * <finish>
   *
   * @endverbatim
   */
  typedef typename TypeTraits<ImageCtxT>::Journaler Journaler;

  librados::IoCtx &m_io_ctx;
  std::string m_image_id;
  std::string m_client_id;
  std::string m_mirror_uuid;
  ContextWQ *m_op_work_queue;
  Context *m_on_finish;

  CephContext *m_cct;
  Journaler *m_journaler = nullptr;
  int m_ret_val = 0;

  uint8_t m_order = 0;
  uint8_t m_splay_width = 0;
  std::string m_object_pool_name;

  void init_journaler();
  void handle_init_journaler(int r);

  void shut_down_journaler();
  void handle_journaler_shutdown(int r);

  void remove_journal();
  void handle_remove_journal(int r);

  void create_journal();
  void handle_create_journal(int r);

  void finish(int r);

};

} // namespace journal
} // namespace librbd

extern template class librbd::journal::ResetRequest<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_JOURNAL_REMOVE_REQUEST_H
