// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_JOURNAL_REMOVE_REQUEST_H
#define CEPH_LIBRBD_JOURNAL_REMOVE_REQUEST_H

#include "include/int_types.h"
#include "include/buffer.h"
#include "include/rados/librados.hpp"
#include "include/rbd/librbd.hpp"
#include "common/Mutex.h"
#include "librbd/ImageCtx.h"
#include "journal/Journaler.h"
#include "librbd/journal/TypeTraits.h"

using librados::IoCtx;
using journal::Journaler;

class Context;
class ContextWQ;
class SafeTimer;

namespace journal {
  class Journaler;
}

namespace librbd {

class ImageCtx;

namespace journal {

template<typename ImageCtxT = ImageCtx>
class RemoveRequest {
public:
  static RemoveRequest *create(IoCtx &ioctx, const std::string &image_id,
                                      const std::string &client_id,
                                      ContextWQ *op_work_queue, Context *on_finish) {
    return new RemoveRequest(ioctx, image_id, client_id,
                                    op_work_queue, on_finish);
  }

  void send();

private:
  typedef typename TypeTraits<ImageCtxT>::Journaler Journaler;

  RemoveRequest(IoCtx &ioctx, const std::string &image_id,
                       const std::string &client_id,
                       ContextWQ *op_work_queue, Context *on_finish);

  IoCtx &m_ioctx;
  std::string m_image_id;
  std::string m_image_client_id;
  ContextWQ *m_op_work_queue;
  Context *m_on_finish;

  CephContext *m_cct;
  Journaler *m_journaler;
  SafeTimer *m_timer;
  Mutex *m_timer_lock;
  int m_r_saved;

  void stat_journal();
  Context *handle_stat_journal(int *result);

  void init_journaler();
  Context *handle_init_journaler(int *result);

  void remove_journal();
  Context *handle_remove_journal(int *result);

  void shut_down_journaler(int r);
  Context *handle_journaler_shutdown(int *result);
};

} // namespace journal
} // namespace librbd

extern template class librbd::journal::RemoveRequest<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_JOURNAL_REMOVE_REQUEST_H
