// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_OPERATION_ENABLE_FEATURES_REQUEST_H
#define CEPH_LIBRBD_OPERATION_ENABLE_FEATURES_REQUEST_H

#include "librbd/operation/Request.h"

class Context;

namespace librbd {

class ImageCtx;

namespace operation {

template <typename ImageCtxT = ImageCtx>
class EnableFeaturesRequest : public Request<ImageCtxT> {
public:
  static EnableFeaturesRequest *create(ImageCtxT &image_ctx, Context *on_finish,
                                       uint64_t journal_op_tid,
                                       uint64_t features) {
    return new EnableFeaturesRequest(image_ctx, on_finish, journal_op_tid,
                                     features);
  }

  EnableFeaturesRequest(ImageCtxT &image_ctx, Context *on_finish,
                        uint64_t journal_op_tid, uint64_t features);

protected:
  void send_op() override;
  bool should_complete(int r) override;
  bool can_affect_io() const override {
    return true;
  }
  journal::Event create_event(uint64_t op_tid) const override {
    return journal::UpdateFeaturesEvent(op_tid, m_features, true);
  }

private:
  /**
   * EnableFeatures goes through the following state machine:
   *
   * @verbatim
   *
   * <start>
   *    |
   *    v
   * STATE_PREPARE_LOCK
   *    |
   *    v
   * STATE_BLOCK_WRITES
   *    |
   *    v
   * STATE_GET_MIRROR_MODE
   *    |
   *    v
   * STATE_CREATE_JOURNAL (skip if not
   *    |                  required)
   *    v
   * STATE_APPEND_OP_EVENT (skip if journaling
   *    |                   disabled)
   *    v
   * STATE_UPDATE_FLAGS
   *    |
   *    v
   * STATE_SET_FEATURES
   *    |
   *    v
   * STATE_CREATE_OBJECT_MAP (skip if not
   *    |                     required)
   *    v
   * STATE_ENABLE_MIRROR_IMAGE
   *    |
   *    V
   * STATE_NOTIFY_UPDATE
   *    |
   *    | (unblock writes)
   *    v
   * <finish>
   * @endverbatim
   *
   */

  uint64_t m_features;

  bool m_enable_mirroring = false;
  bool m_requests_blocked = false;
  bool m_writes_blocked = false;

  uint64_t m_new_features = 0;
  uint64_t m_enable_flags = 0;
  uint64_t m_features_mask = 0;

  bufferlist m_out_bl;

  void send_prepare_lock();
  Context *handle_prepare_lock(int *result);

  void send_block_writes();
  Context *handle_block_writes(int *result);

  void send_get_mirror_mode();
  Context *handle_get_mirror_mode(int *result);

  void send_create_journal();
  Context *handle_create_journal(int *result);

  void send_append_op_event();
  Context *handle_append_op_event(int *result);

  void send_update_flags();
  Context *handle_update_flags(int *result);

  void send_set_features();
  Context *handle_set_features(int *result);

  void send_create_object_map();
  Context *handle_create_object_map(int *result);

  void send_enable_mirror_image();
  Context *handle_enable_mirror_image(int *result);

  void send_notify_update();
  Context *handle_notify_update(int *result);

  Context *handle_finish(int r);
};

} // namespace operation
} // namespace librbd

extern template class librbd::operation::EnableFeaturesRequest<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_OPERATION_ENABLE_FEATURES_REQUEST_H
