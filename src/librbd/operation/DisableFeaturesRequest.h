// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_OPERATION_DISABLE_FEATURES_REQUEST_H
#define CEPH_LIBRBD_OPERATION_DISABLE_FEATURES_REQUEST_H

#include "librbd/ImageCtx.h"
#include "librbd/operation/Request.h"
#include "cls/rbd/cls_rbd_client.h"

class Context;

namespace librbd {

class ImageCtx;

namespace operation {

template <typename ImageCtxT = ImageCtx>
class DisableFeaturesRequest : public Request<ImageCtxT> {
public:
  static DisableFeaturesRequest *create(ImageCtxT &image_ctx, Context *on_finish,
                                        uint64_t journal_op_tid,
                                        uint64_t features, bool force) {
    return new DisableFeaturesRequest(image_ctx, on_finish, journal_op_tid,
                                      features, force);
  }

  DisableFeaturesRequest(ImageCtxT &image_ctx, Context *on_finish,
                         uint64_t journal_op_tid, uint64_t features, bool force);

protected:
  void send_op() override;
  bool should_complete(int r) override;
  bool can_affect_io() const override {
    return true;
  }
  journal::Event create_event(uint64_t op_tid) const override {
    return journal::UpdateFeaturesEvent(op_tid, m_features, false);
  }

private:
  /**
   * DisableFeatures goes through the following state machine:
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
   * STATE_ACQUIRE_EXCLUSIVE_LOCK (skip if not
   *    |                          required)
   *    | (disabling journaling)
   *    \-------------------\
   *    |                    |
   *    |                    V
   *    |                 STATE_GET_MIRROR_MODE
   *    |(not                |
   *    | disabling          v
   *    | journaling)     STATE_GET_MIRROR_IMAGE
   *    |                    |
   *    |                    v
   *    |                 STATE_DISABLE_MIRROR_IMAGE (skip if not
   *    |                    |                        required)
   *    |                    v
   *    |                 STATE_CLOSE_JOURNAL
   *    |                    |
   *    |                    v
   *    |                 STATE_REMOVE_JOURNAL
   *    |                    |
   *    |/-------------------/
   *    |
   *    v
   * STATE_APPEND_OP_EVENT (skip if journaling
   *    |                   disabled)
   *    v
   * STATE_REMOVE_OBJECT_MAP (skip if not
   *    |                     disabling object map)
   *    v
   * STATE_SET_FEATURES
   *    |
   *    v
   * STATE_UPDATE_FLAGS
   *    |
   *    v
   * STATE_NOTIFY_UPDATE
   *    |
   *    v
   * STATE_RELEASE_EXCLUSIVE_LOCK (skip if not
   *    |                          required)
   *    | (unblock writes)
   *    v
   * <finish>
   *
   * @endverbatim
   *
   */

  uint64_t m_features;
  bool m_force;

  bool m_acquired_lock = false;
  bool m_writes_blocked = false;
  bool m_image_lock_acquired = false;
  bool m_requests_blocked = false;

  uint64_t m_new_features = 0;
  uint64_t m_disable_flags = 0;
  uint64_t m_features_mask = 0;

  decltype(ImageCtxT::journal) m_journal = nullptr;
  cls::rbd::MirrorMode m_mirror_mode = cls::rbd::MIRROR_MODE_DISABLED;
  bufferlist m_out_bl;

  void send_prepare_lock();
  Context *handle_prepare_lock(int *result);

  void send_block_writes();
  Context *handle_block_writes(int *result);

  Context *send_acquire_exclusive_lock(int *result);
  Context *handle_acquire_exclusive_lock(int *result);

  void send_get_mirror_mode();
  Context *handle_get_mirror_mode(int *result);

  void send_get_mirror_image();
  Context *handle_get_mirror_image(int *result);

  void send_disable_mirror_image();
  Context *handle_disable_mirror_image(int *result);

  void send_close_journal();
  Context *handle_close_journal(int *result);

  void send_remove_journal();
  Context *handle_remove_journal(int *result);

  void send_append_op_event();
  Context *handle_append_op_event(int *result);

  void send_remove_object_map();
  Context *handle_remove_object_map(int *result);

  void send_set_features();
  Context *handle_set_features(int *result);

  void send_update_flags();
  Context *handle_update_flags(int *result);

  void send_notify_update();
  Context *handle_notify_update(int *result);

  void send_release_exclusive_lock();
  Context *handle_release_exclusive_lock(int *result);

  Context *handle_finish(int r);
};

} // namespace operation
} // namespace librbd

extern template class librbd::operation::DisableFeaturesRequest<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_OPERATION_DISABLE_FEATURES_REQUEST_H
