// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#ifndef CEPH_LIBRBD_OPERATION_RESIZE_REQUEST_H
#define CEPH_LIBRBD_OPERATION_RESIZE_REQUEST_H

#include "librbd/operation/Request.h"
#include "include/xlist.h"

namespace librbd
{

class ImageCtx;
class ProgressContext;

namespace operation {

template <typename ImageCtxT = ImageCtx>
class ResizeRequest : public Request<ImageCtxT> {
public:
  static ResizeRequest *create(ImageCtxT &image_ctx, Context *on_finish,
                               uint64_t new_size, ProgressContext &prog_ctx,
                               uint64_t journal_op_tid, bool disable_journal) {
    return new ResizeRequest(image_ctx, on_finish, new_size, prog_ctx,
                             journal_op_tid, disable_journal);
  }

  ResizeRequest(ImageCtxT &image_ctx, Context *on_finish, uint64_t new_size,
                ProgressContext &prog_ctx, uint64_t journal_op_tid,
                bool disable_journal);
  virtual ~ResizeRequest();

  inline bool shrinking() const {
    return (m_shrink_size_visible && m_new_size < m_original_size);
  }

  inline uint64_t get_image_size() const {
    return m_new_size;
  }

  virtual void send();

protected:
  virtual void send_op();
  virtual bool should_complete(int r) {
    return true;
  }
  virtual bool can_affect_io() const override {
    return true;
  }
  virtual journal::Event create_event(uint64_t op_tid) const {
    return journal::ResizeEvent(op_tid, m_new_size);
  }

private:
  /**
   * Resize goes through the following state machine to resize the image
   * and update the object map:
   *
   * @verbatim
   *
   * <start>
   *    |
   *    v
   * STATE_PRE_BLOCK_WRITES
   *    |
   *    v
   * STATE_APPEND_OP_EVENT (skip if journaling
   *    |                   disabled)
   *    | (unblock writes)
   *    |
   *    |
   *    | (grow)
   *    |\--------> STATE_GROW_OBJECT_MAP (skip if object map
   *    |                 |                disabled)
   *    |                 v
   *    |           STATE_POST_BLOCK_WRITES
   *    |                 |
   *    |                 v
   *    |           STATE_UPDATE_HEADER ----------------------------\
   *    |                                                           |
   *    | (shrink)                                                  |
   *    |\--------> STATE_FLUSH_CACHE                               |
   *    |                 |                                         |
   *    |                 v                                         |
   *    |           STATE_INVALIDATE_CACHE                          |
   *    |                 |                                         |
   *    |                 v                                         |
   *    |           STATE_TRIM_IMAGE                                |
   *    |                 |                                         |
   *    |                 v                                         |
   *    |           STATE_POST_BLOCK_WRITES                         |
   *    |                 |                                         |
   *    |                 v                                         |
   *    |           STATE_UPDATE_HEADER                             |
   *    |                 |                                         |
   *    |                 v                                         |
   *    |           STATE_SHRINK_OBJECT_MAP (skip if object map     |
   *    |                 |                  disabled)              |
   *    |                 | (unblock writes)                        |
   *    | (no change)     v                                         |
   *    \------------> <finish> <-----------------------------------/
   *
   * @endverbatim
   *
   * The _OBJECT_MAP states are skipped if the object map isn't enabled.
   * The state machine will immediately transition to _FINISHED if there
   * are no objects to trim.
   */

  uint64_t m_original_size;
  uint64_t m_new_size;
  ProgressContext &m_prog_ctx;
  uint64_t m_new_parent_overlap;
  bool m_shrink_size_visible = false;
  bool m_disable_journal = false;

  typename xlist<ResizeRequest<ImageCtxT>*>::item m_xlist_item;

  void send_pre_block_writes();
  Context *handle_pre_block_writes(int *result);

  Context *send_append_op_event();
  Context *handle_append_op_event(int *result);

  void send_flush_cache();
  Context *handle_flush_cache(int *result);

  void send_invalidate_cache();
  Context *handle_invalidate_cache(int *result);

  void send_trim_image();
  Context *handle_trim_image(int *result);

  Context *send_grow_object_map();
  Context *handle_grow_object_map(int *result);

  Context *send_shrink_object_map();
  Context *handle_shrink_object_map(int *result);

  void send_post_block_writes();
  Context *handle_post_block_writes(int *result);

  void send_update_header();
  Context *handle_update_header(int *result);

  void compute_parent_overlap();
  void update_size_and_overlap();

};

} // namespace operation
} // namespace librbd

extern template class librbd::operation::ResizeRequest<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_OPERATION_RESIZE_REQUEST_H
