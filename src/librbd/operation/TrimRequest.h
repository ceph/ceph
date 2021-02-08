// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#ifndef CEPH_LIBRBD_OPERATION_TRIM_REQUEST_H
#define CEPH_LIBRBD_OPERATION_TRIM_REQUEST_H

#include "librbd/AsyncRequest.h"

namespace librbd
{

class ImageCtx;
class ProgressContext;

namespace operation {

template <typename ImageCtxT = ImageCtx>
class TrimRequest : public AsyncRequest<ImageCtxT>
{
public:
  static TrimRequest *create(ImageCtxT &image_ctx, Context *on_finish,
                             uint64_t original_size, uint64_t new_size,
                             ProgressContext &prog_ctx) {
    return new TrimRequest(image_ctx, on_finish, original_size, new_size,
                           prog_ctx);
  }

  TrimRequest(ImageCtxT &image_ctx, Context *on_finish,
	      uint64_t original_size, uint64_t new_size,
	      ProgressContext &prog_ctx);

  void send() override;

protected:
  /**
   * Trim goes through the following state machine to remove whole objects,
   * clean partially trimmed objects, and update the object map:
   *
   * @verbatim
   *
   *     <start>  . . . . . . . . . . . . . . . . .
   *        |                                     .
   *        v (skip if not needed)                .
   * STATE_PRE_TRIM                               .
   *        |                                     .
   *        v (skip if not needed)                .
   * STATE_COPYUP_OBJECTS                         .
   *        |                                     .
   *        v (skip if not needed)                .
   * STATE_REMOVE_OBJECTS                         .
   *        |                                     .
   *        v (skip if not needed)                .
   * STATE_POST_TRIM                              .
   *        |                                     .
   *        v (skip if not needed)                .
   * STATE_CLEAN_BOUNDARY                         .
   *        |                                     .
   *        v                                     .
   * STATE_FINISHED < . . . . . . . . . . . . . . .
   *        |
   *        v
   *    <finish>
   *
   * The _COPYUP_OBJECTS state is skipped if there is no parent overlap
   * within the new image size and the image does not have any snapshots.
   * The _PRE_TRIM/_POST_TRIM states are skipped if the object map
   * isn't enabled. The _REMOVE_OBJECTS state is skipped if no whole objects
   * are removed.  The _CLEAN_BOUNDARY state is skipped if no boundary
   * objects are cleaned.  The state machine will immediately transition
   * to _FINISHED state if there are no bytes to trim.
   */

  enum State {
    STATE_PRE_TRIM,
    STATE_COPYUP_OBJECTS,
    STATE_REMOVE_OBJECTS,
    STATE_POST_TRIM,
    STATE_CLEAN_BOUNDARY,
    STATE_FINISHED
  };

  bool should_complete(int r) override;

  State m_state = STATE_PRE_TRIM;

private:
  uint64_t m_delete_start;
  uint64_t m_delete_start_min = 0;
  uint64_t m_num_objects;
  uint64_t m_delete_off;
  uint64_t m_new_size;
  ProgressContext &m_prog_ctx;

  void send_pre_trim();
  void send_copyup_objects();
  void send_remove_objects();
  void send_post_trim();

  void send_clean_boundary();
  void send_finish(int r);
};

} // namespace operation
} // namespace librbd

extern template class librbd::operation::TrimRequest<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_OPERATION_TRIM_REQUEST_H
