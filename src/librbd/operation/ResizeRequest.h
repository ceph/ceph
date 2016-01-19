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
  ResizeRequest(ImageCtxT &image_ctx, Context *on_finish, uint64_t new_size,
                ProgressContext &prog_ctx);
  virtual ~ResizeRequest();

  inline bool shrinking() const {
    return m_new_size < m_original_size;
  }

  inline uint64_t get_image_size() const {
    return m_new_size;
  }

  virtual void send();

protected:
  virtual void send_op();
  virtual bool should_complete(int r);

  virtual journal::Event create_event() const {
    return journal::ResizeEvent(0, m_new_size);
  }

private:
  /**
   * Resize goes through the following state machine to resize the image
   * and update the object map:
   *
   * @verbatim
   *
   * <start> -------------> STATE_FINISHED -----------------------------\
   *  |  .    (no change)                                               |
   *  |  .                                                              |
   *  |  . . . . . . . . . . . . . . . . . . . . .                      |
   *  |                                          .                      |
   *  |                                          v                      |
   *  |----------> STATE_GROW_OBJECT_MAP ---> STATE_UPDATE_HEADER ------|
   *  | (grow)                                                          |
   *  |                                                                 |
   *  |                                                                 |
   *  \----------> STATE_FLUSH -------------> STATE_INVALIDATE_CACHE    |
   *    (shrink)                                 |                      |
   *                                             |                      |
   *                      /----------------------/                      |
   *                      |                                             |
   *                      v                                             |
   *              STATE_TRIM_IMAGE --------> STATE_UPDATE_HEADER . . .  |
   *                                             |                   .  |
   *                                             |                   .  |
   *                                             v                   v  v
   *                                  STATE_SHRINK_OBJECT_MAP ---> <finish>
   *
   * @endverbatim
   *
   * The _OBJECT_MAP states are skipped if the object map isn't enabled.
   * The state machine will immediately transition to _FINISHED if there
   * are no objects to trim.
   */
  enum State {
    STATE_FLUSH,
    STATE_INVALIDATE_CACHE,
    STATE_TRIM_IMAGE,
    STATE_GROW_OBJECT_MAP,
    STATE_UPDATE_HEADER,
    STATE_SHRINK_OBJECT_MAP,
    STATE_FINISHED
  };

  State m_state;
  uint64_t m_original_size;
  uint64_t m_new_size;
  ProgressContext &m_prog_ctx;
  uint64_t m_new_parent_overlap;

  typename xlist<ResizeRequest<ImageCtxT>*>::item m_xlist_item;

  void send_flush();
  void send_invalidate_cache();
  void send_trim_image();
  void send_grow_object_map();
  bool send_shrink_object_map();
  void send_update_header();

  void compute_parent_overlap();
  void update_size_and_overlap();

};

} // namespace operation
} // namespace librbd

extern template class librbd::operation::ResizeRequest<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_OPERATION_RESIZE_REQUEST_H
