// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_EXCLUSIVE_LOCK_H
#define CEPH_LIBRBD_EXCLUSIVE_LOCK_H

#include "common/AsyncOpTracker.h"
#include "librbd/ManagedLock.h"
#include "librbd/exclusive_lock/Policy.h"
#include "librbd/io/Types.h"
#include "common/RefCountedObj.h"

struct Context;

namespace librbd {

namespace exclusive_lock { template <typename> struct ImageDispatch; }

template <typename ImageCtxT = ImageCtx>
class ExclusiveLock : public RefCountedObject,
                      public ManagedLock<ImageCtxT> {
public:
  static ExclusiveLock *create(ImageCtxT &image_ctx) {
    return new ExclusiveLock<ImageCtxT>(image_ctx);
  }

  ExclusiveLock(ImageCtxT &image_ctx);

  bool accept_request(exclusive_lock::OperationRequestType request_type,
                      int *ret_val) const;
  bool accept_ops() const;

  void set_require_lock(io::Direction direction, Context* on_finish);
  void unset_require_lock(io::Direction direction);

  void block_requests(int r);
  void unblock_requests();

  void init(uint64_t features, Context *on_init);
  void shut_down(Context *on_shutdown);

  void handle_peer_notification(int r);

  int get_unlocked_op_error() const;
  Context *start_op(int* ret_val);

protected:
  void shutdown_handler(int r, Context *on_finish) override;
  void pre_acquire_lock_handler(Context *on_finish) override;
  void post_acquire_lock_handler(int r, Context *on_finish) override;
  void pre_release_lock_handler(bool shutting_down,
                                Context *on_finish) override;
  void post_release_lock_handler(bool shutting_down, int r,
                                 Context *on_finish) override;
  void post_reacquire_lock_handler(int r, Context *on_finish) override;

private:

  /**
   * @verbatim
   *
   * <start>                              * * > WAITING_FOR_REGISTER --------\
   *    |                                 * (watch not registered)           |
   *    |                                 *                                  |
   *    |                                 * * > WAITING_FOR_PEER ------------\
   *    |                                 * (request_lock busy)              |
   *    |                                 *                                  |
   *    |                                 * * * * * * * * * * * * * *        |
   *    |                                                           *        |
   *    v            (init)            (try_lock/request_lock)      *        |
   * UNINITIALIZED  -------> UNLOCKED ------------------------> ACQUIRING <--/
   *                            ^                                   |
   *                            |                                   v
   *                         RELEASING                        POST_ACQUIRING
   *                            |                                   |
   *                            |                                   |
   *                            |          (release_lock)           v
   *                      PRE_RELEASING <------------------------ LOCKED
   *
   * <LOCKED state>
   *    |
   *    v
   * REACQUIRING -------------------------------------> <finish>
   *    .                                                 ^
   *    .                                                 |
   *    . . . > <RELEASE action> ---> <ACQUIRE action> ---/
   *
   * <UNLOCKED/LOCKED states>
   *    |
   *    |
   *    v
   * PRE_SHUTTING_DOWN ---> SHUTTING_DOWN ---> SHUTDOWN ---> <finish>
   *
   * @endverbatim
   */

  ImageCtxT& m_image_ctx;
  exclusive_lock::ImageDispatch<ImageCtxT>* m_image_dispatch = nullptr;
  Context *m_pre_post_callback = nullptr;

  AsyncOpTracker m_async_op_tracker;

  uint32_t m_request_blocked_count = 0;
  int m_request_blocked_ret_val = 0;

  int m_acquire_lock_peer_ret_val = 0;

  bool accept_ops(const ceph::mutex &lock) const;

  void handle_init_complete(int r, uint64_t features, Context* on_finish);
  void handle_post_acquiring_lock(int r);
  void handle_post_acquired_lock(int r);
};

} // namespace librbd

#endif // CEPH_LIBRBD_EXCLUSIVE_LOCK_H
