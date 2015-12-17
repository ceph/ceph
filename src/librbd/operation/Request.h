// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_OPERATION_REQUEST_H
#define CEPH_LIBRBD_OPERATION_REQUEST_H

#include "librbd/AsyncRequest.h"
#include "include/Context.h"
#include "common/RWLock.h"
#include "librbd/Utils.h"
#include "librbd/Journal.h"
#include "librbd/journal/Entries.h"

namespace librbd {

class ImageCtx;

namespace operation {

template <typename ImageCtxT = ImageCtx>
class Request : public AsyncRequest<ImageCtxT> {
public:
  Request(ImageCtxT &image_ctx, Context *on_finish);

  virtual void send();

protected:
  virtual void finish(int r) override;
  virtual void send_op() = 0;

  virtual bool can_affect_io() const {
    return false;
  }
  virtual journal::Event create_event(uint64_t op_tid) const = 0;

  template <typename T, Context*(T::*MF)(int*)>
  bool append_op_event(T *request) {
    ImageCtxT &image_ctx = this->m_image_ctx;

    RWLock::RLocker owner_locker(image_ctx.owner_lock);
    RWLock::RLocker snap_locker(image_ctx.snap_lock);
    if (image_ctx.journal != NULL &&
        !image_ctx.journal->is_journal_replaying()) {
      append_op_event(util::create_context_callback<T, MF>(request));
      return true;
    }
    return false;
  }

  bool append_op_event();
  void commit_op_event(int r);

  // NOTE: temporary until converted to new state machine format
  Context *create_context_finisher() {
    return util::create_context_callback<
      Request<ImageCtxT>, &Request<ImageCtxT>::finish>(this);
  }

private:
  struct C_OpEventSafe : public Context {
    Request *request;
    Context *on_safe;
    C_OpEventSafe(Request *request, Context *on_safe)
      : request(request), on_safe(on_safe) {
    }
    virtual void finish(int r) override {
      if (r >= 0) {
        request->m_appended_op_event = true;
      }
      on_safe->complete(r);
    }
  };

  uint64_t m_op_tid = 0;
  bool m_appended_op_event = false;
  bool m_committed_op_event = false;

  void append_op_event(Context *on_safe);
  void handle_op_event_safe(int r);

};

} // namespace operation
} // namespace librbd

extern template class librbd::operation::Request<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_OPERATION_REQUEST_H
