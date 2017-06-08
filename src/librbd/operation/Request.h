// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_OPERATION_REQUEST_H
#define CEPH_LIBRBD_OPERATION_REQUEST_H

#include "librbd/AsyncRequest.h"
#include "include/Context.h"
#include "librbd/JournalTypes.h"

namespace librbd {

class ImageCtx;

namespace operation {

template <typename ImageCtxT = ImageCtx>
class Request : public AsyncRequest<ImageCtxT> {
public:
  Request(ImageCtxT &image_ctx, Context *on_finish);

  virtual void send();

protected:
  virtual void finish(int r);
  virtual void send_op() = 0;

  virtual journal::Event create_event() const = 0;

private:
  struct C_WaitForJournalReady : public Context {
    Request *request;

    C_WaitForJournalReady(Request *_request) : request(_request) {
    }

    virtual void finish(int r) {
      request->handle_journal_ready();
    }
  };

  uint64_t m_tid;

  void handle_journal_ready();
};

} // namespace operation
} // namespace librbd

extern template class librbd::operation::Request<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_OPERATION_REQUEST_H
