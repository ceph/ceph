// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#ifndef CEPH_LIBRBD_ASYNC_OBJECT_THROTTLE_H
#define CEPH_LIBRBD_ASYNC_OBJECT_THROTTLE_H

#include "include/int_types.h"
#include "include/Context.h"

#include <boost/function.hpp>

namespace librbd
{
template <typename ImageCtxT> class AsyncRequest;
class ProgressContext;
struct ImageCtx;

class AsyncObjectThrottleFinisher {
public:
  virtual ~AsyncObjectThrottleFinisher() {};
  virtual void finish_op(int r) = 0;
};

template <typename ImageCtxT = ImageCtx>
class C_AsyncObjectThrottle : public Context {
public:
  C_AsyncObjectThrottle(AsyncObjectThrottleFinisher &finisher,
                        ImageCtxT &image_ctx)
    : m_image_ctx(image_ctx), m_finisher(finisher) {
  }

  virtual int send() = 0;

protected:
  ImageCtxT &m_image_ctx;

  virtual void finish(int r) {
    m_finisher.finish_op(r);
  }

private:
  AsyncObjectThrottleFinisher &m_finisher;
};

template <typename ImageCtxT = ImageCtx>
class AsyncObjectThrottle : public AsyncObjectThrottleFinisher {
public:
  typedef boost::function<
    C_AsyncObjectThrottle<ImageCtxT>* (AsyncObjectThrottle&,
                                       uint64_t)> ContextFactory;

  AsyncObjectThrottle(const AsyncRequest<ImageCtxT> *async_request,
                      ImageCtxT &image_ctx,
                      const ContextFactory& context_factory, Context *ctx,
		      ProgressContext *prog_ctx, uint64_t object_no,
		      uint64_t end_object_no);

  void start_ops(uint64_t max_concurrent);
  virtual void finish_op(int r);

private:
  Mutex m_lock;
  const AsyncRequest<ImageCtxT> *m_async_request;
  ImageCtxT &m_image_ctx;
  ContextFactory m_context_factory;
  Context *m_ctx;
  ProgressContext *m_prog_ctx;
  uint64_t m_object_no;
  uint64_t m_end_object_no;
  uint64_t m_current_ops;
  int m_ret;

  void start_next_op();
};

} // namespace librbd

extern template class librbd::AsyncObjectThrottle<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_ASYNC_OBJECT_THROTTLE_H
