// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_IO_OBJECT_DISPATCHER_H
#define CEPH_LIBRBD_IO_OBJECT_DISPATCHER_H

#include "include/int_types.h"
#include "librbd/io/Types.h"

namespace librbd {

struct ImageCtx;

namespace io {

struct ObjectDispatchSpec;

struct ObjectDispatcherInterface {
public:
  virtual ~ObjectDispatcherInterface() {
  }

private:
  friend class ObjectDispatchSpec;

  virtual void send(ObjectDispatchSpec* object_dispatch_spec) = 0;
};

template <typename ImageCtxT = ImageCtx>
class ObjectDispatcher : public ObjectDispatcherInterface {
public:
  ObjectDispatcher(ImageCtxT* image_ctx) : m_image_ctx(image_ctx) {
  }

private:
  struct SendVisitor;

  ImageCtxT* m_image_ctx;

  void send(ObjectDispatchSpec* object_dispatch_spec);

};

} // namespace io
} // namespace librbd

extern template class librbd::io::ObjectDispatcher<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_IO_OBJECT_DISPATCHER_H
