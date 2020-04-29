// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_IO_IMAGE_DISPATCHER_H
#define CEPH_LIBRBD_IO_IMAGE_DISPATCHER_H

#include "include/int_types.h"
#include "common/ceph_mutex.h"
#include "librbd/io/Dispatcher.h"
#include "librbd/io/ImageDispatchInterface.h"
#include "librbd/io/ImageDispatchSpec.h"
#include "librbd/io/ImageDispatcherInterface.h"
#include "librbd/io/Types.h"
#include <map>

struct Context;

namespace librbd {

struct ImageCtx;

namespace io {

template <typename ImageCtxT = ImageCtx>
class ImageDispatcher : public Dispatcher<ImageCtxT, ImageDispatcherInterface> {
public:
  ImageDispatcher(ImageCtxT* image_ctx);

protected:
  bool send_dispatch(
    ImageDispatchInterface* object_dispatch,
    ImageDispatchSpec<ImageCtxT>* object_dispatch_spec) override;

private:
  struct SendVisitor;

};

} // namespace io
} // namespace librbd

extern template class librbd::io::ImageDispatcher<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_IO_IMAGE_DISPATCHER_H
