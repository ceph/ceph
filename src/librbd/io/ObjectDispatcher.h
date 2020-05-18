// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_IO_OBJECT_DISPATCHER_H
#define CEPH_LIBRBD_IO_OBJECT_DISPATCHER_H

#include "include/int_types.h"
#include "common/ceph_mutex.h"
#include "librbd/io/Dispatcher.h"
#include "librbd/io/ObjectDispatchInterface.h"
#include "librbd/io/ObjectDispatchSpec.h"
#include "librbd/io/ObjectDispatcherInterface.h"
#include "librbd/io/Types.h"
#include <map>

struct Context;

namespace librbd {

struct ImageCtx;

namespace io {

template <typename ImageCtxT = ImageCtx>
class ObjectDispatcher
  : public Dispatcher<ImageCtxT, ObjectDispatcherInterface> {
public:
  ObjectDispatcher(ImageCtxT* image_ctx);

  void invalidate_cache(Context* on_finish) override;
  void reset_existence_cache(Context* on_finish) override;

  void extent_overwritten(
      uint64_t object_no, uint64_t object_off, uint64_t object_len,
      uint64_t journal_tid, uint64_t new_journal_tid) override;

protected:
  bool send_dispatch(ObjectDispatchInterface* object_dispatch,
                     ObjectDispatchSpec* object_dispatch_spec) override;

private:
  struct C_LayerIterator;
  struct C_InvalidateCache;
  struct C_ResetExistenceCache;
  struct SendVisitor;

};

} // namespace io
} // namespace librbd

extern template class librbd::io::ObjectDispatcher<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_IO_OBJECT_DISPATCHER_H
