// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_IO_OBJECT_DISPATCHER_H
#define CEPH_LIBRBD_IO_OBJECT_DISPATCHER_H

#include "include/int_types.h"
#include "common/RWLock.h"
#include "librbd/io/Types.h"
#include <map>

struct AsyncOpTracker;
struct Context;

namespace librbd {

struct ImageCtx;

namespace io {

struct ObjectDispatchInterface;
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
  ObjectDispatcher(ImageCtxT* image_ctx);
  ~ObjectDispatcher();

  void shut_down(Context* on_finish);

  void register_object_dispatch(ObjectDispatchInterface* object_dispatch);
  void shut_down_object_dispatch(ObjectDispatchLayer object_dispatch_layer,
                                 Context* on_finish);

  void invalidate_cache(Context* on_finish);
  void reset_existence_cache(Context* on_finish);

  void extent_overwritten(
      uint64_t object_no, uint64_t object_off, uint64_t object_len,
      uint64_t journal_tid, uint64_t new_journal_tid);

private:
  struct ObjectDispatchMeta {
    ObjectDispatchInterface* object_dispatch = nullptr;
    AsyncOpTracker* async_op_tracker = nullptr;

    ObjectDispatchMeta() {
    }
    ObjectDispatchMeta(ObjectDispatchInterface* object_dispatch,
                       AsyncOpTracker* async_op_tracker)
      : object_dispatch(object_dispatch), async_op_tracker(async_op_tracker) {
    }
  };

  struct C_LayerIterator;
  struct C_InvalidateCache;
  struct C_ResetExistenceCache;
  struct SendVisitor;

  ImageCtxT* m_image_ctx;

  RWLock m_lock;
  std::map<ObjectDispatchLayer, ObjectDispatchMeta> m_object_dispatches;

  void send(ObjectDispatchSpec* object_dispatch_spec);

  void shut_down_object_dispatch(ObjectDispatchMeta& object_dispatch_meta,
                                 Context** on_finish);

};

} // namespace io
} // namespace librbd

extern template class librbd::io::ObjectDispatcher<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_IO_OBJECT_DISPATCHER_H
