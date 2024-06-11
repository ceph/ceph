// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_IO_DISPATCHER_INTERFACE_H
#define CEPH_LIBRBD_IO_DISPATCHER_INTERFACE_H

#include "include/int_types.h"

struct Context;

namespace librbd {
namespace io {

template <typename DispatchT>
struct DispatcherInterface {
public:
  typedef DispatchT Dispatch;
  typedef typename DispatchT::DispatchLayer DispatchLayer;
  typedef typename DispatchT::DispatchSpec DispatchSpec;

  virtual ~DispatcherInterface() {
  }

  virtual void shut_down(Context* on_finish) = 0;

  virtual void register_dispatch(Dispatch* dispatch) = 0;
  virtual bool exists(DispatchLayer dispatch_layer) = 0;
  virtual void shut_down_dispatch(DispatchLayer dispatch_layer,
                                  Context* on_finish) = 0;

  virtual void send(DispatchSpec* dispatch_spec) = 0;
  virtual void finished(DispatchSpec* dispatch_spec) = 0;
};

} // namespace io
} // namespace librbd

#endif // CEPH_LIBRBD_IO_DISPATCHER_INTERFACE_H
