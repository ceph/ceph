// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_IO_OBJECT_DISPATCHER_INTERFACE_H
#define CEPH_LIBRBD_IO_OBJECT_DISPATCHER_INTERFACE_H

#include "include/int_types.h"
#include "librbd/io/DispatcherInterface.h"
#include "librbd/io/ObjectDispatchInterface.h"

struct Context;

namespace librbd {
namespace io {

struct ObjectDispatcherInterface
  : public DispatcherInterface<ObjectDispatchInterface> {
public:
  virtual void invalidate_cache(Context* on_finish) = 0;
  virtual void reset_existence_cache(Context* on_finish) = 0;

  virtual void extent_overwritten(
      uint64_t object_no, uint64_t object_off, uint64_t object_len,
      uint64_t journal_tid, uint64_t new_journal_tid) = 0;
};

} // namespace io
} // namespace librbd

#endif // CEPH_LIBRBD_IO_OBJECT_DISPATCHER_INTERFACE_H
