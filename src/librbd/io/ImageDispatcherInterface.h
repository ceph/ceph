// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_IO_IMAGE_DISPATCHER_INTERFACE_H
#define CEPH_LIBRBD_IO_IMAGE_DISPATCHER_INTERFACE_H

#include "include/int_types.h"
#include "librbd/io/DispatcherInterface.h"
#include "librbd/io/ImageDispatchInterface.h"
#include "librbd/io/Types.h"

struct Context;

namespace librbd {
namespace io {

struct ImageDispatcherInterface
  : public DispatcherInterface<ImageDispatchInterface> {
public:
  virtual void apply_qos_schedule_tick_min(uint64_t tick) = 0;
  virtual void apply_qos_limit(uint64_t flag, uint64_t limit,
                               uint64_t burst, uint64_t burst_seconds) = 0;
  virtual void apply_qos_exclude_ops(uint64_t exclude_ops) = 0;

  virtual bool writes_blocked() const = 0;
  virtual int block_writes() = 0;
  virtual void block_writes(Context *on_blocked) = 0;

  virtual void unblock_writes() = 0;
  virtual void wait_on_writes_unblocked(Context *on_unblocked) = 0;

  virtual void invalidate_cache(Context* on_finish) = 0;
};

} // namespace io
} // namespace librbd

#endif // CEPH_LIBRBD_IO_IMAGE_DISPATCHER_INTERFACE_H
