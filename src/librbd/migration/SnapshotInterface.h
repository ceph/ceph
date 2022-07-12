// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_MIGRATION_SNAPSHOT_INTERFACE_H
#define CEPH_LIBRBD_MIGRATION_SNAPSHOT_INTERFACE_H

#include "include/buffer_fwd.h"
#include "include/int_types.h"
#include "common/zipkin_trace.h"
#include "librbd/Types.h"
#include "librbd/io/Types.h"
#include <string>

struct Context;

namespace librbd {

namespace io {
struct AioCompletion;
struct ReadResult;
} // namespace io

namespace migration {

struct SnapshotInterface {
  virtual ~SnapshotInterface() {
  }

  virtual void open(SnapshotInterface* previous_snapshot,
                    Context* on_finish) = 0;
  virtual void close(Context* on_finish) = 0;

  virtual const SnapInfo& get_snap_info() const = 0;

  virtual void read(io::AioCompletion* aio_comp, io::Extents&& image_extents,
                    io::ReadResult&& read_result, int op_flags, int read_flags,
                    const ZTracer::Trace &parent_trace) = 0;

  virtual void list_snap(io::Extents&& image_extents, int list_snaps_flags,
                         io::SparseExtents* sparse_extents,
                         const ZTracer::Trace &parent_trace,
                         Context* on_finish) = 0;
};

} // namespace migration
} // namespace librbd

#endif // CEPH_LIBRBD_MIGRATION_SNAPSHOT_INTERFACE_H
