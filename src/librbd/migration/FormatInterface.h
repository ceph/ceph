// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_MIGRATION_FORMAT_INTERFACE_H
#define CEPH_LIBRBD_MIGRATION_FORMAT_INTERFACE_H

#include "include/buffer_fwd.h"
#include "include/int_types.h"
#include "common/zipkin_trace.h"
#include "librbd/Types.h"
#include "librbd/io/Types.h"
#include <map>

struct Context;

namespace librbd {

namespace io {
struct AioCompletion;
struct ReadResult;
} // namespace io

namespace migration {

struct FormatInterface {
  typedef std::map<uint64_t, SnapInfo> SnapInfos;

  virtual ~FormatInterface() {
  }

  virtual void open(Context* on_finish) = 0;
  virtual void close(Context* on_finish) = 0;

  virtual void get_snapshots(SnapInfos* snap_infos, Context* on_finish) = 0;
  virtual void get_image_size(uint64_t snap_id, uint64_t* size,
                              Context* on_finish) = 0;

  virtual void read(io::AioCompletion* aio_comp, uint64_t snap_id,
                    io::Extents&& image_extents, io::ReadResult&& read_result,
                    int op_flags, int read_flags,
                    const ZTracer::Trace &parent_trace) = 0;

  virtual void list_snaps(io::Extents&& image_extents, io::SnapIds&& snap_ids,
                          int list_snaps_flags,
                          io::SnapshotDelta* snapshot_delta,
                          const ZTracer::Trace &parent_trace,
                          Context* on_finish) = 0;
};

} // namespace migration
} // namespace librbd

#endif // CEPH_LIBRBD_MIGRATION_FORMAT_INTERFACE_H
