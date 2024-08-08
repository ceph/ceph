// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_TEST_LIBRBD_MOCK_MIGRATION_MOCK_SNAPSHOT_INTERFACE_H
#define CEPH_TEST_LIBRBD_MOCK_MIGRATION_MOCK_SNAPSHOT_INTERFACE_H

#include "include/buffer.h"
#include "gmock/gmock.h"
#include "librbd/io/AioCompletion.h"
#include "librbd/io/ReadResult.h"
#include "librbd/io/Types.h"
#include "librbd/migration/SnapshotInterface.h"

namespace librbd {
namespace migration {

struct MockSnapshotInterface : public SnapshotInterface {
  MOCK_METHOD2(open, void(SnapshotInterface*, Context*));
  MOCK_METHOD1(close, void(Context*));

  MOCK_CONST_METHOD0(get_snap_info, const SnapInfo&());

  MOCK_METHOD3(read, void(io::AioCompletion*, const io::Extents&,
                          io::ReadResult&));
  void read(io::AioCompletion* aio_comp, io::Extents&& image_extents,
            io::ReadResult&& read_result, int op_flags, int read_flags,
            const ZTracer::Trace &parent_trace) override {
    read(aio_comp, image_extents, read_result);
  }

  MOCK_METHOD3(list_snap, void(const io::Extents&, io::SparseExtents*,
                               Context*));
  void list_snap(io::Extents&& image_extents, int list_snaps_flags,
                 io::SparseExtents* sparse_extents,
                 const ZTracer::Trace &parent_trace,
                 Context* on_finish) override {
    list_snap(image_extents, sparse_extents, on_finish);
  }
};

} // namespace migration
} // namespace librbd

#endif // CEPH_TEST_LIBRBD_MOCK_MIGRATION_MOCK_SNAPSHOT_INTERFACE_H
