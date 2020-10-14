// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_MIGRATION_NATIVE_FORMAT_H
#define CEPH_LIBRBD_MIGRATION_NATIVE_FORMAT_H

#include "include/int_types.h"
#include "librbd/Types.h"
#include "librbd/migration/FormatInterface.h"
#include <memory>

struct Context;

namespace librbd {

struct AsioEngine;
struct ImageCtx;

namespace migration {

template <typename ImageCtxT>
class NativeFormat : public FormatInterface {
public:
  static NativeFormat* create(ImageCtxT* image_ctx,
                              const MigrationInfo& migration_info) {
    return new NativeFormat(image_ctx, migration_info);
  }

  NativeFormat(ImageCtxT* image_ctx, const MigrationInfo& migration_info);
  NativeFormat(const NativeFormat&) = delete;
  NativeFormat& operator=(const NativeFormat&) = delete;

  void open(Context* on_finish) override;
  void close(Context* on_finish) override;

  void get_snapshots(SnapInfos* snap_infos, Context* on_finish) override;
  void get_image_size(uint64_t snap_id, uint64_t* size,
                      Context* on_finish) override;

  void read(io::AioCompletion* aio_comp, uint64_t snap_id,
            io::Extents&& image_extents, io::ReadResult&& read_result,
            int op_flags, int read_flags,
            const ZTracer::Trace &parent_trace) override;

  void list_snaps(io::Extents&& image_extents, io::SnapIds&& snap_ids,
                  int list_snaps_flags, io::SnapshotDelta* snapshot_delta,
                  const ZTracer::Trace &parent_trace,
                  Context* on_finish) override;

private:
  ImageCtxT* m_image_ctx;
  MigrationInfo m_migration_info;

};

} // namespace migration
} // namespace librbd

extern template class librbd::migration::NativeFormat<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_MIGRATION_NATIVE_FORMAT_H
