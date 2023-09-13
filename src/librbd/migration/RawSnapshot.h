// -*- mode:c++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_MIGRATION_RAW_SNAPSHOT_H
#define CEPH_LIBRBD_MIGRATION_RAW_SNAPSHOT_H

#include "include/buffer_fwd.h"
#include "include/int_types.h"
#include "common/zipkin_trace.h"
#include "librbd/Types.h"
#include "librbd/io/Types.h"
#include "librbd/migration/SnapshotInterface.h"
#include "json_spirit/json_spirit.h"
#include <memory>

namespace librbd {

struct ImageCtx;

namespace migration {

template <typename> struct SourceSpecBuilder;
struct StreamInterface;

template <typename ImageCtxT>
class RawSnapshot : public SnapshotInterface {
public:
  static RawSnapshot* create(
      ImageCtx* image_ctx, const json_spirit::mObject& json_object,
      const SourceSpecBuilder<ImageCtxT>* source_spec_builder, uint64_t index) {
    return new RawSnapshot(image_ctx, json_object, source_spec_builder, index);
  }

  RawSnapshot(ImageCtxT* image_ctx, const json_spirit::mObject& json_object,
              const SourceSpecBuilder<ImageCtxT>* source_spec_builder,
              uint64_t index);
  RawSnapshot(const RawSnapshot&) = delete;
  RawSnapshot& operator=(const RawSnapshot&) = delete;

  void open(SnapshotInterface* previous_snapshot, Context* on_finish) override;
  void close(Context* on_finish) override;

  const SnapInfo& get_snap_info() const override {
    return m_snap_info;
  }

  void read(io::AioCompletion* aio_comp, io::Extents&& image_extents,
            io::ReadResult&& read_result, int op_flags, int read_flags,
            const ZTracer::Trace &parent_trace) override;

  void list_snap(io::Extents&& image_extents, int list_snaps_flags,
                 io::SparseExtents* sparse_extents,
                 const ZTracer::Trace &parent_trace,
                 Context* on_finish) override;

private:
  struct OpenRequest;

  ImageCtxT* m_image_ctx;
  json_spirit::mObject m_json_object;
  const SourceSpecBuilder<ImageCtxT>* m_source_spec_builder;
  uint64_t m_index = 0;

  SnapInfo m_snap_info;

  std::shared_ptr<StreamInterface> m_stream;

};

} // namespace migration
} // namespace librbd

extern template class librbd::migration::RawSnapshot<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_MIGRATION_RAW_SNAPSHOT_H
