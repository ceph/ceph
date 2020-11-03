// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_MIGRATION_RAW_FORMAT_H
#define CEPH_LIBRBD_MIGRATION_RAW_FORMAT_H

#include "include/int_types.h"
#include "librbd/Types.h"
#include "librbd/migration/FormatInterface.h"
#include "json_spirit/json_spirit.h"
#include <memory>

struct Context;

namespace librbd {

struct AsioEngine;
struct ImageCtx;

namespace migration {

template <typename> struct SourceSpecBuilder;
struct StreamInterface;

template <typename ImageCtxT>
class RawFormat : public FormatInterface {
public:
  static RawFormat* create(
      ImageCtxT* image_ctx, const json_spirit::mObject& json_object,
      const SourceSpecBuilder<ImageCtxT>* source_spec_builder) {
    return new RawFormat(image_ctx, json_object, source_spec_builder);
  }

  RawFormat(ImageCtxT* image_ctx, const json_spirit::mObject& json_object,
            const SourceSpecBuilder<ImageCtxT>* source_spec_builder);
  RawFormat(const RawFormat&) = delete;
  RawFormat& operator=(const RawFormat&) = delete;

  void open(Context* on_finish) override;
  void close(Context* on_finish) override;

  void get_snapshots(SnapInfos* snap_infos, Context* on_finish) override;
  void get_image_size(uint64_t snap_id, uint64_t* size,
                      Context* on_finish) override;

  bool read(io::AioCompletion* aio_comp, uint64_t snap_id,
            io::Extents&& image_extents, io::ReadResult&& read_result,
            int op_flags, int read_flags,
            const ZTracer::Trace &parent_trace) override;

  void list_snaps(io::Extents&& image_extents, io::SnapIds&& snap_ids,
                  int list_snaps_flags, io::SnapshotDelta* snapshot_delta,
                  const ZTracer::Trace &parent_trace,
                  Context* on_finish) override;

private:
  struct OpenRequest;

  ImageCtxT* m_image_ctx;
  json_spirit::mObject m_json_object;
  const SourceSpecBuilder<ImageCtxT>* m_source_spec_builder;

  std::unique_ptr<StreamInterface> m_stream;

};

} // namespace migration
} // namespace librbd

extern template class librbd::migration::RawFormat<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_MIGRATION_RAW_FORMAT_H
