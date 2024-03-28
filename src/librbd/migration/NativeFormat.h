// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_MIGRATION_NATIVE_FORMAT_H
#define CEPH_LIBRBD_MIGRATION_NATIVE_FORMAT_H

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

template <typename ImageCtxT>
class NativeFormat : public FormatInterface<ImageCtxT> {
public:
  static std::string build_source_spec(int64_t pool_id,
                                       const std::string& pool_namespace,
                                       const std::string& image_name,
                                       const std::string& image_id);

  static NativeFormat* create(const json_spirit::mObject& json_object,
                              bool import_only) {
    return new NativeFormat(json_object, import_only);
  }

  NativeFormat(const json_spirit::mObject& json_object, bool import_only);
  NativeFormat(const NativeFormat&) = delete;
  NativeFormat& operator=(const NativeFormat&) = delete;

  void open(librados::IoCtx& dst_io_ctx, ImageCtxT* dst_image_ctx,
            ImageCtxT** src_image_ctx, Context* on_finish) override;
  void close(Context* on_finish) override;

  void get_snapshots(SnapInfos* snap_infos, Context* on_finish) override;
  void get_image_size(uint64_t snap_id, uint64_t* size,
                      Context* on_finish) override;

  bool read(io::AioCompletion* aio_comp, uint64_t snap_id,
            io::Extents&& image_extents, io::ReadResult&& read_result,
            int op_flags, int read_flags,
            const ZTracer::Trace &parent_trace) override {
    return false;
  }

  void list_snaps(io::Extents&& image_extents, io::SnapIds&& snap_ids,
                  int list_snaps_flags, io::SnapshotDelta* snapshot_delta,
                  const ZTracer::Trace &parent_trace,
                  Context* on_finish) override;

private:
  ImageCtxT* m_image_ctx;
  json_spirit::mObject m_json_object;
  bool m_import_only;

  int64_t m_pool_id = -1;
  std::string m_pool_namespace;
  std::string m_image_name;
  std::string m_image_id;
  std::string m_snap_name;
  uint64_t m_snap_id = CEPH_NOSNAP;

  void handle_open(int r, Context* on_finish);
  void handle_snap_set(int r, Context* on_finish);

};

} // namespace migration
} // namespace librbd

extern template class librbd::migration::NativeFormat<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_MIGRATION_NATIVE_FORMAT_H
