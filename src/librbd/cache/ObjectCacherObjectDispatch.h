// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_CACHE_OBJECT_CACHER_OBJECT_DISPATCH_H
#define CEPH_LIBRBD_CACHE_OBJECT_CACHER_OBJECT_DISPATCH_H

#include "librbd/io/ObjectDispatchInterface.h"
#include "common/ceph_mutex.h"
#include "osdc/ObjectCacher.h"

struct WritebackHandler;

namespace librbd {

class ImageCtx;

namespace cache {

/**
 * Facade around the OSDC object cacher to make it align with
 * the object dispatcher interface
 */
template <typename ImageCtxT = ImageCtx>
class ObjectCacherObjectDispatch : public io::ObjectDispatchInterface {
public:
  static ObjectCacherObjectDispatch* create(ImageCtxT* image_ctx,
                                            size_t max_dirty,
                                            bool writethrough_until_flush) {
    return new ObjectCacherObjectDispatch(image_ctx, max_dirty,
                                          writethrough_until_flush);
  }

  ObjectCacherObjectDispatch(ImageCtxT* image_ctx, size_t max_dirty,
                             bool writethrough_until_flush);
  ~ObjectCacherObjectDispatch() override;

  io::ObjectDispatchLayer get_dispatch_layer() const override {
    return io::OBJECT_DISPATCH_LAYER_CACHE;
  }

  void init();
  void shut_down(Context* on_finish) override;

  bool read(
      uint64_t object_no, io::ReadExtents* extents, IOContext io_context,
      int op_flags, int read_flags, const ZTracer::Trace &parent_trace,
      uint64_t* version, int* object_dispatch_flags,
      io::DispatchResult* dispatch_result, Context** on_finish,
      Context* on_dispatched) override;

  bool discard(
      uint64_t object_no, uint64_t object_off, uint64_t object_len,
      IOContext io_context, int discard_flags,
      const ZTracer::Trace &parent_trace, int* object_dispatch_flags,
      uint64_t* journal_tid, io::DispatchResult* dispatch_result,
      Context** on_finish, Context* on_dispatched) override;

  bool write(
      uint64_t object_no, uint64_t object_off, ceph::bufferlist&& data,
      IOContext io_context, int op_flags, int write_flags,
      std::optional<uint64_t> assert_version,
      const ZTracer::Trace &parent_trace, int* object_dispatch_flags,
      uint64_t* journal_tid, io::DispatchResult* dispatch_result,
      Context** on_finish, Context* on_dispatched) override;

  bool write_same(
      uint64_t object_no, uint64_t object_off, uint64_t object_len,
      io::LightweightBufferExtents&& buffer_extents, ceph::bufferlist&& data,
      IOContext io_context, int op_flags,
      const ZTracer::Trace &parent_trace, int* object_dispatch_flags,
      uint64_t* journal_tid, io::DispatchResult* dispatch_result,
      Context** on_finish, Context* on_dispatched) override;

  bool compare_and_write(
      uint64_t object_no, uint64_t object_off, ceph::bufferlist&& cmp_data,
      ceph::bufferlist&& write_data, IOContext io_context, int op_flags,
      const ZTracer::Trace &parent_trace, uint64_t* mismatch_offset,
      int* object_dispatch_flags, uint64_t* journal_tid,
      io::DispatchResult* dispatch_result, Context** on_finish,
      Context* on_dispatched) override;

  bool flush(
      io::FlushSource flush_source, const ZTracer::Trace &parent_trace,
      uint64_t* journal_tid, io::DispatchResult* dispatch_result,
      Context** on_finish, Context* on_dispatched) override;

  bool list_snaps(
      uint64_t object_no, io::Extents&& extents, io::SnapIds&& snap_ids,
      int list_snap_flags, const ZTracer::Trace &parent_trace,
      io::SnapshotDelta* snapshot_delta, int* object_dispatch_flags,
      io::DispatchResult* dispatch_result, Context** on_finish,
      Context* on_dispatched) override {
    return false;
  }

  bool invalidate_cache(Context* on_finish) override;
  bool reset_existence_cache(Context* on_finish) override;

  void extent_overwritten(
      uint64_t object_no, uint64_t object_off, uint64_t object_len,
      uint64_t journal_tid, uint64_t new_journal_tid) {
  }

  int prepare_copyup(
      uint64_t object_no,
      io::SnapshotSparseBufferlist* snapshot_sparse_bufferlist) override {
    return 0;
  }

private:
  struct C_InvalidateCache;

  ImageCtxT* m_image_ctx;
  size_t m_max_dirty;
  bool m_writethrough_until_flush;

  ceph::mutex m_cache_lock;
  ObjectCacher *m_object_cacher = nullptr;
  ObjectCacher::ObjectSet *m_object_set = nullptr;

  WritebackHandler *m_writeback_handler = nullptr;

  bool m_user_flushed = false;

};

} // namespace cache
} // namespace librbd

extern template class librbd::cache::ObjectCacherObjectDispatch<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_CACHE_OBJECT_CACHER_OBJECT_DISPATCH_H
