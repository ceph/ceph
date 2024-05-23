// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_CACHE_PARENT_CACHER_OBJECT_DISPATCH_H
#define CEPH_LIBRBD_CACHE_PARENT_CACHER_OBJECT_DISPATCH_H

#include "librbd/io/ObjectDispatchInterface.h"
#include "common/ceph_mutex.h"
#include "librbd/cache/TypeTraits.h"
#include "tools/immutable_object_cache/CacheClient.h"
#include "tools/immutable_object_cache/Types.h"

namespace librbd {

class ImageCtx;

namespace plugin { template <typename> struct Api; }

namespace cache {

template <typename ImageCtxT = ImageCtx>
class ParentCacheObjectDispatch : public io::ObjectDispatchInterface {
  // mock unit testing support
  typedef cache::TypeTraits<ImageCtxT> TypeTraits;
  typedef typename TypeTraits::CacheClient CacheClient;

public:
  static ParentCacheObjectDispatch* create(ImageCtxT* image_ctx,
                                           plugin::Api<ImageCtxT>& plugin_api) {
    return new ParentCacheObjectDispatch(image_ctx, plugin_api);
  }

  ParentCacheObjectDispatch(ImageCtxT* image_ctx,
                            plugin::Api<ImageCtxT>& plugin_api);
  ~ParentCacheObjectDispatch() override;

  io::ObjectDispatchLayer get_dispatch_layer() const override {
    return io::OBJECT_DISPATCH_LAYER_PARENT_CACHE;
  }

  void init(Context* on_finish = nullptr);
  void shut_down(Context* on_finish) {
    m_image_ctx->op_work_queue->queue(on_finish, 0);
  }

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
      Context** on_finish, Context* on_dispatched) {
    return false;
  }

  bool write(
      uint64_t object_no, uint64_t object_off, ceph::bufferlist&& data,
      IOContext io_context, int op_flags, int write_flags,
      std::optional<uint64_t> assert_version,
      const ZTracer::Trace &parent_trace, int* object_dispatch_flags,
      uint64_t* journal_tid, io::DispatchResult* dispatch_result,
      Context** on_finish, Context* on_dispatched) {
    return false;
  }

  bool write_same(
      uint64_t object_no, uint64_t object_off, uint64_t object_len,
      io::LightweightBufferExtents&& buffer_extents, ceph::bufferlist&& data,
      IOContext io_context, int op_flags,
      const ZTracer::Trace &parent_trace, int* object_dispatch_flags,
      uint64_t* journal_tid, io::DispatchResult* dispatch_result,
      Context** on_finish, Context* on_dispatched) {
    return false;
  }

  bool compare_and_write(
      uint64_t object_no, uint64_t object_off, ceph::bufferlist&& cmp_data,
      ceph::bufferlist&& write_data, IOContext io_context, int op_flags,
      const ZTracer::Trace &parent_trace, uint64_t* mismatch_offset,
      int* object_dispatch_flags, uint64_t* journal_tid,
      io::DispatchResult* dispatch_result, Context** on_finish,
      Context* on_dispatched) {
    return false;
  }

  bool flush(
      io::FlushSource flush_source, const ZTracer::Trace &parent_trace,
      uint64_t* journal_id, io::DispatchResult* dispatch_result,
      Context** on_finish, Context* on_dispatched) {
    return false;
  }

  bool list_snaps(
      uint64_t object_no, io::Extents&& extents, io::SnapIds&& snap_ids,
      int list_snap_flags, const ZTracer::Trace &parent_trace,
      io::SnapshotDelta* snapshot_delta, int* object_dispatch_flags,
      io::DispatchResult* dispatch_result, Context** on_finish,
      Context* on_dispatched) override {
    return false;
  }

  bool invalidate_cache(Context* on_finish) {
    return false;
  }

  bool reset_existence_cache(Context* on_finish) {
    return false;
  }

  void extent_overwritten(
      uint64_t object_no, uint64_t object_off, uint64_t object_len,
      uint64_t journal_tid, uint64_t new_journal_tid) {
  }

  int prepare_copyup(
      uint64_t object_no,
      io::SnapshotSparseBufferlist* snapshot_sparse_bufferlist) override {
    return 0;
  }

  ImageCtxT* get_image_ctx() {
    return m_image_ctx;
  }

  CacheClient* get_cache_client() {
    return m_cache_client;
  }

private:

  int read_object(std::string file_path, ceph::bufferlist* read_data,
                  uint64_t offset, uint64_t length, Context *on_finish);
  void handle_read_cache(ceph::immutable_obj_cache::ObjectCacheRequest* ack,
                         uint64_t object_no, io::ReadExtents* extents,
                         IOContext io_context, int read_flags,
                         const ZTracer::Trace &parent_trace,
                         io::DispatchResult* dispatch_result,
                         Context* on_dispatched);
  int handle_register_client(bool reg);
  void create_cache_session(Context* on_finish, bool is_reconnect);

  ImageCtxT* m_image_ctx;
  plugin::Api<ImageCtxT>& m_plugin_api;

  ceph::mutex m_lock;
  CacheClient *m_cache_client = nullptr;
  bool m_connecting = false;
};

} // namespace cache
} // namespace librbd

extern template class librbd::cache::ParentCacheObjectDispatch<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_CACHE_PARENT_CACHER_OBJECT_DISPATCH_H
