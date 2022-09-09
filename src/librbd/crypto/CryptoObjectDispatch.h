// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_CRYPTO_CRYPTO_OBJECT_DISPATCH_H
#define CEPH_LIBRBD_CRYPTO_CRYPTO_OBJECT_DISPATCH_H

#include "librbd/crypto/CryptoInterface.h"
#include "librbd/io/Types.h"
#include "librbd/io/ObjectDispatchInterface.h"

namespace librbd {

struct ImageCtx;

namespace crypto {

template <typename ImageCtxT = librbd::ImageCtx>
class CryptoObjectDispatch : public io::ObjectDispatchInterface {
public:
  static CryptoObjectDispatch* create(
          ImageCtxT* image_ctx, CryptoInterface* crypto) {
    return new CryptoObjectDispatch(image_ctx, crypto);
  }

  CryptoObjectDispatch(ImageCtxT* image_ctx,
                       CryptoInterface* crypto);

  io::ObjectDispatchLayer get_dispatch_layer() const override {
    return io::OBJECT_DISPATCH_LAYER_CRYPTO;
  }

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
      Context** on_finish, Context* on_dispatched) override {
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

  bool invalidate_cache(Context* on_finish) override {
    return false;
  }
  bool reset_existence_cache(Context* on_finish) override {
    return false;
  }

  void extent_overwritten(
          uint64_t object_no, uint64_t object_off, uint64_t object_len,
          uint64_t journal_tid, uint64_t new_journal_tid) override {
  }

  int prepare_copyup(
      uint64_t object_no,
      io::SnapshotSparseBufferlist* snapshot_sparse_bufferlist) override;

private:
  ImageCtxT* m_image_ctx;
  CryptoInterface* m_crypto;
  uint64_t m_data_offset_object_no;
};

} // namespace crypto
} // namespace librbd

extern template class librbd::crypto::CryptoObjectDispatch<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_CRYPTO_CRYPTO_OBJECT_DISPATCH_H
