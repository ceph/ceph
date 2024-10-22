// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/io/ObjectDispatch.h"
#include "common/dout.h"
#include "librbd/AsioEngine.h"
#include "librbd/ImageCtx.h"
#include "librbd/Utils.h"
#include "librbd/io/ObjectRequest.h"

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::io::ObjectDispatch: " << this \
                           << " " << __func__ << ": "

namespace librbd {
namespace io {

using librbd::util::data_object_name;

template <typename I>
ObjectDispatch<I>::ObjectDispatch(I* image_ctx)
  : m_image_ctx(image_ctx) {
}

template <typename I>
void ObjectDispatch<I>::shut_down(Context* on_finish) {
  auto cct = m_image_ctx->cct;
  ldout(cct, 5) << dendl;

  m_image_ctx->asio_engine->post(on_finish, 0);
}

template <typename I>
bool ObjectDispatch<I>::read(
    uint64_t object_no, ReadExtents* extents, IOContext io_context,
    int op_flags, int read_flags, const ZTracer::Trace &parent_trace,
    uint64_t* version, int* object_dispatch_flags,
    DispatchResult* dispatch_result, Context** on_finish,
    Context* on_dispatched) {
  auto cct = m_image_ctx->cct;
  ldout(cct, 20) << "object_no=" << object_no << " " << *extents << dendl;

  *dispatch_result = DISPATCH_RESULT_COMPLETE;
  auto req = new ObjectReadRequest<I>(m_image_ctx, object_no, extents,
                                      io_context, op_flags, read_flags,
                                      parent_trace, version, on_dispatched);
  req->send();
  return true;
}

template <typename I>
bool ObjectDispatch<I>::discard(
    uint64_t object_no, uint64_t object_off, uint64_t object_len,
    IOContext io_context, int discard_flags,
    const ZTracer::Trace &parent_trace, int* object_dispatch_flags,
    uint64_t* journal_tid, DispatchResult* dispatch_result,
    Context** on_finish, Context* on_dispatched) {
  auto cct = m_image_ctx->cct;
  ldout(cct, 20) << data_object_name(m_image_ctx, object_no) << " "
                 << object_off << "~" << object_len << dendl;

  *dispatch_result = DISPATCH_RESULT_COMPLETE;
  auto req = new ObjectDiscardRequest<I>(m_image_ctx, object_no, object_off,
                                         object_len, io_context, discard_flags,
                                         parent_trace, on_dispatched);
  req->send();
  return true;
}

template <typename I>
bool ObjectDispatch<I>::write(
    uint64_t object_no, uint64_t object_off, ceph::bufferlist&& data,
    IOContext io_context, int op_flags, int write_flags,
    std::optional<uint64_t> assert_version,
    const ZTracer::Trace &parent_trace, int* object_dispatch_flags,
    uint64_t* journal_tid, DispatchResult* dispatch_result,
    Context** on_finish, Context* on_dispatched) {
  auto cct = m_image_ctx->cct;
  ldout(cct, 20) << data_object_name(m_image_ctx, object_no) << " "
                 << object_off << "~" << data.length() << dendl;

  *dispatch_result = DISPATCH_RESULT_COMPLETE;
  auto req = new ObjectWriteRequest<I>(m_image_ctx, object_no, object_off,
                                       std::move(data), io_context, op_flags,
                                       write_flags, assert_version,
                                       parent_trace, on_dispatched);
  req->send();
  return true;
}

template <typename I>
bool ObjectDispatch<I>::write_same(
    uint64_t object_no, uint64_t object_off, uint64_t object_len,
    LightweightBufferExtents&& buffer_extents, ceph::bufferlist&& data,
    IOContext io_context, int op_flags,
    const ZTracer::Trace &parent_trace, int* object_dispatch_flags,
    uint64_t* journal_tid, DispatchResult* dispatch_result,
    Context** on_finish, Context* on_dispatched) {
  auto cct = m_image_ctx->cct;
  ldout(cct, 20) << data_object_name(m_image_ctx, object_no) << " "
                 << object_off << "~" << object_len << dendl;

  *dispatch_result = DISPATCH_RESULT_COMPLETE;
  auto req = new ObjectWriteSameRequest<I>(m_image_ctx, object_no,
                                           object_off, object_len,
                                           std::move(data), io_context,
                                           op_flags, parent_trace,
                                           on_dispatched);
  req->send();
  return true;
}

template <typename I>
bool ObjectDispatch<I>::compare_and_write(
    uint64_t object_no, uint64_t object_off, ceph::bufferlist&& cmp_data,
    ceph::bufferlist&& write_data, IOContext io_context, int op_flags,
    const ZTracer::Trace &parent_trace, uint64_t* mismatch_offset,
    int* object_dispatch_flags, uint64_t* journal_tid,
    DispatchResult* dispatch_result, Context** on_finish,
    Context* on_dispatched) {
  auto cct = m_image_ctx->cct;
  ldout(cct, 20) << data_object_name(m_image_ctx, object_no) << " "
                 << object_off << "~" << write_data.length() << dendl;

  *dispatch_result = DISPATCH_RESULT_COMPLETE;
  auto req = new ObjectCompareAndWriteRequest<I>(m_image_ctx, object_no,
                                                 object_off,
                                                 std::move(cmp_data),
                                                 std::move(write_data),
                                                 io_context, mismatch_offset,
                                                 op_flags, parent_trace,
                                                 on_dispatched);
  req->send();
  return true;
}

template <typename I>
bool ObjectDispatch<I>::list_snaps(
    uint64_t object_no, io::Extents&& extents, SnapIds&& snap_ids,
    int list_snap_flags, const ZTracer::Trace &parent_trace,
    SnapshotDelta* snapshot_delta, int* object_dispatch_flags,
    DispatchResult* dispatch_result, Context** on_finish,
    Context* on_dispatched) {
  auto cct = m_image_ctx->cct;
  ldout(cct, 20) << data_object_name(m_image_ctx, object_no) << " "
                 << "extents=" << extents << ", "
                 << "snap_ids=" << snap_ids << dendl;

  *dispatch_result = DISPATCH_RESULT_COMPLETE;
  auto req = ObjectListSnapsRequest<I>::create(
    m_image_ctx, object_no, std::move(extents), std::move(snap_ids),
    list_snap_flags, parent_trace, snapshot_delta, on_dispatched);
  req->send();
  return true;
}

} // namespace io
} // namespace librbd

template class librbd::io::ObjectDispatch<librbd::ImageCtx>;
