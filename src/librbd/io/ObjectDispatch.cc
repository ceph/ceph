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
    uint64_t object_no, uint64_t object_off, uint64_t object_len,
    librados::snap_t snap_id, int op_flags, const ZTracer::Trace &parent_trace,
    ceph::bufferlist* read_data, ExtentMap* extent_map,
    int* object_dispatch_flags, DispatchResult* dispatch_result,
    Context** on_finish, Context* on_dispatched) {
  auto cct = m_image_ctx->cct;
  ldout(cct, 20) << data_object_name(m_image_ctx, object_no) << " "
                 << object_off << "~" << object_len << dendl;

  *dispatch_result = DISPATCH_RESULT_COMPLETE;
  auto req = new ObjectReadRequest<I>(m_image_ctx, object_no, object_off,
                                      object_len, snap_id, op_flags,
                                      parent_trace, read_data, extent_map,
                                      on_dispatched);
  req->send();
  return true;
}

template <typename I>
bool ObjectDispatch<I>::discard(
    uint64_t object_no, uint64_t object_off, uint64_t object_len,
    const ::SnapContext &snapc, int discard_flags,
    const ZTracer::Trace &parent_trace, int* object_dispatch_flags,
    uint64_t* journal_tid, DispatchResult* dispatch_result,
    Context** on_finish, Context* on_dispatched) {
  auto cct = m_image_ctx->cct;
  ldout(cct, 20) << data_object_name(m_image_ctx, object_no) << " "
                 << object_off << "~" << object_len << dendl;

  *dispatch_result = DISPATCH_RESULT_COMPLETE;
  auto req = new ObjectDiscardRequest<I>(m_image_ctx, object_no, object_off,
                                         object_len, snapc, discard_flags,
                                         parent_trace, on_dispatched);
  req->send();
  return true;
}

template <typename I>
bool ObjectDispatch<I>::write(
    uint64_t object_no, uint64_t object_off, ceph::bufferlist&& data,
    const ::SnapContext &snapc, int op_flags,
    const ZTracer::Trace &parent_trace, int* object_dispatch_flags,
    uint64_t* journal_tid, DispatchResult* dispatch_result,
    Context** on_finish, Context* on_dispatched) {
  auto cct = m_image_ctx->cct;
  ldout(cct, 20) << data_object_name(m_image_ctx, object_no) << " "
                 << object_off << "~" << data.length() << dendl;

  *dispatch_result = DISPATCH_RESULT_COMPLETE;
  auto req = new ObjectWriteRequest<I>(m_image_ctx, object_no, object_off,
                                       std::move(data), snapc, op_flags,
                                       parent_trace, on_dispatched);
  req->send();
  return true;
}

template <typename I>
bool ObjectDispatch<I>::write_same(
    uint64_t object_no, uint64_t object_off, uint64_t object_len,
    LightweightBufferExtents&& buffer_extents, ceph::bufferlist&& data,
    const ::SnapContext &snapc, int op_flags,
    const ZTracer::Trace &parent_trace, int* object_dispatch_flags,
    uint64_t* journal_tid, DispatchResult* dispatch_result,
    Context** on_finish, Context* on_dispatched) {
  auto cct = m_image_ctx->cct;
  ldout(cct, 20) << data_object_name(m_image_ctx, object_no) << " "
                 << object_off << "~" << object_len << dendl;

  *dispatch_result = DISPATCH_RESULT_COMPLETE;
  auto req = new ObjectWriteSameRequest<I>(m_image_ctx, object_no,
                                           object_off, object_len,
                                           std::move(data), snapc, op_flags,
                                           parent_trace, on_dispatched);
  req->send();
  return true;
}

template <typename I>
bool ObjectDispatch<I>::compare_and_write(
    uint64_t object_no, uint64_t object_off, ceph::bufferlist&& cmp_data,
    ceph::bufferlist&& write_data, const ::SnapContext &snapc, int op_flags,
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
                                                 std::move(write_data), snapc,
                                                 mismatch_offset, op_flags,
                                                 parent_trace, on_dispatched);
  req->send();
  return true;
}

} // namespace io
} // namespace librbd

template class librbd::io::ObjectDispatch<librbd::ImageCtx>;
