// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/io/ObjectDispatcher.h"
#include "include/Context.h"
#include "common/AsyncOpTracker.h"
#include "common/dout.h"
#include "librbd/ImageCtx.h"
#include "librbd/Utils.h"
#include "librbd/asio/ContextWQ.h"
#include "librbd/io/ObjectDispatch.h"
#include "librbd/io/ObjectDispatchSpec.h"
#include <boost/variant.hpp>

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::io::ObjectDispatcher: " << this \
                           << " " << __func__ << ": "

namespace librbd {
namespace io {

template <typename I>
struct ObjectDispatcher<I>::C_ResetExistenceCache : public C_LayerIterator {
  C_ResetExistenceCache(ObjectDispatcher* object_dispatcher, Context* on_finish)
    : C_LayerIterator(object_dispatcher, OBJECT_DISPATCH_LAYER_NONE, on_finish) {
  }

  bool execute(ObjectDispatchInterface* object_dispatch,
               Context* on_finish) override {
    return object_dispatch->reset_existence_cache(on_finish);
  }
};

template <typename I>
struct ObjectDispatcher<I>::SendVisitor : public boost::static_visitor<bool> {
  ObjectDispatchInterface* object_dispatch;
  ObjectDispatchSpec* object_dispatch_spec;

  SendVisitor(ObjectDispatchInterface* object_dispatch,
              ObjectDispatchSpec* object_dispatch_spec)
    : object_dispatch(object_dispatch),
      object_dispatch_spec(object_dispatch_spec) {
  }

  bool operator()(ObjectDispatchSpec::ReadRequest& read) const {
    return object_dispatch->read(
      read.object_no, read.extents, object_dispatch_spec->io_context,
      object_dispatch_spec->op_flags, read.read_flags,
      object_dispatch_spec->parent_trace, read.version,
      &object_dispatch_spec->object_dispatch_flags,
      &object_dispatch_spec->dispatch_result,
      &object_dispatch_spec->dispatcher_ctx.on_finish,
      &object_dispatch_spec->dispatcher_ctx);
  }

  bool operator()(ObjectDispatchSpec::DiscardRequest& discard) const {
    return object_dispatch->discard(
      discard.object_no, discard.object_off, discard.object_len,
      object_dispatch_spec->io_context, discard.discard_flags,
      object_dispatch_spec->parent_trace,
      &object_dispatch_spec->object_dispatch_flags, &discard.journal_tid,
      &object_dispatch_spec->dispatch_result,
      &object_dispatch_spec->dispatcher_ctx.on_finish,
      &object_dispatch_spec->dispatcher_ctx);
  }

  bool operator()(ObjectDispatchSpec::WriteRequest& write) const {
    return object_dispatch->write(
      write.object_no, write.object_off, std::move(write.data),
      object_dispatch_spec->io_context, object_dispatch_spec->op_flags,
      write.write_flags, write.assert_version,
      object_dispatch_spec->parent_trace,
      &object_dispatch_spec->object_dispatch_flags, &write.journal_tid,
      &object_dispatch_spec->dispatch_result,
      &object_dispatch_spec->dispatcher_ctx.on_finish,
      &object_dispatch_spec->dispatcher_ctx);
  }

  bool operator()(ObjectDispatchSpec::WriteSameRequest& write_same) const {
    return object_dispatch->write_same(
      write_same.object_no, write_same.object_off, write_same.object_len,
      std::move(write_same.buffer_extents), std::move(write_same.data),
      object_dispatch_spec->io_context, object_dispatch_spec->op_flags,
      object_dispatch_spec->parent_trace,
      &object_dispatch_spec->object_dispatch_flags, &write_same.journal_tid,
      &object_dispatch_spec->dispatch_result,
      &object_dispatch_spec->dispatcher_ctx.on_finish,
      &object_dispatch_spec->dispatcher_ctx);
  }

  bool operator()(
      ObjectDispatchSpec::CompareAndWriteRequest& compare_and_write) const {
    return object_dispatch->compare_and_write(
      compare_and_write.object_no, compare_and_write.object_off,
      std::move(compare_and_write.cmp_data), std::move(compare_and_write.data),
      object_dispatch_spec->io_context, object_dispatch_spec->op_flags,
      object_dispatch_spec->parent_trace, compare_and_write.mismatch_offset,
      &object_dispatch_spec->object_dispatch_flags,
      &compare_and_write.journal_tid,
      &object_dispatch_spec->dispatch_result,
      &object_dispatch_spec->dispatcher_ctx.on_finish,
      &object_dispatch_spec->dispatcher_ctx);
  }

  bool operator()(ObjectDispatchSpec::FlushRequest& flush) const {
    return object_dispatch->flush(
      flush.flush_source, object_dispatch_spec->parent_trace,
      &flush.journal_tid,
      &object_dispatch_spec->dispatch_result,
      &object_dispatch_spec->dispatcher_ctx.on_finish,
      &object_dispatch_spec->dispatcher_ctx);
  }

  bool operator()(ObjectDispatchSpec::ListSnapsRequest& list_snaps) const {
    return object_dispatch->list_snaps(
      list_snaps.object_no, std::move(list_snaps.extents),
      std::move(list_snaps.snap_ids), list_snaps.list_snaps_flags,
      object_dispatch_spec->parent_trace, list_snaps.snapshot_delta,
      &object_dispatch_spec->object_dispatch_flags,
      &object_dispatch_spec->dispatch_result,
      &object_dispatch_spec->dispatcher_ctx.on_finish,
      &object_dispatch_spec->dispatcher_ctx);
  }
};

template <typename I>
ObjectDispatcher<I>::ObjectDispatcher(I* image_ctx)
  : Dispatcher<I, ObjectDispatcherInterface>(image_ctx) {
  // configure the core object dispatch handler on startup
  auto object_dispatch = new ObjectDispatch(image_ctx);
  this->register_dispatch(object_dispatch);
}

template <typename I>
void ObjectDispatcher<I>::invalidate_cache(Context* on_finish) {
  auto image_ctx = this->m_image_ctx;
  auto cct = image_ctx->cct;
  ldout(cct, 5) << dendl;

  on_finish = util::create_async_context_callback(*image_ctx, on_finish);
  auto ctx = new C_InvalidateCache(
    this, OBJECT_DISPATCH_LAYER_NONE, on_finish);
  ctx->complete(0);
}

template <typename I>
void ObjectDispatcher<I>::reset_existence_cache(Context* on_finish) {
  auto image_ctx = this->m_image_ctx;
  auto cct = image_ctx->cct;
  ldout(cct, 5) << dendl;

  on_finish = util::create_async_context_callback(*image_ctx, on_finish);
  auto ctx = new C_ResetExistenceCache(this, on_finish);
  ctx->complete(0);
}

template <typename I>
void ObjectDispatcher<I>::extent_overwritten(
    uint64_t object_no, uint64_t object_off, uint64_t object_len,
    uint64_t journal_tid, uint64_t new_journal_tid) {
  auto cct = this->m_image_ctx->cct;
  ldout(cct, 20) << object_no << " " << object_off << "~" << object_len
                 << dendl;

  this->m_op_tracker->start_op();
  for (auto it : this->m_dispatches) {
    auto& object_dispatch_meta = it.second;
    auto object_dispatch = object_dispatch_meta.dispatch;
    object_dispatch->extent_overwritten(object_no, object_off, object_len,
                                        journal_tid, new_journal_tid);
  }
  this->m_op_tracker->finish_op();

}

template <typename I>
int ObjectDispatcher<I>::prepare_copyup(
    uint64_t object_no,
    SnapshotSparseBufferlist* snapshot_sparse_bufferlist) {
  auto cct = this->m_image_ctx->cct;
  ldout(cct, 20) << "object_no=" << object_no << dendl;

  this->m_op_tracker->start_op();
  for (auto it : this->m_dispatches) {
    auto& object_dispatch_meta = it.second;
    auto object_dispatch = object_dispatch_meta.dispatch;
    auto r = object_dispatch->prepare_copyup(
            object_no, snapshot_sparse_bufferlist);
    if (r < 0) {
      this->m_op_tracker->finish_op();
      return r;
    }
  }

  this->m_op_tracker->finish_op();
  return 0;
}

template <typename I>
bool ObjectDispatcher<I>::send_dispatch(
    ObjectDispatchInterface* object_dispatch,
    ObjectDispatchSpec* object_dispatch_spec) {
  return boost::apply_visitor(
    SendVisitor{object_dispatch, object_dispatch_spec},
    object_dispatch_spec->request);
}

} // namespace io
} // namespace librbd

template class librbd::io::ObjectDispatcher<librbd::ImageCtx>;
