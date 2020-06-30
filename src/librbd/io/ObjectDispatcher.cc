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
struct ObjectDispatcher<I>::C_LayerIterator : public Context {
  ObjectDispatcher* object_dispatcher;
  Context* on_finish;

  ObjectDispatchLayer object_dispatch_layer = OBJECT_DISPATCH_LAYER_NONE;

  C_LayerIterator(ObjectDispatcher* object_dispatcher,
                Context* on_finish)
    : object_dispatcher(object_dispatcher), on_finish(on_finish) {
  }

  void complete(int r) override {
    while (true) {
      object_dispatcher->m_lock.lock_shared();
      auto it = object_dispatcher->m_dispatches.upper_bound(
        object_dispatch_layer);
      if (it == object_dispatcher->m_dispatches.end()) {
        object_dispatcher->m_lock.unlock_shared();
        Context::complete(r);
        return;
      }

      auto& object_dispatch_meta = it->second;
      auto object_dispatch = object_dispatch_meta.dispatch;

      // prevent recursive locking back into the dispatcher while handling IO
      object_dispatch_meta.async_op_tracker->start_op();
      object_dispatcher->m_lock.unlock_shared();

      // next loop should start after current layer
      object_dispatch_layer = object_dispatch->get_dispatch_layer();

      auto handled = execute(object_dispatch, this);
      object_dispatch_meta.async_op_tracker->finish_op();

      if (handled) {
        break;
      }
    }
  }

  void finish(int r) override {
    on_finish->complete(0);
  }

  virtual bool execute(ObjectDispatchInterface* object_dispatch,
                       Context* on_finish) = 0;
};

template <typename I>
struct ObjectDispatcher<I>::C_InvalidateCache : public C_LayerIterator {
  C_InvalidateCache(ObjectDispatcher* object_dispatcher, Context* on_finish)
    : C_LayerIterator(object_dispatcher, on_finish) {
  }

  bool execute(ObjectDispatchInterface* object_dispatch,
               Context* on_finish) override {
    return object_dispatch->invalidate_cache(on_finish);
  }
};

template <typename I>
struct ObjectDispatcher<I>::C_ResetExistenceCache : public C_LayerIterator {
  C_ResetExistenceCache(ObjectDispatcher* object_dispatcher, Context* on_finish)
    : C_LayerIterator(object_dispatcher, on_finish) {
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
      read.object_no, read.object_off, read.object_len, read.snap_id,
      object_dispatch_spec->op_flags, object_dispatch_spec->parent_trace,
      read.read_data, read.extent_map,
      &object_dispatch_spec->object_dispatch_flags,
      &object_dispatch_spec->dispatch_result,
      &object_dispatch_spec->dispatcher_ctx.on_finish,
      &object_dispatch_spec->dispatcher_ctx);
  }

  bool operator()(ObjectDispatchSpec::DiscardRequest& discard) const {
    return object_dispatch->discard(
      discard.object_no, discard.object_off, discard.object_len, discard.snapc,
      discard.discard_flags, object_dispatch_spec->parent_trace,
      &object_dispatch_spec->object_dispatch_flags, &discard.journal_tid,
      &object_dispatch_spec->dispatch_result,
      &object_dispatch_spec->dispatcher_ctx.on_finish,
      &object_dispatch_spec->dispatcher_ctx);
  }

  bool operator()(ObjectDispatchSpec::WriteRequest& write) const {
    return object_dispatch->write(
      write.object_no, write.object_off, std::move(write.data), write.snapc,
      object_dispatch_spec->op_flags, object_dispatch_spec->parent_trace,
      &object_dispatch_spec->object_dispatch_flags, &write.journal_tid,
      &object_dispatch_spec->dispatch_result,
      &object_dispatch_spec->dispatcher_ctx.on_finish,
      &object_dispatch_spec->dispatcher_ctx);
  }

  bool operator()(ObjectDispatchSpec::WriteSameRequest& write_same) const {
    return object_dispatch->write_same(
      write_same.object_no, write_same.object_off, write_same.object_len,
      std::move(write_same.buffer_extents), std::move(write_same.data),
      write_same.snapc, object_dispatch_spec->op_flags,
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
      compare_and_write.snapc, object_dispatch_spec->op_flags,
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
  auto ctx = new C_InvalidateCache(this, on_finish);
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

  std::shared_lock locker{this->m_lock};
  for (auto it : this->m_dispatches) {
    auto& object_dispatch_meta = it.second;
    auto object_dispatch = object_dispatch_meta.dispatch;
    object_dispatch->extent_overwritten(object_no, object_off, object_len,
                                        journal_tid, new_journal_tid);
  }
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
