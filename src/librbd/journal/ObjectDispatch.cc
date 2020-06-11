// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/journal/ObjectDispatch.h"
#include "common/dout.h"
#include "osdc/Striper.h"
#include "librbd/ImageCtx.h"
#include "librbd/Journal.h"
#include "librbd/Utils.h"
#include "librbd/asio/ContextWQ.h"
#include "librbd/io/ObjectDispatchSpec.h"
#include "librbd/io/ObjectDispatcherInterface.h"

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::journal::ObjectDispatch: " << this \
                           << " " << __func__ << ": "

namespace librbd {
namespace journal {

using librbd::util::data_object_name;
using util::create_context_callback;

namespace {

template <typename I>
struct C_CommitIOEvent : public Context {
  I* image_ctx;
  Journal<I>* journal;
  uint64_t object_no;
  uint64_t object_off;
  uint64_t object_len;
  uint64_t journal_tid;
  int object_dispatch_flags;
  Context* on_finish;

  C_CommitIOEvent(I* image_ctx, Journal<I>* journal, uint64_t object_no,
                  uint64_t object_off, uint64_t object_len,
                  uint64_t journal_tid, int object_dispatch_flags,
                  Context* on_finish)
    : image_ctx(image_ctx), journal(journal), object_no(object_no),
      object_off(object_off), object_len(object_len), journal_tid(journal_tid),
      object_dispatch_flags(object_dispatch_flags), on_finish(on_finish) {
  }

  void finish(int r) override {
    // don't commit the IO extent if a previous dispatch handler will just
    // retry the failed IO
    if (r >= 0 ||
        (object_dispatch_flags &
           io::OBJECT_DISPATCH_FLAG_WILL_RETRY_ON_ERROR) == 0) {
      io::Extents file_extents;
      Striper::extent_to_file(image_ctx->cct, &image_ctx->layout, object_no,
                              object_off, object_len, file_extents);
      for (auto& extent : file_extents) {
        journal->commit_io_event_extent(journal_tid, extent.first,
                                        extent.second, r);
      }
    }

    if (on_finish != nullptr) {
      on_finish->complete(r);
    }
  }
};

} // anonymous namespace

template <typename I>
ObjectDispatch<I>::ObjectDispatch(I* image_ctx, Journal<I>* journal)
  : m_image_ctx(image_ctx), m_journal(journal) {
}

template <typename I>
void ObjectDispatch<I>::shut_down(Context* on_finish) {
  m_image_ctx->op_work_queue->queue(on_finish, 0);
}

template <typename I>
bool ObjectDispatch<I>::discard(
    uint64_t object_no, uint64_t object_off, uint64_t object_len,
    const ::SnapContext &snapc, int discard_flags,
    const ZTracer::Trace &parent_trace, int* object_dispatch_flags,
    uint64_t* journal_tid, io::DispatchResult* dispatch_result,
    Context** on_finish, Context* on_dispatched) {
  if (*journal_tid == 0) {
    // non-journaled IO
    return false;
  }

  auto cct = m_image_ctx->cct;
  ldout(cct, 20) << data_object_name(m_image_ctx, object_no) << " "
                 << object_off << "~" << object_len << dendl;

  *on_finish = new C_CommitIOEvent<I>(m_image_ctx, m_journal, object_no,
                                      object_off, object_len, *journal_tid,
                                      *object_dispatch_flags, *on_finish);
  *on_finish = create_context_callback<
    Context, &Context::complete>(*on_finish, m_journal);

  *dispatch_result = io::DISPATCH_RESULT_CONTINUE;
  wait_or_flush_event(*journal_tid, *object_dispatch_flags, on_dispatched);
  return true;
}

template <typename I>
bool ObjectDispatch<I>::write(
    uint64_t object_no, uint64_t object_off, ceph::bufferlist&& data,
    const ::SnapContext &snapc, int op_flags,
    const ZTracer::Trace &parent_trace, int* object_dispatch_flags,
    uint64_t* journal_tid, io::DispatchResult* dispatch_result,
    Context** on_finish, Context* on_dispatched) {
  if (*journal_tid == 0) {
    // non-journaled IO
    return false;
  }

  auto cct = m_image_ctx->cct;
  ldout(cct, 20) << data_object_name(m_image_ctx, object_no) << " "
                 << object_off << "~" << data.length() << dendl;

  *on_finish = new C_CommitIOEvent<I>(m_image_ctx, m_journal, object_no,
                                      object_off, data.length(), *journal_tid,
                                      *object_dispatch_flags, *on_finish);
  *on_finish = create_context_callback<
    Context, &Context::complete>(*on_finish, m_journal);

  *dispatch_result = io::DISPATCH_RESULT_CONTINUE;
  wait_or_flush_event(*journal_tid, *object_dispatch_flags, on_dispatched);
  return true;
}

template <typename I>
bool ObjectDispatch<I>::write_same(
    uint64_t object_no, uint64_t object_off, uint64_t object_len,
    io::LightweightBufferExtents&& buffer_extents, ceph::bufferlist&& data,
    const ::SnapContext &snapc, int op_flags,
    const ZTracer::Trace &parent_trace, int* object_dispatch_flags,
    uint64_t* journal_tid, io::DispatchResult* dispatch_result,
    Context** on_finish, Context* on_dispatched) {
  if (*journal_tid == 0) {
    // non-journaled IO
    return false;
  }

  auto cct = m_image_ctx->cct;
  ldout(cct, 20) << data_object_name(m_image_ctx, object_no) << " "
                 << object_off << "~" << object_len << dendl;

  *on_finish = new C_CommitIOEvent<I>(m_image_ctx, m_journal, object_no,
                                      object_off, object_len, *journal_tid,
                                      *object_dispatch_flags, *on_finish);
  *on_finish = create_context_callback<
    Context, &Context::complete>(*on_finish, m_journal);

  *dispatch_result = io::DISPATCH_RESULT_CONTINUE;
  wait_or_flush_event(*journal_tid, *object_dispatch_flags, on_dispatched);
  return true;
}

template <typename I>
bool ObjectDispatch<I>::compare_and_write(
    uint64_t object_no, uint64_t object_off, ceph::bufferlist&& cmp_data,
    ceph::bufferlist&& write_data, const ::SnapContext &snapc, int op_flags,
    const ZTracer::Trace &parent_trace, uint64_t* mismatch_offset,
    int* object_dispatch_flags, uint64_t* journal_tid,
    io::DispatchResult* dispatch_result, Context** on_finish,
    Context* on_dispatched) {
  if (*journal_tid == 0) {
    // non-journaled IO
    return false;
  }

  auto cct = m_image_ctx->cct;
  ldout(cct, 20) << data_object_name(m_image_ctx, object_no) << " "
                 << object_off << "~" << write_data.length()
                 << dendl;

  *on_finish = new C_CommitIOEvent<I>(m_image_ctx, m_journal, object_no,
                                      object_off, write_data.length(),
                                      *journal_tid, *object_dispatch_flags,
                                      *on_finish);
  *on_finish = create_context_callback<
    Context, &Context::complete>(*on_finish, m_journal);

  *dispatch_result = io::DISPATCH_RESULT_CONTINUE;
  wait_or_flush_event(*journal_tid, *object_dispatch_flags, on_dispatched);
  return true;
}

template <typename I>
bool ObjectDispatch<I>::flush(
    io::FlushSource flush_source, const ZTracer::Trace &parent_trace,
    uint64_t* journal_tid, io::DispatchResult* dispatch_result,
    Context** on_finish, Context* on_dispatched) {
  if (*journal_tid == 0) {
    // non-journaled IO
    return false;
  }

  auto cct = m_image_ctx->cct;
  ldout(cct, 20) << dendl;

  auto ctx = *on_finish;
  *on_finish = new LambdaContext(
    [image_ctx=m_image_ctx, ctx, journal_tid=*journal_tid](int r) {
      image_ctx->journal->commit_io_event(journal_tid, r);
      ctx->complete(r);
    });

  *dispatch_result = io::DISPATCH_RESULT_CONTINUE;
  wait_or_flush_event(*journal_tid, io::OBJECT_DISPATCH_FLAG_FLUSH,
                      on_dispatched);
  return true;
}

template <typename I>
void ObjectDispatch<I>::extent_overwritten(
    uint64_t object_no, uint64_t object_off, uint64_t object_len,
    uint64_t journal_tid, uint64_t new_journal_tid) {
  auto cct = m_image_ctx->cct;
  ldout(cct, 20) << object_no << " " << object_off << "~" << object_len
                 << dendl;

  Context *ctx = new C_CommitIOEvent<I>(m_image_ctx, m_journal, object_no,
                                        object_off, object_len, journal_tid, false,
                                        nullptr);
  if (new_journal_tid != 0) {
    // ensure new journal event is safely committed to disk before
    // committing old event
    m_journal->flush_event(new_journal_tid, ctx);
  } else {
    ctx = create_context_callback<
      Context, &Context::complete>(ctx, m_journal);
    ctx->complete(0);
  }
}

template <typename I>
void ObjectDispatch<I>::wait_or_flush_event(
    uint64_t journal_tid, int object_dispatch_flags, Context* on_dispatched) {
  auto cct = m_image_ctx->cct;
  ldout(cct, 20) << "journal_tid=" << journal_tid << dendl;

  if ((object_dispatch_flags & io::OBJECT_DISPATCH_FLAG_FLUSH) != 0) {
    m_journal->flush_event(journal_tid, on_dispatched);
  } else {
    m_journal->wait_event(journal_tid, on_dispatched);
  }
}

} // namespace journal
} // namespace librbd

template class librbd::journal::ObjectDispatch<librbd::ImageCtx>;
