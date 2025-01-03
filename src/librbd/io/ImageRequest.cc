// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/io/ImageRequest.h"
#include "librbd/ImageCtx.h"
#include "librbd/internal.h"
#include "librbd/Journal.h"
#include "librbd/Types.h"
#include "librbd/Utils.h"
#include "librbd/asio/ContextWQ.h"
#include "librbd/io/AioCompletion.h"
#include "librbd/io/AsyncOperation.h"
#include "librbd/io/ObjectDispatchInterface.h"
#include "librbd/io/ObjectDispatchSpec.h"
#include "librbd/io/ObjectDispatcherInterface.h"
#include "librbd/io/Utils.h"
#include "librbd/journal/Types.h"
#include "include/rados/librados.hpp"
#include "common/errno.h"
#include "common/perf_counters.h"
#include "osdc/Striper.h"
#include <algorithm>
#include <functional>
#include <map>

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::io::ImageRequest: " << __func__ << ": "

namespace librbd {
namespace io {

using librbd::util::data_object_name;
using librbd::util::get_image_ctx;

namespace {

template <typename I>
struct C_AssembleSnapshotDeltas : public C_AioRequest {
  I* image_ctx;
  SnapshotDelta* snapshot_delta;

  ceph::mutex lock = ceph::make_mutex(
    "librbd::io::C_AssembleSnapshotDeltas::lock", false);
  std::map<uint64_t, SnapshotDelta> object_snapshot_delta;

  C_AssembleSnapshotDeltas(I* image_ctx, AioCompletion* aio_comp,
                           SnapshotDelta* snapshot_delta)
    : C_AioRequest(aio_comp),
      image_ctx(image_ctx), snapshot_delta(snapshot_delta) {
  }

  SnapshotDelta* get_snapshot_delta(uint64_t object_no) {
    std::unique_lock locker{lock};
    return &object_snapshot_delta[object_no];
  }

  void finish(int r) override {
    auto cct = image_ctx->cct;

    if (r < 0) {
      lderr(cct) << "C_AssembleSnapshotDeltas: list snaps failed: "
                 << cpp_strerror(r) << dendl;
      C_AioRequest::finish(r);
      return;
    }

    std::unique_lock locker{lock};
    *snapshot_delta = {};
    for (auto& [object_no, object_snapshot_delta] : object_snapshot_delta) {
      SnapshotDelta image_snapshot_delta;
      object_to_image_intervals(object_no, object_snapshot_delta,
                                &image_snapshot_delta, snapshot_delta);

      ldout(cct, 20) << "object_no=" << object_no << ", "
                     << "object_snapshot_delta="
                     << object_snapshot_delta << ", "
                     << "image_snapshot_delta=" << image_snapshot_delta
                     << dendl;
    }

    ldout(cct, 20) << "snapshot_delta=" << *snapshot_delta << dendl;
    C_AioRequest::finish(0);
  }

  void object_to_image_intervals(
      uint64_t object_no, const SnapshotDelta& object_snapshot_delta,
      SnapshotDelta* image_snapshot_delta,
      SnapshotDelta* assembled_image_snapshot_delta) {
    for (auto& [key, object_extents] : object_snapshot_delta) {
      for (auto& object_extent : object_extents) {
        auto [image_extents, _] = io::util::object_to_area_extents(
            image_ctx, object_no,
            {{object_extent.get_off(), object_extent.get_len()}});

        auto& intervals = (*image_snapshot_delta)[key];
        auto& assembled_intervals = (*assembled_image_snapshot_delta)[key];
        for (auto [image_offset, image_length] : image_extents) {
          SparseExtent sparse_extent{object_extent.get_val().state,
                                     image_length};
          intervals.insert(image_offset, image_length, sparse_extent);
          assembled_intervals.insert(image_offset, image_length,
                                     sparse_extent);
        }
      }
    }
  }
};

template <typename I>
struct C_RBD_Readahead : public Context {
  I *ictx;
  uint64_t object_no;
  io::ReadExtents extents;

  C_RBD_Readahead(I *ictx, uint64_t object_no, uint64_t offset, uint64_t length)
    : ictx(ictx), object_no(object_no), extents({{offset, length}}) {
    ictx->readahead.inc_pending();
  }

  void finish(int r) override {
    ceph_assert(extents.size() == 1);
    auto& extent = extents.front();
    ldout(ictx->cct, 20) << "C_RBD_Readahead on "
                         << data_object_name(ictx, object_no) << ": "
                         << extent.offset << "~" << extent.length << dendl;
    ictx->readahead.dec_pending();
  }
};

template <typename I>
void readahead(I *ictx, const Extents& image_extents, IOContext io_context) {
  uint64_t total_bytes = 0;
  for (auto& image_extent : image_extents) {
    total_bytes += image_extent.second;
  }

  ictx->image_lock.lock_shared();
  auto total_bytes_read = ictx->total_bytes_read.fetch_add(total_bytes);
  bool abort = (
    ictx->readahead_disable_after_bytes != 0 &&
    total_bytes_read > ictx->readahead_disable_after_bytes);
  if (abort) {
    ictx->image_lock.unlock_shared();
    return;
  }

  uint64_t data_size = ictx->get_area_size(ImageArea::DATA);
  ictx->image_lock.unlock_shared();

  auto readahead_extent = ictx->readahead.update(image_extents, data_size);
  uint64_t readahead_offset = readahead_extent.first;
  uint64_t readahead_length = readahead_extent.second;

  if (readahead_length > 0) {
    ldout(ictx->cct, 20) << "(readahead logical) " << readahead_offset << "~"
                         << readahead_length << dendl;
    LightweightObjectExtents readahead_object_extents;
    io::util::area_to_object_extents(ictx, readahead_offset, readahead_length,
                                     ImageArea::DATA, 0,
                                     &readahead_object_extents);
    for (auto& object_extent : readahead_object_extents) {
      ldout(ictx->cct, 20) << "(readahead) "
                           << data_object_name(ictx,
                                               object_extent.object_no) << " "
                           << object_extent.offset << "~"
                           << object_extent.length << dendl;

      auto req_comp = new C_RBD_Readahead<I>(ictx, object_extent.object_no,
                                             object_extent.offset,
                                             object_extent.length);
      auto req = io::ObjectDispatchSpec::create_read(
        ictx, io::OBJECT_DISPATCH_LAYER_NONE, object_extent.object_no,
        &req_comp->extents, io_context, 0, 0, {}, nullptr, req_comp);
      req->send();
    }

    ictx->perfcounter->inc(l_librbd_readahead);
    ictx->perfcounter->inc(l_librbd_readahead_bytes, readahead_length);
  }
}

template <typename I>
struct C_UpdateTimestamp : public Context {
public:
  I& m_image_ctx;
  bool m_modify; // if modify set to 'true', modify timestamp is updated,
                 // access timestamp otherwise
  AsyncOperation m_async_op;

  C_UpdateTimestamp(I& ictx, bool m) : m_image_ctx(ictx), m_modify(m) {
    m_async_op.start_op(*get_image_ctx(&m_image_ctx));
  }
  ~C_UpdateTimestamp() override {
    m_async_op.finish_op();
  }

  void send() {
    librados::ObjectWriteOperation op;
    if (m_modify) {
      cls_client::set_modify_timestamp(&op);
    } else {
      cls_client::set_access_timestamp(&op);
    }

    auto comp = librbd::util::create_rados_callback(this);
    int r = m_image_ctx.md_ctx.aio_operate(m_image_ctx.header_oid, comp, &op);
    ceph_assert(r == 0);
    comp->release();
  }

  void finish(int r) override {
    // ignore errors updating timestamp
  }
};

bool should_update_timestamp(const utime_t& now, const utime_t& timestamp,
                             uint64_t interval) {
    return (interval &&
            (static_cast<uint64_t>(now.sec()) >= interval + timestamp));
}

} // anonymous namespace

#undef dout_prefix
#define dout_prefix *_dout << "librbd::io::ImageRequest: " << this \
                           << " " << __func__ << ": "

template <typename I>
void ImageRequest<I>::aio_read(I *ictx, AioCompletion *c,
                               Extents &&image_extents, ImageArea area,
                               ReadResult &&read_result, IOContext io_context,
                               int op_flags, int read_flags,
			       const ZTracer::Trace &parent_trace) {
  ImageReadRequest<I> req(*ictx, c, std::move(image_extents), area,
                          std::move(read_result), io_context, op_flags,
                          read_flags, parent_trace);
  req.send();
}

template <typename I>
void ImageRequest<I>::aio_write(I *ictx, AioCompletion *c,
                                Extents &&image_extents, ImageArea area,
                                bufferlist &&bl, int op_flags,
				const ZTracer::Trace &parent_trace) {
  ImageWriteRequest<I> req(*ictx, c, std::move(image_extents), area,
                           std::move(bl), op_flags, parent_trace);
  req.send();
}

template <typename I>
void ImageRequest<I>::aio_discard(I *ictx, AioCompletion *c,
                                  Extents &&image_extents, ImageArea area,
                                  uint32_t discard_granularity_bytes,
                                  const ZTracer::Trace &parent_trace) {
  ImageDiscardRequest<I> req(*ictx, c, std::move(image_extents), area,
                             discard_granularity_bytes, parent_trace);
  req.send();
}

template <typename I>
void ImageRequest<I>::aio_flush(I *ictx, AioCompletion *c,
                                FlushSource flush_source,
                                const ZTracer::Trace &parent_trace) {
  ImageFlushRequest<I> req(*ictx, c, flush_source, parent_trace);
  req.send();
}

template <typename I>
void ImageRequest<I>::aio_writesame(I *ictx, AioCompletion *c,
                                    Extents &&image_extents, ImageArea area,
                                    bufferlist &&bl, int op_flags,
				    const ZTracer::Trace &parent_trace) {
  ImageWriteSameRequest<I> req(*ictx, c, std::move(image_extents), area,
                               std::move(bl), op_flags, parent_trace);
  req.send();
}

template <typename I>
void ImageRequest<I>::aio_compare_and_write(I *ictx, AioCompletion *c,
                                            Extents &&image_extents,
                                            ImageArea area,
                                            bufferlist &&cmp_bl,
                                            bufferlist &&bl,
                                            uint64_t *mismatch_offset,
                                            int op_flags,
                                            const ZTracer::Trace &parent_trace) {
  ImageCompareAndWriteRequest<I> req(*ictx, c, std::move(image_extents), area,
                                     std::move(cmp_bl), std::move(bl),
                                     mismatch_offset, op_flags, parent_trace);
  req.send();
}

template <typename I>
void ImageRequest<I>::send() {
  I &image_ctx = this->m_image_ctx;
  ceph_assert(m_aio_comp->is_initialized(get_aio_type()));
  ceph_assert(m_aio_comp->is_started());

  CephContext *cct = image_ctx.cct;
  AioCompletion *aio_comp = this->m_aio_comp;
  ldout(cct, 20) << get_request_type() << ": ictx=" << &image_ctx << ", "
                 << "completion=" << aio_comp << dendl;

  update_timestamp();
  send_request();
}

template <typename I>
void ImageRequest<I>::update_timestamp() {
  bool modify = (get_aio_type() != AIO_TYPE_READ);
  uint64_t update_interval;
  if (modify) {
    update_interval = m_image_ctx.mtime_update_interval;
  } else {
    update_interval = m_image_ctx.atime_update_interval;
  }

  if (update_interval == 0) {
    return;
  }

  utime_t (I::*get_timestamp_fn)() const;
  void (I::*set_timestamp_fn)(utime_t);
  if (modify) {
    get_timestamp_fn = &I::get_modify_timestamp;
    set_timestamp_fn = &I::set_modify_timestamp;
  } else {
    get_timestamp_fn = &I::get_access_timestamp;
    set_timestamp_fn = &I::set_access_timestamp;
  }

  utime_t ts = ceph_clock_now();
  {
    std::shared_lock timestamp_locker{m_image_ctx.timestamp_lock};
    if(!should_update_timestamp(ts, std::invoke(get_timestamp_fn, m_image_ctx),
                                update_interval)) {
      return;
    }
  }

  {
    std::unique_lock timestamp_locker{m_image_ctx.timestamp_lock};
    bool update = should_update_timestamp(
      ts, std::invoke(get_timestamp_fn, m_image_ctx), update_interval);
    if (!update) {
      return;
    }

    std::invoke(set_timestamp_fn, m_image_ctx, ts);
  }

  // TODO we fire and forget this outside the IO path to prevent
  // potential race conditions with librbd client IO callbacks
  // between different threads (e.g. librados and object cacher)
  ldout(m_image_ctx.cct, 10) << get_request_type() << dendl;
  auto req = new C_UpdateTimestamp<I>(m_image_ctx, modify);
  req->send();
}

template <typename I>
ImageReadRequest<I>::ImageReadRequest(I &image_ctx, AioCompletion *aio_comp,
                                      Extents &&image_extents, ImageArea area,
                                      ReadResult &&read_result,
                                      IOContext io_context, int op_flags,
				      int read_flags,
                                      const ZTracer::Trace &parent_trace)
  : ImageRequest<I>(image_ctx, aio_comp, std::move(image_extents), area,
                    "read", parent_trace),
    m_io_context(io_context), m_op_flags(op_flags), m_read_flags(read_flags) {
  aio_comp->read_result = std::move(read_result);
}

template <typename I>
void ImageReadRequest<I>::send_request() {
  I &image_ctx = this->m_image_ctx;
  CephContext *cct = image_ctx.cct;

  auto &image_extents = this->m_image_extents;
  if (this->m_image_area == ImageArea::DATA &&
      image_ctx.cache && image_ctx.readahead_max_bytes > 0 &&
      !(m_op_flags & LIBRADOS_OP_FLAG_FADVISE_RANDOM)) {
    readahead(get_image_ctx(&image_ctx), image_extents, m_io_context);
  }

  // map image extents to object extents
  LightweightObjectExtents object_extents;
  uint64_t buffer_ofs = 0;
  for (auto &extent : image_extents) {
    if (extent.second == 0) {
      continue;
    }

    util::area_to_object_extents(&image_ctx, extent.first, extent.second,
                                 this->m_image_area, buffer_ofs,
                                 &object_extents);
    buffer_ofs += extent.second;
  }

  AioCompletion *aio_comp = this->m_aio_comp;
  aio_comp->read_result.set_image_extents(image_extents);

  // issue the requests
  aio_comp->set_request_count(object_extents.size());
  for (auto &oe : object_extents) {
    ldout(cct, 20) << data_object_name(&image_ctx, oe.object_no) << " "
                   << oe.offset << "~" << oe.length << " from "
                   << oe.buffer_extents << dendl;

    auto req_comp = new io::ReadResult::C_ObjectReadRequest(
      aio_comp, {{oe.offset, oe.length, std::move(oe.buffer_extents)}});
    auto req = ObjectDispatchSpec::create_read(
      &image_ctx, OBJECT_DISPATCH_LAYER_NONE, oe.object_no,
      &req_comp->extents, m_io_context, m_op_flags, m_read_flags,
      this->m_trace, nullptr, req_comp);
    req->send();
  }

  image_ctx.perfcounter->inc(l_librbd_rd);
  image_ctx.perfcounter->inc(l_librbd_rd_bytes, buffer_ofs);
}

template <typename I>
void AbstractImageWriteRequest<I>::send_request() {
  I &image_ctx = this->m_image_ctx;

  bool journaling = false;

  AioCompletion *aio_comp = this->m_aio_comp;
  {
    // prevent image size from changing between computing clip and recording
    // pending async operation
    std::shared_lock image_locker{image_ctx.image_lock};
    journaling = (image_ctx.journal != nullptr &&
                  image_ctx.journal->is_journal_appending());
  }

  uint64_t clip_len = 0;
  LightweightObjectExtents object_extents;
  for (auto &extent : this->m_image_extents) {
    if (extent.second == 0) {
      continue;
    }

    // map to object extents
    io::util::area_to_object_extents(&image_ctx, extent.first, extent.second,
                                     this->m_image_area, clip_len,
                                     &object_extents);
    clip_len += extent.second;
  }

  int ret = prune_object_extents(&object_extents);
  if (ret < 0) {
    aio_comp->fail(ret);
    return;
  }

  // reflect changes in object_extents back to m_image_extents
  if (ret == 1) {
    this->m_image_extents.clear();
    for (auto& object_extent : object_extents) {
      auto [image_extents, _] = io::util::object_to_area_extents(
          &image_ctx, object_extent.object_no,
          {{object_extent.offset, object_extent.length}});
      this->m_image_extents.insert(this->m_image_extents.end(),
                                   image_extents.begin(), image_extents.end());
    }
  }

  aio_comp->set_request_count(object_extents.size());
  if (!object_extents.empty()) {
    uint64_t journal_tid = 0;
    if (journaling) {
      // in-flight ops are flushed prior to closing the journal
      ceph_assert(image_ctx.journal != NULL);
      journal_tid = append_journal_event();
    }

    // it's very important that IOContext is captured here instead of
    // e.g. at the API layer so that an up-to-date snap context is used
    // when owning the exclusive lock
    send_object_requests(object_extents, image_ctx.get_data_io_context(),
                         journal_tid);
  }

  update_stats(clip_len);
}

template <typename I>
void AbstractImageWriteRequest<I>::send_object_requests(
    const LightweightObjectExtents &object_extents, IOContext io_context,
    uint64_t journal_tid) {
  I &image_ctx = this->m_image_ctx;
  CephContext *cct = image_ctx.cct;

  AioCompletion *aio_comp = this->m_aio_comp;
  bool single_extent = (object_extents.size() == 1);
  for (auto& oe : object_extents) {
    ldout(cct, 20) << data_object_name(&image_ctx, oe.object_no) << " "
                   << oe.offset << "~" << oe.length << " from "
                   << oe.buffer_extents << dendl;
    C_AioRequest *req_comp = new C_AioRequest(aio_comp);
    auto request = create_object_request(oe, io_context, journal_tid,
                                         single_extent, req_comp);
    request->send();
  }
}

template <typename I>
void ImageWriteRequest<I>::assemble_extent(
    const LightweightObjectExtent &object_extent, bufferlist *bl) {
  for (auto q = object_extent.buffer_extents.begin();
       q != object_extent.buffer_extents.end(); ++q) {
    bufferlist sub_bl;
    sub_bl.substr_of(m_bl, q->first, q->second);
    bl->claim_append(sub_bl);
  }
}

template <typename I>
uint64_t ImageWriteRequest<I>::append_journal_event() {
  I &image_ctx = this->m_image_ctx;

  ceph_assert(!this->m_image_extents.empty());
  return image_ctx.journal->append_write_event(
    this->m_image_extents, m_bl, false);
}

template <typename I>
ObjectDispatchSpec *ImageWriteRequest<I>::create_object_request(
    const LightweightObjectExtent &object_extent, IOContext io_context,
    uint64_t journal_tid, bool single_extent, Context *on_finish) {
  I &image_ctx = this->m_image_ctx;

  bufferlist bl;
  if (single_extent && object_extent.buffer_extents.size() == 1 &&
      m_bl.length() == object_extent.length) {
    // optimization for single object/buffer extent writes
    bl = std::move(m_bl);
  } else {
    assemble_extent(object_extent, &bl);
  }

  auto req = ObjectDispatchSpec::create_write(
    &image_ctx, OBJECT_DISPATCH_LAYER_NONE, object_extent.object_no,
    object_extent.offset, std::move(bl), io_context, m_op_flags, 0,
    std::nullopt, journal_tid, this->m_trace, on_finish);
  return req;
}

template <typename I>
void ImageWriteRequest<I>::update_stats(size_t length) {
  I &image_ctx = this->m_image_ctx;
  image_ctx.perfcounter->inc(l_librbd_wr);
  image_ctx.perfcounter->inc(l_librbd_wr_bytes, length);
}

template <typename I>
uint64_t ImageDiscardRequest<I>::append_journal_event() {
  I &image_ctx = this->m_image_ctx;

  ceph_assert(!this->m_image_extents.empty());
  return image_ctx.journal->append_discard_event(
    this->m_image_extents, m_discard_granularity_bytes, false);
}

template <typename I>
ObjectDispatchSpec *ImageDiscardRequest<I>::create_object_request(
    const LightweightObjectExtent &object_extent, IOContext io_context,
    uint64_t journal_tid, bool single_extent, Context *on_finish) {
  I &image_ctx = this->m_image_ctx;
  auto req = ObjectDispatchSpec::create_discard(
    &image_ctx, OBJECT_DISPATCH_LAYER_NONE, object_extent.object_no,
    object_extent.offset, object_extent.length, io_context,
    OBJECT_DISCARD_FLAG_DISABLE_CLONE_REMOVE, journal_tid, this->m_trace,
    on_finish);
  return req;
}

template <typename I>
void ImageDiscardRequest<I>::update_stats(size_t length) {
  I &image_ctx = this->m_image_ctx;
  image_ctx.perfcounter->inc(l_librbd_discard);
  image_ctx.perfcounter->inc(l_librbd_discard_bytes, length);
}

template <typename I>
int ImageDiscardRequest<I>::prune_object_extents(
    LightweightObjectExtents* object_extents) const {
  if (m_discard_granularity_bytes == 0) {
    return 0;
  }

  // Align the range to discard_granularity_bytes boundary and skip
  // and discards that are too small to free up any space.
  //
  // discard_granularity_bytes >= object_size && tail truncation
  // is a special case for filestore
  bool prune_required = false;
  bool length_modified = false;
  auto object_size = this->m_image_ctx.layout.object_size;
  auto discard_granularity_bytes = std::min(m_discard_granularity_bytes,
                                            object_size);
  auto xform_lambda =
    [discard_granularity_bytes, object_size, &prune_required, &length_modified]
    (LightweightObjectExtent& object_extent) {
      auto& offset = object_extent.offset;
      auto& length = object_extent.length;
      auto next_offset = offset + length;

      if ((discard_granularity_bytes < object_size) ||
          (next_offset < object_size)) {
        offset = p2roundup<uint64_t>(offset, discard_granularity_bytes);
        next_offset = p2align<uint64_t>(next_offset, discard_granularity_bytes);
        if (offset >= next_offset) {
          prune_required = true;
          length = 0;
        } else {
          auto new_length = next_offset - offset;
          if (length != new_length) {
            length_modified = true;
            length = new_length;
          }
        }
      }
    };
  std::for_each(object_extents->begin(), object_extents->end(),
                xform_lambda);

  if (prune_required) {
    // one or more object extents were skipped
    auto remove_lambda =
      [](const LightweightObjectExtent& object_extent) {
        return (object_extent.length == 0);
      };
    object_extents->erase(
      std::remove_if(object_extents->begin(), object_extents->end(),
                     remove_lambda),
      object_extents->end());
  }

  // object extents were modified, image extents needs updating
  if (length_modified || prune_required) {
    return 1;
  }

  return 0;
}

template <typename I>
void ImageFlushRequest<I>::send_request() {
  I &image_ctx = this->m_image_ctx;

  bool journaling = false;
  {
    std::shared_lock image_locker{image_ctx.image_lock};
    journaling = (m_flush_source == FLUSH_SOURCE_USER &&
                  image_ctx.journal != nullptr &&
                  image_ctx.journal->is_journal_appending());
  }

  AioCompletion *aio_comp = this->m_aio_comp;
  aio_comp->set_request_count(1);

  Context *ctx = new C_AioRequest(aio_comp);

  // ensure no locks are held when flush is complete
  ctx = librbd::util::create_async_context_callback(image_ctx, ctx);

  uint64_t journal_tid = 0;
  if (journaling) {
    // in-flight ops are flushed prior to closing the journal
    ceph_assert(image_ctx.journal != NULL);
    journal_tid = image_ctx.journal->append_io_event(
      journal::EventEntry(journal::AioFlushEvent()), 0, 0, false, 0);
    image_ctx.journal->user_flushed();
  }

  auto object_dispatch_spec = ObjectDispatchSpec::create_flush(
    &image_ctx, OBJECT_DISPATCH_LAYER_NONE, m_flush_source, journal_tid,
    this->m_trace, ctx);
  ctx = new LambdaContext([object_dispatch_spec](int r) {
      object_dispatch_spec->send();
    });

  // ensure all in-flight IOs are settled if non-user flush request
  if (m_flush_source == FLUSH_SOURCE_WRITEBACK) {
    ctx->complete(0);
  } else {
    aio_comp->async_op.flush(ctx);
  }

  // might be flushing during image shutdown
  if (image_ctx.perfcounter != nullptr) {
    image_ctx.perfcounter->inc(l_librbd_flush);
  }
}

template <typename I>
uint64_t ImageWriteSameRequest<I>::append_journal_event() {
  I &image_ctx = this->m_image_ctx;

  ceph_assert(!this->m_image_extents.empty());
  return image_ctx.journal->append_write_same_event(
    this->m_image_extents, m_data_bl, false);
}

template <typename I>
ObjectDispatchSpec *ImageWriteSameRequest<I>::create_object_request(
    const LightweightObjectExtent &object_extent, IOContext io_context,
    uint64_t journal_tid, bool single_extent, Context *on_finish) {
  I &image_ctx = this->m_image_ctx;

  bufferlist bl;
  ObjectDispatchSpec *req;

  if (util::assemble_write_same_extent(object_extent, m_data_bl, &bl, false)) {
    auto buffer_extents{object_extent.buffer_extents};

    req = ObjectDispatchSpec::create_write_same(
      &image_ctx, OBJECT_DISPATCH_LAYER_NONE, object_extent.object_no,
      object_extent.offset, object_extent.length, std::move(buffer_extents),
      std::move(bl), io_context, m_op_flags, journal_tid,
      this->m_trace, on_finish);
    return req;
  }
  req = ObjectDispatchSpec::create_write(
    &image_ctx, OBJECT_DISPATCH_LAYER_NONE, object_extent.object_no,
    object_extent.offset, std::move(bl), io_context, m_op_flags, 0,
    std::nullopt, journal_tid, this->m_trace, on_finish);
  return req;
}

template <typename I>
void ImageWriteSameRequest<I>::update_stats(size_t length) {
  I &image_ctx = this->m_image_ctx;
  image_ctx.perfcounter->inc(l_librbd_ws);
  image_ctx.perfcounter->inc(l_librbd_ws_bytes, length);
}

template <typename I>
uint64_t ImageCompareAndWriteRequest<I>::append_journal_event() {
  I &image_ctx = this->m_image_ctx;

  uint64_t tid = 0;
  ceph_assert(this->m_image_extents.size() == 1);
  auto &extent = this->m_image_extents.front();
  tid = image_ctx.journal->append_compare_and_write_event(extent.first,
                                                          extent.second,
                                                          m_cmp_bl,
                                                          m_bl,
                                                          false);

  return tid;
}

template <typename I>
void ImageCompareAndWriteRequest<I>::assemble_extent(
    const LightweightObjectExtent &object_extent, bufferlist *bl,
    bufferlist *cmp_bl) {
  for (auto q = object_extent.buffer_extents.begin();
       q != object_extent.buffer_extents.end(); ++q) {
    bufferlist sub_bl;
    sub_bl.substr_of(m_bl, q->first, q->second);
    bl->claim_append(sub_bl);

    bufferlist sub_cmp_bl;
    sub_cmp_bl.substr_of(m_cmp_bl, q->first, q->second);
    cmp_bl->claim_append(sub_cmp_bl);
  }
}

template <typename I>
ObjectDispatchSpec *ImageCompareAndWriteRequest<I>::create_object_request(
    const LightweightObjectExtent &object_extent, IOContext io_context,
    uint64_t journal_tid, bool single_extent, Context *on_finish) {
  I &image_ctx = this->m_image_ctx;

  bufferlist bl;
  bufferlist cmp_bl;
  assemble_extent(object_extent, &bl, &cmp_bl);
  auto req = ObjectDispatchSpec::create_compare_and_write(
    &image_ctx, OBJECT_DISPATCH_LAYER_NONE, object_extent.object_no,
    object_extent.offset, std::move(cmp_bl), std::move(bl), io_context,
    m_mismatch_offset, m_op_flags, journal_tid, this->m_trace, on_finish);
  return req;
}

template <typename I>
void ImageCompareAndWriteRequest<I>::update_stats(size_t length) {
  I &image_ctx = this->m_image_ctx;
  image_ctx.perfcounter->inc(l_librbd_cmp);
  image_ctx.perfcounter->inc(l_librbd_cmp_bytes, length);
}

template <typename I>
int ImageCompareAndWriteRequest<I>::prune_object_extents(
    LightweightObjectExtents* object_extents) const {
  if (object_extents->size() > 1)
    return -EINVAL;

  I &image_ctx = this->m_image_ctx;
  uint64_t su = image_ctx.layout.stripe_unit;
  auto& object_extent = object_extents->front();
  if (su == 0 || (object_extent.offset % su + object_extent.length > su))
    return -EINVAL;

  return 0;
}

template <typename I>
ImageListSnapsRequest<I>::ImageListSnapsRequest(
    I& image_ctx, AioCompletion* aio_comp, Extents&& image_extents,
    ImageArea area, SnapIds&& snap_ids, int list_snaps_flags,
    SnapshotDelta* snapshot_delta, const ZTracer::Trace& parent_trace)
  : ImageRequest<I>(image_ctx, aio_comp, std::move(image_extents), area,
                    "list-snaps", parent_trace),
    m_snap_ids(std::move(snap_ids)), m_list_snaps_flags(list_snaps_flags),
    m_snapshot_delta(snapshot_delta) {
}

template <typename I>
void ImageListSnapsRequest<I>::send_request() {
  I &image_ctx = this->m_image_ctx;
  CephContext *cct = image_ctx.cct;

  // map image extents to object extents
  auto &image_extents = this->m_image_extents;
  std::map<uint64_t, Extents> object_number_extents;
  for (auto& image_extent : image_extents) {
    if (image_extent.second == 0) {
      continue;
    }

    striper::LightweightObjectExtents object_extents;
    io::util::area_to_object_extents(&image_ctx, image_extent.first,
                                     image_extent.second, this->m_image_area, 0,
                                     &object_extents);
    for (auto& object_extent : object_extents) {
      object_number_extents[object_extent.object_no].emplace_back(
        object_extent.offset, object_extent.length);
    }
  }

  // reassemble the deltas back into image-extents when complete
  auto aio_comp = this->m_aio_comp;
  aio_comp->set_request_count(1);
  auto assemble_ctx = new C_AssembleSnapshotDeltas<I>(
    &image_ctx, aio_comp, m_snapshot_delta);
  auto sub_aio_comp = AioCompletion::create_and_start<
    Context, &Context::complete>(assemble_ctx, get_image_ctx(&image_ctx),
                                 AIO_TYPE_GENERIC);

  // issue the requests
  sub_aio_comp->set_request_count(object_number_extents.size());
  for (auto& oe : object_number_extents) {
    ldout(cct, 20) << data_object_name(&image_ctx, oe.first) << " "
                   << oe.second << dendl;
    auto ctx = new C_AioRequest(sub_aio_comp);
    auto req = ObjectDispatchSpec::create_list_snaps(
      &image_ctx, OBJECT_DISPATCH_LAYER_NONE, oe.first, std::move(oe.second),
      SnapIds{m_snap_ids}, m_list_snaps_flags, this->m_trace,
      assemble_ctx->get_snapshot_delta(oe.first), ctx);
    req->send();
  }
}

} // namespace io
} // namespace librbd

template class librbd::io::ImageRequest<librbd::ImageCtx>;
template class librbd::io::ImageReadRequest<librbd::ImageCtx>;
template class librbd::io::AbstractImageWriteRequest<librbd::ImageCtx>;
template class librbd::io::ImageWriteRequest<librbd::ImageCtx>;
template class librbd::io::ImageDiscardRequest<librbd::ImageCtx>;
template class librbd::io::ImageFlushRequest<librbd::ImageCtx>;
template class librbd::io::ImageWriteSameRequest<librbd::ImageCtx>;
template class librbd::io::ImageCompareAndWriteRequest<librbd::ImageCtx>;
template class librbd::io::ImageListSnapsRequest<librbd::ImageCtx>;
