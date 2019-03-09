// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/io/ImageRequest.h"
#include "librbd/ImageCtx.h"
#include "librbd/internal.h"
#include "librbd/Journal.h"
#include "librbd/Types.h"
#include "librbd/Utils.h"
#include "librbd/cache/ImageCache.h"
#include "librbd/io/AioCompletion.h"
#include "librbd/io/AsyncOperation.h"
#include "librbd/io/ObjectDispatchInterface.h"
#include "librbd/io/ObjectDispatchSpec.h"
#include "librbd/io/ObjectDispatcher.h"
#include "librbd/io/Utils.h"
#include "librbd/journal/Types.h"
#include "include/rados/librados.hpp"
#include "common/perf_counters.h"
#include "common/WorkQueue.h"
#include "osdc/Striper.h"
#include <algorithm>
#include <functional>

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::io::ImageRequest: " << this \
                           << " " << __func__ << ": "

namespace librbd {
namespace io {

using librbd::util::get_image_ctx;

namespace {

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

template <typename I>
void ImageRequest<I>::aio_read(I *ictx, AioCompletion *c,
                               Extents &&image_extents,
                               ReadResult &&read_result, int op_flags,
			       const ZTracer::Trace &parent_trace) {
  ImageReadRequest<I> req(*ictx, c, std::move(image_extents),
                          std::move(read_result), op_flags, parent_trace);
  req.send();
}

template <typename I>
void ImageRequest<I>::aio_write(I *ictx, AioCompletion *c,
                                Extents &&image_extents, bufferlist &&bl,
                                int op_flags,
				const ZTracer::Trace &parent_trace) {
  ImageWriteRequest<I> req(*ictx, c, std::move(image_extents), std::move(bl),
                           op_flags, parent_trace);
  req.send();
}

template <typename I>
void ImageRequest<I>::aio_discard(I *ictx, AioCompletion *c,
                                  Extents &&image_extents,
                                  uint32_t discard_granularity_bytes,
				  const ZTracer::Trace &parent_trace) {
  ImageDiscardRequest<I> req(*ictx, c, std::move(image_extents),
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
                                    Extents &&image_extents,
                                    bufferlist &&bl, int op_flags,
				    const ZTracer::Trace &parent_trace) {
  ImageWriteSameRequest<I> req(*ictx, c, std::move(image_extents),
                               std::move(bl), op_flags, parent_trace);
  req.send();
}

template <typename I>
void ImageRequest<I>::aio_compare_and_write(I *ictx, AioCompletion *c,
                                            Extents &&image_extents,
                                            bufferlist &&cmp_bl,
                                            bufferlist &&bl,
                                            uint64_t *mismatch_offset,
                                            int op_flags,
                                            const ZTracer::Trace &parent_trace) {
  ImageCompareAndWriteRequest<I> req(*ictx, c, std::move(image_extents),
                                     std::move(cmp_bl), std::move(bl),
                                     mismatch_offset, op_flags, parent_trace);
  req.send();
}


template <typename I>
void ImageRequest<I>::send() {
  I &image_ctx = this->m_image_ctx;
  ceph_assert(m_aio_comp->is_initialized(get_aio_type()));
  ceph_assert(m_aio_comp->is_started() ^ (get_aio_type() == AIO_TYPE_FLUSH));

  CephContext *cct = image_ctx.cct;
  AioCompletion *aio_comp = this->m_aio_comp;
  ldout(cct, 20) << get_request_type() << ": ictx=" << &image_ctx << ", "
                 << "completion=" << aio_comp << dendl;

  aio_comp->get();
  int r = clip_request();
  if (r < 0) {
    m_aio_comp->fail(r);
    return;
  }

  if (m_bypass_image_cache || m_image_ctx.image_cache == nullptr) {
    update_timestamp();
    send_request();
  } else {
    send_image_cache_request();
  }
}

template <typename I>
int ImageRequest<I>::clip_request() {
  RWLock::RLocker snap_locker(m_image_ctx.snap_lock);
  for (auto &image_extent : m_image_extents) {
    auto clip_len = image_extent.second;
    int r = clip_io(get_image_ctx(&m_image_ctx), image_extent.first, &clip_len);
    if (r < 0) {
      return r;
    }

    image_extent.second = clip_len;
  }
  return 0;
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
    RWLock::RLocker timestamp_locker(m_image_ctx.timestamp_lock);
    if(!should_update_timestamp(ts, std::invoke(get_timestamp_fn, m_image_ctx),
                                update_interval)) {
      return;
    }
  }

  {
    RWLock::WLocker timestamp_locker(m_image_ctx.timestamp_lock);
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
                                      Extents &&image_extents,
                                      ReadResult &&read_result, int op_flags,
				      const ZTracer::Trace &parent_trace)
  : ImageRequest<I>(image_ctx, aio_comp, std::move(image_extents), "read",
		    parent_trace),
    m_op_flags(op_flags) {
  aio_comp->read_result = std::move(read_result);
}

template <typename I>
int ImageReadRequest<I>::clip_request() {
  int r = ImageRequest<I>::clip_request();
  if (r < 0) {
    return r;
  }

  uint64_t buffer_length = 0;
  auto &image_extents = this->m_image_extents;
  for (auto &image_extent : image_extents) {
    buffer_length += image_extent.second;
  }
  this->m_aio_comp->read_result.set_clip_length(buffer_length);
  return 0;
}

template <typename I>
void ImageReadRequest<I>::send_request() {
  I &image_ctx = this->m_image_ctx;
  CephContext *cct = image_ctx.cct;

  auto &image_extents = this->m_image_extents;
  if (image_ctx.cache && image_ctx.readahead_max_bytes > 0 &&
      !(m_op_flags & LIBRADOS_OP_FLAG_FADVISE_RANDOM)) {
    readahead(get_image_ctx(&image_ctx), image_extents);
  }

  AioCompletion *aio_comp = this->m_aio_comp;
  librados::snap_t snap_id;
  map<object_t,vector<ObjectExtent> > object_extents;
  uint64_t buffer_ofs = 0;
  {
    // prevent image size from changing between computing clip and recording
    // pending async operation
    RWLock::RLocker snap_locker(image_ctx.snap_lock);
    snap_id = image_ctx.snap_id;

    // map image extents to object extents
    for (auto &extent : image_extents) {
      if (extent.second == 0) {
        continue;
      }

      Striper::file_to_extents(cct, image_ctx.format_string, &image_ctx.layout,
                               extent.first, extent.second, 0, object_extents,
                               buffer_ofs);
      buffer_ofs += extent.second;
    }
  }

  // pre-calculate the expected number of read requests
  uint32_t request_count = 0;
  for (auto &object_extent : object_extents) {
    request_count += object_extent.second.size();
  }
  aio_comp->set_request_count(request_count);

  // issue the requests
  for (auto &object_extent : object_extents) {
    for (auto &extent : object_extent.second) {
      ldout(cct, 20) << "oid " << extent.oid << " " << extent.offset << "~"
                     << extent.length << " from " << extent.buffer_extents
                     << dendl;

      auto req_comp = new io::ReadResult::C_ObjectReadRequest(
        aio_comp, extent.offset, extent.length,
        std::move(extent.buffer_extents));
      auto req = ObjectDispatchSpec::create_read(
        &image_ctx, OBJECT_DISPATCH_LAYER_NONE, extent.oid.name,
        extent.objectno, extent.offset, extent.length, snap_id, m_op_flags,
        this->m_trace, &req_comp->bl, &req_comp->extent_map, req_comp);
      req->send();
    }
  }

  aio_comp->put();

  image_ctx.perfcounter->inc(l_librbd_rd);
  image_ctx.perfcounter->inc(l_librbd_rd_bytes, buffer_ofs);
}

template <typename I>
void ImageReadRequest<I>::send_image_cache_request() {
  I &image_ctx = this->m_image_ctx;
  ceph_assert(image_ctx.image_cache != nullptr);

  AioCompletion *aio_comp = this->m_aio_comp;
  aio_comp->set_request_count(1);

  auto *req_comp = new io::ReadResult::C_ImageReadRequest(
    aio_comp, this->m_image_extents);
  image_ctx.image_cache->aio_read(std::move(this->m_image_extents),
                                  &req_comp->bl, m_op_flags,
                                  req_comp);
}

template <typename I>
void AbstractImageWriteRequest<I>::send_request() {
  I &image_ctx = this->m_image_ctx;
  CephContext *cct = image_ctx.cct;

  RWLock::RLocker md_locker(image_ctx.md_lock);

  bool journaling = false;

  AioCompletion *aio_comp = this->m_aio_comp;
  uint64_t clip_len = 0;
  ObjectExtents object_extents;
  ::SnapContext snapc;
  {
    // prevent image size from changing between computing clip and recording
    // pending async operation
    RWLock::RLocker snap_locker(image_ctx.snap_lock);
    if (image_ctx.snap_id != CEPH_NOSNAP || image_ctx.read_only) {
      aio_comp->fail(-EROFS);
      return;
    }

    for (auto &extent : this->m_image_extents) {
      if (extent.second == 0) {
        continue;
      }

      // map to object extents
      Striper::file_to_extents(cct, image_ctx.format_string, &image_ctx.layout,
                               extent.first, extent.second, 0, object_extents);
      clip_len += extent.second;
    }

    snapc = image_ctx.snapc;
    journaling = (image_ctx.journal != nullptr &&
                  image_ctx.journal->is_journal_appending());
  }

  int ret = prune_object_extents(&object_extents);
  if (ret < 0) {
    aio_comp->fail(ret);
    return;
  }

  if (!object_extents.empty()) {
    uint64_t journal_tid = 0;
    if (journaling) {
      // in-flight ops are flushed prior to closing the journal
      ceph_assert(image_ctx.journal != NULL);
      journal_tid = append_journal_event(m_synchronous);
    }

    aio_comp->set_request_count(object_extents.size());
    send_object_requests(object_extents, snapc, journal_tid);
  } else {
    // no IO to perform -- fire completion
    aio_comp->unblock();
  }

  update_stats(clip_len);
  aio_comp->put();
}

template <typename I>
void AbstractImageWriteRequest<I>::send_object_requests(
    const ObjectExtents &object_extents, const ::SnapContext &snapc,
    uint64_t journal_tid) {
  I &image_ctx = this->m_image_ctx;
  CephContext *cct = image_ctx.cct;

  AioCompletion *aio_comp = this->m_aio_comp;
  for (ObjectExtents::const_iterator p = object_extents.begin();
       p != object_extents.end(); ++p) {
    ldout(cct, 20) << "oid " << p->oid << " " << p->offset << "~" << p->length
                   << " from " << p->buffer_extents << dendl;
    C_AioRequest *req_comp = new C_AioRequest(aio_comp);
    auto request = create_object_request(*p, snapc, journal_tid, req_comp);

    // if journaling, stash the request for later; otherwise send
    if (request != NULL) {
      request->send();
    }
  }
}

template <typename I>
void ImageWriteRequest<I>::assemble_extent(const ObjectExtent &object_extent,
                                           bufferlist *bl) {
  for (auto q = object_extent.buffer_extents.begin();
       q != object_extent.buffer_extents.end(); ++q) {
    bufferlist sub_bl;
    sub_bl.substr_of(m_bl, q->first, q->second);
    bl->claim_append(sub_bl);
  }
}

template <typename I>
uint64_t ImageWriteRequest<I>::append_journal_event(bool synchronous) {
  I &image_ctx = this->m_image_ctx;

  uint64_t tid = 0;
  uint64_t buffer_offset = 0;
  ceph_assert(!this->m_image_extents.empty());
  for (auto &extent : this->m_image_extents) {
    bufferlist sub_bl;
    sub_bl.substr_of(m_bl, buffer_offset, extent.second);
    buffer_offset += extent.second;

    tid = image_ctx.journal->append_write_event(extent.first, extent.second,
                                                sub_bl, synchronous);
  }

  return tid;
}

template <typename I>
void ImageWriteRequest<I>::send_image_cache_request() {
  I &image_ctx = this->m_image_ctx;
  ceph_assert(image_ctx.image_cache != nullptr);

  AioCompletion *aio_comp = this->m_aio_comp;
  aio_comp->set_request_count(1);
  C_AioRequest *req_comp = new C_AioRequest(aio_comp);
  image_ctx.image_cache->aio_write(std::move(this->m_image_extents),
                                   std::move(m_bl), m_op_flags, req_comp);
}

template <typename I>
ObjectDispatchSpec *ImageWriteRequest<I>::create_object_request(
    const ObjectExtent &object_extent, const ::SnapContext &snapc,
    uint64_t journal_tid, Context *on_finish) {
  I &image_ctx = this->m_image_ctx;

  bufferlist bl;
  assemble_extent(object_extent, &bl);
  auto req = ObjectDispatchSpec::create_write(
    &image_ctx, OBJECT_DISPATCH_LAYER_NONE, object_extent.oid.name,
    object_extent.objectno, object_extent.offset, std::move(bl), snapc,
    m_op_flags, journal_tid, this->m_trace, on_finish);
  return req;
}

template <typename I>
void ImageWriteRequest<I>::update_stats(size_t length) {
  I &image_ctx = this->m_image_ctx;
  image_ctx.perfcounter->inc(l_librbd_wr);
  image_ctx.perfcounter->inc(l_librbd_wr_bytes, length);
}

template <typename I>
uint64_t ImageDiscardRequest<I>::append_journal_event(bool synchronous) {
  I &image_ctx = this->m_image_ctx;

  uint64_t tid = 0;
  ceph_assert(!this->m_image_extents.empty());
  for (auto &extent : this->m_image_extents) {
    journal::EventEntry event_entry(
      journal::AioDiscardEvent(extent.first,
                               extent.second,
                               this->m_discard_granularity_bytes));
    tid = image_ctx.journal->append_io_event(std::move(event_entry),
                                             extent.first, extent.second,
                                             synchronous, 0);
  }

  return tid;
}

template <typename I>
void ImageDiscardRequest<I>::send_image_cache_request() {
  I &image_ctx = this->m_image_ctx;
  ceph_assert(image_ctx.image_cache != nullptr);

  AioCompletion *aio_comp = this->m_aio_comp;
  aio_comp->set_request_count(this->m_image_extents.size());
  for (auto &extent : this->m_image_extents) {
    C_AioRequest *req_comp = new C_AioRequest(aio_comp);
    image_ctx.image_cache->aio_discard(extent.first, extent.second,
                                       this->m_discard_granularity_bytes,
                                       req_comp);
  }
}

template <typename I>
ObjectDispatchSpec *ImageDiscardRequest<I>::create_object_request(
    const ObjectExtent &object_extent, const ::SnapContext &snapc,
    uint64_t journal_tid, Context *on_finish) {
  I &image_ctx = this->m_image_ctx;
  auto req = ObjectDispatchSpec::create_discard(
    &image_ctx, OBJECT_DISPATCH_LAYER_NONE, object_extent.oid.name,
    object_extent.objectno, object_extent.offset, object_extent.length, snapc,
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
    ObjectExtents* object_extents) const {
  if (m_discard_granularity_bytes == 0) {
    return 0;
  }

  // Align the range to discard_granularity_bytes boundary and skip
  // and discards that are too small to free up any space.
  //
  // discard_granularity_bytes >= object_size && tail truncation
  // is a special case for filestore
  bool prune_required = false;
  auto object_size = this->m_image_ctx.layout.object_size;
  auto discard_granularity_bytes = std::min(m_discard_granularity_bytes,
                                            object_size);
  auto xform_lambda =
    [discard_granularity_bytes, object_size, &prune_required]
    (ObjectExtent& object_extent) {
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
          length = next_offset - offset;
        }
      }
    };
  std::for_each(object_extents->begin(), object_extents->end(),
                xform_lambda);

  if (prune_required) {
    // one or more object extents were skipped
    auto remove_lambda =
      [](const ObjectExtent& object_extent) {
        return (object_extent.length == 0);
      };
    object_extents->erase(
      std::remove_if(object_extents->begin(), object_extents->end(),
                     remove_lambda),
      object_extents->end());
  }
  return 0;
}

template <typename I>
void ImageFlushRequest<I>::send_request() {
  I &image_ctx = this->m_image_ctx;

  bool journaling = false;
  {
    RWLock::RLocker snap_locker(image_ctx.snap_lock);
    journaling = (m_flush_source == FLUSH_SOURCE_USER &&
                  image_ctx.journal != nullptr &&
                  image_ctx.journal->is_journal_appending());
  }

  AioCompletion *aio_comp = this->m_aio_comp;
  aio_comp->set_request_count(1);

  Context *ctx = new C_AioRequest(aio_comp);

  // ensure no locks are held when flush is complete
  ctx = librbd::util::create_async_context_callback(image_ctx, ctx);

  if (journaling) {
    // in-flight ops are flushed prior to closing the journal
    uint64_t journal_tid = image_ctx.journal->append_io_event(
      journal::EventEntry(journal::AioFlushEvent()), 0, 0, false, 0);

    ctx = new FunctionContext(
      [&image_ctx, journal_tid, ctx](int r) {
        image_ctx.journal->commit_io_event(journal_tid, r);
        ctx->complete(r);
      });
    ctx = new FunctionContext(
      [&image_ctx, journal_tid, ctx](int r) {
        image_ctx.journal->flush_event(journal_tid, ctx);
      });
  } else {
    // flush rbd cache only when journaling is not enabled
    auto object_dispatch_spec = ObjectDispatchSpec::create_flush(
      &image_ctx, OBJECT_DISPATCH_LAYER_NONE, m_flush_source, this->m_trace,
      ctx);
    ctx = new FunctionContext([object_dispatch_spec](int r) {
        object_dispatch_spec->send();
      });
  }

  // ensure all in-flight IOs are settled if non-user flush request
  image_ctx.flush_async_operations(ctx);
  aio_comp->start_op(true);
  aio_comp->put();

  // might be flushing during image shutdown
  if (image_ctx.perfcounter != nullptr) {
    image_ctx.perfcounter->inc(l_librbd_flush);
  }
}

template <typename I>
void ImageFlushRequest<I>::send_image_cache_request() {
  I &image_ctx = this->m_image_ctx;
  ceph_assert(image_ctx.image_cache != nullptr);

  AioCompletion *aio_comp = this->m_aio_comp;
  aio_comp->set_request_count(1);
  C_AioRequest *req_comp = new C_AioRequest(aio_comp);
  image_ctx.image_cache->aio_flush(req_comp);
}

template <typename I>
uint64_t ImageWriteSameRequest<I>::append_journal_event(bool synchronous) {
  I &image_ctx = this->m_image_ctx;

  uint64_t tid = 0;
  ceph_assert(!this->m_image_extents.empty());
  for (auto &extent : this->m_image_extents) {
    journal::EventEntry event_entry(journal::AioWriteSameEvent(extent.first,
                                                               extent.second,
                                                               m_data_bl));
    tid = image_ctx.journal->append_io_event(std::move(event_entry),
                                             extent.first, extent.second,
                                             synchronous, 0);
  }

  return tid;
}

template <typename I>
void ImageWriteSameRequest<I>::send_image_cache_request() {
  I &image_ctx = this->m_image_ctx;
  ceph_assert(image_ctx.image_cache != nullptr);

  AioCompletion *aio_comp = this->m_aio_comp;
  aio_comp->set_request_count(this->m_image_extents.size());
  for (auto &extent : this->m_image_extents) {
    C_AioRequest *req_comp = new C_AioRequest(aio_comp);
    image_ctx.image_cache->aio_writesame(extent.first, extent.second,
                                         std::move(m_data_bl), m_op_flags,
                                         req_comp);
  }
}

template <typename I>
ObjectDispatchSpec *ImageWriteSameRequest<I>::create_object_request(
    const ObjectExtent &object_extent, const ::SnapContext &snapc,
    uint64_t journal_tid, Context *on_finish) {
  I &image_ctx = this->m_image_ctx;

  bufferlist bl;
  ObjectDispatchSpec *req;

  if (util::assemble_write_same_extent(object_extent, m_data_bl, &bl, false)) {
    Extents buffer_extents{object_extent.buffer_extents};

    req = ObjectDispatchSpec::create_write_same(
      &image_ctx, OBJECT_DISPATCH_LAYER_NONE, object_extent.oid.name,
      object_extent.objectno, object_extent.offset, object_extent.length,
      std::move(buffer_extents), std::move(bl), snapc, m_op_flags, journal_tid,
      this->m_trace, on_finish);
    return req;
  }
  req = ObjectDispatchSpec::create_write(
    &image_ctx, OBJECT_DISPATCH_LAYER_NONE, object_extent.oid.name,
    object_extent.objectno, object_extent.offset, std::move(bl), snapc,
    m_op_flags, journal_tid, this->m_trace, on_finish);
  return req;
}

template <typename I>
void ImageWriteSameRequest<I>::update_stats(size_t length) {
  I &image_ctx = this->m_image_ctx;
  image_ctx.perfcounter->inc(l_librbd_ws);
  image_ctx.perfcounter->inc(l_librbd_ws_bytes, length);
}

template <typename I>
uint64_t ImageCompareAndWriteRequest<I>::append_journal_event(
    bool synchronous) {
  I &image_ctx = this->m_image_ctx;

  uint64_t tid = 0;
  ceph_assert(this->m_image_extents.size() == 1);
  auto &extent = this->m_image_extents.front();
  journal::EventEntry event_entry(
    journal::AioCompareAndWriteEvent(extent.first, extent.second, m_cmp_bl,
                                     m_bl));
  tid = image_ctx.journal->append_io_event(std::move(event_entry),
                                           extent.first, extent.second,
                                           synchronous, -EILSEQ);

  return tid;
}

template <typename I>
void ImageCompareAndWriteRequest<I>::assemble_extent(
  const ObjectExtent &object_extent, bufferlist *bl) {
  for (auto q = object_extent.buffer_extents.begin();
       q != object_extent.buffer_extents.end(); ++q) {
    bufferlist sub_bl;
    sub_bl.substr_of(m_bl, q->first, q->second);
    bl->claim_append(sub_bl);
  }
}

template <typename I>
void ImageCompareAndWriteRequest<I>::send_image_cache_request() {
  I &image_ctx = this->m_image_ctx;
  ceph_assert(image_ctx.image_cache != nullptr);

  AioCompletion *aio_comp = this->m_aio_comp;
  aio_comp->set_request_count(1);
  C_AioRequest *req_comp = new C_AioRequest(aio_comp);
  image_ctx.image_cache->aio_compare_and_write(
    std::move(this->m_image_extents), std::move(m_cmp_bl), std::move(m_bl),
    m_mismatch_offset, m_op_flags, req_comp);
}

template <typename I>
ObjectDispatchSpec *ImageCompareAndWriteRequest<I>::create_object_request(
    const ObjectExtent &object_extent,
    const ::SnapContext &snapc,
    uint64_t journal_tid, Context *on_finish) {
  I &image_ctx = this->m_image_ctx;

  // NOTE: safe to move m_cmp_bl since we only support this op against
  // a single object
  bufferlist bl;
  assemble_extent(object_extent, &bl);
  auto req = ObjectDispatchSpec::create_compare_and_write(
    &image_ctx, OBJECT_DISPATCH_LAYER_NONE, object_extent.oid.name,
    object_extent.objectno, object_extent.offset, std::move(m_cmp_bl),
    std::move(bl), snapc, m_mismatch_offset, m_op_flags, journal_tid,
    this->m_trace, on_finish);
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
    ObjectExtents* object_extents) const {
  if (object_extents->size() > 1)
    return -EINVAL;

  I &image_ctx = this->m_image_ctx;
  uint64_t sector_size = 512ULL;
  uint64_t su = image_ctx.layout.stripe_unit;
  ObjectExtent object_extent = object_extents->front();
  if (object_extent.offset % sector_size + object_extent.length > sector_size ||
      (su != 0 && (object_extent.offset % su + object_extent.length > su)))
    return -EINVAL;

  return 0;
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
