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
#include "librbd/io/ObjectRequest.h"
#include "librbd/journal/Types.h"
#include "include/rados/librados.hpp"
#include "common/WorkQueue.h"
#include "osdc/Striper.h"

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::io::ImageRequest: " << this \
                           << " " << __func__ << ": "

namespace librbd {
namespace io {

using util::get_image_ctx;

namespace {

template <typename ImageCtxT = ImageCtx>
struct C_DiscardRequest {
  typedef std::vector<ObjectExtent> ObjectExtents;

  ImageCtxT &image_ctx;
  AioCompletion *aio_completion;
  ObjectExtents object_extents;

  C_DiscardRequest(ImageCtxT &image_ctx, AioCompletion *aio_completion,
                   const ObjectExtents &object_extents)
    : image_ctx(image_ctx), aio_completion(aio_completion),
      object_extents(object_extents) {
    aio_completion->add_request();
  }

  void send() {
    discard_writeback();
  }

  void discard_writeback() {
    CephContext *cct = image_ctx.cct;
    ldout(cct, 20) << "C_DiscardRequest: delaying cache discard until "
                   << "writeback flushed" << dendl;

    // ensure we aren't holding the cache lock post-write
    auto ctx = util::create_async_context_callback(
      image_ctx, util::create_context_callback<
        C_DiscardRequest<ImageCtxT>,
        &C_DiscardRequest<ImageCtxT>::handle_discard_writeback>(this));

    // ensure any in-flight writeback is complete before advancing
    // the discard request
    Mutex::Locker cache_locker(image_ctx.cache_lock);
    image_ctx.object_cacher->discard_writeback(image_ctx.object_set,
                                               object_extents, ctx);
  }

  void handle_discard_writeback(int r) {
    CephContext *cct = image_ctx.cct;
    ldout(cct, 20) << "C_DiscardRequest: writeback flushed: " << r << dendl;

    {
      Mutex::Locker cache_locker(image_ctx.cache_lock);
      image_ctx.object_cacher->discard_set(image_ctx.object_set,
                                           object_extents);
    }
    aio_completion->complete_request(r);
    delete this;
  }
};

template <typename ImageCtxT = ImageCtx>
struct C_DiscardJournalCommit : public Context {
  ImageCtxT &image_ctx;
  C_DiscardRequest<ImageCtxT> *discard_request;

  C_DiscardJournalCommit(ImageCtxT &_image_ctx,
                         C_DiscardRequest<ImageCtxT> *discard_request,
                         uint64_t tid)
    : image_ctx(_image_ctx), discard_request(discard_request) {
    CephContext *cct = image_ctx.cct;
    ldout(cct, 20) << "delaying cache discard until journal tid " << tid << " "
                   << "safe" << dendl;
  }

  void finish(int r) override {
    CephContext *cct = image_ctx.cct;
    ldout(cct, 20) << "C_DiscardJournalCommit: "
                   << "journal committed: discarding from cache" << dendl;
    discard_request->send();
  }
};

} // anonymous namespace

template <typename I>
ImageRequest<I>* ImageRequest<I>::create_read_request(
    I &image_ctx, AioCompletion *aio_comp, Extents &&image_extents,
    ReadResult &&read_result, int op_flags,
    const ZTracer::Trace &parent_trace) {
  return new ImageReadRequest<I>(image_ctx, aio_comp,
                                 std::move(image_extents),
                                 std::move(read_result), op_flags,
                                 parent_trace);
}

template <typename I>
ImageRequest<I>* ImageRequest<I>::create_write_request(
    I &image_ctx, AioCompletion *aio_comp, Extents &&image_extents,
    bufferlist &&bl, int op_flags, const ZTracer::Trace &parent_trace) {
  return new ImageWriteRequest<I>(image_ctx, aio_comp, std::move(image_extents),
                                  std::move(bl), op_flags, parent_trace);
}

template <typename I>
ImageRequest<I>* ImageRequest<I>::create_discard_request(
    I &image_ctx, AioCompletion *aio_comp, uint64_t off, uint64_t len,
    bool skip_partial_discard, const ZTracer::Trace &parent_trace) {
  return new ImageDiscardRequest<I>(image_ctx, aio_comp, off, len,
                                    skip_partial_discard, parent_trace);
}

template <typename I>
ImageRequest<I>* ImageRequest<I>::create_flush_request(
    I &image_ctx, AioCompletion *aio_comp,
    const ZTracer::Trace &parent_trace) {
  return new ImageFlushRequest<I>(image_ctx, aio_comp, parent_trace);
}

template <typename I>
ImageRequest<I>* ImageRequest<I>::create_writesame_request(
    I &image_ctx, AioCompletion *aio_comp, uint64_t off, uint64_t len,
    bufferlist &&bl, int op_flags, const ZTracer::Trace &parent_trace) {
  return new ImageWriteSameRequest<I>(image_ctx, aio_comp, off, len,
                                      std::move(bl), op_flags, parent_trace);
}

template <typename I>
ImageRequest<I>* ImageRequest<I>::create_compare_and_write_request(
    I &image_ctx, AioCompletion *c, Extents &&image_extents,
    bufferlist &&cmp_bl, bufferlist &&bl, uint64_t *mismatch_offset,
    int op_flags, const ZTracer::Trace &parent_trace) {
  return new ImageCompareAndWriteRequest<I>(image_ctx, c,
                                            std::move(image_extents),
                                            std::move(cmp_bl),
                                            std::move(bl), mismatch_offset,
                                            op_flags, parent_trace);
}

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
                                  uint64_t off, uint64_t len,
                                  bool skip_partial_discard,
				  const ZTracer::Trace &parent_trace) {
  ImageDiscardRequest<I> req(*ictx, c, off, len, skip_partial_discard,
			     parent_trace);
  req.send();
}

template <typename I>
void ImageRequest<I>::aio_flush(I *ictx, AioCompletion *c,
				const ZTracer::Trace &parent_trace) {
  ImageFlushRequest<I> req(*ictx, c, parent_trace);
  req.send();
}

template <typename I>
void ImageRequest<I>::aio_writesame(I *ictx, AioCompletion *c,
                                    uint64_t off, uint64_t len,
                                    bufferlist &&bl, int op_flags,
				    const ZTracer::Trace &parent_trace) {
  ImageWriteSameRequest<I> req(*ictx, c, off, len, std::move(bl), op_flags,
			       parent_trace);
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
  assert(m_aio_comp->is_initialized(get_aio_type()));
  assert(m_aio_comp->is_started());

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
void ImageRequest<I>::start_op() {
  m_aio_comp->start_op();
}

template <typename I>
void ImageRequest<I>::fail(int r) {
  AioCompletion *aio_comp = this->m_aio_comp;
  aio_comp->get();
  aio_comp->fail(r);
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
  if (image_ctx.object_cacher && image_ctx.readahead_max_bytes > 0 &&
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

      auto req_comp = new io::ReadResult::C_SparseReadRequest<I>(
        aio_comp, std::move(extent.buffer_extents), true);
      ObjectReadRequest<I> *req = ObjectReadRequest<I>::create(
        &image_ctx, extent.oid.name, extent.objectno, extent.offset,
        extent.length, snap_id, m_op_flags, false, this->m_trace, req_comp);
      req_comp->request = req;
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
  assert(image_ctx.image_cache != nullptr);

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

  int ret = prune_object_extents(object_extents);
  if (ret < 0) {
    aio_comp->fail(ret);
    return;
  }

  if (!object_extents.empty()) {
    uint64_t journal_tid = 0;
    aio_comp->set_request_count(
      object_extents.size() + get_object_cache_request_count());

    ObjectRequests requests;
    send_object_requests(object_extents, snapc,
                         (journaling ? &requests : nullptr));

    if (journaling) {
      // in-flight ops are flushed prior to closing the journal
      assert(image_ctx.journal != NULL);
      journal_tid = append_journal_event(requests, m_synchronous);
    }

    if (image_ctx.object_cacher != NULL) {
      send_object_cache_requests(object_extents, journal_tid);
    }
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
    ObjectRequests *object_requests) {
  I &image_ctx = this->m_image_ctx;
  CephContext *cct = image_ctx.cct;

  AioCompletion *aio_comp = this->m_aio_comp;
  for (ObjectExtents::const_iterator p = object_extents.begin();
       p != object_extents.end(); ++p) {
    ldout(cct, 20) << "oid " << p->oid << " " << p->offset << "~" << p->length
                   << " from " << p->buffer_extents << dendl;
    C_AioRequest *req_comp = new C_AioRequest(aio_comp);
    ObjectRequestHandle *request = create_object_request(*p, snapc,
                                                            req_comp);

    // if journaling, stash the request for later; otherwise send
    if (request != NULL) {
      if (object_requests != NULL) {
        object_requests->push_back(request);
      } else {
        request->send();
      }
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
uint64_t ImageWriteRequest<I>::append_journal_event(
    const ObjectRequests &requests, bool synchronous) {
  I &image_ctx = this->m_image_ctx;

  uint64_t tid = 0;
  uint64_t buffer_offset = 0;
  assert(!this->m_image_extents.empty());
  for (auto &extent : this->m_image_extents) {
    bufferlist sub_bl;
    sub_bl.substr_of(m_bl, buffer_offset, extent.second);
    buffer_offset += extent.second;

    tid = image_ctx.journal->append_write_event(extent.first, extent.second,
                                                sub_bl, requests, synchronous);
  }

  if (image_ctx.object_cacher == NULL) {
    AioCompletion *aio_comp = this->m_aio_comp;
    aio_comp->associate_journal_event(tid);
  }
  return tid;
}

template <typename I>
void ImageWriteRequest<I>::send_image_cache_request() {
  I &image_ctx = this->m_image_ctx;
  assert(image_ctx.image_cache != nullptr);

  AioCompletion *aio_comp = this->m_aio_comp;
  aio_comp->set_request_count(1);
  C_AioRequest *req_comp = new C_AioRequest(aio_comp);
  image_ctx.image_cache->aio_write(std::move(this->m_image_extents),
                                   std::move(m_bl), m_op_flags, req_comp);
}

template <typename I>
void ImageWriteRequest<I>::send_object_cache_requests(
    const ObjectExtents &object_extents, uint64_t journal_tid) {
  I &image_ctx = this->m_image_ctx;
  for (auto p = object_extents.begin(); p != object_extents.end(); ++p) {
    const ObjectExtent &object_extent = *p;

    bufferlist bl;
    assemble_extent(object_extent, &bl);

    AioCompletion *aio_comp = this->m_aio_comp;
    C_AioRequest *req_comp = new C_AioRequest(aio_comp);
    image_ctx.write_to_cache(
      object_extent.oid, bl, object_extent.length, object_extent.offset,
      req_comp, m_op_flags, journal_tid,
      (this->m_trace.valid() ? &this->m_trace : nullptr));
  }
}

template <typename I>
void ImageWriteRequest<I>::send_object_requests(
    const ObjectExtents &object_extents, const ::SnapContext &snapc,
    ObjectRequests *object_requests) {
  I &image_ctx = this->m_image_ctx;

  // cache handles creating object requests during writeback
  if (image_ctx.object_cacher == NULL) {
    AbstractImageWriteRequest<I>::send_object_requests(object_extents, snapc,
                                                       object_requests);
  }
}

template <typename I>
ObjectRequestHandle *ImageWriteRequest<I>::create_object_request(
    const ObjectExtent &object_extent, const ::SnapContext &snapc,
    Context *on_finish) {
  I &image_ctx = this->m_image_ctx;
  assert(image_ctx.object_cacher == NULL);

  bufferlist bl;
  assemble_extent(object_extent, &bl);
  ObjectRequest<I> *req = ObjectRequest<I>::create_write(
    &image_ctx, object_extent.oid.name, object_extent.objectno,
    object_extent.offset, bl, snapc, m_op_flags, this->m_trace, on_finish);
  return req;
}

template <typename I>
void ImageWriteRequest<I>::update_stats(size_t length) {
  I &image_ctx = this->m_image_ctx;
  image_ctx.perfcounter->inc(l_librbd_wr);
  image_ctx.perfcounter->inc(l_librbd_wr_bytes, length);
}

template <typename I>
uint64_t ImageDiscardRequest<I>::append_journal_event(
    const ObjectRequests &requests, bool synchronous) {
  I &image_ctx = this->m_image_ctx;

  uint64_t tid = 0;
  assert(!this->m_image_extents.empty());
  for (auto &extent : this->m_image_extents) {
    journal::EventEntry event_entry(journal::AioDiscardEvent(extent.first,
                                                             extent.second,
                                                             this->m_skip_partial_discard));
    tid = image_ctx.journal->append_io_event(std::move(event_entry),
                                             requests, extent.first,
                                             extent.second, synchronous, 0);
  }

  AioCompletion *aio_comp = this->m_aio_comp;
  aio_comp->associate_journal_event(tid);
  return tid;
}

template <typename I>
int ImageDiscardRequest<I>::prune_object_extents(ObjectExtents &object_extents) {
  I &image_ctx = this->m_image_ctx;
  CephContext *cct = image_ctx.cct;
  if (!this->m_skip_partial_discard) {
    return 0;
  }

  for (auto p = object_extents.begin(); p != object_extents.end(); ) {
    if (p->offset + p->length < image_ctx.layout.object_size) {
      ldout(cct, 20) << "oid " << p->oid << " " << p->offset << "~"
		     << p->length << " from " << p->buffer_extents
		     << ": skip partial discard" << dendl;
      p = object_extents.erase(p);
    } else {
      ++p;
    }
  }

  return 0;
}

template <typename I>
uint32_t ImageDiscardRequest<I>::get_object_cache_request_count() const {
  // extra completion request is required for tracking discard
  I &image_ctx = this->m_image_ctx;
  return (image_ctx.object_cacher != nullptr ? 1 : 0);
}

template <typename I>
void ImageDiscardRequest<I>::send_image_cache_request() {
  I &image_ctx = this->m_image_ctx;
  assert(image_ctx.image_cache != nullptr);

  AioCompletion *aio_comp = this->m_aio_comp;
  aio_comp->set_request_count(this->m_image_extents.size());
  for (auto &extent : this->m_image_extents) {
    C_AioRequest *req_comp = new C_AioRequest(aio_comp);
    image_ctx.image_cache->aio_discard(extent.first, extent.second,
                                       this->m_skip_partial_discard, req_comp);
  }
}

template <typename I>
void ImageDiscardRequest<I>::send_object_cache_requests(
    const ObjectExtents &object_extents, uint64_t journal_tid) {
  I &image_ctx = this->m_image_ctx;
  auto aio_comp = this->m_aio_comp;
  auto req = new C_DiscardRequest<I>(image_ctx, aio_comp, object_extents);
  if (journal_tid == 0) {
    req->send();
  } else {
    // cannot discard from cache until journal has committed
    assert(image_ctx.journal != NULL);
    image_ctx.journal->wait_event(
      journal_tid, new C_DiscardJournalCommit<I>(image_ctx, req, journal_tid));
  }
}

template <typename I>
ObjectRequestHandle *ImageDiscardRequest<I>::create_object_request(
    const ObjectExtent &object_extent, const ::SnapContext &snapc,
    Context *on_finish) {
  I &image_ctx = this->m_image_ctx;

  auto req = ObjectRequest<I>::create_discard(
    &image_ctx, object_extent.oid.name, object_extent.objectno,
    object_extent.offset, object_extent.length, snapc, true, true,
    this->m_trace, on_finish);
  return req;
}

template <typename I>
void ImageDiscardRequest<I>::update_stats(size_t length) {
  I &image_ctx = this->m_image_ctx;
  image_ctx.perfcounter->inc(l_librbd_discard);
  image_ctx.perfcounter->inc(l_librbd_discard_bytes, length);
}

template <typename I>
void ImageFlushRequest<I>::send_request() {
  I &image_ctx = this->m_image_ctx;
  image_ctx.user_flushed();

  bool journaling = false;
  {
    RWLock::RLocker snap_locker(image_ctx.snap_lock);
    journaling = (image_ctx.journal != nullptr &&
                  image_ctx.journal->is_journal_appending());
  }

  AioCompletion *aio_comp = this->m_aio_comp;
  aio_comp->set_request_count(1);

  Context* ctx = new C_AioRequest(aio_comp);

  // ensure no locks are held when flush is complete
  ctx = util::create_async_context_callback(image_ctx, ctx);

  if (journaling) {
    // in-flight ops are flushed prior to closing the journal
    uint64_t journal_tid = image_ctx.journal->append_io_event(
      journal::EventEntry(journal::AioFlushEvent()),
      ObjectRequests(), 0, 0, false, 0);

    aio_comp->associate_journal_event(journal_tid);

    ctx = new FunctionContext([ctx, &image_ctx, journal_tid](int r) {
      image_ctx.journal->flush_event(journal_tid, ctx);
    });

  } else if (image_ctx.object_cacher != nullptr) {
    // flush rbd cache only when journaling is not enabled
    ctx = new FunctionContext([ctx, &image_ctx](int r) {
      image_ctx.flush_cache(ctx);
    });
  }

  // ensure all in-flight IOs are settled if non-user flush request
  aio_comp->async_op.flush(ctx);
  aio_comp->put();

  image_ctx.perfcounter->inc(l_librbd_aio_flush);
}

template <typename I>
void ImageFlushRequest<I>::send_image_cache_request() {
  I &image_ctx = this->m_image_ctx;
  assert(image_ctx.image_cache != nullptr);

  AioCompletion *aio_comp = this->m_aio_comp;
  aio_comp->set_request_count(1);
  C_AioRequest *req_comp = new C_AioRequest(aio_comp);
  image_ctx.image_cache->aio_flush(req_comp);
}

template <typename I>
bool ImageWriteSameRequest<I>::assemble_writesame_extent(const ObjectExtent &object_extent,
                                                         bufferlist *bl, bool force_write) {
  size_t m_data_len = m_data_bl.length();

  if (!force_write) {
    bool may_writesame = true;

    for (auto q = object_extent.buffer_extents.begin();
         q != object_extent.buffer_extents.end(); ++q) {
      if (!(q->first % m_data_len == 0 && q->second % m_data_len == 0)) {
        may_writesame = false;
        break;
      }
    }

    if (may_writesame) {
      bl->append(m_data_bl);
      return true;
    }
  }

  for (auto q = object_extent.buffer_extents.begin();
       q != object_extent.buffer_extents.end(); ++q) {
    bufferlist sub_bl;
    uint64_t sub_off = q->first % m_data_len;
    uint64_t sub_len = m_data_len - sub_off;
    uint64_t extent_left = q->second;
    while (extent_left >= sub_len) {
      sub_bl.substr_of(m_data_bl, sub_off, sub_len);
      bl->claim_append(sub_bl);
      extent_left -= sub_len;
      if (sub_off) {
	sub_off = 0;
	sub_len = m_data_len;
      }
    }
    if (extent_left) {
      sub_bl.substr_of(m_data_bl, sub_off, extent_left);
      bl->claim_append(sub_bl);
    }
  }
  return false;
}

template <typename I>
uint64_t ImageWriteSameRequest<I>::append_journal_event(
    const ObjectRequests &requests, bool synchronous) {
  I &image_ctx = this->m_image_ctx;

  uint64_t tid = 0;
  assert(!this->m_image_extents.empty());
  for (auto &extent : this->m_image_extents) {
    journal::EventEntry event_entry(journal::AioWriteSameEvent(extent.first,
                                                               extent.second,
                                                               m_data_bl));
    tid = image_ctx.journal->append_io_event(std::move(event_entry),
                                             requests, extent.first,
                                             extent.second, synchronous, 0);
  }

  if (image_ctx.object_cacher == NULL) {
    AioCompletion *aio_comp = this->m_aio_comp;
    aio_comp->associate_journal_event(tid);
  }
  return tid;
}

template <typename I>
void ImageWriteSameRequest<I>::send_image_cache_request() {
  I &image_ctx = this->m_image_ctx;
  assert(image_ctx.image_cache != nullptr);

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
void ImageWriteSameRequest<I>::send_object_cache_requests(
    const ObjectExtents &object_extents, uint64_t journal_tid) {
  I &image_ctx = this->m_image_ctx;
  for (auto p = object_extents.begin(); p != object_extents.end(); ++p) {
    const ObjectExtent &object_extent = *p;

    bufferlist bl;
    assemble_writesame_extent(object_extent, &bl, true);

    AioCompletion *aio_comp = this->m_aio_comp;
    C_AioRequest *req_comp = new C_AioRequest(aio_comp);
    image_ctx.write_to_cache(
      object_extent.oid, bl, object_extent.length, object_extent.offset,
      req_comp, m_op_flags, journal_tid,
      (this->m_trace.valid() ? &this->m_trace : nullptr));
  }
}

template <typename I>
void ImageWriteSameRequest<I>::send_object_requests(
    const ObjectExtents &object_extents, const ::SnapContext &snapc,
    ObjectRequests *object_requests) {
  I &image_ctx = this->m_image_ctx;

  // cache handles creating object requests during writeback
  if (image_ctx.object_cacher == NULL) {
    AbstractImageWriteRequest<I>::send_object_requests(object_extents, snapc,
                                                       object_requests);
  }
}

template <typename I>
ObjectRequestHandle *ImageWriteSameRequest<I>::create_object_request(
    const ObjectExtent &object_extent, const ::SnapContext &snapc,
    Context *on_finish) {
  I &image_ctx = this->m_image_ctx;
  assert(image_ctx.object_cacher == NULL);

  bufferlist bl;
  ObjectRequest<I> *req;

  if (assemble_writesame_extent(object_extent, &bl, false)) {
    req = ObjectRequest<I>::create_writesame(
      &image_ctx, object_extent.oid.name, object_extent.objectno,
      object_extent.offset, object_extent.length,
      bl, snapc, m_op_flags, this->m_trace, on_finish);
    return req;
  }
  req = ObjectRequest<I>::create_write(
    &image_ctx, object_extent.oid.name, object_extent.objectno,
    object_extent.offset, bl, snapc, m_op_flags, this->m_trace, on_finish);
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
    const ObjectRequests &requests, bool synchronous) {

  I &image_ctx = this->m_image_ctx;

  uint64_t tid = 0;
  assert(this->m_image_extents.size() == 1);
  auto &extent = this->m_image_extents.front();
  journal::EventEntry event_entry(journal::AioCompareAndWriteEvent(extent.first,
                                                                   extent.second,
                                                                   m_cmp_bl, m_bl));
  tid = image_ctx.journal->append_io_event(std::move(event_entry),
                                           requests, extent.first,
                                           extent.second, synchronous, -EILSEQ);

  AioCompletion *aio_comp = this->m_aio_comp;
  aio_comp->associate_journal_event(tid);

  return tid;
}

template <typename I>
uint32_t ImageCompareAndWriteRequest<I>::get_object_cache_request_count() const {
  // extra completion request is required for tracking discard
  I &image_ctx = this->m_image_ctx;
  return (image_ctx.object_cacher != nullptr ? 1 : 0);
}

template <typename I>
void ImageCompareAndWriteRequest<I>::send_object_cache_requests(
  const ObjectExtents &object_extents, uint64_t journal_tid) {
  I &image_ctx = this->m_image_ctx;

  if (image_ctx.object_cacher != NULL) {
    auto aio_comp = this->m_aio_comp;
    auto req = new C_DiscardRequest<I>(image_ctx, aio_comp, object_extents);
    req->send();
  }
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
  assert(image_ctx.image_cache != nullptr);

  AioCompletion *aio_comp = this->m_aio_comp;
  aio_comp->set_request_count(1);
  C_AioRequest *req_comp = new C_AioRequest(aio_comp);
  image_ctx.image_cache->aio_compare_and_write(
    std::move(this->m_image_extents), std::move(m_cmp_bl), std::move(m_bl),
    m_mismatch_offset, m_op_flags, req_comp);
}

template <typename I>
ObjectRequestHandle *ImageCompareAndWriteRequest<I>::create_object_request(
    const ObjectExtent &object_extent,
    const ::SnapContext &snapc,
    Context *on_finish) {
  I &image_ctx = this->m_image_ctx;

  bufferlist bl;
  assemble_extent(object_extent, &bl);
  ObjectRequest<I> *req = ObjectRequest<I>::create_compare_and_write(
                                  &image_ctx, object_extent.oid.name,
                                  object_extent.objectno, object_extent.offset,
                                  m_cmp_bl, bl, snapc, m_mismatch_offset,
                                  m_op_flags, this->m_trace, on_finish);
  return req;
}

template <typename I>
void ImageCompareAndWriteRequest<I>::update_stats(size_t length) {
  I &image_ctx = this->m_image_ctx;
  image_ctx.perfcounter->inc(l_librbd_cmp);
  image_ctx.perfcounter->inc(l_librbd_cmp_bytes, length);
}

template <typename I>
int ImageCompareAndWriteRequest<I>::prune_object_extents(ObjectExtents &object_extents) {
  if (object_extents.size() > 1)
    return -EINVAL;

  I &image_ctx = this->m_image_ctx;
  uint64_t sector_size = 512ULL;
  uint64_t su = image_ctx.layout.stripe_unit;
  ObjectExtent object_extent = object_extents.front();
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
