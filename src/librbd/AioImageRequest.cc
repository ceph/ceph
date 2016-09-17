// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/AioImageRequest.h"
#include "librbd/AioCompletion.h"
#include "librbd/AioObjectRequest.h"
#include "librbd/ImageCtx.h"
#include "librbd/internal.h"
#include "librbd/Journal.h"
#include "librbd/Utils.h"
#include "librbd/cache/ImageCache.h"
#include "librbd/journal/Types.h"
#include "include/rados/librados.hpp"
#include "common/WorkQueue.h"
#include "osdc/Striper.h"

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::AioImageRequest: "

namespace librbd {

using util::get_image_ctx;

namespace {

template <typename ImageCtxT = ImageCtx>
struct C_DiscardJournalCommit : public Context {
  typedef std::vector<ObjectExtent> ObjectExtents;

  ImageCtxT &image_ctx;
  AioCompletion *aio_comp;
  ObjectExtents object_extents;

  C_DiscardJournalCommit(ImageCtxT &_image_ctx, AioCompletion *_aio_comp,
                         const ObjectExtents &_object_extents, uint64_t tid)
    : image_ctx(_image_ctx), aio_comp(_aio_comp),
      object_extents(_object_extents) {
    CephContext *cct = image_ctx.cct;
    ldout(cct, 20) << this << " C_DiscardJournalCommit: "
                   << "delaying cache discard until journal tid " << tid << " "
                   << "safe" << dendl;

    aio_comp->add_request();
  }

  virtual void finish(int r) {
    CephContext *cct = image_ctx.cct;
    ldout(cct, 20) << this << " C_DiscardJournalCommit: "
                   << "journal committed: discarding from cache" << dendl;

    Mutex::Locker cache_locker(image_ctx.cache_lock);
    image_ctx.object_cacher->discard_set(image_ctx.object_set, object_extents);
    aio_comp->complete_request(r);
  }
};

template <typename ImageCtxT = ImageCtx>
struct C_FlushJournalCommit : public Context {
  ImageCtxT &image_ctx;
  AioCompletion *aio_comp;

  C_FlushJournalCommit(ImageCtxT &_image_ctx, AioCompletion *_aio_comp,
                       uint64_t tid)
    : image_ctx(_image_ctx), aio_comp(_aio_comp) {
    CephContext *cct = image_ctx.cct;
    ldout(cct, 20) << this << " C_FlushJournalCommit: "
                   << "delaying flush until journal tid " << tid << " "
                   << "safe" << dendl;

    aio_comp->add_request();
  }

  virtual void finish(int r) {
    CephContext *cct = image_ctx.cct;
    ldout(cct, 20) << this << " C_FlushJournalCommit: journal committed"
                   << dendl;
    aio_comp->complete_request(r);
  }
};

template <typename ImageCtxT>
class C_AioRead : public C_AioRequest {
public:
  C_AioRead(AioCompletion *completion)
    : C_AioRequest(completion), m_req(nullptr) {
  }

  virtual void finish(int r) {
    m_completion->lock.Lock();
    CephContext *cct = m_completion->ictx->cct;
    ldout(cct, 10) << "C_AioRead::finish() " << this << " r = " << r << dendl;

    if (r >= 0 || r == -ENOENT) { // this was a sparse_read operation
      ldout(cct, 10) << " got " << m_req->get_extent_map()
                     << " for " << m_req->get_buffer_extents()
                     << " bl " << m_req->data().length() << dendl;
      // reads from the parent don't populate the m_ext_map and the overlap
      // may not be the full buffer.  compensate here by filling in m_ext_map
      // with the read extent when it is empty.
      if (m_req->get_extent_map().empty()) {
        m_req->get_extent_map()[m_req->get_offset()] = m_req->data().length();
      }

      m_completion->destriper.add_partial_sparse_result(
          cct, m_req->data(), m_req->get_extent_map(), m_req->get_offset(),
          m_req->get_buffer_extents());
      r = m_req->get_length();
    }
    m_completion->lock.Unlock();

    C_AioRequest::finish(r);
  }

  void set_req(AioObjectRead<ImageCtxT> *req) {
    m_req = req;
  }
private:
  AioObjectRead<ImageCtxT> *m_req;
};

template <typename ImageCtxT>
class C_ImageCacheRead : public C_AioRequest {
public:
  typedef std::vector<std::pair<uint64_t,uint64_t> > Extents;

  C_ImageCacheRead(AioCompletion *completion, const Extents &image_extents)
    : C_AioRequest(completion), m_image_extents(image_extents) {
  }

  inline bufferlist &get_data() {
    return m_bl;
  }

protected:
  virtual void finish(int r) {
    CephContext *cct = m_completion->ictx->cct;
    ldout(cct, 10) << "C_ImageCacheRead::finish() " << this << ": r=" << r
                   << dendl;
    if (r >= 0) {
      size_t length = 0;
      for (auto &image_extent : m_image_extents) {
        length += image_extent.second;
      }
      assert(length == m_bl.length());

      m_completion->lock.Lock();
      m_completion->destriper.add_partial_result(cct, m_bl, m_image_extents);
      m_completion->lock.Unlock();
      r = length;
    }
    C_AioRequest::finish(r);
  }

private:
  bufferlist m_bl;
  Extents m_image_extents;
};

template <typename ImageCtxT>
class C_ObjectCacheRead : public Context {
public:
  explicit C_ObjectCacheRead(ImageCtxT &ictx, AioObjectRead<ImageCtxT> *req)
    : m_image_ctx(ictx), m_req(req), m_enqueued(false) {}

  virtual void complete(int r) {
    if (!m_enqueued) {
      // cache_lock creates a lock ordering issue -- so re-execute this context
      // outside the cache_lock
      m_enqueued = true;
      m_image_ctx.op_work_queue->queue(this, r);
      return;
    }
    Context::complete(r);
  }

protected:
  virtual void finish(int r) {
    m_req->complete(r);
  }

private:
  ImageCtxT &m_image_ctx;
  AioObjectRead<ImageCtxT> *m_req;
  bool m_enqueued;
};

} // anonymous namespace

template <typename I>
void AioImageRequest<I>::aio_read(I *ictx, AioCompletion *c,
                                  Extents &&image_extents, char *buf,
                                  bufferlist *pbl, int op_flags) {
  AioImageRead<I> req(*ictx, c, std::move(image_extents), buf, pbl, op_flags);
  req.send();
}

template <typename I>
void AioImageRequest<I>::aio_write(I *ictx, AioCompletion *c, uint64_t off,
                                   size_t len, const char *buf, int op_flags) {
  AioImageWrite<I> req(*ictx, c, off, len, buf, op_flags);
  req.send();
}

template <typename I>
void AioImageRequest<I>::aio_write(I *ictx, AioCompletion *c,
                                   Extents &&image_extents, bufferlist &&bl,
                                   int op_flags) {
  AioImageWrite<I> req(*ictx, c, std::move(image_extents), std::move(bl),
                       op_flags);
  req.send();
}

template <typename I>
void AioImageRequest<I>::aio_discard(I *ictx, AioCompletion *c,
                                     uint64_t off, uint64_t len) {
  AioImageDiscard<I> req(*ictx, c, off, len);
  req.send();
}

template <typename I>
void AioImageRequest<I>::aio_flush(I *ictx, AioCompletion *c) {
  AioImageFlush<I> req(*ictx, c);
  req.send();
}

template <typename I>
void AioImageRequest<I>::send() {
  I &image_ctx = this->m_image_ctx;
  assert(m_aio_comp->is_initialized(get_aio_type()));
  assert(m_aio_comp->is_started() ^ (get_aio_type() == AIO_TYPE_FLUSH));

  CephContext *cct = image_ctx.cct;
  AioCompletion *aio_comp = this->m_aio_comp;
  ldout(cct, 20) << get_request_type() << ": ictx=" << &image_ctx << ", "
                 << "completion=" << aio_comp <<  dendl;

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
int AioImageRequest<I>::clip_request() {
  RWLock::RLocker snap_locker(m_image_ctx.snap_lock);
  for (auto &image_extent : m_image_extents) {
    size_t clip_len = image_extent.second;
    int r = clip_io(get_image_ctx(&m_image_ctx), image_extent.first, &clip_len);
    if (r < 0) {
      return r;
    }

    image_extent.second = clip_len;
  }
  return 0;
}

template <typename I>
void AioImageRequest<I>::fail(int r) {
  AioCompletion *aio_comp = this->m_aio_comp;
  aio_comp->get();
  aio_comp->fail(r);
}

template <typename I>
void AioImageRead<I>::send_request() {
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

  aio_comp->read_buf = m_buf;
  aio_comp->read_buf_len = buffer_ofs;
  aio_comp->read_bl = m_pbl;

  // pre-calculate the expected number of read requests
  uint32_t request_count = 0;
  for (auto &object_extent : object_extents) {
    request_count += object_extent.second.size();
  }
  aio_comp->set_request_count(request_count);

  // issue the requests
  for (auto &object_extent : object_extents) {
    for (auto &extent : object_extent.second) {
      ldout(cct, 20) << " oid " << extent.oid << " " << extent.offset << "~"
                     << extent.length << " from " << extent.buffer_extents
                     << dendl;

      C_AioRead<I> *req_comp = new C_AioRead<I>(aio_comp);
      AioObjectRead<I> *req = AioObjectRead<I>::create(
        &image_ctx, extent.oid.name, extent.objectno, extent.offset,
        extent.length, extent.buffer_extents, snap_id, true, req_comp,
        m_op_flags);
      req_comp->set_req(req);

      if (image_ctx.object_cacher) {
        C_ObjectCacheRead<I> *cache_comp = new C_ObjectCacheRead<I>(image_ctx,
                                                                    req);
        image_ctx.aio_read_from_cache(extent.oid, extent.objectno,
                                      &req->data(), extent.length,
                                      extent.offset, cache_comp, m_op_flags);
      } else {
        req->send();
      }
    }
  }

  aio_comp->put();

  image_ctx.perfcounter->inc(l_librbd_rd);
  image_ctx.perfcounter->inc(l_librbd_rd_bytes, buffer_ofs);
}

template <typename I>
void AioImageRead<I>::send_image_cache_request() {
  I &image_ctx = this->m_image_ctx;
  assert(image_ctx.image_cache != nullptr);

  AioCompletion *aio_comp = this->m_aio_comp;
  aio_comp->set_request_count(1);
  C_ImageCacheRead<I> *req_comp = new C_ImageCacheRead<I>(
    aio_comp, this->m_image_extents);
  image_ctx.image_cache->aio_read(std::move(this->m_image_extents),
                                  &req_comp->get_data(), m_op_flags,
                                  req_comp);
}

template <typename I>
void AbstractAioImageWrite<I>::send_request() {
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

  prune_object_extents(object_extents);

  if (!object_extents.empty()) {
    uint64_t journal_tid = 0;
    aio_comp->set_request_count(
      object_extents.size() + get_object_cache_request_count(journaling));

    AioObjectRequests requests;
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
void AbstractAioImageWrite<I>::send_object_requests(
    const ObjectExtents &object_extents, const ::SnapContext &snapc,
    AioObjectRequests *aio_object_requests) {
  I &image_ctx = this->m_image_ctx;
  CephContext *cct = image_ctx.cct;

  AioCompletion *aio_comp = this->m_aio_comp;
  for (ObjectExtents::const_iterator p = object_extents.begin();
       p != object_extents.end(); ++p) {
    ldout(cct, 20) << " oid " << p->oid << " " << p->offset << "~" << p->length
                   << " from " << p->buffer_extents << dendl;
    C_AioRequest *req_comp = new C_AioRequest(aio_comp);
    AioObjectRequestHandle *request = create_object_request(*p, snapc,
                                                            req_comp);

    // if journaling, stash the request for later; otherwise send
    if (request != NULL) {
      if (aio_object_requests != NULL) {
        aio_object_requests->push_back(request);
      } else {
        request->send();
      }
    }
  }
}

template <typename I>
void AioImageWrite<I>::assemble_extent(const ObjectExtent &object_extent,
                                    bufferlist *bl) {
  for (auto q = object_extent.buffer_extents.begin();
       q != object_extent.buffer_extents.end(); ++q) {
    bufferlist sub_bl;
    sub_bl.substr_of(m_bl, q->first, q->second);
    bl->claim_append(sub_bl);
  }
}

template <typename I>
uint64_t AioImageWrite<I>::append_journal_event(
    const AioObjectRequests &requests, bool synchronous) {
  I &image_ctx = this->m_image_ctx;

  uint64_t tid;
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
void AioImageWrite<I>::send_image_cache_request() {
  I &image_ctx = this->m_image_ctx;
  assert(image_ctx.image_cache != nullptr);

  AioCompletion *aio_comp = this->m_aio_comp;
  aio_comp->set_request_count(1);
  C_AioRequest *req_comp = new C_AioRequest(aio_comp);
  image_ctx.image_cache->aio_write(std::move(this->m_image_extents),
                                   std::move(m_bl), m_op_flags, req_comp);
}

template <typename I>
void AioImageWrite<I>::send_object_cache_requests(const ObjectExtents &object_extents,
                                                  uint64_t journal_tid) {
  I &image_ctx = this->m_image_ctx;
  for (auto p = object_extents.begin(); p != object_extents.end(); ++p) {
    const ObjectExtent &object_extent = *p;

    bufferlist bl;
    assemble_extent(object_extent, &bl);

    AioCompletion *aio_comp = this->m_aio_comp;
    C_AioRequest *req_comp = new C_AioRequest(aio_comp);
    image_ctx.write_to_cache(object_extent.oid, bl, object_extent.length,
                             object_extent.offset, req_comp, m_op_flags,
                               journal_tid);
  }
}

template <typename I>
void AioImageWrite<I>::send_object_requests(
    const ObjectExtents &object_extents, const ::SnapContext &snapc,
    AioObjectRequests *aio_object_requests) {
  I &image_ctx = this->m_image_ctx;

  // cache handles creating object requests during writeback
  if (image_ctx.object_cacher == NULL) {
    AbstractAioImageWrite<I>::send_object_requests(object_extents, snapc,
                                                aio_object_requests);
  }
}

template <typename I>
AioObjectRequestHandle *AioImageWrite<I>::create_object_request(
    const ObjectExtent &object_extent, const ::SnapContext &snapc,
    Context *on_finish) {
  I &image_ctx = this->m_image_ctx;
  assert(image_ctx.object_cacher == NULL);

  bufferlist bl;
  assemble_extent(object_extent, &bl);
  AioObjectRequest<I> *req = AioObjectRequest<I>::create_write(
    &image_ctx, object_extent.oid.name, object_extent.objectno,
    object_extent.offset, bl, snapc, on_finish, m_op_flags);
  return req;
}

template <typename I>
void AioImageWrite<I>::update_stats(size_t length) {
  I &image_ctx = this->m_image_ctx;
  image_ctx.perfcounter->inc(l_librbd_wr);
  image_ctx.perfcounter->inc(l_librbd_wr_bytes, length);
}

template <typename I>
uint64_t AioImageDiscard<I>::append_journal_event(
    const AioObjectRequests &requests, bool synchronous) {
  I &image_ctx = this->m_image_ctx;

  uint64_t tid;
  assert(!this->m_image_extents.empty());
  for (auto &extent : this->m_image_extents) {
    journal::EventEntry event_entry(journal::AioDiscardEvent(extent.first,
                                                             extent.second));
    tid = image_ctx.journal->append_io_event(std::move(event_entry),
                                             requests, extent.first,
                                             extent.second, synchronous);
  }

  AioCompletion *aio_comp = this->m_aio_comp;
  aio_comp->associate_journal_event(tid);
  return tid;
}

template <typename I>
void AioImageDiscard<I>::prune_object_extents(ObjectExtents &object_extents) {
  I &image_ctx = this->m_image_ctx;
  CephContext *cct = image_ctx.cct;
  if (!cct->_conf->rbd_skip_partial_discard) {
    return;
  }

  for (auto p = object_extents.begin(); p != object_extents.end(); ) {
    if (p->offset + p->length < image_ctx.layout.object_size) {
      ldout(cct, 20) << " oid " << p->oid << " " << p->offset << "~"
		     << p->length << " from " << p->buffer_extents
		     << ": skip partial discard" << dendl;
      p = object_extents.erase(p);
    } else {
      ++p;
    }
  }
}

template <typename I>
uint32_t AioImageDiscard<I>::get_object_cache_request_count(bool journaling) const {
  // extra completion request is required for tracking journal commit
  I &image_ctx = this->m_image_ctx;
  return (image_ctx.object_cacher != nullptr && journaling ? 1 : 0);
}

template <typename I>
void AioImageDiscard<I>::send_image_cache_request() {
  I &image_ctx = this->m_image_ctx;
  assert(image_ctx.image_cache != nullptr);

  AioCompletion *aio_comp = this->m_aio_comp;
  aio_comp->set_request_count(this->m_image_extents.size());
  for (auto &extent : this->m_image_extents) {
    C_AioRequest *req_comp = new C_AioRequest(aio_comp);
    image_ctx.image_cache->aio_discard(extent.first, extent.second, req_comp);
  }
}

template <typename I>
void AioImageDiscard<I>::send_object_cache_requests(const ObjectExtents &object_extents,
                                                    uint64_t journal_tid) {
  I &image_ctx = this->m_image_ctx;
  if (journal_tid == 0) {
    Mutex::Locker cache_locker(image_ctx.cache_lock);
    image_ctx.object_cacher->discard_set(image_ctx.object_set,
                                         object_extents);
  } else {
    // cannot discard from cache until journal has committed
    assert(image_ctx.journal != NULL);
    AioCompletion *aio_comp = this->m_aio_comp;
    image_ctx.journal->wait_event(
      journal_tid, new C_DiscardJournalCommit<I>(image_ctx, aio_comp,
                                                 object_extents, journal_tid));
  }
}

template <typename I>
AioObjectRequestHandle *AioImageDiscard<I>::create_object_request(
    const ObjectExtent &object_extent, const ::SnapContext &snapc,
    Context *on_finish) {
  I &image_ctx = this->m_image_ctx;

  AioObjectRequest<I> *req;
  if (object_extent.length == image_ctx.layout.object_size) {
    req = AioObjectRequest<I>::create_remove(
      &image_ctx, object_extent.oid.name, object_extent.objectno, snapc,
      on_finish);
  } else if (object_extent.offset + object_extent.length ==
               image_ctx.layout.object_size) {
    req = AioObjectRequest<I>::create_truncate(
      &image_ctx, object_extent.oid.name, object_extent.objectno,
      object_extent.offset, snapc, on_finish);
  } else {
    req = AioObjectRequest<I>::create_zero(
      &image_ctx, object_extent.oid.name, object_extent.objectno,
      object_extent.offset, object_extent.length, snapc, on_finish);
  }
  return req;
}

template <typename I>
void AioImageDiscard<I>::update_stats(size_t length) {
  I &image_ctx = this->m_image_ctx;
  image_ctx.perfcounter->inc(l_librbd_discard);
  image_ctx.perfcounter->inc(l_librbd_discard_bytes, length);
}

template <typename I>
void AioImageFlush<I>::send_request() {
  I &image_ctx = this->m_image_ctx;
  image_ctx.user_flushed();

  bool journaling = false;
  {
    RWLock::RLocker snap_locker(image_ctx.snap_lock);
    journaling = (image_ctx.journal != nullptr &&
                  image_ctx.journal->is_journal_appending());
  }

  AioCompletion *aio_comp = this->m_aio_comp;
  if (journaling) {
    // in-flight ops are flushed prior to closing the journal
    uint64_t journal_tid = image_ctx.journal->append_io_event(
      journal::EventEntry(journal::AioFlushEvent()),
      AioObjectRequests(), 0, 0, false);

    aio_comp->set_request_count(2);

    C_FlushJournalCommit<I> *ctx = new C_FlushJournalCommit<I>(image_ctx,
                                                               aio_comp,
                                                               journal_tid);
    C_AioRequest *req_comp = new C_AioRequest(aio_comp);
    image_ctx.journal->flush_event(journal_tid, ctx);
    aio_comp->associate_journal_event(journal_tid);
    image_ctx.flush_async_operations(req_comp);
  } else {
    // flush rbd cache only when journaling is not enabled
    aio_comp->set_request_count(1);
    C_AioRequest *req_comp = new C_AioRequest(aio_comp);
    image_ctx.flush(req_comp);
  }

  // track flush op for block writes
  aio_comp->start_op(true);
  aio_comp->put();

  image_ctx.perfcounter->inc(l_librbd_aio_flush);
}

template <typename I>
void AioImageFlush<I>::send_image_cache_request() {
  I &image_ctx = this->m_image_ctx;
  assert(image_ctx.image_cache != nullptr);

  AioCompletion *aio_comp = this->m_aio_comp;
  aio_comp->set_request_count(1);
  C_AioRequest *req_comp = new C_AioRequest(aio_comp);
  image_ctx.image_cache->aio_flush(req_comp);
}

} // namespace librbd

template class librbd::AioImageRequest<librbd::ImageCtx>;
template class librbd::AbstractAioImageWrite<librbd::ImageCtx>;
template class librbd::AioImageWrite<librbd::ImageCtx>;
template class librbd::AioImageDiscard<librbd::ImageCtx>;
template class librbd::AioImageFlush<librbd::ImageCtx>;
