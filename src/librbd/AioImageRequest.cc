// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/AioImageRequest.h"
#include "librbd/AioCompletion.h"
#include "librbd/AioObjectRequest.h"
#include "librbd/ImageCtx.h"
#include "librbd/internal.h"
#include "librbd/Journal.h"
#include "librbd/Utils.h"
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

    RWLock::RLocker owner_locker(image_ctx.owner_lock);
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
class C_CacheRead : public Context {
public:
  explicit C_CacheRead(ImageCtxT &ictx, AioObjectRead<ImageCtxT> *req)
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
AioImageRequest<I>* AioImageRequest<I>::create_read_request(
    I &image_ctx, AioCompletion *aio_comp, uint64_t off, size_t len,
    char *buf, bufferlist *pbl, int op_flags) {
  return new AioImageRead<I>(image_ctx, aio_comp, off, len, buf, pbl,
			     op_flags);
}

template <typename I>
AioImageRequest<I>* AioImageRequest<I>::create_write_request(
    I &image_ctx, AioCompletion *aio_comp, uint64_t off, size_t len,
    const char *buf, int op_flags) {
  return new AioImageWrite<I>(image_ctx, aio_comp, off, len, buf, op_flags);
}

template <typename I>
AioImageRequest<I>* AioImageRequest<I>::create_discard_request(
    I &image_ctx, AioCompletion *aio_comp, uint64_t off, uint64_t len) {
  return new AioImageDiscard<I>(image_ctx, aio_comp, off, len);
}

template <typename I>
AioImageRequest<I>* AioImageRequest<I>::create_flush_request(
    I &image_ctx, AioCompletion *aio_comp) {
  return new AioImageFlush<I>(image_ctx, aio_comp);
}

template <typename I>
void AioImageRequest<I>::aio_read(
    I *ictx, AioCompletion *c,
    const std::vector<std::pair<uint64_t,uint64_t> > &extents,
    char *buf, bufferlist *pbl, int op_flags) {
  AioImageRead<I> req(*ictx, c, extents, buf, pbl, op_flags);
  req.send();
}

template <typename I>
void AioImageRequest<I>::aio_read(I *ictx, AioCompletion *c,
                                  uint64_t off, size_t len, char *buf,
                                  bufferlist *pbl, int op_flags) {
  AioImageRead<I> req(*ictx, c, off, len, buf, pbl, op_flags);
  req.send();
}

template <typename I>
void AioImageRequest<I>::aio_write(I *ictx, AioCompletion *c,
                                   uint64_t off, size_t len, const char *buf,
                                   int op_flags) {
  AioImageWrite<I> req(*ictx, c, off, len, buf, op_flags);
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
  assert(image_ctx.owner_lock.is_locked());
  assert(m_aio_comp->is_initialized(get_aio_type()));
  assert(m_aio_comp->is_started() ^ (get_aio_type() == AIO_TYPE_FLUSH));

  CephContext *cct = image_ctx.cct;
  AioCompletion *aio_comp = this->m_aio_comp;
  ldout(cct, 20) << get_request_type() << ": ictx=" << &image_ctx << ", "
                 << "completion=" << aio_comp <<  dendl;

  aio_comp->get();
  send_request();
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

  if (image_ctx.object_cacher && image_ctx.readahead_max_bytes > 0 &&
      !(m_op_flags & LIBRADOS_OP_FLAG_FADVISE_RANDOM)) {
    readahead(get_image_ctx(&image_ctx), m_image_extents);
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

    // map
    for (vector<pair<uint64_t,uint64_t> >::const_iterator p =
           m_image_extents.begin();
         p != m_image_extents.end(); ++p) {
      uint64_t len = p->second;
      int r = clip_io(get_image_ctx(&image_ctx), p->first, &len);
      if (r < 0) {
        aio_comp->fail(r);
        return;
      }
      if (len == 0) {
        continue;
      }

      Striper::file_to_extents(cct, image_ctx.format_string,
                               &image_ctx.layout, p->first, len, 0,
                               object_extents, buffer_ofs);
      buffer_ofs += len;
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
        C_CacheRead<I> *cache_comp = new C_CacheRead<I>(image_ctx, req);
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
void AbstractAioImageWrite<I>::send_request() {
  I &image_ctx = this->m_image_ctx;
  CephContext *cct = image_ctx.cct;

  RWLock::RLocker md_locker(image_ctx.md_lock);

  bool journaling = false;

  AioCompletion *aio_comp = this->m_aio_comp;
  uint64_t clip_len = m_len;
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

    int r = clip_io(get_image_ctx(&image_ctx), m_off, &clip_len);
    if (r < 0) {
      aio_comp->fail(r);
      return;
    }

    snapc = image_ctx.snapc;

    // map to object extents
    if (clip_len > 0) {
      Striper::file_to_extents(cct, image_ctx.format_string,
                               &image_ctx.layout, m_off, clip_len, 0,
                               object_extents);
    }

    journaling = (image_ctx.journal != nullptr &&
                  image_ctx.journal->is_journal_appending());
  }

  prune_object_extents(object_extents);

  if (!object_extents.empty()) {
    uint64_t journal_tid = 0;
    aio_comp->set_request_count(
      object_extents.size() + get_cache_request_count(journaling));

    AioObjectRequests requests;
    send_object_requests(object_extents, snapc,
                         (journaling ? &requests : nullptr));

    if (journaling) {
      // in-flight ops are flushed prior to closing the journal
      assert(image_ctx.journal != NULL);
      journal_tid = append_journal_event(requests, m_synchronous);
    }

    if (image_ctx.object_cacher != NULL) {
      send_cache_requests(object_extents, journal_tid);
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
    bl->append(m_buf + q->first, q->second);;
  }
}

template <typename I>
uint64_t AioImageWrite<I>::append_journal_event(
    const AioObjectRequests &requests, bool synchronous) {
  bufferlist bl;
  bl.append(m_buf, this->m_len);

  I &image_ctx = this->m_image_ctx;
  uint64_t tid = image_ctx.journal->append_write_event(this->m_off, this->m_len,
                                                       bl, requests,
                                                       synchronous);
  if (image_ctx.object_cacher == NULL) {
    AioCompletion *aio_comp = this->m_aio_comp;
    aio_comp->associate_journal_event(tid);
  }
  return tid;
}

template <typename I>
void AioImageWrite<I>::send_cache_requests(const ObjectExtents &object_extents,
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

  journal::EventEntry event_entry(journal::AioDiscardEvent(this->m_off,
                                                           this->m_len));
  uint64_t tid = image_ctx.journal->append_io_event(std::move(event_entry),
                                                    requests, this->m_off,
                                                    this->m_len, synchronous);

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
uint32_t AioImageDiscard<I>::get_cache_request_count(bool journaling) const {
  // extra completion request is required for tracking journal commit
  I &image_ctx = this->m_image_ctx;
  return (image_ctx.object_cacher != nullptr && journaling ? 1 : 0);
}

template <typename I>
void AioImageDiscard<I>::send_cache_requests(const ObjectExtents &object_extents,
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

    aio_comp->set_request_count(1);
    aio_comp->associate_journal_event(journal_tid);

    FunctionContext *flush_ctx = new FunctionContext(
      [aio_comp, &image_ctx, journal_tid] (int r) {
        C_FlushJournalCommit<I> *ctx = new C_FlushJournalCommit<I>(image_ctx,
                                                                 aio_comp,
                                                                 journal_tid);
        image_ctx.journal->flush_event(journal_tid, ctx);

        // track flush op for block writes
        aio_comp->start_op(true);
        aio_comp->put();
    });

    image_ctx.flush_async_operations(flush_ctx);
  } else {
    // flush rbd cache only when journaling is not enabled
    aio_comp->set_request_count(1);
    C_AioRequest *req_comp = new C_AioRequest(aio_comp);
    image_ctx.flush(req_comp);

    aio_comp->start_op(true);
    aio_comp->put();
  }

  image_ctx.perfcounter->inc(l_librbd_aio_flush);
}

} // namespace librbd

template class librbd::AioImageRequest<librbd::ImageCtx>;
template class librbd::AbstractAioImageWrite<librbd::ImageCtx>;
template class librbd::AioImageWrite<librbd::ImageCtx>;
template class librbd::AioImageDiscard<librbd::ImageCtx>;
template class librbd::AioImageFlush<librbd::ImageCtx>;
