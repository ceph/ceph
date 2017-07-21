// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/io/ObjectRequest.h"
#include "common/ceph_context.h"
#include "common/dout.h"
#include "common/errno.h"
#include "common/Mutex.h"
#include "common/RWLock.h"
#include "common/WorkQueue.h"
#include "include/Context.h"

#include "librbd/ExclusiveLock.h"
#include "librbd/ImageCtx.h"
#include "librbd/ObjectMap.h"
#include "librbd/Utils.h"
#include "librbd/io/AioCompletion.h"
#include "librbd/io/CopyupRequest.h"
#include "librbd/io/ImageRequest.h"
#include "librbd/io/ReadResult.h"

#include <boost/bind.hpp>
#include <boost/optional.hpp>

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::io::ObjectRequest: " << this \
                           << " " << __func__ << ": "

namespace librbd {
namespace io {

template <typename I>
ObjectRequest<I>*
ObjectRequest<I>::create_remove(I *ictx, const std::string &oid,
                                uint64_t object_no,
                                const ::SnapContext &snapc,
				const ZTracer::Trace &parent_trace,
                                Context *completion) {
  return new ObjectRemoveRequest(util::get_image_ctx(ictx), oid, object_no,
                                 snapc, parent_trace, completion);
}

template <typename I>
ObjectRequest<I>*
ObjectRequest<I>::create_truncate(I *ictx, const std::string &oid,
                                  uint64_t object_no, uint64_t object_off,
                                  const ::SnapContext &snapc,
                                  const ZTracer::Trace &parent_trace,
				  Context *completion) {
  return new ObjectTruncateRequest(util::get_image_ctx(ictx), oid, object_no,
                                   object_off, snapc, parent_trace, completion);
}

template <typename I>
ObjectRequest<I>*
ObjectRequest<I>::create_write(I *ictx, const std::string &oid,
                               uint64_t object_no, uint64_t object_off,
                               const ceph::bufferlist &data,
                               const ::SnapContext &snapc, int op_flags,
			       const ZTracer::Trace &parent_trace,
                               Context *completion) {
  return new ObjectWriteRequest(util::get_image_ctx(ictx), oid, object_no,
                                object_off, data, snapc, op_flags, parent_trace,
				completion);
}

template <typename I>
ObjectRequest<I>*
ObjectRequest<I>::create_zero(I *ictx, const std::string &oid,
                              uint64_t object_no, uint64_t object_off,
                              uint64_t object_len,
                              const ::SnapContext &snapc,
			      const ZTracer::Trace &parent_trace,
                              Context *completion) {
  return new ObjectZeroRequest(util::get_image_ctx(ictx), oid, object_no,
                               object_off, object_len, snapc, parent_trace,
			       completion);
}

template <typename I>
ObjectRequest<I>*
ObjectRequest<I>::create_writesame(I *ictx, const std::string &oid,
                                   uint64_t object_no, uint64_t object_off,
                                   uint64_t object_len,
                                   const ceph::bufferlist &data,
                                   const ::SnapContext &snapc, int op_flags,
				   const ZTracer::Trace &parent_trace,
                                   Context *completion) {
  return new ObjectWriteSameRequest(util::get_image_ctx(ictx), oid, object_no,
                                    object_off, object_len, data, snapc,
                                    op_flags, parent_trace, completion);
}

template <typename I>
ObjectRequest<I>::ObjectRequest(ImageCtx *ictx, const std::string &oid,
                                uint64_t objectno, uint64_t off,
                                uint64_t len, librados::snap_t snap_id,
                                bool hide_enoent, const char *trace_name,
				const ZTracer::Trace &trace,
				Context *completion)
  : m_ictx(ictx), m_oid(oid), m_object_no(objectno), m_object_off(off),
    m_object_len(len), m_snap_id(snap_id), m_completion(completion),
    m_hide_enoent(hide_enoent),
    m_trace(util::create_trace(*ictx, "", trace)) {
  if (m_trace.valid()) {
    m_trace.copy_name(trace_name + std::string(" ") + oid);
    m_trace.event("start");
  }

  Striper::extent_to_file(m_ictx->cct, &m_ictx->layout, m_object_no,
                          0, m_ictx->layout.object_size, m_parent_extents);

  RWLock::RLocker snap_locker(m_ictx->snap_lock);
  RWLock::RLocker parent_locker(m_ictx->parent_lock);
  compute_parent_extents();
}

template <typename I>
void ObjectRequest<I>::complete(int r)
{
  if (should_complete(r)) {
    ldout(m_ictx->cct, 20) << dendl;
    if (m_hide_enoent && r == -ENOENT) {
      r = 0;
    }
    m_completion->complete(r);
    delete this;
  }
}

template <typename I>
bool ObjectRequest<I>::compute_parent_extents() {
  assert(m_ictx->snap_lock.is_locked());
  assert(m_ictx->parent_lock.is_locked());

  uint64_t parent_overlap;
  int r = m_ictx->get_parent_overlap(m_snap_id, &parent_overlap);
  if (r < 0) {
    // NOTE: it's possible for a snapshot to be deleted while we are
    // still reading from it
    lderr(m_ictx->cct) << "failed to retrieve parent overlap: "
                       << cpp_strerror(r)
                       << dendl;
    m_has_parent = false;
    m_parent_extents.clear();
    return false;
  }

  uint64_t object_overlap = m_ictx->prune_parent_extents(
    m_parent_extents, parent_overlap);
  if (object_overlap > 0) {
    ldout(m_ictx->cct, 20) << "overlap " << parent_overlap << " "
                           << "extents " << m_parent_extents << dendl;
    m_has_parent = !m_parent_extents.empty();
    return true;
  }
  return false;
}

static inline bool is_copy_on_read(ImageCtx *ictx, librados::snap_t snap_id) {
  assert(ictx->snap_lock.is_locked());
  return (ictx->clone_copy_on_read &&
          !ictx->read_only && snap_id == CEPH_NOSNAP &&
          (ictx->exclusive_lock == nullptr ||
           ictx->exclusive_lock->is_lock_owner()));
}

/** read **/

template <typename I>
ObjectReadRequest<I>::ObjectReadRequest(I *ictx, const std::string &oid,
                                        uint64_t objectno, uint64_t offset,
                                        uint64_t len, Extents& be,
                                        librados::snap_t snap_id, bool sparse,
					int op_flags,
					const ZTracer::Trace &parent_trace,
                                        Context *completion)
  : ObjectRequest<I>(util::get_image_ctx(ictx), oid, objectno, offset, len,
                     snap_id, false, "read", parent_trace, completion),
    m_buffer_extents(be), m_tried_parent(false), m_sparse(sparse),
    m_op_flags(op_flags), m_state(LIBRBD_AIO_READ_FLAT) {
  guard_read();
}

template <typename I>
void ObjectReadRequest<I>::guard_read()
{
  ImageCtx *image_ctx = this->m_ictx;
  RWLock::RLocker snap_locker(image_ctx->snap_lock);
  RWLock::RLocker parent_locker(image_ctx->parent_lock);

  if (this->has_parent()) {
    ldout(image_ctx->cct, 20) << "guarding read" << dendl;
    m_state = LIBRBD_AIO_READ_GUARD;
  }
}

template <typename I>
bool ObjectReadRequest<I>::should_complete(int r)
{
  ImageCtx *image_ctx = this->m_ictx;
  ldout(image_ctx->cct, 20) << this->m_oid << " "
                            << this->m_object_off << "~" << this->m_object_len
                            << " r = " << r << dendl;

  bool finished = true;

  switch (m_state) {
  case LIBRBD_AIO_READ_GUARD:
    ldout(image_ctx->cct, 20) << "READ_CHECK_GUARD" << dendl;

    // This is the step to read from parent
    if (!m_tried_parent && r == -ENOENT) {
      {
        RWLock::RLocker snap_locker(image_ctx->snap_lock);
        RWLock::RLocker parent_locker(image_ctx->parent_lock);
        if (image_ctx->parent == NULL) {
          ldout(image_ctx->cct, 20) << "parent is gone; do nothing" << dendl;
          break;
        }

        // calculate reverse mapping onto the image
        vector<pair<uint64_t,uint64_t> > parent_extents;
        Striper::extent_to_file(image_ctx->cct, &image_ctx->layout,
                                this->m_object_no, this->m_object_off,
                                this->m_object_len, parent_extents);

        uint64_t parent_overlap = 0;
        uint64_t object_overlap = 0;
        r = image_ctx->get_parent_overlap(this->m_snap_id, &parent_overlap);
        if (r == 0) {
          object_overlap = image_ctx->prune_parent_extents(parent_extents,
                                                           parent_overlap);
        }

        if (object_overlap > 0) {
          m_tried_parent = true;
          if (is_copy_on_read(image_ctx, this->m_snap_id)) {
            m_state = LIBRBD_AIO_READ_COPYUP;
          }

          read_from_parent(std::move(parent_extents));
          finished = false;
        }
      }
    }
    break;
  case LIBRBD_AIO_READ_COPYUP:
    ldout(image_ctx->cct, 20) << "READ_COPYUP" << dendl;
    // This is the extra step for copy-on-read: kick off an asynchronous copyup.
    // It is different from copy-on-write as asynchronous copyup will finish
    // by itself so state won't go back to LIBRBD_AIO_READ_GUARD.

    assert(m_tried_parent);
    if (r > 0) {
      // If read entire object from parent success and CoR is possible, kick
      // off a asynchronous copyup. This approach minimizes the latency
      // impact.
      send_copyup();
    }
    break;
  case LIBRBD_AIO_READ_FLAT:
    ldout(image_ctx->cct, 20) << "READ_FLAT" << dendl;
    // The read content should be deposit in m_read_data
    break;
  default:
    lderr(image_ctx->cct) << "invalid request state: " << m_state << dendl;
    ceph_abort();
  }

  return finished;
}

template <typename I>
void ObjectReadRequest<I>::send() {
  ImageCtx *image_ctx = this->m_ictx;
  ldout(image_ctx->cct, 20) << this->m_oid << " " << this->m_object_off
                            << "~" << this->m_object_len
                            << dendl;

  {
    RWLock::RLocker snap_locker(image_ctx->snap_lock);

    // send read request to parent if the object doesn't exist locally
    if (image_ctx->object_map != nullptr &&
        !image_ctx->object_map->object_may_exist(this->m_object_no)) {
      image_ctx->op_work_queue->queue(util::create_context_callback<
        ObjectRequest<I> >(this), -ENOENT);
      return;
    }
  }

  librados::ObjectReadOperation op;
  int flags = image_ctx->get_read_flags(this->m_snap_id);
  if (m_sparse) {
    op.sparse_read(this->m_object_off, this->m_object_len, &m_ext_map,
                   &m_read_data, nullptr);
  } else {
    op.read(this->m_object_off, this->m_object_len, &m_read_data, nullptr);
  }
  op.set_op_flags2(m_op_flags);

  librados::AioCompletion *rados_completion =
    util::create_rados_callback(this);
  int r = image_ctx->data_ctx.aio_operate(
    this->m_oid, rados_completion, &op, flags, nullptr,
    (this->m_trace.valid() ? this->m_trace.get_info() : nullptr));
  assert(r == 0);

  rados_completion->release();
}

template <typename I>
void ObjectReadRequest<I>::send_copyup()
{
  ImageCtx *image_ctx = this->m_ictx;
  ldout(image_ctx->cct, 20) << this->m_oid << " " << this->m_object_off
                            << "~" << this->m_object_len << dendl;

  {
    RWLock::RLocker snap_locker(image_ctx->snap_lock);
    RWLock::RLocker parent_locker(image_ctx->parent_lock);
    if (!this->compute_parent_extents() ||
        (image_ctx->exclusive_lock != nullptr &&
         !image_ctx->exclusive_lock->is_lock_owner())) {
      return;
    }
  }

  Mutex::Locker copyup_locker(image_ctx->copyup_list_lock);
  map<uint64_t, CopyupRequest*>::iterator it =
    image_ctx->copyup_list.find(this->m_object_no);
  if (it == image_ctx->copyup_list.end()) {
    // create and kick off a CopyupRequest
    CopyupRequest *new_req = new CopyupRequest(
      image_ctx, this->m_oid, this->m_object_no,
      std::move(this->m_parent_extents), this->m_trace);
    this->m_parent_extents.clear();

    image_ctx->copyup_list[this->m_object_no] = new_req;
    new_req->send();
  }
}

template <typename I>
void ObjectReadRequest<I>::read_from_parent(Extents&& parent_extents)
{
  ImageCtx *image_ctx = this->m_ictx;
  AioCompletion *parent_completion = AioCompletion::create_and_start<
    ObjectRequest<I> >(this, image_ctx, AIO_TYPE_READ);

  ldout(image_ctx->cct, 20) << "parent completion " << parent_completion
                            << " extents " << parent_extents << dendl;
  ImageRequest<>::aio_read(image_ctx->parent, parent_completion,
                           std::move(parent_extents),
                           ReadResult{&m_read_data}, 0, this->m_trace);
}

/** write **/

AbstractObjectWriteRequest::AbstractObjectWriteRequest(ImageCtx *ictx,
                                                       const std::string &oid,
                                                       uint64_t object_no,
                                                       uint64_t object_off,
                                                       uint64_t len,
                                                       const ::SnapContext &snapc,
						       bool hide_enoent,
						       const char *trace_name,
						       const ZTracer::Trace &parent_trace,
                                                       Context *completion)
  : ObjectRequest(ictx, oid, object_no, object_off, len, CEPH_NOSNAP,
                  hide_enoent, trace_name, parent_trace, completion),
    m_state(LIBRBD_AIO_WRITE_FLAT), m_snap_seq(snapc.seq.val)
{
  m_snaps.insert(m_snaps.end(), snapc.snaps.begin(), snapc.snaps.end());
}

void AbstractObjectWriteRequest::guard_write()
{
  if (has_parent()) {
    m_state = LIBRBD_AIO_WRITE_GUARD;
    m_write.assert_exists();
    ldout(m_ictx->cct, 20) << "guarding write" << dendl;
  }
}

bool AbstractObjectWriteRequest::should_complete(int r)
{
  ldout(m_ictx->cct, 20) << get_op_type() << m_oid << " "
                         << m_object_off << "~" << m_object_len
                         << " r = " << r << dendl;

  bool finished = true;
  switch (m_state) {
  case LIBRBD_AIO_WRITE_PRE:
    ldout(m_ictx->cct, 20) << "WRITE_PRE" << dendl;
    if (r < 0) {
      return true;
    }

    send_write_op();
    finished = false;
    break;

  case LIBRBD_AIO_WRITE_POST:
    ldout(m_ictx->cct, 20) << "WRITE_POST" << dendl;
    finished = true;
    break;

  case LIBRBD_AIO_WRITE_GUARD:
    ldout(m_ictx->cct, 20) << "WRITE_CHECK_GUARD" << dendl;

    if (r == -ENOENT) {
      handle_write_guard();
      finished = false;
      break;
    } else if (r < 0) {
      // pass the error code to the finish context
      m_state = LIBRBD_AIO_WRITE_ERROR;
      complete(r);
      finished = false;
      break;
    }

    finished = send_post_object_map_update();
    break;

  case LIBRBD_AIO_WRITE_COPYUP:
    ldout(m_ictx->cct, 20) << "WRITE_COPYUP" << dendl;
    if (r < 0) {
      m_state = LIBRBD_AIO_WRITE_ERROR;
      complete(r);
      finished = false;
    } else {
      finished = send_post_object_map_update();
    }
    break;

  case LIBRBD_AIO_WRITE_FLAT:
    ldout(m_ictx->cct, 20) << "WRITE_FLAT" << dendl;

    finished = send_post_object_map_update();
    break;

  case LIBRBD_AIO_WRITE_ERROR:
    assert(r < 0);
    lderr(m_ictx->cct) << "WRITE_ERROR: " << cpp_strerror(r) << dendl;
    break;

  default:
    lderr(m_ictx->cct) << "invalid request state: " << m_state << dendl;
    ceph_abort();
  }

  return finished;
}

void AbstractObjectWriteRequest::send() {
  ldout(m_ictx->cct, 20) << get_op_type() << " " << m_oid << " "
                         << m_object_off << "~" << m_object_len << dendl;
  {
    RWLock::RLocker snap_lock(m_ictx->snap_lock);
    if (m_ictx->object_map == nullptr) {
      m_object_exist = true;
    } else {
      // should have been flushed prior to releasing lock
      assert(m_ictx->exclusive_lock->is_lock_owner());
      m_object_exist = m_ictx->object_map->object_may_exist(m_object_no);
    }
  }

  send_write();
}

void AbstractObjectWriteRequest::send_pre_object_map_update() {
  ldout(m_ictx->cct, 20) << dendl;

  {
    RWLock::RLocker snap_lock(m_ictx->snap_lock);
    if (m_ictx->object_map != nullptr) {
      uint8_t new_state;
      pre_object_map_update(&new_state);
      RWLock::WLocker object_map_locker(m_ictx->object_map_lock);
      ldout(m_ictx->cct, 20) << m_oid << " " << m_object_off
                             << "~" << m_object_len << dendl;
      m_state = LIBRBD_AIO_WRITE_PRE;

      if (m_ictx->object_map->aio_update<ObjectRequest>(
            CEPH_NOSNAP, m_object_no, new_state, {}, this->m_trace, this)) {
        return;
      }
    }
  }

  send_write_op();
}

bool AbstractObjectWriteRequest::send_post_object_map_update() {
  ldout(m_ictx->cct, 20) << dendl;

  RWLock::RLocker snap_locker(m_ictx->snap_lock);
  if (m_ictx->object_map == nullptr || !post_object_map_update()) {
    return true;
  }

  // should have been flushed prior to releasing lock
  assert(m_ictx->exclusive_lock->is_lock_owner());

  RWLock::WLocker object_map_locker(m_ictx->object_map_lock);
  ldout(m_ictx->cct, 20) << m_oid << " " << m_object_off
                         << "~" << m_object_len << dendl;
  m_state = LIBRBD_AIO_WRITE_POST;

  if (m_ictx->object_map->aio_update<ObjectRequest>(
        CEPH_NOSNAP, m_object_no, OBJECT_NONEXISTENT, OBJECT_PENDING,
        this->m_trace, this)) {
    return false;
  }

  return true;
}

void AbstractObjectWriteRequest::send_write() {
  ldout(m_ictx->cct, 20) << m_oid << " " << m_object_off << "~" << m_object_len
                         << " object exist " << m_object_exist << dendl;

  if (!m_object_exist && has_parent()) {
    m_state = LIBRBD_AIO_WRITE_GUARD;
    handle_write_guard();
  } else {
    send_pre_object_map_update();
  }
}

void AbstractObjectWriteRequest::send_copyup()
{
  ldout(m_ictx->cct, 20) << m_oid << " " << m_object_off
                         << "~" << m_object_len << dendl;
  m_state = LIBRBD_AIO_WRITE_COPYUP;

  m_ictx->copyup_list_lock.Lock();
  map<uint64_t, CopyupRequest*>::iterator it =
    m_ictx->copyup_list.find(m_object_no);
  if (it == m_ictx->copyup_list.end()) {
    CopyupRequest *new_req = new CopyupRequest(m_ictx, m_oid,
                                               m_object_no,
                                               std::move(m_parent_extents),
					       this->m_trace);
    m_parent_extents.clear();

    // make sure to wait on this CopyupRequest
    new_req->append_request(this);
    m_ictx->copyup_list[m_object_no] = new_req;

    m_ictx->copyup_list_lock.Unlock();
    new_req->send();
  } else {
    it->second->append_request(this);
    m_ictx->copyup_list_lock.Unlock();
  }
}
void AbstractObjectWriteRequest::send_write_op()
{
  m_state = LIBRBD_AIO_WRITE_FLAT;
  if (m_guard) {
    guard_write();
  }

  add_write_ops(&m_write, true);
  assert(m_write.size() != 0);

  librados::AioCompletion *rados_completion =
    util::create_rados_callback(this);
  int r = m_ictx->data_ctx.aio_operate(
    m_oid, rados_completion, &m_write, m_snap_seq, m_snaps,
    (this->m_trace.valid() ? this->m_trace.get_info() : nullptr));
  assert(r == 0);
  rados_completion->release();
}
void AbstractObjectWriteRequest::handle_write_guard()
{
  bool has_parent;
  {
    RWLock::RLocker snap_locker(m_ictx->snap_lock);
    RWLock::RLocker parent_locker(m_ictx->parent_lock);
    has_parent = compute_parent_extents();
  }
  // If parent still exists, overlap might also have changed.
  if (has_parent) {
    send_copyup();
  } else {
    // parent may have disappeared -- send original write again
    ldout(m_ictx->cct, 20) << "should_complete(" << this
                           << "): parent overlap now 0" << dendl;
    send_write();
  }
}

void ObjectWriteRequest::add_write_ops(librados::ObjectWriteOperation *wr,
                                       bool set_hints) {
  RWLock::RLocker snap_locker(m_ictx->snap_lock);
  if (set_hints && m_ictx->enable_alloc_hint &&
      (m_ictx->object_map == nullptr || !m_object_exist)) {
    wr->set_alloc_hint(m_ictx->get_object_size(), m_ictx->get_object_size());
  }

  if (m_object_off == 0 && m_object_len == m_ictx->get_object_size()) {
    wr->write_full(m_write_data);
  } else {
    wr->write(m_object_off, m_write_data);
  }
  wr->set_op_flags2(m_op_flags);
}

void ObjectWriteRequest::send_write() {
  bool write_full = (m_object_off == 0 && m_object_len == m_ictx->get_object_size());
  ldout(m_ictx->cct, 20) << m_oid << " " << m_object_off << "~" << m_object_len
                         << " object exist " << m_object_exist
                         << " write_full " << write_full << dendl;
  if (write_full && !has_parent()) {
    m_guard = false;
  }

  AbstractObjectWriteRequest::send_write();
}

void ObjectRemoveRequest::guard_write() {
  // do nothing to disable write guard only if deep-copyup not required
  RWLock::RLocker snap_locker(m_ictx->snap_lock);
  if (!m_ictx->snaps.empty()) {
    AbstractObjectWriteRequest::guard_write();
  }
}
void ObjectRemoveRequest::send_write() {
  ldout(m_ictx->cct, 20) << m_oid << " remove " << " object exist "
                         << m_object_exist << dendl;
  if (!m_object_exist && !has_parent()) {
    m_state = LIBRBD_AIO_WRITE_FLAT;
    Context *ctx = util::create_context_callback<ObjectRequest>(this);
    m_ictx->op_work_queue->queue(ctx, 0);
  } else {
    send_pre_object_map_update();
  }
}

void ObjectTruncateRequest::send_write() {
  ldout(m_ictx->cct, 20) << m_oid << " truncate " << m_object_off
                         << " object exist " << m_object_exist << dendl;
  if (!m_object_exist && !has_parent()) {
    m_state = LIBRBD_AIO_WRITE_FLAT;
    Context *ctx = util::create_context_callback<ObjectRequest>(this);
    m_ictx->op_work_queue->queue(ctx, 0);
  } else {
    AbstractObjectWriteRequest::send_write();
  }
}

void ObjectZeroRequest::send_write() {
  ldout(m_ictx->cct, 20) << m_oid << " zero " << m_object_off << "~"
                         << m_object_len << " object exist " << m_object_exist
                         << dendl;
  if (!m_object_exist && !has_parent()) {
    m_state = LIBRBD_AIO_WRITE_FLAT;
    Context *ctx = util::create_context_callback<ObjectRequest>(this);
    m_ictx->op_work_queue->queue(ctx, 0);
  } else {
    AbstractObjectWriteRequest::send_write();
  }
}

void ObjectWriteSameRequest::add_write_ops(librados::ObjectWriteOperation *wr,
                                           bool set_hints) {
  RWLock::RLocker snap_locker(m_ictx->snap_lock);
  if (set_hints && m_ictx->enable_alloc_hint &&
      (m_ictx->object_map == nullptr || !m_object_exist)) {
    wr->set_alloc_hint(m_ictx->get_object_size(), m_ictx->get_object_size());
  }

  wr->writesame(m_object_off, m_object_len, m_write_data);
  wr->set_op_flags2(m_op_flags);
}

void ObjectWriteSameRequest::send_write() {
  bool write_full = (m_object_off == 0 && m_object_len == m_ictx->get_object_size());
  ldout(m_ictx->cct, 20) << m_oid << " " << m_object_off << "~" << m_object_len
                         << " write_full " << write_full << dendl;
  if (write_full && !has_parent()) {
    m_guard = false;
  }

  AbstractObjectWriteRequest::send_write();
}

} // namespace io
} // namespace librbd

template class librbd::io::ObjectRequest<librbd::ImageCtx>;
template class librbd::io::ObjectReadRequest<librbd::ImageCtx>;
