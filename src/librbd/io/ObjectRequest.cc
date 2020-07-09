// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/io/ObjectRequest.h"
#include "common/ceph_context.h"
#include "common/dout.h"
#include "common/errno.h"
#include "common/ceph_mutex.h"
#include "include/Context.h"
#include "include/err.h"
#include "include/neorados/RADOS.hpp"
#include "osd/osd_types.h"

#include "librbd/AsioEngine.h"
#include "librbd/ExclusiveLock.h"
#include "librbd/ImageCtx.h"
#include "librbd/ObjectMap.h"
#include "librbd/Utils.h"
#include "librbd/asio/Utils.h"
#include "librbd/io/AioCompletion.h"
#include "librbd/io/CopyupRequest.h"
#include "librbd/io/ImageRequest.h"
#include "librbd/io/ReadResult.h"
#include "librbd/io/Utils.h"

#include <boost/bind.hpp>
#include <boost/optional.hpp>

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::io::ObjectRequest: " << this           \
                           << " " << __func__ << ": "                         \
                           << data_object_name(this->m_ictx,                  \
                                               this->m_object_no) << " "

namespace librbd {
namespace io {

using librbd::util::data_object_name;

namespace {

template <typename I>
inline bool is_copy_on_read(I *ictx, librados::snap_t snap_id) {
  std::shared_lock image_locker{ictx->image_lock};
  return (ictx->clone_copy_on_read &&
          !ictx->read_only && snap_id == CEPH_NOSNAP &&
          (ictx->exclusive_lock == nullptr ||
           ictx->exclusive_lock->is_lock_owner()));
}

} // anonymous namespace

template <typename I>
ObjectRequest<I>*
ObjectRequest<I>::create_write(
    I *ictx, uint64_t object_no, uint64_t object_off, ceph::bufferlist&& data,
    const ::SnapContext &snapc, int op_flags,
    const ZTracer::Trace &parent_trace, Context *completion) {
  return new ObjectWriteRequest<I>(ictx, object_no, object_off,
                                   std::move(data), snapc, op_flags,
                                   parent_trace, completion);
}

template <typename I>
ObjectRequest<I>*
ObjectRequest<I>::create_discard(
    I *ictx, uint64_t object_no, uint64_t object_off, uint64_t object_len,
    const ::SnapContext &snapc, int discard_flags,
    const ZTracer::Trace &parent_trace, Context *completion) {
  return new ObjectDiscardRequest<I>(ictx, object_no, object_off,
                                     object_len, snapc, discard_flags,
                                     parent_trace, completion);
}

template <typename I>
ObjectRequest<I>*
ObjectRequest<I>::create_write_same(
    I *ictx, uint64_t object_no, uint64_t object_off, uint64_t object_len,
    ceph::bufferlist&& data, const ::SnapContext &snapc, int op_flags,
    const ZTracer::Trace &parent_trace, Context *completion) {
  return new ObjectWriteSameRequest<I>(ictx, object_no, object_off,
                                       object_len, std::move(data), snapc,
                                       op_flags, parent_trace, completion);
}

template <typename I>
ObjectRequest<I>*
ObjectRequest<I>::create_compare_and_write(
    I *ictx, uint64_t object_no, uint64_t object_off,
    ceph::bufferlist&& cmp_data, ceph::bufferlist&& write_data,
    const ::SnapContext &snapc, uint64_t *mismatch_offset, int op_flags,
    const ZTracer::Trace &parent_trace, Context *completion) {
  return new ObjectCompareAndWriteRequest<I>(ictx, object_no, object_off,
                                             std::move(cmp_data),
                                             std::move(write_data), snapc,
                                             mismatch_offset, op_flags,
                                             parent_trace, completion);
}

template <typename I>
ObjectRequest<I>::ObjectRequest(
    I *ictx, uint64_t objectno, uint64_t off, uint64_t len,
    librados::snap_t snap_id, const char *trace_name,
    const ZTracer::Trace &trace, Context *completion)
  : m_ictx(ictx), m_object_no(objectno), m_object_off(off),
    m_object_len(len), m_snap_id(snap_id), m_completion(completion),
    m_trace(librbd::util::create_trace(*ictx, "", trace)) {
  ceph_assert(m_ictx->data_ctx.is_valid());
  if (m_trace.valid()) {
    m_trace.copy_name(trace_name + std::string(" ") +
                      data_object_name(ictx, objectno));
    m_trace.event("start");
  }
}

template <typename I>
void ObjectRequest<I>::add_write_hint(I& image_ctx, neorados::WriteOp* wr) {
  auto alloc_hint_flags = static_cast<neorados::alloc_hint::alloc_hint_t>(
    image_ctx.alloc_hint_flags);
  if (image_ctx.enable_alloc_hint) {
    wr->set_alloc_hint(image_ctx.get_object_size(),
                       image_ctx.get_object_size(),
                       alloc_hint_flags);
  } else if (image_ctx.alloc_hint_flags != 0U) {
    wr->set_alloc_hint(0, 0, alloc_hint_flags);
  }
}

template <typename I>
bool ObjectRequest<I>::compute_parent_extents(Extents *parent_extents,
                                              bool read_request) {
  ceph_assert(ceph_mutex_is_locked(m_ictx->image_lock));

  m_has_parent = false;
  parent_extents->clear();

  uint64_t parent_overlap;
  int r = m_ictx->get_parent_overlap(m_snap_id, &parent_overlap);
  if (r < 0) {
    // NOTE: it's possible for a snapshot to be deleted while we are
    // still reading from it
    lderr(m_ictx->cct) << "failed to retrieve parent overlap: "
                       << cpp_strerror(r) << dendl;
    return false;
  }

  if (!read_request && !m_ictx->migration_info.empty()) {
    parent_overlap = m_ictx->migration_info.overlap;
  }

  if (parent_overlap == 0) {
    return false;
  }

  Striper::extent_to_file(m_ictx->cct, &m_ictx->layout, m_object_no, 0,
                          m_ictx->layout.object_size, *parent_extents);
  uint64_t object_overlap = m_ictx->prune_parent_extents(*parent_extents,
                                                         parent_overlap);
  if (object_overlap > 0) {
    ldout(m_ictx->cct, 20) << "overlap " << parent_overlap << " "
                           << "extents " << *parent_extents << dendl;
    m_has_parent = !parent_extents->empty();
    return true;
  }
  return false;
}

template <typename I>
void ObjectRequest<I>::async_finish(int r) {
  ldout(m_ictx->cct, 20) << "r=" << r << dendl;
  m_ictx->asio_engine->post([this, r]() { finish(r); });
}

template <typename I>
void ObjectRequest<I>::finish(int r) {
  ldout(m_ictx->cct, 20) << "r=" << r << dendl;
  m_completion->complete(r);
  delete this;
}

/** read **/

template <typename I>
ObjectReadRequest<I>::ObjectReadRequest(
    I *ictx, uint64_t objectno, uint64_t offset, uint64_t len,
    librados::snap_t snap_id, int op_flags, const ZTracer::Trace &parent_trace,
    bufferlist* read_data, Extents* extent_map, Context *completion)
  : ObjectRequest<I>(ictx, objectno, offset, len, snap_id, "read",
                     parent_trace, completion),
    m_op_flags(op_flags), m_read_data(read_data), m_extent_map(extent_map) {
}

template <typename I>
void ObjectReadRequest<I>::send() {
  I *image_ctx = this->m_ictx;
  ldout(image_ctx->cct, 20) << dendl;

  read_object();
}

template <typename I>
void ObjectReadRequest<I>::read_object() {
  I *image_ctx = this->m_ictx;
  {
    std::shared_lock image_locker{image_ctx->image_lock};
    if (image_ctx->object_map != nullptr &&
        !image_ctx->object_map->object_may_exist(this->m_object_no)) {
      image_ctx->asio_engine->post([this]() { read_parent(); });
      return;
    }
  }

  ldout(image_ctx->cct, 20) << dendl;

  neorados::ReadOp read_op;
  if (this->m_object_len >= image_ctx->sparse_read_threshold_bytes) {
    read_op.sparse_read(this->m_object_off, this->m_object_len, m_read_data,
                        m_extent_map);
  } else {
    read_op.read(this->m_object_off, this->m_object_len, m_read_data);
  }
  util::apply_op_flags(m_op_flags, image_ctx->get_read_flags(this->m_snap_id),
                       &read_op);

  image_ctx->rados_api.execute(
    {data_object_name(this->m_ictx, this->m_object_no)},
    *image_ctx->get_data_io_context(), std::move(read_op), nullptr,
    librbd::asio::util::get_callback_adapter(
      [this](int r) { handle_read_object(r); }), nullptr,
      (this->m_trace.valid() ? this->m_trace.get_info() : nullptr));
}

template <typename I>
void ObjectReadRequest<I>::handle_read_object(int r) {
  I *image_ctx = this->m_ictx;
  ldout(image_ctx->cct, 20) << "r=" << r << dendl;

  if (r == -ENOENT) {
    read_parent();
    return;
  } else if (r < 0) {
    lderr(image_ctx->cct) << "failed to read from object: "
                          << cpp_strerror(r) << dendl;
    this->finish(r);
    return;
  }

  this->finish(0);
}

template <typename I>
void ObjectReadRequest<I>::read_parent() {
  I *image_ctx = this->m_ictx;

  std::shared_lock image_locker{image_ctx->image_lock};

  // calculate reverse mapping onto the image
  Extents parent_extents;
  Striper::extent_to_file(image_ctx->cct, &image_ctx->layout,
                          this->m_object_no, this->m_object_off,
                          this->m_object_len, parent_extents);

  uint64_t parent_overlap = 0;
  uint64_t object_overlap = 0;
  int r = image_ctx->get_parent_overlap(this->m_snap_id, &parent_overlap);
  if (r == 0) {
    object_overlap = image_ctx->prune_parent_extents(parent_extents,
                                                     parent_overlap);
  }

  if (object_overlap == 0) {
    image_locker.unlock();

    this->finish(-ENOENT);
    return;
  }

  ldout(image_ctx->cct, 20) << dendl;

  auto parent_completion = AioCompletion::create_and_start<
    ObjectReadRequest<I>, &ObjectReadRequest<I>::handle_read_parent>(
      this, librbd::util::get_image_ctx(image_ctx->parent), AIO_TYPE_READ);
  ImageRequest<I>::aio_read(image_ctx->parent, parent_completion,
                            std::move(parent_extents), ReadResult{m_read_data},
                            0, this->m_trace);
}

template <typename I>
void ObjectReadRequest<I>::handle_read_parent(int r) {
  I *image_ctx = this->m_ictx;
  ldout(image_ctx->cct, 20) << "r=" << r << dendl;

  if (r == -ENOENT) {
    this->finish(r);
    return;
  } else if (r < 0) {
    lderr(image_ctx->cct) << "failed to read parent extents: "
                          << cpp_strerror(r) << dendl;
    this->finish(r);
    return;
  }

  copyup();
}

template <typename I>
void ObjectReadRequest<I>::copyup() {
  I *image_ctx = this->m_ictx;
  if (!is_copy_on_read(image_ctx, this->m_snap_id)) {
    this->finish(0);
    return;
  }

  image_ctx->owner_lock.lock_shared();
  image_ctx->image_lock.lock_shared();
  Extents parent_extents;
  if (!this->compute_parent_extents(&parent_extents, true) ||
      (image_ctx->exclusive_lock != nullptr &&
       !image_ctx->exclusive_lock->is_lock_owner())) {
    image_ctx->image_lock.unlock_shared();
    image_ctx->owner_lock.unlock_shared();
    this->finish(0);
    return;
  }

  ldout(image_ctx->cct, 20) << dendl;

  image_ctx->copyup_list_lock.lock();
  auto it = image_ctx->copyup_list.find(this->m_object_no);
  if (it == image_ctx->copyup_list.end()) {
    // create and kick off a CopyupRequest
    auto new_req = CopyupRequest<I>::create(
      image_ctx, this->m_object_no, std::move(parent_extents), this->m_trace);

    image_ctx->copyup_list[this->m_object_no] = new_req;
    image_ctx->copyup_list_lock.unlock();
    image_ctx->image_lock.unlock_shared();
    new_req->send();
  } else {
    image_ctx->copyup_list_lock.unlock();
    image_ctx->image_lock.unlock_shared();
  }

  image_ctx->owner_lock.unlock_shared();
  this->finish(0);
}

/** write **/

template <typename I>
AbstractObjectWriteRequest<I>::AbstractObjectWriteRequest(
    I *ictx, uint64_t object_no, uint64_t object_off, uint64_t len,
    const ::SnapContext &snapc, const char *trace_name,
    const ZTracer::Trace &parent_trace, Context *completion)
  : ObjectRequest<I>(ictx, object_no, object_off, len, CEPH_NOSNAP, trace_name,
                     parent_trace, completion),
    m_snap_seq(snapc.seq.val)
{
  m_snaps.insert(m_snaps.end(), snapc.snaps.begin(), snapc.snaps.end());

  if (this->m_object_off == 0 &&
      this->m_object_len == ictx->get_object_size()) {
    m_full_object = true;
  }

  compute_parent_info();

  ictx->image_lock.lock_shared();
  if (!ictx->migration_info.empty()) {
    m_guarding_migration_write = true;
  }
  ictx->image_lock.unlock_shared();
}

template <typename I>
void AbstractObjectWriteRequest<I>::compute_parent_info() {
  I *image_ctx = this->m_ictx;
  std::shared_lock image_locker{image_ctx->image_lock};

  this->compute_parent_extents(&m_parent_extents, false);

  if (!this->has_parent() ||
      (m_full_object && m_snaps.empty() && !is_post_copyup_write_required())) {
    m_copyup_enabled = false;
  }
}

template <typename I>
void AbstractObjectWriteRequest<I>::add_write_hint(
    neorados::WriteOp *wr) {
  I *image_ctx = this->m_ictx;
  std::shared_lock image_locker{image_ctx->image_lock};
  if (image_ctx->object_map == nullptr || !this->m_object_may_exist) {
    ObjectRequest<I>::add_write_hint(*image_ctx, wr);
  }
}

template <typename I>
void AbstractObjectWriteRequest<I>::send() {
  I *image_ctx = this->m_ictx;
  ldout(image_ctx->cct, 20) << this->get_op_type() << " "
                            << this->m_object_off << "~" << this->m_object_len
                            << dendl;
  {
    std::shared_lock image_lock{image_ctx->image_lock};
    if (image_ctx->object_map == nullptr) {
      m_object_may_exist = true;
    } else {
      // should have been flushed prior to releasing lock
      ceph_assert(image_ctx->exclusive_lock->is_lock_owner());
      m_object_may_exist = image_ctx->object_map->object_may_exist(
        this->m_object_no);
    }
  }

  if (!m_object_may_exist && is_no_op_for_nonexistent_object()) {
    ldout(image_ctx->cct, 20) << "skipping no-op on nonexistent object"
                              << dendl;
    this->async_finish(0);
    return;
  }

  pre_write_object_map_update();
}

template <typename I>
void AbstractObjectWriteRequest<I>::pre_write_object_map_update() {
  I *image_ctx = this->m_ictx;

  image_ctx->image_lock.lock_shared();
  if (image_ctx->object_map == nullptr || !is_object_map_update_enabled()) {
    image_ctx->image_lock.unlock_shared();
    write_object();
    return;
  }

  if (!m_object_may_exist && m_copyup_enabled) {
    // optimization: copyup required
    image_ctx->image_lock.unlock_shared();
    copyup();
    return;
  }

  uint8_t new_state = this->get_pre_write_object_map_state();
  ldout(image_ctx->cct, 20) << this->m_object_off << "~" << this->m_object_len
                            << dendl;

  if (image_ctx->object_map->template aio_update<
        AbstractObjectWriteRequest<I>,
        &AbstractObjectWriteRequest<I>::handle_pre_write_object_map_update>(
          CEPH_NOSNAP, this->m_object_no, new_state, {}, this->m_trace, false,
          this)) {
    image_ctx->image_lock.unlock_shared();
    return;
  }

  image_ctx->image_lock.unlock_shared();
  write_object();
}

template <typename I>
void AbstractObjectWriteRequest<I>::handle_pre_write_object_map_update(int r) {
  I *image_ctx = this->m_ictx;
  ldout(image_ctx->cct, 20) << "r=" << r << dendl;
  if (r < 0) {
    lderr(image_ctx->cct) << "failed to update object map: "
                          << cpp_strerror(r) << dendl;
    this->finish(r);
    return;
  }

  write_object();
}

template <typename I>
void AbstractObjectWriteRequest<I>::write_object() {
  I *image_ctx = this->m_ictx;
  ldout(image_ctx->cct, 20) << dendl;

  neorados::WriteOp write_op;
  if (m_copyup_enabled) {
    ldout(image_ctx->cct, 20) << "guarding write" << dendl;
    if (m_guarding_migration_write) {
      cls_client::assert_snapc_seq(
        &write_op, m_snap_seq, cls::rbd::ASSERT_SNAPC_SEQ_LE_SNAPSET_SEQ);
    } else {
      write_op.assert_exists();
    }
  }

  add_write_hint(&write_op);
  add_write_ops(&write_op);
  ceph_assert(write_op.size() != 0);

  image_ctx->rados_api.execute(
    {data_object_name(this->m_ictx, this->m_object_no)},
    *image_ctx->get_data_io_context(), std::move(write_op),
    librbd::asio::util::get_callback_adapter(
      [this](int r) { handle_write_object(r); }), nullptr,
      (this->m_trace.valid() ? this->m_trace.get_info() : nullptr));
}

template <typename I>
void AbstractObjectWriteRequest<I>::handle_write_object(int r) {
  I *image_ctx = this->m_ictx;
  ldout(image_ctx->cct, 20) << "r=" << r << dendl;

  r = filter_write_result(r);
  if (r == -ENOENT) {
    if (m_copyup_enabled) {
      copyup();
      return;
    }
  } else if (r == -ERANGE && m_guarding_migration_write) {
    image_ctx->image_lock.lock_shared();
    m_guarding_migration_write = !image_ctx->migration_info.empty();
    image_ctx->image_lock.unlock_shared();

    if (m_guarding_migration_write) {
      copyup();
    } else {
      ldout(image_ctx->cct, 10) << "migration parent gone, restart io" << dendl;
      compute_parent_info();
      write_object();
    }
    return;
  } else if (r == -EILSEQ) {
    ldout(image_ctx->cct, 10) << "failed to write object" << dendl;
    this->finish(r);
    return;
  } else if (r < 0) {
    lderr(image_ctx->cct) << "failed to write object: " << cpp_strerror(r)
                          << dendl;
    this->finish(r);
    return;
  }

  post_write_object_map_update();
}

template <typename I>
void AbstractObjectWriteRequest<I>::copyup() {
  I *image_ctx = this->m_ictx;
  ldout(image_ctx->cct, 20) << dendl;

  ceph_assert(!m_copyup_in_progress);
  m_copyup_in_progress = true;

  image_ctx->copyup_list_lock.lock();
  auto it = image_ctx->copyup_list.find(this->m_object_no);
  if (it == image_ctx->copyup_list.end()) {
    auto new_req = CopyupRequest<I>::create(
      image_ctx, this->m_object_no, std::move(this->m_parent_extents),
      this->m_trace);
    this->m_parent_extents.clear();

    // make sure to wait on this CopyupRequest
    new_req->append_request(this);
    image_ctx->copyup_list[this->m_object_no] = new_req;

    image_ctx->copyup_list_lock.unlock();
    new_req->send();
  } else {
    it->second->append_request(this);
    image_ctx->copyup_list_lock.unlock();
  }
}

template <typename I>
void AbstractObjectWriteRequest<I>::handle_copyup(int r) {
  I *image_ctx = this->m_ictx;
  ldout(image_ctx->cct, 20) << "r=" << r << dendl;

  ceph_assert(m_copyup_in_progress);
  m_copyup_in_progress = false;

  if (r < 0 && r != -ERESTART) {
    lderr(image_ctx->cct) << "failed to copyup object: " << cpp_strerror(r)
                          << dendl;
    this->finish(r);
    return;
  }

  if (r == -ERESTART || is_post_copyup_write_required()) {
    write_object();
    return;
  }

  post_write_object_map_update();
}

template <typename I>
void AbstractObjectWriteRequest<I>::post_write_object_map_update() {
  I *image_ctx = this->m_ictx;

  image_ctx->image_lock.lock_shared();
  if (image_ctx->object_map == nullptr || !is_object_map_update_enabled() ||
      !is_non_existent_post_write_object_map_state()) {
    image_ctx->image_lock.unlock_shared();
    this->finish(0);
    return;
  }

  ldout(image_ctx->cct, 20) << dendl;

  // should have been flushed prior to releasing lock
  ceph_assert(image_ctx->exclusive_lock->is_lock_owner());
  if (image_ctx->object_map->template aio_update<
        AbstractObjectWriteRequest<I>,
        &AbstractObjectWriteRequest<I>::handle_post_write_object_map_update>(
          CEPH_NOSNAP, this->m_object_no, OBJECT_NONEXISTENT, OBJECT_PENDING,
          this->m_trace, false, this)) {
    image_ctx->image_lock.unlock_shared();
    return;
  }

  image_ctx->image_lock.unlock_shared();
  this->finish(0);
}

template <typename I>
void AbstractObjectWriteRequest<I>::handle_post_write_object_map_update(int r) {
  I *image_ctx = this->m_ictx;
  ldout(image_ctx->cct, 20) << "r=" << r << dendl;
  if (r < 0) {
    lderr(image_ctx->cct) << "failed to update object map: "
                          << cpp_strerror(r) << dendl;
    this->finish(r);
    return;
  }

  this->finish(0);
}

template <typename I>
void ObjectWriteRequest<I>::add_write_ops(neorados::WriteOp* wr) {
  if (this->m_full_object) {
    wr->write_full(bufferlist{m_write_data});
  } else {
    wr->write(this->m_object_off, bufferlist{m_write_data});
  }
  util::apply_op_flags(m_op_flags, 0U, wr);
}

template <typename I>
void ObjectDiscardRequest<I>::add_write_ops(neorados::WriteOp* wr) {
  switch (m_discard_action) {
  case DISCARD_ACTION_REMOVE:
    wr->remove();
    break;
  case DISCARD_ACTION_REMOVE_TRUNCATE:
    wr->create(false);
    // fall through
  case DISCARD_ACTION_TRUNCATE:
    wr->truncate(this->m_object_off);
    break;
  case DISCARD_ACTION_ZERO:
    wr->zero(this->m_object_off, this->m_object_len);
    break;
  default:
    ceph_abort();
    break;
  }
}

template <typename I>
void ObjectWriteSameRequest<I>::add_write_ops(neorados::WriteOp* wr) {
  wr->writesame(this->m_object_off, this->m_object_len,
                bufferlist{m_write_data});
  util::apply_op_flags(m_op_flags, 0U, wr);
}

template <typename I>
void ObjectCompareAndWriteRequest<I>::add_write_ops(neorados::WriteOp* wr) {
  wr->cmpext(this->m_object_off, bufferlist{m_cmp_bl}, nullptr);

  if (this->m_full_object) {
    wr->write_full(bufferlist{m_write_bl});
  } else {
    wr->write(this->m_object_off, bufferlist{m_write_bl});
  }
  util::apply_op_flags(m_op_flags, 0U, wr);
}

template <typename I>
int ObjectCompareAndWriteRequest<I>::filter_write_result(int r) const {
  if (r <= -MAX_ERRNO) {
    I *image_ctx = this->m_ictx;
    Extents image_extents;

    // object extent compare mismatch
    uint64_t offset = -MAX_ERRNO - r;
    Striper::extent_to_file(image_ctx->cct, &image_ctx->layout,
                            this->m_object_no, offset, this->m_object_len,
                            image_extents);
    ceph_assert(image_extents.size() == 1);

    if (m_mismatch_offset) {
      *m_mismatch_offset = image_extents[0].first;
    }
    r = -EILSEQ;
  }
  return r;
}

} // namespace io
} // namespace librbd

template class librbd::io::ObjectRequest<librbd::ImageCtx>;
template class librbd::io::ObjectReadRequest<librbd::ImageCtx>;
template class librbd::io::AbstractObjectWriteRequest<librbd::ImageCtx>;
template class librbd::io::ObjectWriteRequest<librbd::ImageCtx>;
template class librbd::io::ObjectDiscardRequest<librbd::ImageCtx>;
template class librbd::io::ObjectWriteSameRequest<librbd::ImageCtx>;
template class librbd::io::ObjectCompareAndWriteRequest<librbd::ImageCtx>;
