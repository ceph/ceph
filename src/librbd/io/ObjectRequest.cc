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
#include "librados/snap_set_diff.h"
#include "librbd/AsioEngine.h"
#include "librbd/ExclusiveLock.h"
#include "librbd/ImageCtx.h"
#include "librbd/ObjectMap.h"
#include "librbd/Utils.h"
#include "librbd/asio/Utils.h"
#include "librbd/io/AioCompletion.h"
#include "librbd/io/CopyupRequest.h"
#include "librbd/io/ImageRequest.h"
#include "librbd/io/Utils.h"

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
using librbd::util::create_context_callback;
using librbd::util::create_trace;

namespace {

template <typename I>
inline bool is_copy_on_read(I *ictx, const IOContext& io_context) {
  std::shared_lock image_locker{ictx->image_lock};
  return (ictx->clone_copy_on_read && !ictx->read_only &&
          io_context->get_read_snap() == CEPH_NOSNAP &&
          (ictx->exclusive_lock == nullptr ||
           ictx->exclusive_lock->is_lock_owner()));
}

template <typename S, typename D>
void convert_snap_set(const S& src_snap_set,
                      D* dst_snap_set) {
  dst_snap_set->seq = src_snap_set.seq;
  dst_snap_set->clones.reserve(src_snap_set.clones.size());
  for (auto& src_clone : src_snap_set.clones) {
    dst_snap_set->clones.emplace_back();
    auto& dst_clone = dst_snap_set->clones.back();
    dst_clone.cloneid = src_clone.cloneid;
    dst_clone.snaps = src_clone.snaps;
    dst_clone.overlap = src_clone.overlap;
    dst_clone.size = src_clone.size;
  }
}

} // anonymous namespace

template <typename I>
ObjectRequest<I>*
ObjectRequest<I>::create_write(
    I *ictx, uint64_t object_no, uint64_t object_off, ceph::bufferlist&& data,
    IOContext io_context, int op_flags, int write_flags,
    std::optional<uint64_t> assert_version,
    const ZTracer::Trace &parent_trace, Context *completion) {
  return new ObjectWriteRequest<I>(ictx, object_no, object_off,
                                   std::move(data), io_context, op_flags,
                                   write_flags, assert_version,
                                   parent_trace, completion);
}

template <typename I>
ObjectRequest<I>*
ObjectRequest<I>::create_discard(
    I *ictx, uint64_t object_no, uint64_t object_off, uint64_t object_len,
    IOContext io_context, int discard_flags,
    const ZTracer::Trace &parent_trace, Context *completion) {
  return new ObjectDiscardRequest<I>(ictx, object_no, object_off,
                                     object_len, io_context, discard_flags,
                                     parent_trace, completion);
}

template <typename I>
ObjectRequest<I>*
ObjectRequest<I>::create_write_same(
    I *ictx, uint64_t object_no, uint64_t object_off, uint64_t object_len,
    ceph::bufferlist&& data, IOContext io_context, int op_flags,
    const ZTracer::Trace &parent_trace, Context *completion) {
  return new ObjectWriteSameRequest<I>(ictx, object_no, object_off,
                                       object_len, std::move(data), io_context,
                                       op_flags, parent_trace, completion);
}

template <typename I>
ObjectRequest<I>*
ObjectRequest<I>::create_compare_and_write(
    I *ictx, uint64_t object_no, uint64_t object_off,
    ceph::bufferlist&& cmp_data, ceph::bufferlist&& write_data,
    IOContext io_context, uint64_t *mismatch_offset, int op_flags,
    const ZTracer::Trace &parent_trace, Context *completion) {
  return new ObjectCompareAndWriteRequest<I>(ictx, object_no, object_off,
                                             std::move(cmp_data),
                                             std::move(write_data), io_context,
                                             mismatch_offset, op_flags,
                                             parent_trace, completion);
}

template <typename I>
ObjectRequest<I>::ObjectRequest(
    I *ictx, uint64_t objectno, IOContext io_context,
    const char *trace_name, const ZTracer::Trace &trace, Context *completion)
  : m_ictx(ictx), m_object_no(objectno), m_io_context(io_context),
    m_completion(completion),
    m_trace(create_trace(*ictx, "", trace)) {
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
                                              ImageArea *area,
                                              bool read_request) {
  ceph_assert(ceph_mutex_is_locked(m_ictx->image_lock));

  m_has_parent = false;
  parent_extents->clear();
  *area = ImageArea::DATA;

  uint64_t raw_overlap;
  int r = m_ictx->get_parent_overlap(
      m_io_context->get_read_snap(), &raw_overlap);
  if (r < 0) {
    // NOTE: it's possible for a snapshot to be deleted while we are
    // still reading from it
    lderr(m_ictx->cct) << "failed to retrieve parent overlap: "
                       << cpp_strerror(r) << dendl;
    return false;
  }
  bool migration_write = !read_request && !m_ictx->migration_info.empty();
  if (migration_write) {
    raw_overlap = m_ictx->migration_info.overlap;
  }
  if (raw_overlap == 0) {
    return false;
  }

  std::tie(*parent_extents, *area) = io::util::object_to_area_extents(
      m_ictx, m_object_no, {{0, m_ictx->layout.object_size}});
  uint64_t object_overlap = m_ictx->prune_parent_extents(
      *parent_extents, *area, raw_overlap, migration_write);
  if (object_overlap > 0) {
    m_has_parent = true;
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
    I *ictx, uint64_t objectno, ReadExtents* extents,
    IOContext io_context, int op_flags, int read_flags,
    const ZTracer::Trace &parent_trace, uint64_t* version,
    Context *completion)
  : ObjectRequest<I>(ictx, objectno, io_context, "read", parent_trace,
                     completion),
    m_extents(extents), m_op_flags(op_flags),m_read_flags(read_flags),
    m_version(version) {
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

  std::shared_lock image_locker{image_ctx->image_lock};
  auto read_snap_id = this->m_io_context->get_read_snap();
  if (read_snap_id == image_ctx->snap_id &&
      image_ctx->object_map != nullptr &&
      !image_ctx->object_map->object_may_exist(this->m_object_no)) {
    image_ctx->asio_engine->post([this]() { read_parent(); });
    return;
  }
  image_locker.unlock();

  ldout(image_ctx->cct, 20) << "snap_id=" << read_snap_id << dendl;

  neorados::ReadOp read_op;
  for (auto& extent: *this->m_extents) {
    if (extent.length >= image_ctx->sparse_read_threshold_bytes) {
      read_op.sparse_read(extent.offset, extent.length, &extent.bl,
                          &extent.extent_map);
    } else {
      read_op.read(extent.offset, extent.length, &extent.bl);
    }
  }
  util::apply_op_flags(
    m_op_flags, image_ctx->get_read_flags(read_snap_id), &read_op);

  image_ctx->rados_api.execute(
    {data_object_name(this->m_ictx, this->m_object_no)},
    *this->m_io_context, std::move(read_op), nullptr,
    librbd::asio::util::get_callback_adapter(
      [this](int r) { handle_read_object(r); }), m_version,
      (this->m_trace.valid() ? this->m_trace.get_info() : nullptr));
}

template <typename I>
void ObjectReadRequest<I>::handle_read_object(int r) {
  I *image_ctx = this->m_ictx;
  ldout(image_ctx->cct, 20) << "r=" << r << dendl;
  if (m_version != nullptr) {
    ldout(image_ctx->cct, 20) << "version=" << *m_version << dendl;
  }

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
  if ((m_read_flags & READ_FLAG_DISABLE_READ_FROM_PARENT) != 0) {
    this->finish(-ENOENT);
    return;
  }

  I *image_ctx = this->m_ictx;
  ldout(image_ctx->cct, 20) << dendl;

  auto ctx = create_context_callback<
    ObjectReadRequest<I>, &ObjectReadRequest<I>::handle_read_parent>(this);

  io::util::read_parent<I>(
    image_ctx, this->m_object_no, this->m_extents,
    this->m_io_context->get_read_snap(), this->m_trace,
    ctx);
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
  if (!is_copy_on_read(image_ctx, this->m_io_context)) {
    this->finish(0);
    return;
  }

  image_ctx->owner_lock.lock_shared();
  image_ctx->image_lock.lock_shared();
  Extents parent_extents;
  ImageArea area;
  if (!this->compute_parent_extents(&parent_extents, &area, true) ||
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
        image_ctx, this->m_object_no, std::move(parent_extents), area,
        this->m_trace);

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
    IOContext io_context, const char *trace_name,
    const ZTracer::Trace &parent_trace, Context *completion)
  : ObjectRequest<I>(ictx, object_no, io_context, trace_name, parent_trace,
                     completion),
    m_object_off(object_off), m_object_len(len)
{
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

  this->compute_parent_extents(&m_parent_extents, &m_image_area, false);

  if (!this->has_parent() ||
      (m_full_object &&
       !this->m_io_context->get_write_snap_context() &&
       !is_post_copyup_write_required())) {
    m_copyup_enabled = false;
  }
}

template <typename I>
void AbstractObjectWriteRequest<I>::add_write_hint(
    neorados::WriteOp *wr) {
  I *image_ctx = this->m_ictx;
  std::shared_lock image_locker{image_ctx->image_lock};
  if (image_ctx->object_map == nullptr || !this->m_object_may_exist ||
      image_ctx->alloc_hint_flags != 0U) {
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
    if (m_guarding_migration_write) {
      auto snap_seq = (this->m_io_context->get_write_snap_context() ?
          this->m_io_context->get_write_snap_context()->first : 0);
      ldout(image_ctx->cct, 20) << "guarding write: snap_seq=" << snap_seq
                                << dendl;

      cls_client::assert_snapc_seq(
        &write_op, snap_seq, cls::rbd::ASSERT_SNAPC_SEQ_LE_SNAPSET_SEQ);
    } else {
      ldout(image_ctx->cct, 20) << "guarding write" << dendl;
      write_op.assert_exists();
    }
  }

  add_write_hint(&write_op);
  add_write_ops(&write_op);
  ceph_assert(write_op.size() != 0);

  image_ctx->rados_api.execute(
    {data_object_name(this->m_ictx, this->m_object_no)},
    *this->m_io_context, std::move(write_op),
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
        m_image_area, this->m_trace);
    this->m_parent_extents.clear();

    // make sure to wait on this CopyupRequest
    new_req->append_request(this, std::move(get_copyup_overwrite_extents()));
    image_ctx->copyup_list[this->m_object_no] = new_req;

    image_ctx->copyup_list_lock.unlock();
    new_req->send();
  } else {
    it->second->append_request(this, std::move(get_copyup_overwrite_extents()));
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
void ObjectWriteRequest<I>::add_write_hint(neorados::WriteOp* wr) {
  if ((m_write_flags & OBJECT_WRITE_FLAG_CREATE_EXCLUSIVE) != 0) {
    wr->create(true);
  } else if (m_assert_version.has_value()) {
    wr->assert_version(m_assert_version.value());
  }
  AbstractObjectWriteRequest<I>::add_write_hint(wr);
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
  wr->cmpext(this->m_object_off, bufferlist{m_cmp_bl}, &m_mismatch_object_offset);

  if (this->m_full_object) {
    wr->write_full(bufferlist{m_write_bl});
  } else {
    wr->write(this->m_object_off, bufferlist{m_write_bl});
  }
  util::apply_op_flags(m_op_flags, 0U, wr);
}

template <typename I>
int ObjectCompareAndWriteRequest<I>::filter_write_result(int r) const {
  // Error code value for cmpext mismatch. Works for both neorados and
  // mock image, which seems to be short-circuiting on nonexistence.
  if (r == -MAX_ERRNO) {
    I *image_ctx = this->m_ictx;

    // object extent compare mismatch
    auto [image_extents, _] = io::util::object_to_area_extents(
        image_ctx, this->m_object_no, {{m_mismatch_object_offset, this->m_object_len}});
    ceph_assert(image_extents.size() == 1);

    if (m_mismatch_offset) {
      *m_mismatch_offset = image_extents[0].first;
    }
    r = -EILSEQ;
  }
  return r;
}

template <typename I>
ObjectListSnapsRequest<I>::ObjectListSnapsRequest(
    I *ictx, uint64_t objectno, Extents&& object_extents, SnapIds&& snap_ids,
    int list_snaps_flags, const ZTracer::Trace &parent_trace,
    SnapshotDelta* snapshot_delta, Context *completion)
  : ObjectRequest<I>(
      ictx, objectno, ictx->duplicate_data_io_context(), "snap_list",
      parent_trace, completion),
    m_object_extents(std::move(object_extents)),
    m_snap_ids(std::move(snap_ids)), m_list_snaps_flags(list_snaps_flags),
    m_snapshot_delta(snapshot_delta) {
  this->m_io_context->set_read_snap(CEPH_SNAPDIR);
}

template <typename I>
void ObjectListSnapsRequest<I>::send() {
  I *image_ctx = this->m_ictx;
  ldout(image_ctx->cct, 20) << dendl;

  if (m_snap_ids.size() < 2) {
    lderr(image_ctx->cct) << "invalid snap ids: " << m_snap_ids << dendl;
    this->async_finish(-EINVAL);
    return;
  }

  list_snaps();
}

template <typename I>
void ObjectListSnapsRequest<I>::list_snaps() {
  I *image_ctx = this->m_ictx;
  ldout(image_ctx->cct, 20) << dendl;

  neorados::ReadOp read_op;
  read_op.list_snaps(&m_snap_set, &m_ec);

  image_ctx->rados_api.execute(
    {data_object_name(this->m_ictx, this->m_object_no)},
    *this->m_io_context, std::move(read_op), nullptr,
    librbd::asio::util::get_callback_adapter(
      [this](int r) { handle_list_snaps(r); }), nullptr,
      (this->m_trace.valid() ? this->m_trace.get_info() : nullptr));
}

template <typename I>
void ObjectListSnapsRequest<I>::handle_list_snaps(int r) {
  I *image_ctx = this->m_ictx;
  auto cct = image_ctx->cct;

  if (r >= 0) {
    r = -m_ec.value();
  }

  ldout(cct, 20) << "r=" << r << dendl;

  m_snapshot_delta->clear();
  auto& snapshot_delta = *m_snapshot_delta;

  ceph_assert(!m_snap_ids.empty());
  librados::snap_t start_snap_id = 0;
  librados::snap_t first_snap_id = *m_snap_ids.begin();
  librados::snap_t last_snap_id = *m_snap_ids.rbegin();

  if (r == -ENOENT) {
    // the object does not exist -- mark the missing extents
    zero_extent(first_snap_id, true);
    list_from_parent();
    return;
  } else if (r < 0) {
    lderr(cct) << "failed to retrieve object snapshot list: " << cpp_strerror(r)
               << dendl;
    this->finish(r);
    return;
  }

  // helper function requires the librados legacy data structure
  librados::snap_set_t snap_set;
  convert_snap_set(m_snap_set, &snap_set);

  bool initial_extents_written = false;

  interval_set<uint64_t> object_interval;
  for (auto& object_extent : m_object_extents) {
    object_interval.insert(object_extent.first, object_extent.second);
  }
  ldout(cct, 20) << "object_interval=" << object_interval << dendl;

  // loop through all expected snapshots and build interval sets for
  // data and zeroed ranges for each snapshot
  uint64_t prev_end_size = 0;
  interval_set<uint64_t> initial_written_extents;
  for (auto end_snap_id : m_snap_ids) {
    if (start_snap_id == end_snap_id) {
      continue;
    } else if (end_snap_id > last_snap_id) {
      break;
    }

    interval_set<uint64_t> diff;
    uint64_t end_size;
    bool exists;
    librados::snap_t clone_end_snap_id;
    bool read_whole_object;
    calc_snap_set_diff(cct, snap_set, start_snap_id,
                       end_snap_id, &diff, &end_size, &exists,
                       &clone_end_snap_id, &read_whole_object);

    if (read_whole_object) {
      ldout(cct, 1) << "need to read full object" << dendl;
      diff.insert(0, image_ctx->layout.object_size);
      exists = true;
      end_size = image_ctx->layout.object_size;
      clone_end_snap_id = end_snap_id;
    } else if ((m_list_snaps_flags & LIST_SNAPS_FLAG_WHOLE_OBJECT) != 0 &&
               !diff.empty()) {
      ldout(cct, 20) << "expanding diff from " << diff << dendl;
      diff.clear();
      diff.insert(0, image_ctx->layout.object_size);
    }

    if (exists) {
      // reads should be issued against the newest (existing) snapshot within
      // the associated snapshot object clone. writes should be issued
      // against the oldest snapshot in the snap_map.
      ceph_assert(clone_end_snap_id >= end_snap_id);
      if (clone_end_snap_id > last_snap_id) {
        // do not read past the copy point snapshot
        clone_end_snap_id = last_snap_id;
      }
    }

    // clip diff to current object extent
    interval_set<uint64_t> diff_interval;
    diff_interval.intersection_of(object_interval, diff);

    // clip diff to size of object (in case it was truncated)
    interval_set<uint64_t> zero_interval;
    if (end_size < prev_end_size) {
      zero_interval.insert(end_size, prev_end_size - end_size);
      zero_interval.intersection_of(object_interval);

      interval_set<uint64_t> trunc_interval;
      trunc_interval.intersection_of(zero_interval, diff_interval);
      if (!trunc_interval.empty()) {
        diff_interval.subtract(trunc_interval);
        ldout(cct, 20) << "clearing truncate diff: " << trunc_interval << dendl;
      }
    }

    ldout(cct, 20) << "start_snap_id=" << start_snap_id << ", "
                   << "end_snap_id=" << end_snap_id << ", "
                   << "clone_end_snap_id=" << clone_end_snap_id << ", "
                   << "diff=" << diff << ", "
                   << "diff_interval=" << diff_interval<< ", "
                   << "zero_interval=" << zero_interval<< ", "
                   << "end_size=" << end_size << ", "
                   << "prev_end_size=" << prev_end_size << ", "
                   << "exists=" << exists << ", "
                   << "read_whole_object=" << read_whole_object << dendl;

    // check if object exists prior to start of incremental snap delta so that
    // we don't DNE the object if no additional deltas exist
    if (exists && start_snap_id == 0 &&
        (!diff_interval.empty() || !zero_interval.empty())) {
      ldout(cct, 20) << "object exists at snap id " << end_snap_id << dendl;
      initial_extents_written = true;
    }

    prev_end_size = end_size;
    start_snap_id = end_snap_id;

    if (end_snap_id <= first_snap_id) {
      // don't include deltas from the starting snapshots, but we iterate over
      // it to track its existence and size
      // Note: if the image is a clone and a copyup was performed on the
      // object, the older snaps will be updated with the parent data.
      // When called by snapshot based rbd-mirror, the diff in the older snap is ignored
      // as it was processed earlier, causing the snapshot_delta to not include
      // the parent data.
      ldout(cct, 20) << "skipping prior snapshot " << dendl;
      continue;
    }

    if (exists) {
      for (auto& interval : diff_interval) {
        snapshot_delta[{end_snap_id, clone_end_snap_id}].insert(
          interval.first, interval.second,
          SparseExtent(SPARSE_EXTENT_STATE_DATA, interval.second));
      }
    } else {
      zero_interval.union_of(diff_interval);
    }

    if ((m_list_snaps_flags & LIST_SNAPS_FLAG_IGNORE_ZEROED_EXTENTS) == 0) {
      for (auto& interval : zero_interval) {
        snapshot_delta[{end_snap_id, end_snap_id}].insert(
          interval.first, interval.second,
          SparseExtent(SPARSE_EXTENT_STATE_ZEROED, interval.second));
      }
    }
  }

  bool snapshot_delta_empty = snapshot_delta.empty();
  if (!initial_extents_written) {
    zero_extent(first_snap_id, first_snap_id > 0);
  }
  ldout(cct, 20) << "snapshot_delta=" << snapshot_delta << dendl;

  if (snapshot_delta_empty) {
    list_from_parent();
    return;
  }

  this->finish(0);
}

template <typename I>
void ObjectListSnapsRequest<I>::list_from_parent() {
  I *image_ctx = this->m_ictx;
  auto cct = image_ctx->cct;

  ceph_assert(!m_snap_ids.empty());
  librados::snap_t snap_id_start = *m_snap_ids.begin();
  librados::snap_t snap_id_end = *m_snap_ids.rbegin();

  std::unique_lock image_locker{image_ctx->image_lock};
  if ((snap_id_start > 0) || (image_ctx->parent == nullptr) ||
      ((m_list_snaps_flags & LIST_SNAPS_FLAG_DISABLE_LIST_FROM_PARENT) != 0)) {
    image_locker.unlock();

    this->finish(0);
    return;
  }

  Extents parent_extents;
  uint64_t raw_overlap = 0;
  uint64_t object_overlap = 0;
  image_ctx->get_parent_overlap(snap_id_end, &raw_overlap);
  if (raw_overlap > 0) {
    // calculate reverse mapping onto the parent image
    std::tie(parent_extents, m_image_area) = io::util::object_to_area_extents(
        image_ctx, this->m_object_no, m_object_extents);
    object_overlap = image_ctx->prune_parent_extents(
        parent_extents, m_image_area, raw_overlap, false);
  }
  if (object_overlap == 0) {
    image_locker.unlock();

    this->finish(0);
    return;
  }

  auto ctx = create_context_callback<
    ObjectListSnapsRequest<I>,
    &ObjectListSnapsRequest<I>::handle_list_from_parent>(this);
  auto aio_comp = AioCompletion::create_and_start(
    ctx, librbd::util::get_image_ctx(image_ctx->parent), AIO_TYPE_GENERIC);
  ldout(cct, 20) << "completion=" << aio_comp
                 << " parent_extents=" << parent_extents
                 << " area=" << m_image_area << dendl;

   auto list_snaps_flags = (
     m_list_snaps_flags | LIST_SNAPS_FLAG_IGNORE_ZEROED_EXTENTS);

  ImageListSnapsRequest<I> req(
    *image_ctx->parent, aio_comp, std::move(parent_extents), m_image_area,
    {0, image_ctx->parent->snap_id}, list_snaps_flags, &m_parent_snapshot_delta,
    this->m_trace);
  req.send();
}

template <typename I>
void ObjectListSnapsRequest<I>::handle_list_from_parent(int r) {
  I *image_ctx = this->m_ictx;
  auto cct = image_ctx->cct;

  ldout(cct, 20) << "r=" << r << ", "
                 << "parent_snapshot_delta=" << m_parent_snapshot_delta
                 << dendl;

  // ignore special-case of fully empty dataset (we ignore zeroes)
  if (m_parent_snapshot_delta.empty()) {
    this->finish(0);
    return;
  }

  // the write/read snapshot id key is not useful for parent images so
  // map the special-case INITIAL_WRITE_READ_SNAP_IDS key
  *m_snapshot_delta = {};
  auto& intervals = (*m_snapshot_delta)[INITIAL_WRITE_READ_SNAP_IDS];
  for (auto& [key, image_extents] : m_parent_snapshot_delta) {
    for (auto image_extent : image_extents) {
      auto state = image_extent.get_val().state;

      // map image-extents back to this object
      striper::LightweightObjectExtents object_extents;
      io::util::area_to_object_extents(image_ctx, image_extent.get_off(),
                                       image_extent.get_len(), m_image_area, 0,
                                       &object_extents);
      for (auto& object_extent : object_extents) {
        ceph_assert(object_extent.object_no == this->m_object_no);
        intervals.insert(
          object_extent.offset, object_extent.length,
          {state, object_extent.length});
      }
    }
  }

  ldout(cct, 20) << "snapshot_delta=" << *m_snapshot_delta << dendl;
  this->finish(0);
}

template <typename I>
void ObjectListSnapsRequest<I>::zero_extent(uint64_t snap_id, bool dne) {
  I *image_ctx = this->m_ictx;
  auto cct = image_ctx->cct;

  // the object does not exist or is (partially) under whiteout -- mark the
  // missing extents which would be any portion of the object that does not
  // have data in the initial snapshot set
  if ((m_list_snaps_flags & LIST_SNAPS_FLAG_IGNORE_ZEROED_EXTENTS) == 0) {
    interval_set<uint64_t> interval;
    for (auto [object_offset, object_length] : m_object_extents) {
      interval.insert(object_offset, object_length);
    }

    for (auto [offset, length] : interval) {
      ldout(cct, 20) << "snapshot " << snap_id << ": "
                     << (dne ? "DNE" : "zeroed") << " extent "
                     << offset << "~" << length << dendl;
      (*m_snapshot_delta)[{snap_id, snap_id}].insert(
        offset, length,
        SparseExtent(
          (dne ? SPARSE_EXTENT_STATE_DNE : SPARSE_EXTENT_STATE_ZEROED),
          length));
    }
  }
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
template class librbd::io::ObjectListSnapsRequest<librbd::ImageCtx>;
