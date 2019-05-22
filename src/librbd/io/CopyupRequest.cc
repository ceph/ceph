// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/io/CopyupRequest.h"
#include "common/ceph_context.h"
#include "common/dout.h"
#include "common/errno.h"
#include "common/Mutex.h"
#include "common/WorkQueue.h"
#include "librbd/AsyncObjectThrottle.h"
#include "librbd/ExclusiveLock.h"
#include "librbd/ImageCtx.h"
#include "librbd/ObjectMap.h"
#include "librbd/Utils.h"
#include "librbd/deep_copy/ObjectCopyRequest.h"
#include "librbd/io/AioCompletion.h"
#include "librbd/io/ImageRequest.h"
#include "librbd/io/ObjectRequest.h"
#include "librbd/io/ReadResult.h"

#include <boost/bind.hpp>
#include <boost/lambda/bind.hpp>
#include <boost/lambda/construct.hpp>

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::io::CopyupRequest: " << this \
                           << " " << __func__ << ": "

namespace librbd {
namespace io {

namespace {

template <typename I>
class C_UpdateObjectMap : public C_AsyncObjectThrottle<I> {
public:
  C_UpdateObjectMap(AsyncObjectThrottle<I> &throttle, I *image_ctx,
                    uint64_t object_no, uint8_t head_object_map_state,
                    const std::vector<uint64_t> *snap_ids,
                    const ZTracer::Trace &trace, size_t snap_id_idx)
    : C_AsyncObjectThrottle<I>(throttle, *image_ctx), m_object_no(object_no),
      m_head_object_map_state(head_object_map_state), m_snap_ids(*snap_ids),
      m_trace(trace), m_snap_id_idx(snap_id_idx)
  {
  }

  int send() override {
    auto& image_ctx = this->m_image_ctx;
    ceph_assert(image_ctx.owner_lock.is_locked());
    if (image_ctx.exclusive_lock == nullptr) {
      return 1;
    }
    ceph_assert(image_ctx.exclusive_lock->is_lock_owner());

    RWLock::RLocker snap_locker(image_ctx.snap_lock);
    if (image_ctx.object_map == nullptr) {
      return 1;
    }

    uint64_t snap_id = m_snap_ids[m_snap_id_idx];
    if (snap_id == CEPH_NOSNAP) {
      return update_head();
    } else {
      return update_snapshot(snap_id);
    }
  }

  int update_head() {
    auto& image_ctx = this->m_image_ctx;
    RWLock::WLocker object_map_locker(image_ctx.object_map_lock);
    bool sent = image_ctx.object_map->template aio_update<Context>(
      CEPH_NOSNAP, m_object_no, m_head_object_map_state, {}, m_trace, false,
      this);
    return (sent ? 0 : 1);
  }

  int update_snapshot(uint64_t snap_id) {
    auto& image_ctx = this->m_image_ctx;
    uint8_t state = OBJECT_EXISTS;
    if (image_ctx.test_features(RBD_FEATURE_FAST_DIFF, image_ctx.snap_lock) &&
        m_snap_id_idx > 0) {
      // first snapshot should be exists+dirty since it contains
      // the copyup data -- later snapshots inherit the data.
      state = OBJECT_EXISTS_CLEAN;
    }

    RWLock::RLocker object_map_locker(image_ctx.object_map_lock);
    bool sent = image_ctx.object_map->template aio_update<Context>(
      snap_id, m_object_no, state, {}, m_trace, true, this);
    ceph_assert(sent);
    return 0;
  }

private:
  uint64_t m_object_no;
  uint8_t m_head_object_map_state;
  const std::vector<uint64_t> &m_snap_ids;
  const ZTracer::Trace &m_trace;
  size_t m_snap_id_idx;
};

} // anonymous namespace

template <typename I>
CopyupRequest<I>::CopyupRequest(I *ictx, const std::string &oid,
                                uint64_t objectno, Extents &&image_extents,
                                const ZTracer::Trace &parent_trace)
  : m_image_ctx(ictx), m_oid(oid), m_object_no(objectno),
    m_image_extents(image_extents),
    m_trace(util::create_trace(*m_image_ctx, "copy-up", parent_trace)),
    m_lock("CopyupRequest", false, false)
{
  m_async_op.start_op(*util::get_image_ctx(m_image_ctx));
}

template <typename I>
CopyupRequest<I>::~CopyupRequest() {
  ceph_assert(m_pending_requests.empty());
  m_async_op.finish_op();
}

template <typename I>
void CopyupRequest<I>::append_request(AbstractObjectWriteRequest<I> *req) {
  Mutex::Locker locker(m_lock);

  auto cct = m_image_ctx->cct;
  ldout(cct, 20) << "oid=" << m_oid << ", "
                 << "object_request=" << req << ", "
                 << "append=" << m_append_request_permitted << dendl;
  if (m_append_request_permitted) {
    m_pending_requests.push_back(req);
  } else {
    m_restart_requests.push_back(req);
  }
}

template <typename I>
void CopyupRequest<I>::send() {
  read_from_parent();
}

template <typename I>
void CopyupRequest<I>::read_from_parent() {
  auto cct = m_image_ctx->cct;
  RWLock::RLocker snap_locker(m_image_ctx->snap_lock);
  RWLock::RLocker parent_locker(m_image_ctx->parent_lock);

  if (m_image_ctx->parent == nullptr) {
    ldout(cct, 5) << "parent detached" << dendl;

    m_image_ctx->op_work_queue->queue(
      util::create_context_callback<
        CopyupRequest<I>, &CopyupRequest<I>::handle_read_from_parent>(this),
      -ENOENT);
    return;
  } else if (is_deep_copy()) {
    deep_copy();
    return;
  }

  m_deep_copy = false;
  auto comp = AioCompletion::create_and_start<
    CopyupRequest<I>,
    &CopyupRequest<I>::handle_read_from_parent>(
      this, util::get_image_ctx(m_image_ctx->parent), AIO_TYPE_READ);

  ldout(cct, 20) << "oid=" << m_oid << ", "
                 << "completion=" << comp << ", "
                 << "extents=" << m_image_extents
                 << dendl;
  ImageRequest<I>::aio_read(m_image_ctx->parent, comp,
                            std::move(m_image_extents),
                            ReadResult{&m_copyup_data}, 0, m_trace);
}

template <typename I>
void CopyupRequest<I>::handle_read_from_parent(int r) {
  auto cct = m_image_ctx->cct;
  ldout(cct, 20) << "oid=" << m_oid << ", r=" << r << dendl;

  m_lock.Lock();
  m_copyup_required = is_copyup_required();
  disable_append_requests();

  if (r < 0 && r != -ENOENT) {
    m_lock.Unlock();

    lderr(cct) << "error reading from parent: " << cpp_strerror(r) << dendl;
    finish(r);
    return;
  }

  if (!m_copyup_required) {
    m_lock.Unlock();

    ldout(cct, 20) << "no-op, skipping" << dendl;
    finish(0);
    return;
  }
  m_lock.Unlock();

  update_object_maps();
}

template <typename I>
void CopyupRequest<I>::deep_copy() {
  auto cct = m_image_ctx->cct;
  ceph_assert(m_image_ctx->snap_lock.is_locked());
  ceph_assert(m_image_ctx->parent_lock.is_locked());
  ceph_assert(m_image_ctx->parent != nullptr);

  m_lock.Lock();
  m_flatten = is_copyup_required() ? true : m_image_ctx->migration_info.flatten;
  m_lock.Unlock();

  ldout(cct, 20) << "oid=" << m_oid << ", flatten=" << m_flatten << dendl;

  m_deep_copy = true;
  auto ctx = util::create_context_callback<
    CopyupRequest<I>, &CopyupRequest<I>::handle_deep_copy>(this);
  auto req = deep_copy::ObjectCopyRequest<I>::create(
    m_image_ctx->parent, m_image_ctx, m_image_ctx->migration_info.snap_map,
    m_object_no, m_flatten, ctx);

  req->send();
}

template <typename I>
void CopyupRequest<I>::handle_deep_copy(int r) {
  auto cct = m_image_ctx->cct;
  ldout(cct, 20) << "oid=" << m_oid << ", r=" << r << dendl;

  m_image_ctx->snap_lock.get_read();
  m_lock.Lock();
  m_copyup_required = is_copyup_required();
  if (r == -ENOENT && !m_flatten && m_copyup_required) {
    m_lock.Unlock();
    m_image_ctx->snap_lock.put_read();

    ldout(cct, 10) << "restart deep-copy with flatten" << dendl;
    send();
    return;
  }

  disable_append_requests();

  if (r < 0 && r != -ENOENT) {
    m_lock.Unlock();
    m_image_ctx->snap_lock.put_read();

    lderr(cct) << "error encountered during deep-copy: " << cpp_strerror(r)
               << dendl;
    finish(r);
    return;
  }

  if (!m_copyup_required && !is_update_object_map_required(r)) {
    m_lock.Unlock();
    m_image_ctx->snap_lock.put_read();

    if (r == -ENOENT) {
      r = 0;
    }

    ldout(cct, 20) << "skipping" << dendl;
    finish(r);
    return;
  }

  m_lock.Unlock();
  m_image_ctx->snap_lock.put_read();

  update_object_maps();
}

template <typename I>
void CopyupRequest<I>::update_object_maps() {
  RWLock::RLocker owner_locker(m_image_ctx->owner_lock);
  RWLock::RLocker snap_locker(m_image_ctx->snap_lock);
  if (m_image_ctx->object_map == nullptr) {
    snap_locker.unlock();
    owner_locker.unlock();

    copyup();
    return;
  }

  auto cct = m_image_ctx->cct;
  ldout(cct, 20) << "oid=" << m_oid << dendl;

  if (!m_image_ctx->snaps.empty()) {
    if (m_deep_copy) {
      compute_deep_copy_snap_ids();
    } else {
      m_snap_ids.insert(m_snap_ids.end(), m_image_ctx->snaps.rbegin(),
                        m_image_ctx->snaps.rend());
    }
  }

  bool copy_on_read = m_pending_requests.empty();
  uint8_t head_object_map_state = OBJECT_EXISTS;
  if (copy_on_read && !m_snap_ids.empty() &&
      m_image_ctx->test_features(RBD_FEATURE_FAST_DIFF,
                                 m_image_ctx->snap_lock)) {
    // HEAD is non-dirty since data is tied to first snapshot
    head_object_map_state = OBJECT_EXISTS_CLEAN;
  }

  auto r_it = m_pending_requests.rbegin();
  if (r_it != m_pending_requests.rend()) {
    // last write-op determines the final object map state
    head_object_map_state = (*r_it)->get_pre_write_object_map_state();
  }

  RWLock::WLocker object_map_locker(m_image_ctx->object_map_lock);
  if ((*m_image_ctx->object_map)[m_object_no] != head_object_map_state) {
    // (maybe) need to update the HEAD object map state
    m_snap_ids.push_back(CEPH_NOSNAP);
  }
  object_map_locker.unlock();
  snap_locker.unlock();

  ceph_assert(m_image_ctx->exclusive_lock->is_lock_owner());
  typename AsyncObjectThrottle<I>::ContextFactory context_factory(
    boost::lambda::bind(boost::lambda::new_ptr<C_UpdateObjectMap<I>>(),
    boost::lambda::_1, m_image_ctx, m_object_no, head_object_map_state,
    &m_snap_ids, m_trace, boost::lambda::_2));
  auto ctx = util::create_context_callback<
    CopyupRequest<I>, &CopyupRequest<I>::handle_update_object_maps>(this);
  auto throttle = new AsyncObjectThrottle<I>(
    nullptr, *m_image_ctx, context_factory, ctx, nullptr, 0, m_snap_ids.size());
  throttle->start_ops(
    m_image_ctx->config.template get_val<uint64_t>("rbd_concurrent_management_ops"));
}

template <typename I>
void CopyupRequest<I>::handle_update_object_maps(int r) {
  auto cct = m_image_ctx->cct;
  ldout(cct, 20) << "oid=" << m_oid << ", r=" << r << dendl;

  if (r < 0) {
    lderr(m_image_ctx->cct) << "failed to update object map: "
                            << cpp_strerror(r) << dendl;

    finish(r);
    return;
  }

  copyup();
}

template <typename I>
void CopyupRequest<I>::copyup() {
  auto cct = m_image_ctx->cct;
  m_image_ctx->snap_lock.get_read();
  auto snapc = m_image_ctx->snapc;
  m_image_ctx->snap_lock.put_read();

  m_lock.Lock();
  if (!m_copyup_required) {
    m_lock.Unlock();

    ldout(cct, 20) << "skipping copyup" << dendl;
    finish(0);
    return;
  }

  ldout(cct, 20) << "oid=" << m_oid << dendl;

  bool copy_on_read = m_pending_requests.empty();
  bool deep_copyup = !snapc.snaps.empty() && !m_copyup_data.is_zero();
  if (m_copyup_data.is_zero()) {
    m_copyup_data.clear();
  }

  int r;
  librados::ObjectWriteOperation copyup_op;
  if (copy_on_read || deep_copyup) {
    copyup_op.exec("rbd", "copyup", m_copyup_data);
    ObjectRequest<I>::add_write_hint(*m_image_ctx, &copyup_op);
    ++m_pending_copyups;
  }

  librados::ObjectWriteOperation write_op;
  if (!copy_on_read) {
    if (!deep_copyup) {
      write_op.exec("rbd", "copyup", m_copyup_data);
      ObjectRequest<I>::add_write_hint(*m_image_ctx, &write_op);
    }

    // merge all pending write ops into this single RADOS op
    for (auto req : m_pending_requests) {
      ldout(cct, 20) << "add_copyup_ops " << req << dendl;
      req->add_copyup_ops(&write_op);
    }

    if (write_op.size() > 0) {
      ++m_pending_copyups;
    }
  }
  m_lock.Unlock();

  // issue librados ops at the end to simplify test cases
  std::vector<librados::snap_t> snaps;
  if (copyup_op.size() > 0) {
    // send only the copyup request with a blank snapshot context so that
    // all snapshots are detected from the parent for this object.  If
    // this is a CoW request, a second request will be created for the
    // actual modification.
    ldout(cct, 20) << "copyup with empty snapshot context" << dendl;

    auto comp = util::create_rados_callback<
      CopyupRequest<I>, &CopyupRequest<I>::handle_copyup>(this);
    r = m_image_ctx->data_ctx.aio_operate(
      m_oid, comp, &copyup_op, 0, snaps,
      (m_trace.valid() ? m_trace.get_info() : nullptr));
    ceph_assert(r == 0);
    comp->release();
  }

  if (write_op.size() > 0) {
    // compare-and-write doesn't add any write ops (copyup+cmpext+write
    // can't be executed in the same RADOS op because, unless the object
    // was already present in the clone, cmpext wouldn't see it)
    ldout(cct, 20) << (!deep_copyup && write_op.size() > 2 ?
                        "copyup + ops" : !deep_copyup ? "copyup" : "ops")
                   << " with current snapshot context" << dendl;

    snaps.insert(snaps.end(), snapc.snaps.begin(), snapc.snaps.end());
    auto comp = util::create_rados_callback<
      CopyupRequest<I>, &CopyupRequest<I>::handle_copyup>(this);
    r = m_image_ctx->data_ctx.aio_operate(
      m_oid, comp, &write_op, snapc.seq, snaps,
      (m_trace.valid() ? m_trace.get_info() : nullptr));
    ceph_assert(r == 0);
    comp->release();
  }
}

template <typename I>
void CopyupRequest<I>::handle_copyup(int r) {
  auto cct = m_image_ctx->cct;
  unsigned pending_copyups;
  {
    Mutex::Locker locker(m_lock);
    ceph_assert(m_pending_copyups > 0);
    pending_copyups = --m_pending_copyups;
  }

  ldout(cct, 20) << "oid=" << m_oid << ", " << "r=" << r << ", "
                 << "pending=" << pending_copyups << dendl;

  if (r < 0 && r != -ENOENT) {
    lderr(cct) << "failed to copyup object: " << cpp_strerror(r) << dendl;
    complete_requests(false, r);
  }

  if (pending_copyups == 0) {
    finish(0);
  }
}

template <typename I>
void CopyupRequest<I>::finish(int r) {
  auto cct = m_image_ctx->cct;
  ldout(cct, 20) << "oid=" << m_oid << ", r=" << r << dendl;

  complete_requests(true, r);
  delete this;
}

template <typename I>
void CopyupRequest<I>::complete_requests(bool override_restart_retval, int r) {
  auto cct = m_image_ctx->cct;
  remove_from_list();

  while (!m_pending_requests.empty()) {
    auto it = m_pending_requests.begin();
    auto req = *it;
    ldout(cct, 20) << "completing request " << req << dendl;
    req->handle_copyup(r);
    m_pending_requests.erase(it);
  }

  if (override_restart_retval) {
    r = -ERESTART;
  }

  while (!m_restart_requests.empty()) {
    auto it = m_restart_requests.begin();
    auto req = *it;
    ldout(cct, 20) << "restarting request " << req << dendl;
    req->handle_copyup(r);
    m_restart_requests.erase(it);
  }
}

template <typename I>
void CopyupRequest<I>::disable_append_requests() {
  ceph_assert(m_lock.is_locked());
  m_append_request_permitted = false;
}

template <typename I>
void CopyupRequest<I>::remove_from_list() {
  Mutex::Locker copyup_list_locker(m_image_ctx->copyup_list_lock);

  auto it = m_image_ctx->copyup_list.find(m_object_no);
  if (it != m_image_ctx->copyup_list.end()) {
    m_image_ctx->copyup_list.erase(it);
  }
}

template <typename I>
bool CopyupRequest<I>::is_copyup_required() {
  ceph_assert(m_lock.is_locked());

  bool copy_on_read = m_pending_requests.empty();
  if (copy_on_read) {
    // always force a copyup if CoR enabled
    return true;
  }

  if (!m_copyup_data.is_zero()) {
    return true;
  }

  for (auto req : m_pending_requests) {
    if (!req->is_empty_write_op()) {
      return true;
    }
  }
  return false;
}

template <typename I>
bool CopyupRequest<I>::is_deep_copy() const {
  ceph_assert(m_image_ctx->snap_lock.is_locked());
  return !m_image_ctx->migration_info.empty();
}

template <typename I>
bool CopyupRequest<I>::is_update_object_map_required(int r) {
  ceph_assert(m_image_ctx->snap_lock.is_locked());

  if (r < 0) {
    return false;
  }

  if (m_image_ctx->object_map == nullptr) {
    return false;
  }

  if (m_image_ctx->migration_info.empty()) {
    // migration might have completed while IO was in-flight,
    // assume worst-case and perform an object map update
    return true;
  }

  auto it = m_image_ctx->migration_info.snap_map.find(CEPH_NOSNAP);
  ceph_assert(it != m_image_ctx->migration_info.snap_map.end());
  return it->second[0] != CEPH_NOSNAP;
}

template <typename I>
void CopyupRequest<I>::compute_deep_copy_snap_ids() {
  ceph_assert(m_image_ctx->snap_lock.is_locked());

  // don't copy ids for the snaps updated by object deep copy or
  // that don't overlap
  std::set<uint64_t> deep_copied;
  for (auto &it : m_image_ctx->migration_info.snap_map) {
    if (it.first != CEPH_NOSNAP) {
      deep_copied.insert(it.second.front());
    }
  }

  RWLock::RLocker parent_locker(m_image_ctx->parent_lock);
  std::copy_if(m_image_ctx->snaps.rbegin(), m_image_ctx->snaps.rend(),
               std::back_inserter(m_snap_ids),
               [this, cct=m_image_ctx->cct, &deep_copied](uint64_t snap_id) {
      if (deep_copied.count(snap_id)) {
        return false;
      }

      uint64_t parent_overlap = 0;
      int r = m_image_ctx->get_parent_overlap(snap_id, &parent_overlap);
      if (r < 0) {
        ldout(cct, 5) << "failed getting parent overlap for snap_id: "
                      << snap_id << ": " << cpp_strerror(r) << dendl;
      }
      if (parent_overlap == 0) {
        return false;
      }
      std::vector<std::pair<uint64_t, uint64_t>> extents;
      Striper::extent_to_file(cct, &m_image_ctx->layout,
                              m_object_no, 0,
                              m_image_ctx->layout.object_size,
                              extents);
      auto overlap = m_image_ctx->prune_parent_extents(
          extents, parent_overlap);
      return overlap > 0;
    });
}

} // namespace io
} // namespace librbd

template class librbd::io::CopyupRequest<librbd::ImageCtx>;
