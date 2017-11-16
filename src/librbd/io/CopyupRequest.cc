// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/io/CopyupRequest.h"
#include "common/ceph_context.h"
#include "common/dout.h"
#include "common/errno.h"
#include "common/Mutex.h"

#include "librbd/AsyncObjectThrottle.h"
#include "librbd/ExclusiveLock.h"
#include "librbd/ImageCtx.h"
#include "librbd/ObjectMap.h"
#include "librbd/Utils.h"
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

class UpdateObjectMap : public C_AsyncObjectThrottle<> {
public:
  UpdateObjectMap(AsyncObjectThrottle<> &throttle, ImageCtx *image_ctx,
                  uint64_t object_no, const std::vector<uint64_t> *snap_ids,
                  const ZTracer::Trace &trace, size_t snap_id_idx)
    : C_AsyncObjectThrottle(throttle, *image_ctx), m_object_no(object_no),
      m_snap_ids(*snap_ids), m_trace(trace), m_snap_id_idx(snap_id_idx)
  {
  }

  int send() override {
    uint64_t snap_id = m_snap_ids[m_snap_id_idx];
    if (snap_id == CEPH_NOSNAP) {
      RWLock::RLocker snap_locker(m_image_ctx.snap_lock);
      RWLock::WLocker object_map_locker(m_image_ctx.object_map_lock);
      assert(m_image_ctx.exclusive_lock->is_lock_owner());
      assert(m_image_ctx.object_map != nullptr);
      bool sent = m_image_ctx.object_map->aio_update<Context>(
        CEPH_NOSNAP, m_object_no, OBJECT_EXISTS, {}, m_trace, this);
      return (sent ? 0 : 1);
    }

    uint8_t state = OBJECT_EXISTS;
    if (m_image_ctx.test_features(RBD_FEATURE_FAST_DIFF) &&
        m_snap_id_idx + 1 < m_snap_ids.size()) {
      state = OBJECT_EXISTS_CLEAN;
    }

    RWLock::RLocker snap_locker(m_image_ctx.snap_lock);
    RWLock::RLocker object_map_locker(m_image_ctx.object_map_lock);
    if (m_image_ctx.object_map == nullptr) {
      return 1;
    }

    bool sent = m_image_ctx.object_map->aio_update<Context>(
      snap_id, m_object_no, state, {}, m_trace, this);
    assert(sent);
    return 0;
  }

private:
  uint64_t m_object_no;
  const std::vector<uint64_t> &m_snap_ids;
  const ZTracer::Trace &m_trace;
  size_t m_snap_id_idx;
};

} // anonymous namespace

template <typename I>
CopyupRequest<I>::CopyupRequest(I *ictx, const std::string &oid,
                                uint64_t objectno, Extents &&image_extents,
                                const ZTracer::Trace &parent_trace)
  : m_ictx(util::get_image_ctx(ictx)), m_oid(oid), m_object_no(objectno),
    m_image_extents(image_extents),
    m_trace(util::create_trace(*m_ictx, "copy-up", parent_trace)),
    m_state(STATE_READ_FROM_PARENT), m_lock("CopyupRequest", false, false)
{
  m_async_op.start_op(*m_ictx);
}

template <typename I>
CopyupRequest<I>::~CopyupRequest() {
  assert(m_pending_requests.empty());
  m_async_op.finish_op();
}

template <typename I>
void CopyupRequest<I>::append_request(AbstractObjectWriteRequest<I> *req) {
  ldout(m_ictx->cct, 20) << req << dendl;
  m_pending_requests.push_back(req);
}

template <typename I>
void CopyupRequest<I>::complete_requests(int r) {
  while (!m_pending_requests.empty()) {
    auto it = m_pending_requests.begin();
    auto req = *it;
    ldout(m_ictx->cct, 20) << "completing request " << req << dendl;
    req->handle_copyup(r);
    m_pending_requests.erase(it);
  }
}

template <typename I>
bool CopyupRequest<I>::send_copyup() {
  bool copy_on_read = m_pending_requests.empty();
  bool add_copyup_op = !m_copyup_data.is_zero();
  if (!add_copyup_op) {
    m_copyup_data.clear();
  }

  ldout(m_ictx->cct, 20) << "oid " << m_oid << dendl;
  m_state = STATE_COPYUP;

  m_ictx->snap_lock.get_read();
  ::SnapContext snapc = m_ictx->snapc;
  m_ictx->snap_lock.put_read();

  std::vector<librados::snap_t> snaps;

  Mutex::Locker locker(m_lock);
  int r;
  if (copy_on_read || (!snapc.snaps.empty() && add_copyup_op)) {

    librados::ObjectWriteOperation copyup_op;
    copyup_op.exec("rbd", "copyup", m_copyup_data);
    m_copyup_data.clear();

    ObjectRequest<I>::add_write_hint(*m_ictx, &copyup_op);

    // send only the copyup request with a blank snapshot context so that
    // all snapshots are detected from the parent for this object.  If
    // this is a CoW request, a second request will be created for the
    // actual modification.
    m_pending_copyups++;

    ldout(m_ictx->cct, 20) << "copyup with empty snapshot context" << dendl;
    librados::AioCompletion *comp = util::create_rados_callback(this);

    librados::Rados rados(m_ictx->data_ctx);
    r = rados.ioctx_create2(m_ictx->data_ctx.get_id(), m_data_ctx);
    assert(r == 0);

    r = m_data_ctx.aio_operate(
      m_oid, comp, &copyup_op, 0, snaps,
      (m_trace.valid() ? m_trace.get_info() : nullptr));
    assert(r == 0);
    comp->release();
  }

  if (!copy_on_read) {
    librados::ObjectWriteOperation write_op;
    write_op.exec("rbd", "copyup", m_copyup_data);

    // merge all pending write ops into this single RADOS op
    ObjectRequest<I>::add_write_hint(*m_ictx, &write_op);
    for (auto req : m_pending_requests) {
      ldout(m_ictx->cct, 20) << "add_copyup_ops " << req << dendl;
      req->add_copyup_ops(&write_op);
    }

    m_pending_copyups++;

    snaps.insert(snaps.end(), snapc.snaps.begin(), snapc.snaps.end());
    librados::AioCompletion *comp = util::create_rados_callback(this);
    r = m_ictx->data_ctx.aio_operate(
      m_oid, comp, &write_op, snapc.seq, snaps,
      (m_trace.valid() ? m_trace.get_info() : nullptr));
    assert(r == 0);
    comp->release();
  }
  return false;
}

template <typename I>
bool CopyupRequest<I>::is_copyup_required() {
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
void CopyupRequest<I>::send()
{
  m_state = STATE_READ_FROM_PARENT;
  AioCompletion *comp = AioCompletion::create_and_start(
    this, m_ictx, AIO_TYPE_READ);

  ldout(m_ictx->cct, 20) << "completion " << comp
                         << ", oid " << m_oid
                         << ", extents " << m_image_extents
                         << dendl;
  ImageRequest<>::aio_read(m_ictx->parent, comp, std::move(m_image_extents),
                           ReadResult{&m_copyup_data}, 0, m_trace);
}

template <typename I>
void CopyupRequest<I>::complete(int r)
{
  if (should_complete(r)) {
    complete_requests(r);
    delete this;
  }
}

template <typename I>
bool CopyupRequest<I>::should_complete(int r)
{
  CephContext *cct = m_ictx->cct;
  ldout(cct, 20) << "oid " << m_oid
                 << ", r " << r << dendl;

  uint64_t pending_copyups;
  switch (m_state) {
  case STATE_READ_FROM_PARENT:
    ldout(cct, 20) << "READ_FROM_PARENT" << dendl;
    remove_from_list();
    if (r >= 0 || r == -ENOENT) {
      if (!is_copyup_required()) {
        ldout(cct, 20) << "nop, skipping" << dendl;
        return true;
      }

      return send_object_map_head();
    }
    break;

  case STATE_OBJECT_MAP_HEAD:
    ldout(cct, 20) << "OBJECT_MAP_HEAD" << dendl;
    assert(r == 0);
    return send_object_map();

  case STATE_OBJECT_MAP:
    ldout(cct, 20) << "OBJECT_MAP" << dendl;
    assert(r == 0);
    return send_copyup();

  case STATE_COPYUP:
    {
      Mutex::Locker locker(m_lock);
      assert(m_pending_copyups > 0);
      pending_copyups = --m_pending_copyups;
    }
    ldout(cct, 20) << "COPYUP (" << pending_copyups << " pending)"
                   << dendl;
    if (r == -ENOENT) {
      // hide the -ENOENT error if this is the last op
      if (pending_copyups == 0) {
        complete_requests(0);
      }
    } else if (r < 0) {
      complete_requests(r);
    }
    return (pending_copyups == 0);

  default:
    lderr(cct) << "invalid state: " << m_state << dendl;
    ceph_abort();
    break;
  }
  return (r < 0);
}

template <typename I>
void CopyupRequest<I>::remove_from_list()
{
  Mutex::Locker l(m_ictx->copyup_list_lock);

  auto it = m_ictx->copyup_list.find(m_object_no);
  assert(it != m_ictx->copyup_list.end());
  m_ictx->copyup_list.erase(it);
}

template <typename I>
bool CopyupRequest<I>::send_object_map_head() {
  CephContext *cct = m_ictx->cct;
  ldout(cct, 20) << dendl;

  m_state = STATE_OBJECT_MAP_HEAD;

  {
    RWLock::RLocker owner_locker(m_ictx->owner_lock);
    RWLock::RLocker snap_locker(m_ictx->snap_lock);
    if (m_ictx->object_map != nullptr) {
      bool copy_on_read = m_pending_requests.empty();
      assert(m_ictx->exclusive_lock->is_lock_owner());

      RWLock::WLocker object_map_locker(m_ictx->object_map_lock);
      if (!m_ictx->snaps.empty()) {
        m_snap_ids.insert(m_snap_ids.end(), m_ictx->snaps.begin(),
                          m_ictx->snaps.end());
      }
      if (copy_on_read &&
          (*m_ictx->object_map)[m_object_no] != OBJECT_EXISTS) {
        m_snap_ids.insert(m_snap_ids.begin(), CEPH_NOSNAP);
        object_map_locker.unlock();
        snap_locker.unlock();
        owner_locker.unlock();
        return send_object_map();
      }

      bool may_update = false;
      uint8_t new_state;
      uint8_t current_state = (*m_ictx->object_map)[m_object_no];

      auto r_it = m_pending_requests.rbegin();
      if (r_it != m_pending_requests.rend()) {
        auto req = *r_it;
        new_state = req->get_pre_write_object_map_state();

        ldout(cct, 20) << req->get_op_type() << " object no "
                       << m_object_no << " current state "
                       << stringify(static_cast<uint32_t>(current_state))
                       << " new state " << stringify(static_cast<uint32_t>(new_state))
                       << dendl;
        may_update = true;
      }

      if (may_update && (new_state != current_state) &&
          m_ictx->object_map->aio_update<CopyupRequest>(
            CEPH_NOSNAP, m_object_no, new_state, current_state, m_trace,
            this)) {
        return false;
      }
    }
  }

  return send_object_map();
}

template <typename I>
bool CopyupRequest<I>::send_object_map() {
  // avoid possible recursive lock attempts
  if (m_snap_ids.empty()) {
    // no object map update required
    return send_copyup();
  } else {
    // update object maps for HEAD and all existing snapshots
    ldout(m_ictx->cct, 20) << "oid " << m_oid << dendl;
    m_state = STATE_OBJECT_MAP;

    RWLock::RLocker owner_locker(m_ictx->owner_lock);
    AsyncObjectThrottle<>::ContextFactory context_factory(
      boost::lambda::bind(boost::lambda::new_ptr<UpdateObjectMap>(),
      boost::lambda::_1, m_ictx, m_object_no, &m_snap_ids, m_trace,
      boost::lambda::_2));
    AsyncObjectThrottle<> *throttle = new AsyncObjectThrottle<>(
      NULL, *m_ictx, context_factory, util::create_context_callback(this),
      NULL, 0, m_snap_ids.size());
    throttle->start_ops(m_ictx->concurrent_management_ops);
  }
  return false;
}

} // namespace io
} // namespace librbd

template class librbd::io::CopyupRequest<librbd::ImageCtx>;
