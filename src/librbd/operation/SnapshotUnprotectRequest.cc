// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/operation/SnapshotUnprotectRequest.h"
#include "include/rados/librados.hpp"
#include "include/stringify.h"
#include "common/dout.h"
#include "common/errno.h"
#include "librbd/AsyncObjectThrottle.h"
#include "librbd/ImageCtx.h"
#include "librbd/internal.h"
#include "librbd/parent_types.h"
#include "librbd/Utils.h"
#include <list>
#include <set>
#include <vector>
#include <boost/lambda/bind.hpp>
#include <boost/lambda/construct.hpp>

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::SnapshotUnprotectRequest: "

namespace librbd {
namespace operation {

namespace {

typedef std::pair<int64_t, std::string> Pool;
typedef std::vector<Pool> Pools;

template <typename I>
std::ostream& operator<<(std::ostream& os,
                         const typename SnapshotUnprotectRequest<I>::State& state) {
  switch(state) {
  case SnapshotUnprotectRequest<I>::STATE_UNPROTECT_SNAP_START:
    os << "UNPROTECT_SNAP_START";
    break;
  case SnapshotUnprotectRequest<I>::STATE_SCAN_POOL_CHILDREN:
    os << "SCAN_POOL_CHILDREN";
    break;
  case SnapshotUnprotectRequest<I>::STATE_UNPROTECT_SNAP_FINISH:
    os << "UNPROTECT_SNAP_FINISH";
    break;
  case SnapshotUnprotectRequest<I>::STATE_UNPROTECT_SNAP_ROLLBACK:
    os << "UNPROTECT_SNAP_ROLLBACK";
    break;
  default:
    os << "UNKNOWN (" << static_cast<uint32_t>(state) << ")";
    break;
  }
  return os;
}

template <typename I>
class C_ScanPoolChildren : public C_AsyncObjectThrottle<I> {
public:
  C_ScanPoolChildren(AsyncObjectThrottle<I> &throttle, I *image_ctx,
                     const parent_spec &pspec, const Pools &pools,
                     size_t pool_idx)
    : C_AsyncObjectThrottle<I>(throttle, *image_ctx), m_pspec(pspec),
      m_pool(pools[pool_idx]) {
  }

  virtual int send() {
    I &image_ctx = this->m_image_ctx;
    assert(image_ctx.owner_lock.is_locked());

    CephContext *cct = image_ctx.cct;
    ldout(cct, 10) << this << " scanning pool '" << m_pool.second << "'"
                   << dendl;

    librados::Rados rados(image_ctx.md_ctx);
    int64_t base_tier;
    int r = rados.pool_get_base_tier(m_pool.first, &base_tier);
    if (r == -ENOENT) {
      ldout(cct, 1) << "pool '" << m_pool.second << "' no longer exists"
                    << dendl;
      return 1;
    } else if (r < 0) {
      lderr(cct) << "error retrieving base tier for pool '"
                 << m_pool.second << "'" << dendl;
      return r;
    }
    if (m_pool.first != base_tier) {
      // pool is a cache; skip it
      return 1;
    }

    r = rados.ioctx_create2(m_pool.first, m_pool_ioctx);
    if (r == -ENOENT) {
      ldout(cct, 1) << "pool '" << m_pool.second << "' no longer exists"
                    << dendl;
      return 1;
    } else if (r < 0) {
      lderr(cct) << "can't create ioctx for pool '" << m_pool.second
                 << "'" << dendl;
      return r;
    }

    librados::ObjectReadOperation op;
    cls_client::get_children_start(&op, m_pspec);

    librados::AioCompletion *rados_completion =
      util::create_rados_ack_callback(this);
    r = m_pool_ioctx.aio_operate(RBD_CHILDREN, rados_completion, &op,
                                 &m_children_bl);
    assert(r == 0);
    rados_completion->release();
    return 0;
  }

protected:
  virtual void finish(int r) {
    I &image_ctx = this->m_image_ctx;
    CephContext *cct = image_ctx.cct;

    if (r == 0) {
      bufferlist::iterator it = m_children_bl.begin();
      r= cls_client::get_children_finish(&it, &m_children);
    }

    ldout(cct, 10) << this << " retrieved children: r=" << r << dendl;
    if (r == -ENOENT) {
      // no children -- proceed with unprotect
      r = 0;
    } else if (r < 0) {
      lderr(cct) << "cannot get children for pool '" << m_pool.second << "'"
                 << dendl;
    } else {
      lderr(cct) << "cannot unprotect: at least " << m_children.size() << " "
                 << "child(ren) [" << joinify(m_children.begin(),
                                              m_children.end(),
                                              std::string(",")) << "] "
                 << "in pool '" << m_pool.second << "'" << dendl;
      r = -EBUSY;
    }
    C_AsyncObjectThrottle<I>::finish(r);
  }

private:
  parent_spec m_pspec;
  Pool m_pool;

  IoCtx m_pool_ioctx;
  std::set<std::string> m_children;
  bufferlist m_children_bl;
};

} // anonymous namespace

template <typename I>
SnapshotUnprotectRequest<I>::SnapshotUnprotectRequest(I &image_ctx,
                                                      Context *on_finish,
                                                      const std::string &snap_name)
  : Request<I>(image_ctx, on_finish), m_snap_name(snap_name), m_ret_val(0),
    m_snap_id(CEPH_NOSNAP) {
}

template <typename I>
void SnapshotUnprotectRequest<I>::send_op() {
  send_unprotect_snap_start();
}

template <typename I>
bool SnapshotUnprotectRequest<I>::should_complete(int r) {
  I &image_ctx = this->m_image_ctx;
  CephContext *cct = image_ctx.cct;
  ldout(cct, 5) << this << " " << __func__ << ": state=" << m_state << ", "
                << "r=" << r << dendl;
  if (r < 0) {
    if (r == -EINVAL) {
      ldout(cct, 1) << "snapshot is already unprotected" << dendl;
    } else {
      lderr(cct) << "encountered error: " << cpp_strerror(r) << dendl;
    }
    if (m_ret_val == 0) {
      m_ret_val = r;
    }
  }

  // use a different state machine once an error is encountered
  if (m_ret_val < 0) {
    return should_complete_error();
  }

  RWLock::RLocker owner_lock(image_ctx.owner_lock);
  bool finished = false;
  switch (m_state) {
  case STATE_UNPROTECT_SNAP_START:
    send_scan_pool_children();
    break;
  case STATE_SCAN_POOL_CHILDREN:
    send_unprotect_snap_finish();
    break;
  case STATE_UNPROTECT_SNAP_FINISH:
    finished = true;
    break;
  default:
    assert(false);
    break;
  }
  return finished;
}

template <typename I>
bool SnapshotUnprotectRequest<I>::should_complete_error() {
  I &image_ctx = this->m_image_ctx;
  RWLock::RLocker owner_locker(image_ctx.owner_lock);
  CephContext *cct = image_ctx.cct;
  lderr(cct) << this << " " << __func__ << ": "
             << "ret_val=" << m_ret_val << dendl;

  bool finished = true;
  if (m_state == STATE_SCAN_POOL_CHILDREN ||
      m_state == STATE_UNPROTECT_SNAP_FINISH) {
    send_unprotect_snap_rollback();
    finished = false;
  }
  return finished;
}

template <typename I>
void SnapshotUnprotectRequest<I>::send_unprotect_snap_start() {
  I &image_ctx = this->m_image_ctx;
  assert(image_ctx.owner_lock.is_locked());

  CephContext *cct = image_ctx.cct;
  ldout(cct, 5) << this << " " << __func__ << dendl;

  m_state = STATE_UNPROTECT_SNAP_START;

  int r = verify_and_send_unprotect_snap_start();
  if (r < 0) {
    this->async_complete(r);
    return;
  }
}

template <typename I>
void SnapshotUnprotectRequest<I>::send_scan_pool_children() {
  I &image_ctx = this->m_image_ctx;
  assert(image_ctx.owner_lock.is_locked());

  CephContext *cct = image_ctx.cct;
  ldout(cct, 5) << this << " " << __func__ << dendl;
  m_state = STATE_SCAN_POOL_CHILDREN;

  // search all pools for children depending on this snapshot
  // TODO add async version of wait_for_latest_osdmap
  librados::Rados rados(image_ctx.md_ctx);
  rados.wait_for_latest_osdmap();

  // protect against pools being renamed/deleted
  std::list<Pool> pool_list;
  rados.pool_list2(pool_list);

  parent_spec pspec(image_ctx.md_ctx.get_id(), image_ctx.id, m_snap_id);
  Pools pools(pool_list.begin(), pool_list.end());

  Context *ctx = this->create_callback_context();
  typename AsyncObjectThrottle<I>::ContextFactory context_factory(
    boost::lambda::bind(boost::lambda::new_ptr<C_ScanPoolChildren<I> >(),
      boost::lambda::_1, &image_ctx, pspec, pools, boost::lambda::_2));
  AsyncObjectThrottle<I> *throttle = new AsyncObjectThrottle<I>(
    nullptr, image_ctx, context_factory, ctx, NULL, 0, pools.size());
  throttle->start_ops(image_ctx.concurrent_management_ops);
}

template <typename I>
void SnapshotUnprotectRequest<I>::send_unprotect_snap_finish() {
  I &image_ctx = this->m_image_ctx;
  assert(image_ctx.owner_lock.is_locked());

  CephContext *cct = image_ctx.cct;
  ldout(cct, 5) << this << " " << __func__ << dendl;

  m_state = STATE_UNPROTECT_SNAP_FINISH;

  librados::ObjectWriteOperation op;
  cls_client::set_protection_status(&op, m_snap_id,
                                    RBD_PROTECTION_STATUS_UNPROTECTED);

  librados::AioCompletion *comp = this->create_callback_completion();
  int r = image_ctx.md_ctx.aio_operate(image_ctx.header_oid, comp, &op);
  assert(r == 0);
  comp->release();
}

template <typename I>
void SnapshotUnprotectRequest<I>::send_unprotect_snap_rollback() {
  I &image_ctx = this->m_image_ctx;
  assert(image_ctx.owner_lock.is_locked());

  CephContext *cct = image_ctx.cct;
  ldout(cct, 5) << this << " " << __func__ << dendl;

  m_state = STATE_UNPROTECT_SNAP_ROLLBACK;

  librados::ObjectWriteOperation op;
  cls_client::set_protection_status(&op, m_snap_id,
                                    RBD_PROTECTION_STATUS_PROTECTED);

  librados::AioCompletion *comp = this->create_callback_completion();
  int r = image_ctx.md_ctx.aio_operate(image_ctx.header_oid, comp, &op);
  assert(r == 0);
  comp->release();
}

template <typename I>
int SnapshotUnprotectRequest<I>::verify_and_send_unprotect_snap_start() {
  I &image_ctx = this->m_image_ctx;
  RWLock::RLocker md_locker(image_ctx.md_lock);
  RWLock::RLocker snap_locker(image_ctx.snap_lock);

  CephContext *cct = image_ctx.cct;
  if ((image_ctx.features & RBD_FEATURE_LAYERING) == 0) {
    lderr(cct) << "image must support layering" << dendl;
    return -ENOSYS;
  }

  m_snap_id = image_ctx.get_snap_id(m_snap_name);
  if (m_snap_id == CEPH_NOSNAP) {
    return -ENOENT;
  }

  bool is_unprotected;
  int r = image_ctx.is_snap_unprotected(m_snap_id, &is_unprotected);
  if (r < 0) {
    return r;
  }

  if (is_unprotected) {
    lderr(cct) << "snapshot is already unprotected" << dendl;
    return -EINVAL;
  }

  librados::ObjectWriteOperation op;
  cls_client::set_protection_status(&op, m_snap_id,
                                    RBD_PROTECTION_STATUS_UNPROTECTING);

  librados::AioCompletion *comp = this->create_callback_completion();
  r = image_ctx.md_ctx.aio_operate(image_ctx.header_oid, comp, &op);
  assert(r == 0);
  comp->release();

  // TODO legacy code threw a notification post UNPROTECTING update -- required?
  return 0;
}

} // namespace operation
} // namespace librbd

template class librbd::operation::SnapshotUnprotectRequest<librbd::ImageCtx>;
