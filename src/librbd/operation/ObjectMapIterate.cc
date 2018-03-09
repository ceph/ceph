// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/operation/ObjectMapIterate.h"
#include "common/dout.h"
#include "common/errno.h"
#include "osdc/Striper.h"
#include "librbd/AsyncObjectThrottle.h"
#include "librbd/ExclusiveLock.h"
#include "librbd/ImageCtx.h"
#include "librbd/ImageWatcher.h"
#include "librbd/internal.h"
#include "librbd/ObjectMap.h"
#include "librbd/operation/ResizeRequest.h"
#include "librbd/object_map/InvalidateRequest.h"
#include "librbd/Utils.h"
#include <boost/lambda/bind.hpp>
#include <boost/lambda/construct.hpp>

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::ObjectMapIterateRequest: "

namespace librbd {
namespace operation {

namespace {

template <typename I>
class C_VerifyObjectCallback : public C_AsyncObjectThrottle<I> {
public:
  C_VerifyObjectCallback(AsyncObjectThrottle<I> &throttle, I *image_ctx,
			 uint64_t snap_id, uint64_t object_no,
			 ObjectIterateWork<I> handle_mismatch,
			 std::atomic_flag *invalidate)
    : C_AsyncObjectThrottle<I>(throttle, *image_ctx),
    m_snap_id(snap_id), m_object_no(object_no),
    m_oid(image_ctx->get_object_name(m_object_no)),
    m_handle_mismatch(handle_mismatch),
    m_invalidate(invalidate)
  {
    m_io_ctx.dup(image_ctx->data_ctx);
    m_io_ctx.snap_set_read(CEPH_SNAPDIR);
  }

  void complete(int r) override {
    I &image_ctx = this->m_image_ctx;
    if (should_complete(r)) {
      ldout(image_ctx.cct, 20) << m_oid << " C_VerifyObjectCallback completed "
			       << dendl;
      m_io_ctx.close();

      this->finish(r);
      delete this;
    }
  }

  int send() override {
    send_list_snaps();
    return 0;
  }

private:
  librados::IoCtx m_io_ctx;
  uint64_t m_snap_id;
  uint64_t m_object_no;
  std::string m_oid;
  ObjectIterateWork<I> m_handle_mismatch;
  std::atomic_flag *m_invalidate;

  librados::snap_set_t m_snap_set;
  int m_snap_list_ret = 0;

  bool should_complete(int r) {
    I &image_ctx = this->m_image_ctx;
    CephContext *cct = image_ctx.cct;
    if (r == 0) {
      r = m_snap_list_ret;
    }
    if (r < 0 && r != -ENOENT) {
      lderr(cct) << m_oid << " C_VerifyObjectCallback::should_complete: "
                 << "encountered an error: " << cpp_strerror(r) << dendl;
      return true;
    }

    ldout(cct, 20) << m_oid << " C_VerifyObjectCallback::should_complete: "
		   << " r="
                   << r << dendl;
    return object_map_action(get_object_state());
  }

  void send_list_snaps() {
    I &image_ctx = this->m_image_ctx;
    assert(image_ctx.owner_lock.is_locked());
    ldout(image_ctx.cct, 5) << m_oid
			    << " C_VerifyObjectCallback::send_list_snaps"
                            << dendl;

    librados::ObjectReadOperation op;
    op.list_snaps(&m_snap_set, &m_snap_list_ret);

    librados::AioCompletion *comp = util::create_rados_callback(this);
    int r = m_io_ctx.aio_operate(m_oid, comp, &op, NULL);
    assert(r == 0);
    comp->release();
  }

  uint8_t get_object_state() {
    I &image_ctx = this->m_image_ctx;
    RWLock::RLocker snap_locker(image_ctx.snap_lock);
    for (std::vector<librados::clone_info_t>::const_iterator r =
           m_snap_set.clones.begin(); r != m_snap_set.clones.end(); ++r) {
      librados::snap_t from_snap_id;
      librados::snap_t to_snap_id;
      if (r->cloneid == librados::SNAP_HEAD) {
        from_snap_id = next_valid_snap_id(m_snap_set.seq + 1);
        to_snap_id = librados::SNAP_HEAD;
      } else {
        from_snap_id = next_valid_snap_id(r->snaps[0]);
        to_snap_id = r->snaps[r->snaps.size()-1];
      }

      if (to_snap_id < m_snap_id) {
        continue;
      } else if (m_snap_id < from_snap_id) {
        break;
      }

      if ((image_ctx.features & RBD_FEATURE_FAST_DIFF) != 0 &&
          from_snap_id != m_snap_id) {
        return OBJECT_EXISTS_CLEAN;
      }
      return OBJECT_EXISTS;
    }
    return OBJECT_NONEXISTENT;
  }

  uint64_t next_valid_snap_id(uint64_t snap_id) {
    I &image_ctx = this->m_image_ctx;
    assert(image_ctx.snap_lock.is_locked());

    std::map<librados::snap_t, SnapInfo>::iterator it =
      image_ctx.snap_info.lower_bound(snap_id);
    if (it == image_ctx.snap_info.end()) {
      return CEPH_NOSNAP;
    }
    return it->first;
  }

  bool object_map_action(uint8_t new_state) {
    I &image_ctx = this->m_image_ctx;
    CephContext *cct = image_ctx.cct;
    RWLock::RLocker owner_locker(image_ctx.owner_lock);

    // should have been canceled prior to releasing lock
    assert(image_ctx.exclusive_lock == nullptr ||
           image_ctx.exclusive_lock->is_lock_owner());

    RWLock::RLocker snap_locker(image_ctx.snap_lock);
    assert(image_ctx.object_map != nullptr);

    RWLock::WLocker l(image_ctx.object_map_lock);
    uint8_t state = (*image_ctx.object_map)[m_object_no];

    ldout(cct, 10) << "C_VerifyObjectCallback::object_map_action"
		   << " object " << image_ctx.get_object_name(m_object_no)
		   << " state " << (int)state
		   << " new_state " << (int)new_state << dendl;

    if (state != new_state) {
      int r = 0;

      assert(m_handle_mismatch);
      r = m_handle_mismatch(image_ctx, m_object_no, state, new_state);
      if (r) {
	lderr(cct) << "object map error: object "
		   << image_ctx.get_object_name(m_object_no)
		   << " marked as " << (int)state << ", but should be "
		   << (int)new_state << dendl;
	m_invalidate->test_and_set();
      } else {
	ldout(cct, 1) << "object map inconsistent: object "
		   << image_ctx.get_object_name(m_object_no)
		   << " marked as " << (int)state << ", but should be "
		   << (int)new_state << dendl;
      }
    }

    return true;
  }
};

} // anonymous namespace

template <typename I>
void ObjectMapIterateRequest<I>::send() {
  send_verify_objects();
}

template <typename I>
bool ObjectMapIterateRequest<I>::should_complete(int r) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 5) << this << " should_complete: " << " r=" << r << dendl;
  if (r < 0) {
    lderr(cct) << "object map operation encountered an error: "
	       << cpp_strerror(r) << dendl;
  }

  RWLock::RLocker owner_lock(m_image_ctx.owner_lock);
  switch (m_state) {
  case STATE_VERIFY_OBJECTS:
    if (m_invalidate.test_and_set()) {
      send_invalidate_object_map();
      return false;
    } else if (r == 0) {
      return true;
    }
    break;

  case STATE_INVALIDATE_OBJECT_MAP:
    if (r == 0) {
      return true;
    }
    break;

  default:
    ceph_abort();
    break;
  }

  if (r < 0) {
    return true;
  }

  return false;
}

template <typename I>
void ObjectMapIterateRequest<I>::send_verify_objects() {
  assert(m_image_ctx.owner_lock.is_locked());
  CephContext *cct = m_image_ctx.cct;

  uint64_t snap_id;
  uint64_t num_objects;
  {
    RWLock::RLocker l(m_image_ctx.snap_lock);
    snap_id = m_image_ctx.snap_id;
    num_objects = Striper::get_num_objects(m_image_ctx.layout,
                                           m_image_ctx.get_image_size(snap_id));
  }
  ldout(cct, 5) << this << " send_verify_objects" << dendl;

  m_state = STATE_VERIFY_OBJECTS;

  typename AsyncObjectThrottle<I>::ContextFactory context_factory(
    boost::lambda::bind(boost::lambda::new_ptr<C_VerifyObjectCallback<I> >(),
			boost::lambda::_1, &m_image_ctx, snap_id,
			boost::lambda::_2, m_handle_mismatch, &m_invalidate));
  AsyncObjectThrottle<I> *throttle = new AsyncObjectThrottle<I>(
    this, m_image_ctx, context_factory, this->create_callback_context(),
    &m_prog_ctx, 0, num_objects);
  throttle->start_ops(m_image_ctx.concurrent_management_ops);
}

template <typename I>
uint64_t ObjectMapIterateRequest<I>::get_image_size() const {
  assert(m_image_ctx.snap_lock.is_locked());
  if (m_image_ctx.snap_id == CEPH_NOSNAP) {
    if (!m_image_ctx.resize_reqs.empty()) {
      return m_image_ctx.resize_reqs.front()->get_image_size();
    } else {
      return m_image_ctx.size;
    }
  }
  return  m_image_ctx.get_image_size(m_image_ctx.snap_id);
}

template <typename I>
void ObjectMapIterateRequest<I>::send_invalidate_object_map() {
  CephContext *cct = m_image_ctx.cct;

  ldout(cct, 5) << this << " send_invalidate_object_map" << dendl;
  m_state = STATE_INVALIDATE_OBJECT_MAP;

  object_map::InvalidateRequest<I>*req =
    object_map::InvalidateRequest<I>::create(m_image_ctx, m_image_ctx.snap_id,
					     true,
					     this->create_callback_context());

  assert(m_image_ctx.owner_lock.is_locked());
  RWLock::WLocker snap_locker(m_image_ctx.snap_lock);
  req->send();
}

} // namespace operation
} // namespace librbd

template class librbd::operation::ObjectMapIterateRequest<librbd::ImageCtx>;
