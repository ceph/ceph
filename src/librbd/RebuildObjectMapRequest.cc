// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/RebuildObjectMapRequest.h"
#include "common/dout.h"
#include "common/errno.h"
#include "librbd/AsyncObjectThrottle.h"
#include "librbd/AsyncResizeRequest.h"
#include "librbd/AsyncTrimRequest.h"
#include "librbd/ImageCtx.h"
#include "librbd/ImageWatcher.h"
#include "librbd/internal.h"
#include "librbd/ObjectMap.h"
#include <boost/lambda/bind.hpp>
#include <boost/lambda/construct.hpp>

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::RebuildObjectMapRequest: "

namespace librbd {

namespace {

class C_VerifyObject : public C_AsyncObjectThrottle {
public:
  C_VerifyObject(AsyncObjectThrottle &throttle, ImageCtx *image_ctx,
                 uint64_t snap_id, uint64_t object_no)
    : C_AsyncObjectThrottle(throttle), m_image_ctx(*image_ctx),
      m_snap_id(snap_id), m_object_no(object_no),
      m_oid(m_image_ctx.get_object_name(m_object_no))
  {
  }

  virtual void complete(int r) {
    if (should_complete(r)) {
      ldout(m_image_ctx.cct, 5) << " C_VerifyObject completed " << dendl;
      finish(r);
      delete this;
    }
  }

  virtual int send() {
    send_assert_exists();
    return 0;
  }

private:
  /**
   * Verifying the object map for a single object follows the following state
   * machine:
   *
   * <start>
   *    |
   *    v
   * STATE_ASSERT_EXISTS --------> STATE_UPDATE_OBJECT_MAP
   *    .                                     |
   *    .                                     v
   *    . . . . . . . . . . . . . . . . > <finish>
   *
   * The _UPDATE_OBJECT_MAP state is skipped if the object map does not
   * need to be updated.
   */
  enum State {
    STATE_ASSERT_EXISTS,
    STATE_UPDATE_OBJECT_MAP
  };


  ImageCtx &m_image_ctx;
  uint64_t m_snap_id;
  uint64_t m_object_no;
  std::string m_oid;

  State m_state;

  bool should_complete(int r) {
    CephContext *cct = m_image_ctx.cct;
    ldout(cct, 5) << m_oid << " C_VerifyObject::should_complete: " << " r=" << r
                  << dendl;

    bool finished = false;
    switch (m_state) {
    case STATE_ASSERT_EXISTS:
      ldout(cct, 5) << "ASSERT_EXISTS" << dendl;
      if (r == 0 || r == -ENOENT) {
        return send_update_object_map(r == 0);
      }
      break;

    case STATE_UPDATE_OBJECT_MAP:
      ldout(cct, 5) << "UPDATE_OBJECT_MAP" << dendl;
      finished = true;
      break;

    default:
      assert(false);
      break;
    }

    if (r < 0) {
      lderr(cct) << "encountered an error: " << cpp_strerror(r) << dendl;
      return true;
    }
    return finished;
  }

  void send_assert_exists() {
    ldout(m_image_ctx.cct, 5) << m_oid << " C_VerifyObject::assert_exists"
                              << dendl;

    m_state = STATE_ASSERT_EXISTS;
    librados::AioCompletion *comp = librados::Rados::aio_create_completion(
      this, NULL, rados_ctx_cb);

    librados::ObjectReadOperation op;
    op.assert_exists();
    int r = m_image_ctx.data_ctx.aio_operate(m_oid, comp, &op, NULL);
    assert(r == 0);
  }

  bool send_update_object_map(bool exists) {
    CephContext *cct = m_image_ctx.cct;
    bool lost_exclusive_lock = false;

    {
      RWLock::RLocker l(m_image_ctx.owner_lock);
      if (m_image_ctx.image_watcher->is_lock_supported() &&
          !m_image_ctx.image_watcher->is_lock_owner()) {
        ldout(cct, 1) << m_oid << " lost exclusive lock during verify" << dendl;
        lost_exclusive_lock = true;
      } else {
        RWLock::WLocker l(m_image_ctx.object_map_lock);
        uint8_t state = m_image_ctx.object_map[m_object_no];
        uint8_t new_state = state;
        if (exists && state == OBJECT_NONEXISTENT) {
          new_state = OBJECT_EXISTS;
        } else if (!exists && state != OBJECT_NONEXISTENT) {
          new_state = OBJECT_NONEXISTENT;
        }

        if (new_state != state) {
          ldout(cct, 5) << m_oid << " C_VerifyObject::send_update_object_map"
                        << dendl;
          bool updating = m_image_ctx.object_map.aio_update(m_object_no,
                                                            new_state, state,
                                                            this);
          assert(updating);
          return false;
        }
      }
    }

    if (lost_exclusive_lock) {
      complete(-ERESTART);
      return false;
    }
    return true;
  }
};

} // anonymous namespace


void RebuildObjectMapRequest::send() {
  send_resize_object_map();
}

bool RebuildObjectMapRequest::should_complete(int r) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 5) << this << " should_complete: " << " r=" << r << dendl;

  switch (m_state) {
  case STATE_RESIZE_OBJECT_MAP:
    ldout(cct, 5) << "RESIZE_OBJECT_MAP" << dendl;
    if (r == -ESTALE && !m_attempted_trim) {
      // objects are still flagged as in-use -- delete them
      m_attempted_trim = true;
      send_trim_image();
      return false;
    } else if (r == 0) {
      send_verify_objects();
    }
    break;

  case STATE_TRIM_IMAGE:
    ldout(cct, 5) << "TRIM_IMAGE" << dendl;
    if (r == 0) {
      send_resize_object_map();
    }
    break;

  case STATE_VERIFY_OBJECTS:
    ldout(cct, 5) << "VERIFY_OBJECTS" << dendl;
    if (r == 0) {
      return send_update_header();
    }
    break;

  case STATE_UPDATE_HEADER:
    ldout(cct, 5) << "UPDATE_HEADER" << dendl;
    if (r == 0) {
      return true;
    }
    break;

  default:
    assert(false);
    break;
  }

  if (r < 0) {
    lderr(cct) << "rebuild object map encountered an error: " << cpp_strerror(r)
               << dendl;
    return true;
  }
  return false;
}

void RebuildObjectMapRequest::send_resize_object_map() {
  CephContext *cct = m_image_ctx.cct;
  bool lost_exclusive_lock = false;
  bool skip_resize = true;

  m_state = STATE_RESIZE_OBJECT_MAP;
  {
    RWLock::RLocker l(m_image_ctx.owner_lock);
    if (m_image_ctx.image_watcher->is_lock_supported() &&
        !m_image_ctx.image_watcher->is_lock_owner()) {
      ldout(cct, 1) << "lost exclusive lock during resize" << dendl;
      lost_exclusive_lock = true;
    } else {
      RWLock::RLocker l(m_image_ctx.snap_lock);
      uint64_t size = get_image_size();
      uint64_t num_objects = Striper::get_num_objects(m_image_ctx.layout, size);
      if (m_image_ctx.object_map.size() != num_objects) {
        ldout(cct, 5) << this << " send_resize_object_map" << dendl;

        m_image_ctx.object_map.aio_resize(num_objects, OBJECT_NONEXISTENT,
                                          create_callback_context());
        skip_resize = false;
      }
    }
  }

  if (lost_exclusive_lock) {
    complete(-ERESTART);
  } else if (skip_resize) {
    send_verify_objects();
  }
}

void RebuildObjectMapRequest::send_trim_image() {
  CephContext *cct = m_image_ctx.cct;
  bool lost_exclusive_lock = false;
  bool skip_trim = true;

  m_state = STATE_TRIM_IMAGE;
  {
    RWLock::RLocker l(m_image_ctx.owner_lock);
    if (m_image_ctx.image_watcher->is_lock_supported() &&
        !m_image_ctx.image_watcher->is_lock_owner()) {
      ldout(cct, 1) << "lost exclusive lock during trim" << dendl;
      lost_exclusive_lock = true;
    } else {
      ldout(cct, 5) << this << " send_trim_image" << dendl;

      uint64_t new_size;
      uint64_t orig_size;
      {
        RWLock::RLocker l(m_image_ctx.snap_lock);
        new_size = get_image_size();
        orig_size = m_image_ctx.get_object_size() *
                    m_image_ctx.object_map.size();
      }
      AsyncTrimRequest *req = new AsyncTrimRequest(m_image_ctx,
                                                   create_callback_context(),
                                                   orig_size, new_size,
                                                   m_prog_ctx);
      req->send();
      skip_trim = false;
    }
  }

  if (lost_exclusive_lock) {
    complete(-ERESTART);
  } else if (skip_trim) {
    send_resize_object_map();
  }
}

void RebuildObjectMapRequest::send_verify_objects() {
  CephContext *cct = m_image_ctx.cct;

  m_state = STATE_VERIFY_OBJECTS;
  ldout(cct, 5) << this << " send_verify_objects" << dendl;

  uint64_t snap_id;
  uint64_t num_objects;
  {
    RWLock::RLocker l(m_image_ctx.snap_lock);
    snap_id = m_image_ctx.snap_id;
    num_objects = Striper::get_num_objects(m_image_ctx.layout,
                                           m_image_ctx.get_image_size(snap_id));
  }

  AsyncObjectThrottle::ContextFactory context_factory(
    boost::lambda::bind(boost::lambda::new_ptr<C_VerifyObject>(),
      boost::lambda::_1, &m_image_ctx, snap_id, boost::lambda::_2));
  AsyncObjectThrottle *throttle = new AsyncObjectThrottle(
    *this, context_factory, create_callback_context(), m_prog_ctx, 0,
    num_objects);
  throttle->start_ops(cct->_conf->rbd_concurrent_management_ops);
}

bool RebuildObjectMapRequest::send_update_header() {
  CephContext *cct = m_image_ctx.cct;
  bool lost_exclusive_lock = false;

  m_state = STATE_UPDATE_HEADER;
  {
    RWLock::RLocker l(m_image_ctx.owner_lock);
    if (m_image_ctx.image_watcher->is_lock_supported() &&
        !m_image_ctx.image_watcher->is_lock_owner()) {
      ldout(cct, 1) << "lost exclusive lock during header update" << dendl;
      lost_exclusive_lock = true;
    } else {
      ldout(cct, 5) << this << " send_update_header" << dendl;

      librados::ObjectWriteOperation op;
      if (m_image_ctx.image_watcher->is_lock_supported()) {
        m_image_ctx.image_watcher->assert_header_locked(&op);
      }
      cls_client::set_flags(&op, m_image_ctx.snap_id, 0,
                            RBD_FLAG_OBJECT_MAP_INVALID);

      int r = m_image_ctx.md_ctx.aio_operate(m_image_ctx.header_oid,
                                             create_callback_completion(), &op);
      assert(r == 0);

      RWLock::WLocker snap_locker(m_image_ctx.snap_lock);
      m_image_ctx.update_flags(m_image_ctx.snap_id, RBD_FLAG_OBJECT_MAP_INVALID,
                               false);
      return false;
    }
  }

  if (lost_exclusive_lock) {
    complete(-ERESTART);
    return false;
  }
  return true;
}

uint64_t RebuildObjectMapRequest::get_image_size() const {
  assert(m_image_ctx.snap_lock.is_locked());
  if (m_image_ctx.snap_id == CEPH_NOSNAP) {
    if (!m_image_ctx.async_resize_reqs.empty()) {
      return m_image_ctx.async_resize_reqs.front()->get_image_size();
    } else {
      return m_image_ctx.size;
    }
  }
  return  m_image_ctx.get_image_size(m_image_ctx.snap_id);
}

} // namespace librbd
