// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "cls/rbd/cls_rbd_types.h"
#include "librbd/Operations.h"
#include "common/dout.h"
#include "common/errno.h"
#include "common/perf_counters.h"
#include "common/WorkQueue.h"
#include "osdc/Striper.h"

#include "librbd/ExclusiveLock.h"
#include "librbd/ImageCtx.h"
#include "librbd/ImageState.h"
#include "librbd/ImageWatcher.h"
#include "librbd/ObjectMap.h"
#include "librbd/Types.h"
#include "librbd/Utils.h"
#include "librbd/api/Config.h"
#include "librbd/journal/DisabledPolicy.h"
#include "librbd/journal/StandardPolicy.h"
#include "librbd/operation/DisableFeaturesRequest.h"
#include "librbd/operation/EnableFeaturesRequest.h"
#include "librbd/operation/FlattenRequest.h"
#include "librbd/operation/MetadataRemoveRequest.h"
#include "librbd/operation/MetadataSetRequest.h"
#include "librbd/operation/MigrateRequest.h"
#include "librbd/operation/ObjectMapIterate.h"
#include "librbd/operation/RebuildObjectMapRequest.h"
#include "librbd/operation/RenameRequest.h"
#include "librbd/operation/ResizeRequest.h"
#include "librbd/operation/SnapshotCreateRequest.h"
#include "librbd/operation/SnapshotProtectRequest.h"
#include "librbd/operation/SnapshotRemoveRequest.h"
#include "librbd/operation/SnapshotRenameRequest.h"
#include "librbd/operation/SnapshotRollbackRequest.h"
#include "librbd/operation/SnapshotUnprotectRequest.h"
#include "librbd/operation/SnapshotLimitRequest.h"
#include "librbd/operation/SparsifyRequest.h"
#include <set>
#include <boost/bind.hpp>
#include <boost/scope_exit.hpp>

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::Operations: "

namespace librbd {

namespace {

template <typename I>
struct C_NotifyUpdate : public Context {
  I &image_ctx;
  Context *on_finish;
  bool notified = false;

  C_NotifyUpdate(I &image_ctx, Context *on_finish)
    : image_ctx(image_ctx), on_finish(on_finish) {
  }

  void complete(int r) override {
    CephContext *cct = image_ctx.cct;
    if (notified) {
      if (r == -ETIMEDOUT) {
        // don't fail the op if a peer fails to get the update notification
        lderr(cct) << "update notification timed-out" << dendl;
        r = 0;
      } else if (r == -ENOENT) {
        // don't fail if header is missing (e.g. v1 image rename)
        ldout(cct, 5) << "update notification on missing header" << dendl;
        r = 0;
      } else if (r < 0) {
        lderr(cct) << "update notification failed: " << cpp_strerror(r)
                   << dendl;
      }
      Context::complete(r);
      return;
    }

    if (r < 0) {
      // op failed -- no need to send update notification
      Context::complete(r);
      return;
    }

    notified = true;
    image_ctx.notify_update(this);
  }
  void finish(int r) override {
    on_finish->complete(r);
  }
};

template <typename I>
struct C_InvokeAsyncRequest : public Context {
  /**
   * @verbatim
   *
   *               <start>
   *                  |
   *    . . . . . .   |   . . . . . . . . . . . . . . . . . .
   *    .         .   |   .                                 .
   *    .         v   v   v                                 .
   *    .       REFRESH_IMAGE (skip if not needed)          .
   *    .             |                                     .
   *    .             v                                     .
   *    .       ACQUIRE_LOCK (skip if exclusive lock        .
   *    .             |       disabled or has lock)         .
   *    .             |                                     .
   *    .   /--------/ \--------\   . . . . . . . . . . . . .
   *    .   |                   |   .
   *    .   v                   v   .
   *  LOCAL_REQUEST       REMOTE_REQUEST
   *        |                   |
   *        |                   |
   *        \--------\ /--------/
   *                  |
   *                  v
   *              <finish>
   *
   * @endverbatim
   */

  I &image_ctx;
  std::string name;
  exclusive_lock::OperationRequestType request_type;
  bool permit_snapshot;
  boost::function<void(Context*)> local;
  boost::function<void(Context*)> remote;
  std::set<int> filter_error_codes;
  Context *on_finish;
  bool request_lock = false;

  C_InvokeAsyncRequest(I &image_ctx, const std::string& name,
                       exclusive_lock::OperationRequestType request_type,
                       bool permit_snapshot,
                       const boost::function<void(Context*)>& local,
                       const boost::function<void(Context*)>& remote,
                       const std::set<int> &filter_error_codes,
                       Context *on_finish)
    : image_ctx(image_ctx), name(name), request_type(request_type),
      permit_snapshot(permit_snapshot), local(local), remote(remote),
      filter_error_codes(filter_error_codes), on_finish(on_finish) {
  }

  void send() {
    send_refresh_image();
  }

  void send_refresh_image() {
    if (!image_ctx.state->is_refresh_required()) {
      send_acquire_exclusive_lock();
      return;
    }

    CephContext *cct = image_ctx.cct;
    ldout(cct, 20) << __func__ << dendl;

    Context *ctx = util::create_context_callback<
      C_InvokeAsyncRequest<I>,
      &C_InvokeAsyncRequest<I>::handle_refresh_image>(this);
    image_ctx.state->refresh(ctx);
  }

  void handle_refresh_image(int r) {
    CephContext *cct = image_ctx.cct;
    ldout(cct, 20) << __func__ << ": r=" << r << dendl;

    if (r < 0) {
      lderr(cct) << "failed to refresh image: " << cpp_strerror(r) << dendl;
      complete(r);
      return;
    }

    send_acquire_exclusive_lock();
  }

  void send_acquire_exclusive_lock() {
    // context can complete before owner_lock is unlocked
    ceph::shared_mutex &owner_lock(image_ctx.owner_lock);
    owner_lock.lock_shared();
    image_ctx.image_lock.lock_shared();
    if (image_ctx.read_only ||
        (!permit_snapshot && image_ctx.snap_id != CEPH_NOSNAP)) {
      image_ctx.image_lock.unlock_shared();
      owner_lock.unlock_shared();
      complete(-EROFS);
      return;
    }
    image_ctx.image_lock.unlock_shared();

    if (image_ctx.exclusive_lock == nullptr) {
      send_local_request();
      owner_lock.unlock_shared();
      return;
    } else if (image_ctx.image_watcher == nullptr) {
      owner_lock.unlock_shared();
      complete(-EROFS);
      return;
    }

    if (image_ctx.exclusive_lock->is_lock_owner() &&
        image_ctx.exclusive_lock->accept_request(request_type, nullptr)) {
      send_local_request();
      owner_lock.unlock_shared();
      return;
    }

    CephContext *cct = image_ctx.cct;
    ldout(cct, 20) << __func__ << dendl;

    Context *ctx = util::create_async_context_callback(
      image_ctx, util::create_context_callback<
      C_InvokeAsyncRequest<I>,
      &C_InvokeAsyncRequest<I>::handle_acquire_exclusive_lock>(
        this, image_ctx.exclusive_lock));

    if (request_lock) {
      // current lock owner doesn't support op -- try to perform
      // the action locally
      request_lock = false;
      image_ctx.exclusive_lock->acquire_lock(ctx);
    } else {
      image_ctx.exclusive_lock->try_acquire_lock(ctx);
    }
    owner_lock.unlock_shared();
  }

  void handle_acquire_exclusive_lock(int r) {
    CephContext *cct = image_ctx.cct;
    ldout(cct, 20) << __func__ << ": r=" << r << dendl;

    if (r < 0) {
      complete(r == -EBLACKLISTED ? -EBLACKLISTED : -EROFS);
      return;
    }

    // context can complete before owner_lock is unlocked
    ceph::shared_mutex &owner_lock(image_ctx.owner_lock);
    owner_lock.lock_shared();
    if (image_ctx.exclusive_lock == nullptr ||
        image_ctx.exclusive_lock->is_lock_owner()) {
      send_local_request();
      owner_lock.unlock_shared();
      return;
    }

    send_remote_request();
    owner_lock.unlock_shared();
  }

  void send_remote_request() {
    ceph_assert(ceph_mutex_is_locked(image_ctx.owner_lock));

    CephContext *cct = image_ctx.cct;
    ldout(cct, 20) << __func__ << dendl;

    Context *ctx = util::create_async_context_callback(
      image_ctx, util::create_context_callback<
        C_InvokeAsyncRequest<I>,
        &C_InvokeAsyncRequest<I>::handle_remote_request>(this));
    remote(ctx);
  }

  void handle_remote_request(int r) {
    CephContext *cct = image_ctx.cct;
    ldout(cct, 20) << __func__ << ": r=" << r << dendl;

    if (r == -EOPNOTSUPP) {
      ldout(cct, 5) << name << " not supported by current lock owner" << dendl;
      request_lock = true;
      send_refresh_image();
      return;
    } else if (r != -ETIMEDOUT && r != -ERESTART) {
      image_ctx.state->handle_update_notification();

      complete(r);
      return;
    }

    ldout(cct, 5) << name << " timed out notifying lock owner" << dendl;
    send_refresh_image();
  }

  void send_local_request() {
    ceph_assert(ceph_mutex_is_locked(image_ctx.owner_lock));

    CephContext *cct = image_ctx.cct;
    ldout(cct, 20) << __func__ << dendl;

    Context *ctx = util::create_async_context_callback(
      image_ctx, util::create_context_callback<
        C_InvokeAsyncRequest<I>,
        &C_InvokeAsyncRequest<I>::handle_local_request>(this));
    local(ctx);
  }

  void handle_local_request(int r) {
    CephContext *cct = image_ctx.cct;
    ldout(cct, 20) << __func__ << ": r=" << r << dendl;

    if (r == -ERESTART) {
      send_refresh_image();
      return;
    }
    complete(r);
  }

  void finish(int r) override {
    if (filter_error_codes.count(r) != 0) {
      r = 0;
    }
    on_finish->complete(r);
  }
};

template <typename I>
bool needs_invalidate(I& image_ctx, uint64_t object_no,
		     uint8_t current_state, uint8_t new_state) {
  if ( (current_state == OBJECT_EXISTS ||
	current_state == OBJECT_EXISTS_CLEAN) &&
       (new_state == OBJECT_NONEXISTENT ||
	new_state == OBJECT_PENDING)) {
    return false;
  }
  return true;
}

} // anonymous namespace

template <typename I>
Operations<I>::Operations(I &image_ctx)
  : m_image_ctx(image_ctx), m_async_request_seq(0) {
}

template <typename I>
int Operations<I>::flatten(ProgressContext &prog_ctx) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << "flatten" << dendl;

  int r = m_image_ctx.state->refresh_if_required();
  if (r < 0) {
    return r;
  }

  if (m_image_ctx.read_only) {
    return -EROFS;
  }

  {
    std::shared_lock image_locker{m_image_ctx.image_lock};
    if (m_image_ctx.parent_md.spec.pool_id == -1) {
      lderr(cct) << "image has no parent" << dendl;
      return -EINVAL;
    }
  }

  uint64_t request_id = ++m_async_request_seq;
  r = invoke_async_request("flatten",
                           exclusive_lock::OPERATION_REQUEST_TYPE_GENERAL,
                           false,
                           boost::bind(&Operations<I>::execute_flatten, this,
                                       boost::ref(prog_ctx), _1),
                           boost::bind(&ImageWatcher<I>::notify_flatten,
                                       m_image_ctx.image_watcher, request_id,
                                       boost::ref(prog_ctx), _1));

  if (r < 0 && r != -EINVAL) {
    return r;
  }
  ldout(cct, 20) << "flatten finished" << dendl;
  return 0;
}

template <typename I>
void Operations<I>::execute_flatten(ProgressContext &prog_ctx,
                                    Context *on_finish) {
  ceph_assert(ceph_mutex_is_locked(m_image_ctx.owner_lock));
  ceph_assert(m_image_ctx.exclusive_lock == nullptr ||
              m_image_ctx.exclusive_lock->is_lock_owner());

  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << "flatten" << dendl;

  if (m_image_ctx.read_only || m_image_ctx.operations_disabled) {
    on_finish->complete(-EROFS);
    return;
  }

  m_image_ctx.image_lock.lock_shared();

  // can't flatten a non-clone
  if (m_image_ctx.parent_md.spec.pool_id == -1) {
    lderr(cct) << "image has no parent" << dendl;
    m_image_ctx.image_lock.unlock_shared();
    on_finish->complete(-EINVAL);
    return;
  }
  if (m_image_ctx.snap_id != CEPH_NOSNAP) {
    lderr(cct) << "snapshots cannot be flattened" << dendl;
    m_image_ctx.image_lock.unlock_shared();
    on_finish->complete(-EROFS);
    return;
  }

  ::SnapContext snapc = m_image_ctx.snapc;

  uint64_t overlap;
  int r = m_image_ctx.get_parent_overlap(CEPH_NOSNAP, &overlap);
  ceph_assert(r == 0);
  ceph_assert(overlap <= m_image_ctx.size);

  uint64_t overlap_objects = Striper::get_num_objects(m_image_ctx.layout,
                                                      overlap);

  m_image_ctx.image_lock.unlock_shared();

  operation::FlattenRequest<I> *req = new operation::FlattenRequest<I>(
    m_image_ctx, new C_NotifyUpdate<I>(m_image_ctx, on_finish), overlap_objects,
    snapc, prog_ctx);
  req->send();
}

template <typename I>
int Operations<I>::rebuild_object_map(ProgressContext &prog_ctx) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << "rebuild_object_map" << dendl;

  int r = m_image_ctx.state->refresh_if_required();
  if (r < 0) {
    return r;
  }

  uint64_t request_id = ++m_async_request_seq;
  r = invoke_async_request("rebuild object map",
                           exclusive_lock::OPERATION_REQUEST_TYPE_GENERAL, true,
                           boost::bind(&Operations<I>::execute_rebuild_object_map,
                                       this, boost::ref(prog_ctx), _1),
                           boost::bind(&ImageWatcher<I>::notify_rebuild_object_map,
                                       m_image_ctx.image_watcher, request_id,
                                       boost::ref(prog_ctx), _1));

  ldout(cct, 10) << "rebuild object map finished" << dendl;
  if (r < 0) {
    return r;
  }
  return 0;
}

template <typename I>
void Operations<I>::execute_rebuild_object_map(ProgressContext &prog_ctx,
                                               Context *on_finish) {
  ceph_assert(ceph_mutex_is_locked(m_image_ctx.owner_lock));
  ceph_assert(m_image_ctx.exclusive_lock == nullptr ||
              m_image_ctx.exclusive_lock->is_lock_owner());

  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 5) << this << " " << __func__ << dendl;

  if (m_image_ctx.read_only || m_image_ctx.operations_disabled) {
    on_finish->complete(-EROFS);
    return;
  }

  if (!m_image_ctx.test_features(RBD_FEATURE_OBJECT_MAP)) {
    lderr(cct) << "image must support object-map feature" << dendl;
    on_finish->complete(-EINVAL);
    return;
  }

  operation::RebuildObjectMapRequest<I> *req =
    new operation::RebuildObjectMapRequest<I>(
      m_image_ctx, new C_NotifyUpdate<I>(m_image_ctx, on_finish), prog_ctx);
  req->send();
}

template <typename I>
int Operations<I>::check_object_map(ProgressContext &prog_ctx) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 5) << this << " " << __func__ << dendl;
  int r = m_image_ctx.state->refresh_if_required();
  if (r < 0) {
    return r;
  }

  r = invoke_async_request("check object map",
                           exclusive_lock::OPERATION_REQUEST_TYPE_GENERAL, true,
                           boost::bind(&Operations<I>::check_object_map, this,
                                       boost::ref(prog_ctx), _1),
			   [this](Context *c) {
                             m_image_ctx.op_work_queue->queue(c, -EOPNOTSUPP);
                           });

  return r;
}

template <typename I>
void Operations<I>::object_map_iterate(ProgressContext &prog_ctx,
				       operation::ObjectIterateWork<I> handle_mismatch,
				       Context *on_finish) {
  ceph_assert(ceph_mutex_is_locked(m_image_ctx.owner_lock));
  ceph_assert(m_image_ctx.exclusive_lock == nullptr ||
              m_image_ctx.exclusive_lock->is_lock_owner());

  if (!m_image_ctx.test_features(RBD_FEATURE_OBJECT_MAP)) {
    on_finish->complete(-EINVAL);
    return;
  }

  operation::ObjectMapIterateRequest<I> *req =
    new operation::ObjectMapIterateRequest<I>(m_image_ctx, on_finish,
					      prog_ctx, handle_mismatch);
  req->send();
}

template <typename I>
void Operations<I>::check_object_map(ProgressContext &prog_ctx,
				     Context *on_finish) {
  object_map_iterate(prog_ctx, needs_invalidate, on_finish);
}

template <typename I>
int Operations<I>::rename(const char *dstname) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 5) << this << " " << __func__ << ": dest_name=" << dstname
                << dendl;

  int r = librbd::detect_format(m_image_ctx.md_ctx, dstname, NULL, NULL);
  if (r < 0 && r != -ENOENT) {
    lderr(cct) << "error checking for existing image called "
               << dstname << ":" << cpp_strerror(r) << dendl;
    return r;
  }
  if (r == 0) {
    lderr(cct) << "rbd image " << dstname << " already exists" << dendl;
    return -EEXIST;
  }

  if (m_image_ctx.test_features(RBD_FEATURE_JOURNALING)) {
    r = invoke_async_request("rename",
                             exclusive_lock::OPERATION_REQUEST_TYPE_GENERAL,
                             true,
                             boost::bind(&Operations<I>::execute_rename, this,
                                         dstname, _1),
                             boost::bind(&ImageWatcher<I>::notify_rename,
                                         m_image_ctx.image_watcher, dstname,
                                         _1));
    if (r < 0 && r != -EEXIST) {
      return r;
    }
  } else {
    C_SaferCond cond_ctx;
    {
      std::shared_lock owner_lock{m_image_ctx.owner_lock};
      execute_rename(dstname, &cond_ctx);
    }

    r = cond_ctx.wait();
    if (r < 0) {
      return r;
    }
  }

  m_image_ctx.set_image_name(dstname);
  return 0;
}

template <typename I>
void Operations<I>::execute_rename(const std::string &dest_name,
                                   Context *on_finish) {
  ceph_assert(ceph_mutex_is_locked(m_image_ctx.owner_lock));
  if (m_image_ctx.test_features(RBD_FEATURE_JOURNALING)) {
    ceph_assert(m_image_ctx.exclusive_lock == nullptr ||
                m_image_ctx.exclusive_lock->is_lock_owner());
  }

  if (m_image_ctx.operations_disabled) {
    on_finish->complete(-EROFS);
    return;
  }

  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 5) << this << " " << __func__ << ": dest_name=" << dest_name
                << dendl;

  if (m_image_ctx.old_format) {
    m_image_ctx.image_lock.lock_shared();
    if (m_image_ctx.name == dest_name) {
      m_image_ctx.image_lock.unlock_shared();
      on_finish->complete(-EEXIST);
      return;
    }
    m_image_ctx.image_lock.unlock_shared();

    // unregister watch before and register back after rename
    on_finish = new C_NotifyUpdate<I>(m_image_ctx, on_finish);
    on_finish = new LambdaContext([this, on_finish](int r) {
        if (m_image_ctx.old_format) {
          m_image_ctx.image_watcher->set_oid(m_image_ctx.header_oid);
        }
	m_image_ctx.image_watcher->register_watch(on_finish);
      });
    on_finish = new LambdaContext([this, dest_name, on_finish](int r) {
        std::shared_lock owner_locker{m_image_ctx.owner_lock};
	operation::RenameRequest<I> *req = new operation::RenameRequest<I>(
	  m_image_ctx, on_finish, dest_name);
	req->send();
      });
    m_image_ctx.image_watcher->unregister_watch(on_finish);
    return;
  }
  operation::RenameRequest<I> *req = new operation::RenameRequest<I>(
    m_image_ctx, on_finish, dest_name);
  req->send();
}

template <typename I>
int Operations<I>::resize(uint64_t size, bool allow_shrink, ProgressContext& prog_ctx) {
  CephContext *cct = m_image_ctx.cct;

  m_image_ctx.image_lock.lock_shared();
  ldout(cct, 5) << this << " " << __func__ << ": "
                << "size=" << m_image_ctx.size << ", "
                << "new_size=" << size << dendl;
  m_image_ctx.image_lock.unlock_shared();

  int r = m_image_ctx.state->refresh_if_required();
  if (r < 0) {
    return r;
  }

  if (m_image_ctx.test_features(RBD_FEATURE_OBJECT_MAP) &&
      !ObjectMap<>::is_compatible(m_image_ctx.layout, size)) {
    lderr(cct) << "New size not compatible with object map" << dendl;
    return -EINVAL;
  }

  uint64_t request_id = ++m_async_request_seq;
  r = invoke_async_request("resize",
                           exclusive_lock::OPERATION_REQUEST_TYPE_GENERAL,
                           false,
                           boost::bind(&Operations<I>::execute_resize, this,
                                       size, allow_shrink, boost::ref(prog_ctx), _1, 0),
                           boost::bind(&ImageWatcher<I>::notify_resize,
                                       m_image_ctx.image_watcher, request_id,
                                       size, allow_shrink, boost::ref(prog_ctx), _1));

  m_image_ctx.perfcounter->inc(l_librbd_resize);
  ldout(cct, 2) << "resize finished" << dendl;
  return r;
}

template <typename I>
void Operations<I>::execute_resize(uint64_t size, bool allow_shrink, ProgressContext &prog_ctx,
                                   Context *on_finish,
                                   uint64_t journal_op_tid) {
  ceph_assert(ceph_mutex_is_locked(m_image_ctx.owner_lock));
  ceph_assert(m_image_ctx.exclusive_lock == nullptr ||
              m_image_ctx.exclusive_lock->is_lock_owner());

  CephContext *cct = m_image_ctx.cct;
  m_image_ctx.image_lock.lock_shared();
  ldout(cct, 5) << this << " " << __func__ << ": "
                << "size=" << m_image_ctx.size << ", "
                << "new_size=" << size << dendl;

  if (m_image_ctx.snap_id != CEPH_NOSNAP || m_image_ctx.read_only ||
      m_image_ctx.operations_disabled) {
    m_image_ctx.image_lock.unlock_shared();
    on_finish->complete(-EROFS);
    return;
  } else if (m_image_ctx.test_features(RBD_FEATURE_OBJECT_MAP,
                                       m_image_ctx.image_lock) &&
             !ObjectMap<>::is_compatible(m_image_ctx.layout, size)) {
    m_image_ctx.image_lock.unlock_shared();
    on_finish->complete(-EINVAL);
    return;
  }
  m_image_ctx.image_lock.unlock_shared();

  operation::ResizeRequest<I> *req = new operation::ResizeRequest<I>(
    m_image_ctx, new C_NotifyUpdate<I>(m_image_ctx, on_finish), size, allow_shrink,
    prog_ctx, journal_op_tid, false);
  req->send();
}

template <typename I>
int Operations<I>::snap_create(const cls::rbd::SnapshotNamespace &snap_namespace,
			       const std::string& snap_name) {
  if (m_image_ctx.read_only) {
    return -EROFS;
  }

  int r = m_image_ctx.state->refresh_if_required();
  if (r < 0) {
    return r;
  }

  C_SaferCond ctx;
  snap_create(snap_namespace, snap_name, &ctx);
  r = ctx.wait();

  if (r < 0) {
    return r;
  }

  m_image_ctx.perfcounter->inc(l_librbd_snap_create);
  return r;
}

template <typename I>
void Operations<I>::snap_create(const cls::rbd::SnapshotNamespace &snap_namespace,
				const std::string& snap_name,
				Context *on_finish) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 5) << this << " " << __func__ << ": snap_name=" << snap_name
                << dendl;

  if (m_image_ctx.read_only) {
    on_finish->complete(-EROFS);
    return;
  }

  m_image_ctx.image_lock.lock_shared();
  if (m_image_ctx.get_snap_id(snap_namespace, snap_name) != CEPH_NOSNAP) {
    m_image_ctx.image_lock.unlock_shared();
    on_finish->complete(-EEXIST);
    return;
  }
  m_image_ctx.image_lock.unlock_shared();

  C_InvokeAsyncRequest<I> *req = new C_InvokeAsyncRequest<I>(
    m_image_ctx, "snap_create", exclusive_lock::OPERATION_REQUEST_TYPE_GENERAL,
    true,
    boost::bind(&Operations<I>::execute_snap_create, this, snap_namespace, snap_name,
		_1, 0, false),
    boost::bind(&ImageWatcher<I>::notify_snap_create, m_image_ctx.image_watcher,
                snap_namespace, snap_name, _1),
    {-EEXIST}, on_finish);
  req->send();
}

template <typename I>
void Operations<I>::execute_snap_create(const cls::rbd::SnapshotNamespace &snap_namespace,
					const std::string &snap_name,
                                        Context *on_finish,
                                        uint64_t journal_op_tid,
                                        bool skip_object_map) {
  ceph_assert(ceph_mutex_is_locked(m_image_ctx.owner_lock));
  ceph_assert(m_image_ctx.exclusive_lock == nullptr ||
              m_image_ctx.exclusive_lock->is_lock_owner());

  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 5) << this << " " << __func__ << ": snap_name=" << snap_name
                << dendl;

  if (m_image_ctx.operations_disabled) {
    on_finish->complete(-EROFS);
    return;
  }

  m_image_ctx.image_lock.lock_shared();
  if (m_image_ctx.get_snap_id(snap_namespace, snap_name) != CEPH_NOSNAP) {
    m_image_ctx.image_lock.unlock_shared();
    on_finish->complete(-EEXIST);
    return;
  }
  m_image_ctx.image_lock.unlock_shared();

  operation::SnapshotCreateRequest<I> *req =
    new operation::SnapshotCreateRequest<I>(
      m_image_ctx, new C_NotifyUpdate<I>(m_image_ctx, on_finish),
      snap_namespace, snap_name, journal_op_tid, skip_object_map);
  req->send();
}

template <typename I>
int Operations<I>::snap_rollback(const cls::rbd::SnapshotNamespace& snap_namespace,
				 const std::string& snap_name,
                                 ProgressContext& prog_ctx) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 5) << this << " " << __func__ << ": snap_name=" << snap_name
                << dendl;

  int r = m_image_ctx.state->refresh_if_required();
  if (r < 0)
    return r;

  C_SaferCond cond_ctx;
  {
    std::shared_lock owner_locker{m_image_ctx.owner_lock};
    {
      // need to drop image_lock before invalidating cache
      std::shared_lock image_locker{m_image_ctx.image_lock};
      if (!m_image_ctx.snap_exists) {
        return -ENOENT;
      }

      if (m_image_ctx.snap_id != CEPH_NOSNAP || m_image_ctx.read_only) {
        return -EROFS;
      }

      uint64_t snap_id = m_image_ctx.get_snap_id(snap_namespace, snap_name);
      if (snap_id == CEPH_NOSNAP) {
        lderr(cct) << "No such snapshot found." << dendl;
        return -ENOENT;
      }
    }

    r = prepare_image_update(exclusive_lock::OPERATION_REQUEST_TYPE_GENERAL,
                             false);
    if (r < 0) {
      return r;
    }

    execute_snap_rollback(snap_namespace, snap_name, prog_ctx, &cond_ctx);
  }

  r = cond_ctx.wait();
  if (r < 0) {
    return r;
  }

  m_image_ctx.perfcounter->inc(l_librbd_snap_rollback);
  return r;
}

template <typename I>
void Operations<I>::execute_snap_rollback(const cls::rbd::SnapshotNamespace& snap_namespace,
					  const std::string &snap_name,
                                          ProgressContext& prog_ctx,
                                          Context *on_finish) {
  ceph_assert(ceph_mutex_is_locked(m_image_ctx.owner_lock));
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 5) << this << " " << __func__ << ": snap_name=" << snap_name
                << dendl;

  if (m_image_ctx.operations_disabled) {
    on_finish->complete(-EROFS);
    return;
  }

  m_image_ctx.image_lock.lock_shared();
  uint64_t snap_id = m_image_ctx.get_snap_id(snap_namespace, snap_name);
  if (snap_id == CEPH_NOSNAP) {
    lderr(cct) << "No such snapshot found." << dendl;
    m_image_ctx.image_lock.unlock_shared();
    on_finish->complete(-ENOENT);
    return;
  }

  uint64_t new_size = m_image_ctx.get_image_size(snap_id);
  m_image_ctx.image_lock.unlock_shared();

  // async mode used for journal replay
  operation::SnapshotRollbackRequest<I> *request =
    new operation::SnapshotRollbackRequest<I>(
      m_image_ctx, new C_NotifyUpdate<I>(m_image_ctx, on_finish), snap_namespace, snap_name,
      snap_id, new_size, prog_ctx);
  request->send();
}

template <typename I>
int Operations<I>::snap_remove(const cls::rbd::SnapshotNamespace& snap_namespace,
			       const std::string& snap_name) {
  if (m_image_ctx.read_only) {
    return -EROFS;
  }

  int r = m_image_ctx.state->refresh_if_required();
  if (r < 0) {
    return r;
  }

  C_SaferCond ctx;
  snap_remove(snap_namespace, snap_name, &ctx);
  r = ctx.wait();

  if (r < 0) {
    return r;
  }

  m_image_ctx.perfcounter->inc(l_librbd_snap_remove);
  return 0;
}

template <typename I>
void Operations<I>::snap_remove(const cls::rbd::SnapshotNamespace& snap_namespace,
				const std::string& snap_name,
				Context *on_finish) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 5) << this << " " << __func__ << ": snap_name=" << snap_name
                << dendl;

  if (m_image_ctx.read_only) {
    on_finish->complete(-EROFS);
    return;
  }

  // quickly filter out duplicate ops
  m_image_ctx.image_lock.lock_shared();
  if (m_image_ctx.get_snap_id(snap_namespace, snap_name) == CEPH_NOSNAP) {
    m_image_ctx.image_lock.unlock_shared();
    on_finish->complete(-ENOENT);
    return;
  }

  bool proxy_op = ((m_image_ctx.features & RBD_FEATURE_FAST_DIFF) != 0 ||
                   (m_image_ctx.features & RBD_FEATURE_JOURNALING) != 0);
  m_image_ctx.image_lock.unlock_shared();

  if (proxy_op) {
    auto request_type = exclusive_lock::OPERATION_REQUEST_TYPE_GENERAL;
    if (cls::rbd::get_snap_namespace_type(snap_namespace) ==
        cls::rbd::SNAPSHOT_NAMESPACE_TYPE_TRASH) {
      request_type = exclusive_lock::OPERATION_REQUEST_TYPE_TRASH_SNAP_REMOVE;
    }
    C_InvokeAsyncRequest<I> *req = new C_InvokeAsyncRequest<I>(
      m_image_ctx, "snap_remove", request_type, true,
      boost::bind(&Operations<I>::execute_snap_remove, this, snap_namespace, snap_name, _1),
      boost::bind(&ImageWatcher<I>::notify_snap_remove, m_image_ctx.image_watcher,
                  snap_namespace, snap_name, _1),
      {-ENOENT}, on_finish);
    req->send();
  } else {
    std::shared_lock owner_lock{m_image_ctx.owner_lock};
    execute_snap_remove(snap_namespace, snap_name, on_finish);
  }
}

template <typename I>
void Operations<I>::execute_snap_remove(const cls::rbd::SnapshotNamespace& snap_namespace,
					const std::string &snap_name,
                                        Context *on_finish) {
  ceph_assert(ceph_mutex_is_locked(m_image_ctx.owner_lock));
  {
    if ((m_image_ctx.features & RBD_FEATURE_FAST_DIFF) != 0) {
      ceph_assert(m_image_ctx.exclusive_lock == nullptr ||
                  m_image_ctx.exclusive_lock->is_lock_owner());
    }
  }

  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 5) << this << " " << __func__ << ": snap_name=" << snap_name
                << dendl;

  if (m_image_ctx.operations_disabled) {
    on_finish->complete(-EROFS);
    return;
  }

  m_image_ctx.image_lock.lock_shared();
  uint64_t snap_id = m_image_ctx.get_snap_id(snap_namespace, snap_name);
  if (snap_id == CEPH_NOSNAP) {
    lderr(m_image_ctx.cct) << "No such snapshot found." << dendl;
    m_image_ctx.image_lock.unlock_shared();
    on_finish->complete(-ENOENT);
    return;
  }

  bool is_protected;
  int r = m_image_ctx.is_snap_protected(snap_id, &is_protected);
  if (r < 0) {
    m_image_ctx.image_lock.unlock_shared();
    on_finish->complete(r);
    return;
  } else if (is_protected) {
    lderr(m_image_ctx.cct) << "snapshot is protected" << dendl;
    m_image_ctx.image_lock.unlock_shared();
    on_finish->complete(-EBUSY);
    return;
  }
  m_image_ctx.image_lock.unlock_shared();

  operation::SnapshotRemoveRequest<I> *req =
    new operation::SnapshotRemoveRequest<I>(
      m_image_ctx, new C_NotifyUpdate<I>(m_image_ctx, on_finish),
      snap_namespace, snap_name, snap_id);
  req->send();
}

template <typename I>
int Operations<I>::snap_rename(const char *srcname, const char *dstname) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 5) << this << " " << __func__ << ": "
                << "snap_name=" << srcname << ", "
                << "new_snap_name=" << dstname << dendl;

  snapid_t snap_id;
  if (m_image_ctx.read_only) {
    return -EROFS;
  }

  int r = m_image_ctx.state->refresh_if_required();
  if (r < 0)
    return r;

  {
    std::shared_lock l{m_image_ctx.image_lock};
    snap_id = m_image_ctx.get_snap_id(cls::rbd::UserSnapshotNamespace(), srcname);
    if (snap_id == CEPH_NOSNAP) {
      return -ENOENT;
    }
    if (m_image_ctx.get_snap_id(cls::rbd::UserSnapshotNamespace(), dstname) != CEPH_NOSNAP) {
      return -EEXIST;
    }
  }

  if (m_image_ctx.test_features(RBD_FEATURE_JOURNALING)) {
    r = invoke_async_request("snap_rename",
                             exclusive_lock::OPERATION_REQUEST_TYPE_GENERAL,
                             true,
                             boost::bind(&Operations<I>::execute_snap_rename,
                                         this, snap_id, dstname, _1),
                             boost::bind(&ImageWatcher<I>::notify_snap_rename,
                                         m_image_ctx.image_watcher, snap_id,
                                         dstname, _1));
    if (r < 0 && r != -EEXIST) {
      return r;
    }
  } else {
    C_SaferCond cond_ctx;
    {
      std::shared_lock owner_lock{m_image_ctx.owner_lock};
      execute_snap_rename(snap_id, dstname, &cond_ctx);
    }

    r = cond_ctx.wait();
    if (r < 0) {
      return r;
    }
  }

  m_image_ctx.perfcounter->inc(l_librbd_snap_rename);
  return 0;
}

template <typename I>
void Operations<I>::execute_snap_rename(const uint64_t src_snap_id,
                                        const std::string &dest_snap_name,
                                        Context *on_finish) {
  ceph_assert(ceph_mutex_is_locked(m_image_ctx.owner_lock));
  if ((m_image_ctx.features & RBD_FEATURE_JOURNALING) != 0) {
    ceph_assert(m_image_ctx.exclusive_lock == nullptr ||
                m_image_ctx.exclusive_lock->is_lock_owner());
  }

  if (m_image_ctx.operations_disabled) {
    on_finish->complete(-EROFS);
    return;
  }

  m_image_ctx.image_lock.lock_shared();
  if (m_image_ctx.get_snap_id(cls::rbd::UserSnapshotNamespace(),
			      dest_snap_name) != CEPH_NOSNAP) {
    // Renaming is supported for snapshots from user namespace only.
    m_image_ctx.image_lock.unlock_shared();
    on_finish->complete(-EEXIST);
    return;
  }
  m_image_ctx.image_lock.unlock_shared();

  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 5) << this << " " << __func__ << ": "
                << "snap_id=" << src_snap_id << ", "
                << "new_snap_name=" << dest_snap_name << dendl;

  operation::SnapshotRenameRequest<I> *req =
    new operation::SnapshotRenameRequest<I>(
      m_image_ctx, new C_NotifyUpdate<I>(m_image_ctx, on_finish), src_snap_id,
      dest_snap_name);
  req->send();
}

template <typename I>
int Operations<I>::snap_protect(const cls::rbd::SnapshotNamespace& snap_namespace,
				const std::string& snap_name) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 5) << this << " " << __func__ << ": snap_name=" << snap_name
                << dendl;

  if (m_image_ctx.read_only) {
    return -EROFS;
  }

  if (!m_image_ctx.test_features(RBD_FEATURE_LAYERING)) {
    lderr(cct) << "image must support layering" << dendl;
    return -ENOSYS;
  }

  int r = m_image_ctx.state->refresh_if_required();
  if (r < 0) {
    return r;
  }

  {
    std::shared_lock image_locker{m_image_ctx.image_lock};
    bool is_protected;
    r = m_image_ctx.is_snap_protected(m_image_ctx.get_snap_id(snap_namespace, snap_name),
                                      &is_protected);
    if (r < 0) {
      return r;
    }

    if (is_protected) {
      return -EBUSY;
    }
  }

  if (m_image_ctx.test_features(RBD_FEATURE_JOURNALING)) {
    r = invoke_async_request("snap_protect",
                             exclusive_lock::OPERATION_REQUEST_TYPE_GENERAL,
                             true,
                             boost::bind(&Operations<I>::execute_snap_protect,
                                         this, snap_namespace, snap_name, _1),
                             boost::bind(&ImageWatcher<I>::notify_snap_protect,
                                         m_image_ctx.image_watcher,
					 snap_namespace, snap_name, _1));
    if (r < 0 && r != -EBUSY) {
      return r;
    }
  } else {
    C_SaferCond cond_ctx;
    {
      std::shared_lock owner_lock{m_image_ctx.owner_lock};
      execute_snap_protect(snap_namespace, snap_name, &cond_ctx);
    }

    r = cond_ctx.wait();
    if (r < 0) {
      return r;
    }
  }
  return 0;
}

template <typename I>
void Operations<I>::execute_snap_protect(const cls::rbd::SnapshotNamespace& snap_namespace,
					 const std::string &snap_name,
                                         Context *on_finish) {
  ceph_assert(ceph_mutex_is_locked(m_image_ctx.owner_lock));
  if (m_image_ctx.test_features(RBD_FEATURE_JOURNALING)) {
    ceph_assert(m_image_ctx.exclusive_lock == nullptr ||
                m_image_ctx.exclusive_lock->is_lock_owner());
  }

  if (m_image_ctx.operations_disabled) {
    on_finish->complete(-EROFS);
    return;
  }

  m_image_ctx.image_lock.lock_shared();
  bool is_protected;
  int r = m_image_ctx.is_snap_protected(m_image_ctx.get_snap_id(snap_namespace, snap_name),
                                        &is_protected);
  if (r < 0) {
    m_image_ctx.image_lock.unlock_shared();
    on_finish->complete(r);
    return;
  } else if (is_protected) {
    m_image_ctx.image_lock.unlock_shared();
    on_finish->complete(-EBUSY);
    return;
  }
  m_image_ctx.image_lock.unlock_shared();

  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 5) << this << " " << __func__ << ": snap_name=" << snap_name
                << dendl;

  operation::SnapshotProtectRequest<I> *request =
    new operation::SnapshotProtectRequest<I>(
      m_image_ctx, new C_NotifyUpdate<I>(m_image_ctx, on_finish), snap_namespace, snap_name);
  request->send();
}

template <typename I>
int Operations<I>::snap_unprotect(const cls::rbd::SnapshotNamespace& snap_namespace,
				  const std::string& snap_name) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 5) << this << " " << __func__ << ": snap_name=" << snap_name
                << dendl;

  if (m_image_ctx.read_only) {
    return -EROFS;
  }

  int r = m_image_ctx.state->refresh_if_required();
  if (r < 0) {
    return r;
  }

  {
    std::shared_lock image_locker{m_image_ctx.image_lock};
    bool is_unprotected;
    r = m_image_ctx.is_snap_unprotected(m_image_ctx.get_snap_id(snap_namespace, snap_name),
                                  &is_unprotected);
    if (r < 0) {
      return r;
    }

    if (is_unprotected) {
      return -EINVAL;
    }
  }

  if (m_image_ctx.test_features(RBD_FEATURE_JOURNALING)) {
    r = invoke_async_request("snap_unprotect",
                             exclusive_lock::OPERATION_REQUEST_TYPE_GENERAL,
                             true,
                             boost::bind(&Operations<I>::execute_snap_unprotect,
                                         this, snap_namespace, snap_name, _1),
                             boost::bind(&ImageWatcher<I>::notify_snap_unprotect,
                                         m_image_ctx.image_watcher,
					 snap_namespace, snap_name, _1));
    if (r < 0 && r != -EINVAL) {
      return r;
    }
  } else {
    C_SaferCond cond_ctx;
    {
      std::shared_lock owner_lock{m_image_ctx.owner_lock};
      execute_snap_unprotect(snap_namespace, snap_name, &cond_ctx);
    }

    r = cond_ctx.wait();
    if (r < 0) {
      return r;
    }
  }
  return 0;
}

template <typename I>
void Operations<I>::execute_snap_unprotect(const cls::rbd::SnapshotNamespace& snap_namespace,
					   const std::string &snap_name,
                                           Context *on_finish) {
  ceph_assert(ceph_mutex_is_locked(m_image_ctx.owner_lock));
  if (m_image_ctx.test_features(RBD_FEATURE_JOURNALING)) {
    ceph_assert(m_image_ctx.exclusive_lock == nullptr ||
                m_image_ctx.exclusive_lock->is_lock_owner());
  }

  if (m_image_ctx.operations_disabled) {
    on_finish->complete(-EROFS);
    return;
  }

  m_image_ctx.image_lock.lock_shared();
  bool is_unprotected;
  int r = m_image_ctx.is_snap_unprotected(m_image_ctx.get_snap_id(snap_namespace, snap_name),
                                          &is_unprotected);
  if (r < 0) {
    m_image_ctx.image_lock.unlock_shared();
    on_finish->complete(r);
    return;
  } else if (is_unprotected) {
    m_image_ctx.image_lock.unlock_shared();
    on_finish->complete(-EINVAL);
    return;
  }
  m_image_ctx.image_lock.unlock_shared();

  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 5) << this << " " << __func__ << ": snap_name=" << snap_name
                << dendl;

  operation::SnapshotUnprotectRequest<I> *request =
    new operation::SnapshotUnprotectRequest<I>(
      m_image_ctx, new C_NotifyUpdate<I>(m_image_ctx, on_finish), snap_namespace, snap_name);
  request->send();
}

template <typename I>
int Operations<I>::snap_set_limit(uint64_t limit) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 5) << this << " " << __func__ << ": limit=" << limit << dendl;

  if (m_image_ctx.read_only) {
    return -EROFS;
  }

  int r = m_image_ctx.state->refresh_if_required();
  if (r < 0) {
    return r;
  }

  C_SaferCond limit_ctx;
  {
    std::shared_lock owner_lock{m_image_ctx.owner_lock};
    r = prepare_image_update(exclusive_lock::OPERATION_REQUEST_TYPE_GENERAL,
                             true);
    if (r < 0) {
      return r;
    }

    execute_snap_set_limit(limit, &limit_ctx);
  }

  r = limit_ctx.wait();
  return r;
}

template <typename I>
void Operations<I>::execute_snap_set_limit(const uint64_t limit,
					   Context *on_finish) {
  ceph_assert(ceph_mutex_is_locked(m_image_ctx.owner_lock));

  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 5) << this << " " << __func__ << ": limit=" << limit
                << dendl;

  operation::SnapshotLimitRequest<I> *request =
    new operation::SnapshotLimitRequest<I>(m_image_ctx, on_finish, limit);
  request->send();
}

template <typename I>
int Operations<I>::update_features(uint64_t features, bool enabled) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 5) << this << " " << __func__ << ": features=" << features
                << ", enabled=" << enabled << dendl;

  int r = m_image_ctx.state->refresh_if_required();
  if (r < 0) {
    return r;
  }

  if (m_image_ctx.read_only) {
    return -EROFS;
  } else if (m_image_ctx.old_format) {
    lderr(cct) << "old-format images do not support features" << dendl;
    return -EINVAL;
  }

  uint64_t disable_mask = (RBD_FEATURES_MUTABLE |
                           RBD_FEATURES_DISABLE_ONLY);
  if ((enabled && (features & RBD_FEATURES_MUTABLE) != features) ||
      (!enabled && (features & disable_mask) != features) ||
      ((features & ~RBD_FEATURES_MUTABLE_INTERNAL) != features)) {
    lderr(cct) << "cannot update immutable features" << dendl;
    return -EINVAL;
  }

  bool set_object_map = (features & RBD_FEATURE_OBJECT_MAP) == RBD_FEATURE_OBJECT_MAP;
  bool set_fast_diff = (features & RBD_FEATURE_FAST_DIFF) == RBD_FEATURE_FAST_DIFF;
  bool exist_fast_diff = (m_image_ctx.features & RBD_FEATURE_FAST_DIFF) != 0;
  bool exist_object_map = (m_image_ctx.features & RBD_FEATURE_OBJECT_MAP) != 0;

  if ((enabled && ((set_object_map && !exist_fast_diff) || (set_fast_diff && !exist_object_map)))
      || (!enabled && (set_object_map && exist_fast_diff))) {
    features |= (RBD_FEATURE_OBJECT_MAP | RBD_FEATURE_FAST_DIFF);
  }

  if (features == 0) {
    lderr(cct) << "update requires at least one feature" << dendl;
    return -EINVAL;
  }
  {
    std::shared_lock image_locker{m_image_ctx.image_lock};
    if (enabled && (features & m_image_ctx.features) != 0) {
      lderr(cct) << "one or more requested features are already enabled"
		 << dendl;
      return -EINVAL;
    }
    if (!enabled && (features & ~m_image_ctx.features) != 0) {
      lderr(cct) << "one or more requested features are already disabled"
		 << dendl;
      return -EINVAL;
    }
  }

  // if disabling journaling, avoid attempting to open the journal
  // when acquiring the exclusive lock in case the journal is corrupt
  bool disabling_journal = false;
  if (!enabled && ((features & RBD_FEATURE_JOURNALING) != 0)) {
    std::unique_lock image_locker{m_image_ctx.image_lock};
    m_image_ctx.set_journal_policy(new journal::DisabledPolicy());
    disabling_journal = true;
  }
  BOOST_SCOPE_EXIT_ALL( (this)(disabling_journal) ) {
    if (disabling_journal) {
      std::unique_lock image_locker{m_image_ctx.image_lock};
      m_image_ctx.set_journal_policy(
        new journal::StandardPolicy<I>(&m_image_ctx));
    }
  };

  // The journal options are not passed to the lock owner in the
  // update features request. Therefore, if journaling is being
  // enabled, the lock should be locally acquired instead of
  // attempting to send the request to the peer.
  if (enabled && (features & RBD_FEATURE_JOURNALING) != 0) {
    C_SaferCond cond_ctx;
    {
      std::shared_lock owner_lock{m_image_ctx.owner_lock};
      r = prepare_image_update(exclusive_lock::OPERATION_REQUEST_TYPE_GENERAL,
                               true);
      if (r < 0) {
        return r;
      }

      execute_update_features(features, enabled, &cond_ctx, 0);
    }

    r = cond_ctx.wait();
  } else {
    r = invoke_async_request("update_features",
                             exclusive_lock::OPERATION_REQUEST_TYPE_GENERAL,
                             false,
                             boost::bind(&Operations<I>::execute_update_features,
                                         this, features, enabled, _1, 0),
                             boost::bind(&ImageWatcher<I>::notify_update_features,
                                         m_image_ctx.image_watcher, features,
                                         enabled, _1));
  }
  ldout(cct, 2) << "update_features finished" << dendl;
  return r;
}

template <typename I>
void Operations<I>::execute_update_features(uint64_t features, bool enabled,
                                            Context *on_finish,
                                            uint64_t journal_op_tid) {
  ceph_assert(ceph_mutex_is_locked(m_image_ctx.owner_lock));
  ceph_assert(m_image_ctx.exclusive_lock == nullptr ||
              m_image_ctx.exclusive_lock->is_lock_owner());

  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 5) << this << " " << __func__ << ": features=" << features
                << ", enabled=" << enabled << dendl;

  if (m_image_ctx.operations_disabled) {
    on_finish->complete(-EROFS);
    return;
  }

  if (enabled) {
    operation::EnableFeaturesRequest<I> *req =
      new operation::EnableFeaturesRequest<I>(
        m_image_ctx, on_finish, journal_op_tid, features);
    req->send();
  } else {
    operation::DisableFeaturesRequest<I> *req =
      new operation::DisableFeaturesRequest<I>(
        m_image_ctx, on_finish, journal_op_tid, features, false);
    req->send();
  }
}

template <typename I>
int Operations<I>::metadata_set(const std::string &key,
                                const std::string &value) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 5) << this << " " << __func__ << ": key=" << key << ", value="
                << value << dendl;

  std::string config_key;
  bool config_override = util::is_metadata_config_override(key, &config_key);
  if (config_override) {
    // validate config setting
    if (!librbd::api::Config<I>::is_option_name(&m_image_ctx, config_key)) {
      lderr(cct) << "validation for " << key
                 << " failed: not allowed image level override" << dendl;
      return -EINVAL;
    }
    int r = ConfigProxy{false}.set_val(config_key.c_str(), value);
    if (r < 0) {
      return r;
    }
  }

  int r = m_image_ctx.state->refresh_if_required();
  if (r < 0) {
    return r;
  }

  if (m_image_ctx.read_only) {
    return -EROFS;
  }

  C_SaferCond metadata_ctx;
  {
    std::shared_lock owner_lock{m_image_ctx.owner_lock};
    r = prepare_image_update(exclusive_lock::OPERATION_REQUEST_TYPE_GENERAL,
                             true);
    if (r < 0) {
      return r;
    }

    execute_metadata_set(key, value, &metadata_ctx);
  }

  r = metadata_ctx.wait();
  if (config_override && r >= 0) {
    // apply new config key immediately
    r = m_image_ctx.state->refresh_if_required();
  }

  return r;
}

template <typename I>
void Operations<I>::execute_metadata_set(const std::string &key,
					const std::string &value,
					Context *on_finish) {
  ceph_assert(ceph_mutex_is_locked(m_image_ctx.owner_lock));

  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 5) << this << " " << __func__ << ": key=" << key << ", value="
                << value << dendl;

  if (m_image_ctx.operations_disabled) {
    on_finish->complete(-EROFS);
    return;
  }

  operation::MetadataSetRequest<I> *request =
    new operation::MetadataSetRequest<I>(m_image_ctx,
					 new C_NotifyUpdate<I>(m_image_ctx, on_finish),
					 key, value);
  request->send();
}

template <typename I>
int Operations<I>::metadata_remove(const std::string &key) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 5) << this << " " << __func__ << ": key=" << key << dendl;

  int r = m_image_ctx.state->refresh_if_required();
  if (r < 0) {
    return r;
  }

  if (m_image_ctx.read_only) {
    return -EROFS;
  }

  std::string value;
  r = cls_client::metadata_get(&m_image_ctx.md_ctx, m_image_ctx.header_oid, key, &value);
  if(r < 0)
    return r;

  C_SaferCond metadata_ctx;
  {
    std::shared_lock owner_lock{m_image_ctx.owner_lock};
    r = prepare_image_update(exclusive_lock::OPERATION_REQUEST_TYPE_GENERAL,
                             true);
    if (r < 0) {
      return r;
    }

    execute_metadata_remove(key, &metadata_ctx);
  }

  r = metadata_ctx.wait();

  std::string config_key;
  if (util::is_metadata_config_override(key, &config_key) && r >= 0) {
    // apply new config key immediately
    r = m_image_ctx.state->refresh_if_required();
  }

  return r;
}

template <typename I>
void Operations<I>::execute_metadata_remove(const std::string &key,
                                           Context *on_finish) {
  ceph_assert(ceph_mutex_is_locked(m_image_ctx.owner_lock));

  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 5) << this << " " << __func__ << ": key=" << key << dendl;

  if (m_image_ctx.operations_disabled) {
    on_finish->complete(-EROFS);
    return;
  }

  operation::MetadataRemoveRequest<I> *request =
    new operation::MetadataRemoveRequest<I>(
	m_image_ctx,
	new C_NotifyUpdate<I>(m_image_ctx, on_finish), key);
  request->send();
}

template <typename I>
int Operations<I>::migrate(ProgressContext &prog_ctx) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << "migrate" << dendl;

  int r = m_image_ctx.state->refresh_if_required();
  if (r < 0) {
    return r;
  }

  if (m_image_ctx.read_only) {
    return -EROFS;
  }

  {
    std::shared_lock image_locker{m_image_ctx.image_lock};
    if (m_image_ctx.migration_info.empty()) {
      lderr(cct) << "image has no migrating parent" << dendl;
      return -EINVAL;
    }
  }

  uint64_t request_id = ++m_async_request_seq;
  r = invoke_async_request("migrate",
                           exclusive_lock::OPERATION_REQUEST_TYPE_GENERAL,
                           false,
                           boost::bind(&Operations<I>::execute_migrate, this,
                                       boost::ref(prog_ctx), _1),
                           boost::bind(&ImageWatcher<I>::notify_migrate,
                                       m_image_ctx.image_watcher, request_id,
                                       boost::ref(prog_ctx), _1));

  if (r < 0 && r != -EINVAL) {
    return r;
  }
  ldout(cct, 20) << "migrate finished" << dendl;
  return 0;
}

template <typename I>
void Operations<I>::execute_migrate(ProgressContext &prog_ctx,
                                    Context *on_finish) {
  ceph_assert(ceph_mutex_is_locked(m_image_ctx.owner_lock));
  ceph_assert(m_image_ctx.exclusive_lock == nullptr ||
              m_image_ctx.exclusive_lock->is_lock_owner());

  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << "migrate" << dendl;

  if (m_image_ctx.read_only || m_image_ctx.operations_disabled) {
    on_finish->complete(-EROFS);
    return;
  }

  m_image_ctx.image_lock.lock_shared();

  if (m_image_ctx.migration_info.empty()) {
    lderr(cct) << "image has no migrating parent" << dendl;
    m_image_ctx.image_lock.unlock_shared();
    on_finish->complete(-EINVAL);
    return;
  }
  if (m_image_ctx.snap_id != CEPH_NOSNAP) {
    lderr(cct) << "snapshots cannot be migrated" << dendl;
    m_image_ctx.image_lock.unlock_shared();
    on_finish->complete(-EROFS);
    return;
  }

  m_image_ctx.image_lock.unlock_shared();

  operation::MigrateRequest<I> *req = new operation::MigrateRequest<I>(
    m_image_ctx, new C_NotifyUpdate<I>(m_image_ctx, on_finish), prog_ctx);
  req->send();
}

template <typename I>
int Operations<I>::sparsify(size_t sparse_size, ProgressContext &prog_ctx) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << "sparsify" << dendl;

  if (sparse_size < 4096 || sparse_size > m_image_ctx.get_object_size() ||
      (sparse_size & (sparse_size - 1)) != 0) {
    lderr(cct) << "sparse size should be power of two not less than 4096"
               << " and not larger image object size" << dendl;
    return -EINVAL;
  }

  uint64_t request_id = ++m_async_request_seq;
  int r = invoke_async_request("sparsify",
                               exclusive_lock::OPERATION_REQUEST_TYPE_GENERAL,
                               false,
                               boost::bind(&Operations<I>::execute_sparsify,
                                           this, sparse_size,
                                           boost::ref(prog_ctx), _1),
                               boost::bind(&ImageWatcher<I>::notify_sparsify,
                                           m_image_ctx.image_watcher,
                                           request_id, sparse_size,
                                           boost::ref(prog_ctx), _1));
  if (r < 0 && r != -EINVAL) {
    return r;
  }
  ldout(cct, 20) << "resparsify finished" << dendl;
  return 0;
}

template <typename I>
void Operations<I>::execute_sparsify(size_t sparse_size,
                                     ProgressContext &prog_ctx,
                                     Context *on_finish) {
  ceph_assert(ceph_mutex_is_locked(m_image_ctx.owner_lock));
  ceph_assert(m_image_ctx.exclusive_lock == nullptr ||
              m_image_ctx.exclusive_lock->is_lock_owner());

  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << "sparsify" << dendl;

  if (m_image_ctx.operations_disabled) {
    on_finish->complete(-EROFS);
    return;
  }

  auto req = new operation::SparsifyRequest<I>(
    m_image_ctx, sparse_size, new C_NotifyUpdate<I>(m_image_ctx, on_finish),
    prog_ctx);
  req->send();
}

template <typename I>
int Operations<I>::prepare_image_update(
    exclusive_lock::OperationRequestType request_type, bool request_lock) {
  ceph_assert(ceph_mutex_is_rlocked(m_image_ctx.owner_lock));
  if (m_image_ctx.image_watcher == nullptr) {
    return -EROFS;
  }

  // need to upgrade to a write lock
  C_SaferCond ctx;
  m_image_ctx.owner_lock.unlock_shared();
  bool attempting_lock = false;
  {
    std::unique_lock owner_locker{m_image_ctx.owner_lock};
    if (m_image_ctx.exclusive_lock != nullptr &&
        (!m_image_ctx.exclusive_lock->is_lock_owner() ||
         !m_image_ctx.exclusive_lock->accept_request(request_type, nullptr))) {

      attempting_lock = true;
      m_image_ctx.exclusive_lock->block_requests(0);

      if (request_lock) {
        m_image_ctx.exclusive_lock->acquire_lock(&ctx);
      } else {
        m_image_ctx.exclusive_lock->try_acquire_lock(&ctx);
      }
    }
  }

  int r = 0;
  if (attempting_lock) {
    r = ctx.wait();
  }

  m_image_ctx.owner_lock.lock_shared();
  if (attempting_lock && m_image_ctx.exclusive_lock != nullptr) {
    m_image_ctx.exclusive_lock->unblock_requests();
  }

  if (r == -EAGAIN || r == -EBUSY) {
    r = 0;
  }
  if (r < 0) {
    return r;
  } else if (m_image_ctx.exclusive_lock != nullptr &&
             !m_image_ctx.exclusive_lock->is_lock_owner()) {
    return m_image_ctx.exclusive_lock->get_unlocked_op_error();
  }

  return 0;
}

template <typename I>
int Operations<I>::invoke_async_request(
    const std::string& name, exclusive_lock::OperationRequestType request_type,
    bool permit_snapshot, const boost::function<void(Context*)>& local_request,
    const boost::function<void(Context*)>& remote_request) {
  C_SaferCond ctx;
  C_InvokeAsyncRequest<I> *req = new C_InvokeAsyncRequest<I>(m_image_ctx, name,
                                                             request_type,
                                                             permit_snapshot,
                                                             local_request,
                                                             remote_request,
                                                             {}, &ctx);
  req->send();
  return ctx.wait();
}

} // namespace librbd

template class librbd::Operations<librbd::ImageCtx>;
