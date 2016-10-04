// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "LeaderWatcher.h"
#include "common/debug.h"
#include "common/errno.h"
#include "librbd/Utils.h"
#include "librbd/watcher/Types.h"

#define dout_subsys ceph_subsys_rbd_mirror
#undef dout_prefix
#define dout_prefix *_dout << "rbd::mirror::LeaderWatcher: " \
                           << this << " " << __func__ << ": "

namespace rbd {

namespace mirror {

using namespace leader_watcher;

using librbd::util::create_rados_ack_callback;

namespace {

static const uint64_t NOTIFY_TIMEOUT_MS = 5000;

} // anonymous namespace

LeaderWatcher::LeaderWatcher(librados::IoCtx &io_ctx, ContextWQ *work_queue)
  : Watcher(io_ctx, work_queue, RBD_MIRROR_LEADER),
    m_notifier_id(librados::Rados(io_ctx).get_instance_id()) {
}

int LeaderWatcher::init() {
  dout(20) << dendl;

  int r = m_ioctx.create(m_oid, false);
  if (r < 0) {
    derr << "error creating " << m_oid << " object: " << cpp_strerror(r)
         << dendl;
    return r;
  }

  C_SaferCond register_ctx;
  register_watch(&register_ctx);
  r = register_ctx.wait();
  if (r < 0) {
    derr << "error registering leader watcher for " << m_oid << " object: "
         << cpp_strerror(r) << dendl;
    return r;
  }

  return 0;
}

void LeaderWatcher::shut_down() {
  C_SaferCond unregister_ctx;
  unregister_watch(&unregister_ctx);
  int r = unregister_ctx.wait();
  if (r < 0) {
    derr << "error unregistering leader watcher for " << m_oid
         << " object: " << cpp_strerror(r) << dendl;
  }
}

void LeaderWatcher::notify_heartbeat(Context *on_finish) {
  dout(20) << dendl;

  bufferlist bl;
  ::encode(NotifyMessage{HeartbeatPayload{}}, bl);

  librados::AioCompletion *comp = create_rados_ack_callback(on_finish);
  int r = m_ioctx.aio_notify(m_oid, comp, bl, NOTIFY_TIMEOUT_MS, nullptr);
  assert(r == 0);
  comp->release();
}

void LeaderWatcher::notify_lock_acquired(Context *on_finish) {
  dout(20) << dendl;

  bufferlist bl;
  ::encode(NotifyMessage{LockAcquiredPayload{}}, bl);

  librados::AioCompletion *comp = create_rados_ack_callback(on_finish);
  int r = m_ioctx.aio_notify(m_oid, comp, bl, NOTIFY_TIMEOUT_MS, nullptr);
  assert(r == 0);
  comp->release();
}

void LeaderWatcher::notify_lock_released(Context *on_finish) {
  dout(20) << dendl;

  bufferlist bl;
  ::encode(NotifyMessage{LockReleasedPayload{}}, bl);

  librados::AioCompletion *comp = create_rados_ack_callback(on_finish);
  int r = m_ioctx.aio_notify(m_oid, comp, bl, NOTIFY_TIMEOUT_MS, nullptr);
  assert(r == 0);
  comp->release();
}

void LeaderWatcher::handle_notify(uint64_t notify_id, uint64_t handle,
                                  uint64_t notifier_id, bufferlist &bl) {
  dout(20) << "notify_id=" << notify_id << ", handle=" << handle << ", "
	   << "notifier_id=" << notifier_id << dendl;

  Context *ctx = new librbd::watcher::C_NotifyAck(this, notify_id, handle);

  if (notifier_id == m_notifier_id) {
    dout(20) << "our own notification, ignoring" << dendl;
    ctx->complete(0);
    return;
  }

  NotifyMessage notify_message;
  try {
    bufferlist::iterator iter = bl.begin();
    ::decode(notify_message, iter);
  } catch (const buffer::error &err) {
    derr << ": error decoding image notification: " << err.what() << dendl;
    ctx->complete(0);
    return;
  }

  apply_visitor(HandlePayloadVisitor(this, ctx), notify_message.payload);
}

void LeaderWatcher::handle_payload(const HeartbeatPayload &payload,
                                   Context *on_notify_ack) {
  dout(20) << "heartbeat" << dendl;

  handle_heartbeat(on_notify_ack);
}

void LeaderWatcher::handle_payload(const LockAcquiredPayload &payload,
                                   Context *on_notify_ack) {
  dout(20) << "lock_acquired" << dendl;

  handle_lock_acquired(on_notify_ack);
}

void LeaderWatcher::handle_payload(const LockReleasedPayload &payload,
                                   Context *on_notify_ack) {
  dout(20) << "lock_released" << dendl;

  handle_lock_released(on_notify_ack);
}

void LeaderWatcher::handle_payload(const UnknownPayload &payload,
                                   Context *on_notify_ack) {
  dout(20) << "unknown" << dendl;

  on_notify_ack->complete(0);
}

} // namespace mirror
} // namespace rbd
