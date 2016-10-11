// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "LeaderWatcher.h"
#include "common/errno.h"
#include "librbd/Utils.h"

#define dout_subsys ceph_subsys_rbd_mirror
#undef dout_prefix
#define dout_prefix *_dout << "rbd::mirror::LeaderWatcher: " \
                           << this << " " << __func__

namespace rbd {

namespace mirror {

using namespace leader_watcher;

using librbd::util::create_rados_ack_callback;

namespace {

static const uint64_t NOTIFY_TIMEOUT_MS = 5000;

} // anonymous namespace

int LeaderWatcher::init() {
  dout(20) << dendl;

  int r = m_io_ctx.create(get_oid(), false);
  if (r < 0) {
    derr << "error creating " << get_oid() << " object: " << cpp_strerror(r)
         << dendl;
    return r;
  }

  C_SaferCond register_ctx;
  register_watch(&register_ctx);
  r = register_ctx.wait();
  if (r < 0) {
    derr << "error registering leader watcher for " << get_oid() << " object: "
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
    derr << "error unregistering leader watcher for " << get_oid()
         << " object: " << cpp_strerror(r) << dendl;
  }
}

LeaderWatcher::LeaderWatcher(librados::IoCtx &io_ctx,
                             ContextWQT *work_queue)
  : ObjectWatcher<>(io_ctx, work_queue) {
}

std::string LeaderWatcher::get_oid() const {
  return RBD_MIRROR_LEADER;
}

void LeaderWatcher::notify_heartbeat(Context *on_finish) {
  dout(20) << dendl;

  bufferlist bl;
  ::encode(NotifyMessage{HeartbeatPayload{}}, bl);

  librados::AioCompletion *comp = create_rados_ack_callback(on_finish);
  int r = m_io_ctx.aio_notify(get_oid(), comp, bl, NOTIFY_TIMEOUT_MS, nullptr);
  assert(r == 0);
  comp->release();
}

void LeaderWatcher::notify_lock_acquired(Context *on_finish) {
  dout(20) << dendl;

  bufferlist bl;
  ::encode(NotifyMessage{LockAcquiredPayload{}}, bl);

  librados::AioCompletion *comp = create_rados_ack_callback(on_finish);
  int r = m_io_ctx.aio_notify(get_oid(), comp, bl, NOTIFY_TIMEOUT_MS, nullptr);
  assert(r == 0);
  comp->release();
}

void LeaderWatcher::notify_lock_released(Context *on_finish) {
  dout(20) << dendl;

  bufferlist bl;
  ::encode(NotifyMessage{LockReleasedPayload{}}, bl);

  librados::AioCompletion *comp = create_rados_ack_callback(on_finish);
  int r = m_io_ctx.aio_notify(get_oid(), comp, bl, NOTIFY_TIMEOUT_MS, nullptr);
  assert(r == 0);
  comp->release();
}

void LeaderWatcher::handle_notify(uint64_t notify_id, uint64_t handle,
                                  bufferlist &bl) {
  dout(15) << ": notify_id=" << notify_id << ", " << "handle=" << handle
           << dendl;

  Context *ctx = new typename ObjectWatcher<>::C_NotifyAck(this, notify_id,
                                                           handle);

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
  dout(20) << ": heartbeat" << dendl;

  // XXXMG: Need a way to filter out own notifications
  if (true) {
    dout(20) << ": ignoring our own notification" << dendl;
    on_notify_ack->complete(0);
    return;
  }

  handle_heartbeat(on_notify_ack);
}

void LeaderWatcher::handle_payload(const LockAcquiredPayload &payload,
                                   Context *on_notify_ack) {
  dout(20) << ": lock_acquired" << dendl;

  // XXXMG: Need a way to filter out own notifications
  if (true) {
    dout(20) << ": ignoring our own notification" << dendl;
    on_notify_ack->complete(0);
    return;
  }

  handle_lock_acquired(on_notify_ack);
}

void LeaderWatcher::handle_payload(const LockReleasedPayload &payload,
                                   Context *on_notify_ack) {
  dout(20) << ": lock_released" << dendl;

  // XXXMG: Need a way to filter out own notifications
  if (true) {
    dout(20) << ": ignoring our own notification" << dendl;
    on_notify_ack->complete(0);
    return;
  }

  handle_lock_released(on_notify_ack);
}

void LeaderWatcher::handle_payload(const UnknownPayload &payload,
                                   Context *on_notify_ack) {
  dout(20) << ": unknown" << dendl;

  on_notify_ack->complete(0);
}

} // namespace mirror
} // namespace rbd
