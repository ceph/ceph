// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/TrashWatcher.h"
#include "include/rbd_types.h"
#include "include/rados/librados.hpp"
#include "common/errno.h"
#include "librbd/Utils.h"
#include "librbd/watcher/Utils.h"

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::TrashWatcher: " << __func__ << ": "

namespace librbd {

using namespace trash_watcher;
using namespace watcher;

using librbd::util::create_rados_callback;

namespace {

static const uint64_t NOTIFY_TIMEOUT_MS = 5000;

} // anonymous namespace

template <typename I>
TrashWatcher<I>::TrashWatcher(librados::IoCtx &io_ctx,
                              asio::ContextWQ *work_queue)
  : Watcher(io_ctx, work_queue, RBD_TRASH) {
}

template <typename I>
void TrashWatcher<I>::notify_image_added(
    librados::IoCtx &io_ctx, const std::string& image_id,
    const cls::rbd::TrashImageSpec& trash_image_spec, Context *on_finish) {
  CephContext *cct = reinterpret_cast<CephContext*>(io_ctx.cct());
  ldout(cct, 20) << dendl;

  bufferlist bl;
  encode(NotifyMessage{ImageAddedPayload{image_id, trash_image_spec}}, bl);

  librados::AioCompletion *comp = create_rados_callback(on_finish);
  int r = io_ctx.aio_notify(RBD_TRASH, comp, bl, NOTIFY_TIMEOUT_MS, nullptr);
  ceph_assert(r == 0);
  comp->release();
}

template <typename I>
void TrashWatcher<I>::notify_image_removed(librados::IoCtx &io_ctx,
                                           const std::string& image_id,
                                           Context *on_finish) {
  CephContext *cct = reinterpret_cast<CephContext*>(io_ctx.cct());
  ldout(cct, 20) << dendl;

  bufferlist bl;
  encode(NotifyMessage{ImageRemovedPayload{image_id}}, bl);

  librados::AioCompletion *comp = create_rados_callback(on_finish);
  int r = io_ctx.aio_notify(RBD_TRASH, comp, bl, NOTIFY_TIMEOUT_MS, nullptr);
  ceph_assert(r == 0);
  comp->release();
}

template <typename I>
void TrashWatcher<I>::handle_notify(uint64_t notify_id, uint64_t handle,
                                    uint64_t notifier_id, bufferlist &bl) {
  CephContext *cct = this->m_cct;
  ldout(cct, 15) << "notify_id=" << notify_id << ", "
                 << "handle=" << handle << dendl;


  NotifyMessage notify_message;
  try {
    auto iter = bl.cbegin();
    decode(notify_message, iter);
  } catch (const buffer::error &err) {
    lderr(cct) << "error decoding image notification: " << err.what()
               << dendl;
    Context *ctx = new C_NotifyAck(this, notify_id, handle);
    ctx->complete(0);
    return;
  }

  apply_visitor(watcher::util::HandlePayloadVisitor<TrashWatcher<I>>(
    this, notify_id, handle), notify_message.payload);
}

template <typename I>
bool TrashWatcher<I>::handle_payload(const ImageAddedPayload &payload,
                                     Context *on_notify_ack) {
  CephContext *cct = this->m_cct;
  ldout(cct, 20) << dendl;
  handle_image_added(payload.image_id, payload.trash_image_spec);
  return true;
}

template <typename I>
bool TrashWatcher<I>::handle_payload(const ImageRemovedPayload &payload,
                                     Context *on_notify_ack) {
  CephContext *cct = this->m_cct;
  ldout(cct, 20) << dendl;
  handle_image_removed(payload.image_id);
  return true;
}

template <typename I>
bool TrashWatcher<I>::handle_payload(const UnknownPayload &payload,
                                     Context *on_notify_ack) {
  return true;
}

} // namespace librbd

template class librbd::TrashWatcher<librbd::ImageCtx>;
