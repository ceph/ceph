// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/exclusive_lock/BreakRequest.h"
#include "common/dout.h"
#include "common/errno.h"
#include "common/WorkQueue.h"
#include "include/stringify.h"
#include "cls/lock/cls_lock_client.h"
#include "cls/lock/cls_lock_types.h"
#include "librbd/ExclusiveLock.h"
#include "librbd/Utils.h"
#include "librbd/exclusive_lock/Types.h"

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::exclusive_lock::BreakRequest: " << this \
                           << " " << __func__ << ": "

namespace librbd {
namespace exclusive_lock {

using util::create_context_callback;
using util::create_rados_ack_callback;
using util::create_rados_safe_callback;

namespace {

template <typename I>
struct C_BlacklistClient : public Context {
  I &image_ctx;
  std::string locker_address;
  Context *on_finish;

  C_BlacklistClient(I &image_ctx, const std::string &locker_address,
                    Context *on_finish)
    : image_ctx(image_ctx), locker_address(locker_address),
      on_finish(on_finish) {
  }

  virtual void finish(int r) override {
    librados::Rados rados(image_ctx.md_ctx);
    r = rados.blacklist_add(locker_address,
                            image_ctx.blacklist_expire_seconds);
    on_finish->complete(r);
  }
};

} // anonymous namespace

template <typename I>
void BreakRequest<I>::send() {
  send_get_watchers();
}

template <typename I>
void BreakRequest<I>::send_get_watchers() {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << dendl;

  librados::ObjectReadOperation op;
  op.list_watchers(&m_watchers, &m_watchers_ret_val);

  using klass = BreakRequest<I>;
  librados::AioCompletion *rados_completion =
    create_rados_ack_callback<klass, &klass::handle_get_watchers>(this);
  m_out_bl.clear();
  int r = m_image_ctx.md_ctx.aio_operate(m_image_ctx.header_oid,
                                         rados_completion, &op, &m_out_bl);
  assert(r == 0);
  rados_completion->release();
}

template <typename I>
void BreakRequest<I>::handle_get_watchers(int r) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << "r=" << r << dendl;

  if (r == 0) {
    r = m_watchers_ret_val;
  }
  if (r < 0) {
    lderr(cct) << "failed to retrieve watchers: " << cpp_strerror(r)
               << dendl;
    finish(r);
    return;
  }

  for (auto &watcher : m_watchers) {
    if ((strncmp(m_locker.address.c_str(),
                 watcher.addr, sizeof(watcher.addr)) == 0) &&
        (m_locker.handle == watcher.cookie)) {
      ldout(cct, 10) << "lock owner is still alive" << dendl;

      if (m_force_break_lock) {
        break;
      } else {
        finish(-EAGAIN);
        return;
      }
    }
  }

  send_blacklist();
}

template <typename I>
void BreakRequest<I>::send_blacklist() {
  if (!m_blacklist_locker) {
    send_break_lock();
    return;
  }

  CephContext *cct = m_image_ctx.cct;
  entity_name_t entity_name = entity_name_t::CLIENT(
    m_image_ctx.md_ctx.get_instance_id());
  ldout(cct, 10) << "local entity=" << entity_name << ", "
                 << "locker entity=" << m_locker.entity << dendl;

  if (m_locker.entity == entity_name) {
    lderr(cct) << "attempting to self-blacklist" << dendl;
    finish(-EINVAL);
    return;
  }

  // TODO: need async version of RadosClient::blacklist_add
  using klass = BreakRequest<I>;
  Context *ctx = create_context_callback<klass, &klass::handle_blacklist>(
    this);
  m_image_ctx.op_work_queue->queue(new C_BlacklistClient<I>(m_image_ctx,
                                                            m_locker.address,
                                                            ctx), 0);
}

template <typename I>
void BreakRequest<I>::handle_blacklist(int r) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << "r=" << r << dendl;

  if (r < 0) {
    lderr(cct) << "failed to blacklist lock owner: " << cpp_strerror(r)
               << dendl;
    finish(r);
    return;
  }
  send_break_lock();
}

template <typename I>
void BreakRequest<I>::send_break_lock() {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << dendl;

  librados::ObjectWriteOperation op;
  rados::cls::lock::break_lock(&op, RBD_LOCK_NAME, m_locker.cookie,
                               m_locker.entity);

  using klass = BreakRequest<I>;
  librados::AioCompletion *rados_completion =
    create_rados_safe_callback<klass, &klass::handle_break_lock>(this);
  int r = m_image_ctx.md_ctx.aio_operate(m_image_ctx.header_oid,
                                         rados_completion, &op);
  assert(r == 0);
  rados_completion->release();
}

template <typename I>
void BreakRequest<I>::handle_break_lock(int r) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << "r=" << r << dendl;

  if (r < 0 && r != -ENOENT) {
    lderr(cct) << "failed to break lock: " << cpp_strerror(r) << dendl;
    finish(r);
    return;
  }

  finish(0);
}

template <typename I>
void BreakRequest<I>::finish(int r) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << "r=" << r << dendl;

  m_on_finish->complete(r);
  delete this;
}

} // namespace exclusive_lock
} // namespace librbd

template class librbd::exclusive_lock::BreakRequest<librbd::ImageCtx>;
