// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/managed_lock/BreakRequest.h"
#include "common/dout.h"
#include "common/errno.h"
#include "include/neorados/RADOS.hpp"
#include "include/stringify.h"
#include "cls/lock/cls_lock_client.h"
#include "cls/lock/cls_lock_types.h"
#include "librbd/AsioEngine.h"
#include "librbd/ImageCtx.h"
#include "librbd/Utils.h"
#include "librbd/asio/ContextWQ.h"
#include "librbd/managed_lock/GetLockerRequest.h"

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::managed_lock::BreakRequest: " << this \
                           << " " << __func__ << ": "

namespace librbd {
namespace managed_lock {

using util::create_context_callback;
using util::create_rados_callback;

namespace {

struct C_BlacklistClient : public Context {
  librados::IoCtx &ioctx;
  std::string locker_address;
  uint32_t expire_seconds;
  Context *on_finish;

  C_BlacklistClient(librados::IoCtx &ioctx, const std::string &locker_address,
                    uint32_t expire_seconds, Context *on_finish)
    : ioctx(ioctx), locker_address(locker_address),
      expire_seconds(expire_seconds), on_finish(on_finish) {
  }

  void finish(int r) override {
    librados::Rados rados(ioctx);
    r = rados.blacklist_add(locker_address, expire_seconds);
    on_finish->complete(r);
  }
};

} // anonymous namespace

template <typename I>
BreakRequest<I>::BreakRequest(librados::IoCtx& ioctx,
                              AsioEngine& asio_engine,
                              const std::string& oid, const Locker &locker,
                              bool exclusive, bool blacklist_locker,
                              uint32_t blacklist_expire_seconds,
                              bool force_break_lock, Context *on_finish)
  : m_ioctx(ioctx), m_cct(reinterpret_cast<CephContext *>(m_ioctx.cct())),
    m_asio_engine(asio_engine), m_oid(oid), m_locker(locker),
    m_exclusive(exclusive), m_blacklist_locker(blacklist_locker),
    m_blacklist_expire_seconds(blacklist_expire_seconds),
    m_force_break_lock(force_break_lock), m_on_finish(on_finish) {
}

template <typename I>
void BreakRequest<I>::send() {
  send_get_watchers();
}

template <typename I>
void BreakRequest<I>::send_get_watchers() {
  ldout(m_cct, 10) << dendl;

  librados::ObjectReadOperation op;
  op.list_watchers(&m_watchers, &m_watchers_ret_val);

  using klass = BreakRequest<I>;
  librados::AioCompletion *rados_completion =
    create_rados_callback<klass, &klass::handle_get_watchers>(this);
  m_out_bl.clear();
  int r = m_ioctx.aio_operate(m_oid, rados_completion, &op, &m_out_bl);
  ceph_assert(r == 0);
  rados_completion->release();
}

template <typename I>
void BreakRequest<I>::handle_get_watchers(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;

  if (r == 0) {
    r = m_watchers_ret_val;
  }
  if (r < 0) {
    lderr(m_cct) << "failed to retrieve watchers: " << cpp_strerror(r)
                 << dendl;
    finish(r);
    return;
  }

  bool found_alive_locker = false;
  for (auto &watcher : m_watchers) {
    ldout(m_cct, 20) << "watcher=["
                     << "addr=" << watcher.addr << ", "
                     << "entity=client." << watcher.watcher_id << "]" << dendl;

    if ((strncmp(m_locker.address.c_str(),
                 watcher.addr, sizeof(watcher.addr)) == 0) &&
        (m_locker.handle == watcher.cookie)) {
      ldout(m_cct, 10) << "lock owner is still alive" << dendl;
      found_alive_locker = true;
    }
  }

  if (!m_force_break_lock && found_alive_locker) {
    finish(-EAGAIN);
    return;
  }

  send_get_locker();
}

template <typename I>
void BreakRequest<I>::send_get_locker() {
  ldout(m_cct, 10) << dendl;

  using klass = BreakRequest<I>;
  Context *ctx = create_context_callback<klass, &klass::handle_get_locker>(
    this);
  auto req = GetLockerRequest<I>::create(m_ioctx, m_oid, m_exclusive,
                                         &m_refreshed_locker, ctx);
  req->send();
}

template <typename I>
void BreakRequest<I>::handle_get_locker(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;

  if (r == -ENOENT) {
    ldout(m_cct, 5) << "no lock owner" << dendl;
    finish(0);
    return;
  } else if (r < 0 && r != -EBUSY) {
    lderr(m_cct) << "failed to retrieve lockers: " << cpp_strerror(r) << dendl;
    finish(r);
    return;
  } else if (r < 0) {
    m_refreshed_locker = {};
  }

  if (m_refreshed_locker != m_locker || m_refreshed_locker == Locker{}) {
    ldout(m_cct, 5) << "no longer lock owner" << dendl;
    finish(-EAGAIN);
    return;
  }

  send_blacklist();
}

template <typename I>
void BreakRequest<I>::send_blacklist() {
  if (!m_blacklist_locker) {
    send_break_lock();
    return;
  }

  entity_name_t entity_name = entity_name_t::CLIENT(m_ioctx.get_instance_id());
  ldout(m_cct, 10) << "local entity=" << entity_name << ", "
                   << "locker entity=" << m_locker.entity << dendl;

  if (m_locker.entity == entity_name) {
    lderr(m_cct) << "attempting to self-blacklist" << dendl;
    finish(-EINVAL);
    return;
  }

  // TODO: need async version of RadosClient::blacklist_add
  using klass = BreakRequest<I>;
  Context *ctx = create_context_callback<klass, &klass::handle_blacklist>(
    this);
  m_asio_engine.get_work_queue()->queue(
    new C_BlacklistClient(m_ioctx, m_locker.address,
                          m_blacklist_expire_seconds, ctx), 0);
}

template <typename I>
void BreakRequest<I>::handle_blacklist(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;

  if (r < 0) {
    lderr(m_cct) << "failed to blacklist lock owner: " << cpp_strerror(r)
                 << dendl;
    finish(r);
    return;
  }
  send_break_lock();
}

template <typename I>
void BreakRequest<I>::send_break_lock() {
  ldout(m_cct, 10) << dendl;

  librados::ObjectWriteOperation op;
  rados::cls::lock::break_lock(&op, RBD_LOCK_NAME, m_locker.cookie,
                               m_locker.entity);

  using klass = BreakRequest<I>;
  librados::AioCompletion *rados_completion =
    create_rados_callback<klass, &klass::handle_break_lock>(this);
  int r = m_ioctx.aio_operate(m_oid, rados_completion, &op);
  ceph_assert(r == 0);
  rados_completion->release();
}

template <typename I>
void BreakRequest<I>::handle_break_lock(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;

  if (r < 0 && r != -ENOENT) {
    lderr(m_cct) << "failed to break lock: " << cpp_strerror(r) << dendl;
    finish(r);
    return;
  }

  finish(0);
}

template <typename I>
void BreakRequest<I>::finish(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;

  m_on_finish->complete(r);
  delete this;
}

} // namespace managed_lock
} // namespace librbd

template class librbd::managed_lock::BreakRequest<librbd::ImageCtx>;
