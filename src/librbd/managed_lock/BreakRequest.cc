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
#include "librbd/asio/Utils.h"
#include "librbd/managed_lock/GetLockerRequest.h"

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::managed_lock::BreakRequest: " << this \
                           << " " << __func__ << ": "

namespace librbd {
namespace managed_lock {

using util::create_context_callback;
using util::create_rados_callback;

template <typename I>
BreakRequest<I>::BreakRequest(librados::IoCtx& ioctx,
                              AsioEngine& asio_engine,
                              const std::string& oid, const Locker &locker,
                              bool exclusive, bool blocklist_locker,
                              uint32_t blocklist_expire_seconds,
                              bool force_break_lock, Context *on_finish)
  : m_ioctx(ioctx), m_cct(reinterpret_cast<CephContext *>(m_ioctx.cct())),
    m_asio_engine(asio_engine), m_oid(oid), m_locker(locker),
    m_exclusive(exclusive), m_blocklist_locker(blocklist_locker),
    m_blocklist_expire_seconds(blocklist_expire_seconds),
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

  send_blocklist();
}

template <typename I>
void BreakRequest<I>::send_blocklist() {
  if (!m_blocklist_locker) {
    send_break_lock();
    return;
  }

  entity_name_t entity_name = entity_name_t::CLIENT(m_ioctx.get_instance_id());
  ldout(m_cct, 10) << "local entity=" << entity_name << ", "
                   << "locker entity=" << m_locker.entity << dendl;

  if (m_locker.entity == entity_name) {
    lderr(m_cct) << "attempting to self-blocklist" << dendl;
    finish(-EINVAL);
    return;
  }

  entity_addr_t locker_addr;
  if (!locker_addr.parse(m_locker.address)) {
    lderr(m_cct) << "unable to parse locker address: " << m_locker.address
                 << dendl;
    finish(-EINVAL);
    return;
  }

  std::optional<std::chrono::seconds> expire;
  if (m_blocklist_expire_seconds != 0) {
    expire = std::chrono::seconds(m_blocklist_expire_seconds);
  }
  m_asio_engine.get_rados_api().blocklist_add(
    m_locker.address, expire,
    librbd::asio::util::get_callback_adapter(
      [this](int r) { handle_blocklist(r); }));
}

template <typename I>
void BreakRequest<I>::handle_blocklist(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;

  if (r < 0) {
    lderr(m_cct) << "failed to blocklist lock owner: " << cpp_strerror(r)
                 << dendl;
    finish(r);
    return;
  }

  wait_for_osd_map();
}

template <typename I>
void BreakRequest<I>::wait_for_osd_map() {
  ldout(m_cct, 10) << dendl;

  m_asio_engine.get_rados_api().wait_for_latest_osd_map(
    librbd::asio::util::get_callback_adapter(
      [this](int r) { handle_wait_for_osd_map(r); }));
}

template <typename I>
void BreakRequest<I>::handle_wait_for_osd_map(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;

  if (r < 0) {
    lderr(m_cct) << "failed to wait for updated OSD map: " << cpp_strerror(r)
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
