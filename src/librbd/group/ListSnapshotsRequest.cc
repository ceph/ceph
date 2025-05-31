// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/group/ListSnapshotsRequest.h"
#include "include/ceph_assert.h"
#include "common/dout.h"
#include "common/errno.h"
#include "common/ceph_context.h"
#include "cls/rbd/cls_rbd_client.h"
#include "librbd/Utils.h"

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::group::ListSnapshotsRequest: " << this \
                           << " " << __func__ << ": "

namespace librbd {
namespace group {

namespace {

const uint32_t MAX_RETURN = 1024;

} // anonymous namespace

template <typename I>
ListSnapshotsRequest<I>::ListSnapshotsRequest(librados::IoCtx &group_io_ctx,
                                              const std::string &group_id,
                                              bool try_to_sort,
                                              bool fail_if_not_sorted,
                                              std::vector<cls::rbd::GroupSnapshot> *snaps,
                                              Context *on_finish)
     : m_group_io_ctx(group_io_ctx), m_group_id(group_id),
       m_try_to_sort(try_to_sort), m_fail_if_not_sorted(fail_if_not_sorted),
       m_snaps(snaps), m_on_finish(on_finish) {
  auto cct = reinterpret_cast<CephContext*>(m_group_io_ctx.cct());
  ldout(cct, 20) << "group_id=" << m_group_id
                 << ", try_to_sort=" << m_try_to_sort
                 << ", fail_if_not_sorted=" << m_fail_if_not_sorted
                 << dendl;
}

template <typename I>
void ListSnapshotsRequest<I>::send() {
  list_snap_orders();
}

template <typename I>
void ListSnapshotsRequest<I>::list_snap_orders() {
  if (!m_try_to_sort) {
    list_snaps();
    return;
  }

  auto cct = reinterpret_cast<CephContext*>(m_group_io_ctx.cct());
  ldout(cct, 10) << dendl;

  librados::ObjectReadOperation op;
  cls_client::group_snap_list_order_start(&op, m_start_after_order, MAX_RETURN);
  auto comp = util::create_rados_callback<
      ListSnapshotsRequest<I>,
      &ListSnapshotsRequest<I>::handle_list_snap_orders>(this);
  m_out_bl.clear();
  int r = m_group_io_ctx.aio_operate(util::group_header_name(m_group_id), comp,
                                     &op, &m_out_bl);
  ceph_assert(r == 0);
  comp->release();
}

template <typename I>
void ListSnapshotsRequest<I>::handle_list_snap_orders(int r) {
  auto cct = reinterpret_cast<CephContext*>(m_group_io_ctx.cct());
  ldout(cct, 10) << "r=" << r << dendl;

  std::map<std::string, uint64_t> snap_orders;
  if (r == 0) {
    auto iter = m_out_bl.cbegin();
    r = cls_client::group_snap_list_order_finish(&iter, &snap_orders);
  }

  if (r < 0) {
    if (r == -EOPNOTSUPP && !m_fail_if_not_sorted) {
      list_snaps();
      return;
    } else {
      lderr(cct) << "failed to get group snapshot orders: " << cpp_strerror(r)
                 << dendl;
      finish(r);
      return;
    }
  }

  m_snap_orders.insert(snap_orders.begin(), snap_orders.end());
  if (snap_orders.size() < MAX_RETURN) {
    if (m_retried_snap_orders) {
      sort_snaps();
    } else {
      list_snaps();
    }
    return;
  }

  m_start_after_order = snap_orders.rbegin()->first;
  list_snap_orders();
}

template <typename I>
void ListSnapshotsRequest<I>::list_snaps() {
  auto cct = reinterpret_cast<CephContext*>(m_group_io_ctx.cct());
  ldout(cct, 10) << dendl;

  librados::ObjectReadOperation op;
  cls_client::group_snap_list_start(&op, m_start_after, MAX_RETURN);
  auto comp = util::create_rados_callback<
      ListSnapshotsRequest<I>,
      &ListSnapshotsRequest<I>::handle_list_snaps>(this);
  m_out_bl.clear();
  int r = m_group_io_ctx.aio_operate(util::group_header_name(m_group_id), comp,
                                     &op, &m_out_bl);
  ceph_assert(r == 0);
  comp->release();
}

template <typename I>
void ListSnapshotsRequest<I>::handle_list_snaps(int r) {
  auto cct = reinterpret_cast<CephContext*>(m_group_io_ctx.cct());
  ldout(cct, 10) << "r=" << r << dendl;

  std::vector<cls::rbd::GroupSnapshot> snaps;
  if (r == 0) {
    auto iter = m_out_bl.cbegin();
    r = cls_client::group_snap_list_finish(&iter, &snaps);
  }

  if (r < 0) {
    lderr(cct) << "failed to list group snapshots: " << cpp_strerror(r)
               << dendl;
    finish(r);
    return;
  }

  m_snaps->insert(m_snaps->end(), snaps.begin(), snaps.end());
  if (snaps.size() < MAX_RETURN) {
    sort_snaps();
    return;
  }

  m_start_after = *snaps.rbegin();
  list_snaps();
}

template <typename I>
void ListSnapshotsRequest<I>::sort_snaps() {
  if (!m_try_to_sort) {
    finish(0);
    return;
  }

  auto cct = reinterpret_cast<CephContext*>(m_group_io_ctx.cct());
  ldout(cct, 10) << dendl;

  for (const auto& snap : *m_snaps) {
    if (m_snap_orders.find(snap.id) == m_snap_orders.end()) {
      ldout(cct, 10) << "Missing order for snap_id=" << snap.id << dendl;
      if (m_fail_if_not_sorted) {
        if (!m_retried_snap_orders) {
          ldout(cct, 10) << "Retrying to fetch missing snap orders..." << dendl;
          m_retried_snap_orders = true;
          m_start_after_order = "";
          list_snap_orders();
        } else {
          finish(-EINVAL);
        }
      } else {
        finish(0);
      }
      return;
    }
  }

  std::sort(m_snaps->begin(), m_snaps->end(),
            [this](const cls::rbd::GroupSnapshot &a,
                   const cls::rbd::GroupSnapshot &b) {
	       return this->m_snap_orders[a.id] < this->m_snap_orders[b.id];
	    });

  finish(0);
}

template <typename I>
void ListSnapshotsRequest<I>::finish(int r) {
  auto cct = reinterpret_cast<CephContext*>(m_group_io_ctx.cct());
  ldout(cct, 10) << "r=" << r << dendl;

  m_on_finish->complete(r);
  delete this;
}

} // namespace group
} // namespace librbd

template class librbd::group::ListSnapshotsRequest<librbd::ImageCtx>;
