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

using util::create_rados_callback;

namespace {

static const uint32_t MAX_RETURN = 1024;

} // anonymous namespace

template <typename I>
void ListSnapshotsRequest<I>::send() {
  list_snapshots();
}

template <typename I>
void ListSnapshotsRequest<I>::list_snapshots() {
  CephContext *cct = (CephContext *)m_group_io_ctx.cct();
  ldout(cct, 10) << dendl;

  librados::ObjectReadOperation op;
  cls_client::group_snap_list_start(&op, m_start_after, MAX_RETURN);
  auto comp = create_rados_callback<
      ListSnapshotsRequest<I>, &ListSnapshotsRequest<I>::handle_list_snapshots>(this);
  m_out_bl.clear();
  int r = m_group_io_ctx.aio_operate(util::group_header_name(m_group_id), comp,
                                     &op, &m_out_bl);
  ceph_assert(r == 0);
  comp->release();
}

template <typename I>
void ListSnapshotsRequest<I>::handle_list_snapshots(int r) {
  CephContext *cct = (CephContext *)m_group_io_ctx.cct();
  ldout(cct, 10) << "r=" << r << dendl;

  if (r == -ERESTART && !m_start_after.id.empty()) {
    m_snaps->clear();
    m_start_after = {};
    list_snapshots();
    return;
  }

  std::vector<cls::rbd::GroupSnapshot> snaps;
  if (r == 0) {
    auto iter = m_out_bl.cbegin();
    r = librbd::cls_client::group_snap_list_finish(&iter, &snaps);
  }

  if (r < 0) {
    lderr(cct) << "failed to list group snapshots: " << cpp_strerror(r)
               << dendl;
    finish(r);
    return;
  }

  m_snaps->insert(m_snaps->end(), snaps.begin(), snaps.end());
  if (snaps.size() < MAX_RETURN) {
    finish(0);
    return;
  }

  m_start_after = *snaps.rbegin();
  list_snapshots();
}

template <typename I>
void ListSnapshotsRequest<I>::finish(int r) {
  CephContext *cct = (CephContext *)m_group_io_ctx.cct();
  ldout(cct, 10) << "r=" << r << dendl;

  m_on_finish->complete(r);
}

} // namespace group
} // namespace librbd

template class librbd::group::ListSnapshotsRequest<librbd::ImageCtx>;
