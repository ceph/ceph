// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "SyncPointHandler.h"
#include "StateBuilder.h"
#include "include/ceph_assert.h"
#include "include/Context.h"
#include "common/debug.h"
#include "common/errno.h"
#include "journal/Journaler.h"
#include "librbd/ImageCtx.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rbd_mirror
#undef dout_prefix
#define dout_prefix *_dout << "rbd::mirror::image_replayer::journal::" \
                           << "SyncPointHandler: " << this << " " \
                           << __func__ << ": "

namespace rbd {
namespace mirror {
namespace image_replayer {
namespace journal {

template <typename I>
SyncPointHandler<I>::SyncPointHandler(StateBuilder<I>* state_builder)
  : m_state_builder(state_builder),
    m_client_meta_copy(state_builder->remote_client_meta) {
}

template <typename I>
typename SyncPointHandler<I>::SyncPoints
SyncPointHandler<I>::get_sync_points() const {
  SyncPoints sync_points;
  for (auto& sync_point : m_client_meta_copy.sync_points) {
    sync_points.emplace_back(
      sync_point.snap_namespace,
      sync_point.snap_name,
      sync_point.from_snap_name,
      sync_point.object_number);
  }
  return sync_points;
}

template <typename I>
librbd::SnapSeqs SyncPointHandler<I>::get_snap_seqs() const {
  return m_client_meta_copy.snap_seqs;
}

template <typename I>
void SyncPointHandler<I>::update_sync_points(
    const librbd::SnapSeqs& snap_seqs, const SyncPoints& sync_points,
    bool sync_complete, Context* on_finish) {
  dout(10) << dendl;

  if (sync_complete && sync_points.empty()) {
    m_client_meta_copy.state = librbd::journal::MIRROR_PEER_STATE_REPLAYING;
  }

  m_client_meta_copy.snap_seqs = snap_seqs;
  m_client_meta_copy.sync_points.clear();
  for (auto& sync_point : sync_points) {
    m_client_meta_copy.sync_points.emplace_back(
      sync_point.snap_namespace,
      sync_point.snap_name,
      sync_point.from_snap_name,
      sync_point.object_number);

    if (sync_point.object_number) {
      m_client_meta_copy.sync_object_count = std::max(
        m_client_meta_copy.sync_object_count, *sync_point.object_number + 1);
    }
  }

  dout(20) << "client_meta=" << m_client_meta_copy << dendl;
  bufferlist client_data_bl;
  librbd::journal::ClientData client_data{m_client_meta_copy};
  encode(client_data, client_data_bl);

  auto ctx = new LambdaContext([this, on_finish](int r) {
      handle_update_sync_points(r, on_finish);
    });
  m_state_builder->remote_journaler->update_client(client_data_bl, ctx);
}

template <typename I>
void SyncPointHandler<I>::handle_update_sync_points(int r, Context* on_finish) {
  dout(10) << "r=" << r << dendl;

  if (r >= 0) {
    m_state_builder->remote_client_meta.snap_seqs =
      m_client_meta_copy.snap_seqs;
    m_state_builder->remote_client_meta.sync_points =
      m_client_meta_copy.sync_points;
  } else {
    derr << "failed to update remote journal client meta for image "
         << m_state_builder->global_image_id << ": " << cpp_strerror(r)
         << dendl;
  }

  on_finish->complete(r);
}

} // namespace journal
} // namespace image_sync
} // namespace mirror
} // namespace rbd

template class rbd::mirror::image_replayer::journal::SyncPointHandler<librbd::ImageCtx>;
