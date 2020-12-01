// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/migration/RawFormat.h"
#include "common/dout.h"
#include "common/errno.h"
#include "librbd/ImageCtx.h"
#include "librbd/ImageState.h"
#include "librbd/Utils.h"
#include "librbd/io/AioCompletion.h"
#include "librbd/io/ReadResult.h"
#include "librbd/migration/SnapshotInterface.h"
#include "librbd/migration/SourceSpecBuilder.h"

namespace librbd {
namespace migration {

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::migration::RawFormat: " << this \
                           << " " << __func__ << ": "

template <typename I>
RawFormat<I>::RawFormat(
    I* image_ctx, const json_spirit::mObject& json_object,
    const SourceSpecBuilder<I>* source_spec_builder)
  : m_image_ctx(image_ctx), m_json_object(json_object),
    m_source_spec_builder(source_spec_builder) {
}

template <typename I>
void RawFormat<I>::open(Context* on_finish) {
  auto cct = m_image_ctx->cct;
  ldout(cct, 10) << dendl;

  on_finish = new LambdaContext([this, on_finish](int r) {
    handle_open(r, on_finish); });

  // treat the base image as a HEAD-revision snapshot
  int r = m_source_spec_builder->build_snapshot(m_json_object, CEPH_NOSNAP,
                                                &m_snapshots[CEPH_NOSNAP]);
  if (r < 0) {
    lderr(cct) << "failed to build HEAD revision handler: " << cpp_strerror(r)
               << dendl;
    on_finish->complete(r);
    return;
  }

  auto gather_ctx = new C_Gather(cct, on_finish);
  SnapshotInterface* previous_snapshot = nullptr;
  for (auto& [_, snapshot] : m_snapshots) {
    snapshot->open(previous_snapshot, gather_ctx->new_sub());
    previous_snapshot = snapshot.get();
  }
  gather_ctx->activate();
}

template <typename I>
void RawFormat<I>::handle_open(int r, Context* on_finish) {
  auto cct = m_image_ctx->cct;
  ldout(cct, 10) << "r=" << r << dendl;

  if (r < 0) {
    lderr(cct) << "failed to open raw image: " << cpp_strerror(r)
               << dendl;
    m_image_ctx->state->close(new LambdaContext(
      [r, on_finish=on_finish](int _) { on_finish->complete(r); }));
    return;
  }

  auto head_snapshot = m_snapshots[CEPH_NOSNAP];
  ceph_assert(head_snapshot);

  m_image_ctx->image_lock.lock();
  m_image_ctx->size = head_snapshot->get_snap_info().size;
  m_image_ctx->image_lock.unlock();
  on_finish->complete(0);
}

template <typename I>
void RawFormat<I>::close(Context* on_finish) {
  auto cct = m_image_ctx->cct;
  ldout(cct, 10) << dendl;

  auto gather_ctx = new C_Gather(cct, on_finish);
  for (auto& [snap_id, snapshot] : m_snapshots) {
    snapshot->close(gather_ctx->new_sub());
  }

  gather_ctx->activate();
}

template <typename I>
void RawFormat<I>::get_snapshots(SnapInfos* snap_infos, Context* on_finish) {
  auto cct = m_image_ctx->cct;
  ldout(cct, 10) << dendl;

  snap_infos->clear();
  for (auto& [snap_id, snapshot] : m_snapshots) {
    if (snap_id == CEPH_NOSNAP) {
      continue;
    }
    snap_infos->emplace(snap_id, snapshot->get_snap_info());
  }
  on_finish->complete(0);
}

template <typename I>
void RawFormat<I>::get_image_size(uint64_t snap_id, uint64_t* size,
                                  Context* on_finish) {
  auto cct = m_image_ctx->cct;
  ldout(cct, 10) << dendl;

  auto snapshot_it = m_snapshots.find(snap_id);
  if (snapshot_it == m_snapshots.end()) {
    on_finish->complete(-ENOENT);
    return;
  }

  *size = snapshot_it->second->get_snap_info().size;
  on_finish->complete(0);
}

template <typename I>
bool RawFormat<I>::read(
    io::AioCompletion* aio_comp, uint64_t snap_id, io::Extents&& image_extents,
    io::ReadResult&& read_result, int op_flags, int read_flags,
    const ZTracer::Trace &parent_trace) {
  auto cct = m_image_ctx->cct;
  ldout(cct, 20) << "image_extents=" << image_extents << dendl;

  auto snapshot_it = m_snapshots.find(snap_id);
  if (snapshot_it == m_snapshots.end()) {
    aio_comp->fail(-ENOENT);
    return true;
  }

  snapshot_it->second->read(aio_comp, std::move(image_extents),
                            std::move(read_result), op_flags, read_flags,
                            parent_trace);
  return true;
}

template <typename I>
void RawFormat<I>::list_snaps(io::Extents&& image_extents,
                              io::SnapIds&& snap_ids, int list_snaps_flags,
                              io::SnapshotDelta* snapshot_delta,
                              const ZTracer::Trace &parent_trace,
                              Context* on_finish) {
  // raw does support snapshots so list the full IO extent as a delta
  auto& snapshot = (*snapshot_delta)[{CEPH_NOSNAP, CEPH_NOSNAP}];
  for (auto& image_extent : image_extents) {
    snapshot.insert(image_extent.first, image_extent.second,
                    {io::SPARSE_EXTENT_STATE_DATA, image_extent.second});
  }
  on_finish->complete(0);
}

} // namespace migration
} // namespace librbd

template class librbd::migration::RawFormat<librbd::ImageCtx>;
