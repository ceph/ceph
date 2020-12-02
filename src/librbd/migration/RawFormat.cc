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

namespace {

static const std::string SNAPSHOTS_KEY {"snapshots"};


} // anonymous namespace

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
  Snapshots snapshots;
  int r = m_source_spec_builder->build_snapshot(m_json_object, CEPH_NOSNAP,
                                                &snapshots[CEPH_NOSNAP]);
  if (r < 0) {
    lderr(cct) << "failed to build HEAD revision handler: " << cpp_strerror(r)
               << dendl;
    on_finish->complete(r);
    return;
  }

  auto& snapshots_val = m_json_object[SNAPSHOTS_KEY];
  if (snapshots_val.type() == json_spirit::array_type) {
    auto& snapshots_arr = snapshots_val.get_array();
    for (auto& snapshot_val : snapshots_arr) {
      uint64_t index = snapshots.size();
      if (snapshot_val.type() != json_spirit::obj_type) {
        lderr(cct) << "invalid snapshot " << index << " JSON: "
                   << cpp_strerror(r) << dendl;
        on_finish->complete(-EINVAL);
        return;
      }

      auto& snapshot_obj = snapshot_val.get_obj();
      r = m_source_spec_builder->build_snapshot(snapshot_obj, index,
                                                &snapshots[index]);
      if (r < 0) {
        lderr(cct) << "failed to build snapshot " << index << " handler: "
                   << cpp_strerror(r) << dendl;
        on_finish->complete(r);
        return;
      }
    }
  } else if (snapshots_val.type() != json_spirit::null_type) {
    lderr(cct) << "invalid snapshots array" << dendl;
    on_finish->complete(-EINVAL);
    return;
  }

  m_snapshots = std::move(snapshots);

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

    auto gather_ctx = new C_Gather(cct, on_finish);
    for (auto& [_, snapshot] : m_snapshots) {
      snapshot->close(gather_ctx->new_sub());
    }

    m_image_ctx->state->close(new LambdaContext(
      [r, on_finish=gather_ctx->new_sub()](int _) { on_finish->complete(r); }));

    gather_ctx->activate();
    return;
  }

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
  ldout(cct, 20) << "snap_id=" << snap_id << ", "
                 << "image_extents=" << image_extents << dendl;

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
  auto cct = m_image_ctx->cct;
  ldout(cct, 20) << "image_extents=" << image_extents << dendl;

  on_finish = new LambdaContext([this, snap_ids=std::move(snap_ids),
                                 snapshot_delta, on_finish](int r) mutable {
    handle_list_snaps(r, std::move(snap_ids), snapshot_delta, on_finish);
  });

  auto gather_ctx = new C_Gather(cct, on_finish);

  std::optional<uint64_t> previous_size = std::nullopt;
  for (auto& [snap_id, snapshot] : m_snapshots) {
    auto& sparse_extents = (*snapshot_delta)[{snap_id, snap_id}];

    // zero out any space between the previous snapshot end and this
    // snapshot's end
    auto& snap_info = snapshot->get_snap_info();
    if (previous_size && *previous_size > snap_info.size) {
      ldout(cct, 20) << "snapshot resize " << *previous_size << " -> "
                     << snap_info.size << dendl;
      interval_set<uint64_t> zero_interval;
      zero_interval.insert(snap_info.size, *previous_size - snap_info.size);

      for (auto& image_extent : image_extents) {
        interval_set<uint64_t> image_interval;
        image_interval.insert(image_extent.first, image_extent.second);

        image_interval.intersection_of(zero_interval);
        for (auto [image_offset, image_length] : image_interval) {
          ldout(cct, 20) << "zeroing extent " << image_offset << "~"
                         << image_length << " at snapshot " << snap_id << dendl;
          sparse_extents.insert(image_offset, image_length,
                                {io::SPARSE_EXTENT_STATE_ZEROED, image_length});
        }
      }
    }
    previous_size = snap_info.size;

    // build set of data/zeroed extents for the current snapshot
    snapshot->list_snap(io::Extents{image_extents}, list_snaps_flags,
                        &sparse_extents, parent_trace, gather_ctx->new_sub());
  }

  gather_ctx->activate();
}

template <typename I>
void RawFormat<I>::handle_list_snaps(int r, io::SnapIds&& snap_ids,
                                     io::SnapshotDelta* snapshot_delta,
                                     Context* on_finish) {
  auto cct = m_image_ctx->cct;
  ldout(cct, 20) << "r=" << r << ", "
                 << "snapshot_delta=" << snapshot_delta << dendl;

  io::SnapshotDelta orig_snapshot_delta = std::move(*snapshot_delta);
  snapshot_delta->clear();

  auto snap_id_it = snap_ids.begin();
  ceph_assert(snap_id_it != snap_ids.end());

  // merge any snapshot intervals that were not requested
  std::list<io::SparseExtents*> pending_sparse_extents;
  for (auto& [snap_key, sparse_extents] : orig_snapshot_delta) {
    // advance to next valid requested snap id
    while (snap_id_it != snap_ids.end() && *snap_id_it < snap_key.first) {
      ++snap_id_it;
    }
    if (snap_id_it == snap_ids.end()) {
      break;
    }

    // loop through older write/read snapshot sparse extents to remove any
    // overlaps with the current sparse extent
    for (auto prev_sparse_extents : pending_sparse_extents) {
      for (auto& sparse_extent : sparse_extents) {
        prev_sparse_extents->erase(sparse_extent.get_off(),
                                   sparse_extent.get_len());
      }
    }

    auto write_read_snap_ids = std::make_pair(*snap_id_it, snap_key.second);
    (*snapshot_delta)[write_read_snap_ids] = std::move(sparse_extents);

    if (write_read_snap_ids.first > snap_key.first) {
      // the current snapshot wasn't requested so it might need to get
      // merged with a later snapshot
      pending_sparse_extents.push_back(&(*snapshot_delta)[write_read_snap_ids]);
    } else {
      // we don't merge results passed a valid requested snapshot
      pending_sparse_extents.clear();
    }
  }

  on_finish->complete(r);
}

} // namespace migration
} // namespace librbd

template class librbd::migration::RawFormat<librbd::ImageCtx>;
