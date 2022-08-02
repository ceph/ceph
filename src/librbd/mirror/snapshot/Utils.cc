// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "common/dout.h"
#include "common/errno.h"
#include "include/stringify.h"
#include "librbd/ImageCtx.h"
#include "librbd/mirror/snapshot/Utils.h"

#define dout_subsys ceph_subsys_rbd

#undef dout_prefix
#define dout_prefix *_dout << "librbd::mirror::snapshot::util: " \
                           << " " << __func__ << ": "

namespace librbd {
namespace mirror {
namespace snapshot {
namespace util {

namespace {

const std::string IMAGE_STATE_OBJECT_PREFIX = "rbd_mirror_snapshot.";

bool get_rollback_snap_id(
    std::map<librados::snap_t, SnapInfo>::reverse_iterator it,
    std::map<librados::snap_t, SnapInfo>::reverse_iterator end,
    uint64_t *rollback_snap_id) {

  for (; it != end; it++) {
    auto mirror_ns = std::get<cls::rbd::MirrorSnapshotNamespace>(
      it->second.snap_namespace);
    if (mirror_ns.state != cls::rbd::MIRROR_SNAPSHOT_STATE_NON_PRIMARY) {
      break;
    }
    if (mirror_ns.complete) {
      break;
    }
  }

  if (it != end) {
    *rollback_snap_id = it->first;
    return true;
  }

  return false;
}

} // anonymous namespace

std::string get_image_meta_key(const std::string& mirror_uuid) {
  return ".rbd_mirror." + mirror_uuid;
}

template <typename I>
bool can_create_primary_snapshot(I *image_ctx, bool demoted, bool force,
                                 bool* requires_orphan,
                                 uint64_t *rollback_snap_id) {
  CephContext *cct = image_ctx->cct;

  if (requires_orphan != nullptr) {
    *requires_orphan = false;
  }
  if (rollback_snap_id) {
    *rollback_snap_id = CEPH_NOSNAP;
  }

  std::shared_lock image_locker{image_ctx->image_lock};

  for (auto it = image_ctx->snap_info.rbegin();
       it != image_ctx->snap_info.rend(); it++) {
    auto mirror_ns = std::get_if<cls::rbd::MirrorSnapshotNamespace>(
      &it->second.snap_namespace);
    if (mirror_ns == nullptr) {
      continue;
    }
    ldout(cct, 20) << "previous snapshot snap_id=" << it->first << " "
                   << *mirror_ns << dendl;
    if (mirror_ns->is_demoted() && !force) {
      lderr(cct) << "trying to create primary snapshot without force "
                 << "when previous primary snapshot is demoted"
                 << dendl;
      return false;
    }

    if (mirror_ns->state == cls::rbd::MIRROR_SNAPSHOT_STATE_NON_PRIMARY) {
      if (!force) {
        lderr(cct) << "trying to create primary snapshot without force "
                   << "when previous snapshot is non-primary"
                   << dendl;
        return false;
      }
      if (demoted) {
        lderr(cct) << "trying to create primary demoted snapshot "
                   << "when previous snapshot is non-primary"
                   << dendl;
        return false;
      }

      if (requires_orphan != nullptr) {
        *requires_orphan = !mirror_ns->is_demoted();
      }
      if (!mirror_ns->complete) {
        ldout(cct, 20) << "needs rollback" << dendl;
        if (!rollback_snap_id) {
          lderr(cct) << "trying to create primary snapshot "
                     << "when previous non-primary snapshot is not copied yet"
                     << dendl;
          return false;
        }
        if (!get_rollback_snap_id(++it, image_ctx->snap_info.rend(),
                                  rollback_snap_id)) {
          lderr(cct) << "cannot rollback" << dendl;
          return false;
        }
        ldout(cct, 20) << "rollback_snap_id=" << *rollback_snap_id << dendl;
      }
      return true;
    }

    return true;
  }

  ldout(cct, 20) << "no previous mirror snapshots found" << dendl;
  return true;
}

template <typename I>
bool can_create_non_primary_snapshot(I *image_ctx) {
  CephContext *cct = image_ctx->cct;

  std::shared_lock image_locker{image_ctx->image_lock};

  for (auto it = image_ctx->snap_info.rbegin();
       it != image_ctx->snap_info.rend(); it++) {
    auto mirror_ns = std::get_if<cls::rbd::MirrorSnapshotNamespace>(
      &it->second.snap_namespace);
    if (mirror_ns != nullptr) {
      ldout(cct, 20) << "previous mirror snapshot snap_id=" << it->first << " "
                     << *mirror_ns << dendl;

      if (mirror_ns->state == cls::rbd::MIRROR_SNAPSHOT_STATE_NON_PRIMARY) {
        if (!mirror_ns->complete) {
          lderr(cct) << "trying to create non-primary snapshot "
                     << "when previous non-primary snapshot is not copied yet"
                     << dendl;
          return false;
        }
        return true;
      }

      if (mirror_ns->state == cls::rbd::MIRROR_SNAPSHOT_STATE_PRIMARY) {
        lderr(cct) << "trying to create non-primary snapshot "
                   << "when previous primary snapshot is not in demoted state"
                   << dendl;
        return false;
      }
      return true;
    }
  }

  ldout(cct, 20) << "no previous mirror snapshots found" << dendl;
  return true;
}

template <typename I>
std::string image_state_object_name(I *image_ctx, uint64_t snap_id,
                                    uint64_t index) {
  return IMAGE_STATE_OBJECT_PREFIX + image_ctx->id + "." +
    stringify(snap_id) + "." + stringify(index);
}

} // namespace util
} // namespace snapshot
} // namespace mirror
} // namespace librbd

template bool librbd::mirror::snapshot::util::can_create_primary_snapshot(
  librbd::ImageCtx *image_ctx, bool demoted, bool force,
  bool* requires_orphan, uint64_t *rollback_snap_id);

template bool librbd::mirror::snapshot::util::can_create_non_primary_snapshot(
  librbd::ImageCtx *image_ctx);

template std::string librbd::mirror::snapshot::util::image_state_object_name(
  librbd::ImageCtx *image_ctx, uint64_t snap_id, uint64_t index);
