// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/api/Snapshot.h"
#include "cls/rbd/cls_rbd_types.h"
#include "common/errno.h"
#include "librbd/internal.h"
#include "librbd/ImageCtx.h"
#include "librbd/ImageState.h"
#include "librbd/Operations.h"
#include "librbd/Utils.h"
#include "librbd/api/Image.h"
#include "include/Context.h"
#include "common/Cond.h"

#include <bitset>
#include <boost/variant.hpp>

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::api::Snapshot: " << __func__ << ": "

using librados::snap_t;

namespace librbd {
namespace api {

namespace {

class GetGroupVisitor : public boost::static_visitor<int> {
public:
  CephContext* cct;
  librados::IoCtx *image_ioctx;
  snap_group_namespace_t *group_snap;

  explicit GetGroupVisitor(CephContext* cct, librados::IoCtx *_image_ioctx,
                           snap_group_namespace_t *group_snap)
    : cct(cct), image_ioctx(_image_ioctx), group_snap(group_snap) {};

  template <typename T>
  inline int operator()(const T&) const {
    // ignore other than GroupSnapshotNamespace types.
    return -EINVAL;
  }

  inline int operator()(
      const cls::rbd::GroupSnapshotNamespace& snap_namespace) {
    IoCtx group_ioctx;
    int r = util::create_ioctx(*image_ioctx, "group", snap_namespace.group_pool,
                               {}, &group_ioctx);
    if (r < 0) {
      return r;
    }

    cls::rbd::GroupSnapshot group_snapshot;

    std::string group_name;
    r = cls_client::dir_get_name(&group_ioctx, RBD_GROUP_DIRECTORY,
				 snap_namespace.group_id, &group_name);
    if (r < 0) {
      lderr(cct) << "failed to retrieve group name: " << cpp_strerror(r)
                 << dendl;
      return r;
    }

    string group_header_oid = util::group_header_name(snap_namespace.group_id);
    r = cls_client::group_snap_get_by_id(&group_ioctx,
					 group_header_oid,
					 snap_namespace.group_snapshot_id,
					 &group_snapshot);
    if (r < 0) {
      lderr(cct) << "failed to retrieve group snapshot: " << cpp_strerror(r)
                 << dendl;
      return r;
    }

    group_snap->group_pool = group_ioctx.get_id();
    group_snap->group_name = group_name;
    group_snap->group_snap_name = group_snapshot.name;
    return 0;
  }
};

class GetTrashVisitor : public boost::static_visitor<int> {
public:
  std::string* original_name;

  explicit GetTrashVisitor(std::string* original_name)
    : original_name(original_name) {
  }

  template <typename T>
  inline int operator()(const T&) const {
    return -EINVAL;
  }

  inline int operator()(
      const cls::rbd::TrashSnapshotNamespace& snap_namespace) {
    *original_name = snap_namespace.original_name;
    return 0;
  }
};

class GetMirrorVisitor : public boost::static_visitor<int> {
public:
  snap_mirror_namespace_t *mirror_snap;

  explicit GetMirrorVisitor(snap_mirror_namespace_t *mirror_snap)
    : mirror_snap(mirror_snap) {
  }

  template <typename T>
  inline int operator()(const T&) const {
    return -EINVAL;
  }

  inline int operator()(
      const cls::rbd::MirrorSnapshotNamespace& snap_namespace) {
    mirror_snap->state = static_cast<snap_mirror_state_t>(snap_namespace.state);
    mirror_snap->complete = snap_namespace.complete;
    mirror_snap->mirror_peer_uuids = snap_namespace.mirror_peer_uuids;
    mirror_snap->primary_mirror_uuid = snap_namespace.primary_mirror_uuid;
    mirror_snap->primary_snap_id = snap_namespace.primary_snap_id;
    mirror_snap->last_copied_object_number =
      snap_namespace.last_copied_object_number;
    return 0;
  }
};

} // anonymous namespace

template <typename I>
int Snapshot<I>::get_group_namespace(I *ictx, uint64_t snap_id,
                                     snap_group_namespace_t *group_snap) {
  int r = ictx->state->refresh_if_required();
  if (r < 0) {
    return r;
  }

  std::shared_lock image_locker{ictx->image_lock};
  auto snap_info = ictx->get_snap_info(snap_id);
  if (snap_info == nullptr) {
    return -ENOENT;
  }

  GetGroupVisitor ggv = GetGroupVisitor(ictx->cct, &ictx->md_ctx, group_snap);
  r = boost::apply_visitor(ggv, snap_info->snap_namespace);
  if (r < 0) {
    return r;
  }

  return 0;
}

template <typename I>
int Snapshot<I>::get_trash_namespace(I *ictx, uint64_t snap_id,
                                     std::string* original_name) {
  int r = ictx->state->refresh_if_required();
  if (r < 0) {
    return r;
  }

  std::shared_lock image_locker{ictx->image_lock};
  auto snap_info = ictx->get_snap_info(snap_id);
  if (snap_info == nullptr) {
    return -ENOENT;
  }

  auto visitor = GetTrashVisitor(original_name);
  r = boost::apply_visitor(visitor, snap_info->snap_namespace);
  if (r < 0) {
    return r;
  }

  return 0;
}

template <typename I>
int Snapshot<I>::get_mirror_namespace(
    I *ictx, uint64_t snap_id, snap_mirror_namespace_t *mirror_snap) {
  int r = ictx->state->refresh_if_required();
  if (r < 0) {
    return r;
  }

  std::shared_lock image_locker{ictx->image_lock};
  auto snap_info = ictx->get_snap_info(snap_id);
  if (snap_info == nullptr) {
    return -ENOENT;
  }

  auto gmv = GetMirrorVisitor(mirror_snap);
  r = boost::apply_visitor(gmv, snap_info->snap_namespace);
  if (r < 0) {
    return r;
  }

  return 0;
}

template <typename I>
int Snapshot<I>::get_namespace_type(I *ictx, uint64_t snap_id,
                                snap_namespace_type_t *namespace_type) {
  int r = ictx->state->refresh_if_required();
  if (r < 0) {
    return r;
  }

  std::shared_lock l{ictx->image_lock};
  auto snap_info = ictx->get_snap_info(snap_id);
  if (snap_info == nullptr) {
    return -ENOENT;
  }

  *namespace_type = static_cast<snap_namespace_type_t>(
    cls::rbd::get_snap_namespace_type(snap_info->snap_namespace));
  return 0;
}

template <typename I>
int Snapshot<I>::remove(I *ictx, uint64_t snap_id) {
  ldout(ictx->cct, 20) << "snap_remove " << ictx << " " << snap_id << dendl;

  int r = ictx->state->refresh_if_required();
  if (r < 0) {
    return r;
  }

  cls::rbd::SnapshotNamespace snapshot_namespace;
  std::string snapshot_name;
  {
    std::shared_lock image_locker{ictx->image_lock};
    auto it = ictx->snap_info.find(snap_id);
    if (it == ictx->snap_info.end()) {
      return -ENOENT;
    }

    snapshot_namespace = it->second.snap_namespace;
    snapshot_name = it->second.name;
  }

  C_SaferCond ctx;
  ictx->operations->snap_remove(snapshot_namespace, snapshot_name, &ctx);
  r = ctx.wait();
  return r;
}

template <typename I>
int Snapshot<I>::get_name(I *ictx, uint64_t snap_id, std::string *snap_name)
  {
    ldout(ictx->cct, 20) << "snap_get_name " << ictx << " " << snap_id << dendl;

    int r = ictx->state->refresh_if_required();
    if (r < 0)
      return r;

    std::shared_lock image_locker{ictx->image_lock};
    r = ictx->get_snap_name(snap_id, snap_name);

    return r;
  }

template <typename I>
int Snapshot<I>::get_id(I *ictx, const std::string& snap_name, uint64_t *snap_id)
  {
    ldout(ictx->cct, 20) << "snap_get_id " << ictx << " " << snap_name << dendl;

    int r = ictx->state->refresh_if_required();
    if (r < 0)
      return r;

    std::shared_lock image_locker{ictx->image_lock};
    *snap_id = ictx->get_snap_id(cls::rbd::UserSnapshotNamespace(), snap_name);
    if (*snap_id == CEPH_NOSNAP)
      return -ENOENT;

    return 0;
  }

template <typename I>
int Snapshot<I>::list(I *ictx, vector<snap_info_t>& snaps) {
  ldout(ictx->cct, 20) << "snap_list " << ictx << dendl;

  int r = ictx->state->refresh_if_required();
  if (r < 0)
    return r;

  std::shared_lock l{ictx->image_lock};
  for (auto &it : ictx->snap_info) {
    snap_info_t info;
    info.name = it.second.name;
    info.id = it.first;
    info.size = it.second.size;
    snaps.push_back(info);
  }

  return 0;
}

template <typename I>
int Snapshot<I>::exists(I *ictx, const cls::rbd::SnapshotNamespace& snap_namespace,
		        const char *snap_name, bool *exists) {
  ldout(ictx->cct, 20) << "snap_exists " << ictx << " " << snap_name << dendl;

  int r = ictx->state->refresh_if_required();
  if (r < 0)
    return r;

  std::shared_lock l{ictx->image_lock};
  *exists = ictx->get_snap_id(snap_namespace, snap_name) != CEPH_NOSNAP;
  return 0;
}

template <typename I>
int Snapshot<I>::create(I *ictx, const char *snap_name, uint32_t flags,
                        ProgressContext& pctx) {
  ldout(ictx->cct, 20) << "snap_create " << ictx << " " << snap_name
                       << " flags: " << flags << dendl;

  uint64_t internal_flags = 0;

  if (flags & RBD_SNAP_CREATE_SKIP_QUIESCE) {
    internal_flags |= SNAP_CREATE_FLAG_SKIP_NOTIFY_QUIESCE;
    flags &= ~RBD_SNAP_CREATE_SKIP_QUIESCE;
  } else if (flags & RBD_SNAP_CREATE_IGNORE_QUIESCE_ERROR) {
    internal_flags |= SNAP_CREATE_FLAG_IGNORE_NOTIFY_QUIESCE_ERROR;
    flags &= ~RBD_SNAP_CREATE_IGNORE_QUIESCE_ERROR;
  }

  if (flags != 0) {
    lderr(ictx->cct) << "invalid snap create flags: " << std::bitset<32>(flags)
                     << dendl;
    return -EINVAL;
  }

  return ictx->operations->snap_create(cls::rbd::UserSnapshotNamespace(),
                                       snap_name, internal_flags, pctx);
}

template <typename I>
int Snapshot<I>::remove(I *ictx, const char *snap_name, uint32_t flags,
                        ProgressContext& pctx) {
  ldout(ictx->cct, 20) << "snap_remove " << ictx << " " << snap_name << " flags: " << flags << dendl;

  int r = 0;

  r = ictx->state->refresh_if_required();
  if (r < 0)
    return r;

  if (flags & RBD_SNAP_REMOVE_FLATTEN) {
     r = Image<I>::flatten_children(ictx, snap_name, pctx);
     if (r < 0) {
	return r;
     }
  }

  bool protect;
  r = is_protected(ictx, snap_name, &protect);
  if (r < 0) {
    return r;
  }

  if (protect && flags & RBD_SNAP_REMOVE_UNPROTECT) {
    r = ictx->operations->snap_unprotect(cls::rbd::UserSnapshotNamespace(), snap_name);
    if (r < 0) {
      lderr(ictx->cct) << "failed to unprotect snapshot: " << snap_name << dendl;
      return r;
    }

    r = is_protected(ictx, snap_name, &protect);
    if (r < 0) {
      return r;
    }
    if (protect) {
      lderr(ictx->cct) << "snapshot is still protected after unprotection" << dendl;
      ceph_abort();
    }
  }

  C_SaferCond ctx;
  ictx->operations->snap_remove(cls::rbd::UserSnapshotNamespace(), snap_name, &ctx);

  r = ctx.wait();
  return r;
}

template <typename I>
int Snapshot<I>::get_timestamp(I *ictx, uint64_t snap_id, struct timespec *timestamp) {
  auto snap_it = ictx->snap_info.find(snap_id);
  ceph_assert(snap_it != ictx->snap_info.end());
  utime_t time = snap_it->second.timestamp;
  time.to_timespec(timestamp);
  return 0;
}

template <typename I>
int Snapshot<I>::get_limit(I *ictx, uint64_t *limit) {
  int r = cls_client::snapshot_get_limit(&ictx->md_ctx, ictx->header_oid,
                                         limit);
  if (r == -EOPNOTSUPP) {
    *limit = UINT64_MAX;
    r = 0;
  }
  return r;
}

template <typename I>
int Snapshot<I>::set_limit(I *ictx, uint64_t limit) {
  return ictx->operations->snap_set_limit(limit);
}

template <typename I>
int Snapshot<I>::is_protected(I *ictx, const char *snap_name, bool *protect) {
  ldout(ictx->cct, 20) << "snap_is_protected " << ictx << " " << snap_name
		       << dendl;

  int r = ictx->state->refresh_if_required();
  if (r < 0)
    return r;

  std::shared_lock l{ictx->image_lock};
  snap_t snap_id = ictx->get_snap_id(cls::rbd::UserSnapshotNamespace(), snap_name);
  if (snap_id == CEPH_NOSNAP)
    return -ENOENT;
  bool is_unprotected;
  r = ictx->is_snap_unprotected(snap_id, &is_unprotected);
  // consider both PROTECTED or UNPROTECTING to be 'protected',
  // since in either state they can't be deleted
  *protect = !is_unprotected;
  return r;
}

template <typename I>
int Snapshot<I>::get_namespace(I *ictx, const char *snap_name,
                               cls::rbd::SnapshotNamespace *snap_namespace) {
  ldout(ictx->cct, 20) << "get_snap_namespace " << ictx << " " << snap_name
                       << dendl;

  int r = ictx->state->refresh_if_required();
  if (r < 0)
    return r;
  std::shared_lock l{ictx->image_lock};
  snap_t snap_id = ictx->get_snap_id(*snap_namespace, snap_name);
  if (snap_id == CEPH_NOSNAP)
    return -ENOENT;
  r = ictx->get_snap_namespace(snap_id, snap_namespace);
  return r;
}

} // namespace api
} // namespace librbd

template class librbd::api::Snapshot<librbd::ImageCtx>;
