// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/api/Snapshot.h"
#include "cls/rbd/cls_rbd_types.h"
#include "common/errno.h"
#include "librbd/ImageCtx.h"
#include "librbd/Utils.h"
#include <boost/variant.hpp>

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::api::Snapshot: " << __func__ << ": "

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
    librados::Rados rados(*image_ioctx);
    IoCtx group_ioctx;
    int r = rados.ioctx_create2(snap_namespace.group_pool, group_ioctx);
    if (r < 0) {
      lderr(cct) << "failed to open group pool: " << cpp_strerror(r) << dendl;
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

} // anonymous namespace

template <typename I>
int Snapshot<I>::get_group_namespace(I *ictx, uint64_t snap_id,
                                     snap_group_namespace_t *group_snap) {
  const SnapInfo *snap_info;
  {
    RWLock::RLocker snap_locker(ictx->snap_lock);
    snap_info = ictx->get_snap_info(snap_id);
    if (snap_info) {
      GetGroupVisitor ggv = GetGroupVisitor(ictx->cct, &ictx->data_ctx,
                                            group_snap);
      int r = boost::apply_visitor(ggv, snap_info->snap_namespace);
      if (r < 0) {
	return r;
      }
    }
  }
  return 0;
}

template <typename I>
int Snapshot<I>::get_namespace_type(I *ictx, uint64_t snap_id,
                                snap_namespace_type_t *namespace_type) {
  const SnapInfo *snap_info;
  {
    RWLock::RLocker l(ictx->snap_lock);
    snap_info = ictx->get_snap_info(snap_id);
    if (snap_info) {
      *namespace_type = static_cast<snap_namespace_type_t>(
        cls::rbd::get_snap_namespace_type(snap_info->snap_namespace));
    }
  }
  return 0;
}

} // namespace api
} // namespace librbd

template class librbd::api::Snapshot<librbd::ImageCtx>;
