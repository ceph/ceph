// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_API_SNAPSHOT_H
#define CEPH_LIBRBD_API_SNAPSHOT_H

#include "include/rbd/librbd.hpp"

namespace librbd {

struct ImageCtx;

namespace api {

template <typename ImageCtxT = librbd::ImageCtx>
struct Snapshot {

  static int get_group(ImageCtxT *ictx, uint64_t snap_id,
		       group_snap_t *group_snap);

  static int get_namespace_type(ImageCtxT *ictx, uint64_t snap_id,
			        snap_namespace_type_t *namespace_type);

};

} // namespace api
} // namespace librbd

extern template class librbd::api::Snapshot<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_API_SNAPSHOT_H
