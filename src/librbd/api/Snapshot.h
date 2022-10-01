// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_API_SNAPSHOT_H
#define CEPH_LIBRBD_API_SNAPSHOT_H

#include "include/rbd/librbd.hpp"
#include "cls/rbd/cls_rbd_types.h"
#include <string>

namespace librbd {

struct ImageCtx;

namespace api {

template <typename ImageCtxT = librbd::ImageCtx>
struct Snapshot {

  static int get_group_namespace(ImageCtxT *ictx, uint64_t snap_id,
                                 snap_group_namespace_t *group_snap);

  static int get_trash_namespace(ImageCtxT *ictx, uint64_t snap_id,
                                 std::string *original_name);

  static int get_mirror_namespace(
      ImageCtxT *ictx, uint64_t snap_id,
      snap_mirror_namespace_t *mirror_snap);

  static int get_namespace_type(ImageCtxT *ictx, uint64_t snap_id,
			        snap_namespace_type_t *namespace_type);

  static int remove(ImageCtxT *ictx, uint64_t snap_id);

  static int get_name(ImageCtxT *ictx, uint64_t snap_id, std::string *snap_name);

  static int get_id(ImageCtxT *ictx, const std::string& snap_name, uint64_t *snap_id);

  static int list(ImageCtxT *ictx, std::vector<snap_info_t>& snaps);

  static int exists(ImageCtxT *ictx, const cls::rbd::SnapshotNamespace& snap_namespace,
		    const char *snap_name, bool *exists);

  static int create(ImageCtxT *ictx, const char *snap_name, uint32_t flags,
                    ProgressContext& pctx);

  static int remove(ImageCtxT *ictx, const char *snap_name, uint32_t flags, ProgressContext& pctx);

  static int get_limit(ImageCtxT *ictx, uint64_t *limit);

  static int set_limit(ImageCtxT *ictx, uint64_t limit);

  static int get_timestamp(ImageCtxT *ictx, uint64_t snap_id, struct timespec *timestamp);

  static int is_protected(ImageCtxT *ictx, const char *snap_name, bool *protect);

  static int get_namespace(ImageCtxT *ictx, const char *snap_name,
                           cls::rbd::SnapshotNamespace *snap_namespace);

};

} // namespace api
} // namespace librbd

extern template class librbd::api::Snapshot<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_API_SNAPSHOT_H
