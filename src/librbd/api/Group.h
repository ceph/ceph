// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_API_GROUP_H
#define CEPH_LIBRBD_API_GROUP_H

#include "include/rbd/librbd.hpp"
#include "include/rados/librados_fwd.hpp"
#include <string>
#include <vector>

namespace librbd {

struct ImageCtx;

namespace api {

template <typename ImageCtxT = librbd::ImageCtx>
struct Group {

  static int create(librados::IoCtx& io_ctx, const char *group_name);
  static int remove(librados::IoCtx& io_ctx, const char *group_name);
  static int list(librados::IoCtx& io_ctx, std::vector<std::string> *names);
  static int get_id(librados::IoCtx& io_ctx, const char *group_name,
                    std::string *group_id);
  static int rename(librados::IoCtx& io_ctx, const char *src_group_name,
                    const char *dest_group_name);

  static int image_add(librados::IoCtx& group_ioctx, const char *group_name,
		       librados::IoCtx& image_ioctx, const char *image_name);
  static int image_remove(librados::IoCtx& group_ioctx, const char *group_name,
		          librados::IoCtx& image_ioctx, const char *image_name);
  static int image_remove_by_id(librados::IoCtx& group_ioctx,
                                const char *group_name,
                                librados::IoCtx& image_ioctx,
                                const char *image_id);
  static int image_list(librados::IoCtx& group_ioctx, const char *group_name,
		        std::vector<group_image_info_t> *images);

  static int image_get_group(ImageCtxT *ictx, group_info_t *group_info);

  static int snap_create(librados::IoCtx& group_ioctx,
                         const char *group_name, const char *snap_name,
                         uint32_t flags);
  static int snap_remove(librados::IoCtx& group_ioctx,
                         const char *group_name, const char *snap_name);
  static int snap_rename(librados::IoCtx& group_ioctx, const char *group_name,
                         const char *old_snap_name, const char *new_snap_name);
  static int snap_list(librados::IoCtx& group_ioctx, const char *group_name,
                       std::vector<group_snap_info_t> *snaps);
  static int snap_rollback(librados::IoCtx& group_ioctx,
                           const char *group_name, const char *snap_name,
                           ProgressContext& pctx);

};

} // namespace api
} // namespace librbd

extern template class librbd::api::Group<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_API_GROUP_H
