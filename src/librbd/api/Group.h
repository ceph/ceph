// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_API_GROUP_H
#define CEPH_LIBRBD_API_GROUP_H

#include "cls/rbd/cls_rbd_client.h"
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
  static int list(librados::IoCtx& io_ctx,
                  std::map<std::string, std::string> *name_to_id_map);
  static int rename(librados::IoCtx& io_ctx, const char *src_group_name,
                    const char *dest_group_name);

  static int image_add(librados::IoCtx& group_ioctx, const char *group_name,
                       librados::IoCtx& image_ioctx, const char *image_name,
                       uint32_t flags);
  static int image_remove(librados::IoCtx& group_ioctx, const char *group_name,
                          librados::IoCtx& image_ioctx, const char *image_name,
                          uint32_t flags);
  static int image_remove_by_id(librados::IoCtx& group_ioctx,
                                const char *group_name,
                                librados::IoCtx& image_ioctx,
                                const char *image_id,
                                uint32_t flags);
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
                       bool try_to_sort, bool fail_if_not_sorted,
                       std::vector<group_snap_info2_t> *snaps);
  static int snap_get_info(librados::IoCtx& group_ioctx,
                           const char *group_name, const char *snap_name,
                           group_snap_info2_t* group_snap);
  static int snap_rollback(librados::IoCtx& group_ioctx,
                           const char *group_name, const char *snap_name,
                           ProgressContext& pctx);

  static int group_image_list_by_id(librados::IoCtx& group_ioctx,
                                    const std::string &group_id,
                                    std::vector<cls::rbd::GroupImageStatus> *images);
  static int group_image_remove(librados::IoCtx& group_ioctx,
                                std::string group_id,
                                librados::IoCtx& image_ioctx,
                                std::string image_id,
                                bool resync,
                                uint32_t flags);

};

} // namespace api
} // namespace librbd

extern template class librbd::api::Group<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_API_GROUP_H
