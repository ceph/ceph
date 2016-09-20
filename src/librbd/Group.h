// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#ifndef CEPH_LIBRBD_GROUP_H
#define CEPH_LIBRBD_GROUP_H

namespace librbd {

// Consistency groups functions
int group_create(librados::IoCtx& io_ctx, const char *imgname);
int group_remove(librados::IoCtx& io_ctx, const char *group_name);
int group_list(librados::IoCtx& io_ctx, std::vector<std::string>& names);
int group_image_add(librados::IoCtx& group_ioctx, const char *group_name,
		    librados::IoCtx& image_ioctx, const char *image_name);
int group_image_remove(librados::IoCtx& group_ioctx, const char *group_name,
		       librados::IoCtx& image_ioctx, const char *image_name);
int group_image_list(librados::IoCtx& group_ioctx, const char *group_name,
		     std::vector<group_image_status_t>& images);
int image_get_group(ImageCtx *ictx, group_spec_t *group_spec);
}
#endif // CEPH_LIBRBD_GROUP_H
