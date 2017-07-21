// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/api/Image.h"
#include "include/rados/librados.hpp"
#include "common/dout.h"
#include "common/errno.h"
#include "cls/rbd/cls_rbd_client.h"
#include "librbd/ImageCtx.h"
#include "librbd/ImageState.h"

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::api::Image: " << __func__ << ": "

namespace librbd {
namespace api {

template <typename I>
int Image<I>::list_images(librados::IoCtx& io_ctx, ImageNameToIds *images) {
  CephContext *cct = (CephContext *)io_ctx.cct();
  ldout(cct, 20) << "io_ctx=" << &io_ctx << dendl;

  // new format images are accessed by class methods
  int r;
  int max_read = 1024;
  string last_read = "";
  do {
    map<string, string> images_page;
    r = cls_client::dir_list(&io_ctx, RBD_DIRECTORY,
      		   last_read, max_read, &images_page);
    if (r < 0 && r != -ENOENT) {
      lderr(cct) << "error listing image in directory: "
                 << cpp_strerror(r) << dendl;
      return r;
    } else if (r == -ENOENT) {
      break;
    }
    for (map<string, string>::const_iterator it = images_page.begin();
         it != images_page.end(); ++it) {
      images->insert(*it);
    }
    if (!images_page.empty()) {
      last_read = images_page.rbegin()->first;
    }
    r = images_page.size();
  } while (r == max_read);

  return 0;
}

template <typename I>
int Image<I>::list_children(I *ictx, const ParentSpec &parent_spec,
                            PoolImageIds *pool_image_ids)
{
  CephContext *cct = ictx->cct;
  int r = ictx->state->refresh_if_required();
  if (r < 0) {
    return r;
  }

  // no children for non-layered or old format image
  if (!ictx->test_features(RBD_FEATURE_LAYERING, ictx->snap_lock)) {
    return 0;
  }

  pool_image_ids->clear();
  // search all pools for children depending on this snapshot
  librados::Rados rados(ictx->md_ctx);
  std::list<std::pair<int64_t, std::string> > pools;
  r = rados.pool_list2(pools);
  if (r < 0) {
    lderr(cct) << "error listing pools: " << cpp_strerror(r) << dendl;
    return r;
  }

  for (auto it = pools.begin(); it != pools.end(); ++it) {
    int64_t base_tier;
    r = rados.pool_get_base_tier(it->first, &base_tier);
    if (r == -ENOENT) {
      ldout(cct, 1) << "pool " << it->second << " no longer exists" << dendl;
      continue;
    } else if (r < 0) {
      lderr(cct) << "error retrieving base tier for pool " << it->second
                 << dendl;
      return r;
    }
    if (it->first != base_tier) {
      // pool is a cache; skip it
      continue;
    }

    IoCtx ioctx;
    r = rados.ioctx_create2(it->first, ioctx);
    if (r == -ENOENT) {
      ldout(cct, 1) << "pool " << it->second << " no longer exists" << dendl;
      continue;
    } else if (r < 0) {
      lderr(cct) << "error accessing child image pool " << it->second
                 << dendl;
      return r;
    }

    set<string> image_ids;
    r = cls_client::get_children(&ioctx, RBD_CHILDREN, parent_spec,
                                 image_ids);
    if (r < 0 && r != -ENOENT) {
      lderr(cct) << "error reading list of children from pool " << it->second
      	   << dendl;
      return r;
    }
    pool_image_ids->insert({*it, image_ids});
  }

  return 0;
}

} // namespace api
} // namespace librbd

template class librbd::api::Image<librbd::ImageCtx>;
