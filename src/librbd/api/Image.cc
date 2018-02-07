// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/api/Image.h"
#include "include/rados/librados.hpp"
#include "common/dout.h"
#include "common/errno.h"
#include "cls/rbd/cls_rbd_client.h"
#include "librbd/DeepCopyRequest.h"
#include "librbd/ExclusiveLock.h"
#include "librbd/ImageCtx.h"
#include "librbd/ImageState.h"
#include "librbd/Utils.h"
#include "librbd/image/CloneRequest.h"
#include "librbd/internal.h"
#include "librbd/Utils.h"

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::api::Image: " << __func__ << ": "

namespace librbd {
namespace api {

template <typename I>
int Image<I>::get_op_features(I *ictx, uint64_t *op_features) {
  CephContext *cct = ictx->cct;
  ldout(cct, 20) << "image_ctx=" << ictx << dendl;

  int r = ictx->state->refresh_if_required();
  if (r < 0) {
    return r;
  }

  RWLock::RLocker snap_locker(ictx->snap_lock);
  *op_features = ictx->op_features;
  return 0;
}

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

  // no children for non-layered or old format image
  if (!ictx->test_features(RBD_FEATURE_LAYERING, ictx->snap_lock)) {
    return 0;
  }

  pool_image_ids->clear();

  librados::Rados rados(ictx->md_ctx);

  // search all pools for clone v1 children depending on this snapshot
  std::list<std::pair<int64_t, std::string> > pools;
  int r = rados.pool_list2(pools);
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

  // retrieve clone v2 children attached to this snapshot
  IoCtx parent_io_ctx;
  r = rados.ioctx_create2(parent_spec.pool_id, parent_io_ctx);
  assert(r == 0);

  cls::rbd::ChildImageSpecs child_images;
  r = cls_client::children_list(&parent_io_ctx,
                                util::header_name(parent_spec.image_id),
                                parent_spec.snap_id, &child_images);
  if (r < 0 && r != -ENOENT && r != -EOPNOTSUPP) {
    lderr(cct) << "error retrieving children: " << cpp_strerror(r) << dendl;
    return r;
  }

  for (auto& child_image : child_images) {
    IoCtx io_ctx;
    r = rados.ioctx_create2(child_image.pool_id, io_ctx);
    if (r == -ENOENT) {
      ldout(cct, 1) << "pool " << child_image.pool_id << " no longer exists"
                    << dendl;
      continue;
    }

    PoolSpec pool_spec = {child_image.pool_id, io_ctx.get_pool_name()};
    (*pool_image_ids)[pool_spec].insert(child_image.image_id);
  }

  return 0;
}

template <typename I>
int Image<I>::deep_copy(I *src, librados::IoCtx& dest_md_ctx,
                        const char *destname, ImageOptions& opts,
                        ProgressContext &prog_ctx) {
  CephContext *cct = (CephContext *)dest_md_ctx.cct();
  ldout(cct, 20) << src->name
                 << (src->snap_name.length() ? "@" + src->snap_name : "")
                 << " -> " << destname << " opts = " << opts << dendl;

  uint64_t features;
  uint64_t src_size;
  {
    RWLock::RLocker snap_locker(src->snap_lock);
    features = src->features;
    src_size = src->get_image_size(src->snap_id);
  }
  uint64_t format = 2;
  if (opts.get(RBD_IMAGE_OPTION_FORMAT, &format) != 0) {
    opts.set(RBD_IMAGE_OPTION_FORMAT, format);
  }
  if (format == 1) {
    lderr(cct) << "old format not supported for destination image" << dendl;
    return -EINVAL;
  }
  uint64_t stripe_unit = src->stripe_unit;
  if (opts.get(RBD_IMAGE_OPTION_STRIPE_UNIT, &stripe_unit) != 0) {
    opts.set(RBD_IMAGE_OPTION_STRIPE_UNIT, stripe_unit);
  }
  uint64_t stripe_count = src->stripe_count;
  if (opts.get(RBD_IMAGE_OPTION_STRIPE_COUNT, &stripe_count) != 0) {
    opts.set(RBD_IMAGE_OPTION_STRIPE_COUNT, stripe_count);
  }
  uint64_t order = src->order;
  if (opts.get(RBD_IMAGE_OPTION_ORDER, &order) != 0) {
    opts.set(RBD_IMAGE_OPTION_ORDER, order);
  }
  if (opts.get(RBD_IMAGE_OPTION_FEATURES, &features) != 0) {
    opts.set(RBD_IMAGE_OPTION_FEATURES, features);
  }
  if (features & ~RBD_FEATURES_ALL) {
    lderr(cct) << "librbd does not support requested features" << dendl;
    return -ENOSYS;
  }

  ParentSpec parent_spec;
  {
    RWLock::RLocker snap_locker(src->snap_lock);
    RWLock::RLocker parent_locker(src->parent_lock);

    // use oldest snapshot or HEAD for parent spec
    if (!src->snap_info.empty()) {
      parent_spec = src->snap_info.begin()->second.parent.spec;
    } else {
      parent_spec = src->parent_md.spec;
    }
  }

  int r;
  if (parent_spec.pool_id == -1) {
    r = create(dest_md_ctx, destname, "", src_size, opts, "", "", false);
  } else {
    librados::Rados rados(src->md_ctx);
    librados::IoCtx parent_io_ctx;
    r = rados.ioctx_create2(parent_spec.pool_id, parent_io_ctx);
    if (r < 0) {
      lderr(cct) << "failed to open source parent pool: "
                 << cpp_strerror(r) << dendl;
      return r;
    }
    ImageCtx *src_parent_image_ctx =
      new ImageCtx("", parent_spec.image_id, nullptr, parent_io_ctx, false);
    r = src_parent_image_ctx->state->open(true);
    if (r < 0) {
      if (r != -ENOENT) {
        lderr(cct) << "failed to open source parent image: "
                   << cpp_strerror(r) << dendl;
      }
      return r;
    }
    std::string snap_name;
    {
      RWLock::RLocker parent_snap_locker(src_parent_image_ctx->snap_lock);
      auto it = src_parent_image_ctx->snap_info.find(parent_spec.snap_id);
      if (it == src_parent_image_ctx->snap_info.end()) {
        return -ENOENT;
      }
      snap_name = it->second.name;
    }

    C_SaferCond cond;
    src_parent_image_ctx->state->snap_set(cls::rbd::UserSnapshotNamespace(),
                                          snap_name, &cond);
    r = cond.wait();
    if (r < 0) {
      if (r != -ENOENT) {
        lderr(cct) << "failed to set snapshot: " << cpp_strerror(r) << dendl;
      }
      return r;
    }

    C_SaferCond ctx;
    std::string dest_id = util::generate_image_id(dest_md_ctx);
    auto *req = image::CloneRequest<I>::create(
      src_parent_image_ctx, dest_md_ctx, destname, dest_id, opts,
      "", "", src->op_work_queue, &ctx);
    req->send();
    r = ctx.wait();
    int close_r = src_parent_image_ctx->state->close();
    if (r == 0 && close_r < 0) {
      r = close_r;
    }
  }
  if (r < 0) {
    lderr(cct) << "header creation failed" << dendl;
    return r;
  }
  opts.set(RBD_IMAGE_OPTION_ORDER, static_cast<uint64_t>(order));

  ImageCtx *dest = new librbd::ImageCtx(destname, "", NULL,
                                        dest_md_ctx, false);
  r = dest->state->open(false);
  if (r < 0) {
    lderr(cct) << "failed to read newly created header" << dendl;
    return r;
  }

  C_SaferCond lock_ctx;
  {
    RWLock::WLocker locker(dest->owner_lock);

    if (dest->exclusive_lock == nullptr ||
        dest->exclusive_lock->is_lock_owner()) {
      lock_ctx.complete(0);
    } else {
      dest->exclusive_lock->acquire_lock(&lock_ctx);
    }
  }

  r = lock_ctx.wait();
  if (r < 0) {
    lderr(cct) << "failed to request exclusive lock: " << cpp_strerror(r)
               << dendl;
    dest->state->close();
    return r;
  }

  r = deep_copy(src, dest, prog_ctx);

  int close_r = dest->state->close();
  if (r == 0 && close_r < 0) {
    r = close_r;
  }
  return r;
}

template <typename I>
int Image<I>::deep_copy(I *src, I *dest, ProgressContext &prog_ctx) {
  CephContext *cct = src->cct;
  librados::snap_t snap_id_start = 0;
  librados::snap_t snap_id_end;
  {
    RWLock::RLocker snap_locker(src->snap_lock);
    snap_id_end = src->snap_id;
  }

  ThreadPool *thread_pool;
  ContextWQ *op_work_queue;
  ImageCtx::get_thread_pool_instance(cct, &thread_pool, &op_work_queue);

  C_SaferCond cond;
  SnapSeqs snap_seqs;
  auto req = DeepCopyRequest<>::create(src, dest, snap_id_start, snap_id_end,
                                       boost::none, op_work_queue, &snap_seqs,
                                       &prog_ctx, &cond);
  req->send();
  int r = cond.wait();
  if (r < 0) {
    return r;
  }

  return 0;
}

} // namespace api
} // namespace librbd

template class librbd::api::Image<librbd::ImageCtx>;
