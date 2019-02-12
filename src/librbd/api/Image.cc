// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/api/Image.h"
#include "include/rados/librados.hpp"
#include "common/dout.h"
#include "common/errno.h"
#include "common/Cond.h"
#include "cls/rbd/cls_rbd_client.h"
#include "librbd/DeepCopyRequest.h"
#include "librbd/ExclusiveLock.h"
#include "librbd/ImageCtx.h"
#include "librbd/ImageState.h"
#include "librbd/Utils.h"
#include "librbd/image/CloneRequest.h"
#include "librbd/image/RemoveRequest.h"
#include "librbd/internal.h"
#include "librbd/Utils.h"
#include "librbd/api/Config.h"
#include "librbd/api/Trash.h"
#include <boost/scope_exit.hpp>

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::api::Image: " << __func__ << ": "

namespace librbd {
namespace api {

namespace {

bool compare_by_pool(const librbd::linked_image_spec_t& lhs,
                     const librbd::linked_image_spec_t& rhs)
{
  if (lhs.pool_id != rhs.pool_id) {
    return lhs.pool_id < rhs.pool_id;
  } else if (lhs.pool_namespace != rhs.pool_namespace) {
    return lhs.pool_namespace < rhs.pool_namespace;
  }
  return false;
}

bool compare(const librbd::linked_image_spec_t& lhs,
             const librbd::linked_image_spec_t& rhs)
{
  if (lhs.pool_name != rhs.pool_name) {
    return lhs.pool_name < rhs.pool_name;
  } else if (lhs.pool_id != rhs.pool_id) {
    return lhs.pool_id < rhs.pool_id;
  } else if (lhs.pool_namespace != rhs.pool_namespace) {
    return lhs.pool_namespace < rhs.pool_namespace;
  } else if (lhs.image_name != rhs.image_name) {
    return lhs.image_name < rhs.image_name;
  } else if (lhs.image_id != rhs.image_id) {
    return lhs.image_id < rhs.image_id;
  }
  return false;
}


} // anonymous namespace

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
int Image<I>::list_images(librados::IoCtx& io_ctx,
                          std::vector<image_spec_t> *images) {
  CephContext *cct = (CephContext *)io_ctx.cct();
  ldout(cct, 20) << "list " << &io_ctx << dendl;

  int r;
  images->clear();

  if (io_ctx.get_namespace().empty()) {
    bufferlist bl;
    r = io_ctx.read(RBD_DIRECTORY, bl, 0, 0);
    if (r == -ENOENT) {
      return 0;
    } else if (r < 0) {
      lderr(cct) << "error listing v1 images: " << cpp_strerror(r) << dendl;
      return r;
    }

    // old format images are in a tmap
    if (bl.length()) {
      auto p = bl.cbegin();
      bufferlist header;
      std::map<std::string, bufferlist> m;
      decode(header, p);
      decode(m, p);
      for (auto& it : m) {
        images->push_back({.id ="", .name = it.first});
      }
    }
  }

  std::map<std::string, std::string> image_names_to_ids;
  r = list_images_v2(io_ctx, &image_names_to_ids);
  if (r < 0) {
    lderr(cct) << "error listing v2 images: " << cpp_strerror(r) << dendl;
    return r;
  }

  for (const auto& img_pair : image_names_to_ids) {
    images->push_back({.id = img_pair.second,
                       .name = img_pair.first});
  }

  return 0;
}

template <typename I>
int Image<I>::list_images_v2(librados::IoCtx& io_ctx, ImageNameToIds *images) {
  CephContext *cct = (CephContext *)io_ctx.cct();
  ldout(cct, 20) << "io_ctx=" << &io_ctx << dendl;

  // new format images are accessed by class methods
  int r;
  int max_read = 1024;
  string last_read = "";
  do {
    map<string, string> images_page;
    r = cls_client::dir_list(&io_ctx, RBD_DIRECTORY, last_read, max_read,
                             &images_page);
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
int Image<I>::get_parent(I *ictx,
                         librbd::linked_image_spec_t *parent_image,
                         librbd::snap_spec_t *parent_snap) {
  auto cct = ictx->cct;
  ldout(cct, 20) << "image_ctx=" << ictx << dendl;

  int r = ictx->state->refresh_if_required();
  if (r < 0) {
    return r;
  }

  RWLock::RLocker snap_locker(ictx->snap_lock);
  RWLock::RLocker parent_locker(ictx->parent_lock);

  bool release_parent_locks = false;
  BOOST_SCOPE_EXIT_ALL(ictx, &release_parent_locks) {
    if (release_parent_locks) {
      ictx->parent->parent_lock.put_read();
      ictx->parent->snap_lock.put_read();
    }
  };

  // if a migration is in-progress, the true parent is the parent
  // of the migration source image
  auto parent = ictx->parent;
  if (!ictx->migration_info.empty() && ictx->parent != nullptr) {
    release_parent_locks = true;
    ictx->parent->snap_lock.get_read();
    ictx->parent->parent_lock.get_read();

    parent = ictx->parent->parent;
  }

  if (parent == nullptr) {
    return -ENOENT;
  }

  parent_image->pool_id = parent->md_ctx.get_id();
  parent_image->pool_name = parent->md_ctx.get_pool_name();
  parent_image->pool_namespace = parent->md_ctx.get_namespace();

  RWLock::RLocker parent_snap_locker(parent->snap_lock);
  parent_snap->id = parent->snap_id;
  parent_snap->namespace_type = RBD_SNAP_NAMESPACE_TYPE_USER;
  if (parent->snap_id != CEPH_NOSNAP) {
    auto snap_info = parent->get_snap_info(parent->snap_id);
    if (snap_info == nullptr) {
      lderr(cct) << "error finding parent snap name: " << cpp_strerror(r)
                 << dendl;
      return -ENOENT;
    }

    parent_snap->namespace_type = static_cast<snap_namespace_type_t>(
      cls::rbd::get_snap_namespace_type(snap_info->snap_namespace));
    parent_snap->name = snap_info->name;
  }

  parent_image->image_id = parent->id;
  parent_image->image_name = parent->name;
  parent_image->trash = true;

  librbd::trash_image_info_t trash_info;
  r = Trash<I>::get(parent->md_ctx, parent->id, &trash_info);
  if (r == -ENOENT || r == -EOPNOTSUPP) {
    parent_image->trash = false;
  } else if (r < 0) {
    lderr(cct) << "error looking up trash status: " << cpp_strerror(r)
               << dendl;
    return r;
  }

  return 0;
}

template <typename I>
int Image<I>::list_children(I *ictx,
                            std::vector<librbd::linked_image_spec_t> *images) {
  RWLock::RLocker l(ictx->snap_lock);
  cls::rbd::ParentImageSpec parent_spec{ictx->md_ctx.get_id(),
                                        ictx->md_ctx.get_namespace(),
                                        ictx->id, ictx->snap_id};
  return list_children(ictx, parent_spec, images);
}

template <typename I>
int Image<I>::list_children(I *ictx,
                            const cls::rbd::ParentImageSpec &parent_spec,
                            std::vector<librbd::linked_image_spec_t> *images) {
  CephContext *cct = ictx->cct;
  ldout(cct, 20) << "ictx=" << ictx << dendl;

  // no children for non-layered or old format image
  if (!ictx->test_features(RBD_FEATURE_LAYERING, ictx->snap_lock)) {
    return 0;
  }

  images->clear();

  librados::Rados rados(ictx->md_ctx);

  // search all pools for clone v1 children dependent on this snapshot
  std::list<std::pair<int64_t, std::string> > pools;
  int r = rados.pool_list2(pools);
  if (r < 0) {
    lderr(cct) << "error listing pools: " << cpp_strerror(r) << dendl;
    return r;
  }

  for (auto& it : pools) {
    int64_t base_tier;
    r = rados.pool_get_base_tier(it.first, &base_tier);
    if (r == -ENOENT) {
      ldout(cct, 1) << "pool " << it.second << " no longer exists" << dendl;
      continue;
    } else if (r < 0) {
      lderr(cct) << "error retrieving base tier for pool " << it.second
                 << dendl;
      return r;
    }
    if (it.first != base_tier) {
      // pool is a cache; skip it
      continue;
    }

    IoCtx ioctx;
    r = util::create_ioctx(ictx->md_ctx, "child image", it.first, {}, &ioctx);
    if (r == -ENOENT) {
      continue;
    } else if (r < 0) {
      return r;
    }

    std::set<std::string> image_ids;
    r = cls_client::get_children(&ioctx, RBD_CHILDREN, parent_spec,
                                 image_ids);
    if (r < 0 && r != -ENOENT) {
      lderr(cct) << "error reading list of children from pool " << it.second
                 << dendl;
      return r;
    }

    for (auto& image_id : image_ids) {
      images->push_back({
        it.first, "", ictx->md_ctx.get_namespace(), image_id, "", false});
    }
  }

  // retrieve clone v2 children attached to this snapshot
  IoCtx parent_io_ctx;
  r = util::create_ioctx(ictx->md_ctx, "parent image", parent_spec.pool_id,
                         parent_spec.pool_namespace, &parent_io_ctx);
  if (r < 0) {
    return r;
  }

  cls::rbd::ChildImageSpecs child_images;
  r = cls_client::children_list(&parent_io_ctx,
                                util::header_name(parent_spec.image_id),
                                parent_spec.snap_id, &child_images);
  if (r < 0 && r != -ENOENT && r != -EOPNOTSUPP) {
    lderr(cct) << "error retrieving children: " << cpp_strerror(r) << dendl;
    return r;
  }

  for (auto& child_image : child_images) {
    images->push_back({
      child_image.pool_id, "", child_image.pool_namespace,
      child_image.image_id, "", false});
  }

  // batch lookups by pool + namespace
  std::sort(images->begin(), images->end(), compare_by_pool);

  int64_t child_pool_id = -1;
  librados::IoCtx child_io_ctx;
  std::map<std::string, std::pair<std::string, bool>> child_image_id_to_info;
  for (auto& image : *images) {
    if (child_pool_id == -1 || child_pool_id != image.pool_id ||
        child_io_ctx.get_namespace() != image.pool_namespace) {
      r = util::create_ioctx(ictx->md_ctx, "child image", image.pool_id,
                             image.pool_namespace, &child_io_ctx);
      if (r < 0) {
        return r;
      }
      child_pool_id = image.pool_id;

      child_image_id_to_info.clear();

      std::map<std::string, std::string> image_names_to_ids;
      r = list_images_v2(child_io_ctx, &image_names_to_ids);
      if (r < 0) {
        lderr(cct) << "error listing v2 images: " << cpp_strerror(r) << dendl;
        return r;
      }

      for (auto& [name, id] : image_names_to_ids) {
        child_image_id_to_info.insert({id, {name, false}});
      }

      std::vector<librbd::trash_image_info_t> trash_images;
      r = Trash<I>::list(child_io_ctx, trash_images);
      if (r < 0 && r != -EOPNOTSUPP) {
        lderr(cct) << "error listing trash images: " << cpp_strerror(r)
                   << dendl;
        return r;
      }

      for (auto& it : trash_images) {
        child_image_id_to_info[it.id] = {it.name, true};
      }
    }

    auto it = child_image_id_to_info.find(image.image_id);
    if (it == child_image_id_to_info.end()) {
          lderr(cct) << "error looking up name for image id "
                     << image.image_id << " in pool "
                     << child_io_ctx.get_pool_name()
                     << (image.pool_namespace.empty() ?
                          "" : "/" + image.pool_namespace) << dendl;
      return -ENOENT;
    }

    image.pool_name = child_io_ctx.get_pool_name();
    image.image_name = it->second.first;
    image.trash = it->second.second;
  }

  // final sort by pool + image names
  std::sort(images->begin(), images->end(), compare);
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
    features = (src->features & ~RBD_FEATURES_IMPLICIT_ENABLE);
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

  uint64_t flatten = 0;
  if (opts.get(RBD_IMAGE_OPTION_FLATTEN, &flatten) == 0) {
    opts.unset(RBD_IMAGE_OPTION_FLATTEN);
  }

  cls::rbd::ParentImageSpec parent_spec;
  if (flatten > 0) {
    parent_spec.pool_id = -1;
  } else {
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
    librados::IoCtx parent_io_ctx;
    r = util::create_ioctx(src->md_ctx, "parent image", parent_spec.pool_id,
                           parent_spec.pool_namespace, &parent_io_ctx);
    if (r < 0) {
      return r;
    }

    ConfigProxy config{cct->_conf};
    api::Config<I>::apply_pool_overrides(dest_md_ctx, &config);

    C_SaferCond ctx;
    std::string dest_id = util::generate_image_id(dest_md_ctx);
    auto *req = image::CloneRequest<I>::create(
      config, parent_io_ctx, parent_spec.image_id, "", parent_spec.snap_id,
      dest_md_ctx, destname, dest_id, opts, "", "", src->op_work_queue, &ctx);
    req->send();
    r = ctx.wait();
  }
  if (r < 0) {
    lderr(cct) << "header creation failed" << dendl;
    return r;
  }
  opts.set(RBD_IMAGE_OPTION_ORDER, static_cast<uint64_t>(order));

  ImageCtx *dest = new librbd::ImageCtx(destname, "", nullptr,
                                        dest_md_ctx, false);
  r = dest->state->open(0);
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

  r = deep_copy(src, dest, flatten > 0, prog_ctx);

  int close_r = dest->state->close();
  if (r == 0 && close_r < 0) {
    r = close_r;
  }
  return r;
}

template <typename I>
int Image<I>::deep_copy(I *src, I *dest, bool flatten,
                        ProgressContext &prog_ctx) {
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
                                       flatten, boost::none, op_work_queue,
                                       &snap_seqs, &prog_ctx, &cond);
  req->send();
  int r = cond.wait();
  if (r < 0) {
    return r;
  }

  return 0;
}

template <typename I>
int Image<I>::snap_set(I *ictx,
                       const cls::rbd::SnapshotNamespace &snap_namespace,
                       const char *snap_name) {
  ldout(ictx->cct, 20) << "snap_set " << ictx << " snap = "
                       << (snap_name ? snap_name : "NULL") << dendl;

  // ignore return value, since we may be set to a non-existent
  // snapshot and the user is trying to fix that
  ictx->state->refresh_if_required();

  uint64_t snap_id = CEPH_NOSNAP;
  std::string name(snap_name == nullptr ? "" : snap_name);
  if (!name.empty()) {
    RWLock::RLocker snap_locker(ictx->snap_lock);
    snap_id = ictx->get_snap_id(cls::rbd::UserSnapshotNamespace{},
                                snap_name);
    if (snap_id == CEPH_NOSNAP) {
      return -ENOENT;
    }
  }

  return snap_set(ictx, snap_id);
}

template <typename I>
int Image<I>::snap_set(I *ictx, uint64_t snap_id) {
  ldout(ictx->cct, 20) << "snap_set " << ictx << " "
                       << "snap_id=" << snap_id << dendl;

  // ignore return value, since we may be set to a non-existent
  // snapshot and the user is trying to fix that
  ictx->state->refresh_if_required();

  C_SaferCond ctx;
  ictx->state->snap_set(snap_id, &ctx);
  int r = ctx.wait();
  if (r < 0) {
    if (r != -ENOENT) {
      lderr(ictx->cct) << "failed to " << (snap_id == CEPH_NOSNAP ? "un" : "")
                       << "set snapshot: " << cpp_strerror(r) << dendl;
    }
    return r;
  }

  return 0;
}

template <typename I>
int Image<I>::remove(IoCtx& io_ctx, const std::string &image_name,
                     const std::string &image_id, ProgressContext& prog_ctx,
                     bool force, bool from_trash_remove)
{
  CephContext *cct((CephContext *)io_ctx.cct());
  ldout(cct, 20) << (image_id.empty() ? image_name : image_id) << dendl;

  if (image_id.empty()) {
    // id will only be supplied when used internally
    ConfigProxy config(cct->_conf);
    Config<I>::apply_pool_overrides(io_ctx, &config);

    if (config.get_val<bool>("rbd_move_to_trash_on_remove")) {
      return Trash<I>::move(
        io_ctx, RBD_TRASH_IMAGE_SOURCE_USER, image_name,
        config.get_val<uint64_t>("rbd_move_to_trash_on_remove_expire_seconds"));
    }
  }

  ThreadPool *thread_pool;
  ContextWQ *op_work_queue;
  ImageCtx::get_thread_pool_instance(cct, &thread_pool, &op_work_queue);

  C_SaferCond cond;
  auto req = librbd::image::RemoveRequest<>::create(
    io_ctx, image_name, image_id, force, from_trash_remove, prog_ctx,
    op_work_queue, &cond);
  req->send();

  return cond.wait();
}

} // namespace api
} // namespace librbd

template class librbd::api::Image<librbd::ImageCtx>;
