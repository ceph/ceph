// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/api/Image.h"
#include "include/rados/librados.hpp"
#include "common/dout.h"
#include "common/errno.h"
#include "common/Cond.h"
#include "cls/rbd/cls_rbd_client.h"
#include "librbd/AsioEngine.h"
#include "librbd/DeepCopyRequest.h"
#include "librbd/ExclusiveLock.h"
#include "librbd/ImageCtx.h"
#include "librbd/ImageState.h"
#include "librbd/internal.h"
#include "librbd/Operations.h"
#include "librbd/Utils.h"
#include "librbd/api/Config.h"
#include "librbd/api/Trash.h"
#include "librbd/deep_copy/Handler.h"
#include "librbd/image/CloneRequest.h"
#include "librbd/image/RemoveRequest.h"
#include "librbd/image/PreRemoveRequest.h"
#include <boost/scope_exit.hpp>

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::api::Image: " << __func__ << ": "

using librados::snap_t;

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

template <typename I>
int pre_remove_image(librados::IoCtx& io_ctx, const std::string& image_id) {
  I *image_ctx = I::create("", image_id, nullptr, io_ctx, false);
  int r = image_ctx->state->open(OPEN_FLAG_SKIP_OPEN_PARENT);
  if (r < 0) {
    return r;
  }

  C_SaferCond ctx;
  auto req = image::PreRemoveRequest<I>::create(image_ctx, false, &ctx);
  req->send();

  r = ctx.wait();
  image_ctx->state->close();
  return r;
}

} // anonymous namespace

template <typename I>
int64_t Image<I>::get_data_pool_id(I *ictx) {
  if (ictx->data_ctx.is_valid()) {
    return ictx->data_ctx.get_id();
  }

  int64_t pool_id;
  int r = cls_client::get_data_pool(&ictx->md_ctx, ictx->header_oid, &pool_id);
  if (r < 0) {
    CephContext *cct = ictx->cct;
    lderr(cct) << "error getting data pool ID: " << cpp_strerror(r) << dendl;
    return r;
  }

  return pool_id;
}

template <typename I>
int Image<I>::get_op_features(I *ictx, uint64_t *op_features) {
  CephContext *cct = ictx->cct;
  ldout(cct, 20) << "image_ctx=" << ictx << dendl;

  int r = ictx->state->refresh_if_required();
  if (r < 0) {
    return r;
  }

  std::shared_lock image_locker{ictx->image_lock};
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

    // V1 format images are in a tmap
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

  // V2 format images
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

  // include V2 images in a partially removed state
  std::vector<librbd::trash_image_info_t> trash_images;
  r = Trash<I>::list(io_ctx, trash_images, false);
  if (r < 0 && r != -EOPNOTSUPP) {
    lderr(cct) << "error listing trash images: " << cpp_strerror(r) << dendl;
    return r;
  }

  for (const auto& trash_image : trash_images) {
    if (trash_image.source == RBD_TRASH_IMAGE_SOURCE_REMOVING) {
      images->push_back({.id = trash_image.id,
                         .name = trash_image.name});

    }
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

  std::shared_lock image_locker{ictx->image_lock};

  bool release_image_lock = false;
  BOOST_SCOPE_EXIT_ALL(ictx, &release_image_lock) {
    if (release_image_lock) {
      ictx->parent->image_lock.unlock_shared();
    }
  };

  // if a migration is in-progress, the true parent is the parent
  // of the migration source image
  auto parent = ictx->parent;
  if (!ictx->migration_info.empty() && ictx->parent != nullptr) {
    release_image_lock = true;
    ictx->parent->image_lock.lock_shared();

    parent = ictx->parent->parent;
  }

  if (parent == nullptr) {
    return -ENOENT;
  }

  parent_image->pool_id = parent->md_ctx.get_id();
  parent_image->pool_name = parent->md_ctx.get_pool_name();
  parent_image->pool_namespace = parent->md_ctx.get_namespace();

  std::shared_lock parent_image_locker{parent->image_lock};
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
  images->clear();
  return list_descendants(ictx, 1, images);
}

template <typename I>
int Image<I>::list_children(I *ictx,
                            const cls::rbd::ParentImageSpec &parent_spec,
                            std::vector<librbd::linked_image_spec_t> *images) {
  images->clear();
  return list_descendants(ictx, parent_spec, 1, images);
}

template <typename I>
int Image<I>::list_descendants(
    librados::IoCtx& io_ctx, const std::string &image_id,
    const std::optional<size_t> &max_level,
    std::vector<librbd::linked_image_spec_t> *images) {
  ImageCtx *ictx = new librbd::ImageCtx("", image_id, nullptr,
                                        io_ctx, true);
  int r = ictx->state->open(OPEN_FLAG_SKIP_OPEN_PARENT);
  if (r < 0) {
    if (r == -ENOENT) {
      return 0;
    }
    lderr(ictx->cct) << "failed to open descendant " << image_id
                     << " from pool " << io_ctx.get_pool_name() << ":"
                     << cpp_strerror(r) << dendl;
    return r;
  }

  r = list_descendants(ictx, max_level, images);

  int r1 = ictx->state->close();
  if (r1 < 0) {
    lderr(ictx->cct) << "error when closing descendant " << image_id
                     << " from pool " << io_ctx.get_pool_name() << ":"
                     << cpp_strerror(r) << dendl;
  }

  return r;
}

template <typename I>
int Image<I>::list_descendants(
    I *ictx, const std::optional<size_t> &max_level,
    std::vector<librbd::linked_image_spec_t> *images) {
  std::shared_lock l{ictx->image_lock};
  std::vector<librados::snap_t> snap_ids;
  if (ictx->snap_id != CEPH_NOSNAP) {
    snap_ids.push_back(ictx->snap_id);
  } else {
    snap_ids = ictx->snaps;
  }
  for (auto snap_id : snap_ids) {
    cls::rbd::ParentImageSpec parent_spec{ictx->md_ctx.get_id(),
                                          ictx->md_ctx.get_namespace(),
                                          ictx->id, snap_id};
    int r = list_descendants(ictx, parent_spec, max_level, images);
    if (r < 0) {
      return r;
    }
  }
  return 0;
}

template <typename I>
int Image<I>::list_descendants(
    I *ictx, const cls::rbd::ParentImageSpec &parent_spec,
    const std::optional<size_t> &max_level,
    std::vector<librbd::linked_image_spec_t> *images) {
  auto child_max_level = max_level;
  if (child_max_level) {
    if (child_max_level == 0) {
      return 0;
    }
    (*child_max_level)--;
  }
  CephContext *cct = ictx->cct;
  ldout(cct, 20) << "ictx=" << ictx << dendl;

  // no children for non-layered or old format image
  if (!ictx->test_features(RBD_FEATURE_LAYERING, ictx->image_lock)) {
    return 0;
  }

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
      r = list_descendants(ioctx, image_id, child_max_level, images);
      if (r < 0) {
        return r;
      }
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
    if (!child_max_level || *child_max_level > 0) {
      IoCtx ioctx;
      r = util::create_ioctx(ictx->md_ctx, "child image", child_image.pool_id,
                             child_image.pool_namespace, &ioctx);
      if (r == -ENOENT) {
        continue;
      } else if (r < 0) {
        return r;
      }
      r = list_descendants(ioctx, child_image.image_id, child_max_level,
                           images);
      if (r < 0) {
        return r;
      }
    }
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
      if (r == -ENOENT) {
        image.pool_name = "";
        image.image_name = "";
        continue;
      } else if (r < 0) {
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
      r = Trash<I>::list(child_io_ctx, trash_images, false);
      if (r < 0 && r != -EOPNOTSUPP) {
        lderr(cct) << "error listing trash images: " << cpp_strerror(r)
                   << dendl;
        return r;
      }

      for (auto& it : trash_images) {
        child_image_id_to_info.insert({
          it.id,
          {it.name,
           it.source == RBD_TRASH_IMAGE_SOURCE_REMOVING ? false : true}});
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
    std::shared_lock image_locker{src->image_lock};

    if (!src->migration_info.empty()) {
      lderr(cct) << "cannot deep copy migrating image" << dendl;
      return -EBUSY;
    }

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
    std::shared_lock image_locker{src->image_lock};

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
      config, parent_io_ctx, parent_spec.image_id, "", {}, parent_spec.snap_id,
      dest_md_ctx, destname, dest_id, opts, cls::rbd::MIRROR_IMAGE_MODE_JOURNAL,
      "", "", src->op_work_queue, &ctx);
    req->send();
    r = ctx.wait();
  }
  if (r < 0) {
    lderr(cct) << "header creation failed" << dendl;
    return r;
  }
  opts.set(RBD_IMAGE_OPTION_ORDER, static_cast<uint64_t>(order));

  auto dest = new I(destname, "", nullptr, dest_md_ctx, false);
  r = dest->state->open(0);
  if (r < 0) {
    lderr(cct) << "failed to read newly created header" << dendl;
    return r;
  }

  C_SaferCond lock_ctx;
  {
    std::unique_lock locker{dest->owner_lock};

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
  librados::snap_t snap_id_start = 0;
  librados::snap_t snap_id_end;
  {
    std::shared_lock image_locker{src->image_lock};
    snap_id_end = src->snap_id;
  }

  AsioEngine asio_engine(src->md_ctx);

  C_SaferCond cond;
  SnapSeqs snap_seqs;
  deep_copy::ProgressHandler progress_handler{&prog_ctx};
  auto req = DeepCopyRequest<I>::create(
    src, dest, snap_id_start, snap_id_end, 0U, flatten, boost::none,
    asio_engine.get_work_queue(), &snap_seqs, &progress_handler, &cond);
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
    std::shared_lock image_locker{ictx->image_lock};
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
                     ProgressContext& prog_ctx)
{
  CephContext *cct((CephContext *)io_ctx.cct());
  ldout(cct, 20) << "name=" << image_name << dendl;

  // look up the V2 image id based on the image name
  std::string image_id;
  int r = cls_client::dir_get_id(&io_ctx, RBD_DIRECTORY, image_name,
                                 &image_id);
  if (r == -ENOENT) {
    // check if it already exists in trash from an aborted trash remove attempt
    std::vector<trash_image_info_t> trash_entries;
    r = Trash<I>::list(io_ctx, trash_entries, false);
    if (r < 0) {
      return r;
    } else if (r >= 0) {
      for (auto& entry : trash_entries) {
        if (entry.name == image_name &&
            entry.source == RBD_TRASH_IMAGE_SOURCE_REMOVING) {
          return Trash<I>::remove(io_ctx, entry.id, true, prog_ctx);
        }
      }
    }

    // fall-through if we failed to locate the image in the V2 directory and
    // trash
  } else if (r < 0) {
    lderr(cct) << "failed to retrieve image id: " << cpp_strerror(r) << dendl;
    return r;
  } else {
    // attempt to move the image to the trash (and optionally immediately
    // delete the image)
    ConfigProxy config(cct->_conf);
    Config<I>::apply_pool_overrides(io_ctx, &config);

    rbd_trash_image_source_t trash_image_source =
      RBD_TRASH_IMAGE_SOURCE_REMOVING;
    uint64_t expire_seconds = 0;
    if (config.get_val<bool>("rbd_move_to_trash_on_remove")) {
      // keep the image in the trash upon remove requests
      trash_image_source = RBD_TRASH_IMAGE_SOURCE_USER;
      expire_seconds = config.get_val<uint64_t>(
        "rbd_move_to_trash_on_remove_expire_seconds");
    } else {
      // attempt to pre-validate the removal before moving to trash and
      // removing
      r = pre_remove_image<I>(io_ctx, image_id);
      if (r == -ECHILD) {
        if (config.get_val<bool>("rbd_move_parent_to_trash_on_remove")) {
          // keep the image in the trash until the last child is removed
          trash_image_source = RBD_TRASH_IMAGE_SOURCE_USER_PARENT;
        } else {
          lderr(cct) << "image has snapshots - not removing" << dendl;
          return -ENOTEMPTY;
        }
      } else if (r < 0 && r != -ENOENT) {
        return r;
      }
    }

    r = Trash<I>::move(io_ctx, trash_image_source, image_name, image_id,
                       expire_seconds);
    if (r >= 0) {
      if (trash_image_source == RBD_TRASH_IMAGE_SOURCE_REMOVING) {
        // proceed with attempting to immediately remove the image
        r = Trash<I>::remove(io_ctx, image_id, true, prog_ctx);

        if (r == -ENOTEMPTY || r == -EBUSY || r == -EMLINK) {
          // best-effort try to restore the image if the removal
          // failed for possible expected reasons
          Trash<I>::restore(io_ctx, {cls::rbd::TRASH_IMAGE_SOURCE_REMOVING},
                            image_id, image_name);
        }
      }
      return r;
    } else if (r < 0 && r != -EOPNOTSUPP) {
      return r;
    }

    // fall-through if trash isn't supported
  }

  AsioEngine asio_engine(io_ctx);

  // might be a V1 image format that cannot be moved to the trash
  // and would not have been listed in the V2 directory -- or the OSDs
  // are too old and don't support the trash feature
  C_SaferCond cond;
  auto req = librbd::image::RemoveRequest<I>::create(
    io_ctx, image_name, "", false, false, prog_ctx,
    asio_engine.get_work_queue(), &cond);
  req->send();

  return cond.wait();
}

template <typename I>
int Image<I>::flatten_children(I *ictx, const char* snap_name,
                               ProgressContext& pctx) {
  CephContext *cct = ictx->cct;
  ldout(cct, 20) << "children flatten " << ictx->name << dendl;

  int r = ictx->state->refresh_if_required();
  if (r < 0) {
    return r;
  }

  std::shared_lock l{ictx->image_lock};
  snap_t snap_id = ictx->get_snap_id(cls::rbd::UserSnapshotNamespace(),
                                     snap_name);

  cls::rbd::ParentImageSpec parent_spec{ictx->md_ctx.get_id(),
                                        ictx->md_ctx.get_namespace(),
                                        ictx->id, snap_id};
  std::vector<librbd::linked_image_spec_t> child_images;
  r = list_children(ictx, parent_spec, &child_images);
  if (r < 0) {
    return r;
  }

  size_t size = child_images.size();
  if (size == 0) {
    return 0;
  }

  librados::IoCtx child_io_ctx;
  int64_t child_pool_id = -1;
  size_t i = 0;
  for (auto &child_image : child_images){
    std::string pool = child_image.pool_name;
    if (child_pool_id == -1 ||
        child_pool_id != child_image.pool_id ||
        child_io_ctx.get_namespace() != child_image.pool_namespace) {
      r = util::create_ioctx(ictx->md_ctx, "child image",
                             child_image.pool_id, child_image.pool_namespace,
                             &child_io_ctx);
      if (r < 0) {
        return r;
      }

      child_pool_id = child_image.pool_id;
    }

    ImageCtx *imctx = new ImageCtx("", child_image.image_id, nullptr,
                                   child_io_ctx, false);
    r = imctx->state->open(0);
    if (r < 0) {
      lderr(cct) << "error opening image: " << cpp_strerror(r) << dendl;
      return r;
    }

    if ((imctx->features & RBD_FEATURE_DEEP_FLATTEN) == 0 &&
        !imctx->snaps.empty()) {
      lderr(cct) << "snapshot in-use by " << pool << "/" << imctx->name
                 << dendl;
      imctx->state->close();
      return -EBUSY;
    }

    librbd::NoOpProgressContext prog_ctx;
    r = imctx->operations->flatten(prog_ctx);
    if (r < 0) {
      lderr(cct) << "error flattening image: " << pool << "/"
                 << (child_image.pool_namespace.empty() ?
                      "" : "/" + child_image.pool_namespace)
                 << child_image.image_name << cpp_strerror(r) << dendl;
      imctx->state->close();
      return r;
    }

    r = imctx->state->close();
    if (r < 0) {
      lderr(cct) << "failed to close image: " << cpp_strerror(r) << dendl;
      return r;
    }

    pctx.update_progress(++i, size);
    ceph_assert(i <= size);
  }

  return 0;
}

} // namespace api
} // namespace librbd

template class librbd::api::Image<librbd::ImageCtx>;
