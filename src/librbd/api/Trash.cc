// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/api/Trash.h"
#include "include/rados/librados.hpp"
#include "common/dout.h"
#include "common/errno.h"
#include "cls/rbd/cls_rbd_client.h"
#include "librbd/ExclusiveLock.h"
#include "librbd/ImageCtx.h"
#include "librbd/ImageState.h"
#include "librbd/internal.h"
#include "librbd/Operations.h"
#include "librbd/TrashWatcher.h"
#include "librbd/Utils.h"
#include "librbd/api/Image.h"
#include "librbd/trash/MoveRequest.h"

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::api::Trash: " << __func__ << ": "

namespace librbd {
namespace api {

template <typename I>
int Trash<I>::move(librados::IoCtx &io_ctx, rbd_trash_image_source_t source,
                   const std::string &image_name, uint64_t delay) {
  CephContext *cct((CephContext *)io_ctx.cct());
  ldout(cct, 20) << "trash_move " << &io_ctx << " " << image_name
                 << dendl;

  // try to get image id from the directory
  std::string image_id;
  int r = cls_client::dir_get_id(&io_ctx, RBD_DIRECTORY, image_name,
                                 &image_id);
  if (r < 0 && r != -ENOENT) {
    lderr(cct) << "failed to retrieve image id: " << cpp_strerror(r) << dendl;
    return r;
  }

  ImageCtx *ictx = new ImageCtx((image_id.empty() ? image_name : ""),
                                image_id, nullptr, io_ctx, false);
  r = ictx->state->open(OPEN_FLAG_SKIP_OPEN_PARENT);
  if (r == -ENOENT) {
    return r;
  } else if (r < 0) {
    lderr(cct) << "failed to open image: " << cpp_strerror(r) << dendl;
    return r;
  } else if (ictx->old_format) {
    ldout(cct, 10) << "cannot move v1 image to trash" << dendl;
    ictx->state->close();
    return -EOPNOTSUPP;
  }

  image_id = ictx->id;
  ictx->owner_lock.get_read();
  if (ictx->exclusive_lock != nullptr) {
    ictx->exclusive_lock->block_requests(0);

    r = ictx->operations->prepare_image_update(false);
    if (r < 0) {
      lderr(cct) << "cannot obtain exclusive lock - not removing" << dendl;
      ictx->owner_lock.put_read();
      ictx->state->close();
      return -EBUSY;
    }
  }
  ictx->owner_lock.put_read();

  if (!ictx->migration_info.empty()) {
    lderr(cct) << "cannot move migrating image to trash" << dendl;
    ictx->state->close();
    return -EINVAL;
  }

  utime_t delete_time{ceph_clock_now()};
  utime_t deferment_end_time{delete_time};
  deferment_end_time += delay;
  cls::rbd::TrashImageSpec trash_image_spec{
    static_cast<cls::rbd::TrashImageSource>(source), ictx->name,
    delete_time, deferment_end_time};

  trash_image_spec.state = cls::rbd::TRASH_IMAGE_STATE_MOVING;
  C_SaferCond ctx;
  auto req = trash::MoveRequest<I>::create(io_ctx, ictx->id, trash_image_spec,
                                           &ctx);
  req->send();

  r = ctx.wait();
  ictx->state->close();
  trash_image_spec.state = cls::rbd::TRASH_IMAGE_STATE_NORMAL;
  int ret = cls_client::trash_state_set(&io_ctx, image_id,
                                        trash_image_spec.state,
                                        cls::rbd::TRASH_IMAGE_STATE_MOVING);
  if (ret < 0 && ret != -EOPNOTSUPP) {
    lderr(cct) << "error setting trash image state: "
               << cpp_strerror(ret) << dendl;
    return ret;
  }
  if (r < 0) {
    return r;
  }

  C_SaferCond notify_ctx;
  TrashWatcher<I>::notify_image_added(io_ctx, image_id, trash_image_spec,
                                      &notify_ctx);
  r = notify_ctx.wait();
  if (r < 0) {
    lderr(cct) << "failed to send update notification: " << cpp_strerror(r)
               << dendl;
  }

  return 0;
}

template <typename I>
int Trash<I>::get(IoCtx &io_ctx, const std::string &id,
              trash_image_info_t *info) {
  CephContext *cct((CephContext *)io_ctx.cct());
  ldout(cct, 20) << __func__ << " " << &io_ctx << dendl;

  cls::rbd::TrashImageSpec spec;
  int r = cls_client::trash_get(&io_ctx, id, &spec);
  if (r == -ENOENT) {
    return r;
  } else if (r < 0) {
    lderr(cct) << "error retrieving trash entry: " << cpp_strerror(r)
               << dendl;
    return r;
  }

  rbd_trash_image_source_t source = static_cast<rbd_trash_image_source_t>(
    spec.source);
  *info = trash_image_info_t{id, spec.name, source, spec.deletion_time.sec(),
                             spec.deferment_end_time.sec()};
  return 0;
}

template <typename I>
int Trash<I>::list(IoCtx &io_ctx, vector<trash_image_info_t> &entries) {
  CephContext *cct((CephContext *)io_ctx.cct());
  ldout(cct, 20) << "trash_list " << &io_ctx << dendl;

  bool more_entries;
  uint32_t max_read = 1024;
  std::string last_read = "";
  do {
    map<string, cls::rbd::TrashImageSpec> trash_entries;
    int r = cls_client::trash_list(&io_ctx, last_read, max_read,
                                   &trash_entries);
    if (r < 0 && r != -ENOENT) {
      lderr(cct) << "error listing rbd trash entries: " << cpp_strerror(r)
                 << dendl;
      return r;
    } else if (r == -ENOENT) {
      break;
    }

    if (trash_entries.empty()) {
      break;
    }

    for (const auto &entry : trash_entries) {
      rbd_trash_image_source_t source =
          static_cast<rbd_trash_image_source_t>(entry.second.source);
      entries.push_back({entry.first, entry.second.name, source,
                         entry.second.deletion_time.sec(),
                         entry.second.deferment_end_time.sec()});
    }
    last_read = trash_entries.rbegin()->first;
    more_entries = (trash_entries.size() >= max_read);
  } while (more_entries);

  return 0;
}

template <typename I>
int Trash<I>::remove(IoCtx &io_ctx, const std::string &image_id, bool force,
                     ProgressContext& prog_ctx) {
  CephContext *cct((CephContext *)io_ctx.cct());
  ldout(cct, 20) << "trash_remove " << &io_ctx << " " << image_id
                 << " " << force << dendl;

  cls::rbd::TrashImageSpec trash_spec;
  int r = cls_client::trash_get(&io_ctx, image_id, &trash_spec);
  if (r < 0) {
    lderr(cct) << "error getting image id " << image_id
               << " info from trash: " << cpp_strerror(r) << dendl;
    return r;
  }

  utime_t now = ceph_clock_now();
  if (now < trash_spec.deferment_end_time && !force) {
    lderr(cct) << "error: deferment time has not expired." << dendl;
    return -EPERM;
  }
  if (trash_spec.state != cls::rbd::TRASH_IMAGE_STATE_NORMAL &&
      trash_spec.state != cls::rbd::TRASH_IMAGE_STATE_REMOVING) {
    lderr(cct) << "error: image is pending restoration." << dendl;
    return -EBUSY;
  }

  r = cls_client::trash_state_set(&io_ctx, image_id,
                                  cls::rbd::TRASH_IMAGE_STATE_REMOVING,
                                  cls::rbd::TRASH_IMAGE_STATE_NORMAL);
  if (r < 0 && r != -EOPNOTSUPP) {
    lderr(cct) << "error setting trash image state: "
               << cpp_strerror(r) << dendl;
    return r;
  }

  r = Image<I>::remove(io_ctx, "", image_id, prog_ctx, false, true);
  if (r < 0) {
    lderr(cct) << "error removing image " << image_id
               << ", which is pending deletion" << dendl;
    int ret = cls_client::trash_state_set(&io_ctx, image_id,
                                          cls::rbd::TRASH_IMAGE_STATE_NORMAL,
                                          cls::rbd::TRASH_IMAGE_STATE_REMOVING);
    if (ret < 0 && ret != -EOPNOTSUPP) {
      lderr(cct) << "error setting trash image state: "
                 << cpp_strerror(ret) << dendl;
    }
    return r;
  }
  r = cls_client::trash_remove(&io_ctx, image_id);
  if (r < 0 && r != -ENOENT) {
    lderr(cct) << "error removing image " << image_id
               << " from rbd_trash object" << dendl;
    return r;
  }

  C_SaferCond notify_ctx;
  TrashWatcher<I>::notify_image_removed(io_ctx, image_id, &notify_ctx);
  r = notify_ctx.wait();
  if (r < 0) {
    lderr(cct) << "failed to send update notification: " << cpp_strerror(r)
               << dendl;
  }

  return 0;
}

template <typename I>
int Trash<I>::restore(librados::IoCtx &io_ctx, const std::string &image_id,
                      const std::string &image_new_name) {
  CephContext *cct((CephContext *)io_ctx.cct());
  ldout(cct, 20) << "trash_restore " << &io_ctx << " " << image_id << " "
                 << image_new_name << dendl;

  cls::rbd::TrashImageSpec trash_spec;
  int r = cls_client::trash_get(&io_ctx, image_id, &trash_spec);
  if (r < 0) {
    lderr(cct) << "error getting image id " << image_id
               << " info from trash: " << cpp_strerror(r) << dendl;
    return r;
  }

  std::string image_name = image_new_name;
  if (trash_spec.state != cls::rbd::TRASH_IMAGE_STATE_NORMAL &&
      trash_spec.state != cls::rbd::TRASH_IMAGE_STATE_RESTORING) {
    lderr(cct) << "error restoring image id " << image_id
               << ", which is pending deletion" << dendl;
    return -EBUSY;
  }
  r = cls_client::trash_state_set(&io_ctx, image_id,
                                  cls::rbd::TRASH_IMAGE_STATE_RESTORING,
                                  cls::rbd::TRASH_IMAGE_STATE_NORMAL);
  if (r < 0 && r != -EOPNOTSUPP) {
    lderr(cct) << "error setting trash image state: "
               << cpp_strerror(r) << dendl;
    return r;
  }

  if (image_name.empty()) {
    // if user didn't specify a new name, let's try using the old name
    image_name = trash_spec.name;
    ldout(cct, 20) << "restoring image id " << image_id << " with name "
                   << image_name << dendl;
  }

  // check if no image exists with the same name
  bool create_id_obj = true;
  std::string existing_id;
  r = cls_client::get_id(&io_ctx, util::id_obj_name(image_name), &existing_id);
  if (r < 0 && r != -ENOENT) {
    lderr(cct) << "error checking if image " << image_name << " exists: "
               << cpp_strerror(r) << dendl;
    int ret = cls_client::trash_state_set(&io_ctx, image_id,
                                          cls::rbd::TRASH_IMAGE_STATE_NORMAL,
                                          cls::rbd::TRASH_IMAGE_STATE_RESTORING);
    if (ret < 0 && ret != -EOPNOTSUPP) {
      lderr(cct) << "error setting trash image state: "
                 << cpp_strerror(ret) << dendl;
    }
    return r;
  } else if (r != -ENOENT){
    // checking if we are recovering from an incomplete restore
    if (existing_id != image_id) {
      ldout(cct, 2) << "an image with the same name already exists" << dendl;
      int r2 = cls_client::trash_state_set(&io_ctx, image_id,
                                           cls::rbd::TRASH_IMAGE_STATE_NORMAL,
                                           cls::rbd::TRASH_IMAGE_STATE_RESTORING);
      if (r2 < 0 && r2 != -EOPNOTSUPP) {
      lderr(cct) << "error setting trash image state: "
                 << cpp_strerror(r2) << dendl;
     }
      return -EEXIST;
    }
    create_id_obj = false;
  }

  if (create_id_obj) {
    ldout(cct, 2) << "adding id object" << dendl;
    librados::ObjectWriteOperation op;
    op.create(true);
    cls_client::set_id(&op, image_id);
    r = io_ctx.operate(util::id_obj_name(image_name), &op);
    if (r < 0) {
      lderr(cct) << "error adding id object for image " << image_name
                 << ": " << cpp_strerror(r) << dendl;
      return r;
    }
  }

  ldout(cct, 2) << "adding rbd image from v2 directory..." << dendl;
  r = cls_client::dir_add_image(&io_ctx, RBD_DIRECTORY, image_name,
                                image_id);
  if (r < 0 && r != -EEXIST) {
    lderr(cct) << "error adding image to v2 directory: "
               << cpp_strerror(r) << dendl;
    return r;
  }

  ldout(cct, 2) << "removing image from trash..." << dendl;
  r = cls_client::trash_remove(&io_ctx, image_id);
  if (r < 0 && r != -ENOENT) {
    lderr(cct) << "error removing image id " << image_id << " from trash: "
               << cpp_strerror(r) << dendl;
    return r;
  }

  C_SaferCond notify_ctx;
  TrashWatcher<I>::notify_image_removed(io_ctx, image_id, &notify_ctx);
  r = notify_ctx.wait();
  if (r < 0) {
    lderr(cct) << "failed to send update notification: " << cpp_strerror(r)
               << dendl;
  }

  return 0;
}

} // namespace api
} // namespace librbd

template class librbd::api::Trash<librbd::ImageCtx>;
