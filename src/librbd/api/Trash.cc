// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/api/Trash.h"
#include "include/rados/librados.hpp"
#include "common/dout.h"
#include "common/errno.h"
#include "common/Cond.h"
#include "cls/rbd/cls_rbd_client.h"
#include "librbd/ExclusiveLock.h"
#include "librbd/ImageCtx.h"
#include "librbd/ImageState.h"
#include "librbd/internal.h"
#include "librbd/Operations.h"
#include "librbd/TrashWatcher.h"
#include "librbd/Utils.h"
#include "librbd/api/DiffIterate.h"
#include "librbd/image/RemoveRequest.h"
#include "librbd/mirror/DisableRequest.h"
#include "librbd/mirror/EnableRequest.h"
#include "librbd/trash/MoveRequest.h"
#include <json_spirit/json_spirit.h>
#include "librbd/journal/DisabledPolicy.h"
#include "librbd/image/ListWatchersRequest.h"

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::api::Trash: " << __func__ << ": "

namespace librbd {
namespace api {

namespace {

template <typename I>
int disable_mirroring(I *ictx) {
  if (!ictx->test_features(RBD_FEATURE_JOURNALING)) {
    return 0;
  }

  cls::rbd::MirrorImage mirror_image;
  int r = cls_client::mirror_image_get(&ictx->md_ctx, ictx->id, &mirror_image);
  if (r == -ENOENT) {
    ldout(ictx->cct, 10) << "mirroring is not enabled for this image" << dendl;
    return 0;
  }

  if (r < 0) {
    lderr(ictx->cct) << "failed to retrieve mirror image: " << cpp_strerror(r)
                     << dendl;
    return r;
  }

  ldout(ictx->cct, 10) << dendl;

  C_SaferCond ctx;
  auto req = mirror::DisableRequest<I>::create(ictx, false, true, &ctx);
  req->send();
  r = ctx.wait();
  if (r < 0) {
    lderr(ictx->cct) << "failed to disable mirroring: " << cpp_strerror(r)
                     << dendl;
    return r;
  }

  return 0;
}

template <typename I>
int enable_mirroring(IoCtx &io_ctx, const std::string &image_id) {
  auto cct = reinterpret_cast<CephContext*>(io_ctx.cct());

  uint64_t features;
  uint64_t incompatible_features;
  int r = cls_client::get_features(&io_ctx, util::header_name(image_id), true,
                                   &features, &incompatible_features);
  if (r < 0) {
    lderr(cct) << "failed to retrieve features: " << cpp_strerror(r) << dendl;
    return r;
  }

  if ((features & RBD_FEATURE_JOURNALING) == 0) {
    return 0;
  }

  cls::rbd::MirrorMode mirror_mode;
  r = cls_client::mirror_mode_get(&io_ctx, &mirror_mode);
  if (r < 0 && r != -ENOENT) {
    lderr(cct) << "failed to retrieve mirror mode: " << cpp_strerror(r)
               << dendl;
    return r;
  }

  if (mirror_mode != cls::rbd::MIRROR_MODE_POOL) {
    ldout(cct, 10) << "not pool mirroring mode" << dendl;
    return 0;
  }

  ldout(cct, 10) << dendl;

  ThreadPool *thread_pool;
  ContextWQ *op_work_queue;
  ImageCtx::get_thread_pool_instance(cct, &thread_pool, &op_work_queue);
  C_SaferCond ctx;
  auto req = mirror::EnableRequest<I>::create(io_ctx, image_id, "",
                                              op_work_queue, &ctx);
  req->send();
  r = ctx.wait();
  if (r < 0) {
    lderr(cct) << "failed to enable mirroring: " << cpp_strerror(r)
               << dendl;
    return r;
  }

  return 0;
}

} // anonymous namespace

template <typename I>
int Trash<I>::move(librados::IoCtx &io_ctx, rbd_trash_image_source_t source,
                   const std::string &image_name, const std::string &image_id,
                   uint64_t delay) {
  ceph_assert(!image_name.empty() && !image_id.empty());
  CephContext *cct((CephContext *)io_ctx.cct());
  ldout(cct, 20) << &io_ctx << " name=" << image_name << ", id=" << image_id
                 << dendl;

  auto ictx = new I("", image_id, nullptr, io_ctx, false);
  int r = ictx->state->open(OPEN_FLAG_SKIP_OPEN_PARENT);

  if (r < 0 && r != -ENOENT) {
    lderr(cct) << "failed to open image: " << cpp_strerror(r) << dendl;
    return r;
  }

  if (r == 0) {
    if (ictx->test_features(RBD_FEATURE_JOURNALING)) {
      RWLock::WLocker snap_locker(ictx->snap_lock);
      ictx->set_journal_policy(new journal::DisabledPolicy());
    }

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

    ictx->snap_lock.get_read();
    if (!ictx->migration_info.empty()) {
      lderr(cct) << "cannot move migrating image to trash" << dendl;
      ictx->snap_lock.put_read();
      ictx->state->close();
      return -EBUSY;
    }
    ictx->snap_lock.put_read();

    r = disable_mirroring<I>(ictx);
    if (r < 0) {
      ictx->state->close();
      return r;
    }

    ictx->state->close();
  }

  utime_t delete_time{ceph_clock_now()};
  utime_t deferment_end_time{delete_time};
  deferment_end_time += delay;
  cls::rbd::TrashImageSpec trash_image_spec{
    static_cast<cls::rbd::TrashImageSource>(source), image_name,
    delete_time, deferment_end_time};

  trash_image_spec.state = cls::rbd::TRASH_IMAGE_STATE_MOVING;
  C_SaferCond ctx;
  auto req = trash::MoveRequest<I>::create(io_ctx, image_id, trash_image_spec,
                                           &ctx);
  req->send();

  r = ctx.wait();
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
int Trash<I>::move(librados::IoCtx &io_ctx, rbd_trash_image_source_t source,
                   const std::string &image_name, uint64_t delay) {
  CephContext *cct((CephContext *)io_ctx.cct());
  ldout(cct, 20) << &io_ctx << " name=" << image_name << dendl;

  // try to get image id from the directory
  std::string image_id;
  int r = cls_client::dir_get_id(&io_ctx, RBD_DIRECTORY, image_name,
                                 &image_id);
  if (r == -ENOENT) {
    r = io_ctx.stat(util::old_header_name(image_name), nullptr, nullptr);
    if (r == 0) {
      // cannot move V1 image to trash
      ldout(cct, 10) << "cannot move v1 image to trash" << dendl;
      return -EOPNOTSUPP;
    }

    // image doesn't exist -- perhaps already in the trash since removing
    // from the directory is the last step
    return -ENOENT;
  } else if (r < 0) {
    lderr(cct) << "failed to retrieve image id: " << cpp_strerror(r) << dendl;
    return r;
  }

  ceph_assert(!image_name.empty() && !image_id.empty());
  return Trash<I>::move(io_ctx, source, image_name, image_id, delay);
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
int Trash<I>::list(IoCtx &io_ctx, vector<trash_image_info_t> &entries,
                   bool exclude_user_remove_source) {
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
      if (exclude_user_remove_source &&
          source == RBD_TRASH_IMAGE_SOURCE_REMOVING) {
        continue;
      }
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
int Trash<I>::purge(IoCtx& io_ctx, time_t expire_ts,
                    float threshold, ProgressContext& pctx) {
  auto *cct((CephContext *) io_ctx.cct());
  ldout(cct, 20) << &io_ctx << dendl;

  std::vector<librbd::trash_image_info_t> trash_entries;
  int r = librbd::api::Trash<I>::list(io_ctx, trash_entries, true);
  if (r < 0) {
    return r;
  }

  trash_entries.erase(
      std::remove_if(trash_entries.begin(), trash_entries.end(),
                     [](librbd::trash_image_info_t info) {
                       return info.source != RBD_TRASH_IMAGE_SOURCE_USER;
                     }),
      trash_entries.end());

  std::set<std::string> to_be_removed;
  if (threshold != -1) {
    if (threshold < 0 || threshold > 1) {
      lderr(cct) << "argument 'threshold' is out of valid range"
                 << dendl;
      return -EINVAL;
    }

    librados::bufferlist inbl;
    librados::bufferlist outbl;
    std::string pool_name = io_ctx.get_pool_name();

    librados::Rados rados(io_ctx);
    rados.mon_command(R"({"prefix": "df", "format": "json"})", inbl,
                      &outbl, nullptr);

    json_spirit::mValue json;
    if (!json_spirit::read(outbl.to_str(), json)) {
      lderr(cct) << "ceph df json output could not be parsed"
                 << dendl;
      return -EBADMSG;
    }

    json_spirit::mArray arr = json.get_obj()["pools"].get_array();

    double pool_percent_used = 0;
    uint64_t pool_total_bytes = 0;

    std::map<std::string, std::vector<std::string>> datapools;

    std::sort(trash_entries.begin(), trash_entries.end(),
        [](librbd::trash_image_info_t a, librbd::trash_image_info_t b) {
          return a.deferment_end_time < b.deferment_end_time;
        }
    );

    for (const auto &entry : trash_entries) {
      int64_t data_pool_id = -1;
      r = cls_client::get_data_pool(&io_ctx, util::header_name(entry.id),
                                    &data_pool_id);
      if (r < 0 && r != -ENOENT && r != -EOPNOTSUPP) {
        lderr(cct) << "failed to query data pool: " << cpp_strerror(r) << dendl;
        return r;
      } else if (data_pool_id == -1) {
        data_pool_id = io_ctx.get_id();
      }

      if (data_pool_id != io_ctx.get_id()) {
        librados::IoCtx data_io_ctx;
        r = util::create_ioctx(io_ctx, "image", data_pool_id,
                               {}, &data_io_ctx);
        if (r < 0) {
          lderr(cct) << "error accessing data pool" << dendl;
          continue;
        }
        auto data_pool = data_io_ctx.get_pool_name();
        datapools[data_pool].push_back(entry.id);
      } else {
        datapools[pool_name].push_back(entry.id);
      }
    }

    uint64_t bytes_to_free = 0;

    for (uint8_t i = 0; i < arr.size(); ++i) {
      json_spirit::mObject obj = arr[i].get_obj();
      std::string name = obj.find("name")->second.get_str();
      auto img = datapools.find(name);
      if (img != datapools.end()) {
        json_spirit::mObject stats = arr[i].get_obj()["stats"].get_obj();
        pool_percent_used = stats["percent_used"].get_real();
        if (pool_percent_used <= threshold) continue;

        bytes_to_free = 0;

        pool_total_bytes = stats["max_avail"].get_uint64() +
                           stats["bytes_used"].get_uint64();

        auto bytes_threshold = (uint64_t) (pool_total_bytes *
                                          (pool_percent_used - threshold));

        for (const auto &it : img->second) {
          auto ictx = new I("", it, nullptr, io_ctx, false);
          r = ictx->state->open(OPEN_FLAG_SKIP_OPEN_PARENT);
          if (r == -ENOENT) {
            continue;
          } else if (r < 0) {
            lderr(cct) << "failed to open image " << it << ": "
                       << cpp_strerror(r) << dendl;
          }

          r = librbd::api::DiffIterate<I>::diff_iterate(
            ictx, cls::rbd::UserSnapshotNamespace(), nullptr, 0, ictx->size,
            false, true,
            [](uint64_t offset, size_t len, int exists, void *arg) {
                auto *to_free = reinterpret_cast<uint64_t *>(arg);
                if (exists)
                  (*to_free) += len;
                return 0;
            }, &bytes_to_free);

          ictx->state->close();
          if (r < 0) {
            lderr(cct) << "failed to calculate disk usage for image " << it
                       << ": " << cpp_strerror(r) << dendl;
            continue;
          }

          to_be_removed.insert(it);
          if (bytes_to_free >= bytes_threshold) {
            break;
          }
        }
      }
    }

    if (bytes_to_free == 0) {
      ldout(cct, 10) << "pool usage is lower than or equal to "
                     << (threshold * 100)
                     << "%" << dendl;
      return 0;
    }
  }

  if (expire_ts == 0) {
    struct timespec now;
    clock_gettime(CLOCK_REALTIME, &now);
    expire_ts = now.tv_sec;
  }

  for (const auto &entry : trash_entries) {
    if (expire_ts >= entry.deferment_end_time) {
      to_be_removed.insert(entry.id);
    }
  }

  NoOpProgressContext remove_pctx;
  uint64_t list_size = to_be_removed.size(), i = 0;
  for (const auto &entry_id : to_be_removed) {
    r = librbd::api::Trash<I>::remove(io_ctx, entry_id, true, remove_pctx);
    if (r < 0) {
      if (r == -ENOTEMPTY) {
        ldout(cct, 5) << "image has snapshots - these must be deleted "
                      << "with 'rbd snap purge' before the image can be "
                      << "removed." << dendl;
      } else if (r == -EBUSY) {
        ldout(cct, 5) << "error: image still has watchers" << std::endl
                      << "This means the image is still open or the client "
                      << "using it crashed. Try again after closing/unmapping "
                      << "it or waiting 30s for the crashed client to timeout."
                      << dendl;
      } else if (r == -EMLINK) {
        ldout(cct, 5) << "Remove the image from the group and try again."
                      << dendl;
      } else {
        lderr(cct) << "remove error: " << cpp_strerror(r) << dendl;
      }
      return r;
    }
    pctx.update_progress(++i, list_size);
  }

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

  ThreadPool *thread_pool;
  ContextWQ *op_work_queue;
  ImageCtx::get_thread_pool_instance(cct, &thread_pool, &op_work_queue);

  C_SaferCond cond;
  auto req = librbd::image::RemoveRequest<I>::create(
    io_ctx, "", image_id, force, true, prog_ctx, op_work_queue, &cond);
  req->send();

  r = cond.wait();
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
int Trash<I>::restore(librados::IoCtx &io_ctx, rbd_trash_image_source_t source,
                      const std::string &image_id,
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

  if (trash_spec.source !=  static_cast<cls::rbd::TrashImageSource>(source)) {
    lderr(cct) << "Current trash source: " << trash_spec.source
               << " does not match expected: "
               << static_cast<cls::rbd::TrashImageSource>(source) << dendl;
    return -EINVAL;
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

  ldout(cct, 2) << "adding rbd image to v2 directory..." << dendl;
  r = cls_client::dir_add_image(&io_ctx, RBD_DIRECTORY, image_name,
                                image_id);
  if (r < 0 && r != -EEXIST) {
    lderr(cct) << "error adding image to v2 directory: "
               << cpp_strerror(r) << dendl;
    return r;
  }

  r = enable_mirroring<I>(io_ctx, image_id);
  if (r < 0) {
    // not fatal -- ignore
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
