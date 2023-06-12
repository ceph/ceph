/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2023 Cloudbase Solutions
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include "rbd_mapping.h"

#include "common/debug.h"
#include "common/dout.h"
#include "common/errno.h"

#include "global/global_init.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "rbd-wnbd: "


int RbdMapping::init()
{
  librbd::image_info_t info;

  int r = rados.ioctx_create(cfg.poolname.c_str(), io_ctx);
  if (r < 0) {
    derr << "rbd-wnbd: couldn't create IO context: " << cpp_strerror(r)
         << dendl;
    return r;
  }

  io_ctx.set_namespace(cfg.nsname);

  r = rbd.open(io_ctx, image, cfg.imgname.c_str());
  if (r < 0) {
    derr << "rbd-wnbd: couldn't open rbd image: " << cpp_strerror(r)
         << dendl;
    return r;
  }

  if (cfg.exclusive) {
    r = image.lock_acquire(RBD_LOCK_MODE_EXCLUSIVE);
    if (r < 0) {
      derr << "rbd-wnbd: failed to acquire exclusive lock: " << cpp_strerror(r)
           << dendl;
      return r;
    }
  }

  if (!cfg.snapname.empty()) {
    r = image.snap_set(cfg.snapname.c_str());
    if (r < 0) {
      derr << "rbd-wnbd: couldn't use snapshot: " << cpp_strerror(r)
         << dendl;
      return r;
    }
  }

  r = image.stat(info, sizeof(info));
  if (r < 0)
    return r;

  initial_image_size = info.size;

  // We're storing mapping details in the registry even for non-persistent
  // mappings. This allows us to easily retrieve mapping details such
  // as the rbd pool or admin socket path.
  // We're cleaning up the registry entry when the non-persistent mapping
  // gets disconnected or when the ceph service restarts.
  r = save_config_to_registry(&cfg, command_line);
  if (r < 0)
    return r;

  handler = new WnbdHandler(image, cfg.devpath,
                            info.size / RBD_WNBD_BLKSIZE,
                            RBD_WNBD_BLKSIZE,
                            !cfg.snapname.empty() || cfg.readonly,
                            g_conf().get_val<bool>("rbd_cache"),
                            cfg.io_req_workers,
                            cfg.io_reply_workers);
  return 0;
}

void RbdMapping::shutdown()
{
  std::unique_lock l{shutdown_lock};

  int r = 0;
  if (!cfg.persistent) {
    dout(5) << __func__ << ": cleaning up non-persistent mapping: "
            << cfg.devpath << dendl;
    r = remove_config_from_registry(&cfg);
    if (r) {
      derr << __func__ << ": could not clean up non-persistent mapping: "
           << cfg.devpath << ". Error: " << cpp_strerror(r) << dendl;
    }
  }

  if (watch_ctx) {
    r = image.update_unwatch(watch_handle);
    if (r < 0) {
      derr << __func__ << ": update_unwatch failed with error: "
           << cpp_strerror(r) << dendl;
    }
    delete watch_ctx;
    watch_ctx = nullptr;
  }

  if (handler) {
    handler->shutdown();
    delete handler;
    handler = nullptr;
  }

  image.close();
  io_ctx.close();
  rados.shutdown();
}

int RbdMapping::start()
{
  int r = init();
  if (r < 0) {
    return r;
  }

  r = handler->start();
  if (r) {
    return r == ERROR_ALREADY_EXISTS ? -EEXIST : -EINVAL;
  }

  watch_ctx = new WNBDWatchCtx(io_ctx, handler, image, initial_image_size);
  r = image.update_watch(watch_ctx, &watch_handle);
  if (r < 0) {
    derr << __func__ << ": update_watch failed with error: "
         << cpp_strerror(r) << dendl;
    return r;
  }

  // We're informing the parent processes that the initialization
  // was successful.
  int err = 0;
  if (!cfg.parent_pipe.empty()) {
    HANDLE parent_pipe_handle = CreateFile(
      cfg.parent_pipe.c_str(), GENERIC_WRITE, 0, NULL,
      OPEN_EXISTING, 0, NULL);
    if (parent_pipe_handle == INVALID_HANDLE_VALUE) {
      err = GetLastError();
      derr << "Could not open parent pipe: " << win32_strerror(err) << dendl;
    } else if (!WriteFile(parent_pipe_handle, "a", 1, NULL, NULL)) {
      // TODO: consider exiting in this case. The parent didn't wait for us,
      // maybe it was killed after a timeout.
      err = GetLastError();
      derr << "Failed to communicate with the parent: "
           << win32_strerror(err) << dendl;
    } else {
      dout(5) << __func__ << ": submitted parent notification." << dendl;
    }

    if (parent_pipe_handle != INVALID_HANDLE_VALUE)
      CloseHandle(parent_pipe_handle);

    global_init_postfork_finish(g_ceph_context);
  }

  return 0;
}

int RbdMapping::wait() {
  if (handler) {
    return handler->wait();
  }
  return 0;
}
