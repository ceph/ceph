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

#define DISK_STATUS_POLLING_INTERVAL_MS 500


int RbdMapping::init()
{
  librbd::image_info_t info;

  rados = client_cache.get_client(cfg.entity_name, cfg.cluster_name);
  if (!rados) {
    return -EINVAL;
  }

  int r = rados->ioctx_create(cfg.poolname.c_str(), io_ctx);
  if (r < 0) {
    derr << "rbd-wnbd: couldn't create IO context: " << cpp_strerror(r)
         << ". Pool name: " << cfg.poolname
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

  CephContext* cct = reinterpret_cast<CephContext*>(io_ctx.cct());
  ceph_assert(cct != nullptr);

  handler = new WnbdHandler(image, cfg.devpath,
                            info.size / RBD_WNBD_BLKSIZE,
                            RBD_WNBD_BLKSIZE,
                            !cfg.snapname.empty() || cfg.readonly,
                            g_conf().get_val<bool>("rbd_cache"),
                            cfg.io_req_workers,
                            cfg.io_reply_workers,
                            cct->get_admin_socket());
  return 0;
}

void RbdMapping::shutdown()
{
  std::unique_lock l{shutdown_lock};

  dout(5) << __func__ << ": removing RBD mapping: " << cfg.devpath << dendl;

  int r = 0;
  if (!cfg.persistent && saved_cfg_to_registry) {
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
}

int RbdMapping::start()
{
  dout(10) << "initializing mapping" << dendl;
  int r = init();
  if (r < 0) {
    return r;
  }

  dout(10) << "starting wnbd handler" << dendl;
  r = handler->start();
  if (r) {
    return r == ERROR_ALREADY_EXISTS ? -EEXIST : -EINVAL;
  }

  dout(10) << "setting up watcher" << dendl;
  watch_ctx = new WNBDWatchCtx(io_ctx, handler, image, initial_image_size);
  r = image.update_watch(watch_ctx, &watch_handle);
  if (r < 0) {
    derr << __func__ << ": update_watch failed with error: "
         << cpp_strerror(r) << dendl;
    return r;
  }

  // Wait for the mapped disk to become available.
  r = wait_mapped_disk(cfg);
  if (r < 0) {
    return r;
  }

  // We're storing mapping details in the registry even for non-persistent
  // mappings. This allows us to easily retrieve mapping details such
  // as the rbd pool or admin socket path.
  // We're cleaning up the registry entry when the non-persistent mapping
  // gets disconnected or when the ceph service restarts.
  r = save_config_to_registry(&cfg);
  if (r < 0) {
    return r;
  } else {
    saved_cfg_to_registry = true;
  }

  if (disconnect_cbk) {
    monitor_thread = std::thread([this]{
      int ret = this->wait();
      // Allow "this" to be destroyed by the disconnect callback.
      this->monitor_thread.detach();
      dout(5) << "finished waiting for: " << this->cfg.devpath
              << ", ret: " << ret << dendl;
      disconnect_cbk(this->cfg.devpath, ret);
    });
  }

  return 0;
}

// Wait until the image gets disconnected.
int RbdMapping::wait()
{
  if (handler) {
    return handler->wait();
  }
  return 0;
}

RbdMapping::~RbdMapping()
{
  dout(10) << __func__ << ": cleaning up rbd mapping: "
           << cfg.devpath << dendl;
  shutdown();
}

// Wait for the mapped disk to become available.
int wait_mapped_disk(Config& cfg)
{
  DWORD status = WnbdPollDiskNumber(
    cfg.devpath.c_str(),
    TRUE, // ExpectMapped
    TRUE, // TryOpen
    cfg.image_map_timeout * 1000,
    DISK_STATUS_POLLING_INTERVAL_MS,
    (PDWORD) &cfg.disk_number);
  if (status) {
    derr << "WNBD disk unavailable, error: "
         << win32_strerror(status) << dendl;
    return -EINVAL;
  }
  dout(0) << "Successfully mapped image: " << cfg.devpath
          << ". Windows disk path: "
          << "\\\\.\\PhysicalDrive" + std::to_string(cfg.disk_number)
          << dendl;
  return 0;
}

int RbdMappingDispatcher::create(Config& cfg)
{
  if (cfg.devpath.empty()) {
    derr << "missing device identifier" << dendl;
    return -EINVAL;
  }

  if (get_mapping(cfg.devpath)) {
    derr << "already mapped: " << cfg.devpath << dendl;
    return -EEXIST;
  }

  if (stop_requested) {
    derr << "service stop requested, refusing to create new mapping."
         << dendl;
    return -ESHUTDOWN;
  }

  auto rbd_mapping = std::make_shared<RbdMapping>(
    cfg, client_cache,
    std::bind(
      &RbdMappingDispatcher::disconnect_cbk,
      this,
      std::placeholders::_1,
      std::placeholders::_2));

  int r = rbd_mapping.get()->start();
  if (!r) {
    std::unique_lock l{map_mutex};
    mappings.insert(std::make_pair(cfg.devpath, rbd_mapping));
  }
  return r;
}

std::shared_ptr<RbdMapping> RbdMappingDispatcher::get_mapping(
  std::string& devpath)
{
  std::unique_lock l{map_mutex};

  auto mapping_it = mappings.find(devpath);
  if (mapping_it == mappings.end()) {
    // not found
    return std::shared_ptr<RbdMapping>();
  } else {
    return mapping_it->second;
  }
}

void RbdMappingDispatcher::disconnect_cbk(std::string devpath, int ret)
{
  dout(10) << "RbdMappingDispatcher: cleaning up stopped mapping" << dendl;
  if (ret) {
    derr << "rbd mapping wait error: " << ret
         << ", allowing cleanup to proceed"
         << dendl;
  }

  auto mapping = get_mapping(devpath);
  if (mapping) {
    // This step can be fairly time consuming, especially when
    // cumulated. For this reason, we'll ensure that multiple mappings
    // can be cleaned up simultaneously.
    mapping->shutdown();

    std::unique_lock l{map_mutex};
    mappings.erase(devpath);
  }
}

int RbdMappingDispatcher::stop(
  bool hard_disconnect,
  int soft_disconnect_timeout,
  int worker_count)
{
  stop_requested = true;

  // Although not generally recommended, soft_disconnect_timeout can be 0,
  // which means infinite timeout.
  ceph_assert(soft_disconnect_timeout >= 0);
  ceph_assert(worker_count > 0);

  std::atomic<int> err = 0;

  boost::asio::thread_pool pool(worker_count);
  LARGE_INTEGER start_t, counter_freq;
  QueryPerformanceFrequency(&counter_freq);
  QueryPerformanceCounter(&start_t);

  // We're initiating the disk removal through libwnbd, which uses PNP
  // to notify the storage stack that the disk is going to be removed
  // and waits for pending operations to complete.
  //
  // Once ready, the WNBD driver notifies rbd-wnbd that the disk has been
  // disconnected.
  auto mapped_devpaths = get_mapped_devpaths();
  for (const auto& devpath: mapped_devpaths) {
    boost::asio::post(pool,
      [devpath, start_t, counter_freq, soft_disconnect_timeout,
       hard_disconnect, &err]()
      {
        LARGE_INTEGER curr_t, elapsed_ms;
        QueryPerformanceCounter(&curr_t);
        elapsed_ms.QuadPart = curr_t.QuadPart - start_t.QuadPart;
        elapsed_ms.QuadPart *= 1000;
        elapsed_ms.QuadPart /= counter_freq.QuadPart;

        int64_t time_left_ms = std::max(
          (int64_t)0,
          soft_disconnect_timeout * 1000 - elapsed_ms.QuadPart);

        dout(1) << "Removing mapping: " << devpath
                << ". Timeout: " << time_left_ms
                << "ms. Hard disconnect: " << hard_disconnect
                << dendl;

        WNBD_REMOVE_OPTIONS remove_options = {0};
        remove_options.Flags.HardRemove = hard_disconnect || !time_left_ms;
        remove_options.Flags.HardRemoveFallback = true;
        remove_options.SoftRemoveTimeoutMs = time_left_ms;
        remove_options.SoftRemoveRetryIntervalMs = SOFT_REMOVE_RETRY_INTERVAL * 1000;

        // This is asynchronous, it may take a few seconds for the disk to be
        // removed. We'll perform the wait outside the loop to speed up the
        // process.
        int r = WnbdRemoveEx(devpath.c_str(), &remove_options);
        if (r && r != ERROR_FILE_NOT_FOUND) {
          err = -EINVAL;
          derr << "Could not initiate mapping removal: " << devpath
               << ". Error: " << win32_strerror(r) << dendl;
        } else {
          dout(1) << "Successfully initiated mapping removal: " << devpath << dendl;
        }
      });
  }
  pool.join();

  if (err) {
    derr << "Could not initiate removal of all mappings. Error: " << err << dendl;
    return err;
  }

  // Wait for the cleanup to complete on the rbd-wnbd service side.
  return wait_for_mappings_removal(10000);
}

std::vector<std::string> RbdMappingDispatcher::get_mapped_devpaths() {
  std::vector<std::string> out;
  std::unique_lock l{map_mutex};

  for (auto it = mappings.begin(); it != mappings.end(); it++) {
    out.push_back(it->first);
  }

  return out;
}

int RbdMappingDispatcher::get_mappings_count() {
  std::unique_lock l{map_mutex};
  return mappings.size();
}

int RbdMappingDispatcher::wait_for_mappings_removal(int timeout_ms) {
  LARGE_INTEGER start_t, counter_freq;
  QueryPerformanceFrequency(&counter_freq);
  QueryPerformanceCounter(&start_t);

  int time_left_ms = timeout_ms;
  int mappings_count = get_mappings_count();

  while (mappings_count && time_left_ms > 0) {
    dout(1) << "Waiting for " << mappings_count << " mapping(s) to be removed. "
            << "Time left: " << time_left_ms << "ms." << dendl;
    Sleep(2000);

    LARGE_INTEGER curr_t, elapsed_ms;
    QueryPerformanceCounter(&curr_t);
    elapsed_ms.QuadPart = curr_t.QuadPart - start_t.QuadPart;
    elapsed_ms.QuadPart *= 1000;
    elapsed_ms.QuadPart /= counter_freq.QuadPart;

    time_left_ms = timeout_ms - elapsed_ms.QuadPart;
    mappings_count = get_mappings_count();
  }

  if (mappings_count) {
    derr << "Timed out waiting for disk mappings to be removed. "
         << "Remaining mapping(s): " << mappings_count << dendl;
    return -ETIMEDOUT;
  } else {
    dout(1) << "Successfully removed all mappings." << dendl;
  }

  return 0;
}
