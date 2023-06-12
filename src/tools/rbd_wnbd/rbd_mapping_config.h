/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2020 SUSE LINUX GmbH
 * Copyright (C) 2023 Cloudbase Solutions
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#pragma once

#include <string>

#include <wnbd.h>

#define SERVICE_REG_KEY "SYSTEM\\CurrentControlSet\\Services\\rbd-wnbd"

#define DEFAULT_SERVICE_START_TIMEOUT 120
#define DEFAULT_IMAGE_MAP_TIMEOUT 20
#define DEFAULT_SERVICE_THREAD_COUNT 8
#define DEFAULT_SOFT_REMOVE_TIMEOUT 15
#define DEFAULT_IO_WORKER_COUNT 4

#define RBD_WNBD_BLKSIZE 512UL

struct Config {
  bool exclusive = false;
  bool readonly = false;

  std::string parent_pipe;

  std::string poolname;
  std::string nsname;
  std::string imgname;
  std::string snapname;
  std::string devpath;

  std::string format;
  bool pretty_format = false;

  bool hard_disconnect = false;
  int soft_disconnect_timeout = DEFAULT_SOFT_REMOVE_TIMEOUT;
  bool hard_disconnect_fallback = true;

  int service_start_timeout = DEFAULT_SERVICE_START_TIMEOUT;
  int image_map_timeout = DEFAULT_IMAGE_MAP_TIMEOUT;
  bool remap_failure_fatal = false;
  bool adapter_monitoring_enabled = false;

  // TODO: consider moving those fields to a separate structure. Those
  // provide connection information without actually being configurable.
  // The disk number is provided by Windows.
  int disk_number = -1;
  int pid = 0;
  std::string serial_number;
  bool active = false;
  bool wnbd_mapped = false;
  std::string command_line;
  std::string admin_sock_path;

  WnbdLogLevel wnbd_log_level = WnbdLogLevelInfo;
  int io_req_workers = DEFAULT_IO_WORKER_COUNT;
  int io_reply_workers = DEFAULT_IO_WORKER_COUNT;
  int service_thread_count = DEFAULT_SERVICE_THREAD_COUNT;

  // register the mapping, recreating it when the Ceph service starts.
  bool persistent = true;
};

int construct_devpath_if_missing(Config* cfg);
int save_config_to_registry(Config* cfg, std::string command_line);
int remove_config_from_registry(Config* cfg);
int load_mapping_config_from_registry(std::string devpath, Config* cfg);
