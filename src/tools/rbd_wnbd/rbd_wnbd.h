/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2020 SUSE LINUX GmbH
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef RBD_WNBD_H
#define RBD_WNBD_H

#include <string.h>
#include <iostream>
#include <vector>

#include "include/compat.h"
#include "common/win32/registry.h"

#include "wnbd_handler.h"

#define SERVICE_REG_KEY "SYSTEM\\CurrentControlSet\\Services\\rbd-wnbd"
#define SERVICE_PIPE_NAME "\\\\.\\pipe\\rbd-wnbd"
#define SERVICE_PIPE_TIMEOUT_MS 5000
#define SERVICE_PIPE_BUFFSZ 4096

#define DEFAULT_MAP_TIMEOUT_MS 30000

#define RBD_WNBD_BLKSIZE 512UL

#define DEFAULT_SERVICE_START_TIMEOUT 120
#define DEFAULT_IMAGE_MAP_TIMEOUT 20
#define DISK_STATUS_POLLING_INTERVAL_MS 500

#define HELP_INFO 1
#define VERSION_INFO 2

#define WNBD_STATUS_ACTIVE "active"
#define WNBD_STATUS_INACTIVE "inactive"

#define DEFAULT_SERVICE_THREAD_COUNT 8

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

enum Command {
  None,
  Connect,
  Disconnect,
  List,
  Show,
  Service,
  Stats
};

typedef struct {
  Command command;
  BYTE arguments[1];
} ServiceRequest;

typedef struct {
  int status;
} ServiceReply;

bool is_process_running(DWORD pid);
void unmap_at_exit();

int disconnect_all_mappings(
  bool unregister,
  bool hard_disconnect,
  int soft_disconnect_timeout,
  int worker_count);
int restart_registered_mappings(
  int worker_count, int total_timeout, int image_map_timeout);
int map_device_using_suprocess(std::string command_line);

int construct_devpath_if_missing(Config* cfg);
int save_config_to_registry(Config* cfg);
int remove_config_from_registry(Config* cfg);
int load_mapping_config_from_registry(std::string devpath, Config* cfg);

BOOL WINAPI console_handler_routine(DWORD dwCtrlType);

static int parse_args(std::vector<const char*>& args,
                      std::ostream *err_msg,
                      Command *command, Config *cfg);
static int do_unmap(Config *cfg, bool unregister);


class BaseIterator {
  public:
    virtual ~BaseIterator() {};
    virtual bool get(Config *cfg) = 0;

    int get_error() {
      return error;
    }
  protected:
    int error = 0;
    int index = -1;
};

// Iterate over mapped devices, retrieving info from the driver.
class WNBDActiveDiskIterator : public BaseIterator {
  public:
    WNBDActiveDiskIterator();
    ~WNBDActiveDiskIterator();

    bool get(Config *cfg);

  private:
    PWNBD_CONNECTION_LIST conn_list = NULL;

    static DWORD fetch_list(PWNBD_CONNECTION_LIST* conn_list);
};


// Iterate over the Windows registry key, retrieving registered mappings.
class RegistryDiskIterator : public BaseIterator {
  public:
    RegistryDiskIterator();
    ~RegistryDiskIterator() {
      delete reg_key;
    }

    bool get(Config *cfg);
  private:
    DWORD subkey_count = 0;
    char subkey_name[MAX_PATH];

  RegistryKey* reg_key = NULL;
};

// Iterate over all RBD mappings, getting info from the registry and driver.
class WNBDDiskIterator : public BaseIterator {
  public:
    bool get(Config *cfg);

  private:
    // We'll keep track of the active devices.
    std::set<std::string> active_devices;

    WNBDActiveDiskIterator active_iterator;
    RegistryDiskIterator registry_iterator;
};

#endif // RBD_WNBD_H
