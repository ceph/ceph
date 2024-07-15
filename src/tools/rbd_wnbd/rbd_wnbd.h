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

#include "rbd_mapping_config.h"
#include "rbd_mapping.h"

#define SERVICE_PIPE_NAME "\\\\.\\pipe\\rbd-wnbd"
#define SERVICE_PIPE_TIMEOUT_MS 5000
#define SERVICE_PIPE_BUFFSZ 4096

#define HELP_INFO 1
#define VERSION_INFO 2

#define WNBD_STATUS_ACTIVE "active"
#define WNBD_STATUS_INACTIVE "inactive"

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

int restart_registered_mappings(
  int worker_count, int total_timeout, int image_map_timeout);
int map_device_using_same_process(std::string command_line);

BOOL WINAPI console_handler_routine(DWORD dwCtrlType);

static int parse_args(std::vector<const char*>& args,
                      std::ostream *err_msg,
                      Command *command, Config *cfg);
static int do_map(Config *cfg);
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
