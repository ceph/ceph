/*
 * Copyright (C) 2021 SUSE LINUX GmbH
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
*/

#pragma once

#define CEPH_DOKAN_IO_DEFAULT_TIMEOUT 60 * 5 // Seconds

// Avoid conflicting COM types, exposed when using C++.
#define _OLE2_H_

#include <bcrypt.h>  // for typedef of NTSTATUS
#include <dokan.h>

struct Config {
  bool removable = false;
  bool readonly = false;
  bool use_win_mount_mgr = false;
  bool current_session_only = false;
  bool debug = false;
  bool dokan_stderr = false;

  int operation_timeout = CEPH_DOKAN_IO_DEFAULT_TIMEOUT;

  std::wstring mountpoint = L"";
  std::string root_path = "/";

  std::wstring win_vol_name = L"";
  unsigned long win_vol_serial = 0;
  unsigned long max_path_len = 256;
  mode_t file_mode = 0755;
  mode_t dir_mode = 0755;

  bool case_sensitive = true;
  // Convert new file paths to upper case in case of case insensitive mounts.
  // Visual Studio recommends normalizing to uppercase in order to avoid
  // locale issues (CA1308).
  bool convert_to_uppercase = true;
  // TODO: consider adding an option to preserve the original case.
  // It could be stored using an extended attribute.
};

extern Config *g_cfg;

// TODO: list and service commands.
enum class Command {
  None,
  Version,
  Help,
  Map,
  Unmap,
};

void print_usage();
int parse_args(
  std::vector<const char*>& args,
  std::ostream *err_msg,
  Command *command, Config *cfg);
int set_dokan_options(Config *cfg, PDOKAN_OPTIONS dokan_options);
