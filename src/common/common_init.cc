// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2010 Dreamhost
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include "auth/AuthSupported.h"
#include "auth/KeyRing.h"
#include "config.h"
#include "common/errno.h"
#include "include/color.h"
#include "tls.h"

static void set_no_logging_impl()
{
  free((void*)g_conf.log_file);
  g_conf.log_file = NULL;

  free((void*)g_conf.log_dir);
  g_conf.log_dir = NULL;

  free((void*)g_conf.log_sym_dir);
  g_conf.log_sym_dir = NULL;

  g_conf.log_sym_history = 0;

  g_conf.log_to_stderr = LOG_TO_STDERR_NONE;

  g_conf.log_to_syslog = false;

  g_conf.log_per_instance = false;

  g_conf.log_to_file = false;
}

/* Set no logging
 */
void set_no_logging()
{
  set_no_logging_impl();

  if (_dout_need_open) {
    Mutex::Locker l(_dout_lock);
    _dout_open_log(false);
  }
}

/* Set foreground logging
 *
 * Forces the process to log only to stderr, overriding whatever was in the ceph.conf.
 *
 * TODO: make this configurable by a command line switch or environment variable, if users want
 * an unusual logging setup for their foreground process.
 */
void set_foreground_logging()
{
  set_no_logging_impl();
  g_conf.log_to_stderr = LOG_TO_STDERR_ALL;

  if (_dout_need_open) {
    Mutex::Locker l(_dout_lock);
    _dout_open_log(false);
  }
}

void common_set_defaults(bool daemon)
{
  if (daemon) {
    cout << TEXT_YELLOW << " ** WARNING: Ceph is still under heavy development, and is only suitable for **" << TEXT_NORMAL << std::endl;
    cout << TEXT_YELLOW <<  " **          testing and review.  Do not trust it with important data.       **" << TEXT_NORMAL << std::endl;

    g_conf.daemonize = true;
    g_conf.logger = true;
  } else {
    g_conf.pid_file = 0;
  }
}

static void keyring_init(const char *filesearch)
{
  const char *filename = filesearch;
  string keyring_search = g_conf.keyring;
  string new_keyring;
  if (ceph_resolve_file_search(keyring_search, new_keyring)) {
    filename = new_keyring.c_str();
  }

  int ret = g_keyring.load(filename);
  if (ret) {
    derr << "keyring_init: failed to load " << filename << dendl;
    return;
  }

  if (g_conf.key && g_conf.key[0]) {
    string k = g_conf.key;
    EntityAuth ea;
    ea.key.decode_base64(k);
    g_keyring.add(*g_conf.entity_name, ea);
  } else if (g_conf.keyfile && g_conf.keyfile[0]) {
    char buf[100];
    int fd = ::open(g_conf.keyfile, O_RDONLY);
    if (fd < 0) {
      int err = errno;
      derr << "unable to open " << g_conf.keyfile << ": "
	   << cpp_strerror(err) << dendl;
      exit(1);
    }
    int len = ::read(fd, buf, sizeof(buf));
    if (len < 0) {
      int err = errno;
      derr << "unable to read key from " << g_conf.keyfile << ": "
	   << cpp_strerror(err) << dendl;
      exit(1);
    }
    ::close(fd);

    buf[len] = 0;
    string k = buf;
    EntityAuth ea;
    ea.key.decode_base64(k);
    g_keyring.add(*g_conf.entity_name, ea);
  }
}

void common_init(std::vector<const char*>& args, const char *module_type, bool init_keys)
{
  tls_init();
  tls_get_val()->disable_assert = 0;

  parse_startup_config_options(args, module_type);
  parse_config_options(args);

#ifdef HAVE_LIBTCMALLOC
  if (g_conf.tcmalloc_profiler_run && g_conf.tcmalloc_have) {
    char profile_name[PATH_MAX];
    sprintf(profile_name, "%s/%s", g_conf.log_dir, g_conf.name);
    char *val = new char[sizeof(int)*8+1];
    sprintf(val, "%i", g_conf.profiler_allocation_interval);
    setenv("HEAP_PROFILE_ALLOCATION_INTERVAL", val, g_conf.profiler_allocation_interval);
    sprintf(val, "%i", g_conf.profiler_highwater_interval);
    setenv("HEAP_PROFILE_INUSE_INTERVAL", val, g_conf.profiler_highwater_interval);
    generic_dout(0) << "turning on heap profiler with prefix " << profile_name << dendl;
    g_conf.profiler_start(profile_name);
  }
#endif //HAVE_LIBTCMALLOC

  if (init_keys && is_supported_auth(CEPH_AUTH_CEPHX))
    keyring_init(g_conf.keyring);
}

