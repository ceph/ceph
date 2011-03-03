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
#include "common/ceph_argparse.h"
#include "common/code_environment.h"
#include "common/safe_io.h"
#include "common/signal.h"
#include "common/version.h"
#include "common/config.h"
#include "common/common_init.h"
#include "common/errno.h"
#include "include/color.h"

#include <syslog.h>

#ifdef HAVE_PROFILER
# include <google/profiler.h>
#endif

/* Set foreground logging
 *
 * Forces the process to log only to stderr, overriding whatever was in the ceph.conf.
 *
 * TODO: make this configurable by a command line switch or environment variable, if users want
 * an unusual logging setup for their foreground process.
 */
void set_foreground_logging()
{
  free((void*)g_conf.log_file);
  g_conf.log_file = NULL;

  free((void*)g_conf.log_dir);
  g_conf.log_dir = NULL;

  free((void*)g_conf.log_sym_dir);
  g_conf.log_sym_dir = NULL;

  g_conf.log_sym_history = 0;

  g_conf.log_to_stderr = LOG_TO_STDERR_ALL;

  g_conf.log_to_syslog = false;

  g_conf.log_per_instance = false;
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

  if (g_conf.key && g_conf.key[0]) {
    string k = g_conf.key;
    EntityAuth ea;
    ea.key.decode_base64(k);
    g_keyring.add(*g_conf.entity_name, ea);

    ret = 0;
  } else if (g_conf.keyfile && g_conf.keyfile[0]) {
    char buf[100];
    int fd = ::open(g_conf.keyfile, O_RDONLY);
    if (fd < 0) {
      int err = errno;
      derr << "unable to open " << g_conf.keyfile << ": "
	   << cpp_strerror(err) << dendl;
      ceph_abort();
    }
    memset(buf, 0, sizeof(buf));
    int len = safe_read(fd, buf, sizeof(buf) - 1);
    if (len < 0) {
      derr << "unable to read key from " << g_conf.keyfile << ": "
	   << cpp_strerror(len) << dendl;
      TEMP_FAILURE_RETRY(::close(fd));
      ceph_abort();
    }
    TEMP_FAILURE_RETRY(::close(fd));

    buf[len] = 0;
    string k = buf;
    EntityAuth ea;
    ea.key.decode_base64(k);
    g_keyring.add(*g_conf.entity_name, ea);

    ret = 0;
  }

  if (ret) {
    derr << "keyring_init: failed to load " << filename << dendl;
    return;
  }
}

static void set_cv(const char ** key, const char * const val)
{
  free((void*)*key);
  *key = strdup(val);
}

void common_init(std::vector<const char*>& args, const char *module_type, int flags)
{
  bool force_fg_logging = false;

  // TODO: make callers specify code env explicitly.
  if (flags & STARTUP_FLAG_DAEMON)
    g_code_env = CODE_ENVIRONMENT_DAEMON;
  else if (flags & STARTUP_FLAG_LIBRARY)
    g_code_env = CODE_ENVIRONMENT_LIBRARY;
  else
    g_code_env = CODE_ENVIRONMENT_UTILITY;

  if (g_code_env == CODE_ENVIRONMENT_DAEMON) {
    cout << TEXT_YELLOW << " ** WARNING: Ceph is still under heavy development, "
         << "and is only suitable for **" << TEXT_NORMAL << std::endl;
    cout << TEXT_YELLOW <<  " **          testing and review.  Do not trust it "
         << "with important data.       **" << TEXT_NORMAL << std::endl;

    // some daemon-specific defaults
    g_conf.daemonize = true;
    g_conf.log_to_stderr = LOG_TO_STDERR_SOME;
    set_cv(&g_conf.log_dir, "/var/log/ceph");
    set_cv(&g_conf.pid_file, "/var/run/ceph/$type.$id.pid");

    // block SIGPIPE
    int siglist[] = { SIGPIPE, 0 };
    block_signals(NULL, siglist);
  }
  else {
    g_conf.pid_file = 0;
  }

  parse_startup_config_options(args, module_type, flags, &force_fg_logging);

  if (g_conf.log_to_syslog || g_conf.clog_to_syslog) {
    closelog();
    // It's ok if g_conf.name is NULL here.
    openlog(g_conf.name, LOG_ODELAY | LOG_PID, LOG_USER);
  }

  if (force_fg_logging)
    set_foreground_logging();

#ifdef HAVE_PROFILER
  /*
   * We need to call _something_ in libprofile.so so that the
   * --as-needed stuff doesn't drop it from our .so dependencies.
   * This is basically a no-op.
   */
  ProfilerFlush();
#endif

  parse_config_options(args);

  {
    // In the long term, it would be best to ensure that we read ceph.conf
    // before initializing dout(). For now, just force a reopen here with the
    // configuration we have just read.
    DoutLocker _dout_locker;
    _dout_open_log();
  }

  install_standard_sighandlers();

  if (flags & STARTUP_FLAG_INIT_KEYS)  {
    if (is_supported_auth(CEPH_AUTH_CEPHX))
      keyring_init(g_conf.keyring);
  }
}

