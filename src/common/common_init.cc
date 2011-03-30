// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2010-2011 Dreamhost
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
#include "common/ceph_crypto.h"
#include "include/color.h"

#include <errno.h>
#include <syslog.h>

int keyring_init(md_config_t *conf)
{
  if (!is_supported_auth(CEPH_AUTH_CEPHX))
    return 0;

  int ret = 0;
  string filename;
  if (ceph_resolve_file_search(conf->keyring, filename)) {
    ret = g_keyring.load(filename);
  }

  if (!conf->key.empty()) {
    EntityAuth ea;
    ea.key.decode_base64(conf->key);
    g_keyring.add(*conf->name, ea);

    ret = 0;
  } else if (!conf->keyfile.empty()) {
    char buf[100];
    int fd = ::open(conf->keyfile.c_str(), O_RDONLY);
    if (fd < 0) {
      int err = errno;
      derr << "unable to open " << conf->keyfile << ": "
	   << cpp_strerror(err) << dendl;
      ceph_abort();
    }
    memset(buf, 0, sizeof(buf));
    int len = safe_read(fd, buf, sizeof(buf) - 1);
    if (len < 0) {
      derr << "unable to read key from " << conf->keyfile << ": "
	   << cpp_strerror(len) << dendl;
      TEMP_FAILURE_RETRY(::close(fd));
      ceph_abort();
    }
    TEMP_FAILURE_RETRY(::close(fd));

    buf[len] = 0;
    string k = buf;
    EntityAuth ea;
    ea.key.decode_base64(k);

    g_keyring.add(*conf->name, ea);

    ret = 0;
  }

  if (ret)
    derr << "keyring_init: failed to load " << filename << dendl;
  return ret;
}

md_config_t *common_preinit(const CephInitParameters &iparams,
			  enum code_environment_t code_env, int flags)
{
  // set code environment
  g_code_env = code_env;

  // Create a configuration object
  // TODO: de-globalize
  md_config_t *conf = &g_conf; //new md_config_t();

  // Set up our entity name.
  conf->name = new EntityName(iparams.name);

  // Set some defaults based on code type
  switch (code_env) {
    case CODE_ENVIRONMENT_DAEMON:
      conf->daemonize = true;
      if (!(flags & CINIT_FLAG_UNPRIVILEGED_DAEMON_DEFAULTS)) {
	conf->log_dir = "/var/log/ceph";
	conf->pid_file = "/var/run/ceph/$type.$id.pid";
      }
      conf->log_to_stderr = LOG_TO_STDERR_SOME;
      break;
    default:
      conf->daemonize = false;
      break;
  }

  return conf;
}

void common_init(std::vector < const char* >& args,
	       uint32_t module_type, code_environment_t code_env, int flags)
{
  CephInitParameters iparams =
    ceph_argparse_early_args(args, module_type, flags);
  md_config_t *conf = common_preinit(iparams, code_env, flags);

  int ret = conf->parse_config_files(iparams.get_conf_files());
  if (ret == -EDOM) {
    derr << "common_init: error parsing config file." << dendl;
    _exit(1);
  }
  else if (ret == -EINVAL) {
    if (!(flags & CINIT_FLAG_NO_DEFAULT_CONFIG_FILE)) {
      derr << "common_init: unable to open config file." << dendl;
      _exit(1);
    }
  }
  else if (ret) {
    derr << "common_init: error reading config file." << dendl;
    _exit(1);
  }

  conf->parse_env(); // environment variables override

  conf->parse_argv(args); // argv override

  if (code_env != CODE_ENVIRONMENT_DAEMON) {
    // The symlink stuff only really makes sense for daemons
    conf->log_per_instance = false;
  }

  if (conf->log_to_syslog || conf->clog_to_syslog) {
    closelog();
    openlog(g_conf.name->to_cstr(), LOG_ODELAY | LOG_PID, LOG_USER);
  }

  {
    // Force a reopen of dout() with the configuration we have just read.
    DoutLocker _dout_locker;
    _dout_open_log();
  }

  // signal stuff
  int siglist[] = { SIGPIPE, 0 };
  block_signals(NULL, siglist);
  install_standard_sighandlers();

  if (code_env == CODE_ENVIRONMENT_DAEMON) {
    cout << TEXT_YELLOW << " ** WARNING: Ceph is still under heavy development, "
	 << "and is only suitable for **" << TEXT_NORMAL << std::endl;
    cout << TEXT_YELLOW <<  " **          testing and review.  Do not trust it "
	 << "with important data.       **" << TEXT_NORMAL << std::endl;
  }

  // this probably belongs somewhere else...
  buffer_track_alloc = g_conf.buffer_track_alloc;

  ceph::crypto::init();
}
