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
#include "common/DoutStreambuf.h"
#include "common/Thread.h"
#include "common/ceph_argparse.h"
#include "common/ceph_crypto.h"
#include "common/code_environment.h"
#include "common/common_init.h"
#include "common/config.h"
#include "common/errno.h"
#include "common/pidfile.h"
#include "common/safe_io.h"
#include "common/signal.h"
#include "common/version.h"
#include "include/color.h"
#include "common/Thread.h"
#include "common/pidfile.h"

#include <errno.h>
#include <deque>
#include <syslog.h>

#define _STR(x) #x
#define STRINGIFY(x) _STR(x)

int keyring_init(CephContext *cct)
{
  md_config_t *conf = cct->_conf;

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
    g_keyring.add(conf->name, ea);

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

    g_keyring.add(conf->name, ea);

    ret = 0;
  }

  if (ret)
    derr << "keyring_init: failed to load " << filename << dendl;
  return ret;
}

CephContext *common_preinit(const CephInitParameters &iparams,
			  enum code_environment_t code_env, int flags)
{
  // set code environment
  g_code_env = code_env;

  // Create a configuration object
  // TODO: de-globalize
  CephContext *cct = &g_ceph_context; //new CephContext();
  md_config_t *conf = cct->_conf;
  // add config observers here

  // Set up our entity name.
  conf->name = iparams.name;

  // Set some defaults based on code type
  switch (code_env) {
    case CODE_ENVIRONMENT_DAEMON:
      conf->daemonize = true;
      if (!(flags & CINIT_FLAG_UNPRIVILEGED_DAEMON_DEFAULTS)) {
	conf->set_val_or_die("pid_file", "/var/run/ceph/$type.$id.pid");
      }
      conf->set_val_or_die("log_to_stderr", STRINGIFY(LOG_TO_STDERR_SOME));
      break;
    default:
      conf->set_val_or_die("daemonize", "false");
      break;
  }
  return cct;
}

void complain_about_parse_errors(std::deque<std::string> *parse_errors)
{
  if (parse_errors->empty())
    return;
  derr << "Errors while parsing config file!" << dendl;
  int cur_err = 0;
  static const int MAX_PARSE_ERRORS = 20;
  for (std::deque<std::string>::const_iterator p = parse_errors->begin();
       p != parse_errors->end(); ++p)
  {
    derr << *p << dendl;
    if (cur_err == MAX_PARSE_ERRORS) {
      derr << "Suppressed " << (parse_errors->size() - MAX_PARSE_ERRORS)
	   << " more errors." << dendl;
      break;
    }
    ++cur_err;
  }
}

void common_init(std::vector < const char* >& args,
	       uint32_t module_type, code_environment_t code_env, int flags)
{
  CephInitParameters iparams =
    ceph_argparse_early_args(args, module_type, flags);
  CephContext *cct = common_preinit(iparams, code_env, flags);
  md_config_t *conf = cct->_conf;

  std::deque<std::string> parse_errors;
  int ret = conf->parse_config_files(iparams.get_conf_files(), &parse_errors);
  if (ret == -EDOM) {
    dout_emergency("common_init: error parsing config file.\n");
    _exit(1);
  }
  else if (ret == -EINVAL) {
    if (!(flags & CINIT_FLAG_NO_DEFAULT_CONFIG_FILE)) {
      dout_emergency("common_init: unable to open config file.\n");
      _exit(1);
    }
  }
  else if (ret) {
    dout_emergency("common_init: error reading config file.\n");
    _exit(1);
  }

  conf->parse_env(); // environment variables override

  conf->parse_argv(args); // argv override

  if (code_env == CODE_ENVIRONMENT_DAEMON) {
    if (conf->log_dir.empty() && conf->log_file.empty()) {
	conf->set_val_or_die("log_file", "/var/log/ceph/$name.log");
    }
  }

  // Expand metavariables. Invoke configuration observers.
  conf->apply_changes();

  // Now we're ready to complain about config file parse errors
  complain_about_parse_errors(&parse_errors);

  // signal stuff
  int siglist[] = { SIGPIPE, 0 };
  block_signals(NULL, siglist);
  install_standard_sighandlers();

  if (code_env == CODE_ENVIRONMENT_DAEMON) {
    cout << TEXT_YELLOW
	 << " ** WARNING: Ceph is still under development.  Any feedback can be directed  **"
	 << TEXT_NORMAL << "\n" << TEXT_YELLOW
	 << " **          at ceph-devel@vger.kernel.org or http://ceph.newdream.net/.     **"
	 << TEXT_NORMAL << std::endl;
    output_ceph_version();
  }
}

static void pidfile_remove_void(void)
{
  pidfile_remove();
}

/* Map stderr to /dev/null. This isn't really re-entrant; we rely on the old unix
 * behavior that the file descriptor that gets assigned is the lowest
 * available one.
 */
int common_init_shutdown_stderr(void)
{
  TEMP_FAILURE_RETRY(close(STDERR_FILENO));
  if (open("/dev/null", O_RDONLY) < 0) {
    int err = errno;
    derr << "common_init_shutdown_stderr: open(/dev/null) failed: error "
	 << err << dendl;
    return 1;
  }
  g_ceph_context._doss->handle_stderr_shutdown();
  return 0;
}

void common_init_daemonize(const CephContext *cct, int flags)
{
  if (g_code_env != CODE_ENVIRONMENT_DAEMON)
    return;
  const md_config_t *conf = cct->_conf;
  if (!conf->daemonize)
    return;
  int num_threads = Thread::get_num_threads();
  if (num_threads > 1) {
    derr << "common_init_daemonize: BUG: there are " << num_threads - 1
	 << " child threads already started that will now die!" << dendl;
    exit(1);
  }

  int ret = daemon(1, 1);
  if (ret) {
    ret = errno;
    derr << "common_init_daemonize: BUG: daemon error: "
	 << cpp_strerror(ret) << dendl;
    exit(1);
  }

  if (!conf->chdir.empty()) {
    if (::chdir(conf->chdir.c_str())) {
      int err = errno;
      derr << "common_init_daemonize: failed to chdir to directory: '"
	   << conf->chdir << "': " << cpp_strerror(err) << dendl;
    }
  }

  if (atexit(pidfile_remove_void)) {
    derr << "common_init_daemonize: failed to set pidfile_remove function "
	 << "to run at exit." << dendl;
  }

  /* This is the old trick where we make file descriptors 0, 1, and possibly 2
   * point to /dev/null.
   *
   * We have to do this because otherwise some arbitrary call to open() later
   * in the program might get back one of these file descriptors. It's hard to
   * guarantee that nobody ever writes to stdout, even though they're not
   * supposed to.
   */
  TEMP_FAILURE_RETRY(close(STDIN_FILENO));
  if (open("/dev/null", O_RDONLY) < 0) {
    int err = errno;
    derr << "common_init_daemonize: open(/dev/null) failed: error "
	 << err << dendl;
    exit(1);
  }
  TEMP_FAILURE_RETRY(close(STDOUT_FILENO));
  if (open("/dev/null", O_RDONLY) < 0) {
    int err = errno;
    derr << "common_init_daemonize: open(/dev/null) failed: error "
	 << err << dendl;
    exit(1);
  }
  if (!(flags & CINIT_FLAG_NO_DEFAULT_CONFIG_FILE)) {
    ret = common_init_shutdown_stderr();
    if (ret) {
      derr << "common_init_daemonize: common_init_shutdown_stderr failed with "
	   << "error code " << ret << dendl;
      exit(1);
    }
  }
  pidfile_write(&g_conf);
  ret = g_ceph_context._doss->handle_pid_change(&g_conf);
  if (ret) {
    derr << "common_init_daemonize: _doss->handle_pid_change failed with "
	 << "error code " << ret << dendl;
    exit(1);
  }
  dout(1) << "finished common_init_daemonize" << dendl;
}

/* Please be sure that this can safely be called multiple times by the
 * same application. */
void common_init_finish(CephContext *cct)
{
  ceph::crypto::init();
  keyring_init(cct);
  cct->start_service_thread();
}
