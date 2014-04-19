// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2011 New Dream Network
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include "common/Thread.h"
#include "common/ceph_argparse.h"
#include "common/code_environment.h"
#include "common/common_init.h"
#include "common/config.h"
#include "common/debug.h"
#include "common/errno.h"
#include "common/safe_io.h"
#include "common/signal.h"
#include "common/version.h"
#include "global/global_context.h"
#include "global/global_init.h"
#include "global/pidfile.h"
#include "global/signal_handler.h"
#include "include/compat.h"
#include "include/color.h"

#include <errno.h>
#include <deque>

#define dout_subsys ceph_subsys_

static void global_init_set_globals(CephContext *cct)
{
  g_ceph_context = cct;
  g_conf = cct->_conf;
}

static void output_ceph_version()
{
  char buf[1024];
  snprintf(buf, sizeof(buf), "%s, process %s, pid %d",
	   pretty_version_to_str().c_str(),
	   get_process_name_cpp().c_str(), getpid());
  generic_dout(0) << buf << dendl;
}

static const char* c_str_or_null(const std::string &str)
{
  if (str.empty())
    return NULL;
  return str.c_str();
}

void global_pre_init(std::vector < const char * > *alt_def_args,
		     std::vector < const char* >& args,
		     uint32_t module_type, code_environment_t code_env,
		     int flags)
{
  // You can only call global_init once.
  assert(!g_ceph_context);
  std::string conf_file_list;
  std::string cluster = "ceph";
  CephInitParameters iparams = ceph_argparse_early_args(args, module_type, flags,
							&cluster, &conf_file_list);
  CephContext *cct = common_preinit(iparams, code_env, flags);
  cct->_conf->cluster = cluster;
  global_init_set_globals(cct);
  md_config_t *conf = cct->_conf;

  if (alt_def_args)
    conf->parse_argv(*alt_def_args);  // alternative default args

  std::deque<std::string> parse_errors;
  int ret = conf->parse_config_files(c_str_or_null(conf_file_list), &parse_errors, &cerr, flags);
  if (ret == -EDOM) {
    dout_emergency("global_init: error parsing config file.\n");
    _exit(1);
  }
  else if (ret == -EINVAL) {
    if (!(flags & CINIT_FLAG_NO_DEFAULT_CONFIG_FILE)) {
      if (conf_file_list.length()) {
	ostringstream oss;
	oss << "global_init: unable to open config file from search list "
	    << conf_file_list << "\n";
        dout_emergency(oss.str());
        _exit(1);
      } else {
        derr <<"did not load config file, using default settings." << dendl;
      }
    }
  }
  else if (ret) {
    dout_emergency("global_init: error reading config file.\n");
    _exit(1);
  }

  conf->parse_env(); // environment variables override

  conf->parse_argv(args); // argv override

  // Expand metavariables. Invoke configuration observers.
  conf->apply_changes(NULL);

  // Now we're ready to complain about config file parse errors
  complain_about_parse_errors(cct, &parse_errors);
}

void global_init(std::vector < const char * > *alt_def_args,
		 std::vector < const char* >& args,
		 uint32_t module_type, code_environment_t code_env, int flags)
{
  global_pre_init(alt_def_args, args, module_type, code_env, flags);

  g_lockdep = g_ceph_context->_conf->lockdep;

  // signal stuff
  int siglist[] = { SIGPIPE, 0 };
  block_signals(siglist, NULL);

  if (g_conf->fatal_signal_handlers)
    install_standard_sighandlers();

  if (g_conf->log_flush_on_exit)
    g_ceph_context->_log->set_flush_on_exit();

  if (g_conf->run_dir.length() &&
      code_env == CODE_ENVIRONMENT_DAEMON &&
      !(flags & CINIT_FLAG_NO_DAEMON_ACTIONS)) {
    int r = ::mkdir(g_conf->run_dir.c_str(), 0755);
    if (r < 0 && errno != EEXIST) {
      r = -errno;
      derr << "warning: unable to create " << g_conf->run_dir << ": " << cpp_strerror(r) << dendl;
    }
  }

  if (g_lockdep) {
    lockdep_register_ceph_context(g_ceph_context);
  }
  register_assert_context(g_ceph_context);

  // call all observers now.  this has the side-effect of configuring
  // and opening the log file immediately.
  g_conf->call_all_observers();

  if (code_env == CODE_ENVIRONMENT_DAEMON && !(flags & CINIT_FLAG_NO_DAEMON_ACTIONS))
    output_ceph_version();
}

void global_print_banner(void)
{
  output_ceph_version();
}

static void pidfile_remove_void(void)
{
  pidfile_remove();
}

int global_init_prefork(CephContext *cct, int flags)
{
  if (g_code_env != CODE_ENVIRONMENT_DAEMON)
    return -1;
  const md_config_t *conf = cct->_conf;
  if (!conf->daemonize)
    return -1;

  // stop log thread
  g_ceph_context->_log->flush();
  g_ceph_context->_log->stop();
  return 0;
}

void global_init_daemonize(CephContext *cct, int flags)
{
  if (global_init_prefork(cct, flags) < 0)
    return;

  int ret = daemon(1, 1);
  if (ret) {
    ret = errno;
    derr << "global_init_daemonize: BUG: daemon error: "
	 << cpp_strerror(ret) << dendl;
    exit(1);
  }

  global_init_postfork_start(cct);
  global_init_postfork_finish(cct, flags);
}

void global_init_postfork_start(CephContext *cct)
{
  // restart log thread
  g_ceph_context->_log->start();

  if (atexit(pidfile_remove_void)) {
    derr << "global_init_daemonize: failed to set pidfile_remove function "
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
  VOID_TEMP_FAILURE_RETRY(close(STDIN_FILENO));
  if (open("/dev/null", O_RDONLY) < 0) {
    int err = errno;
    derr << "global_init_daemonize: open(/dev/null) failed: error "
	 << err << dendl;
    exit(1);
  }
  VOID_TEMP_FAILURE_RETRY(close(STDOUT_FILENO));
  if (open("/dev/null", O_RDONLY) < 0) {
    int err = errno;
    derr << "global_init_daemonize: open(/dev/null) failed: error "
	 << err << dendl;
    exit(1);
  }

  pidfile_write(g_conf);
}

void global_init_postfork_finish(CephContext *cct, int flags)
{
  /* We only close stderr once the caller decides the daemonization
   * process is finished.  This way we can allow error messages to be
   * propagated in a manner that the user is able to see.
   */
  if (!(flags & CINIT_FLAG_NO_CLOSE_STDERR)) {
    int ret = global_init_shutdown_stderr(cct);
    if (ret) {
      derr << "global_init_daemonize: global_init_shutdown_stderr failed with "
	   << "error code " << ret << dendl;
      exit(1);
    }
  }
  ldout(cct, 1) << "finished global_init_daemonize" << dendl;
}


void global_init_chdir(const CephContext *cct)
{
  const md_config_t *conf = cct->_conf;
  if (conf->chdir.empty())
    return;
  if (::chdir(conf->chdir.c_str())) {
    int err = errno;
    derr << "global_init_chdir: failed to chdir to directory: '"
	 << conf->chdir << "': " << cpp_strerror(err) << dendl;
  }
}

/* Map stderr to /dev/null. This isn't really re-entrant; we rely on the old unix
 * behavior that the file descriptor that gets assigned is the lowest
 * available one.
 */
int global_init_shutdown_stderr(CephContext *cct)
{
  VOID_TEMP_FAILURE_RETRY(close(STDERR_FILENO));
  if (open("/dev/null", O_RDONLY) < 0) {
    int err = errno;
    derr << "global_init_shutdown_stderr: open(/dev/null) failed: error "
	 << err << dendl;
    return 1;
  }
  cct->_log->set_stderr_level(-1, -1);
  return 0;
}

