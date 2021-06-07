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

#include <filesystem>
#include "common/async/context_pool.h"
#include "common/ceph_argparse.h"
#include "common/code_environment.h"
#include "common/config.h"
#include "common/debug.h"
#include "common/errno.h"
#include "common/signal.h"
#include "common/version.h"
#include "erasure-code/ErasureCodePlugin.h"
#include "global/global_context.h"
#include "global/global_init.h"
#include "global/pidfile.h"
#include "global/signal_handler.h"
#include "include/compat.h"
#include "include/str_list.h"
#include "mon/MonClient.h"

#ifndef _WIN32
#include <pwd.h>
#include <grp.h>
#endif
#include <errno.h>

#ifdef HAVE_SYS_PRCTL_H
#include <sys/prctl.h>
#endif

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_

namespace fs = std::filesystem;

using std::cerr;
using std::string;

static void global_init_set_globals(CephContext *cct)
{
  g_ceph_context = cct;
  get_process_name(g_process_name, sizeof(g_process_name));
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

static int chown_path(const std::string &pathname, const uid_t owner, const gid_t group,
		      const std::string &uid_str, const std::string &gid_str)
{
  #ifdef _WIN32
  return 0;
  #else

  const char *pathname_cstr = c_str_or_null(pathname);

  if (!pathname_cstr) {
    return 0;
  }

  int r = ::chown(pathname_cstr, owner, group);

  if (r < 0) {
    r = -errno;
    cerr << "warning: unable to chown() " << pathname << " as "
	 << uid_str << ":" << gid_str << ": " << cpp_strerror(r) << std::endl;
  }

  return r;
  #endif
}

void global_pre_init(
  const std::map<std::string,std::string> *defaults,
  std::vector < const char* >& args,
  uint32_t module_type, code_environment_t code_env,
  int flags)
{
  std::string conf_file_list;
  std::string cluster = "";

  // ensure environment arguments are included in early processing
  env_to_vec(args);

  CephInitParameters iparams = ceph_argparse_early_args(
    args, module_type,
    &cluster, &conf_file_list);

  CephContext *cct = common_preinit(iparams, code_env, flags);
  cct->_conf->cluster = cluster;
  global_init_set_globals(cct);
  auto& conf = cct->_conf;

  if (flags & (CINIT_FLAG_NO_DEFAULT_CONFIG_FILE|
	       CINIT_FLAG_NO_MON_CONFIG)) {
    conf->no_mon_config = true;
  }

  // alternate defaults
  if (defaults) {
    for (auto& i : *defaults) {
      conf.set_val_default(i.first, i.second);
    }
  }

  if (conf.get_val<bool>("no_config_file")) {
    flags |= CINIT_FLAG_NO_DEFAULT_CONFIG_FILE;
  }

  int ret = conf.parse_config_files(c_str_or_null(conf_file_list),
				    &cerr, flags);
  if (ret == -EDOM) {
    cct->_log->flush();
    cerr << "global_init: error parsing config file." << std::endl;
    _exit(1);
  }
  else if (ret == -ENOENT) {
    if (!(flags & CINIT_FLAG_NO_DEFAULT_CONFIG_FILE)) {
      if (conf_file_list.length()) {
	cct->_log->flush();
	cerr << "global_init: unable to open config file from search list "
	     << conf_file_list << std::endl;
        _exit(1);
      } else {
	cerr << "did not load config file, using default settings."
	     << std::endl;
      }
    }
  }
  else if (ret) {
    cct->_log->flush();
    cerr << "global_init: error reading config file. "
         << conf.get_parse_error() << std::endl;
    _exit(1);
  }

  // environment variables override (CEPH_ARGS, CEPH_KEYRING)
  conf.parse_env(cct->get_module_type());

  // command line (as passed by caller)
  conf.parse_argv(args);

  if (!cct->_log->is_started()) {
    cct->_log->start();
  }

  // do the --show-config[-val], if present in argv
  conf.do_argv_commands();

  // Now we're ready to complain about config file parse errors
  g_conf().complain_about_parse_error(g_ceph_context);
}

boost::intrusive_ptr<CephContext>
global_init(const std::map<std::string,std::string> *defaults,
	    std::vector < const char* >& args,
	    uint32_t module_type, code_environment_t code_env,
	    int flags, bool run_pre_init)
{
  // Ensure we're not calling the global init functions multiple times.
  static bool first_run = true;
  if (run_pre_init) {
    // We will run pre_init from here (default).
    ceph_assert(!g_ceph_context && first_run);
    global_pre_init(defaults, args, module_type, code_env, flags);
  } else {
    // Caller should have invoked pre_init manually.
    ceph_assert(g_ceph_context && first_run);
  }
  first_run = false;

  // Verify flags have not changed if global_pre_init() has been called
  // manually. If they have, update them.
  if (g_ceph_context->get_init_flags() != flags) {
    g_ceph_context->set_init_flags(flags);
  }

  #ifndef _WIN32
  // signal stuff
  int siglist[] = { SIGPIPE, 0 };
  block_signals(siglist, NULL);
  #endif

  if (g_conf()->fatal_signal_handlers) {
    install_standard_sighandlers();
  }
  ceph::register_assert_context(g_ceph_context);

  if (g_conf()->log_flush_on_exit)
    g_ceph_context->_log->set_flush_on_exit();

  // drop privileges?
  ostringstream priv_ss;

  #ifndef _WIN32
  // consider --setuser root a no-op, even if we're not root
  if (getuid() != 0) {
    if (g_conf()->setuser.length()) {
      cerr << "ignoring --setuser " << g_conf()->setuser << " since I am not root"
	   << std::endl;
    }
    if (g_conf()->setgroup.length()) {
      cerr << "ignoring --setgroup " << g_conf()->setgroup
	   << " since I am not root" << std::endl;
    }
  } else if (g_conf()->setgroup.length() ||
             g_conf()->setuser.length()) {
    uid_t uid = 0;  // zero means no change; we can only drop privs here.
    gid_t gid = 0;
    std::string uid_string;
    std::string gid_string;
    std::string home_directory;
    if (g_conf()->setuser.length()) {
      char buf[4096];
      struct passwd pa;
      struct passwd *p = 0;

      uid = atoi(g_conf()->setuser.c_str());
      if (uid) {
        getpwuid_r(uid, &pa, buf, sizeof(buf), &p);
      } else {
	getpwnam_r(g_conf()->setuser.c_str(), &pa, buf, sizeof(buf), &p);
        if (!p) {
	  cerr << "unable to look up user '" << g_conf()->setuser << "'"
	       << std::endl;
	  exit(1);
        }

        uid = p->pw_uid;
        gid = p->pw_gid;
        uid_string = g_conf()->setuser;
      }

      if (p && p->pw_dir != nullptr) {
        home_directory = std::string(p->pw_dir);
      }
    }
    if (g_conf()->setgroup.length() > 0) {
      gid = atoi(g_conf()->setgroup.c_str());
      if (!gid) {
	char buf[4096];
	struct group gr;
	struct group *g = 0;
	getgrnam_r(g_conf()->setgroup.c_str(), &gr, buf, sizeof(buf), &g);
	if (!g) {
	  cerr << "unable to look up group '" << g_conf()->setgroup << "'"
	       << ": " << cpp_strerror(errno) << std::endl;
	  exit(1);
	}
	gid = g->gr_gid;
	gid_string = g_conf()->setgroup;
      }
    }
    if ((uid || gid) &&
	g_conf()->setuser_match_path.length()) {
      // induce early expansion of setuser_match_path config option
      string match_path = g_conf()->setuser_match_path;
      g_conf().early_expand_meta(match_path, &cerr);
      struct stat st;
      int r = ::stat(match_path.c_str(), &st);
      if (r < 0) {
	cerr << "unable to stat setuser_match_path "
	     << g_conf()->setuser_match_path
	     << ": " << cpp_strerror(errno) << std::endl;
	exit(1);
      }
      if ((uid && uid != st.st_uid) ||
	  (gid && gid != st.st_gid)) {
	cerr << "WARNING: will not setuid/gid: " << match_path
	     << " owned by " << st.st_uid << ":" << st.st_gid
	     << " and not requested " << uid << ":" << gid
	     << std::endl;
	uid = 0;
	gid = 0;
	uid_string.erase();
	gid_string.erase();
      } else {
	priv_ss << "setuser_match_path "
		<< match_path << " owned by "
		<< st.st_uid << ":" << st.st_gid << ". ";
      }
    }
    g_ceph_context->set_uid_gid(uid, gid);
    g_ceph_context->set_uid_gid_strings(uid_string, gid_string);
    if ((flags & CINIT_FLAG_DEFER_DROP_PRIVILEGES) == 0) {
      if (setgid(gid) != 0) {
	cerr << "unable to setgid " << gid << ": " << cpp_strerror(errno)
	     << std::endl;
	exit(1);
      }
      if (setuid(uid) != 0) {
	cerr << "unable to setuid " << uid << ": " << cpp_strerror(errno)
	     << std::endl;
	exit(1);
      }
      if (setenv("HOME", home_directory.c_str(), 1) != 0) {
	cerr << "warning: unable to set HOME to " << home_directory << ": "
             << cpp_strerror(errno) << std::endl;
      }
      priv_ss << "set uid:gid to " << uid << ":" << gid << " (" << uid_string << ":" << gid_string << ")";
    } else {
      priv_ss << "deferred set uid:gid to " << uid << ":" << gid << " (" << uid_string << ":" << gid_string << ")";
    }
  }
  #endif /* _WIN32 */

#if defined(HAVE_SYS_PRCTL_H)
  if (prctl(PR_SET_DUMPABLE, 1) == -1) {
    cerr << "warning: unable to set dumpable flag: " << cpp_strerror(errno) << std::endl;
  }
#  if defined(PR_SET_THP_DISABLE)
  if (!g_conf().get_val<bool>("thp") && prctl(PR_SET_THP_DISABLE, 1, 0, 0, 0) == -1) {
    cerr << "warning: unable to disable THP: " << cpp_strerror(errno) << std::endl;
  }
#  endif
#endif

  //
  // Utterly important to run first network connection after setuid().
  // In case of rdma transport uverbs kernel module starts returning
  // -EACCESS on each operation if credentials has been changed, see
  // callers of ib_safe_file_access() for details.
  //
  // fork() syscall also matters, so daemonization won't work in case
  // of rdma.
  //
  if (!g_conf()->no_mon_config) {
    // make sure our mini-session gets legacy values
    g_conf().apply_changes(nullptr);

    ceph::async::io_context_pool cp(1);
    MonClient mc_bootstrap(g_ceph_context, cp);
    if (mc_bootstrap.get_monmap_and_config() < 0) {
      cp.stop();
      g_ceph_context->_log->flush();
      cerr << "failed to fetch mon config (--no-mon-config to skip)"
	   << std::endl;
      _exit(1);
    }
    cp.stop();
  }

  // Expand metavariables. Invoke configuration observers. Open log file.
  g_conf().apply_changes(nullptr);

  if (g_conf()->run_dir.length() &&
      code_env == CODE_ENVIRONMENT_DAEMON &&
      !(flags & CINIT_FLAG_NO_DAEMON_ACTIONS)) {

    if (!fs::exists(g_conf()->run_dir.c_str())) {
      std::error_code ec;
      if (!fs::create_directory(g_conf()->run_dir, ec)) {
       cerr << "warning: unable to create " << g_conf()->run_dir
            << ec.message() << std::endl;
      }
      fs::permissions(
        g_conf()->run_dir.c_str(),
        fs::perms::owner_all |
        fs::perms::group_read | fs::perms::group_exec |
        fs::perms::others_read | fs::perms::others_exec);
    }
  }

  // call all observers now.  this has the side-effect of configuring
  // and opening the log file immediately.
  g_conf().call_all_observers();

  if (priv_ss.str().length()) {
    dout(0) << priv_ss.str() << dendl;
  }

  if ((flags & CINIT_FLAG_DEFER_DROP_PRIVILEGES) &&
      (g_ceph_context->get_set_uid() || g_ceph_context->get_set_gid())) {
    // Fix ownership on log files and run directories if needed.
    // Admin socket files are chown()'d during the common init path _after_
    // the service thread has been started. This is sadly a bit of a hack :(
    chown_path(g_conf()->run_dir,
	       g_ceph_context->get_set_uid(),
	       g_ceph_context->get_set_gid(),
	       g_ceph_context->get_set_uid_string(),
	       g_ceph_context->get_set_gid_string());
    g_ceph_context->_log->chown_log_file(
      g_ceph_context->get_set_uid(),
      g_ceph_context->get_set_gid());
  }

  // Now we're ready to complain about config file parse errors
  g_conf().complain_about_parse_error(g_ceph_context);

  // test leak checking
  if (g_conf()->debug_deliberately_leak_memory) {
    derr << "deliberately leaking some memory" << dendl;
    char *s = new char[1234567];
    (void)s;
    // cppcheck-suppress memleak
  }

  if (code_env == CODE_ENVIRONMENT_DAEMON && !(flags & CINIT_FLAG_NO_DAEMON_ACTIONS))
    output_ceph_version();

  if (g_ceph_context->crush_location.init_on_startup()) {
    cerr << " failed to init_on_startup : " << cpp_strerror(errno) << std::endl;
    exit(1);
  }

  return boost::intrusive_ptr<CephContext>{g_ceph_context, false};
}

void global_print_banner(void)
{
  output_ceph_version();
}

int global_init_prefork(CephContext *cct)
{
  if (g_code_env != CODE_ENVIRONMENT_DAEMON)
    return -1;

  const auto& conf = cct->_conf;
  if (!conf->daemonize) {

    if (pidfile_write(conf->pid_file) < 0)
      exit(1);

    if ((cct->get_init_flags() & CINIT_FLAG_DEFER_DROP_PRIVILEGES) &&
	(cct->get_set_uid() || cct->get_set_gid())) {
      chown_path(conf->pid_file, cct->get_set_uid(), cct->get_set_gid(),
		 cct->get_set_uid_string(), cct->get_set_gid_string());
    }

    return -1;
  }

  cct->notify_pre_fork();
  // stop log thread
  cct->_log->flush();
  cct->_log->stop();
  return 0;
}

void global_init_daemonize(CephContext *cct)
{
  if (global_init_prefork(cct) < 0)
    return;

#if !defined(_AIX) && !defined(_WIN32)
  int ret = daemon(1, 1);
  if (ret) {
    ret = errno;
    derr << "global_init_daemonize: BUG: daemon error: "
	 << cpp_strerror(ret) << dendl;
    exit(1);
  }
 
  global_init_postfork_start(cct);
  global_init_postfork_finish(cct);
#else
# warning daemon not supported on aix
#endif
}

int reopen_as_null(CephContext *cct, int fd)
{
  int newfd = open(DEV_NULL, O_RDONLY | O_CLOEXEC);
  if (newfd < 0) {
    int err = errno;
    lderr(cct) << __func__ << " failed to open /dev/null: " << cpp_strerror(err)
	       << dendl;
    return -1;
  }
  // atomically dup newfd to target fd.  target fd is implicitly closed if
  // open and atomically replaced; see man dup2
  int r = dup2(newfd, fd);
  if (r < 0) {
    int err = errno;
    lderr(cct) << __func__ << " failed to dup2 " << fd << ": "
	       << cpp_strerror(err) << dendl;
    return -1;
  }
  // close newfd (we cloned it to target fd)
  VOID_TEMP_FAILURE_RETRY(close(newfd));
  // N.B. FD_CLOEXEC is cleared on fd (see dup2(2))
  return 0;
}

void global_init_postfork_start(CephContext *cct)
{
  // reexpand the meta in child process
  cct->_conf.finalize_reexpand_meta();

  // restart log thread
  cct->_log->start();
  cct->notify_post_fork();

  /* This is the old trick where we make file descriptors 0, 1, and possibly 2
   * point to /dev/null.
   *
   * We have to do this because otherwise some arbitrary call to open() later
   * in the program might get back one of these file descriptors. It's hard to
   * guarantee that nobody ever writes to stdout, even though they're not
   * supposed to.
   */
  reopen_as_null(cct, STDIN_FILENO);

  const auto& conf = cct->_conf;
  if (pidfile_write(conf->pid_file) < 0)
    exit(1);

  if ((cct->get_init_flags() & CINIT_FLAG_DEFER_DROP_PRIVILEGES) &&
      (cct->get_set_uid() || cct->get_set_gid())) {
    chown_path(conf->pid_file, cct->get_set_uid(), cct->get_set_gid(),
	       cct->get_set_uid_string(), cct->get_set_gid_string());
  }
}

void global_init_postfork_finish(CephContext *cct)
{
  /* We only close stdout+stderr once the caller decides the daemonization
   * process is finished.  This way we can allow error or other messages to be
   * propagated in a manner that the user is able to see.
   */
  if (!(cct->get_init_flags() & CINIT_FLAG_NO_CLOSE_STDERR)) {
    int ret = global_init_shutdown_stderr(cct);
    if (ret) {
      derr << "global_init_daemonize: global_init_shutdown_stderr failed with "
	   << "error code " << ret << dendl;
      exit(1);
    }
  }

  reopen_as_null(cct, STDOUT_FILENO);

  ldout(cct, 1) << "finished global_init_daemonize" << dendl;
}


void global_init_chdir(const CephContext *cct)
{
  const auto& conf = cct->_conf;
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
  reopen_as_null(cct, STDERR_FILENO);
  int l = cct->_conf->err_to_stderr ? -1 : -2;
  cct->_log->set_stderr_level(l, l);
  return 0;
}

int global_init_preload_erasure_code(const CephContext *cct)
{
  const auto& conf = cct->_conf;
  string plugins = conf->osd_erasure_code_plugins;

  // validate that this is a not a legacy plugin
  std::list<string> plugins_list;
  get_str_list(plugins, plugins_list);
  for (auto i = plugins_list.begin(); i != plugins_list.end(); ++i) {
	string plugin_name = *i;
	string replacement = "";

	if (plugin_name == "jerasure_generic" || 
	    plugin_name == "jerasure_sse3" ||
	    plugin_name == "jerasure_sse4" ||
	    plugin_name == "jerasure_neon") {
	  replacement = "jerasure";
	}
	else if (plugin_name == "shec_generic" ||
		 plugin_name == "shec_sse3" ||
		 plugin_name == "shec_sse4" ||
		 plugin_name == "shec_neon") {
	  replacement = "shec";
	}

	if (replacement != "") {
	  dout(0) << "WARNING: osd_erasure_code_plugins contains plugin "
		  << plugin_name << " that is now deprecated. Please modify the value "
		  << "for osd_erasure_code_plugins to use "  << replacement << " instead." << dendl;
	}
  }

  std::stringstream ss;
  int r = ceph::ErasureCodePluginRegistry::instance().preload(
    plugins,
    conf.get_val<std::string>("erasure_code_dir"),
    &ss);
  if (r)
    derr << ss.str() << dendl;
  else
    dout(0) << ss.str() << dendl;
  return r;
}
