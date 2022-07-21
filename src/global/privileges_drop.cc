// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifdef WITH_SEASTAR
#include <stdlib.h>
#endif
#include <string>
#ifndef _WIN32
#include <pwd.h>
#include <grp.h>
#endif

#include <fmt/format.h>

#include "common/ceph_context.h"
#include "common/common_init.h"
#include "common/errno.h"
#include "global/global_context.h"

using namespace std;

#ifdef WITH_SEASTAR
#include "crimson/common/log.h"
#else
struct Logger {
  std::ostringstream ss;

  template <class... Args>
  static void error(fmt::format_string<Args...> fmt, Args&&... args) {
    cerr << fmt::format(fmt, std::forward<Args>(args)...) << std::endl;
  }

  template <class... Args>
  void debug(fmt::format_string<Args...> fmt, Args&&... args) {
    ss << fmt::format(fmt, std::forward<Args>(args)...) << std::endl;
  }
};
#endif


template <class LoggerT>
void _maybe_drop_privileges(LoggerT& logger, const int flags) {
  // drop privileges?
  #ifndef _WIN32
  // consider --setuser root a no-op, even if we're not root
  if (getuid() != 0) {
    if (g_conf()->setuser.length()) {
      logger.error("ignoring --setuser {} since I am not root",
		   g_conf()->setuser);
    }
    if (g_conf()->setgroup.length()) {
      logger.debug("ignoring --setgroup {} since I am not root",
		   g_conf()->setgroup);
    }
  } else if (g_conf()->setgroup.length() ||
             g_conf()->setuser.length()) {
    #ifdef WITH_SEASTAR
    // nowydays getpwnam looks look for "dynamic user names" via systemd
    // and dbus if it can't be found elsewhere. Unfortunately, this way
    // is uncompliant with Seastar's memory allocator.
    setenv("SYSTEMD_NSS_DYNAMIC_BYPASS", "1", true);
    #endif
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
	  logger.error("unable to look up user '{}'", g_conf()->setuser);
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
	  logger.error("unable to look up group '{}': {}",
		       g_conf()->setgroup, cpp_strerror(errno));
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
	logger.error("unable to stat setuser_match_path {}: {}",
		     g_conf()->setuser_match_path, cpp_strerror(errno));
	exit(1);
      }
      if ((uid && uid != st.st_uid) ||
	  (gid && gid != st.st_gid)) {
	logger.error("WARNING: will not setuid/gid: {} owned by {}:{}"
		     " and not requested {}:{}",
                     match_path, st.st_uid, st.st_gid, uid, gid);
	uid = 0;
	gid = 0;
	uid_string.erase();
	gid_string.erase();
      } else {
	logger.debug("setuser_match_path {} owned by {}:{}. ",
                     match_path, st.st_uid, st.st_gid);
      }
    }
#ifndef WITH_SEASTAR
    g_ceph_context->set_uid_gid(uid, gid);
    g_ceph_context->set_uid_gid_strings(uid_string, gid_string);
#endif
    if ((flags & CINIT_FLAG_DEFER_DROP_PRIVILEGES) == 0) {
      if (setgid(gid) != 0) {
	logger.error("unable to setgid {}: {}", gid, cpp_strerror(errno));
	exit(1);
      }
      if (setuid(uid) != 0) {
	logger.error("unable to setuid {}: {}", uid, cpp_strerror(errno));
	exit(1);
      }
      if (setenv("HOME", home_directory.c_str(), 1) != 0) {
	logger.error("warning: unable to set HOME to {}: {}",
                     home_directory, cpp_strerror(errno));
      }
      logger.debug("set uid:gid to {}:{} ({}:{})",
		   uid, gid, uid_string, gid_string);
    } else {
      logger.debug("deferred set uid:gid to {}:{} ({}:{})",
		   uid, gid, uid_string, gid_string);
    }
  }
  #endif /* _WIN32 */
}

#ifdef WITH_SEASTAR
void maybe_drop_privileges() {
  return _maybe_drop_privileges(crimson::get_logger(ceph_subsys_osd), 0);
}
#else
std::ostringstream maybe_drop_privileges(const int flags) {
  Logger logger{};
  _maybe_drop_privileges(logger, flags);
  return std::move(logger.ss);
}
#endif
