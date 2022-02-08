// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <string>
#ifndef _WIN32
#include <pwd.h>
#include <grp.h>
#endif

#include "common/ceph_context.h"
#include "common/common_init.h"
#include "common/errno.h"
#include "global/global_context.h"

using namespace std;

std::ostringstream maybe_drop_privileges(const int flags) {
  // drop privileges?
  std::ostringstream priv_ss;

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
  return priv_ss;
}

