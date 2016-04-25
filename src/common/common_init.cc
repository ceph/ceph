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

#include "common/admin_socket.h"
#include "common/ceph_argparse.h"
#include "common/ceph_context.h"
#include "common/ceph_crypto.h"
#include "common/code_environment.h"
#include "common/common_init.h"
#include "common/config.h"
#include "common/dout.h"
#include "common/errno.h"
#include "common/safe_io.h"
#include "common/valgrind.h"
#include "common/version.h"
#include "include/color.h"

#include <errno.h>
#include <deque>

#define dout_subsys ceph_subsys_

#define _STR(x) #x
#define STRINGIFY(x) _STR(x)

CephContext *common_preinit(const CephInitParameters &iparams,
			    enum code_environment_t code_env, int flags,
			    const char *data_dir_option)
{
  // set code environment
  ANNOTATE_BENIGN_RACE_SIZED(&g_code_env, sizeof(g_code_env), "g_code_env");
  g_code_env = code_env;

  // Create a configuration object
  CephContext *cct = new CephContext(iparams.module_type, flags);

  md_config_t *conf = cct->_conf;
  // add config observers here

  // Set up our entity name.
  conf->name = iparams.name;

  if (data_dir_option)
    conf->data_dir_option = data_dir_option;

  // Set some defaults based on code type
  switch (code_env) {
  case CODE_ENVIRONMENT_DAEMON:
    conf->set_val_or_die("daemonize", "true");
    conf->set_val_or_die("log_to_stderr", "false");
    conf->set_val_or_die("err_to_stderr", "true");

    // different default keyring locations for osd and mds.  this is
    // for backward compatibility.  moving forward, we want all keyrings
    // in these locations.  the mon already forces $mon_data/keyring.
    if (conf->name.is_mds())
      conf->set_val("keyring", "$mds_data/keyring", false);
    else if (conf->name.is_osd())
      conf->set_val("keyring", "$osd_data/keyring", false);
    break;

  case CODE_ENVIRONMENT_UTILITY_NODOUT:
  case CODE_ENVIRONMENT_LIBRARY:
    conf->set_val_or_die("log_to_stderr", "false");
    conf->set_val_or_die("err_to_stderr", "false");
    conf->set_val_or_die("log_flush_on_exit", "false");
    break;

  default:
    break;
  }

  if (flags & CINIT_FLAG_UNPRIVILEGED_DAEMON_DEFAULTS) {
    // do nothing special!  we used to do no default log, pid_file,
    // admin_socket, but changed our minds.  let's make ceph-fuse
    // and radosgw use the same defaults as ceph-{osd,mon,mds,...}
  } else if (code_env != CODE_ENVIRONMENT_DAEMON) {
    // no default log, pid_file, admin_socket
    conf->set_val_or_die("pid_file", "");
    conf->set_val_or_die("admin_socket", "");
    conf->set_val_or_die("log_file", "");
    // use less memory for logs
    conf->set_val_or_die("log_max_recent", "500");
  }

  return cct;
}

void complain_about_parse_errors(CephContext *cct,
				 std::deque<std::string> *parse_errors)
{
  if (parse_errors->empty())
    return;
  lderr(cct) << "Errors while parsing config file!" << dendl;
  int cur_err = 0;
  static const int MAX_PARSE_ERRORS = 20;
  for (std::deque<std::string>::const_iterator p = parse_errors->begin();
       p != parse_errors->end(); ++p)
  {
    lderr(cct) << *p << dendl;
    if (cur_err == MAX_PARSE_ERRORS) {
      lderr(cct) << "Suppressed " << (parse_errors->size() - MAX_PARSE_ERRORS)
	   << " more errors." << dendl;
      break;
    }
    ++cur_err;
  }
}

/* Please be sure that this can safely be called multiple times by the
 * same application. */
void common_init_finish(CephContext *cct)
{
  cct->init_crypto();

  int flags = cct->get_init_flags();
  if (!(flags & CINIT_FLAG_NO_DAEMON_ACTIONS))
    cct->start_service_thread();

  if ((flags & CINIT_FLAG_DEFER_DROP_PRIVILEGES) &&
      (cct->get_set_uid() || cct->get_set_gid())) {
    cct->get_admin_socket()->chown(cct->get_set_uid(), cct->get_set_gid());
  }
}
