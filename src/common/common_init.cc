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

#include "common/common_init.h"
#include "common/admin_socket.h"
#include "common/ceph_argparse.h"
#include "common/ceph_context.h"
#include "common/config.h"
#include "common/dout.h"
#include "common/hostname.h"
#include "common/strtol.h"
#include "common/valgrind.h"
#include "common/zipkin_trace.h"
#include "include/compat.h"
#include "log/Log.h"

#define dout_subsys ceph_subsys_

#ifndef WITH_CRIMSON
CephContext *common_preinit(const CephInitParameters &iparams,
			    enum code_environment_t code_env, int flags)
{
  // set code environment
  ANNOTATE_BENIGN_RACE_SIZED(&g_code_env, sizeof(g_code_env), "g_code_env");
  g_code_env = code_env;

  // Create a configuration object
  CephContext *cct = new CephContext(iparams.module_type, code_env, flags);

  auto& conf = cct->_conf;
  // add config observers here

  // Set up our entity name.
  conf->name = iparams.name;

  // different default keyring locations for osd and mds.  this is
  // for backward compatibility.  moving forward, we want all keyrings
  // in these locations.  the mon already forces $mon_data/keyring.
  if (conf->name.is_mds()) {
    conf.set_val_default("keyring", "$mds_data/keyring");
  } else if (conf->name.is_osd()) {
    conf.set_val_default("keyring", "$osd_data/keyring");
  }

  if ((flags & CINIT_FLAG_UNPRIVILEGED_DAEMON_DEFAULTS)) {
    // make this unique despite multiple instances by the same name.
    conf.set_val_default("admin_socket",
			  "$run_dir/$cluster-$name.$pid.$cctid.asok");
  }

  if (code_env == CODE_ENVIRONMENT_LIBRARY ||
      code_env == CODE_ENVIRONMENT_UTILITY_NODOUT) {
    conf.set_val_default("log_to_stderr", "false");
    conf.set_val_default("err_to_stderr", "false");
    conf.set_val_default("log_flush_on_exit", "false");
  }

  conf.set_val("no_config_file", iparams.no_config_file ? "true" : "false");

  if (conf->host.empty()) {
    conf.set_val("host", ceph_get_short_hostname());
  }
  return cct;
}
#endif	// #ifndef WITH_CRIMSON

void complain_about_parse_error(CephContext *cct,
				const std::string& parse_error)
{
  if (parse_error.empty())
    return;
  lderr(cct) << "Errors while parsing config file!" << dendl;
  lderr(cct) << parse_error << dendl;
}

#ifndef WITH_CRIMSON

/* Please be sure that this can safely be called multiple times by the
 * same application. */
void common_init_finish(CephContext *cct)
{
  // only do this once per cct
  if (cct->_finished) {
    return;
  }
  cct->_finished = true;
  cct->init_crypto();
  ZTracer::ztrace_init();

  if (!cct->_log->is_started()) {
    cct->_log->start();
  }

  int flags = cct->get_init_flags();
  if (!(flags & CINIT_FLAG_NO_DAEMON_ACTIONS))
    cct->start_service_thread();

  if ((flags & CINIT_FLAG_DEFER_DROP_PRIVILEGES) &&
      (cct->get_set_uid() || cct->get_set_gid())) {
    cct->get_admin_socket()->chown(cct->get_set_uid(), cct->get_set_gid());
  }

  const auto& conf = cct->_conf;

  if (!conf->admin_socket.empty() && !conf->admin_socket_mode.empty()) {
    int ret = 0;
    std::string err;

    ret = strict_strtol(conf->admin_socket_mode.c_str(), 8, &err);
    if (err.empty()) {
      if (!(ret & (~ACCESSPERMS))) {
        cct->get_admin_socket()->chmod(static_cast<mode_t>(ret));
      } else {
        lderr(cct) << "Invalid octal permissions string: "
            << conf->admin_socket_mode << dendl;
      }
    } else {
      lderr(cct) << "Invalid octal string: " << err << dendl;
    }
  }
}

#endif	// #ifndef WITH_CRIMSON
