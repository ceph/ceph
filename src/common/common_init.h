// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2009-2011 New Dream Network
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef CEPH_COMMON_INIT_H
#define CEPH_COMMON_INIT_H

#include <deque>
#include <stdint.h>
#include <string>
#include <vector>

#include "common/code_environment.h"

class CephContext;
class CephInitParameters;

enum common_init_flags_t {
  // Set up defaults that make sense for an unprivileged deamon
  CINIT_FLAG_UNPRIVILEGED_DAEMON_DEFAULTS = 0x1,

  // By default, don't read a configuration file
  CINIT_FLAG_NO_DEFAULT_CONFIG_FILE = 0x2,

  // Don't close stderr (in daemonize)
  CINIT_FLAG_NO_CLOSE_STDERR = 0x4,

  // don't do anything daemonish, like create /var/run/ceph, or print a banner
  CINIT_FLAG_NO_DAEMON_ACTIONS = 0x8,

  // don't drop privileges
  CINIT_FLAG_DEFER_DROP_PRIVILEGES = 0x16,
};

/*
 * NOTE: If you are writing a Ceph daemon, ignore this function and call
 * global_init instead. It will call common_preinit for you.
 *
 * common_preinit creates the CephContext.
 *
 * After this function gives you a CephContext, you need to set up the
 * Ceph configuration, which lives inside the CephContext as md_config_t.
 * The initial settings are not very useful because they do not reflect what
 * the user asked for.
 *
 * This is usually done by something like this:
 * cct->_conf->parse_env();
 * cct->_conf->apply_changes();
 *
 * Your library may also supply functions to read a configuration file.
 */
CephContext *common_preinit(const CephInitParameters &iparams,
			    enum code_environment_t code_env, int flags,
			    const char *data_dir_option = 0);

/* Print out some parse errors. */
void complain_about_parse_errors(CephContext *cct,
				 std::deque<std::string> *parse_errors);

/* This function is called after you have done your last
 * fork. When you make this call, the system will initialize everything that
 * cannot be initialized before a fork.
 *
 * This includes things like starting threads, initializing libraries that
 * can't handle forking, and so forth.
 *
 * If you are writing a Ceph library, you can call this pretty much any time.
 * We do not allow our library users to fork and continue using the Ceph
 * libraries. The most obvious reason for this is that the threads started by
 * the Ceph libraries would be destroyed by a fork().
 */
void common_init_finish(CephContext *cct);

#endif
