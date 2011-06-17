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
};

CephContext *common_preinit(const CephInitParameters &iparams,
			    enum code_environment_t code_env, int flags);
void complain_about_parse_errors(std::deque<std::string> *parse_errors);
void common_init_finish(CephContext *cct);

#endif
