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

#ifndef CEPH_CEPHCONTEXT_H
#define CEPH_CEPHCONTEXT_H

#include <iostream>

/* Forward declarations */ 
template <typename T, typename U>
class DoutStreambuf;

class md_config_t;
class md_config_obs_t;

/* A CephContext represents the context held by a single library user.
 * There can be multiple CephContexts in the same process.
 *
 * For daemons and utility programs, there will be only one CephContext.  The
 * CephContext contains the configuration, the dout object, and anything else
 * that you might want to pass to libcommon with every function call.
 */
class CephContext {
public:
  CephContext();
  ~CephContext();
  md_config_t *_conf;
  DoutStreambuf <char, std::basic_string<char>::traits_type> *_doss;
  std::ostream _dout;

private:
  md_config_obs_t *_prof_logger_conf_obs;
};

/* Globals (FIXME: remove) */ 
extern CephContext g_ceph_context;
extern md_config_t &g_conf;
extern std::ostream *_dout;
extern DoutStreambuf <char, std::basic_string<char>::traits_type> *_doss;


#endif
