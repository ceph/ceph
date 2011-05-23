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

#include "common/DoutStreambuf.h"
#include "common/ProfLogger.h"
#include "common/ceph_context.h"
#include "common/config.h"

#include <iostream>

// FIXME
// These variables are here temporarily to make the transition easier.
CephContext g_ceph_context __attribute__((init_priority(103)));
md_config_t &g_conf(*g_ceph_context._conf);
std::ostream *_dout(&g_ceph_context._dout);
DoutStreambuf <char, std::basic_string<char>::traits_type> *_doss(g_ceph_context._doss);

/*
 * The dout lock protects calls to dout()
 * TODO: needs to become part of CephContext
 */
pthread_mutex_t _dout_lock = PTHREAD_MUTEX_INITIALIZER;

CephContext::
CephContext()
  : _doss(new DoutStreambuf <char, std::basic_string<char>::traits_type>()),
    _dout(_doss),
    _prof_logger_conf_obs(new ProfLoggerConfObs())
{
  _conf = new md_config_t();
  _conf->add_observer(_doss);
  _conf->add_observer(_prof_logger_conf_obs);
}

CephContext::
~CephContext()
{
  _conf->remove_observer(_prof_logger_conf_obs);
  _conf->remove_observer(_doss);

  delete _doss;
  _doss = NULL;
  delete _prof_logger_conf_obs;
  _prof_logger_conf_obs = NULL;

  delete _conf;
}
