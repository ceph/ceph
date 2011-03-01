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

#ifndef CEPH_COMMON_CODE_ENVIRONMENT_H
#define CEPH_COMMON_CODE_ENVIRONMENT_H

enum code_environment_t {
  CODE_ENVIRONMENT_TEST = 0,
  CODE_ENVIRONMENT_UTILITY = 1,
  CODE_ENVIRONMENT_DAEMON = 2,
  CODE_ENVIRONMENT_LIBRARY = 3,
};

#ifdef __cplusplus
#include <iosfwd>

extern "C" code_environment_t g_code_env;
extern "C" const char *code_environment_to_str(enum code_environment_t e);
std::ostream &operator<<(std::ostream &oss, enum code_environment_t e);

#else

extern code_environment_t g_code_env;
const char *code_environment_to_str(enum code_environment_t e);

#endif

#endif
