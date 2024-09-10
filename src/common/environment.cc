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

#include "common/environment.h"

#include <stdlib.h>
#include <strings.h>

bool get_env_bool(const char *key)
{
  const char *val = getenv(key);
  if (!val)
    return false;
  if (strcasecmp(val, "off") == 0)
    return false;
  if (strcasecmp(val, "no") == 0)
    return false;
  if (strcasecmp(val, "false") == 0)
    return false;
  if (strcasecmp(val, "0") == 0)
    return false;
  return true;
}

int get_env_int(const char *key)
{
  const char *val = getenv(key);
  if (!val)
    return 0;
  int v = atoi(val);
  return v;
}
