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

#include "common/code_environment.h"

#include <errno.h>
#include <iostream>
#include <stdlib.h>
#include <string.h>
#include <string>
#include <sys/prctl.h>

code_environment_t g_code_env;

extern "C" const char *code_environment_to_str(enum code_environment_t e)
{
  switch (e) {
    case CODE_ENVIRONMENT_TEST:
      return "CODE_ENVIRONMENT_TEST";
    case CODE_ENVIRONMENT_UTILITY:
      return "CODE_ENVIRONMENT_UTILITY";
    case CODE_ENVIRONMENT_DAEMON:
      return "CODE_ENVIRONMENT_DAEMON";
    case CODE_ENVIRONMENT_LIBRARY:
      return "CODE_ENVIRONMENT_LIBRARY";
    default:
      return NULL;
  }
}

std::ostream &operator<<(std::ostream &oss, enum code_environment_t e)
{
  oss << code_environment_to_str(e);
  return oss;
}

int get_process_name(char *buf, int len)
{
  int ret;
  if (len <= 16) {
    /* The man page discourages using this prctl with a buffer shorter
     * than 16 bytes. With a 16-byte buffer, it might not be
     * null-terminated. */
    return -ENAMETOOLONG;
  }
  memset(buf, 0, len);
  ret = prctl(PR_GET_NAME, buf);
  return ret;
}

std::string get_process_name_cpp()
{
  char buf[32];
  if (get_process_name(buf, sizeof(buf))) {
    return "(unknown)";
  }
  return std::string(buf);
}
