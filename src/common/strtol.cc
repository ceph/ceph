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

#include <errno.h>
#include <limits.h>
#include <sstream>
#include <stdlib.h>
#include <string>

using std::ostringstream;

long long strict_strtoll(const char *str, int base, std::string *err)
{
  char *endptr;
  errno = 0; /* To distinguish success/failure after call (see man page) */
  long long ret = strtoll(str, &endptr, base);

  if ((errno == ERANGE && (ret == LLONG_MAX || ret == LLONG_MIN))
      || (errno != 0 && ret == 0)) {
    ostringstream oss;
    oss << "strict_strtoll: integer underflow or overflow parsing '" << str << "'";
    *err = oss.str();
    return 0;
  }
  if (endptr == str) {
    ostringstream oss;
    oss << "strict_strtoll: expected integer, got: '" << str << "'";
    *err = oss.str();
    return 0;
  }
  if (*endptr != '\0') {
    ostringstream oss;
    oss << "strict_strtoll: garbage at end of string. got: '" << str << "'";
    *err = oss.str();
    return 0;
  }
  return ret;
}

int strict_strtol(const char *str, int base, std::string *err)
{
  long long ret = strict_strtoll(str, base, err);
  if (!err->empty())
    return 0;
  if (ret <= INT_MIN) {
    ostringstream oss;
    oss << "strict_strtol: integer underflow parsing '" << str << "'";
    *err = oss.str();
    return 0;
  }
  if (ret >= INT_MAX) {
    ostringstream oss;
    oss << "strict_strtol: integer overflow parsing '" << str << "'";
    *err = oss.str();
    return 0;
  }
  return static_cast<int>(ret);
}
