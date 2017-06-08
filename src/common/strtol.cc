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

#include "strtol.h"

#include <errno.h>
#include <limits.h>
#include <sstream>
#include <stdlib.h>

using std::ostringstream;

long long strict_strtoll(const char *str, int base, std::string *err)
{
  char *endptr;
  std::string errStr;
  errno = 0; /* To distinguish success/failure after call (see man page) */
  long long ret = strtoll(str, &endptr, base);

  if ((errno == ERANGE && (ret == LLONG_MAX || ret == LLONG_MIN))
      || (errno != 0 && ret == 0)) {
    errStr = "The option value '";
    errStr.append(str);
    errStr.append("'");
    errStr.append(" seems to be invalid");
    *err = errStr;
    return 0;
  }
  if (endptr == str) {
    errStr = "Expected option value to be integer, got '";
    errStr.append(str);
    errStr.append("'");
    *err =  errStr;
    return 0;
  }
  if (*endptr != '\0') {
    errStr = "The option value '";
    errStr.append(str);
    errStr.append("'");
    errStr.append(" seems to be invalid");
    *err =  errStr;
    return 0;
  }
  *err = "";
  return ret;
}

int strict_strtol(const char *str, int base, std::string *err)
{
  std::string errStr;
  long long ret = strict_strtoll(str, base, err);
  if (!err->empty())
    return 0;
  if ((ret <= INT_MIN) || (ret >= INT_MAX)) {
    errStr = "The option value '";
    errStr.append(str);
    errStr.append("'");
    errStr.append(" seems to be invalid");
    *err = errStr;
    return 0;
  }
  return static_cast<int>(ret);
}

double strict_strtod(const char *str, std::string *err)
{
  char *endptr;
  errno = 0; /* To distinguish success/failure after call (see man page) */
  double ret = strtod(str, &endptr);
  if (errno == ERANGE) {
    ostringstream oss;
    oss << "strict_strtod: floating point overflow or underflow parsing '"
	<< str << "'";
    *err = oss.str();
    return 0.0;
  }
  if (endptr == str) {
    ostringstream oss;
    oss << "strict_strtod: expected double, got: '" << str << "'";
    *err = oss.str();
    return 0;
  }
  if (*endptr != '\0') {
    ostringstream oss;
    oss << "strict_strtod: garbage at end of string. got: '" << str << "'";
    *err = oss.str();
    return 0;
  }
  *err = "";
  return ret;
}

float strict_strtof(const char *str, std::string *err)
{
  char *endptr;
  errno = 0; /* To distinguish success/failure after call (see man page) */
  float ret = strtof(str, &endptr);
  if (errno == ERANGE) {
    ostringstream oss;
    oss << "strict_strtof: floating point overflow or underflow parsing '"
	<< str << "'";
    *err = oss.str();
    return 0.0;
  }
  if (endptr == str) {
    ostringstream oss;
    oss << "strict_strtof: expected float, got: '" << str << "'";
    *err = oss.str();
    return 0;
  }
  if (*endptr != '\0') {
    ostringstream oss;
    oss << "strict_strtof: garbage at end of string. got: '" << str << "'";
    *err = oss.str();
    return 0;
  }
  *err = "";
  return ret;
}

uint64_t strict_sistrtoll(const char *str, std::string *err)
{
  std::string s(str);
  if (s.empty()) {
    *err = "strict_sistrtoll: value not specified";
    return 0;
  }
  const char &u = s.at(s.size()-1); //str[std::strlen(str)-1];
  int m = 0;
  if (u == 'B')
    m = 0;
  else if (u == 'K')
    m = 10;
  else if (u == 'M')
    m = 20;
  else if (u == 'G')
    m = 30;
  else if (u == 'T')
    m = 40;
  else if (u == 'P')
    m = 50;
  else if (u == 'E')
    m = 60;
  else
    m = -1;

  const char *v = NULL;
  if (m >= 0)
    s = std::string(str, s.size()-1);
  v = s.c_str();

  long long r_ll = strict_strtoll(v, 10, err);

  if (r_ll < 0) {
    *err = "strict_sistrtoll: value should not be negative";
    return 0;
  }

  uint64_t r = r_ll;
  if (err->empty() && m > 0) {
    if (r > (std::numeric_limits<uint64_t>::max() >> m)) {
      *err = "strict_sistrtoll: value seems to be too large";
      return 0;
    }
    r <<= m;
  }
  return r;
}

template <>
uint64_t strict_si_cast(const char *str, std::string *err) {
  return strict_sistrtoll(str, err);
}
