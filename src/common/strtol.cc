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

#include <cerrno>
#include <climits>
#include <cstdlib>
#include <sstream>

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

template<typename T>
T strict_si_cast(const char *str, std::string *err)
{
  std::string s(str);
  if (s.empty()) {
    *err = "strict_sistrtoll: value not specified";
    return 0;
  }
  const char &u = s.back();
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

  if (m >= 0)
    s.pop_back();
  else
    m = 0;

  long long ll = strict_strtoll(s.c_str(), 10, err);
  if (ll < 0 && !std::numeric_limits<T>::is_signed) {
    *err = "strict_sistrtoll: value should not be negative";
    return 0;
  }
  if (static_cast<unsigned>(m) >= sizeof(T) * CHAR_BIT) {
    *err = ("strict_sistrtoll: the SI prefix is too large for the designated "
	    "type");
    return 0;
  }
  using promoted_t = typename std::common_type<decltype(ll), T>::type;
  if (static_cast<promoted_t>(ll) <
      static_cast<promoted_t>(std::numeric_limits<T>::min()) >> m) {
    *err = "strict_sistrtoll: value seems to be too small";
    return 0;
  }
  if (static_cast<promoted_t>(ll) >
      static_cast<promoted_t>(std::numeric_limits<T>::max()) >> m) {
    *err = "strict_sistrtoll: value seems to be too large";
    return 0;
  }
  return (ll << m);
}

template int strict_si_cast<int>(const char *str, std::string *err);
template long strict_si_cast<long>(const char *str, std::string *err);
template long long strict_si_cast<long long>(const char *str, std::string *err);
template uint64_t strict_si_cast<uint64_t>(const char *str, std::string *err);
template uint32_t strict_si_cast<uint32_t>(const char *str, std::string *err);

uint64_t strict_sistrtoll(const char *str, std::string *err)
{
  return strict_si_cast<uint64_t>(str, err);
}
