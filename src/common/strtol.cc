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

#include <algorithm>
#include <climits>
#include <limits>
#include <cmath>
#include <sstream>
#include <strings.h>
#include <string_view>

using std::ostringstream;

bool strict_strtob(const char* str, std::string *err)
{
  if (strcasecmp(str, "false") == 0) {
    return false;
  } else if (strcasecmp(str, "true") == 0) {
    return true;
  } else {
    int b = strict_strtol(str, 10, err);
    return (bool)!!b;
  }
}

long long strict_strtoll(std::string_view str, int base, std::string *err)
{
  char *endptr;
  errno = 0; /* To distinguish success/failure after call (see man page) */
  long long ret = strtoll(str.data(), &endptr, base);
  if (endptr == str.data() || endptr != str.data() + str.size()) {
    *err = (std::string{"Expected option value to be integer, got '"} +
	    std::string{str} + "'");
    return 0;
  }
  if (errno) {
    *err = (std::string{"The option value '"} + std::string{str} +
	    "' seems to be invalid");
    return 0;
  }
  *err = "";
  return ret;
}

long long strict_strtoll(const char *str, int base, std::string *err)
{
  return strict_strtoll(std::string_view(str), base, err);
}

int strict_strtol(std::string_view str, int base, std::string *err)
{
  long long ret = strict_strtoll(str, base, err);
  if (!err->empty())
    return 0;
  if ((ret < INT_MIN) || (ret > INT_MAX)) {
    ostringstream errStr;
    errStr << "The option value '" << str << "' seems to be invalid";
    *err = errStr.str();
    return 0;
  }
  return static_cast<int>(ret);
}

int strict_strtol(const char *str, int base, std::string *err)
{
  return strict_strtol(std::string_view(str), base, err);
}

double strict_strtod(std::string_view str, std::string *err)
{
  char *endptr;
  errno = 0; /* To distinguish success/failure after call (see man page) */
  double ret = strtod(str.data(), &endptr);
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

double strict_strtod(const char *str, std::string *err)
{
  return strict_strtod(std::string_view(str), err);
}

float strict_strtof(std::string_view str, std::string *err)
{
  char *endptr;
  errno = 0; /* To distinguish success/failure after call (see man page) */
  float ret = strtof(str.data(), &endptr);
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

float strict_strtof(const char *str, std::string *err)
{
  return strict_strtof(std::string_view(str), err);
}

template<typename T>
T strict_iec_cast(std::string_view str, std::string *err)
{
  if (str.empty()) {
    *err = "strict_iecstrtoll: value not specified";
    return 0;
  }
  // get a view of the unit and of the value
  std::string_view unit;
  std::string_view n = str;
  size_t u = str.find_first_not_of("0123456789-+");
  int m = 0;
  // deal with unit prefix if there is one
  if (u != std::string_view::npos) {
    n = str.substr(0, u);
    unit = str.substr(u, str.length() - u);
    // we accept both old si prefixes as well as the proper iec prefixes
    // i.e. K, M, ... and Ki, Mi, ...
    if (unit.back() == 'i') {
      if (unit.front() == 'B') {
        *err = "strict_iecstrtoll: illegal prefix \"Bi\"";
        return 0;
      }
    }
    if (unit.length() > 2) {
      *err = "strict_iecstrtoll: illegal prefix (length > 2)";
      return 0;
    }
    switch(unit.front()) {
      case 'K':
        m = 10;
        break;
      case 'M':
        m = 20;
        break;
      case 'G':
        m = 30;
        break;
      case 'T':
        m = 40;
        break;
      case 'P':
        m = 50;
        break;
      case 'E':
        m = 60;
        break;
      case 'B':
        break;
      default:
        *err = "strict_iecstrtoll: unit prefix not recognized";
        return 0;
    }
  }

  long long ll = strict_strtoll(n, 10, err);
  if (ll < 0 && !std::numeric_limits<T>::is_signed) {
    *err = "strict_iecstrtoll: value should not be negative";
    return 0;
  }
  if (static_cast<unsigned>(m) >= sizeof(T) * CHAR_BIT) {
    *err = ("strict_iecstrtoll: the IEC prefix is too large for the designated "
        "type");
    return 0;
  }
  using promoted_t = typename std::common_type<decltype(ll), T>::type;
  if (static_cast<promoted_t>(ll) <
      static_cast<promoted_t>(std::numeric_limits<T>::min()) >> m) {
    *err = "strict_iecstrtoll: value seems to be too small";
    return 0;
  }
  if (static_cast<promoted_t>(ll) >
      static_cast<promoted_t>(std::numeric_limits<T>::max()) >> m) {
    *err = "strict_iecstrtoll: value seems to be too large";
    return 0;
  }
  return (ll << m);
}

template int strict_iec_cast<int>(std::string_view str, std::string *err);
template long strict_iec_cast<long>(std::string_view str, std::string *err);
template long long strict_iec_cast<long long>(std::string_view str, std::string *err);
template uint64_t strict_iec_cast<uint64_t>(std::string_view str, std::string *err);
template uint32_t strict_iec_cast<uint32_t>(std::string_view str, std::string *err);

uint64_t strict_iecstrtoll(std::string_view str, std::string *err)
{
  return strict_iec_cast<uint64_t>(str, err);
}

uint64_t strict_iecstrtoll(const char *str, std::string *err)
{
  return strict_iec_cast<uint64_t>(std::string_view(str), err);
}

template<typename T>
T strict_iec_cast(const char *str, std::string *err)
{
  return strict_iec_cast<T>(std::string_view(str), err);
}

template int strict_iec_cast<int>(const char *str, std::string *err);
template long strict_iec_cast<long>(const char *str, std::string *err);
template long long strict_iec_cast<long long>(const char *str, std::string *err);
template uint64_t strict_iec_cast<uint64_t>(const char *str, std::string *err);
template uint32_t strict_iec_cast<uint32_t>(const char *str, std::string *err);

template<typename T>
T strict_si_cast(std::string_view str, std::string *err)
{
  if (str.empty()) {
    *err = "strict_sistrtoll: value not specified";
    return 0;
  }
  std::string_view n = str;
  int m = 0;
  // deal with unit prefix is there is one
  if (str.find_first_not_of("0123456789+-") != std::string_view::npos) {
    const char &u = str.back();
    if (u == 'K')
      m = 3;
    else if (u == 'M')
      m = 6;
    else if (u == 'G')
      m = 9;
    else if (u == 'T')
      m = 12;
    else if (u == 'P')
      m = 15;
    else if (u == 'E')
      m = 18;
    else if (u != 'B') {
      *err = "strict_si_cast: unit prefix not recognized";
      return 0;
    }

    if (m >= 3)
      n = str.substr(0, str.length() -1);
  }

  long long ll = strict_strtoll(n, 10, err);
  if (ll < 0 && !std::numeric_limits<T>::is_signed) {
    *err = "strict_sistrtoll: value should not be negative";
    return 0;
  }
  using promoted_t = typename std::common_type<decltype(ll), T>::type;
  auto v = static_cast<promoted_t>(ll);
  auto coefficient = static_cast<promoted_t>(powl(10, m));
  if (v != std::clamp(v,
		      (static_cast<promoted_t>(std::numeric_limits<T>::min()) /
		       coefficient),
		      (static_cast<promoted_t>(std::numeric_limits<T>::max()) /
		       coefficient))) {
    *err = "strict_sistrtoll: value out of range";
    return 0;
  }
  return v * coefficient;
}

template int strict_si_cast<int>(std::string_view str, std::string *err);
template long strict_si_cast<long>(std::string_view str, std::string *err);
template long long strict_si_cast<long long>(std::string_view str, std::string *err);
template uint64_t strict_si_cast<uint64_t>(std::string_view str, std::string *err);
template uint32_t strict_si_cast<uint32_t>(std::string_view str, std::string *err);

uint64_t strict_sistrtoll(std::string_view str, std::string *err)
{
  return strict_si_cast<uint64_t>(str, err);
}

uint64_t strict_sistrtoll(const char *str, std::string *err)
{
  return strict_si_cast<uint64_t>(str, err);
}

template<typename T>
T strict_si_cast(const char *str, std::string *err)
{
  return strict_si_cast<T>(std::string_view(str), err);
}

template int strict_si_cast<int>(const char *str, std::string *err);
template long strict_si_cast<long>(const char *str, std::string *err);
template long long strict_si_cast<long long>(const char *str, std::string *err);
template uint64_t strict_si_cast<uint64_t>(const char *str, std::string *err);
template uint32_t strict_si_cast<uint32_t>(const char *str, std::string *err);
