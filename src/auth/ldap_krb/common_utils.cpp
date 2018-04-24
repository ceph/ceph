// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2017 Daniel Oliveira <doliveira@suse.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

/* Include order and names:
 * a) Immediate related header
 * b) C libraries (if any),
 * c) C++ libraries,
 * d) Other support libraries
 * e) Other project's support libraries
 *
 * Within each section the includes should
 * be ordered alphabetically.
 */

#include "common_utils.hpp"

#include <cstring>
#include <iterator>
#include <regex>


namespace common_utils
{

std::string str_trim(const std::string& str_to_trim)
{
  const auto wsfront = std::find_if_not(std::begin(str_to_trim),
                                        std::end(str_to_trim),
                                        [](int c) {
                                          return std::isspace(c);
                                        });
  const auto wsback = std::find_if_not(std::rbegin(str_to_trim),
                                       std::rend(str_to_trim),
                                       [](int c) {
                                          return std::isspace(c);
                                       }).base();
  return (wsback <= wsfront ? std::string() : std::string(wsfront,
                                                          wsback));
}

/* C++17 proper conversion.
CharT const * from std::basic_string<CharT>

  std::string const cstr = { "..." };
  char const * p = cstr.data(); // or .c_str()


CharT * from std::basic_string<CharT>

  std::string str = { "..." };
  char * p = str.data();

  //------
  Given say...
  std::string x = "hello";

  Getting a `char *` or `const char*` from a `string`

  How to get a character pointer that's valid while x remains in scope and isn't modified further

  C++11 simplifies things; the following all give access to the same internal string buffer:

  const char* p_c_str = x.c_str();
  const char* p_data  = x.data();
  const char* p_x0    = &x[0];

  char* p_x0_rw = &x[0];  // compiles iff x is not const...
  //------
*/

auto from_const_str_to_char(const std::string& str_to_convert)
{
  return str_to_convert.c_str();

  /* If we need a vector<char>
  std::vector<char> writable_copy(str_to_convert.data(),
                                  (str_to_convert.data() +
                                   str_to_convert.size() + 1u));
  // char* c = &writable_copy[0];
  //or:  &*writable.begin()
  return (writable_copy.data());
   */
}

auto from_const_ptr_char_to_string(const char* str_to_convert)
{
  std::string tmp_str(str_to_convert);
  return tmp_str;
}

auto duplicate_ptr_char(const char* str_to_convert)
{
  auto str_size(std::strlen(str_to_convert));
  char* str_copy = (new char[str_size]);
  std::copy(str_to_convert, str_to_convert + str_size, str_copy);
  str_copy[str_size] = ZERO;
  return str_copy;
}

// Based on RFC1123, it allows hostname labels to start with digits.
// As opposed to old/original RFC952.
auto is_valid_hostname(const std::string& host_name_rfc1123)
{
  const std::string host_fqdn_filter("^([a-z0-9]|[a-z0-9][a-z0-9\\"
                                         "-]{0,61}[a-z0-9])\\"
                                         "(\\.([a-z0-9]|[a-z0-9][a-z0-9"
                                         "\\-]{0,61}[a-z0-9]))*$");
  std::regex domain_regex(host_fqdn_filter, std::regex_constants::icase);
  return (std::regex_search(host_name_rfc1123, domain_regex));
}

// Based on RFC1918
auto is_valid_ipaddress(const std::string& host_ipaddress)
{
  const std::string ipaddr_filter("^(([01]?[0-9]?[0-9]|2([0-4][0-9]|5[0-5]))\\"
                                      ".){3}([01]?[0-9]?[0-9]|2([0-4][0-9]|5[0-5]))$");
  std::regex ipaddr_regex(ipaddr_filter);
  return (std::regex_search(host_ipaddress, ipaddr_regex));
}


// trap error code and can be used to set breakpoints.
uint32_t auth_trap_error(const uint32_t  error_code)
{
  return (error_code);
}


timeval
from_chrono_to_timeval(const system_clock::time_point& src_time_point)
{
  using namespace std::chrono;

  auto secs = time_point_cast<seconds>(src_time_point);
  if (secs > src_time_point) {
    secs = (secs - seconds{1});
  }
  auto micro_secs = duration_cast<microseconds>(src_time_point - secs);
  timeval time_val;
  time_val.tv_sec  = secs.time_since_epoch().count();
  time_val.tv_usec = micro_secs.count();
  return time_val;
}

  
system_clock::time_point
from_timeval_to_chrono(const timeval& time_val)
{
  using namespace std::chrono;

  return system_clock::time_point{seconds{time_val.tv_sec} +
                                  microseconds{time_val.tv_usec}};
}


}   //-- namespace common_utils

// ----------------------------- END-OF-FILE --------------------------------//
