// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2009-2010 Dreamhost
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include "include/str_list.h"

using std::string;
using std::vector;
using std::list;
using ceph::for_each_substr;
using namespace std::literals;

void get_str_list(std::string_view str, std::string_view delims,
		  std::list<std::string>& str_list)
{
  str_list.clear();
  for_each_substr(str, delims, [&str_list] (auto token) {
      str_list.emplace_back(token.begin(), token.end());
    });
}

void get_str_list(std::string_view str, std::list<std::string>& str_list)
{
  get_str_list(str, ";,= \t"sv, str_list);
}

std::list<std::string> get_str_list(std::string_view str,
				    std::string_view delims)
{
  std::list<std::string> result;
  get_str_list(str, delims, result);
  return result;
}

void get_str_vec(std::string_view str, std::string_view delims,
		 std::vector<string>& str_vec)
{
  str_vec.clear();
  for_each_substr(str, delims, [&str_vec] (auto token) {
      str_vec.emplace_back(token.begin(), token.end());
    });
}

void get_str_vec(std::string_view str, std::vector<std::string>& str_vec)
{
  get_str_vec(str, ";,= \t"sv, str_vec);
}

std::vector<std::string> get_str_vec(std::string_view str, std::string_view delims)
{
  std::vector<std::string> result;
  get_str_vec(str, delims, result);
  return result;
}
