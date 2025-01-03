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

void get_str_list(const string& str, const char *delims, list<string>& str_list)
{
  str_list.clear();
  for_each_substr(str, delims, [&str_list] (auto token) {
      str_list.emplace_back(token.begin(), token.end());
    });
}

void get_str_list(const string& str, list<string>& str_list)
{
  const char *delims = ";,= \t";
  get_str_list(str, delims, str_list);
}

list<string> get_str_list(const string& str, const char *delims)
{
  list<string> result;
  get_str_list(str, delims, result);
  return result;
}

void get_str_vec(std::string_view str, const char *delims, vector<string>& str_vec)
{
  str_vec.clear();
  for_each_substr(str, delims, [&str_vec] (auto token) {
      str_vec.emplace_back(token.begin(), token.end());
    });
}

void get_str_vec(std::string_view str, vector<string>& str_vec)
{
  const char *delims = ";,= \t";
  get_str_vec(str, delims, str_vec);
}

vector<string> get_str_vec(std::string_view str, const char *delims)
{
  vector<string> result;
  for_each_substr(str, delims, [&result] (auto token) {
      result.emplace_back(token.begin(), token.end());
    });
  return result;
}
