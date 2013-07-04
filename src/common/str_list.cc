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
using std::set;
using std::list;

static bool get_next_token(const string &s, size_t& pos, const char *delims, string& token)
{
  int start = s.find_first_not_of(delims, pos);
  int end;

  if (start < 0){
    pos = s.size();
    return false;
  }

  end = s.find_first_of(delims, start);
  if (end >= 0)
    pos = end + 1;
  else {
    pos = end = s.size();
  }

  token = s.substr(start, end - start);
  return true;
}

void get_str_list(const string& str, const char *delims, list<string>& str_list)
{
  size_t pos = 0;
  string token;

  str_list.clear();

  while (pos < str.size()) {
    if (get_next_token(str, pos, delims, token)) {
      if (token.size() > 0) {
        str_list.push_back(token);
      }
    }
  }
}

void get_str_list(const string& str, list<string>& str_list)
{
  const char *delims = ";,= \t";
  return get_str_list(str, delims, str_list);
}

void get_str_vec(const string& str, const char *delims, vector<string>& str_vec)
{
  size_t pos = 0;
  string token;
  str_vec.clear();

  while (pos < str.size()) {
    if (get_next_token(str, pos, delims, token)) {
      if (token.size() > 0) {
        str_vec.push_back(token);
      }
    }
  }
}

void get_str_vec(const string& str, vector<string>& str_vec)
{
  const char *delims = ";,= \t";
  return get_str_vec(str, delims, str_vec);
}

void get_str_set(const string& str, const char *delims, set<string>& str_set)
{
  size_t pos = 0;
  string token;

  str_set.clear();

  while (pos < str.size()) {
    if (get_next_token(str, pos, delims, token)) {
      if (token.size() > 0) {
        str_set.insert(token);
      }
    }
  }
}

void get_str_set(const string& str, set<string>& str_set)
{
  const char *delims = ";,= \t";
  return get_str_set(str, delims, str_set);
}
