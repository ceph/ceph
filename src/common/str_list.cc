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

#include <iostream>
#include <list>
#include <set>

using std::string;

static bool get_next_token(const std::string &s, size_t& pos, const char *delims, string& token)
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

static bool get_next_token(const std::string &s, size_t& pos, string& token)
{
  const char *delims = ";,= \t";
  return get_next_token(s, pos, delims, token);
}

void get_str_list(const std::string& str, const char *delims, std::list<string>& str_list)
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

void get_str_list(const std::string& str, std::list<string>& str_list)
{
  const char *delims = ";,= \t";
  return get_str_list(str, delims, str_list);
}

void get_str_set(const std::string& str, std::set<std::string>& str_set)
{
  size_t pos = 0;
  string token;

  str_set.clear();

  while (pos < str.size()) {
    if (get_next_token(str, pos, token)) {
      if (token.size() > 0) {
        str_set.insert(token);
      }
    }
  }
}
