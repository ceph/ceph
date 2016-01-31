// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2013 Cloudwatt <libre.licensing@cloudwatt.com>
 *
 * Author: Loic Dachary <loic@dachary.org>
 *
 *  This library is free software; you can redistribute it and/or
 *  modify it under the terms of the GNU Lesser General Public
 *  License as published by the Free Software Foundation; either
 *  version 2.1 of the License, or (at your option) any later version.
 * 
 */

#include <errno.h>

#include "include/str_map.h"
#include "include/str_list.h"

#include "json_spirit/json_spirit.h"

using namespace std;

int get_json_str_map(
    const string &str,
    ostream &ss,
    map<string,string> *str_map,
    bool fallback_to_plain)
{
  json_spirit::mValue json;
  try {
    // try json parsing first

    json_spirit::read_or_throw(str, json);

    if (json.type() != json_spirit::obj_type) {
      ss << str << " must be a JSON object but is of type "
	 << json.type() << " instead";
      return -EINVAL;
    }

    json_spirit::mObject o = json.get_obj();

    for (map<string, json_spirit::mValue>::iterator i = o.begin();
	 i != o.end();
	 ++i) {
      (*str_map)[i->first] = i->second.get_str();
    }
  } catch (json_spirit::Error_position &e) {
    if (fallback_to_plain) {
      // fallback to key=value format
      get_str_map(str, str_map, "\t\n ");
    } else {
      return -EINVAL;
    }
  }
  return 0;
}
string trim(const string& str) {
  size_t start = 0;
  size_t end = str.size() - 1;
  while (isspace(str[start]) != 0 && start <= end) {
    ++start;
  }
  while (isspace(str[end]) != 0 && start <= end) {
    --end;
  }
  if (start <= end) {
    return str.substr(start, end - start + 1);
  }
  return string();
}

int get_str_map(
    const string &str,
    map<string,string> *str_map,
    const char *delims)
{
  list<string> pairs;
  get_str_list(str, delims, pairs);
  for (list<string>::iterator i = pairs.begin(); i != pairs.end(); ++i) {
    size_t equal = i->find('=');
    if (equal == string::npos)
      (*str_map)[*i] = string();
    else {
      const string key = trim(i->substr(0, equal));
      equal++;
      const string value = trim(i->substr(equal));
      (*str_map)[key] = value;
    }
  }
  return 0;
}

string get_str_map_value(
    const map<string,string> &str_map,
    const string &key,
    const string *def_val)
{
  map<string,string>::const_iterator p = str_map.find(key);

  // key exists in str_map
  if (p != str_map.end()) {
    // but value is empty
    if (p->second.empty())
      return p->first;
    // and value is not empty
    return p->second;
  }

  // key DNE in str_map and def_val was specified
  if (def_val != NULL)
    return *def_val;

  // key DNE in str_map, no def_val was specified
  return string();
}

string get_str_map_key(
    const map<string,string> &str_map,
    const string &key,
    const string *fallback_key)
{
  map<string,string>::const_iterator p = str_map.find(key);
  if (p != str_map.end())
    return p->second;

  if (fallback_key != NULL) {
    p = str_map.find(*fallback_key);
    if (p != str_map.end())
      return p->second;
  }
  return string();
}

// This function's only purpose is to check whether a given map has only
// ONE key with an empty value (which would mean that 'get_str_map()' read
// a map in the form of 'VALUE', without any KEY/VALUE pairs) and, in such
// event, to assign said 'VALUE' to a given 'def_key', such that we end up
// with a map of the form "m = { 'def_key' : 'VALUE' }" instead of the
// original "m = { 'VALUE' : '' }".
int get_conf_str_map_helper(
    const string &str,
    ostringstream &oss,
    map<string,string> *m,
    const string &def_key)
{
  int r = get_str_map(str, m);

  if (r < 0) {
    return r;
  }

  if (r >= 0 && m->size() == 1) {
    map<string,string>::iterator p = m->begin();
    if (p->second.empty()) {
      string s = p->first;
      m->erase(s);
      (*m)[def_key] = s;
    }
  }
  return r;
}
