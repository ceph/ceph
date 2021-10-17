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

#include "include/str_map.h"
#include "include/str_list.h"

#include <boost/algorithm/string.hpp>

#include "json_spirit/json_spirit.h"

using namespace std;

int get_json_str_map(
    const string &str,
    ostream &ss,
    str_map_t *str_map,
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
  return boost::algorithm::trim_copy_if(
    str,
    [](unsigned char c) {
      return std::isspace(c);
    });
}

int get_str_map(
    const string &str,
    str_map_t* str_map,
    const char *delims)
{
  auto pairs = get_str_list(str, delims);
  for (const auto& pr : pairs) {
    size_t equal = pr.find('=');
    if (equal == string::npos)
      (*str_map)[pr] = string();
    else {
      const string key = trim(pr.substr(0, equal));
      equal++;
      const string value = trim(pr.substr(equal));
      (*str_map)[key] = value;
    }
  }
  return 0;
}

str_map_t get_str_map(
  const string& str,
  const char* delim)
{
  auto pairs = get_str_list(str, delim);
  str_map_t str_map;

  for (const auto& pr : pairs) {
    auto equal = pr.find('=');

    // is the format 'K=V' or just 'K'?
    if (equal == std::string::npos) {
      str_map[pr] = std::string{};
    } else {
      const string key = trim(pr.substr(0, equal));
      equal++;
      const string value = trim(pr.substr(equal));
      str_map[key] = value;
    }
  }

  return str_map;
}

string get_str_map_value(
    const str_map_t &str_map,
    const string &key,
    const string *def_val)
{
  auto p = str_map.find(key);

  // key exists in str_map
  if (p != str_map.end()) {
    // but value is empty
    if (p->second.empty())
      return p->first;
    // and value is not empty
    return p->second;
  }

  // key DNE in str_map and def_val was specified
  if (def_val != nullptr)
    return *def_val;

  // key DNE in str_map, no def_val was specified
  return string();
}

string get_str_map_key(
    const str_map_t &str_map,
    const string &key,
    const string *fallback_key)
{
  auto p = str_map.find(key);
  if (p != str_map.end())
    return p->second;

  if (fallback_key != nullptr) {
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
    str_map_t* str_map,
    const string &default_key)
{
  get_str_map(str, str_map);

  if (str_map->size() == 1) {
    auto p = str_map->begin();
    if (p->second.empty()) {
      string s = p->first;
      str_map->erase(s);
      (*str_map)[default_key] = s;
    }
  }
  return 0;
}

std::string get_value_via_strmap(
  const string& conf_string,
  std::string_view default_key)
{
  auto mp = get_str_map(conf_string);
  if (mp.size() != 1) {
    return "";
  }

  // if the one-elem "map" is of the form { 'value' : '' }
  // replace it with { 'default_key' : 'value' }
  const auto& [k, v] = *(mp.begin());
  if (v.empty()) {
    return k;
  }
  return v;
}

std::string get_value_via_strmap(
  const string& conf_string,
  const string& key,
  std::string_view default_key)
{
  auto mp = get_str_map(conf_string);
  if (mp.size() != 1) {
    return std::string{};
  }

  // if the one-elem "map" is of the form { 'value' : '' }
  // replace it with { 'default_key' : 'value' }
  const auto& [k, v] = *(mp.begin());
  if (v.empty()) {
    return k;
  }
  if (k == key) {
    return k;
  }
  if (k == default_key) {
    return v;
  }

  return string{};
}
