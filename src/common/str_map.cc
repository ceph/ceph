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

int get_str_map(const string &str,
                ostream &ss,
                map<string,string> *str_map)
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
    // fallback to key=value format

    list<string> pairs;
    get_str_list(str, "\t\n ", pairs);
    for (list<string>::iterator i = pairs.begin(); i != pairs.end(); i++) {
      size_t equal = i->find('=');
      if (equal == string::npos)
	(*str_map)[*i] = string();
      else {
	const string key = i->substr(0, equal);
	equal++;
	const string value = i->substr(equal);
	(*str_map)[key] = value;
      }
    }
  }
  return 0;
}
