// -*- mode:C++; tab-width:4; c-basic-offset:2; indent-tabs-mode:t -*- 
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 * 
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 * 
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA  02110-1301  USA
 */

#ifndef __LOGTYPE_H
#define __LOGTYPE_H

#include "include/types.h"

#include <string>
#include <fstream>
using namespace std;
#include <ext/hash_map>
#include <ext/hash_set>
using namespace __gnu_cxx;

#include "Mutex.h"

class LogType {
 protected:
  hash_map<__uint32_t, int> keymap;  
  vector<const char*>   keys;
  set<int>              inc_keys;

  int version;

  friend class Logger;

 public:
  LogType() {
	version = 1;
  }
  int add_key(const char* key, bool is_inc) {
	int i = lookup_key(key);
	if (i >= 0) return i;

	i = keys.size();
	keys.push_back(key);

	__uint32_t p = (__uint32_t)key;
	keymap[p] = i;
	if (is_inc) inc_keys.insert(i);

	version++;
	return i;
  }
  int add_inc(const char* key) {
	return add_key(key, true);
  }
  int add_set(const char *key) {
	return add_key(key, false);
  }
  
  bool have_key(const char* key) {
	return lookup_key(key) < 0;
  }

  int lookup_key(const char* key) {
	__uint32_t p = (__uint32_t)key;

	if (keymap.count(p)) 
	  return keymap[p];

	for (unsigned i=0; i<keys.size(); i++)
	  if (strcmp(keys[i], key) == 0) {
		keymap[p] = i;
		return i; 
	  }
	return -1;
  }

};

#endif
