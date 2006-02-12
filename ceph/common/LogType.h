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
using namespace __gnu_cxx;

#include "Mutex.h"

class LogType {
 protected:
  set<const char*, ltstr>   keyset;
  vector<const char*>   keys;
  vector<const char*>   inc_keys;
  vector<const char*>   set_keys;

  int version;

  friend class Logger;

 public:
  LogType() {
	version = 1;
  }
  void add_inc(const char* key) {
    if (!have_key(key)) {
	  keys.push_back(key);
	  keyset.insert(key);
	  inc_keys.push_back(key);
	  version++;
	}
  }
  void add_set(const char* key){
	if (!have_key(key)) {
	  keys.push_back(key);
	  keyset.insert(key);
	  set_keys.push_back(key);
	  version++;
	}
  }
  bool have_key(const char* key) {
	return keyset.count(key) ? true:false;
  }

};

#endif
