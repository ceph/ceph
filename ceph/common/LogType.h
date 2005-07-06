#ifndef __LOGTYPE_H
#define __LOGTYPE_H

#include "include/types.h"

#include <string>
#include <fstream>
using namespace std;
#include <ext/hash_map>
using namespace __gnu_cxx;

#include "Mutex.h"

// for const char* comparisons
struct ltstr
{
  bool operator()(const char* s1, const char* s2) const
  {
    return strcmp(s1, s2) < 0;
  }
};

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
