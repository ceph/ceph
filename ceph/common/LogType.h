#ifndef __LOGTYPE_H
#define __LOGTYPE_H

#include "types.h"
#include <ext/hash_map>
#include <string>
#include <fstream>
using namespace std;

class LogType {
 protected:
  vector<string>   keys;
  vector<string>   inc_keys;
  vector<string>   set_keys;

  friend class Logger;

 public:
  void add_inc(char *s) {
	string name = s;
	add_inc(name);
  }
  void add_inc(string& key) {
    if (have_key(key)) return;
	keys.push_back(key);
	inc_keys.push_back(key);
  }
  void add_set(char *s) {
	string name = s;
	add_set(name);
  }
  void add_set(string& key){
	if (have_key(key)) return;
	keys.push_back(key);
	set_keys.push_back(key);
  }
  bool have_key(char *s) {
	string n = s;
	return have_key(n);
  }
  bool have_key(string& key) {
	for (vector<string>::iterator it = keys.begin(); it != keys.end(); it++) {
	  if (*it == key) return true;
	}
	return false;
  }

};

#endif
