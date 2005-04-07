#ifndef __LOGGER_H
#define __LOGGER_H

#include "types.h"
#include <string>
#include <fstream>
using namespace std;

#include <ext/hash_map>
using namespace __gnu_cxx;

class LogType;
class Logger {
 protected:
  hash_map<string, long> vals;

  LogType *type;

  double start;
  double last_logged;
  double interval;
  int wrote_header;

  string filename;

  ofstream out;
  bool open;

 public:
  Logger(string& fn, LogType *type);
  ~Logger();

  long inc(char *s, long v = 1);
  long inc(string& key, long v = 1);
  long set(char *s, long v);
  long set(string& key, long v);
  long get(char *s);
  long get(string& key);

  void flush(bool force = false);
};

#endif
