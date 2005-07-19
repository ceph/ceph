#ifndef __LOGGER_H
#define __LOGGER_H

#include "include/types.h"
#include "Clock.h"
#include "Mutex.h"

#include <string>
#include <fstream>
using namespace std;

#include <ext/hash_map>
using namespace __gnu_cxx;

#include "LogType.h"




class Logger {
 protected:
  hash_map<const char*, long, hash<const char*>, eqstr> vals;
  hash_map<const char*, double, hash<const char*>, eqstr> fvals;

  //Mutex lock;
  LogType *type;

  utime_t start;
  int last_logged;
  int interval;
  int wrote_header;
  int wrote_header_last;

  string filename;

  ofstream out;
  bool open;

 public:
  Logger(string fn, LogType *type);
  ~Logger();

  long inc(const char *s, long v = 1);
  long set(const char *s, long v);
  long get(const char *s);

  double fset(const char *s, double v);
  double finc(const char *s, double v);
  
  void flush(bool force = false);
};

#endif
