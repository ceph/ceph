
#ifndef __LOGEVENT_H
#define __LOGEVENT_H

#include <stdlib.h>
#include <string>
using namespace std;

// generic log event
class LogEvent {
 protected:
  string event;

  char *serial;

 public:
  LogEvent(string e) {
	event = e;
  }
  LogEvent(char *e) {
	event = e;
  }
  ~LogEvent() {
	if (serial) { delete serial; serial = 0; }
  }
  
  // note: LogEvent owns serialized buffer
  virtual int serialize(char **buf, size_t *len) {
	*len = event.length()+1+4+4;  // pad with NULL term
	*buf = new char[*len];
	*((__uint32_t*)*buf) = *len;
	*((__uint32_t*)*buf +1) = 1; //EVENT_STRING;
	memcpy(*buf+8, event.c_str(), *len-8);
	return 0;
  }
};

#endif
