
#ifndef __ESTRING_H
#define __ESTRING_H

#include <stdlib.h>
#include <string>
using namespace std;

#include "../LogEvent.h"

// generic log event
class EString : public LogEvent {
 protected:
  string event;

 public:
  EString(string e) :
	LogEvent(EVENT_STRING) {
	event = e;
  }
  EString(char *e) :
	LogEvent(EVENT_STRING) {
	event = e;
  }
  
  // note: LogEvent owns serialized buffer
  virtual int serialize() {
	int len = event.length()+1+4+4;
	char *buf = alloc_serial_buf( len );
	memcpy(buf, event.c_str(), len);
	return 0;
  }
};

#endif
