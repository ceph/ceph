
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
  EString(crope s) :
	LogEvent(EVENT_STRING) {
	event = s.c_str();
  }
  
  // note: LogEvent owns serialized buffer
  virtual crope get_payload() {
	return crope(event.c_str());
  }
};

#endif
