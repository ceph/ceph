
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
  EString() :
	LogEvent(EVENT_STRING) {
  }

  void decode_payload(bufferlist& bl, int& off) {
	event = bl.c_str() + off;
	off += event.length() + 1;
  }
  
  void encode_payload(bufferlist& bl) {
	bl.append(event.c_str(), event.length()+1);
  }
};

#endif
