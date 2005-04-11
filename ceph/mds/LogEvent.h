
#ifndef __LOGEVENT_H
#define __LOGEVENT_H

#include <stdlib.h>
#include <string>
#include <ext/rope>
using namespace std;

#define EVENT_STRING       1
#define EVENT_INODEUPDATE  2
#define EVENT_UNLINK       3

// generic log event
class LogEvent {
 protected:
  int type;

 public:
  LogEvent(int t) {
	type = t;
  }
  
  int get_type() { return type; }

  virtual crope get_payload() = 0;   // children overload this

  crope get_serialized() {
	crope s;

	// type
	__uint32_t ptype = type;
	s.append((char*)&ptype, sizeof(ptype));

	// len+payload
	crope payload = get_payload();
	__uint32_t plen = payload.length();
	s.append((char*)&plen, sizeof(plen));
	s.append(payload);
	return s;
  }
  
  virtual bool obsolete(MDS *m) {
	return true;
  }

  virtual void retire(MDS *m, Context *c) {
	c->finish(0);
	delete c;
  }
};

#endif
