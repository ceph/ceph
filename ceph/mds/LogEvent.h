
#ifndef __LOGEVENT_H
#define __LOGEVENT_H

#include <stdlib.h>
#include <string>
using namespace std;

#define EVENT_STRING       1
#define EVENT_INODEUPDATE  2

// generic log event
class LogEvent {
 protected:
  int type;
  char *serial;

 public:
  LogEvent(int t) {
	type = t;
  }
  ~LogEvent() {
	if (serial) { delete serial; serial = 0; }
  }
  
  int get_type() { return type; }

  // note: LogEvent owns serialized buffer
  // leave 8 leading bytes free for size/type header,
  // filled in by writer.
  virtual int serialize(char **buf, size_t *len) = 0;

  virtual bool obsolete(MDS *m) {
	return true;
  }

  virtual void retire(MDS *m, Context *c) {
	c->finish(0);
	delete c;
  }
};

#endif
