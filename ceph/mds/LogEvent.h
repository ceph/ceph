
#ifndef __LOGEVENT_H
#define __LOGEVENT_H

#include <stdlib.h>
#include <string>
using namespace std;

#define EVENT_STRING       1
#define EVENT_INODEUPDATE  2

// generic log event
class LogEvent {
 private:
  char *serial_buf;
  long serial_len;

 protected:
  int type;

  char *alloc_serial_buf(long size) {
	serial_len = size + 8;
	serial_buf = new char[serial_len];
	return serial_buf + 8;
  }
  
 public:
  LogEvent(int t) {
	type = t;
	serial_buf = 0;
  }
  ~LogEvent() {
	if (serial_buf) { delete serial_buf; serial_buf = 0; }
  }
  
  int get_type() { return type; }
  char *get_serial_buf() { return serial_buf; }
  long get_serial_len() { return serial_len; }

  // note: LogEvent owns serialized buffer
  // leave 8 leading bytes free for size/type header,
  // filled in by LogStream writer.
  virtual int serialize() = 0; //char **buf, size_t *len) = 0;

  virtual bool obsolete(MDS *m) {
	return true;
  }

  virtual void retire(MDS *m, Context *c) {
	c->finish(0);
	delete c;
  }
};

#endif
