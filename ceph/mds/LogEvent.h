
#ifndef __LOGEVENT_H
#define __LOGEVENT_H

#include <stdlib.h>
#include <string>
#include <ext/rope>
using namespace std;

#define EVENT_STRING       1
#define EVENT_INODEUPDATE  2
#define EVENT_UNLINK       3
#define EVENT_ALLOC        4

// generic log event
class LogEvent {
 protected:
  int type;

 public:
  LogEvent(int t) {
	type = t;
  }
  
  int get_type() { return type; }

  virtual void encode_payload(bufferlist& bl) = 0;
  virtual void decode_payload(bufferlist& bl, int& off) = 0;

  void encode(bufferlist& bl) {
	// type
	bl.append((char*)&type, sizeof(type));

	// len placeholder
	int len = 0;   // we don't know just yet...
	int off = bl.length();
	bl.append((char*)&type, sizeof(len)); 

	// payload
	encode_payload(bl);
	len = bl.length() - off - sizeof(len);
	bl.copy_in(off, sizeof(len), (char*)&len);
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
