
#include "LogStream.h"
#include "MDS.h"
#include "LogEvent.h"

#include "events/EString.h"
#include "events/EInodeUpdate.h"

#include <iostream>
using namespace std;


// writing

int LogStream::append(LogEvent *e, Context *c)
{
  // serialize
  crope buffer = e->get_serialized();
  size_t buflen = buffer.length();
  
  // advance ptr for later
  append_pos += buffer.length();
  
  // submit write
  mds->osd_write(osd, oid,
				 buflen, append_pos-buflen,
				 buffer,
				 0,
				 c);
  return 0;
}


// reading

#define READ_INC  1024    // make this bigger than biggest event

class C_LS_ReadNext : public Context {
  LogStream *ls;
  LogEvent **le;
  Context *c;
public:
  C_LS_ReadNext(LogStream *ls, LogEvent **le, Context *c) {
	this->ls = ls;
	this->le = le;
	this->c = c;
  }
  void finish(int result) {
	ls->read_next(le,c,2);
  }
};

int LogStream::read_next(LogEvent **le, Context *c, int step) 
{
  if (step == 1) {
	// does buffer have what we want?
	if (buf_start > cur_pos ||
		buf_start+buffer.length() < cur_pos+4) {
	  // nope.  read a chunk
	  buf_start = cur_pos;
	  buffer.clear();
	  mds->osd_read(osd, oid,
					READ_INC, cur_pos,
					&buffer, 
					new C_LS_ReadNext(this, le, c));
	  return 0;
	}
	step = 2;
  }


  if (step == 2) {
	// decode event
	unsigned off = cur_pos-buf_start;
	__uint32_t type, length;
	buffer.copy(off, sizeof(__uint32_t), (char*)&type);
	buffer.copy(off+sizeof(__uint32_t), sizeof(__uint32_t), (char*)&length);
				
	switch (type) {
	  
	case EVENT_STRING:  // string
	  cout << "it's a string event" << endl;
	  *le = new EString(buffer.substr(off,length));
	  break;
	  
	case EVENT_INODEUPDATE:
	  cout << "read inodeupdate event" << endl;
	  *le = new EInodeUpdate(buffer.substr(off,length));
	  break;

	default:
	  cout << "uh oh, unknown event type " << type << endl;
	  assert(0);
	}
	
	// finish
	if (c) {
	  c->finish(0);
	  delete c;
	}
  }
}
