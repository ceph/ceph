
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
  e->serialize();
  char *buf = e->get_serial_buf();
  long buflen = e->get_serial_len();
  
  *((__uint32_t*)buf) = e->get_type();
  *((__uint32_t*)buf+1) = buflen;
  
  // advance ptr for later
  append_pos += buflen;
  
  // submit write
  mds->osd_write(osd, oid,
				 buflen, append_pos-buflen,
				 buf,
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
	// alloc buffer?
	if (!buf) {
	  buf = new char[READ_INC];
	  buf_valid = 0;  // no data yet
	  buf_start = 0;
	}
	
	// does buffer have what we want?
	if (buf_start > cur_pos ||
		buf_start+buf_valid < cur_pos+4) {
	  // nope.  re-read a chunk
	  buf_start = cur_pos;
	  mds->osd_read(osd, oid,
					READ_INC, cur_pos,
					buf, 
					&buf_valid,
					new C_LS_ReadNext(this, le, c));
	  return 0;
	}
	step = 2;
  }


  if (step == 2) {
	// decode event
	unsigned off = cur_pos-buf_start;
	__uint32_t type = *((__uint32_t*)(buf+off));
	switch (type) {

	case EVENT_STRING:  // string
	  cout << "it's a string event" << endl;
	  *le = new EString(buf + off + 8);
	  break;
	  
	case EVENT_INODEUPDATE:
	  cout << "read inodeupdate event" << endl;
	  *le = new EInodeUpdate(buf + off + 8);
	  break;

	default:
	  cout << "uh oh, unknown event type " << type << endl;
	}
	
	// finish
	if (c) {
	  c->finish(0);
	  delete c;
	}
  }
}
