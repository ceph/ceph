
#include "LogStream.h"
#include "MDS.h"
#include "LogEvent.h"

#include "events/EString.h"
#include "events/EInodeUpdate.h"

#include <iostream>
using namespace std;

#include "include/config.h"
#define  dout(l)    if (l<=DEBUG_LEVEL) cout << "mds" << mds->get_nodeid() << ".logstream "
#define  dout2(l)    if (1<=DEBUG_LEVEL) cout

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
	  if (reading_block) {
		dout(5) << "read_next already reading log head from disk, offset " << cur_pos << endl;
	  } else {
		dout(5) << "read_next reading log head from disk, offset " << cur_pos << endl;
		// nope.  read a chunk
		buf_start = cur_pos;
		buffer.clear();
		reading_block = true;
		mds->osd_read(osd, oid,
					  LOGSTREAM_READ_INC, cur_pos,
					  &buffer, 
					  new C_LS_ReadNext(this, le, c));
	  }
	  return 0;
	}
	step = 2;
  }


  if (step == 2) {
	reading_block = false;

	// decode event
	unsigned off = cur_pos-buf_start;
	__uint32_t type, length;
	buffer.copy(off, sizeof(__uint32_t), (char*)&type);
	buffer.copy(off+sizeof(__uint32_t), sizeof(__uint32_t), (char*)&length);
	off += sizeof(type) + sizeof(length);

	dout(5) << "read_next got event type " << type << " size " << length << " at log offset " << cur_pos << endl;
	cur_pos += sizeof(type) + sizeof(length) + length;

	switch (type) {
	  
	case EVENT_STRING:  // string
	  *le = new EString(buffer.substr(off,length));
	  break;
	  
	case EVENT_INODEUPDATE:
	  *le = new EInodeUpdate(buffer.substr(off,length));
	  break;

	default:
	  dout(1) << "uh oh, unknown event type " << type << endl;
	  assert(0);
	}
	
	// finish
	if (c) {
	  c->finish(0);
	  delete c;
	}
  }
}
