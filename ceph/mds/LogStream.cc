
#include "LogStream.h"
#include "MDS.h"
#include "LogEvent.h"

#include "events/EString.h"
#include "events/EInodeUpdate.h"
#include "events/EUnlink.h"

#include <iostream>
using namespace std;

#include "include/config.h"
#undef dout
#define  dout(l)    if (l<=g_conf.debug) cout << "mds" << mds->get_nodeid() << ".logstream "

// writing

int LogStream::append(LogEvent *e, Context *c)
{
  // serialize
  crope buffer = e->get_serialized();
  size_t buflen = buffer.length();

  dout(10) << "append event type " << e->get_type() << " size " << buflen << " at log offset " << append_pos << endl;

  
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

/*
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
*/

class C_LS_ReadChunk : public Context {
public:
  crope next_bit;
  LogStream *ls;
  LogEvent **le;
  Context *c;
  C_LS_ReadChunk(LogStream *ls, LogEvent **le, Context *c) {
	this->ls = ls;
	this->le = le;
	this->c = c;
  }
  void finish(int result) {
	ls->did_read_bit(next_bit, le, c);
  }
};

void LogStream::did_read_bit(crope& next_bit, LogEvent **le, Context *c) 
{
  // add to our buffer
  buffer.append(next_bit);
  reading_block = false;
  
  // throw off beginning part
  if (buffer.length() > g_conf.mds_log_read_inc*2) {
	int trim = buffer.length() - g_conf.mds_log_read_inc*2;
	buf_start += trim;
	buffer = buffer.substr(trim, buffer.length() - trim);
	dout(10) << "did_read_bit adjusting buf_start now +" << trim << " = " << buf_start << " len " << buffer.length() << endl;
  }
  
  // continue at step 2
  read_next(le, c, 2);
}

int LogStream::read_next(LogEvent **le, Context *c, int step) 
{
  if (step == 1) {
	// does buffer have what we want?
	//if (buf_start > cur_pos ||
	//buf_start+buffer.length() < cur_pos+4) {
	if (buf_start+buffer.length() < cur_pos+ g_conf.mds_log_read_inc/2) {

	  // make sure block is being read
	  if (reading_block) {
		dout(10) << "read_next already reading log head from disk, offset " << cur_pos << endl;
		assert(0);  
	  } else {
		off_t start = buf_start+buffer.length();
		dout(10) << "read_next reading log head from disk, offset " << start << " len " << g_conf.mds_log_read_inc << endl;
		// nope.  read a chunk
		C_LS_ReadChunk *readc = new C_LS_ReadChunk(this, le, c);
		reading_block = true;
		mds->osd_read(osd, oid,
					  g_conf.mds_log_read_inc, start,
					  &readc->next_bit,
					  readc);
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

	dout(10) << "read_next got event type " << type << " size " << length << " at log offset " << cur_pos << endl;
	cur_pos += sizeof(type) + sizeof(length) + length;

	switch (type) {
	  
	case EVENT_STRING:  // string
	  *le = new EString(buffer.substr(off,length));
	  break;
	  
	case EVENT_INODEUPDATE:
	  *le = new EInodeUpdate(buffer.substr(off,length));
	  break;

	case EVENT_UNLINK:
	  *le = new EUnlink(buffer.substr(off,length));
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

	/*
	// any other waiters too!
	list<Context*> finished = waiting_for_read_block;
	waiting_for_read_block.clear();
	for (list<Context*>::iterator it = finished.begin();
		 it != finished.end();
		 it++) {
	  Context *c = *it;
	  if (c) {
		c->finish(0);
		delete c;
	  }
	}
	*/
	
  }
}
