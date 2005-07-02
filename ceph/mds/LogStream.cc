
#include "LogStream.h"
#include "MDS.h"
#include "LogEvent.h"

#include "osd/Filer.h"

#include "events/EString.h"
#include "events/EInodeUpdate.h"
#include "events/EUnlink.h"
#include "events/EAlloc.h"

#include <iostream>
using namespace std;

#include "include/config.h"
#undef dout
#define  dout(l)    if (mds->get_nodeid() == 0 && (l<=g_conf.debug || l<=g_conf.debug_mds_log)) cout << "mds" << mds->get_nodeid() << ".logstream "






// ----------------------------
// writing

class C_LS_Append : public Context {
  LogStream *ls;
  off_t off;
public:
  C_LS_Append(LogStream *ls, off_t off) {
	this->ls = ls;
	this->off = off;
  }
  void finish(int r) {
	ls->_append_2(off);
  }
};


off_t LogStream::append(LogEvent *e)
{
  // serialize
  bufferlist bl;
  e->encode(bl);

  size_t elen = bl.length();
  
  // append
  dout(15) << "append event type " << e->get_type() << " size " << elen << " at log offset " << append_pos << endl;
  
  off_t off = append_pos;
  append_pos += elen;
  
  //dout(15) << "write buf was " << write_buf.length() << " bl " << write_buf << endl;
  write_buf.claim_append(bl);
  //dout(15) << "write buf now " << write_buf.length() << " bl " << write_buf << endl;

  return off;
}

void LogStream::_append_2(off_t off)
{
  dout(15) << "sync_pos now " << off << " skew " << off % g_conf.mds_log_pad_entry << endl;
  sync_pos = off;

  // discard written bufferlist
  assert(writing_buffers.count(off) == 1);
  delete writing_buffers[off];
  writing_buffers.erase(off);
  
  // wake up waiters
  map< off_t, list<Context*> >::iterator it = waiting_for_sync.begin();
  while (it != waiting_for_sync.end()) {
	if (it->first > sync_pos) break;
	
	// wake them up!
	dout(15) << it->second.size() << " waiters at offset " << it->first << " <= " << sync_pos << endl;
	for (list<Context*>::iterator cit = it->second.begin();
		 cit != it->second.end();
		 cit++) 
	  mds->finished_queue.push_back(*cit);
	
	// continue
	waiting_for_sync.erase(it++);
  }
}


void LogStream::wait_for_sync(Context *c, off_t off) 
{
  if (off == 0) off = append_pos;
  assert(off > sync_pos);
  
  dout(15) << "sync " << c << " waiting for " << off << "  (sync_pos currently " << sync_pos << ")" << endl;
  waiting_for_sync[off].push_back(c);
  
  // initiate flush now?  (since we have a waiter...)
  if (autoflush) flush();
}

void LogStream::flush()
{
  // write to disk
  if (write_buf.length()) {
	dout(15) << "flush flush_pos " << flush_pos << " < append_pos " << append_pos << ", writing " << write_buf.length() << " bytes" << endl;
	
	assert(write_buf.length() == append_pos - flush_pos);
	
	// tuck writing buffer away until write finishes
	writing_buffers[append_pos] = new bufferlist;
	writing_buffers[append_pos]->claim(write_buf);

	// write it
	mds->filer->write(log_ino, 
					  g_OSD_MDLogLayout,
					  writing_buffers[append_pos]->length(), flush_pos,
					  *writing_buffers[append_pos],
					  0,
					  new C_LS_Append(this, append_pos));

	flush_pos = append_pos;
  } else {
	dout(15) << "flush flush_pos " << flush_pos << " == append_pos " << append_pos << ", nothing to do" << endl;
  }

}






// -------------------------------------------------
// reading


LogEvent *LogStream::get_next_event()
{
  if (read_buf.length() < 2*sizeof(__uint32_t)) 
	return 0;
  
  // parse type, length
  int off = 0;
  __uint32_t type, length;
  read_buf.copy(off, sizeof(__uint32_t), (char*)&type);
  off += sizeof(__uint32_t);
  read_buf.copy(off, sizeof(__uint32_t), (char*)&length);
  off += sizeof(__uint32_t);

  dout(15) << "getting next event from " << read_pos << ", type " << type << ", size " << length << endl;

  assert(type > 0);

  if (read_buf.length() < off + length)
	return 0;
  
  // create event
  LogEvent *le;
  switch (type) {
  case EVENT_STRING:  // string
	le = new EString();
	break;
	
  case EVENT_INODEUPDATE:
	le = new EInodeUpdate();
	break;
	
  case EVENT_UNLINK:
	le = new EUnlink();
	break;
	
  case EVENT_ALLOC:
	le = new EAlloc();
	break;

  default:
	dout(1) << "uh oh, unknown event type " << type << endl;
	assert(0);
  }

  // decode
  le->decode_payload(read_buf, off);
  off = sizeof(type) + sizeof(length) + length;  // advance past any padding that wasn't decoded..

  // discard front of read_buf
  read_pos += off;
  read_buf.splice(0, off);  

  dout(15) << "get_next_event got event, read_pos now " << read_pos << " (append_pos is " << append_pos << ")" << endl;

  // ok!
  return le;
}



class C_LS_ReadChunk : public Context {
public:
  bufferlist bl;
  LogStream *ls;

  C_LS_ReadChunk(LogStream *ls) {
	this->ls = ls;
  }
  void finish(int result) {
	ls->_did_read(bl);
  }
};


void LogStream::wait_for_next_event(Context *c)
{
  // add waiter
  if (c) waiting_for_read.push_back(c);

  // issue read
  off_t tail = read_pos + read_buf.length();
  size_t size = g_conf.mds_log_read_inc;
  if (tail + size > sync_pos) {
	size = sync_pos - tail;
	dout(15) << "wait_for_next_event ugh.. read_pos is " << read_pos << ", tail is " << tail << ", sync_pos only " << sync_pos << ", flush_pos " << flush_pos << ", append_pos " << append_pos << endl;
	assert(size > 0);   // bleh, wait for sync, etc.
  }

  dout(15) << "wait_for_next_event reading from pos " << tail << " len " << size << endl;
  C_LS_ReadChunk *readc = new C_LS_ReadChunk(this);
  mds->filer->read(log_ino,  
				   g_OSD_MDLogLayout,
				   g_conf.mds_log_read_inc, tail,
				   &readc->bl,
				   readc);
}


void LogStream::_did_read(bufferlist& blist)
{
  dout(15) << "_did_read got " << blist.length() << " bytes at offset " << (read_pos + read_buf.length()) << endl;
  read_buf.claim_append(blist);

  list<Context*> finished;
  finished.splice(finished.begin(), waiting_for_read);
  finish_contexts(finished, 0);
}

