// -*- mode:C++; tab-width:4; c-basic-offset:2; indent-tabs-mode:t -*- 
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software 
 * Foundation.  See file COPYING.
 * 
 */



#include "LogStream.h"
#include "MDS.h"
#include "LogEvent.h"

#include "osdc/Filer.h"

#include "common/Logger.h"

#include "events/EString.h"
#include "events/EInodeUpdate.h"
#include "events/EDirUpdate.h"
#include "events/EUnlink.h"
#include "events/EAlloc.h"

#include <iostream>
using namespace std;

#include "config.h"
#undef dout
#define  dout(l)    if (l<=g_conf.debug || l<=g_conf.debug_mds_log) cout << "mds" << mds->get_nodeid() << ".logstream "




LogStream::LogStream(MDS *mds, Filer *filer, inodeno_t log_ino, Logger *l) 
{
  this->mds = mds;
  this->filer = filer;
  this->logger = l;
  
  // inode
  memset(&log_inode, 0, sizeof(log_inode));
  log_inode.ino = log_ino;
  log_inode.layout = g_OSD_MDLogLayout;
  
  if (g_conf.mds_local_osd) {
	log_inode.layout.object_layout = OBJECT_LAYOUT_STARTOSD;
	log_inode.layout.osd = mds->get_nodeid() + 10000;   // hack
  }
  
  // wr
  sync_pos = flush_pos = append_pos = 0;
  autoflush = true;
  
  // rd
  read_pos = 0;
  reading = false;
}



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
  dout(15) << g_clock.now() << " sync_pos now " << off << " skew " << off % g_conf.mds_log_pad_entry << endl;
  sync_pos = off;

  // discard written bufferlist
  assert(writing_buffers.count(off) == 1);
  delete writing_buffers[off];
  writing_buffers.erase(off);
  
  utime_t now = g_clock.now();
  now -= writing_latency[off];
  writing_latency.erase(off);
  logger->finc("lsum", (double)now);
  logger->inc("lnum", 1);

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

	writing_latency[append_pos] = g_clock.now();

	// write it
	mds->filer->write(log_inode, 
					  flush_pos, writing_buffers[append_pos]->length(), 
					  *writing_buffers[append_pos],
					  0,
					  new C_LS_Append(this, append_pos), // on ack
					  NULL);                             // on safe

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
	
  case EVENT_DIRUPDATE:
	le = new EDirUpdate();
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



class C_LS_ReadAfterSync : public Context {
public:
  LogStream *ls;
  Context *waitfornext;
  C_LS_ReadAfterSync(LogStream *l, Context *c) : ls(l), waitfornext(c) {}
  void finish(int) {
	ls->wait_for_next_event(waitfornext);
  }
};

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
  if (read_pos == sync_pos) {
	dout(-15) << "waiting for sync before initiating read at " << read_pos << endl;
	wait_for_sync(new C_LS_ReadAfterSync(this, c));
	return;				 
  }

  // add waiter
  if (c) waiting_for_read.push_back(c);

  // issue read
  off_t tail = read_pos + read_buf.length();
  size_t size = g_conf.mds_log_read_inc;
  if (tail + (off_t)size > sync_pos) {
	size = sync_pos - tail;
	dout(15) << "wait_for_next_event ugh.. read_pos is " << read_pos << ", tail is " << tail << ", sync_pos only " << sync_pos << ", flush_pos " << flush_pos << ", append_pos " << append_pos << endl;
	
	if (size == 0) {
	  //	assert(size > 0);   // bleh, wait for sync, etc.
	  // just do it.  communication is ordered, right?   FIXME SOMEDAY this is totally gross blech
	  //size = flush_pos - tail;
	  // read tiny bit, kill some time
	  assert(flush_pos > sync_pos);
	  size = 1;
	}
  }

  dout(15) << "wait_for_next_event reading from pos " << tail << " len " << size << endl;
  C_LS_ReadChunk *readc = new C_LS_ReadChunk(this);
  mds->filer->read(log_inode,  
				   tail, g_conf.mds_log_read_inc, 
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

