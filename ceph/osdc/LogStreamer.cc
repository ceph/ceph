// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
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

#include "LogStreamer.h"

#include "include/Context.h"
#include "common/Logger.h"
#include "msg/Messenger.h"

#include "config.h"
#undef dout
#define dout(x)  if (x <= g_conf.debug || x <= g_conf.debug_objecter) cout << g_clock.now() << " " << objecter->messenger->get_myaddr() << ".logstreamer "
#define derr(x)  if (x <= g_conf.debug || x <= g_conf.debug_objecter) cerr << g_clock.now() << " " << objecter->messenger->get_myaddr() << ".logstreamer "


/***************** WRITING *******************/

class LogStreamer::C_Flush : public Context {
  LogStreamer *ls;
  off_t start;
public:
  C_Flush(LogStreamer *l, off_t s) : ls(l), start(s) {}
  void finish(int r) { ls->_finish_flush(r, start); }
};

void LogStreamer::_finish_flush(int r, off_t start)
{
  assert(r>=0);

  assert(start >= ack_pos);
  assert(start < flush_pos);
  assert(pending_flush.count(start));

  // calc latency?
  if (logger) {
    utime_t lat = g_clock.now();
    lat -= pending_flush[start];
    logger->finc("lsum", lat);
    logger->inc("lnum");
  }

  pending_flush.erase(start);

  // adjust ack_pos
  if (pending_flush.empty())
    ack_pos = flush_pos;
  else
    ack_pos = pending_flush.begin()->first;

  dout(10) << "_finish_flush from " << start
	   << ", pending_flush now " << pending_flush 
	   << ", write positions now " << write_pos << "/" << flush_pos << "/" << ack_pos
	   << endl;

  // kick waiters <= ack_pos
  while (!waitfor_flush.empty()) {
    if (waitfor_flush.begin()->first > ack_pos) break;
    finish_contexts(waitfor_flush.begin()->second);
    waitfor_flush.erase(waitfor_flush.begin());
  }
}


off_t LogStreamer::append_entry(bufferlist& bl, Context *onsync)
{
  dout(10) << "append_entry len " << bl.length() << " to " << write_pos << "~" << (bl.length() + sizeof(size_t)) << endl;
  
  // append
  size_t s = bl.length();
  write_buf.append((char*)&s, sizeof(s));
  write_buf.append(bl);
  write_pos += sizeof(s) + s;

  // flush now?
  if (onsync) 
    flush(onsync);

  return write_pos;
}


void LogStreamer::flush(Context *onsync)
{
  if (write_pos == flush_pos) {
    assert(write_buf.length() == 0);
    dout(10) << "flush nothing to flush, write pointers at " << write_pos << "/" << flush_pos << "/" << ack_pos << endl;

    if (onsync) {
      onsync->finish(0);
      delete onsync;
    }
    return;
  }

  unsigned len = write_pos - flush_pos;
  assert(len == write_buf.length());
  dout(10) << "flush flushing " << flush_pos << "~" << len << endl;

  // submit write for anything pending
  filer.write(inode, flush_pos, len, write_buf, 0,
	      new C_Flush(this, flush_pos), 0);  // flush _start_ pos to _finish_flush
  pending_flush[flush_pos] = g_clock.now();
  
  // adjust pointers
  flush_pos = write_pos;
  write_buf.clear();  

  dout(10) << "flush write pointers now at " << write_pos << "/" << flush_pos << "/" << ack_pos << endl;

  // queue waiter (at _new_ write_pos; will go when reached by ack_pos)
  if (onsync) 
    waitfor_flush[write_pos].push_back(onsync);
}



/***************** READING *******************/


class LogStreamer::C_Read : public Context {
  LogStreamer *ls;
public:
  C_Read(LogStreamer *l) : ls(l) {}
  void finish(int r) { ls->_finish_read(r); }
};

class LogStreamer::C_RetryRead : public Context {
  LogStreamer *ls;
public:
  C_RetryRead(LogStreamer *l) : ls(l) {}
  void finish(int r) { ls->is_readable(); }  // this'll kickstart.
};

void LogStreamer::_finish_read(int r)
{
  assert(r>=0);

  dout(10) << "_finish_read got " << received_pos << "~" << reading_buf.length() << endl;
  received_pos += reading_buf.length();
  read_buf.claim_append(reading_buf);
  assert(received_pos <= requested_pos);
  dout(10) << "_finish_read read_buf now " << read_pos << "~" << read_buf.length() 
	   << ", read pointers " << read_pos << "/" << received_pos << "/" << requested_pos
	   << endl;
  
  if (is_readable()) { // NOTE: this check may read more
    // readable!
    dout(10) << "_finish_read now readable" << endl;
    if (on_readable) {
      Context *f = on_readable;
      on_readable = 0;
      f->finish(0);
      delete f;
    }

    if (read_bl) {
      bool r = try_read_entry(*read_bl);
      assert(r);  // this should have worked.

      // clear state
      Context *f = on_read_finish;
      on_read_finish = 0;
      read_bl = 0;
      
      // do callback
      f->finish(0);
      delete f;
    }
  }
  
  // prefetch?
  _prefetch();
}

/* NOTE: this could be slightly smarter... we could allow
 * multiple reads to be in progress.  e.g., if we prefetch, but
 * then discover we need even more for an especially large entry.
 * i don't think that circumstance will arise particularly often.
 */
void LogStreamer::_issue_read(off_t len)
{
  if (_is_reading()) {
    dout(10) << "_issue_read " << len << " waiting, already reading " 
	     << received_pos << "~" << (requested_pos-received_pos) << endl;
    return;
  } 
  assert(requested_pos == received_pos);

  // stuck at ack_pos?
  assert(requested_pos <= ack_pos);
  if (requested_pos == ack_pos) {
    dout(10) << "_issue_read requested_pos = ack_pos = " << ack_pos << ", waiting" << endl;
    assert(write_pos > requested_pos);
    if (flush_pos == ack_pos)
      flush();
    assert(flush_pos > ack_pos);
    waitfor_flush[flush_pos].push_back(new C_RetryRead(this));
    return;
  }

  // don't read too much
  if (requested_pos + len > ack_pos) {
    len = ack_pos - requested_pos;
    dout(10) << "_issue_read reading only up to ack_pos " << ack_pos << endl;
  }

  // go.
  dout(10) << "_issue_read reading " << requested_pos << "~" << len 
	   << ", read pointers " << read_pos << "/" << received_pos << "/" << (requested_pos+len)
	   << endl;
  
  filer.read(inode, requested_pos, len, &reading_buf, 
	     new C_Read(this));
  requested_pos += len;
}

void LogStreamer::_prefetch()
{
  // prefetch?
  off_t left = requested_pos - read_pos;
  if (left <= prefetch_from &&      // should read more,
      !_is_reading() &&             // and not reading anything right now
      write_pos > requested_pos) {  // there's something more to read...
    dout(10) << "_prefetch only " << left << " < " << prefetch_from
	     << ", prefetching " << endl;
    _issue_read(fetch_len);
  }
}


void LogStreamer::read_entry(bufferlist *bl, Context *onfinish)
{
  // only one read at a time!
  assert(read_bl == 0);
  assert(on_read_finish == 0);
  
  if (is_readable()) {
    dout(10) << "read_entry at " << read_pos << ", read_buf is " 
	     << read_pos << "~" << read_buf.length() 
	     << ", readable now" << endl;

    // nice, just do it now.
    bool r = try_read_entry(*bl);
    assert(r);
    
    // callback
    onfinish->finish(0);
    delete onfinish;    
  } else {
    dout(10) << "read_entry at " << read_pos << ", read_buf is " 
	     << read_pos << "~" << read_buf.length() 
	     << ", not readable now" << endl;

    bl->clear();

    // set it up
    read_bl = bl;
    on_read_finish = onfinish;

    // is_readable() will have already initiated a read (if it was possible)
  }
}


/* is_readable()
 *  return true if next entry is ready.
 *  kickstart read as necessary.
 */
bool LogStreamer::is_readable()
{
  // anything to read?
  if (read_pos == write_pos) return false;

  // have enough for entry size?
  size_t s;
  if (read_buf.length() < sizeof(s)) {
    if (!_is_reading())
      _issue_read(fetch_len);
    return false;
  }
  read_buf.copy(0, sizeof(s), (char*)&s);

  // have entirely of next entry?
  if (read_buf.length() < sizeof(s) + s) {
    if (!_is_reading())
      _issue_read(MAX(fetch_len, sizeof(s)+s-read_buf.length())); 
    return false;
  }
  
  // next entry is ready!
  return true;
}


/* try_read_entry(bl)
 *  read entry into bl if it's ready.
 *  otherwise, do nothing.  (well, we'll start fetching it for good measure.)
 */
bool LogStreamer::try_read_entry(bufferlist& bl)
{
  if (!is_readable()) {  // this may start a read. 
    dout(10) << "try_read_entry at " << read_pos << " not readable" << endl;
    return false;
  }
  
  size_t s;
  assert(read_buf.length() >= sizeof(s));
  read_buf.copy(0, sizeof(s), (char*)&s);
  assert(read_buf.length() >= sizeof(s) + s);
  
  dout(10) << "try_read_entry at " << read_pos << " reading " 
	   << read_pos << "~" << (sizeof(s)+s) << endl;

  // do it
  assert(bl.length() == 0);
  read_buf.splice(0, sizeof(s));
  read_buf.splice(0, s, &bl);
  read_pos += sizeof(s) + s;

  // prefetch?
  _prefetch();
  return true;
}

void LogStreamer::wait_for_readable(Context *onreadable)
{
  dout(10) << "wait_for_readable at " << read_pos << " onreadable " << onreadable << endl;
  assert(!is_readable());
  assert(on_readable == 0);
  on_readable = onreadable;
}

// eof.
