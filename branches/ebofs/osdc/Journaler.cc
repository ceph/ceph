// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
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

#include "Journaler.h"

#include "include/Context.h"
#include "common/Logger.h"
#include "msg/Messenger.h"

#include "config.h"

#define dout(x)  if (x <= g_conf.debug || x <= g_conf.debug_journaler) *_dout << dbeginl << g_clock.now() << " " << objecter->messenger->get_myname() << ".journaler "
#define derr(x)  if (x <= g_conf.debug || x <= g_conf.debug_journaler) *_derr << dbeginl << g_clock.now() << " " << objecter->messenger->get_myname() << ".journaler "



void Journaler::reset()
{
  dout(1) << "reset to blank journal" << dendl;
  state = STATE_ACTIVE;
  write_pos = flush_pos = ack_pos =
    read_pos = requested_pos = received_pos =
    expire_pos = trimming_pos = trimmed_pos = ceph_file_layout_period(inode.layout);
}


/***************** HEADER *******************/

ostream& operator<<(ostream& out, Journaler::Header &h) 
{
  return out << "loghead(trim " << h.trimmed_pos
	     << ", expire " << h.expire_pos
	     << ", read " << h.read_pos
	     << ", write " << h.write_pos
	     << ")";
}

class Journaler::C_ReadHead : public Context {
  Journaler *ls;
public:
  bufferlist bl;
  C_ReadHead(Journaler *l) : ls(l) {}
  void finish(int r) {
    ls->_finish_read_head(r, bl);
  }
};

class Journaler::C_ProbeEnd : public Context {
  Journaler *ls;
public:
  off_t end;
  C_ProbeEnd(Journaler *l) : ls(l), end(-1) {}
  void finish(int r) {
    ls->_finish_probe_end(r, end);
  }
};

void Journaler::recover(Context *onread) 
{
  assert(state != STATE_ACTIVE);

  if (onread)
    waitfor_recover.push_back(onread);
  
  if (state != STATE_UNDEF) {
    dout(1) << "recover - already recoverying" << dendl;
    return;
  }

  dout(1) << "read_head" << dendl;
  state = STATE_READHEAD;
  C_ReadHead *fin = new C_ReadHead(this);
  filer.read(inode, 0, sizeof(Header), &fin->bl, fin);
}

void Journaler::_finish_read_head(int r, bufferlist& bl)
{
  assert(state == STATE_READHEAD);

  if (bl.length() == 0) {
    dout(1) << "_finish_read_head r=" << r << " read 0 bytes, assuming empty log" << dendl;    
    state = STATE_ACTIVE;
    list<Context*> ls;
    ls.swap(waitfor_recover);
    finish_contexts(ls, 0);
    return;
  } 

  // unpack header
  Header h;
  assert(bl.length() == sizeof(h));
  bl.copy(0, sizeof(h), (char*)&h);

  write_pos = flush_pos = ack_pos = h.write_pos;
  read_pos = requested_pos = received_pos = h.read_pos;
  expire_pos = h.expire_pos;
  trimmed_pos = trimming_pos = h.trimmed_pos;

  dout(1) << "_finish_read_head " << h << ".  probing for end of log (from " << write_pos << ")..." << dendl;

  // probe the log
  state = STATE_PROBING;
  C_ProbeEnd *fin = new C_ProbeEnd(this);
  filer.probe_fwd(inode, h.write_pos, &fin->end, fin);
}

void Journaler::_finish_probe_end(int r, off_t end)
{
  assert(state == STATE_PROBING);
  
  if (end == -1) {
    end = write_pos;
    dout(1) << "_finish_probe_end write_pos = " << end 
	    << " (header had " << write_pos << "). log was empty. recovered."
	    << dendl;
    assert(0); // hrm.
  } else {
    assert(end >= write_pos);
    assert(r >= 0);
    dout(1) << "_finish_probe_end write_pos = " << end 
	    << " (header had " << write_pos << "). recovered."
	    << dendl;
  }

  write_pos = flush_pos = ack_pos = end;
  
  // done.
  list<Context*> ls;
  ls.swap(waitfor_recover);
  finish_contexts(ls, 0);
}


// WRITING

class Journaler::C_WriteHead : public Context {
public:
  Journaler *ls;
  Header h;
  Context *oncommit;
  C_WriteHead(Journaler *l, Header& h_, Context *c) : ls(l), h(h_), oncommit(c) {}
  void finish(int r) {
    ls->_finish_write_head(h, oncommit);
  }
};

void Journaler::write_head(Context *oncommit)
{
  assert(state == STATE_ACTIVE);
  last_written.trimmed_pos = trimmed_pos;
  last_written.expire_pos = expire_pos;
  last_written.read_pos = read_pos;
  last_written.write_pos = ack_pos; //write_pos;
  dout(10) << "write_head " << last_written << dendl;
  
  last_wrote_head = g_clock.now();

  bufferlist bl;
  bl.append((char*)&last_written, sizeof(last_written));
  filer.write(inode, 0, bl.length(), bl, 0, 
	      0, 
	      new C_WriteHead(this, last_written, oncommit));
}

void Journaler::_finish_write_head(Header &wrote, Context *oncommit)
{
  dout(10) << "_finish_write_head " << wrote << dendl;
  last_committed = wrote;
  if (oncommit) {
    oncommit->finish(0);
    delete oncommit;
  }

  trim();  // trim?
}


/***************** WRITING *******************/

class Journaler::C_Flush : public Context {
  Journaler *ls;
  off_t start;
public:
  C_Flush(Journaler *l, off_t s) : ls(l), start(s) {}
  void finish(int r) { ls->_finish_flush(r, start); }
};

void Journaler::_finish_flush(int r, off_t start)
{
  assert(r>=0);

  assert(start >= ack_pos);
  assert(start < flush_pos);
  assert(pending_flush.count(start));

  // calc latency?
  if (logger) {
    utime_t lat = g_clock.now();
    lat -= pending_flush[start];
    logger->favg("jlat", lat);
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
	   << dendl;

  // kick waiters <= ack_pos
  while (!waitfor_flush.empty()) {
    if (waitfor_flush.begin()->first > ack_pos) break;
    finish_contexts(waitfor_flush.begin()->second);
    waitfor_flush.erase(waitfor_flush.begin());
  }
}


off_t Journaler::append_entry(bufferlist& bl, Context *onsync)
{
  uint32_t s = bl.length();

  if (!g_conf.journaler_allow_split_entries) {
    // will we span a stripe boundary?
    int p = inode.layout.fl_stripe_unit;
    if (write_pos / p != (write_pos + (off_t)(bl.length() + sizeof(s))) / p) {
      // yes.
      // move write_pos forward.
      off_t owp = write_pos;
      write_pos += p;
      write_pos -= (write_pos % p);
      
      // pad with zeros.
      bufferptr bp(write_pos - owp);
      bp.zero();
      assert(bp.length() >= 4);
      write_buf.push_back(bp);
      
      // now flush.
      flush();
      
      dout(12) << "append_entry skipped " << (write_pos-owp) << " bytes to " << write_pos << " to avoid spanning stripe boundary" << dendl;
    }
  }
	
  dout(10) << "append_entry len " << bl.length() << " to " << write_pos << "~" << (bl.length() + sizeof(uint32_t)) << dendl;
  
  // cache?
  //  NOTE: this is a dumb thing to do; this is used for a benchmarking
  //   purposes only.
  if (g_conf.journaler_cache &&
      write_pos == read_pos + read_buf.length()) {
    dout(10) << "append_entry caching in read_buf too" << dendl;
    assert(requested_pos == received_pos);
    assert(requested_pos == read_pos + read_buf.length());
    read_buf.append((char*)&s, sizeof(s));
    read_buf.append(bl);
    requested_pos = received_pos = write_pos + sizeof(s) + s;
  }

  // append
  write_buf.append((char*)&s, sizeof(s));
  write_buf.claim_append(bl);
  write_pos += sizeof(s) + s;

  // flush now?
  if (onsync) 
    flush(onsync);

  return write_pos;
}


void Journaler::_do_flush()
{
  if (write_pos == flush_pos) return;
  assert(write_pos > flush_pos);

  // flush
  unsigned len = write_pos - flush_pos;
  assert(len == write_buf.length());
  dout(10) << "_do_flush flushing " << flush_pos << "~" << len << dendl;
  
  // submit write for anything pending
  // flush _start_ pos to _finish_flush
  filer.write(inode, flush_pos, len, write_buf, 0,
	      g_conf.journaler_safe ? 0:new C_Flush(this, flush_pos),  // on ACK
	      g_conf.journaler_safe ?   new C_Flush(this, flush_pos):0); // on COMMIT
  pending_flush[flush_pos] = g_clock.now();
  
  // adjust pointers
  flush_pos = write_pos;
  write_buf.clear();  
  
  dout(10) << "_do_flush write pointers now at " << write_pos << "/" << flush_pos << "/" << ack_pos << dendl;
}

  

void Journaler::flush(Context *onsync)
{
  // all flushed and acked?
  if (write_pos == ack_pos) {
    assert(write_buf.length() == 0);
    dout(10) << "flush nothing to flush, write pointers at " << write_pos << "/" << flush_pos << "/" << ack_pos << dendl;
    if (onsync) {
      onsync->finish(0);
      delete onsync;
    }
    return;
  }

  if (write_pos == flush_pos) {
    assert(write_buf.length() == 0);
    dout(10) << "flush nothing to flush, write pointers at " << write_pos << "/" << flush_pos << "/" << ack_pos << dendl;
  } else {
    if (1) {
      // maybe buffer
      if (write_buf.length() < g_conf.journaler_batch_max) {
	// delay!  schedule an event.
	dout(20) << "flush delaying flush" << dendl;
	if (delay_flush_event) timer.cancel_event(delay_flush_event);
	delay_flush_event = new C_DelayFlush(this);
	timer.add_event_after(g_conf.journaler_batch_interval, delay_flush_event);	
      } else {
	dout(20) << "flush not delaying flush" << dendl;
	_do_flush();
      }
    } else {
      // always flush
      _do_flush();
    }
  }

  // queue waiter (at _new_ write_pos; will go when reached by ack_pos)
  if (onsync) 
    waitfor_flush[write_pos].push_back(onsync);

  // write head?
  if (last_wrote_head.sec() + g_conf.journaler_write_head_interval < g_clock.now().sec()) {
    write_head();
  }
}



/***************** READING *******************/


class Journaler::C_Read : public Context {
  Journaler *ls;
public:
  C_Read(Journaler *l) : ls(l) {}
  void finish(int r) { ls->_finish_read(r); }
};

class Journaler::C_RetryRead : public Context {
  Journaler *ls;
public:
  C_RetryRead(Journaler *l) : ls(l) {}
  void finish(int r) { ls->is_readable(); }  // this'll kickstart.
};

void Journaler::_finish_read(int r)
{
  assert(r>=0);

  dout(10) << "_finish_read got " << received_pos << "~" << reading_buf.length() << dendl;
  received_pos += reading_buf.length();
  read_buf.claim_append(reading_buf);
  assert(received_pos <= requested_pos);
  dout(10) << "_finish_read read_buf now " << read_pos << "~" << read_buf.length() 
	   << ", read pointers " << read_pos << "/" << received_pos << "/" << requested_pos
	   << dendl;
  
  if (is_readable()) { // NOTE: this check may read more
    // readable!
    dout(10) << "_finish_read now readable" << dendl;
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
void Journaler::_issue_read(off_t len)
{
  // make sure we're fully flushed
  _do_flush();

  if (_is_reading()) {
    dout(10) << "_issue_read " << len << " waiting, already reading " 
	     << received_pos << "~" << (requested_pos-received_pos) << dendl;
    return;
  } 
  assert(requested_pos == received_pos);

  // stuck at ack_pos?
  assert(requested_pos <= ack_pos);
  if (requested_pos == ack_pos) {
    dout(10) << "_issue_read requested_pos = ack_pos = " << ack_pos << ", waiting" << dendl;
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
    dout(10) << "_issue_read reading only up to ack_pos " << ack_pos << dendl;
  }

  // go.
  dout(10) << "_issue_read reading " << requested_pos << "~" << len 
	   << ", read pointers " << read_pos << "/" << received_pos << "/" << (requested_pos+len)
	   << dendl;
  
  filer.read(inode, requested_pos, len, &reading_buf, 
	     new C_Read(this));
  requested_pos += len;
}

void Journaler::_prefetch()
{
  // prefetch?
  off_t left = requested_pos - read_pos;
  if (left <= prefetch_from &&      // should read more,
      !_is_reading() &&             // and not reading anything right now
      write_pos > requested_pos) {  // there's something more to read...
    dout(10) << "_prefetch only " << left << " < " << prefetch_from
	     << ", prefetching " << dendl;
    _issue_read(fetch_len);
  }
}


void Journaler::read_entry(bufferlist *bl, Context *onfinish)
{
  // only one read at a time!
  assert(read_bl == 0);
  assert(on_read_finish == 0);
  
  if (is_readable()) {
    dout(10) << "read_entry at " << read_pos << ", read_buf is " 
	     << read_pos << "~" << read_buf.length() 
	     << ", readable now" << dendl;

    // nice, just do it now.
    bool r = try_read_entry(*bl);
    assert(r);
    
    // callback
    onfinish->finish(0);
    delete onfinish;    
  } else {
    dout(10) << "read_entry at " << read_pos << ", read_buf is " 
	     << read_pos << "~" << read_buf.length() 
	     << ", not readable now" << dendl;

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
bool Journaler::is_readable() 
{
  // anything to read?
  if (read_pos == write_pos) return false;

  // have enough for entry size?
  uint32_t s = 0;
  if (read_buf.length() >= sizeof(s)) 
    read_buf.copy(0, sizeof(s), (char*)&s);

  // entry and payload?
  if (read_buf.length() >= sizeof(s) &&
      read_buf.length() >= sizeof(s) + s) 
    return true;  // yep, next entry is ready.

  // darn it!

  // partial fragment at the end?
  if (received_pos == write_pos) {
    dout(10) << "is_readable() detected partial entry at tail, adjusting write_pos to " << read_pos << dendl;
    write_pos = flush_pos = ack_pos = read_pos;
    assert(write_buf.length() == 0);

    // truncate?
    // FIXME: how much?
    
    return false;
  } 

  // start reading some more?
  if (!_is_reading()) {
    if (s)
      fetch_len = MAX(fetch_len, (off_t)(sizeof(s)+s-read_buf.length())); 
    _issue_read(fetch_len);
  }

  return false;
}


/* try_read_entry(bl)
 *  read entry into bl if it's ready.
 *  otherwise, do nothing.  (well, we'll start fetching it for good measure.)
 */
bool Journaler::try_read_entry(bufferlist& bl)
{
  if (!is_readable()) {  // this may start a read. 
    dout(10) << "try_read_entry at " << read_pos << " not readable" << dendl;
    return false;
  }
  
  uint32_t s;
  assert(read_buf.length() >= sizeof(s));
  read_buf.copy(0, sizeof(s), (char*)&s);
  assert(read_buf.length() >= sizeof(s) + s);
  
  dout(10) << "try_read_entry at " << read_pos << " reading " 
	   << read_pos << "~" << (sizeof(s)+s) << dendl;

  // do it
  assert(bl.length() == 0);
  read_buf.splice(0, sizeof(s));
  read_buf.splice(0, s, &bl);
  read_pos += sizeof(s) + s;

  // prefetch?
  _prefetch();
  return true;
}

void Journaler::wait_for_readable(Context *onreadable)
{
  dout(10) << "wait_for_readable at " << read_pos << " onreadable " << onreadable << dendl;
  assert(!is_readable());
  assert(on_readable == 0);
  on_readable = onreadable;
}




/***************** TRIMMING *******************/


class Journaler::C_Trim : public Context {
  Journaler *ls;
  off_t to;
public:
  C_Trim(Journaler *l, off_t t) : ls(l), to(t) {}
  void finish(int r) {
    ls->_trim_finish(r, to);
  }
};

void Journaler::trim()
{
  off_t trim_to = last_committed.expire_pos;
  trim_to -= trim_to % ceph_file_layout_period(inode.layout);
  dout(10) << "trim last_commited head was " << last_committed
	   << ", can trim to " << trim_to
	   << dendl;
  if (trim_to == 0 || trim_to == trimming_pos) {
    dout(10) << "trim already trimmed/trimming to " 
	     << trimmed_pos << "/" << trimming_pos << dendl;
    return;
  }

  if (trimming_pos > trimmed_pos) {
    dout(10) << "trim already trimming atm, try again later.  trimmed/trimming is " 
	     << trimmed_pos << "/" << trimming_pos << dendl;
    return;
  }
  
  // trim
  assert(trim_to <= write_pos);
  assert(trim_to > trimming_pos);
  dout(10) << "trim trimming to " << trim_to 
	   << ", trimmed/trimming/expire are " 
	   << trimmed_pos << "/" << trimming_pos << "/" << expire_pos
	   << dendl;
  
  filer.remove(inode, trimming_pos, trim_to-trimming_pos, 
	       0, new C_Trim(this, trim_to));
  trimming_pos = trim_to;  
}

void Journaler::_trim_finish(int r, off_t to)
{
  dout(10) << "_trim_finish trimmed_pos was " << trimmed_pos
	   << ", trimmed/trimming/expire now "
	   << to << "/" << trimming_pos << "/" << expire_pos
	   << dendl;
  assert(r >= 0);
  
  assert(to <= trimming_pos);
  assert(to > trimmed_pos);
  trimmed_pos = to;

  // finishers?
  while (!waitfor_trim.empty() &&
	 waitfor_trim.begin()->first <= trimmed_pos) {
    finish_contexts(waitfor_trim.begin()->second, 0);
    waitfor_trim.erase(waitfor_trim.begin());
  }
}


// eof.
