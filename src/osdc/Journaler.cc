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
#include "common/ProfLogger.h"
#include "msg/Messenger.h"

#include "common/config.h"

#define DOUT_SUBSYS journaler
#undef dout_prefix
#define dout_prefix *_dout << objecter->messenger->get_myname() << ".journaler "



void Journaler::create(ceph_file_layout *l)
{
  assert(!readonly);
  dout(1) << "create blank journal" << dendl;
  state = STATE_ACTIVE;

  set_layout(l);

  write_pos = flush_pos = ack_pos = safe_pos =
    read_pos = requested_pos = received_pos =
    expire_pos = trimming_pos = trimmed_pos = layout.fl_stripe_count * layout.fl_object_size;
}

void Journaler::set_layout(ceph_file_layout *l)
{
  assert(!readonly);
  layout = *l;

  assert(layout.fl_pg_pool == pg_pool);
  last_written.layout = layout;
  last_committed.layout = layout;

  // prefetch intelligently.
  // (watch out, this is big if you use big objects or weird striping)
  fetch_len = layout.fl_stripe_count * layout.fl_object_size * g_conf.journaler_prefetch_periods;
  prefetch_from = fetch_len / 2;
}


/***************** HEADER *******************/

ostream& operator<<(ostream& out, Journaler::Header &h) 
{
  return out << "loghead(trim " << h.trimmed_pos
	     << ", expire " << h.expire_pos
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

class Journaler::C_RereadHead : public Context {
  Journaler *ls;
  Context *onfinish;
public:
  bufferlist bl;
  C_RereadHead(Journaler *l, Context *onfinish_) : ls (l), onfinish(onfinish_){}
  void finish(int r) {
    ls->_finish_reread_head(r, bl, onfinish);
  }
};

class Journaler::C_ProbeEnd : public Context {
  Journaler *ls;
public:
  uint64_t end;
  C_ProbeEnd(Journaler *l) : ls(l), end(-1) {}
  void finish(int r) {
    ls->_finish_probe_end(r, end);
  }
};

class Journaler::C_ReProbe : public Context {
  Journaler *ls;
  Context *onfinish;
public:
  uint64_t end;
  C_ReProbe(Journaler *l, Context *onfinish_) :
    ls(l), onfinish(onfinish_), end(0) {}
  void finish(int r) {
    ls->_finish_reprobe(r, end, onfinish);
  }
};

void Journaler::recover(Context *onread) 
{
  dout(1) << "recover start" << dendl;
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
  read_head(fin, &fin->bl);
}

void Journaler::read_head(Context *on_finish, bufferlist *bl)
{
  assert(state == STATE_READHEAD || state == STATE_REREADHEAD);

  object_t oid = file_object_t(ino, 0);
  object_locator_t oloc(pg_pool);
  objecter->read_full(oid, oloc, CEPH_NOSNAP, bl, 0, on_finish);
}

/**
 * Re-read the head from disk, and set the write_pos, expire_pos, trimmed_pos
 * from the on-disk header. This switches the state to STATE_REREADHEAD for
 * the duration, and you shouldn't start a re-read while other operations are
 * in-flight, nor start other operations while a re-read is in progress.
 * Also, don't call this until the Journaler has finished its recovery and has
 * gone STATE_ACTIVE!
 */
void Journaler::reread_head(Context *onfinish)
{
  dout(10) << "reread_head" << dendl;
  assert(state == STATE_ACTIVE);

  state = STATE_REREADHEAD;
  C_RereadHead *fin = new C_RereadHead(this, onfinish);
  read_head(fin, &fin->bl);
}

void Journaler::_finish_reread_head(int r, bufferlist& bl, Context *finish)
{
  //read on-disk header into
  assert (bl.length());

  // unpack header
  Header h;
  bufferlist::iterator p = bl.begin();
  ::decode(h, p);
  write_pos = flush_pos = h.write_pos;
  expire_pos = h.expire_pos;
  trimmed_pos = h.trimmed_pos;
  init_headers(h);
  state = STATE_ACTIVE;
  finish->finish(r);
  delete finish;
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
  bufferlist::iterator p = bl.begin();
  ::decode(h, p);

  if (h.magic != magic) {
    dout(0) << "on disk magic '" << h.magic << "' != my magic '"
	    << magic << "'" << dendl;
    list<Context*> ls;
    ls.swap(waitfor_recover);
    finish_contexts(ls, -EINVAL);
    return;
  }

  write_pos = flush_pos = ack_pos = safe_pos = h.write_pos;
  read_pos = requested_pos = received_pos = expire_pos = h.expire_pos;
  trimmed_pos = trimming_pos = h.trimmed_pos;

  init_headers(h);
  set_layout(&h.layout);

  dout(1) << "_finish_read_head " << h << ".  probing for end of log (from " << write_pos << ")..." << dendl;
  C_ProbeEnd *fin = new C_ProbeEnd(this);
  state = STATE_PROBING;
  probe(fin, &fin->end);
}

void Journaler::probe(Context *finish, uint64_t *end)
{
  dout(1) << "probing for end of the log" << dendl;
  assert(state == STATE_PROBING || state == STATE_REPROBING);
  // probe the log
  filer.probe(ino, &layout, CEPH_NOSNAP,
	      write_pos, end, 0, true, 0, finish);
}

void Journaler::reprobe(Context *finish)
{
  dout(10) << "reprobe" << dendl;
  assert(state == STATE_ACTIVE);

  state = STATE_REPROBING;
  C_ReProbe *fin = new C_ReProbe(this, finish);
  probe(fin, &fin->end);
}


void Journaler::_finish_reprobe(int r, uint64_t new_end, Context *onfinish) {
  assert(new_end >= write_pos);
  assert(r >= 0);
  write_pos = flush_pos = ack_pos = safe_pos = new_end;
  state = STATE_ACTIVE;
  onfinish->finish(r);
  delete onfinish;
}

void Journaler::_finish_probe_end(int r, uint64_t end)
{
  assert(state == STATE_PROBING);
  
  if (((int64_t)end) == -1) {
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

  state = STATE_ACTIVE;

  write_pos = flush_pos = ack_pos = safe_pos = end;
  
  // done.
  list<Context*> ls;
  ls.swap(waitfor_recover);
  finish_contexts(ls, 0);
}

class Journaler::C_RereadHeadProbe : public Context
{
  Journaler *ls;
  Context *final_finish;
public:
  C_RereadHeadProbe(Journaler *l, Context *finish) :
    ls(l), final_finish(finish) {}
  void finish(int r) {
    ls->_finish_reread_head_and_probe(r, final_finish);
  }
};

void Journaler::reread_head_and_probe(Context *onfinish)
{
  assert(state == STATE_ACTIVE);
  reread_head(new C_RereadHeadProbe(this, onfinish));
}

void Journaler::_finish_reread_head_and_probe(int r, Context *onfinish)
{
  assert(!r); //if we get an error, we're boned
  reprobe(onfinish);
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
  assert (!readonly);
  assert(state == STATE_ACTIVE);
  last_written.trimmed_pos = trimmed_pos;
  last_written.expire_pos = expire_pos;
  last_written.unused_field = expire_pos;
  last_written.write_pos = safe_pos;
  dout(10) << "write_head " << last_written << dendl;
  
  last_wrote_head = g_clock.now();

  bufferlist bl;
  ::encode(last_written, bl);
  SnapContext snapc;
  
  object_t oid = file_object_t(ino, 0);
  object_locator_t oloc(pg_pool);
  objecter->write_full(oid, oloc, snapc, bl, g_clock.now(), 0, 
		       NULL, 
		       new C_WriteHead(this, last_written, oncommit));
}

void Journaler::_finish_write_head(Header &wrote, Context *oncommit)
{
  assert(!readonly);
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
  uint64_t start;
  utime_t stamp;
  bool safe;
public:
  C_Flush(Journaler *l, int64_t s, utime_t st, bool sa) : ls(l), start(s), stamp(st), safe(sa) {}
  void finish(int r) { ls->_finish_flush(r, start, stamp, safe); }
};

void Journaler::_finish_flush(int r, uint64_t start, utime_t stamp, bool safe)
{
  assert(!readonly);
  assert(r>=0);

  assert((!safe && start >= ack_pos) || (safe && start >= safe_pos));
  assert(start < flush_pos);

  // calc latency?
  if (logger) {
    utime_t lat = g_clock.now();
    lat -= stamp;
    logger->favg(logger_key_lat, lat);
  }

  // adjust ack_pos
  if (!safe) {
    assert(pending_ack.count(start));
    pending_ack.erase(start);
    if (pending_ack.empty())
      ack_pos = flush_pos;
    else
      ack_pos = *pending_ack.begin();
    if (!ack_barrier.empty() && *ack_barrier.begin() < ack_pos)
      ack_pos = *ack_barrier.begin();
  } else {
    assert(pending_safe.count(start));
    pending_safe.erase(start);
    if (pending_safe.empty())
      safe_pos = flush_pos;
    else
      safe_pos = *pending_safe.begin();
    if (ack_pos < safe_pos)
      ack_pos = safe_pos;
    
    if (ack_barrier.count(start)) {
      ack_barrier.erase(start);
      
      if (ack_pos == start) {
	if (pending_ack.empty())
	  ack_pos = flush_pos;
	else
	  ack_pos = *pending_ack.begin();
	if (!ack_barrier.empty() && *ack_barrier.begin() < ack_pos)
	  ack_pos = *ack_barrier.begin();
      }
    }
  }

  dout(10) << "_finish_flush " << (safe ? "safe":"ack") << " from " << start
	   << ", pending_ack " << pending_ack
    //<< ", pending_safe " << pending_safe
	   << ", ack_barrier " << ack_barrier
	   << ", write positions now " << write_pos << "/" << flush_pos
	   << "/" << ack_pos << "/" << safe_pos
	   << dendl;

  // kick waiters <= ack_pos
  while (!waitfor_ack.empty()) {
    if (waitfor_ack.begin()->first > ack_pos) break;
    finish_contexts(waitfor_ack.begin()->second);
    waitfor_ack.erase(waitfor_ack.begin());
  }
  if (safe) {
    while (!waitfor_safe.empty()) {
      if (waitfor_safe.begin()->first > safe_pos) break;
      finish_contexts(waitfor_safe.begin()->second);
      waitfor_safe.erase(waitfor_safe.begin());
    }

  }
}


uint64_t Journaler::append_entry(bufferlist& bl)
{
  assert(!readonly);
  uint32_t s = bl.length();

  if (!g_conf.journaler_allow_split_entries) {
    // will we span a stripe boundary?
    int p = layout.fl_stripe_unit;
    if (write_pos / p != (write_pos + (int64_t)(bl.length() + sizeof(s))) / p) {
      // yes.
      // move write_pos forward.
      int64_t owp = write_pos;
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
    ::encode(s, read_buf);
    read_buf.append(bl);
    requested_pos = received_pos = write_pos + sizeof(s) + s;
  }

  // append
  ::encode(s, write_buf);
  write_buf.claim_append(bl);
  write_pos += sizeof(s) + s;

  // flush previous object?
  int su = layout.fl_stripe_unit;
  int write_off = write_pos % su;
  int write_obj = write_pos / su;
  int flush_obj = flush_pos / su;
  if (write_obj != flush_obj) {
    dout(10) << " flushing completed object(s) (su " << su << " wro " << write_obj << " flo " << flush_obj << ")" << dendl;
    _do_flush(write_buf.length() - write_off);
  }

  return write_pos;
}


void Journaler::_do_flush(unsigned amount)
{
  if (write_pos == flush_pos) return;
  assert(write_pos > flush_pos);
  assert(!readonly);

  // flush
  unsigned len = write_pos - flush_pos;
  assert(len == write_buf.length());
  if (amount && amount < len)
    len = amount;
  dout(10) << "_do_flush flushing " << flush_pos << "~" << len << dendl;
  
  // submit write for anything pending
  // flush _start_ pos to _finish_flush
  utime_t now = g_clock.now();
  SnapContext snapc;

  Context *onack = 0;
  if (!g_conf.journaler_safe) {
    onack = new C_Flush(this, flush_pos, now, false);  // on ACK
    pending_ack.insert(flush_pos);
  }

  Context *onsafe = new C_Flush(this, flush_pos, now, true);  // on COMMIT
  pending_safe.insert(flush_pos);

  bufferlist write_bl;

  // adjust pointers
  if (len == write_buf.length()) {
    write_bl.swap(write_buf);
  } else {
    write_buf.splice(0, len, &write_bl);
  }

  filer.write(ino, &layout, snapc,
	      flush_pos, len, write_bl, g_clock.now(),
	      0,
	      onack, onsafe);

  flush_pos += len;
  assert(write_buf.length() == write_pos - flush_pos);
    
  dout(10) << "_do_flush write pointers now at " << write_pos << "/" << flush_pos << "/" << ack_pos << "/" << safe_pos << dendl;
}



void Journaler::wait_for_flush(Context *onsync, Context *onsafe, bool add_ack_barrier)
{
  assert(!readonly);
  if (g_conf.journaler_safe && onsync) {
    assert(!onsafe);
    onsafe = onsync;
    onsync = 0;
  }
  
  // all flushed and acked?
  if (write_pos == ack_pos) {
    assert(write_buf.length() == 0);
    dout(10) << "flush nothing to flush, write pointers at " 
	     << write_pos << "/" << flush_pos << "/" << ack_pos << "/" << safe_pos << dendl;
    if (onsync) {
      onsync->finish(0);
      delete onsync;
      onsync = 0;
    }
    if (onsafe) {
      if (write_pos == safe_pos) {
	onsafe->finish(0);
	delete onsafe;
	onsafe = 0;
      } else {
	waitfor_safe[write_pos].push_back(onsafe);
      }
    }
    return;
  }

  // queue waiter
  if (onsync) 
    waitfor_ack[write_pos].push_back(onsync);
  if (onsafe) 
    waitfor_safe[write_pos].push_back(onsafe);
  if (add_ack_barrier)
    ack_barrier.insert(write_pos);
}  

void Journaler::flush(Context *onsync, Context *onsafe, bool add_ack_barrier)
{
  assert(!readonly);
  wait_for_flush(onsync, onsafe, add_ack_barrier);
  if (write_pos == ack_pos)
    return;

  if (write_pos == flush_pos) {
    assert(write_buf.length() == 0);
    dout(10) << "flush nothing to flush, write pointers at "
	     << write_pos << "/" << flush_pos << "/" << ack_pos << "/" << safe_pos << dendl;
  } else {
    if (1) {
      // maybe buffer
      if (write_buf.length() < g_conf.journaler_batch_max) {
	// delay!  schedule an event.
	dout(20) << "flush delaying flush" << dendl;
	if (delay_flush_event)
	  timer->cancel_event(delay_flush_event);
	delay_flush_event = new C_DelayFlush(this);
	timer->add_event_after(g_conf.journaler_batch_interval, delay_flush_event);	
      } else {
	dout(20) << "flush not delaying flush" << dendl;
	_do_flush();
      }
    } else {
      // always flush
      _do_flush();
    }
  }

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
  if (r < 0) {
    dout(0) << "_finish_read got error " << r << dendl;
    error = r;
    if (on_readable) {
      Context *f = on_readable;
      on_readable = 0;
      f->finish(0);
      delete f;
    }
    return;
  }
  assert(r>=0);

  dout(10) << "_finish_read got " << received_pos << "~" << reading_buf.length() << dendl;
  received_pos += reading_buf.length();
  read_buf.claim_append(reading_buf);
  assert(received_pos <= requested_pos);
  dout(10) << "_finish_read read_buf now " << read_pos << "~" << read_buf.length() 
	   << ", read pointers " << read_pos << "/" << received_pos << "/" << requested_pos
	   << dendl;
  
  if (is_readable() || read_pos == write_pos) { // NOTE: this check may read more
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
void Journaler::_issue_read(int64_t len)
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
    waitfor_ack[flush_pos].push_back(new C_RetryRead(this));
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
  
  filer.read(ino, &layout, CEPH_NOSNAP,
	     requested_pos, len, &reading_buf, 0,
	     new C_Read(this));
  requested_pos += len;
}

void Journaler::_prefetch()
{
  // prefetch?
  uint64_t left = requested_pos - read_pos;
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
  bufferlist::iterator p = read_buf.begin();
  if (read_buf.length() >= sizeof(s))
    ::decode(s, p);

  // entry and payload?
  if (read_buf.length() >= sizeof(s) &&
      read_buf.length() >= sizeof(s) + s) 
    return true;  // yep, next entry is ready.

  // darn it!

  // partial fragment at the end?
  if (received_pos == write_pos) {
    dout(10) << "is_readable() detected partial entry at tail, adjusting write_pos to " << read_pos << dendl;
    if (write_pos > read_pos)
      junk_tail_pos = write_pos; // note old tail
    write_pos = flush_pos = ack_pos = safe_pos = read_pos;
    requested_pos = received_pos = read_pos;
    read_buf.clear();
    assert(write_buf.length() == 0);

    // truncate?
    // FIXME: how much?
    
    return false;
  } 

  // start reading some more?
  if (!_is_reading()) {
    if (s)
      fetch_len = MAX(fetch_len, (sizeof(s)+s-read_buf.length()));
    _issue_read(fetch_len);
  }

  return false;
}

bool Journaler::truncate_tail_junk(Context *c)
{
  if(readonly)
    return true; //can't touch journal in readonly mode!
  if (!junk_tail_pos) {
    dout(10) << "truncate_tail_junk -- no trailing junk" << dendl;
    return true;
  }

  int64_t len = junk_tail_pos - write_pos;
  dout(10) << "truncate_tail_junk " << write_pos << "~" << len << dendl;
  SnapContext snapc;
  filer.zero(ino, &layout, snapc, write_pos, len, g_clock.now(), 0, NULL, c);
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
  {
    bufferlist::iterator p = read_buf.begin();
    ::decode(s, p);
  }
  assert(read_buf.length() >= sizeof(s) + s);
  
  dout(10) << "try_read_entry at " << read_pos << " reading " 
	   << read_pos << "~" << (sizeof(s)+s) << " (have " << read_buf.length() << ")" << dendl;

  if (s == 0) {
    dout(0) << "try_read_entry got 0 len entry at offset " << read_pos << dendl;
    error = -EINVAL;
    return false;
  }

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
  uint64_t to;
public:
  C_Trim(Journaler *l, int64_t t) : ls(l), to(t) {}
  void finish(int r) {
    ls->_trim_finish(r, to);
  }
};

void Journaler::trim()
{
  assert(!readonly);
  uint64_t period = layout.fl_stripe_count * layout.fl_object_size;

  uint64_t trim_to = last_committed.expire_pos;
  trim_to -= trim_to % period;
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
  assert(trim_to <= expire_pos);
  assert(trim_to > trimming_pos);
  dout(10) << "trim trimming to " << trim_to 
	   << ", trimmed/trimming/expire are " 
	   << trimmed_pos << "/" << trimming_pos << "/" << expire_pos
	   << dendl;

  // delete range of objects
  uint64_t first = trimming_pos / period;
  uint64_t num = (trim_to - trimming_pos) / period;
  SnapContext snapc;
  filer.purge_range(ino, &layout, snapc, first, num, g_clock.now(), 0, 
		    new C_Trim(this, trim_to));
  trimming_pos = trim_to;  
}

void Journaler::_trim_finish(int r, uint64_t to)
{
  assert(!readonly);
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
