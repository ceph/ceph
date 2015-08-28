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

#include "common/perf_counters.h"
#include "common/dout.h"
#include "include/Context.h"
#include "msg/Messenger.h"
#include "osdc/Journaler.h"
#include "common/errno.h"
#include "include/assert.h"
#include "common/Finisher.h"

#define dout_subsys ceph_subsys_journaler
#undef dout_prefix
#define dout_prefix *_dout << objecter->messenger->get_myname() << ".journaler" << (readonly ? "(ro) ":"(rw) ")


void Journaler::set_readonly()
{
  Mutex::Locker l(lock);

  ldout(cct, 1) << "set_readonly" << dendl;
  readonly = true;
}

void Journaler::set_writeable()
{
  Mutex::Locker l(lock);

  ldout(cct, 1) << "set_writeable" << dendl;
  readonly = false;
}

void Journaler::create(ceph_file_layout *l, stream_format_t const sf)
{
  Mutex::Locker lk(lock);

  assert(!readonly);
  state = STATE_ACTIVE;

  stream_format = sf;
  journal_stream.set_format(sf);
  _set_layout(l);

  prezeroing_pos = prezero_pos = write_pos = flush_pos = safe_pos =
    read_pos = requested_pos = received_pos =
    expire_pos = trimming_pos = trimmed_pos = (uint64_t)layout.fl_stripe_count * layout.fl_object_size;

  ldout(cct, 1) << "created blank journal at inode 0x" << std::hex << ino << std::dec
    << ", format=" << stream_format << dendl;
}

void Journaler::set_layout(ceph_file_layout const *l)
{
    Mutex::Locker lk(lock);
    _set_layout(l);
}

void Journaler::_set_layout(ceph_file_layout const *l)
{
  layout = *l;

  assert(layout.fl_pg_pool == pg_pool);
  last_written.layout = layout;
  last_committed.layout = layout;

  // prefetch intelligently.
  // (watch out, this is big if you use big objects or weird striping)
  uint64_t periods = cct->_conf->journaler_prefetch_periods;
  if (periods < 2)
    periods = 2;  // we need at least 2 periods to make progress.
  fetch_len = layout.fl_stripe_count * layout.fl_object_size * periods;
}


/***************** HEADER *******************/

ostream& operator<<(ostream& out, Journaler::Header &h) 
{
  return out << "loghead(trim " << h.trimmed_pos
	     << ", expire " << h.expire_pos
	     << ", write " << h.write_pos
	     << ", stream_format " << (int)(h.stream_format)
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
  C_OnFinisher *onfinish;
public:
  uint64_t end;
  C_ReProbe(Journaler *l, C_OnFinisher *onfinish_) :
    ls(l), onfinish(onfinish_), end(0) {}
  void finish(int r) {
    ls->_finish_reprobe(r, end, onfinish);
  }
};

void Journaler::recover(Context *onread) 
{
  Mutex::Locker l(lock);
  if (stopping) {
    onread->complete(-EAGAIN);
    return;
  }

  ldout(cct, 1) << "recover start" << dendl;
  assert(state != STATE_ACTIVE);
  assert(readonly);

  if (onread)
    waitfor_recover.push_back(onread);
  
  if (state != STATE_UNDEF) {
    ldout(cct, 1) << "recover - already recovering" << dendl;
    return;
  }

  ldout(cct, 1) << "read_head" << dendl;
  state = STATE_READHEAD;
  C_ReadHead *fin = new C_ReadHead(this);
  _read_head(fin, &fin->bl);
}

void Journaler::_read_head(Context *on_finish, bufferlist *bl)
{
  assert(lock.is_locked_by_me());
  assert(state == STATE_READHEAD || state == STATE_REREADHEAD);

  object_t oid = file_object_t(ino, 0);
  object_locator_t oloc(pg_pool);
  objecter->read_full(oid, oloc, CEPH_NOSNAP, bl, 0, wrap_finisher(on_finish));
}

void Journaler::reread_head(Context *onfinish)
{
  Mutex::Locker l(lock);
  _reread_head(wrap_finisher(onfinish));
}

/**
 * Re-read the head from disk, and set the write_pos, expire_pos, trimmed_pos
 * from the on-disk header. This switches the state to STATE_REREADHEAD for
 * the duration, and you shouldn't start a re-read while other operations are
 * in-flight, nor start other operations while a re-read is in progress.
 * Also, don't call this until the Journaler has finished its recovery and has
 * gone STATE_ACTIVE!
 */
void Journaler::_reread_head(Context *onfinish)
{
  ldout(cct, 10) << "reread_head" << dendl;
  assert(state == STATE_ACTIVE);

  state = STATE_REREADHEAD;
  C_RereadHead *fin = new C_RereadHead(this, onfinish);
  _read_head(fin, &fin->bl);
}

void Journaler::_finish_reread_head(int r, bufferlist& bl, Context *finish)
{
  Mutex::Locker l(lock);

  //read on-disk header into
  assert(bl.length() || r < 0 );

  // unpack header
  if (r == 0) {
    Header h;
    bufferlist::iterator p = bl.begin();
    try {
      ::decode(h, p);
    } catch (const buffer::error &e) {
      finish->complete(-EINVAL);
      return;
    }
    prezeroing_pos = prezero_pos = write_pos = flush_pos = safe_pos = h.write_pos;
    expire_pos = h.expire_pos;
    trimmed_pos = trimming_pos = h.trimmed_pos;
    init_headers(h);
    state = STATE_ACTIVE;
  }

  finish->complete(r);
}

void Journaler::_finish_read_head(int r, bufferlist& bl)
{
  Mutex::Locker l(lock);

  assert(state == STATE_READHEAD);

  if (r!=0) {
    ldout(cct, 0) << "error getting journal off disk"
                  << dendl;
    list<Context*> ls;
    ls.swap(waitfor_recover);
    finish_contexts(cct, ls, r);
    return;
  }

  if (bl.length() == 0) {
    ldout(cct, 1) << "_finish_read_head r=" << r << " read 0 bytes, assuming empty log" << dendl;    
    state = STATE_ACTIVE;
    list<Context*> ls;
    ls.swap(waitfor_recover);
    finish_contexts(cct, ls, 0);
    return;
  } 

  // unpack header
  bool corrupt = false;
  Header h;
  bufferlist::iterator p = bl.begin();
  try {
    ::decode(h, p);

    if (h.magic != magic) {
      ldout(cct, 0) << "on disk magic '" << h.magic << "' != my magic '"
              << magic << "'" << dendl;
      corrupt = true;
    } else if (h.write_pos < h.expire_pos || h.expire_pos < h.trimmed_pos) {
      ldout(cct, 0) << "Corrupt header (bad offsets): " << h << dendl;
      corrupt = true;
    }
  } catch (const buffer::error &e) {
    corrupt = true;
  }

  if (corrupt) {
    list<Context*> ls;
    ls.swap(waitfor_recover);
    finish_contexts(cct, ls, -EINVAL);
    return;
  }

  prezeroing_pos = prezero_pos = write_pos = flush_pos = safe_pos = h.write_pos;
  read_pos = requested_pos = received_pos = expire_pos = h.expire_pos;
  trimmed_pos = trimming_pos = h.trimmed_pos;

  init_headers(h);
  _set_layout(&h.layout);
  stream_format = h.stream_format;
  journal_stream.set_format(h.stream_format);

  ldout(cct, 1) << "_finish_read_head " << h << ".  probing for end of log (from " << write_pos << ")..." << dendl;
  C_ProbeEnd *fin = new C_ProbeEnd(this);
  state = STATE_PROBING;
  _probe(fin, &fin->end);
}

void Journaler::_probe(Context *finish, uint64_t *end)
{
  assert(lock.is_locked_by_me());
  ldout(cct, 1) << "probing for end of the log" << dendl;
  assert(state == STATE_PROBING || state == STATE_REPROBING);
  // probe the log
  filer.probe(ino, &layout, CEPH_NOSNAP,
	      write_pos, end, 0, true, 0, wrap_finisher(finish));
}

void Journaler::_reprobe(C_OnFinisher *finish)
{
  ldout(cct, 10) << "reprobe" << dendl;
  assert(state == STATE_ACTIVE);

  state = STATE_REPROBING;
  C_ReProbe *fin = new C_ReProbe(this, finish);
  _probe(fin, &fin->end);
}


void Journaler::_finish_reprobe(int r, uint64_t new_end, C_OnFinisher *onfinish)
{
  Mutex::Locker l(lock);

  assert(new_end >= write_pos || r < 0);
  ldout(cct, 1) << "_finish_reprobe new_end = " << new_end 
	  << " (header had " << write_pos << ")."
	  << dendl;
  prezeroing_pos = prezero_pos = write_pos = flush_pos = safe_pos = new_end;
  state = STATE_ACTIVE;
  onfinish->complete(r);
}

void Journaler::_finish_probe_end(int r, uint64_t end)
{
  Mutex::Locker l(lock);

  assert(state == STATE_PROBING);
  if (r < 0) { // error in probing
    goto out;
  }
  if (((int64_t)end) == -1) {
    end = write_pos;
    ldout(cct, 1) << "_finish_probe_end write_pos = " << end 
	    << " (header had " << write_pos << "). log was empty. recovered."
	    << dendl;
    assert(0); // hrm.
  } else {
    assert(end >= write_pos);
    ldout(cct, 1) << "_finish_probe_end write_pos = " << end 
	    << " (header had " << write_pos << "). recovered."
	    << dendl;
  }

  state = STATE_ACTIVE;

  prezeroing_pos = prezero_pos = write_pos = flush_pos = safe_pos = end;
  
out:
  // done.
  list<Context*> ls;
  ls.swap(waitfor_recover);
  finish_contexts(cct, ls, r);
}

class Journaler::C_RereadHeadProbe : public Context
{
  Journaler *ls;
  C_OnFinisher *final_finish;
public:
  C_RereadHeadProbe(Journaler *l, C_OnFinisher *finish) :
    ls(l), final_finish(finish) {}
  void finish(int r) {
    ls->_finish_reread_head_and_probe(r, final_finish);
  }
};

void Journaler::reread_head_and_probe(Context *onfinish)
{
  Mutex::Locker l(lock);

  assert(state == STATE_ACTIVE);
  _reread_head(new C_RereadHeadProbe(this, wrap_finisher(onfinish)));
}

void Journaler::_finish_reread_head_and_probe(int r, C_OnFinisher *onfinish)
{
  // Expect to be called back from finish_reread_head, which already takes lock
  assert(lock.is_locked_by_me());

  assert(!r); //if we get an error, we're boned
  _reprobe(onfinish);
}


// WRITING

class Journaler::C_WriteHead : public Context {
public:
  Journaler *ls;
  Header h;
  C_OnFinisher *oncommit;
  C_WriteHead(Journaler *l, Header& h_, C_OnFinisher *c) : ls(l), h(h_), oncommit(c) {}
  void finish(int r) {
    ls->_finish_write_head(r, h, oncommit);
  }
};

void Journaler::write_head(Context *oncommit)
{
  Mutex::Locker l(lock);
  _write_head(oncommit);
}


void Journaler::_write_head(Context *oncommit)
{
  assert(!readonly);
  assert(state == STATE_ACTIVE);
  last_written.trimmed_pos = trimmed_pos;
  last_written.expire_pos = expire_pos;
  last_written.unused_field = expire_pos;
  last_written.write_pos = safe_pos;
  last_written.stream_format = stream_format;
  ldout(cct, 10) << "write_head " << last_written << dendl;
  
  // Avoid persisting bad pointers in case of bugs
  assert(last_written.write_pos >= last_written.expire_pos);
  assert(last_written.expire_pos >= last_written.trimmed_pos);

  last_wrote_head = ceph_clock_now(cct);

  bufferlist bl;
  ::encode(last_written, bl);
  SnapContext snapc;
  
  object_t oid = file_object_t(ino, 0);
  object_locator_t oloc(pg_pool);
  objecter->write_full(oid, oloc, snapc, bl, ceph_clock_now(cct), 0, 
		       NULL, 
		       wrap_finisher(new C_WriteHead(this, last_written, wrap_finisher(oncommit))));
}

void Journaler::_finish_write_head(int r, Header &wrote, C_OnFinisher *oncommit)
{
  Mutex::Locker l(lock);

  if (r < 0) {
    lderr(cct) << "_finish_write_head got " << cpp_strerror(r) << dendl;
    handle_write_error(r);
    return;
  }
  assert(!readonly);
  ldout(cct, 10) << "_finish_write_head " << wrote << dendl;
  last_committed = wrote;
  if (oncommit) {
    oncommit->complete(r);
  }

  _trim();  // trim?
}


/***************** WRITING *******************/

class Journaler::C_Flush : public Context {
  Journaler *ls;
  uint64_t start;
  utime_t stamp;
public:
  C_Flush(Journaler *l, int64_t s, utime_t st) : ls(l), start(s), stamp(st) {}
  void finish(int r) {
    ls->_finish_flush(r, start, stamp);
  }
};

void Journaler::_finish_flush(int r, uint64_t start, utime_t stamp)
{
  Mutex::Locker l(lock);
  assert(!readonly);

  if (r < 0) {
    lderr(cct) << "_finish_flush got " << cpp_strerror(r) << dendl;
    handle_write_error(r);
    return;
  }

  assert(start >= safe_pos);
  assert(start < flush_pos);

  // calc latency?
  if (logger) {
    utime_t lat = ceph_clock_now(cct);
    lat -= stamp;
    logger->tinc(logger_key_lat, lat);
  }

  // adjust safe_pos
  assert(pending_safe.count(start));
  pending_safe.erase(start);
  if (pending_safe.empty())
    safe_pos = flush_pos;
  else
    safe_pos = *pending_safe.begin();

  ldout(cct, 10) << "_finish_flush safe from " << start
		 << ", pending_safe " << pending_safe
		 << ", (prezeroing/prezero)/write/flush/safe positions now "
		 << "(" << prezeroing_pos << "/" << prezero_pos << ")/" << write_pos
		 << "/" << flush_pos << "/" << safe_pos
		 << dendl;

  // kick waiters <= safe_pos
  while (!waitfor_safe.empty()) {
    if (waitfor_safe.begin()->first > safe_pos)
      break;
    finish_contexts(cct, waitfor_safe.begin()->second);
    waitfor_safe.erase(waitfor_safe.begin());
  }
}



uint64_t Journaler::append_entry(bufferlist& bl)
{
  Mutex::Locker l(lock);

  assert(!readonly);
  uint32_t s = bl.length();

  if (!cct->_conf->journaler_allow_split_entries) {
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
      
      ldout(cct, 12) << "append_entry skipped " << (write_pos-owp) << " bytes to " << write_pos << " to avoid spanning stripe boundary" << dendl;
    }
  }
	
  
  // append
  size_t wrote = journal_stream.write(bl, &write_buf, write_pos);
  ldout(cct, 10) << "append_entry len " << s << " to " << write_pos << "~" << wrote << dendl;
  write_pos += wrote;

  // flush previous object?
  uint64_t su = get_layout_period();
  assert(su > 0);
  uint64_t write_off = write_pos % su;
  uint64_t write_obj = write_pos / su;
  uint64_t flush_obj = flush_pos / su;
  if (write_obj != flush_obj) {
    ldout(cct, 10) << " flushing completed object(s) (su " << su << " wro " << write_obj << " flo " << flush_obj << ")" << dendl;
    _do_flush(write_buf.length() - write_off);
  }

  return write_pos;
}


void Journaler::_do_flush(unsigned amount)
{
  if (write_pos == flush_pos)
    return;
  assert(write_pos > flush_pos);
  assert(!readonly);

  // flush
  unsigned len = write_pos - flush_pos;
  assert(len == write_buf.length());
  if (amount && amount < len)
    len = amount;

  // zero at least two full periods ahead.  this ensures
  // that the next object will not exist.
  uint64_t period = get_layout_period();
  if (flush_pos + len + 2*period > prezero_pos) {
    _issue_prezero();

    int64_t newlen = prezero_pos - flush_pos - period;
    if (newlen <= 0) {
      ldout(cct, 10) << "_do_flush wanted to do " << flush_pos << "~" << len
	       << " already too close to prezero_pos " << prezero_pos << ", zeroing first" << dendl;
      waiting_for_zero = true;
      return;
    }
    if (newlen < len) {
      ldout(cct, 10) << "_do_flush wanted to do " << flush_pos << "~" << len << " but hit prezero_pos " << prezero_pos
	       << ", will do " << flush_pos << "~" << newlen << dendl;
      len = newlen;
    } else {
      waiting_for_zero = false;
    }
  } else {
    waiting_for_zero = false;
  }
  ldout(cct, 10) << "_do_flush flushing " << flush_pos << "~" << len << dendl;
  
  // submit write for anything pending
  // flush _start_ pos to _finish_flush
  utime_t now = ceph_clock_now(cct);
  SnapContext snapc;

  Context *onsafe = new C_Flush(this, flush_pos, now);  // on COMMIT
  pending_safe.insert(flush_pos);

  bufferlist write_bl;

  // adjust pointers
  if (len == write_buf.length()) {
    write_bl.swap(write_buf);
  } else {
    write_buf.splice(0, len, &write_bl);
  }

  filer.write(ino, &layout, snapc,
	      flush_pos, len, write_bl, ceph_clock_now(cct),
	      0,
	      NULL, wrap_finisher(onsafe));

  flush_pos += len;
  assert(write_buf.length() == write_pos - flush_pos);
    
  ldout(cct, 10) << "_do_flush (prezeroing/prezero)/write/flush/safe pointers now at "
	   << "(" << prezeroing_pos << "/" << prezero_pos << ")/" << write_pos << "/" << flush_pos << "/" << safe_pos << dendl;

  _issue_prezero();
}


void Journaler::wait_for_flush(Context *onsafe)
{
  Mutex::Locker l(lock);
  if (stopping) {
    onsafe->complete(-EAGAIN);
    return;
  }
  _wait_for_flush(onsafe);
}

void Journaler::_wait_for_flush(Context *onsafe)
{
  assert(!readonly);
  
  // all flushed and safe?
  if (write_pos == safe_pos) {
    assert(write_buf.length() == 0);
    ldout(cct, 10) << "flush nothing to flush, (prezeroing/prezero)/write/flush/safe pointers at " 
	     << "(" << prezeroing_pos << "/" << prezero_pos << ")/" << write_pos << "/" << flush_pos << "/" << safe_pos << dendl;
    if (onsafe) {
      finisher->queue(onsafe, 0);
    }
    return;
  }

  // queue waiter
  if (onsafe) {
    waitfor_safe[write_pos].push_back(wrap_finisher(onsafe));
  }
}  

void Journaler::flush(Context *onsafe)
{
  Mutex::Locker l(lock);
  _flush(wrap_finisher(onsafe));
}

void Journaler::_flush(C_OnFinisher *onsafe)
{
  assert(!readonly);

  if (write_pos == flush_pos) {
    assert(write_buf.length() == 0);
    ldout(cct, 10) << "flush nothing to flush, (prezeroing/prezero)/write/flush/safe pointers at "
	     << "(" << prezeroing_pos << "/" << prezero_pos << ")/" << write_pos << "/" << flush_pos << "/" << safe_pos << dendl;
    if (onsafe) {
      onsafe->complete(0);
    }
  } else {
    // maybe buffer
    if (write_buf.length() < cct->_conf->journaler_batch_max) {
      // delay!  schedule an event.
      ldout(cct, 20) << "flush delaying flush" << dendl;
      if (delay_flush_event) {
        timer->cancel_event(delay_flush_event);
      }
      delay_flush_event = new C_DelayFlush(this);
      timer->add_event_after(cct->_conf->journaler_batch_interval, delay_flush_event);	
    } else {
      ldout(cct, 20) << "flush not delaying flush" << dendl;
      _do_flush();
    }
    _wait_for_flush(onsafe);
  }

  // write head?
  if (last_wrote_head.sec() + cct->_conf->journaler_write_head_interval < ceph_clock_now(cct).sec()) {
    _write_head();
  }
}


/*************** prezeroing ******************/

struct C_Journaler_Prezero : public Context {
  Journaler *journaler;
  uint64_t from, len;
  C_Journaler_Prezero(Journaler *j, uint64_t f, uint64_t l) : journaler(j), from(f), len(l) {}
  void finish(int r) {
    journaler->_finish_prezero(r, from, len);
  }
};

void Journaler::_issue_prezero()
{
  assert(prezeroing_pos >= flush_pos);

  // we need to zero at least two periods, minimum, to ensure that we have a full
  // empty object/period in front of us.
  uint64_t num_periods = MAX(2, cct->_conf->journaler_prezero_periods);

  /*
   * issue zero requests based on write_pos, even though the invariant
   * is that we zero ahead of flush_pos.
   */
  uint64_t period = get_layout_period();
  uint64_t to = write_pos + period * num_periods  + period - 1;
  to -= to % period;

  if (prezeroing_pos >= to) {
    ldout(cct, 20) << "_issue_prezero target " << to << " <= prezeroing_pos " << prezeroing_pos << dendl;
    return;
  }

  while (prezeroing_pos < to) {
    uint64_t len;
    if (prezeroing_pos % period == 0) {
      len = period;
      ldout(cct, 10) << "_issue_prezero removing " << prezeroing_pos << "~" << period << " (full period)" << dendl;
    } else {
      len = period - (prezeroing_pos % period);
      ldout(cct, 10) << "_issue_prezero zeroing " << prezeroing_pos << "~" << len << " (partial period)" << dendl;
    }
    SnapContext snapc;
    Context *c = wrap_finisher(new C_Journaler_Prezero(this, prezeroing_pos, len));
    filer.zero(ino, &layout, snapc, prezeroing_pos, len, ceph_clock_now(cct), 0, NULL, c);
    prezeroing_pos += len;
  }
}

// Lock cycle because we get called out of objecter callback (holding
// objecter read lock), but there are also cases where we take the journaler
// lock before calling into objecter to do I/O.
void Journaler::_finish_prezero(int r, uint64_t start, uint64_t len)
{
  Mutex::Locker l(lock);

  ldout(cct, 10) << "_prezeroed to " << start << "~" << len
	   << ", prezeroing/prezero was " << prezeroing_pos << "/" << prezero_pos
	   << ", pending " << pending_zero
	   << dendl;
  if (r < 0 && r != -ENOENT) {
    lderr(cct) << "_prezeroed got " << cpp_strerror(r) << dendl;
    handle_write_error(r);
    return;
  }

  assert(r == 0 || r == -ENOENT);

  if (start == prezero_pos) {
    prezero_pos += len;
    while (!pending_zero.empty() &&
	   pending_zero.begin().get_start() == prezero_pos) {
      interval_set<uint64_t>::iterator b(pending_zero.begin());
      prezero_pos += b.get_len();
      pending_zero.erase(b);
    }

    if (waiting_for_zero) {
      _do_flush();
    }
  } else {
    pending_zero.insert(start, len);
  }
  ldout(cct, 10) << "_prezeroed prezeroing/prezero now " << prezeroing_pos << "/" << prezero_pos
	   << ", pending " << pending_zero
	   << dendl;
}



/***************** READING *******************/


class Journaler::C_Read : public Context {
  Journaler *ls;
  uint64_t offset;
public:
  bufferlist bl;
  C_Read(Journaler *l, uint64_t o) : ls(l), offset(o) {}
  void finish(int r) {
    ls->_finish_read(r, offset, bl);
  }
};

class Journaler::C_RetryRead : public Context {
  Journaler *ls;
public:
  C_RetryRead(Journaler *l) : ls(l) {}
  void finish(int r) {
    // Should only be called from waitfor_safe i.e. already inside lock
    assert(ls->lock.is_locked_by_me());
    ls->_prefetch();
  }  
};

void Journaler::_finish_read(int r, uint64_t offset, bufferlist& bl)
{
  Mutex::Locker l(lock);

  if (r < 0) {
    ldout(cct, 0) << "_finish_read got error " << r << dendl;
    error = r;
    if (on_readable) {
      C_OnFinisher *f = on_readable;
      on_readable = 0;
      f->complete(r);
    }
    return;
  }
  assert(r>=0);

  ldout(cct, 10) << "_finish_read got " << offset << "~" << bl.length() << dendl;
  prefetch_buf[offset].swap(bl);

  try {
    _assimilate_prefetch();
  } catch (const buffer::error &err) {
    lderr(cct) << "_decode error from assimilate_prefetch" << dendl;
    error = -EINVAL;
    if (on_readable) {
      C_OnFinisher *f = on_readable;
      on_readable = 0;
      f->complete(error);
    }
    return;
  }
  _prefetch();
}

void Journaler::_assimilate_prefetch()
{
  bool was_readable = readable;

  bool got_any = false;
  while (!prefetch_buf.empty()) {
    map<uint64_t,bufferlist>::iterator p = prefetch_buf.begin();
    if (p->first != received_pos) {
      uint64_t gap = p->first - received_pos;
      ldout(cct, 10) << "_assimilate_prefetch gap of " << gap << " from received_pos " << received_pos
	       << " to first prefetched buffer " << p->first << dendl;
      break;
    }

    ldout(cct, 10) << "_assimilate_prefetch " << p->first << "~" << p->second.length() << dendl;
    received_pos += p->second.length();
    read_buf.claim_append(p->second);
    assert(received_pos <= requested_pos);
    prefetch_buf.erase(p);
    got_any = true;
  }

  if (got_any) {
    ldout(cct, 10) << "_assimilate_prefetch read_buf now " << read_pos << "~" << read_buf.length() 
	     << ", read pointers " << read_pos << "/" << received_pos << "/" << requested_pos
	     << dendl;

    // Update readability (this will also hit any decode errors resulting
    // from bad data)
    readable = _is_readable();
  }

  if ((got_any && !was_readable && readable) || read_pos == write_pos) {
    // readable!
    ldout(cct, 10) << "_finish_read now readable (or at journal end)" << dendl;
    if (on_readable) {
      C_OnFinisher *f = on_readable;
      on_readable = 0;
      f->complete(0);
    }
  }
}

void Journaler::_issue_read(uint64_t len)
{
  // make sure we're fully flushed
  _do_flush();

  // stuck at safe_pos?
  //  (this is needed if we are reading the tail of a journal we are also writing to)
  assert(requested_pos <= safe_pos);
  if (requested_pos == safe_pos) {
    ldout(cct, 10) << "_issue_read requested_pos = safe_pos = " << safe_pos << ", waiting" << dendl;
    assert(write_pos > requested_pos);
    if (flush_pos == safe_pos) {
      _flush(NULL);
    }
    assert(flush_pos > safe_pos);
    waitfor_safe[flush_pos].push_back(new C_RetryRead(this));
    return;
  }

  // don't read too much
  if (requested_pos + len > safe_pos) {
    len = safe_pos - requested_pos;
    ldout(cct, 10) << "_issue_read reading only up to safe_pos " << safe_pos << dendl;
  }

  // go.
  ldout(cct, 10) << "_issue_read reading " << requested_pos << "~" << len 
	   << ", read pointers " << read_pos << "/" << received_pos << "/" << (requested_pos+len)
	   << dendl;
  
  // step by period (object).  _don't_ do a single big filer.read()
  // here because it will wait for all object reads to complete before
  // giving us back any data.  this way we can process whatever bits
  // come in that are contiguous.
  uint64_t period = get_layout_period();
  while (len > 0) {
    uint64_t e = requested_pos + period;
    e -= e % period;
    uint64_t l = e - requested_pos;
    if (l > len)
      l = len;
    C_Read *c = new C_Read(this, requested_pos);
    filer.read(ino, &layout, CEPH_NOSNAP, requested_pos, l, &c->bl, 0, wrap_finisher(c), CEPH_OSD_OP_FLAG_FADVISE_DONTNEED);
    requested_pos += l;
    len -= l;
  }
}

void Journaler::_prefetch()
{
  ldout(cct, 10) << "_prefetch" << dendl;
  // prefetch
  uint64_t pf;
  if (temp_fetch_len) {
    ldout(cct, 10) << "_prefetch temp_fetch_len " << temp_fetch_len << dendl;
    pf = temp_fetch_len;
    temp_fetch_len = 0;
  } else {
    pf = fetch_len;
  }

  uint64_t raw_target = read_pos + pf;

  // read full log segments, so increase if necessary
  uint64_t period = get_layout_period();
  uint64_t remainder = raw_target % period;
  uint64_t adjustment = remainder ? period - remainder : 0;
  uint64_t target = raw_target + adjustment;

  // don't read past the log tail
  if (target > write_pos)
    target = write_pos;

  if (requested_pos < target) {
    uint64_t len = target - requested_pos;
    ldout(cct, 10) << "_prefetch " << pf << " requested_pos " << requested_pos << " < target " << target
	     << " (" << raw_target << "), prefetching " << len << dendl;
    _issue_read(len);
  }
}


/*
 * _is_readable() - return true if next entry is ready.
 */
bool Journaler::_is_readable()
{
  // anything to read?
  if (read_pos == write_pos)
    return false;

  // Check if the retrieve bytestream has enough for an entry
  uint64_t need;
  if (journal_stream.readable(read_buf, &need)) {
    return true;
  }

  ldout (cct, 10) << "_is_readable read_buf.length() == " << read_buf.length()
		  << ", but need " << need << " for next entry; fetch_len is " << fetch_len << dendl;

  // partial fragment at the end?
  if (received_pos == write_pos) {
    ldout(cct, 10) << "is_readable() detected partial entry at tail, adjusting write_pos to " << read_pos << dendl;

    // adjust write_pos
    prezeroing_pos = prezero_pos = write_pos = flush_pos = safe_pos = read_pos;
    assert(write_buf.length() == 0);

    // reset read state
    requested_pos = received_pos = read_pos;
    read_buf.clear();    
    
    // FIXME: truncate on disk?

    return false;
  }

  if (need > fetch_len) {
    temp_fetch_len = need;
    ldout(cct, 10) << "_is_readable noting temp_fetch_len " << temp_fetch_len << dendl;
  }

  ldout(cct, 10) << "_is_readable: not readable, returning false" << dendl;
  return false;
}

/*
 * is_readable() - kickstart prefetch, too
 */
bool Journaler::is_readable() 
{
  Mutex::Locker l(lock);

  if (error != 0) {
    return false;
  }

  bool r = readable;
  _prefetch();
  return r;
}

class Journaler::C_EraseFinish : public Context {
  Journaler *journaler;
  C_OnFinisher *completion;
  public:
  C_EraseFinish(Journaler *j, C_OnFinisher *c) : journaler(j), completion(c) {}
  void finish(int r) {
    journaler->_finish_erase(r, completion);
  }
};

/**
 * Entirely erase the journal, including header.  For use when you
 * have already made a copy of the journal somewhere else.
 */
void Journaler::erase(Context *completion)
{
  Mutex::Locker l(lock);

  // Async delete the journal data
  uint64_t first = trimmed_pos / get_layout_period();
  uint64_t num = (write_pos - trimmed_pos) / get_layout_period() + 2;
  filer.purge_range(ino, &layout, SnapContext(), first, num, ceph_clock_now(cct), 0,
      wrap_finisher(new C_EraseFinish(this, wrap_finisher(completion))));

  // We will not start the operation to delete the header until _finish_erase has
  // seen the data deletion succeed: otherwise if there was an error deleting data
  // we might prematurely delete the header thereby lose our reference to the data.
}

void Journaler::_finish_erase(int data_result, C_OnFinisher *completion)
{
  Mutex::Locker l(lock);

  if (data_result == 0) {
    // Async delete the journal header
    filer.purge_range(ino, &layout, SnapContext(), 0, 1, ceph_clock_now(cct), 0,
        wrap_finisher(completion));
  } else {
    lderr(cct) << "Failed to delete journal " << ino << " data: " << cpp_strerror(data_result) << dendl;
    completion->complete(data_result);
  }
}

/* try_read_entry(bl)
 *  read entry into bl if it's ready.
 *  otherwise, do nothing.
 */
bool Journaler::try_read_entry(bufferlist& bl)
{
  Mutex::Locker l(lock);

  if (!readable) {
    ldout(cct, 10) << "try_read_entry at " << read_pos << " not readable" << dendl;
    return false;
  }

  uint64_t start_ptr;
  size_t consumed;
  try {
    consumed = journal_stream.read(read_buf, &bl, &start_ptr);
    if (stream_format >= JOURNAL_FORMAT_RESILIENT) {
      assert(start_ptr == read_pos);
    }
  } catch (const buffer::error &e) {
    lderr(cct) << __func__ << ": decode error from journal_stream" << dendl;
    error = -EINVAL;
    return false;
  }

  ldout(cct, 10) << "try_read_entry at " << read_pos << " read " 
	   << read_pos << "~" << consumed << " (have " << read_buf.length() << ")" << dendl;

  read_pos += consumed;
  try {
    // We were readable, we might not be any more
    readable = _is_readable();
  } catch (const buffer::error &e) {
    lderr(cct) << __func__ << ": decode error from _is_readable" << dendl;
    error = -EINVAL;
    return false;
  }

  // prefetch?
  _prefetch();
  return true;
}

void Journaler::wait_for_readable(Context *onreadable)
{
  Mutex::Locker l(lock);
  if (stopping) {
    onreadable->complete(-EAGAIN);
    return;
  }

  assert(on_readable == 0);
  if (!readable) {
    ldout(cct, 10) << "wait_for_readable at " << read_pos << " onreadable " << onreadable << dendl;
    on_readable = wrap_finisher(onreadable);
  } else {
    // race with OSD reply
    finisher->queue(onreadable, 0);
  }
}




/***************** TRIMMING *******************/


class Journaler::C_Trim : public Context {
  Journaler *ls;
  uint64_t to;
public:
  C_Trim(Journaler *l, int64_t t) : ls(l), to(t) {}
  void finish(int r) {
    ls->_finish_trim(r, to);
  }
};

void Journaler::trim()
{
  Mutex::Locker l(lock);
  _trim();
}

void Journaler::_trim()
{
  assert(!readonly);
  uint64_t period = get_layout_period();
  uint64_t trim_to = last_committed.expire_pos;
  trim_to -= trim_to % period;
  ldout(cct, 10) << "trim last_commited head was " << last_committed
	   << ", can trim to " << trim_to
	   << dendl;
  if (trim_to == 0 || trim_to == trimming_pos) {
    ldout(cct, 10) << "trim already trimmed/trimming to " 
	     << trimmed_pos << "/" << trimming_pos << dendl;
    return;
  }

  if (trimming_pos > trimmed_pos) {
    ldout(cct, 10) << "trim already trimming atm, try again later.  trimmed/trimming is " 
	     << trimmed_pos << "/" << trimming_pos << dendl;
    return;
  }
  
  // trim
  assert(trim_to <= write_pos);
  assert(trim_to <= expire_pos);
  assert(trim_to > trimming_pos);
  ldout(cct, 10) << "trim trimming to " << trim_to 
	   << ", trimmed/trimming/expire are " 
	   << trimmed_pos << "/" << trimming_pos << "/" << expire_pos
	   << dendl;

  // delete range of objects
  uint64_t first = trimming_pos / period;
  uint64_t num = (trim_to - trimming_pos) / period;
  SnapContext snapc;
  filer.purge_range(ino, &layout, snapc, first, num, ceph_clock_now(cct), 0, 
		    wrap_finisher(new C_Trim(this, trim_to)));
  trimming_pos = trim_to;  
}

void Journaler::_finish_trim(int r, uint64_t to)
{
  Mutex::Locker l(lock);

  assert(!readonly);
  ldout(cct, 10) << "_finish_trim trimmed_pos was " << trimmed_pos
	   << ", trimmed/trimming/expire now "
	   << to << "/" << trimming_pos << "/" << expire_pos
	   << dendl;
  if (r < 0 && r != -ENOENT) {
    lderr(cct) << "_finish_trim got " << cpp_strerror(r) << dendl;
    handle_write_error(r);
    return;
  }

  assert(r >= 0 || r == -ENOENT);
  
  assert(to <= trimming_pos);
  assert(to > trimmed_pos);
  trimmed_pos = to;
}

void Journaler::handle_write_error(int r)
{
  assert(lock.is_locked_by_me());

  lderr(cct) << "handle_write_error " << cpp_strerror(r) << dendl;
  if (on_write_error) {
    on_write_error->complete(r);
    on_write_error = NULL;
    called_write_error = true;
  } else if (called_write_error) {
    /* We don't call error handler more than once, subsequent errors are dropped --
     * this is okay as long as the error handler does something dramatic like respawn */
    lderr(cct) << __func__ << ": multiple write errors, handler already called" << dendl;
  } else {
    assert(0 == "unhandled write error");
  }
}


/**
 * Test whether the 'read_buf' byte stream has enough data to read
 * an entry
 *
 * sets 'next_envelope_size' to the number of bytes needed to advance (enough
 * to get the next header if header was unavailable, or enough to get the whole
 * next entry if the header was available but the body wasn't).
 */
bool JournalStream::readable(bufferlist &read_buf, uint64_t *need) const
{
  assert(need != NULL);

  uint32_t entry_size = 0;
  uint64_t entry_sentinel = 0;
  bufferlist::iterator p = read_buf.begin();

  // Do we have enough data to decode an entry prefix?
  if (format >= JOURNAL_FORMAT_RESILIENT) {
    *need = sizeof(entry_size) + sizeof(entry_sentinel);
  } else {
    *need = sizeof(entry_size);
  }
  if (read_buf.length() >= *need) {
    if (format >= JOURNAL_FORMAT_RESILIENT) {
      ::decode(entry_sentinel, p);
      if (entry_sentinel != sentinel) {
        throw buffer::malformed_input("Invalid sentinel"); 
      }
    }

    ::decode(entry_size, p);
  } else {
    return false;
  }

  // Do we have enough data to decode an entry prefix, payload and suffix?
  if (format >= JOURNAL_FORMAT_RESILIENT) {
    *need = JOURNAL_ENVELOPE_RESILIENT + entry_size;
  } else {
    *need = JOURNAL_ENVELOPE_LEGACY + entry_size;
  }
  if (read_buf.length() >= *need) {
    return true;  // No more bytes needed
  }

  return false;
}


/**
 * Consume one entry from a journal byte stream 'from', splicing a
 * serialized LogEvent blob into 'entry'.
 *
 * 'entry' must be non null and point to an empty bufferlist.
 *
 * 'from' must contain sufficient valid data (i.e. readable is true).
 *
 * 'start_ptr' will be set to the entry's start pointer, if the collection
 * format provides it.  It may not be null.
 *
 * @returns The number of bytes consumed from the `from` byte stream.  Note
 *          that this is not equal to the length of `entry`, which contains
 *          the inner serialized LogEvent and not the envelope.
 */
size_t JournalStream::read(bufferlist &from, bufferlist *entry, uint64_t *start_ptr)
{
  assert(start_ptr != NULL);
  assert(entry != NULL);
  assert(entry->length() == 0);

  uint32_t entry_size = 0;

  // Consume envelope prefix: entry_size and entry_sentinel
  bufferlist::iterator from_ptr = from.begin();
  if (format >= JOURNAL_FORMAT_RESILIENT) {
    uint64_t entry_sentinel = 0;
    ::decode(entry_sentinel, from_ptr);
    // Assertion instead of clean check because of precondition of this
    // fn is that readable() already passed
    assert(entry_sentinel == sentinel);
  }
  ::decode(entry_size, from_ptr);

  // Read out the payload
  from_ptr.copy(entry_size, *entry);

  // Consume the envelope suffix (start_ptr)
  if (format >= JOURNAL_FORMAT_RESILIENT) {
    ::decode(*start_ptr, from_ptr);
  } else {
    *start_ptr = 0;
  }

  // Trim the input buffer to discard the bytes we have consumed
  from.splice(0, from_ptr.get_off());

  return from_ptr.get_off();
}


/**
 * Append one entry
 */
size_t JournalStream::write(bufferlist &entry, bufferlist *to, uint64_t const &start_ptr)
{
  assert(to != NULL);

  uint32_t const entry_size = entry.length();
  if (format >= JOURNAL_FORMAT_RESILIENT) {
    ::encode(sentinel, *to);
  }
  ::encode(entry_size, *to);
  to->claim_append(entry);
  if (format >= JOURNAL_FORMAT_RESILIENT) {
    ::encode(start_ptr, *to);
  }

  if (format >= JOURNAL_FORMAT_RESILIENT) {
    return JOURNAL_ENVELOPE_RESILIENT + entry_size;
  } else {
    return JOURNAL_ENVELOPE_LEGACY + entry_size;
  }
}

/**
 * set write error callback
 *
 * Set a callback/context to trigger if we get a write error from
 * the objecter.  This may be from an explicit request (e.g., flush)
 * or something async the journaler did on its own (e.g., journal
 * header update).
 *
 * It is only used once; if the caller continues to use the
 * Journaler and wants to hear about errors, it needs to reset the
 * error_handler.
 *
 * @param c callback/context to trigger on error
 */
void Journaler::set_write_error_handler(Context *c) {
  Mutex::Locker l(lock);
  assert(!on_write_error);
  on_write_error = wrap_finisher(c);
  called_write_error = false;
}


/**
 * Wrap a context in a C_OnFinisher, if it is non-NULL
 *
 * Utility function to avoid lots of error-prone and verbose
 * NULL checking on contexts passed in.
 */
C_OnFinisher *Journaler::wrap_finisher(Context *c)
{
  if (c != NULL) {
    return new C_OnFinisher(c, finisher);
  } else {
    return NULL;
  }
}

void Journaler::shutdown()
{
  Mutex::Locker l(lock);

  ldout(cct, 1) << __func__ << dendl;

  readable = false;
  stopping = true;

  // Kick out anyone reading from journal
  error = -EAGAIN;
  if (on_readable) {
    C_OnFinisher *f = on_readable;
    on_readable = 0;
    f->complete(-EAGAIN);
  }

  finish_contexts(cct, waitfor_recover, 0);

  std::map<uint64_t, std::list<Context*> >::iterator i;
  for (i = waitfor_safe.begin(); i != waitfor_safe.end(); ++i) {
    finish_contexts(cct, i->second, -EAGAIN);
  }
  waitfor_safe.clear();
}

