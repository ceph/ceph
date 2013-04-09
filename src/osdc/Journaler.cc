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

#define dout_subsys ceph_subsys_journaler
#undef dout_prefix
#define dout_prefix *_dout << objecter->messenger->get_myname() << ".journaler" << (readonly ? "(ro) ":"(rw) ")


void Journaler::set_readonly()
{
  ldout(cct, 1) << "set_readonly" << dendl;
  readonly = true;
}

void Journaler::set_writeable()
{
  ldout(cct, 1) << "set_writeable" << dendl;
  readonly = false;
}

void Journaler::create(ceph_file_layout *l)
{
  assert(!readonly);
  ldout(cct, 1) << "create blank journal" << dendl;
  state = STATE_ACTIVE;

  set_layout(l);

  prezeroing_pos = prezero_pos = write_pos = flush_pos = safe_pos =
    read_pos = requested_pos = received_pos =
    expire_pos = trimming_pos = trimmed_pos = layout.fl_stripe_count * layout.fl_object_size;
}

void Journaler::set_layout(ceph_file_layout *l)
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
  ldout(cct, 1) << "recover start" << dendl;
  assert(state != STATE_ACTIVE);
  assert(readonly);

  if (onread)
    waitfor_recover.push_back(onread);
  
  if (state != STATE_UNDEF) {
    ldout(cct, 1) << "recover - already recoverying" << dendl;
    return;
  }

  ldout(cct, 1) << "read_head" << dendl;
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
  ldout(cct, 10) << "reread_head" << dendl;
  assert(state == STATE_ACTIVE);

  state = STATE_REREADHEAD;
  C_RereadHead *fin = new C_RereadHead(this, onfinish);
  read_head(fin, &fin->bl);
}

void Journaler::_finish_reread_head(int r, bufferlist& bl, Context *finish)
{
  //read on-disk header into
  assert(bl.length() || r < 0 );

  // unpack header
  Header h;
  bufferlist::iterator p = bl.begin();
  ::decode(h, p);
  prezeroing_pos = prezero_pos = write_pos = flush_pos = safe_pos = h.write_pos;
  expire_pos = h.expire_pos;
  trimmed_pos = trimming_pos = h.trimmed_pos;
  init_headers(h);
  state = STATE_ACTIVE;
  finish->finish(r);
  delete finish;
}

void Journaler::_finish_read_head(int r, bufferlist& bl)
{
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
  Header h;
  bufferlist::iterator p = bl.begin();
  ::decode(h, p);

  if (h.magic != magic) {
    ldout(cct, 0) << "on disk magic '" << h.magic << "' != my magic '"
	    << magic << "'" << dendl;
    list<Context*> ls;
    ls.swap(waitfor_recover);
    finish_contexts(cct, ls, -EINVAL);
    return;
  }

  prezeroing_pos = prezero_pos = write_pos = flush_pos = safe_pos = h.write_pos;
  read_pos = requested_pos = received_pos = expire_pos = h.expire_pos;
  trimmed_pos = trimming_pos = h.trimmed_pos;

  init_headers(h);
  set_layout(&h.layout);

  ldout(cct, 1) << "_finish_read_head " << h << ".  probing for end of log (from " << write_pos << ")..." << dendl;
  C_ProbeEnd *fin = new C_ProbeEnd(this);
  state = STATE_PROBING;
  probe(fin, &fin->end);
}

void Journaler::probe(Context *finish, uint64_t *end)
{
  ldout(cct, 1) << "probing for end of the log" << dendl;
  assert(state == STATE_PROBING || state == STATE_REPROBING);
  // probe the log
  filer.probe(ino, &layout, CEPH_NOSNAP,
	      write_pos, end, 0, true, 0, finish);
}

void Journaler::reprobe(Context *finish)
{
  ldout(cct, 10) << "reprobe" << dendl;
  assert(state == STATE_ACTIVE);

  state = STATE_REPROBING;
  C_ReProbe *fin = new C_ReProbe(this, finish);
  probe(fin, &fin->end);
}


void Journaler::_finish_reprobe(int r, uint64_t new_end, Context *onfinish) {
  assert(new_end >= write_pos || r < 0);
  ldout(cct, 1) << "_finish_reprobe new_end = " << new_end 
	  << " (header had " << write_pos << ")."
	  << dendl;
  prezeroing_pos = prezero_pos = write_pos = flush_pos = safe_pos = new_end;
  state = STATE_ACTIVE;
  onfinish->finish(r);
  delete onfinish;
}

void Journaler::_finish_probe_end(int r, uint64_t end)
{
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
    ls->_finish_write_head(r, h, oncommit);
  }
};

void Journaler::write_head(Context *oncommit)
{
  assert(!readonly);
  assert(state == STATE_ACTIVE);
  last_written.trimmed_pos = trimmed_pos;
  last_written.expire_pos = expire_pos;
  last_written.unused_field = expire_pos;
  last_written.write_pos = safe_pos;
  ldout(cct, 10) << "write_head " << last_written << dendl;
  
  last_wrote_head = ceph_clock_now(cct);

  bufferlist bl;
  ::encode(last_written, bl);
  SnapContext snapc;
  
  object_t oid = file_object_t(ino, 0);
  object_locator_t oloc(pg_pool);
  objecter->write_full(oid, oloc, snapc, bl, ceph_clock_now(cct), 0, 
		       NULL, 
		       new C_WriteHead(this, last_written, oncommit));
}

void Journaler::_finish_write_head(int r, Header &wrote, Context *oncommit)
{
  if (r < 0) {
    lderr(cct) << "_finish_write_head got " << cpp_strerror(r) << dendl;
    handle_write_error(r);
    return;
  }
  assert(!readonly);
  ldout(cct, 10) << "_finish_write_head " << wrote << dendl;
  last_committed = wrote;
  if (oncommit) {
    oncommit->finish(r);
    delete oncommit;
  }

  trim();  // trim?
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
	
  ldout(cct, 10) << "append_entry len " << bl.length() << " to " << write_pos << "~" << (bl.length() + sizeof(uint32_t)) << dendl;
  
  // append
  ::encode(s, write_buf);
  write_buf.claim_append(bl);
  write_pos += sizeof(s) + s;

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
	      NULL, onsafe);

  flush_pos += len;
  assert(write_buf.length() == write_pos - flush_pos);
    
  ldout(cct, 10) << "_do_flush (prezeroing/prezero)/write/flush/safe pointers now at "
	   << "(" << prezeroing_pos << "/" << prezero_pos << ")/" << write_pos << "/" << flush_pos << "/" << safe_pos << dendl;

  _issue_prezero();
}



void Journaler::wait_for_flush(Context *onsafe)
{
  assert(!readonly);
  
  // all flushed and safe?
  if (write_pos == safe_pos) {
    assert(write_buf.length() == 0);
    ldout(cct, 10) << "flush nothing to flush, (prezeroing/prezero)/write/flush/safe pointers at " 
	     << "(" << prezeroing_pos << "/" << prezero_pos << ")/" << write_pos << "/" << flush_pos << "/" << safe_pos << dendl;
    if (onsafe) {
      onsafe->finish(0);
      delete onsafe;
      onsafe = 0;
    }
    return;
  }

  // queue waiter
  if (onsafe) 
    waitfor_safe[write_pos].push_back(onsafe);
}  

void Journaler::flush(Context *onsafe)
{
  assert(!readonly);

  if (write_pos == flush_pos) {
    assert(write_buf.length() == 0);
    ldout(cct, 10) << "flush nothing to flush, (prezeroing/prezero)/write/flush/safe pointers at "
	     << "(" << prezeroing_pos << "/" << prezero_pos << ")/" << write_pos << "/" << flush_pos << "/" << safe_pos << dendl;
    if (onsafe) {
      onsafe->finish(0);
      delete onsafe;
    }
  } else {
    if (1) {
      // maybe buffer
      if (write_buf.length() < cct->_conf->journaler_batch_max) {
	// delay!  schedule an event.
	ldout(cct, 20) << "flush delaying flush" << dendl;
	if (delay_flush_event)
	  timer->cancel_event(delay_flush_event);
	delay_flush_event = new C_DelayFlush(this);
	timer->add_event_after(cct->_conf->journaler_batch_interval, delay_flush_event);	
      } else {
	ldout(cct, 20) << "flush not delaying flush" << dendl;
	_do_flush();
      }
    } else {
      // always flush
      _do_flush();
    }
    wait_for_flush(onsafe);
  }

  // write head?
  if (last_wrote_head.sec() + cct->_conf->journaler_write_head_interval < ceph_clock_now(cct).sec()) {
    write_head();
  }
}


/*************** prezeroing ******************/

struct C_Journaler_Prezero : public Context {
  Journaler *journaler;
  uint64_t from, len;
  C_Journaler_Prezero(Journaler *j, uint64_t f, uint64_t l) : journaler(j), from(f), len(l) {}
  void finish(int r) {
    journaler->_prezeroed(r, from, len);
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
    Context *c = new C_Journaler_Prezero(this, prezeroing_pos, len);
    filer.zero(ino, &layout, snapc, prezeroing_pos, len, ceph_clock_now(cct), 0, NULL, c);
    prezeroing_pos += len;
  }
}

void Journaler::_prezeroed(int r, uint64_t start, uint64_t len)
{
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
    // kickstart.
    ls->_prefetch();
  }  
};

void Journaler::_finish_read(int r, uint64_t offset, bufferlist& bl)
{
  if (r < 0) {
    ldout(cct, 0) << "_finish_read got error " << r << dendl;
    error = r;
    if (on_readable) {
      Context *f = on_readable;
      on_readable = 0;
      f->finish(r);
      delete f;
    }
    return;
  }
  assert(r>=0);

  ldout(cct, 10) << "_finish_read got " << offset << "~" << bl.length() << dendl;
  prefetch_buf[offset].swap(bl);

  _assimilate_prefetch();
  _prefetch();
}

void Journaler::_assimilate_prefetch()
{
  bool was_readable = _is_readable();

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

  if (got_any)
    ldout(cct, 10) << "_assimilate_prefetch read_buf now " << read_pos << "~" << read_buf.length() 
	     << ", read pointers " << read_pos << "/" << received_pos << "/" << requested_pos
	     << dendl;

  if ((got_any && !was_readable && _is_readable()) ||
      read_pos == write_pos) {
    // readable!
    ldout(cct, 10) << "_finish_read now readable (or at journal end)" << dendl;
    if (on_readable) {
      Context *f = on_readable;
      on_readable = 0;
      f->finish(0);
      delete f;
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
    if (flush_pos == safe_pos)
      flush();
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
    filer.read(ino, &layout, CEPH_NOSNAP, requested_pos, l, &c->bl, 0, c);
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

  // have enough for entry size?
  uint32_t s = 0;
  bufferlist::iterator p = read_buf.begin();
  if (read_buf.length() >= sizeof(s))
    ::decode(s, p);

  // entry and payload?
  if (read_buf.length() >= sizeof(s) &&
      read_buf.length() >= sizeof(s) + s) 
    return true;  // yep, next entry is ready.

  ldout (cct, 10) << "_is_readable read_buf.length() == " << read_buf.length()
		  << ", but need " << s + sizeof(s)
		  << " for next entry; fetch_len is " << fetch_len << dendl;

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

  uint64_t need = sizeof(s) + s;
  if (need > fetch_len) {
    temp_fetch_len = sizeof(s) + s;
    ldout(cct, 10) << "_is_readable noting temp_fetch_len " << temp_fetch_len
	     << " for len " << s << " entry" << dendl;
  }

  ldout(cct, 10) << "_is_readable: not readable, returning false" << dendl;
  return false;
}

/*
 * is_readable() - kickstart prefetch, too
 */
bool Journaler::is_readable() 
{
  bool r = _is_readable();
  _prefetch();
  return r;
}


/* try_read_entry(bl)
 *  read entry into bl if it's ready.
 *  otherwise, do nothing.  (well, we'll start fetching it for good measure.)
 */
bool Journaler::try_read_entry(bufferlist& bl)
{
  if (!is_readable()) {  // this may start a read. 
    ldout(cct, 10) << "try_read_entry at " << read_pos << " not readable" << dendl;
    return false;
  }
  
  uint32_t s;
  {
    bufferlist::iterator p = read_buf.begin();
    ::decode(s, p);
  }
  assert(read_buf.length() >= sizeof(s) + s);
  
  ldout(cct, 10) << "try_read_entry at " << read_pos << " reading " 
	   << read_pos << "~" << (sizeof(s)+s) << " (have " << read_buf.length() << ")" << dendl;

  if (s == 0) {
    ldout(cct, 0) << "try_read_entry got 0 len entry at offset " << read_pos << dendl;
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
  ldout(cct, 10) << "wait_for_readable at " << read_pos << " onreadable " << onreadable << dendl;
  assert(!_is_readable());
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
		    new C_Trim(this, trim_to));
  trimming_pos = trim_to;  
}

void Journaler::_trim_finish(int r, uint64_t to)
{
  assert(!readonly);
  ldout(cct, 10) << "_trim_finish trimmed_pos was " << trimmed_pos
	   << ", trimmed/trimming/expire now "
	   << to << "/" << trimming_pos << "/" << expire_pos
	   << dendl;
  if (r < 0 && r != -ENOENT) {
    lderr(cct) << "_trim_finish got " << cpp_strerror(r) << dendl;
    handle_write_error(r);
    return;
  }

  assert(r >= 0 || r == -ENOENT);
  
  assert(to <= trimming_pos);
  assert(to > trimmed_pos);
  trimmed_pos = to;

  // finishers?
  while (!waitfor_trim.empty() &&
	 waitfor_trim.begin()->first <= trimmed_pos) {
    finish_contexts(cct, waitfor_trim.begin()->second, 0);
    waitfor_trim.erase(waitfor_trim.begin());
  }
}

void Journaler::handle_write_error(int r)
{
  lderr(cct) << "handle_write_error " << cpp_strerror(r) << dendl;
  if (on_write_error) {
    on_write_error->finish(r);
    delete on_write_error;
    on_write_error = NULL;
  } else {
    assert(0 == "unhandled write error");
  }
}


// eof.
