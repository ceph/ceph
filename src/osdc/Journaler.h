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

/* Journaler
 *
 * This class stripes a serial log over objects on the store.  Four logical pointers:
 *
 *  write_pos - where we're writing new entries
 *   unused_field - where we're reading old entires
 * expire_pos - what is deemed "old" by user
 *   trimmed_pos - where we're expiring old items
 *
 *  trimmed_pos <= expire_pos <= unused_field <= write_pos.
 *
 * Often, unused_field <= write_pos (as with MDS log).  During recovery, write_pos is undefined
 * until the end of the log is discovered.
 *
 * A "head" struct at the beginning of the log is used to store metadata at
 * regular intervals.  The basic invariants include:
 *
 *   head.unused_field   <= unused_field   -- the head may "lag", since it's updated lazily.
 *   head.write_pos  <= write_pos
 *   head.expire_pos <= expire_pos
 *   head.trimmed_pos   <= trimmed_pos
 *
 * More significantly,
 *
 *   head.expire_pos >= trimmed_pos -- this ensures we can find the "beginning" of the log
 *                                  as last recorded, before it is trimmed.  trimming will
 *                                  block until a sufficiently current expire_pos is committed.
 *
 * To recover log state, we simply start at the last write_pos in the head, and probe the
 * object sequence sizes until we read the end.  
 *
 * Head struct is stored in the first object.  Actual journal starts after layout.period() bytes.
 *
 */

#ifndef CEPH_JOURNALER_H
#define CEPH_JOURNALER_H

#include "Objecter.h"
#include "Filer.h"

#include <list>
#include <map>

class CephContext;
class Context;
class PerfCounters;

class Journaler {
public:
  CephContext *cct;
  // this goes at the head of the log "file".
  struct Header {
    uint64_t trimmed_pos;
    uint64_t expire_pos;
    uint64_t unused_field;
    uint64_t write_pos;
    string magic;
    ceph_file_layout layout;

    Header(const char *m="") :
      trimmed_pos(0), expire_pos(0), unused_field(0), write_pos(0),
      magic(m) {
      memset(&layout, 0, sizeof(layout));
    }

    void encode(bufferlist &bl) const {
      __u8 struct_v = 1;
      ::encode(struct_v, bl);
      ::encode(magic, bl);
      ::encode(trimmed_pos, bl);
      ::encode(expire_pos, bl);
      ::encode(unused_field, bl);
      ::encode(write_pos, bl);
      ::encode(layout, bl);
    }
    void decode(bufferlist::iterator &bl) {
      __u8 struct_v;
      ::decode(struct_v, bl);
      ::decode(magic, bl);
      ::decode(trimmed_pos, bl);
      ::decode(expire_pos, bl);
      ::decode(unused_field, bl);
      ::decode(write_pos, bl);
      ::decode(layout, bl);
    }

    void dump(Formatter *f) const {
      f->open_object_section("journal_header");
      {
	f->dump_string("magic", magic);
	f->dump_unsigned("write_pos", write_pos);
	f->dump_unsigned("expire_pos", expire_pos);
	f->dump_unsigned("trimmed_pos", trimmed_pos);
	f->open_object_section("layout");
	{
	  f->dump_unsigned("stripe_unit", layout.fl_stripe_unit);
	  f->dump_unsigned("stripe_count", layout.fl_stripe_unit);
	  f->dump_unsigned("object_size", layout.fl_stripe_unit);
	  f->dump_unsigned("cas_hash", layout.fl_stripe_unit);
	  f->dump_unsigned("object_stripe_unit", layout.fl_stripe_unit);
	  f->dump_unsigned("pg_pool", layout.fl_stripe_unit);
	}
	f->close_section(); // layout
      }
      f->close_section(); // journal_header
    }

    static void generate_test_instances(list<Header*> &ls)
    {
      ls.push_back(new Header());
      ls.push_back(new Header());
      ls.back()->trimmed_pos = 1;
      ls.back()->expire_pos = 2;
      ls.back()->unused_field = 3;
      ls.back()->write_pos = 4;
      ls.back()->magic = "magique";
    }
  } last_written, last_committed;
  WRITE_CLASS_ENCODER(Header)

private:
  // me
  inodeno_t ino;
  int64_t pg_pool;
  bool readonly;
  ceph_file_layout layout;

  const char *magic;
  Objecter *objecter;
  Filer filer;

  PerfCounters *logger;
  int logger_key_lat;

  SafeTimer *timer;

  class C_DelayFlush : public Context {
    Journaler *journaler;
  public:
    C_DelayFlush(Journaler *j) : journaler(j) {}
    void finish(int r) {
      journaler->delay_flush_event = 0;
      journaler->_do_flush();
    }
  } *delay_flush_event;


  // my state
  static const int STATE_UNDEF = 0;
  static const int STATE_READHEAD = 1;
  static const int STATE_PROBING = 2;
  static const int STATE_ACTIVE = 3;
  static const int STATE_REREADHEAD = 4;
  static const int STATE_REPROBING = 5;

  int state;
  int error;

  // header
  utime_t last_wrote_head;
  void _finish_write_head(int r, Header &wrote, Context *oncommit);
  class C_WriteHead;
  friend class C_WriteHead;

  list<Context*> waitfor_recover;
  void read_head(Context *on_finish, bufferlist *bl);
  void _finish_read_head(int r, bufferlist& bl);
  void _finish_reread_head(int r, bufferlist& bl, Context *finish);
  void probe(Context *finish, uint64_t *end);
  void _finish_probe_end(int r, uint64_t end);
  void _finish_reprobe(int r, uint64_t end, Context *onfinish);
  void _finish_reread_head_and_probe(int r, Context *onfinish);
  class C_ReadHead;
  friend class C_ReadHead;
  class C_ProbeEnd;
  friend class C_ProbeEnd;
  class C_RereadHead;
  friend class C_RereadHead;
  class C_ReProbe;
  friend class C_ReProbe;
  class C_RereadHeadProbe;
  friend class C_RereadHeadProbe;



  // writer
  uint64_t prezeroing_pos;
  uint64_t prezero_pos;     // we zero journal space ahead of write_pos to avoid problems with tail probing
  uint64_t write_pos;       // logical write position, where next entry will go
  uint64_t flush_pos;       // where we will flush. if write_pos>flush_pos, we're buffering writes.
  uint64_t safe_pos;        // what has been committed safely to disk.
  bufferlist write_buf;  // write buffer.  flush_pos + write_buf.length() == write_pos.

  bool waiting_for_zero;
  interval_set<uint64_t> pending_zero;  // non-contig bits we've zeroed
  std::set<uint64_t> pending_safe;
  std::map<uint64_t, std::list<Context*> > waitfor_safe; // when safe through given offset

  void _do_flush(unsigned amount=0);
  void _finish_flush(int r, uint64_t start, utime_t stamp);
  class C_Flush;
  friend class C_Flush;

  // reader
  uint64_t read_pos;      // logical read position, where next entry starts.
  uint64_t requested_pos; // what we've requested from OSD.
  uint64_t received_pos;  // what we've received from OSD.
  bufferlist read_buf; // read buffer.  unused_field + read_buf.length() == prefetch_pos.

  map<uint64_t,bufferlist> prefetch_buf;

  uint64_t fetch_len;     // how much to read at a time
  uint64_t temp_fetch_len;

  // for wait_for_readable()
  Context    *on_readable;

  Context    *on_write_error;

  void _finish_read(int r, uint64_t offset, bufferlist &bl); // read completion callback
  void _assimilate_prefetch();
  void _issue_read(uint64_t len);  // read some more
  void _prefetch();             // maybe read ahead
  class C_Read;
  friend class C_Read;
  class C_RetryRead;
  friend class C_RetryRead;

  // trimmer
  uint64_t expire_pos;    // what we're allowed to trim to
  uint64_t trimming_pos;      // what we've requested to trim through
  uint64_t trimmed_pos;   // what has been trimmed
  map<uint64_t, list<Context*> > waitfor_trim;

  void _trim_finish(int r, uint64_t to);
  class C_Trim;
  friend class C_Trim;

  void _issue_prezero();
  void _prezeroed(int r, uint64_t from, uint64_t len);
  friend struct C_Journaler_Prezero;

  // only init_headers when following or first reading off-disk
  void init_headers(Header& h) {
    assert(readonly ||
           state == STATE_READHEAD ||
           state == STATE_REREADHEAD);
    last_written = last_committed = h;
  }

  /**
   * handle a write error
   *
   * called when we get an objecter error on a write.
   *
   * @param r error code
   */
  void handle_write_error(int r);

public:
  Journaler(inodeno_t ino_, int64_t pool, const char *mag, Objecter *obj, PerfCounters *l, int lkey, SafeTimer *tim) : 
    cct(obj->cct), last_written(mag), last_committed(mag),
    ino(ino_), pg_pool(pool), readonly(true), magic(mag),
    objecter(obj), filer(objecter), logger(l), logger_key_lat(lkey),
    timer(tim), delay_flush_event(0),
    state(STATE_UNDEF), error(0),
    prezeroing_pos(0), prezero_pos(0), write_pos(0), flush_pos(0), safe_pos(0),
    waiting_for_zero(false),
    read_pos(0), requested_pos(0), received_pos(0),
    fetch_len(0), temp_fetch_len(0),
    on_readable(0), on_write_error(NULL),
    expire_pos(0), trimming_pos(0), trimmed_pos(0) 
  {
    memset(&layout, 0, sizeof(layout));
  }

  void reset() {
    assert(state == STATE_ACTIVE);
    readonly = true;
    delay_flush_event = 0;
    state = STATE_UNDEF;
    error = 0;
    prezeroing_pos = 0;
    prezero_pos = 0;
    write_pos = 0;
    flush_pos = 0;
    safe_pos = 0;
    read_pos = 0;
    requested_pos = 0;
    received_pos = 0;
    fetch_len = 0;
    assert(!on_readable);
    expire_pos = 0;
    trimming_pos = 0;
    trimmed_pos = 0;
    waiting_for_zero = false;
  }

  // me
  //void open(Context *onopen);
  //void claim(Context *onclaim, msg_addr_t from);

  /* reset 
   *  NOTE: we assume the caller knows/has ensured that any objects 
   * in our sequence do not exist.. e.g. after a MKFS.  this is _not_
   * an "erase" method.
   */
  void create(ceph_file_layout *layout);
  void recover(Context *onfinish);
  void reread_head(Context *onfinish);
  void reprobe(Context *onfinish);
  void reread_head_and_probe(Context *onfinish);
  void write_head(Context *onsave=0);

  void set_layout(ceph_file_layout *l);

  void set_readonly();
  void set_writeable();
  bool is_readonly() { return readonly; }

  bool is_active() { return state == STATE_ACTIVE; }
  int get_error() { return error; }

  uint64_t get_write_pos() const { return write_pos; }
  uint64_t get_write_safe_pos() const { return safe_pos; }
  uint64_t get_read_pos() const { return read_pos; }
  uint64_t get_expire_pos() const { return expire_pos; }
  uint64_t get_trimmed_pos() const { return trimmed_pos; }

  uint64_t get_layout_period() const { return (uint64_t)layout.fl_stripe_count * (uint64_t)layout.fl_object_size; }
  ceph_file_layout& get_layout() { return layout; }

  // write
  uint64_t append_entry(bufferlist& bl);
  void wait_for_flush(Context *onsafe = 0);
  void flush(Context *onsafe = 0);

  // read
  void set_read_pos(int64_t p) { 
    assert(requested_pos == received_pos);  // we can't cope w/ in-progress read right now.
    read_pos = requested_pos = received_pos = p;
    read_buf.clear();
  }

  bool _is_readable();
  bool is_readable();
  bool try_read_entry(bufferlist& bl);
  void wait_for_readable(Context *onfinish);
  
  void set_write_pos(int64_t p) { 
    prezeroing_pos = prezero_pos = write_pos = flush_pos = safe_pos = p;
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
  void set_write_error_handler(Context *c) {
    assert(!on_write_error);
    on_write_error = c;
  }

  // trim
  void set_expire_pos(int64_t ep) { expire_pos = ep; }
  void set_trimmed_pos(int64_t p) { trimming_pos = trimmed_pos = p; }

  void trim();
  void trim_tail() {
    assert(!readonly);
    _issue_prezero();
  }
};
WRITE_CLASS_ENCODER(Journaler::Header)

#endif
