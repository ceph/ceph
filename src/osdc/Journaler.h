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
 * This class stripes a serial log over objects on the store.  Four
 * logical pointers:
 *
 *  write_pos - where we're writing new entries
 *  unused_field - where we're reading old entires
 *  expire_pos - what is deemed "old" by user
 *  trimmed_pos - where we're expiring old items
 *
 *  trimmed_pos <= expire_pos <= unused_field <= write_pos.
 *
 * Often, unused_field <= write_pos (as with MDS log).  During
 * recovery, write_pos is undefined until the end of the log is
 * discovered.
 *
 * A "head" struct at the beginning of the log is used to store
 * metadata at regular intervals.  The basic invariants include:
 *
 *   head.unused_field <= unused_field -- the head may "lag", since
 *                                        it's updated lazily.
 *   head.write_pos  <= write_pos
 *   head.expire_pos <= expire_pos
 *   head.trimmed_pos   <= trimmed_pos
 *
 * More significantly,
 *
 *   head.expire_pos >= trimmed_pos -- this ensures we can find the
 *                                     "beginning" of the log as last
 *                                     recorded, before it is trimmed.
 *                                     trimming will block until a
 *                                     sufficiently current expire_pos
 *                                     is committed.
 *
 * To recover log state, we simply start at the last write_pos in the
 * head, and probe the object sequence sizes until we read the end.
 *
 * Head struct is stored in the first object.  Actual journal starts
 * after layout.period() bytes.
 *
 */

#ifndef CEPH_JOURNALER_H
#define CEPH_JOURNALER_H

#include <list>
#include <map>

#include "Objecter.h"
#include "Filer.h"

#include "common/Timer.h"
#include "common/Throttle.h"
#include "include/common_fwd.h"

class Context;
class Finisher;
class C_OnFinisher;

typedef __u8 stream_format_t;

// Legacy envelope is leading uint32_t size
enum StreamFormat {
    JOURNAL_FORMAT_LEGACY = 0,
    JOURNAL_FORMAT_RESILIENT = 1,
    // Insert new formats here, before COUNT
    JOURNAL_FORMAT_COUNT
};

// Highest journal format version that we support
#define JOURNAL_FORMAT_MAX (JOURNAL_FORMAT_COUNT - 1)

// Legacy envelope is leading uint32_t size
#define JOURNAL_ENVELOPE_LEGACY (sizeof(uint32_t))

// Resilient envelope is leading uint64_t sentinel, uint32_t size,
// trailing uint64_t start_ptr
#define JOURNAL_ENVELOPE_RESILIENT (sizeof(uint32_t) + sizeof(uint64_t) + \
				    sizeof(uint64_t))

/**
 * Represents a collection of entries serialized in a byte stream.
 *
 * Each entry consists of:
 *  - a blob (used by the next level up as a serialized LogEvent)
 *  - a uint64_t (used by the next level up as a pointer to the start
 *    of the entry in the collection bytestream)
 */
class JournalStream
{
  stream_format_t format;

  public:
  JournalStream(stream_format_t format_) : format(format_) {}

  void set_format(stream_format_t format_) {format = format_;}

  bool readable(bufferlist &bl, uint64_t *need) const;
  size_t read(bufferlist &from, bufferlist *to, uint64_t *start_ptr);
  size_t write(bufferlist &entry, bufferlist *to, uint64_t const &start_ptr);
  size_t get_envelope_size() const {
     if (format >= JOURNAL_FORMAT_RESILIENT) {
       return JOURNAL_ENVELOPE_RESILIENT;
     } else {
       return JOURNAL_ENVELOPE_LEGACY;
     }
  }

  // A magic number for the start of journal entries, so that we can
  // identify them in damaged journals.
  static const uint64_t sentinel = 0x3141592653589793;
};


class Journaler {
public:
  // this goes at the head of the log "file".
  class Header {
    public:
    uint64_t trimmed_pos;
    uint64_t expire_pos;
    uint64_t unused_field;
    uint64_t write_pos;
    std::string magic;
    file_layout_t layout; //< The mapping from byte stream offsets
			     //  to RADOS objects
    stream_format_t stream_format; //< The encoding of LogEvents
				   //  within the journal byte stream

    Header(const char *m="") :
      trimmed_pos(0), expire_pos(0), unused_field(0), write_pos(0), magic(m),
      stream_format(-1) {
    }

    void encode(bufferlist &bl) const {
      ENCODE_START(2, 2, bl);
      encode(magic, bl);
      encode(trimmed_pos, bl);
      encode(expire_pos, bl);
      encode(unused_field, bl);
      encode(write_pos, bl);
      encode(layout, bl, 0);  // encode in legacy format
      encode(stream_format, bl);
      ENCODE_FINISH(bl);
    }
    void decode(bufferlist::const_iterator &bl) {
      DECODE_START_LEGACY_COMPAT_LEN(2, 2, 2, bl);
      decode(magic, bl);
      decode(trimmed_pos, bl);
      decode(expire_pos, bl);
      decode(unused_field, bl);
      decode(write_pos, bl);
      decode(layout, bl);
      if (struct_v > 1) {
	decode(stream_format, bl);
      } else {
	stream_format = JOURNAL_FORMAT_LEGACY;
      }
      DECODE_FINISH(bl);
    }

    void dump(Formatter *f) const {
      f->open_object_section("journal_header");
      {
	f->dump_string("magic", magic);
	f->dump_unsigned("write_pos", write_pos);
	f->dump_unsigned("expire_pos", expire_pos);
	f->dump_unsigned("trimmed_pos", trimmed_pos);
	f->dump_unsigned("stream_format", stream_format);
	f->dump_object("layout", layout);
      }
      f->close_section(); // journal_header
    }

    static void generate_test_instances(std::list<Header*> &ls)
    {
      ls.push_back(new Header());

      ls.push_back(new Header());
      ls.back()->trimmed_pos = 1;
      ls.back()->expire_pos = 2;
      ls.back()->unused_field = 3;
      ls.back()->write_pos = 4;
      ls.back()->magic = "magique";

      ls.push_back(new Header());
      ls.back()->stream_format = JOURNAL_FORMAT_RESILIENT;
    }
  };
  WRITE_CLASS_ENCODER(Header)

  uint32_t get_stream_format() const {
    return stream_format;
  }

  Header last_committed;

private:
  // me
  CephContext *cct;
  std::mutex lock;
  const std::string name;
  typedef std::lock_guard<std::mutex> lock_guard;
  typedef std::unique_lock<std::mutex> unique_lock;
  Finisher *finisher;
  Header last_written;
  inodeno_t ino;
  int64_t pg_pool;
  bool readonly;
  file_layout_t layout;
  uint32_t stream_format;
  JournalStream journal_stream;

  const char *magic;
  Objecter *objecter;
  Filer filer;

  PerfCounters *logger;
  int logger_key_lat;

  class C_DelayFlush;
  C_DelayFlush *delay_flush_event;
  /*
   * Do a flush as a result of a C_DelayFlush context.
   */
  void _do_delayed_flush()
  {
    ceph_assert(delay_flush_event != NULL);
    lock_guard l(lock);
    delay_flush_event = NULL;
    _do_flush();
  }

  // my state
  static const int STATE_UNDEF = 0;
  static const int STATE_READHEAD = 1;
  static const int STATE_PROBING = 2;
  static const int STATE_ACTIVE = 3;
  static const int STATE_REREADHEAD = 4;
  static const int STATE_REPROBING = 5;
  static const int STATE_STOPPING = 6;

  int state;
  int error;

  void _write_head(Context *oncommit=NULL);
  void _wait_for_flush(Context *onsafe);
  void _trim();

  // header
  ceph::real_time last_wrote_head;
  void _finish_write_head(int r, Header &wrote, C_OnFinisher *oncommit);
  class C_WriteHead;
  friend class C_WriteHead;

  void _reread_head(Context *onfinish);
  void _set_layout(file_layout_t const *l);
  std::list<Context*> waitfor_recover;
  void _read_head(Context *on_finish, bufferlist *bl);
  void _finish_read_head(int r, bufferlist& bl);
  void _finish_reread_head(int r, bufferlist& bl, Context *finish);
  void _probe(Context *finish, uint64_t *end);
  void _finish_probe_end(int r, uint64_t end);
  void _reprobe(C_OnFinisher *onfinish);
  void _finish_reprobe(int r, uint64_t end, C_OnFinisher *onfinish);
  void _finish_reread_head_and_probe(int r, C_OnFinisher *onfinish);
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
  uint64_t prezero_pos; ///< we zero journal space ahead of write_pos to
			//   avoid problems with tail probing
  uint64_t write_pos; ///< logical write position, where next entry
		      //   will go
  uint64_t flush_pos; ///< where we will flush. if
		      ///  write_pos>flush_pos, we're buffering writes.
  uint64_t safe_pos; ///< what has been committed safely to disk.

  uint64_t next_safe_pos; /// start position of the first entry that isn't
			  /// being fully flushed. If we don't flush any
			  // partial entry, it's equal to flush_pos.

  bufferlist write_buf; ///< write buffer.  flush_pos +
			///  write_buf.length() == write_pos.

  // protect write_buf from bufferlist _len overflow 
  Throttle write_buf_throttle;

  uint64_t waiting_for_zero_pos;
  interval_set<uint64_t> pending_zero;  // non-contig bits we've zeroed
  std::list<Context*> waitfor_prezero;

  std::map<uint64_t, uint64_t> pending_safe; // flush_pos -> safe_pos
  // when safe through given offset
  std::map<uint64_t, std::list<Context*> > waitfor_safe;

  void _flush(C_OnFinisher *onsafe);
  void _do_flush(unsigned amount=0);
  void _finish_flush(int r, uint64_t start, ceph::real_time stamp);
  class C_Flush;
  friend class C_Flush;

  // reader
  uint64_t read_pos;      // logical read position, where next entry starts.
  uint64_t requested_pos; // what we've requested from OSD.
  uint64_t received_pos;  // what we've received from OSD.
  // read buffer.  unused_field + read_buf.length() == prefetch_pos.
  bufferlist read_buf;

  std::map<uint64_t,bufferlist> prefetch_buf;

  uint64_t fetch_len;     // how much to read at a time
  uint64_t temp_fetch_len;

  // for wait_for_readable()
  C_OnFinisher *on_readable;
  C_OnFinisher *on_write_error;
  bool called_write_error;

  // read completion callback
  void _finish_read(int r, uint64_t offset, uint64_t length, bufferlist &bl);
  void _finish_retry_read(int r);
  void _assimilate_prefetch();
  void _issue_read(uint64_t len); // read some more
  void _prefetch(); // maybe read ahead
  class C_Read;
  friend class C_Read;
  class C_RetryRead;
  friend class C_RetryRead;

  // trimmer
  uint64_t expire_pos;    // what we're allowed to trim to
  uint64_t trimming_pos;      // what we've requested to trim through
  uint64_t trimmed_pos;   // what has been trimmed

  bool readable;

  void _finish_trim(int r, uint64_t to);
  class C_Trim;
  friend class C_Trim;

  void _issue_prezero();
  void _finish_prezero(int r, uint64_t from, uint64_t len);
  friend struct C_Journaler_Prezero;

  // only init_headers when following or first reading off-disk
  void init_headers(Header& h) {
    ceph_assert(readonly ||
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

  bool _have_next_entry();

  void _finish_erase(int data_result, C_OnFinisher *completion);
  class C_EraseFinish;
  friend class C_EraseFinish;

  C_OnFinisher *wrap_finisher(Context *c);

  uint32_t write_iohint; // the fadvise flags for write op, see
			 // CEPH_OSD_OP_FADIVSE_*

public:
  Journaler(const std::string &name_, inodeno_t ino_, int64_t pool,
      const char *mag, Objecter *obj, PerfCounters *l, int lkey, Finisher *f) :
    last_committed(mag),
    cct(obj->cct), name(name_), finisher(f), last_written(mag),
    ino(ino_), pg_pool(pool), readonly(true),
    stream_format(-1), journal_stream(-1),
    magic(mag),
    objecter(obj), filer(objecter, f), logger(l), logger_key_lat(lkey),
    delay_flush_event(0),
    state(STATE_UNDEF), error(0),
    prezeroing_pos(0), prezero_pos(0), write_pos(0), flush_pos(0),
    safe_pos(0), next_safe_pos(0),
    write_buf_throttle(cct, "write_buf_throttle", UINT_MAX - (UINT_MAX >> 3)),
    waiting_for_zero_pos(0),
    read_pos(0), requested_pos(0), received_pos(0),
    fetch_len(0), temp_fetch_len(0),
    on_readable(0), on_write_error(NULL), called_write_error(false),
    expire_pos(0), trimming_pos(0), trimmed_pos(0), readable(false),
    write_iohint(0)
  {
  }

  /* reset
   *
   * NOTE: we assume the caller knows/has ensured that any objects in
   * our sequence do not exist.. e.g. after a MKFS.  this is _not_ an
   * "erase" method.
   */
  void reset() {
    lock_guard l(lock);
    ceph_assert(state == STATE_ACTIVE);

    readonly = true;
    delay_flush_event = NULL;
    state = STATE_UNDEF;
    error = 0;
    prezeroing_pos = 0;
    prezero_pos = 0;
    write_pos = 0;
    flush_pos = 0;
    safe_pos = 0;
    next_safe_pos = 0;
    read_pos = 0;
    requested_pos = 0;
    received_pos = 0;
    fetch_len = 0;
    ceph_assert(!on_readable);
    expire_pos = 0;
    trimming_pos = 0;
    trimmed_pos = 0;
    waiting_for_zero_pos = 0;
  }

  // Asynchronous operations
  // =======================
  void erase(Context *completion);
  void create(file_layout_t *layout, stream_format_t const sf);
  void recover(Context *onfinish);
  void reread_head(Context *onfinish);
  void reread_head_and_probe(Context *onfinish);
  void write_head(Context *onsave=0);
  void wait_for_flush(Context *onsafe = 0);
  void flush(Context *onsafe = 0);
  void wait_for_readable(Context *onfinish);
  void _wait_for_readable(Context *onfinish);
  bool have_waiter() const;
  void wait_for_prezero(Context *onfinish);

  // Synchronous setters
  // ===================
  void set_layout(file_layout_t const *l);
  void set_readonly();
  void set_writeable();
  void set_write_pos(uint64_t p) {
    lock_guard l(lock);
    prezeroing_pos = prezero_pos = write_pos = flush_pos = safe_pos = next_safe_pos = p;
  }
  void set_read_pos(uint64_t p) {
    lock_guard l(lock);
    // we can't cope w/ in-progress read right now.
    ceph_assert(requested_pos == received_pos);
    read_pos = requested_pos = received_pos = p;
    read_buf.clear();
  }
  uint64_t append_entry(bufferlist& bl);
  void set_expire_pos(uint64_t ep) {
      lock_guard l(lock);
      expire_pos = ep;
  }
  void set_trimmed_pos(uint64_t p) {
      lock_guard l(lock);
      trimming_pos = trimmed_pos = p;
  }

  bool _write_head_needed();
  bool write_head_needed() {
    lock_guard l(lock);
    return _write_head_needed();
  }


  void trim();
  void trim_tail() {
    lock_guard l(lock);

    ceph_assert(!readonly);
    _issue_prezero();
  }

  void set_write_error_handler(Context *c);

  void set_write_iohint(uint32_t iohint_flags) {
    write_iohint = iohint_flags;
  }
  /**
   * Cause any ongoing waits to error out with -EAGAIN, set error
   * to -EAGAIN.
   */
  void shutdown();
public:

  // Synchronous getters
  // ===================
  // TODO: need some locks on reads for true safety
  uint64_t get_layout_period() const {
    return layout.get_period();
  }
  file_layout_t& get_layout() { return layout; }
  bool is_active() { return state == STATE_ACTIVE; }
  bool is_stopping() { return state == STATE_STOPPING; }
  int get_error() { return error; }
  bool is_readonly() { return readonly; }
  bool is_readable();
  bool _is_readable();
  bool try_read_entry(bufferlist& bl);
  uint64_t get_write_pos() const { return write_pos; }
  uint64_t get_write_safe_pos() const { return safe_pos; }
  uint64_t get_read_pos() const { return read_pos; }
  uint64_t get_expire_pos() const { return expire_pos; }
  uint64_t get_trimmed_pos() const { return trimmed_pos; }
  size_t get_journal_envelope_size() const { 
    return journal_stream.get_envelope_size(); 
  }
  void check_isreadable();
};
WRITE_CLASS_ENCODER(Journaler::Header)

#endif
