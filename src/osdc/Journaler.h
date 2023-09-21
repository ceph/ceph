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

#include "common/Cond.h"
#include "include/fs_types.h"

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

struct Journaler
{
  // this goes at the head of the log "file".
  struct Header {
    uint64_t trimmed_pos;
    uint64_t expire_pos;
    uint64_t unused_field;
    uint64_t write_pos;
    std::string magic;
    file_layout_t layout; //< The mapping from byte stream offsets
                          //  to RADOS objects
    stream_format_t stream_format; //< The encoding of LogEvents
                                   //  within the journal byte stream

    Header(const char* m = "")
        : trimmed_pos(0)
        , expire_pos(0)
        , unused_field(0)
        , write_pos(0)
        , magic(m)
        , stream_format(-1)
    {
    }

    void encode(bufferlist& bl) const
    {
      ENCODE_START(2, 2, bl);
      encode(magic, bl);
      encode(trimmed_pos, bl);
      encode(expire_pos, bl);
      encode(unused_field, bl);
      encode(write_pos, bl);
      encode(layout, bl, 0); // encode in legacy format
      encode(stream_format, bl);
      ENCODE_FINISH(bl);
    }
    void decode(bufferlist::const_iterator& bl)
    {
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

    void dump(Formatter* f) const
    {
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

    static void generate_test_instances(std::list<Header*>& ls)
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

  virtual ~Journaler() {}
  /* reset
   *
   * NOTE: we assume the caller knows/has ensured that any objects in
   * our sequence do not exist.. e.g. after a MKFS.  this is _not_ an
   * "erase" method.
   */
  virtual void reset() = 0;
  virtual void create(file_layout_t const* layout, stream_format_t const sf) = 0;  
  virtual bool try_read_entry(bufferlist& bl) = 0;
  virtual uint64_t append_entry(bufferlist& bl) = 0;
  virtual void trim() = 0;
  virtual void trim_tail() = 0;
  /**
   * Cause any ongoing waits to error out with -EAGAIN, set error
   * to -EAGAIN.
   */
  virtual void shutdown() = 0;

  // Asynchronous operations
  // =======================
  virtual void erase(Context* completion) = 0;
  virtual void recover(Context* onfinish) = 0;
  virtual void reread_head(Context* onfinish) = 0;
  virtual void reread_head_and_probe(Context* onfinish) = 0;
  virtual void write_head(Context* onsave = 0) = 0;
  virtual void wait_for_flush(Context* onsafe = 0) = 0;
  virtual void flush(Context* onsafe = 0) = 0;
  virtual void wait_for_readable(Context* onfinish) = 0;
  virtual void wait_for_prezero(Context* onfinish) = 0;

  // Synchronous setters
  // ===================
  virtual void set_layout(file_layout_t const* l) = 0;
  virtual void set_readonly() = 0;
  virtual void set_writeable() = 0;
  virtual void set_write_pos(uint64_t p) = 0;
  virtual void set_read_pos(uint64_t p) = 0;
  virtual void set_expire_pos(uint64_t ep) = 0;
  virtual void set_trimmed_pos(uint64_t p) = 0;
  virtual void set_write_error_handler(Context* c) = 0;
  virtual void set_write_iohint(uint32_t iohint_flags) = 0;


  // Synchronous getters
  // ===================
  virtual bool have_waiter() const = 0;
  virtual uint64_t get_layout_period() const = 0;
  virtual const file_layout_t& get_layout() const = 0;
  virtual uint32_t get_stream_format() const = 0;
  virtual bool is_active() const = 0;
  virtual bool is_stopping() const = 0;
  virtual int get_error() const = 0;
  virtual bool is_readonly() const = 0;

  // not const because of prefetch and flushing
  // that may happen when this is called
  // TODO: this should be const
  //       this will require changes to RadosJournaler
  virtual bool is_readable() = 0;
  virtual bool is_write_head_needed() const = 0;
  virtual uint64_t get_write_pos() const = 0;
  virtual uint64_t get_write_safe_pos() const = 0;
  virtual uint64_t get_read_pos() const = 0;
  virtual uint64_t get_expire_pos() const = 0;
  virtual uint64_t get_trimmed_pos() const = 0;
  virtual size_t get_journal_envelope_size() const = 0;

  // common implementations
public:
  bool poll()
  {
    bool has_data = true;
    while (!is_readable() && !get_error() && get_read_pos() < get_write_pos()) {
      has_data = false;
      C_SaferCond readable_waiter;
      wait_for_readable(&readable_waiter);
      if (readable_waiter.wait()) {
        return false;
      }
      has_data = true;
    }
    return has_data;
  }
};

WRITE_CLASS_ENCODER(Journaler::Header)

#endif
