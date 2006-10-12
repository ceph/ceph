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

#ifndef __LOGSTREAMER_H
#define __LOGSTREAMER_H

#include "Objecter.h"
#include "Filer.h"

#include <list>
#include <map>

class Context;
class Logger;

class LogStreamer {
  // me
  inode_t inode;
  Objecter *objecter;
  Filer filer;

  Logger *logger;

  // writer
  off_t write_pos;       // logical write position, where next entry will go
  off_t flush_pos;       // where we will flush. if write_pos>flush_pos, we're buffering writes.
  off_t ack_pos;         // what has been acked.
  bufferlist write_buf;  // write buffer.  flush_pos + write_buf.length() == write_pos.

  std::map<off_t, utime_t> pending_flush;  // start offsets and times for pending flushes
  std::map<off_t, std::list<Context*> > waitfor_flush; // when flushed through given offset

  void _finish_flush(int r, off_t start);
  class C_Flush;
  friend class C_Flush;

  // reader
  off_t read_pos;      // logical read position, where next entry starts.
  off_t requested_pos; // what we've requested from OSD.
  off_t received_pos;  // what we've received from OSD.
  bufferlist read_buf; // read buffer.  read_pos + read_buf.length() == prefetch_pos.
  bufferlist reading_buf; // what i'm reading into

  off_t fetch_len;     // how much to read at a time
  off_t prefetch_from; // how far from end do we read next chunk

  // for read_entry() in-progress read
  bufferlist *read_bl;
  Context    *on_read_finish;
  // for wait_for_readable()
  Context    *on_readable;

  bool _is_reading() {
    return requested_pos > received_pos;
  }
  void _finish_read(int r);     // we just read some (read completion callback)
  void _issue_read(off_t len);  // read some more
  void _prefetch();             // maybe read ahead

  class C_Read;
  friend class C_Read;
  class C_RetryRead;
  friend class C_RetryRead;

public:
  LogStreamer(inode_t& inode_, Objecter *obj, Logger *l, off_t fl=0, off_t pff=0) : 
    inode(inode_), objecter(obj), filer(objecter), logger(l),
    write_pos(0), flush_pos(0), ack_pos(0),
    read_pos(0), requested_pos(0), received_pos(0),
    fetch_len(fl), prefetch_from(pff),
    read_bl(0), on_read_finish(0), on_readable(0) {
    // prefetch intelligently.
    // (watch out, this is big if you use big objects or weird striping)
    if (!fetch_len)
      fetch_len = inode.layout.object_size*inode.layout.stripe_count;      
    if (!prefetch_from)
      prefetch_from = fetch_len / 2;
  }

  // me
  void open(Context *onopen);
  void claim(Context *onclaim, msg_addr_t from);
  void save(Context *onsave);

  off_t get_write_pos() { return write_pos; }
  off_t get_read_pos() { return read_pos; }

  // write
  off_t append_entry(bufferlist& bl, Context *onsync = 0);
  void flush(Context *onsync = 0);

  // read
  bool is_readable();
  bool try_read_entry(bufferlist& bl);
  void wait_for_readable(Context *onfinish);
  void read_entry(bufferlist* bl, Context *onfinish);
  
};


#endif
