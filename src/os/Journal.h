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


#ifndef CEPH_EBOFS_JOURNAL_H
#define CEPH_EBOFS_JOURNAL_H

#include "include/buffer.h"
#include "include/Context.h"
#include "common/Finisher.h"

class Journal {
protected:
  uint64_t fsid;
  Finisher *finisher;
  Cond *do_sync_cond;
  bool wait_on_full;

public:
  Journal(uint64_t f, Finisher *fin, Cond *c=0) : fsid(f), finisher(fin),
					       do_sync_cond(c),
					       wait_on_full(false) { }
  virtual ~Journal() { }

  virtual int create() = 0;
  virtual int open(uint64_t last_seq) = 0;
  virtual void close() = 0;

  virtual void flush() = 0;
  virtual void throttle() = 0;

  void set_wait_on_full(bool b) { wait_on_full = b; }

  // writes
  virtual bool is_writeable() = 0;
  virtual void make_writeable() = 0;
  virtual void submit_entry(uint64_t seq, bufferlist& e, unsigned alignment, Context *oncommit) = 0;
  virtual void committed_thru(uint64_t seq) = 0;
  virtual bool read_entry(bufferlist& bl, uint64_t &seq) = 0;

  // reads/recovery
  
};

#endif
