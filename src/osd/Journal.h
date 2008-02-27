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


#ifndef __EBOFS_JOURNAL_H
#define __EBOFS_JOURNAL_H

#include "include/buffer.h"
#include "include/Context.h"
#include "common/Finisher.h"

class Journal {
protected:
  __u64 fsid;
  Finisher *finisher;

public:
  Journal(__u64 f, Finisher *fin) : fsid(f), finisher(fin) { }
  virtual ~Journal() { }

  virtual int create() = 0;
  virtual int open(epoch_t epoch) = 0;
  virtual void close() = 0;

  // writes
  virtual void make_writeable() = 0;
  virtual void submit_entry(epoch_t epoch, bufferlist& e, Context *oncommit) = 0;
  virtual void commit_epoch_start(epoch_t) = 0;  // mark epoch boundary
  virtual void commit_epoch_finish(epoch_t) = 0; // mark prior epoch as committed (we can expire)
  virtual bool read_entry(bufferlist& bl, epoch_t &e) = 0;
  virtual bool is_full() = 0;

  // reads/recovery
  
};

#endif
