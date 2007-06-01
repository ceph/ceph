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


#ifndef __EBOFS_JOURNAL_H
#define __EBOFS_JOURNAL_H


class Journal {
  Ebofs *ebofs;

 public:
  Journal(Ebofs *e) : ebofs(e) { }
  virtual ~Journal() { }

  virtual void create() = 0;
  virtual void open() = 0;
  virtual void close() = 0;

  // writes
  virtual void submit_entry(bufferlist& e, Context *oncommit) = 0;// submit an item
  virtual void commit_epoch_start() = 0;  // mark epoch boundary
  virtual void commit_epoch_finish(list<Context*>& ls) = 0; // mark prior epoch as committed (we can expire)

  // reads/recovery
  
};

#endif
