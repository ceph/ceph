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

#ifndef __JOURNALINGOBJECTSTORE_H
#define __JOURNALINGOBJECTSTORE_H

#include "ObjectStore.h"
#include "Journal.h"

class JournalingObjectStore : public ObjectStore {
protected:
  epoch_t super_epoch;
  Journal *journal;
  Finisher finisher;
  map<version_t, list<Context*> > commit_waiters;

  void journal_start() {
    finisher.start();
  }
  void journal_stop() {
    finisher.stop();
  }
  int journal_replay();

  void commit_start() {
    super_epoch++;
    if (journal)
      journal->commit_epoch_start(super_epoch);
  }
  void commit_finish() {
    finisher.queue(commit_waiters[super_epoch-1]);
    if (journal)
      journal->commit_epoch_finish(super_epoch);
  }

  void queue_commit_waiter(Context *oncommit) {
    if (oncommit) 
      commit_waiters[super_epoch].push_back(oncommit);
  }

  void journal_write(pobject_t oid, off_t off, size_t len, const bufferlist& bl, Context *onsafe) {
    if (journal && journal->is_writeable()) {
      Transaction t;
      t.write(oid, off, len, bl);
      bufferlist tbl;
      t.encode(tbl);
      journal->submit_entry(super_epoch, tbl, onsafe);
    } else
      queue_commit_waiter(onsafe);
  }
  
  void journal_zero(pobject_t oid, off_t off, size_t len, Context *onsafe) {
    if (journal && journal->is_writeable()) {
      Transaction t;
      t.zero(oid, off, len);
      bufferlist tbl;
      t.encode(tbl);
      journal->submit_entry(super_epoch, tbl, onsafe);
    } else
      queue_commit_waiter(onsafe);
  }
  
  void journal_remove(pobject_t oid, Context *onsafe) {
    if (journal && journal->is_writeable()) {
      Transaction t;
      t.remove(oid);
      bufferlist bl;
      t.encode(bl);
      journal->submit_entry(super_epoch, bl, onsafe);
    } else
      queue_commit_waiter(onsafe);
  }

  void journal_truncate(pobject_t oid, off_t size, Context *onsafe) {
    if (journal && journal->is_writeable()) {
      Transaction t;
      t.truncate(oid, size);
      bufferlist bl;
      t.encode(bl);
      journal->submit_entry(super_epoch, bl, onsafe);
    } else
      queue_commit_waiter(onsafe);
  }

  void journal_clone(pobject_t from, pobject_t to, Context *onsafe) {
    if (journal && journal->is_writeable()) {
      Transaction t;
      t.clone(from, to);
      bufferlist bl;
      t.encode(bl);
      journal->submit_entry(super_epoch, bl, onsafe);
    } else
      queue_commit_waiter(onsafe);
  }

  void journal_setattr(pobject_t oid, const char *name, const void *value, size_t size, Context *onsafe) {
    if (journal && journal->is_writeable()) {
      Transaction t;
      t.setattr(oid, name, value, size);
      bufferlist bl;
      t.encode(bl);
      journal->submit_entry(super_epoch, bl, onsafe);
    } else
      queue_commit_waiter(onsafe);
  }

  void journal_setattrs(pobject_t oid, map<string,bufferptr>& attrset, Context *onsafe) {
    if (journal && journal->is_writeable()) {
      Transaction t;
      t.setattrs(oid, attrset);
      bufferlist bl;
      t.encode(bl);
      journal->submit_entry(super_epoch, bl, onsafe);
    } else
      queue_commit_waiter(onsafe);
  }

  void journal_rmattr(pobject_t oid, const char *name, Context *onsafe) {
    if (journal && journal->is_writeable()) {
      Transaction t;
      t.rmattr(oid, name);
      bufferlist bl;
      t.encode(bl);
      journal->submit_entry(super_epoch, bl, onsafe);
    } else
      queue_commit_waiter(onsafe);
  }

  void journal_create_collection(coll_t cid, Context *onsafe) {
    if (journal && journal->is_writeable()) {
      Transaction t;
      t.create_collection(cid);
      bufferlist bl;
      t.encode(bl);
      journal->submit_entry(super_epoch, bl, onsafe);
    } else
      queue_commit_waiter(onsafe);
  }

  void journal_destroy_collection(coll_t cid, Context *onsafe) {
    if (journal && journal->is_writeable()) {
      Transaction t;
      t.remove_collection(cid);
      bufferlist bl;
      t.encode(bl);
      journal->submit_entry(super_epoch, bl, onsafe);
    } else
      queue_commit_waiter(onsafe);
  }
  
  void journal_collection_add(coll_t cid, pobject_t oid, Context *onsafe) {
    if (journal && journal->is_writeable()) {
      Transaction t;
      t.collection_add(cid, oid);
      bufferlist bl;
      t.encode(bl);
      journal->submit_entry(super_epoch, bl, onsafe);
    } else
      queue_commit_waiter(onsafe);
  }

  void journal_collection_remove(coll_t cid, pobject_t oid, Context *onsafe) {
    if (journal && journal->is_writeable()) {
      Transaction t;
      t.collection_remove(cid, oid);
      bufferlist bl;
      t.encode(bl);
      journal->submit_entry(super_epoch, bl, onsafe);
    } else
      queue_commit_waiter(onsafe);
  }

  void journal_collection_setattr(coll_t cid, const char *name, const void *value, size_t size, Context *onsafe) {
    if (journal && journal->is_writeable()) {
      Transaction t;
      t.collection_setattr(cid, name, value, size);
      bufferlist bl;
      t.encode(bl);
      journal->submit_entry(super_epoch, bl, onsafe);
    } else
      queue_commit_waiter(onsafe);
  }

  void journal_collection_setattrs(coll_t cid, map<string,bufferptr>& aset, Context *onsafe) {
    if (journal && journal->is_writeable()) {
      Transaction t;
      t.collection_setattrs(cid, aset);
      bufferlist bl;
      t.encode(bl);
      journal->submit_entry(super_epoch, bl, onsafe);
    } else
      queue_commit_waiter(onsafe);
  }

  void journal_sync(Context *onsafe) {
    if (journal) {  
      // journal empty transaction
      Transaction t;
      bufferlist bl;
      t.encode(bl);
      journal->submit_entry(super_epoch, bl, onsafe);
    } else
      queue_commit_waiter(onsafe);
  }

public:
  JournalingObjectStore() : super_epoch(0), journal(0) { }
  
};

#endif
