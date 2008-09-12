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
#include "common/RWLock.h"

class JournalingObjectStore : public ObjectStore {
protected:
  __u64 op_seq;
  __u64 committing_op_seq;
  Journal *journal;
  Finisher finisher;
  map<version_t, vector<Context*> > commit_waiters;
  RWLock op_lock;
  Mutex journal_lock;

  void journal_start() {
    finisher.start();
  }
  void journal_stop() {
    finisher.stop();
    if (journal) {
      journal->close();
      delete journal;
      journal = 0;
    }
  }
  int journal_replay();

  void op_start() {
    op_lock.get_read();
  }
  void op_journal_start() {
    journal_lock.Lock();
  }
  void op_finish() {
    journal_lock.Unlock();
    op_lock.put_read();    
  }

  void commit_start() {
    // suspend new ops...
    op_lock.get_write();
  }
  void commit_started() {
    // allow new ops
    // (underlying fs should now be committing all prior ops)
    committing_op_seq = op_seq;
    op_lock.put_write();
  }
  void commit_finish() {
    if (journal)
      journal->committed_thru(committing_op_seq);
    finisher.queue(commit_waiters[committing_op_seq]);
  }

  void queue_commit_waiter(Context *oncommit) {
    if (oncommit) 
      commit_waiters[op_seq].push_back(oncommit);
  }

  void journal_transaction(Transaction &t, Context *onsafe) {
    ++op_seq;
    if (journal && journal->is_writeable()) {
      bufferlist tbl;
      t.encode(tbl);
      journal->submit_entry(op_seq, tbl, onsafe);
    } else
      queue_commit_waiter(onsafe);
  }
  void journal_transaction(bufferlist& tbl, Context *onsafe) {
    ++op_seq;
    if (journal && journal->is_writeable()) {
      journal->submit_entry(op_seq, tbl, onsafe);
    } else
      queue_commit_waiter(onsafe);
  }

  void journal_write(coll_t cid, pobject_t oid, loff_t off, size_t len, const bufferlist& bl, Context *onsafe) {
    ++op_seq;
    if (journal && journal->is_writeable()) {
      Transaction t;
      t.write(cid, oid, off, len, bl);
      bufferlist tbl;
      t.encode(tbl);
      journal->submit_entry(op_seq, tbl, onsafe);
    } else
      queue_commit_waiter(onsafe);
  }
  
  void journal_zero(coll_t cid, pobject_t oid, loff_t off, size_t len, Context *onsafe) {
    ++op_seq;
    if (journal && journal->is_writeable()) {
      Transaction t;
      t.zero(cid, oid, off, len);
      bufferlist tbl;
      t.encode(tbl);
      journal->submit_entry(op_seq, tbl, onsafe);
    } else
      queue_commit_waiter(onsafe);
  }
  
  void journal_remove(coll_t cid, pobject_t oid, Context *onsafe) {
    ++op_seq;
    if (journal && journal->is_writeable()) {
      Transaction t;
      t.remove(cid, oid);
      bufferlist bl;
      t.encode(bl);
      journal->submit_entry(op_seq, bl, onsafe);
    } else
      queue_commit_waiter(onsafe);
  }

  void journal_truncate(coll_t cid, pobject_t oid, loff_t size, Context *onsafe) {
    ++op_seq;
    if (journal && journal->is_writeable()) {
      Transaction t;
      t.truncate(cid, oid, size);
      bufferlist bl;
      t.encode(bl);
      journal->submit_entry(op_seq, bl, onsafe);
    } else
      queue_commit_waiter(onsafe);
  }

  void journal_clone(coll_t cid, pobject_t from, pobject_t to, Context *onsafe) {
    ++op_seq;
    if (journal && journal->is_writeable()) {
      Transaction t;
      t.clone(cid, from, to);
      bufferlist bl;
      t.encode(bl);
      journal->submit_entry(op_seq, bl, onsafe);
    } else
      queue_commit_waiter(onsafe);
  }

  void journal_setattr(coll_t cid, pobject_t oid, const char *name, const void *value, size_t size, Context *onsafe) {
    ++op_seq;
    if (journal && journal->is_writeable()) {
      Transaction t;
      t.setattr(cid, oid, name, value, size);
      bufferlist bl;
      t.encode(bl);
      journal->submit_entry(op_seq, bl, onsafe);
    } else
      queue_commit_waiter(onsafe);
  }

  void journal_setattrs(coll_t cid, pobject_t oid, map<string,bufferptr>& attrset, Context *onsafe) {
    ++op_seq;
    if (journal && journal->is_writeable()) {
      Transaction t;
      t.setattrs(cid, oid, attrset);
      bufferlist bl;
      t.encode(bl);
      journal->submit_entry(op_seq, bl, onsafe);
    } else
      queue_commit_waiter(onsafe);
  }

  void journal_rmattr(coll_t cid, pobject_t oid, const char *name, Context *onsafe) {
    ++op_seq;
    if (journal && journal->is_writeable()) {
      Transaction t;
      t.rmattr(cid, oid, name);
      bufferlist bl;
      t.encode(bl);
      journal->submit_entry(op_seq, bl, onsafe);
    } else
      queue_commit_waiter(onsafe);
  }

  void journal_create_collection(coll_t cid, Context *onsafe) {
    ++op_seq;
    if (journal && journal->is_writeable()) {
      Transaction t;
      t.create_collection(cid);
      bufferlist bl;
      t.encode(bl);
      journal->submit_entry(op_seq, bl, onsafe);
    } else
      queue_commit_waiter(onsafe);
  }

  void journal_destroy_collection(coll_t cid, Context *onsafe) {
    ++op_seq;
    if (journal && journal->is_writeable()) {
      Transaction t;
      t.remove_collection(cid);
      bufferlist bl;
      t.encode(bl);
      journal->submit_entry(op_seq, bl, onsafe);
    } else
      queue_commit_waiter(onsafe);
  }
  
  void journal_collection_add(coll_t cid, coll_t ocid, pobject_t oid, Context *onsafe) {
    ++op_seq;
    if (journal && journal->is_writeable()) {
      Transaction t;
      t.collection_add(cid, ocid, oid);
      bufferlist bl;
      t.encode(bl);
      journal->submit_entry(op_seq, bl, onsafe);
    } else
      queue_commit_waiter(onsafe);
  }

  void journal_collection_remove(coll_t cid, pobject_t oid, Context *onsafe) {
    ++op_seq;
    if (journal && journal->is_writeable()) {
      Transaction t;
      t.collection_remove(cid, oid);
      bufferlist bl;
      t.encode(bl);
      journal->submit_entry(op_seq, bl, onsafe);
    } else
      queue_commit_waiter(onsafe);
  }

  void journal_collection_setattr(coll_t cid, const char *name, const void *value, size_t size, Context *onsafe) {
    ++op_seq;
    if (journal && journal->is_writeable()) {
      Transaction t;
      t.collection_setattr(cid, name, value, size);
      bufferlist bl;
      t.encode(bl);
      journal->submit_entry(op_seq, bl, onsafe);
    } else
      queue_commit_waiter(onsafe);
  }
  
  void journal_collection_setattrs(coll_t cid, map<string,bufferptr>& aset, Context *onsafe) {
    ++op_seq;
    if (journal && journal->is_writeable()) {
      Transaction t;
      t.collection_setattrs(cid, aset);
      bufferlist bl;
      t.encode(bl);
      journal->submit_entry(op_seq, bl, onsafe);
    } else
      queue_commit_waiter(onsafe);
  }
  
  void journal_sync(Context *onsafe) {
    ++op_seq;
    if (journal) {  
      // journal empty transaction
      Transaction t;
      bufferlist bl;
      t.encode(bl);
      journal->submit_entry(op_seq, bl, onsafe);
    } else
      queue_commit_waiter(onsafe);
  }

public:
  JournalingObjectStore() : op_seq(0), committing_op_seq(0), journal(0) { }
  
};

#endif
