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
  __u64 committing_op_seq, committed_op_seq;
  Journal *journal;
  Finisher finisher;
  map<version_t, vector<Context*> > commit_waiters;
  RWLock op_lock;
  Mutex journal_lock;
  Mutex lock;

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

  bool commit_start() {
    // suspend new ops...
    op_lock.get_write();
    Mutex::Locker l(lock);
    if (op_seq == committed_op_seq) {
      op_lock.put_write();
      assert(commit_waiters.empty());
      return false;
    }
    return true;
  }
  void commit_started() {
    Mutex::Locker l(lock);

    // allow new ops
    // (underlying fs should now be committing all prior ops)
    committing_op_seq = op_seq;
    op_lock.put_write();
  }
  void commit_finish() {
    Mutex::Locker l(lock);

    if (journal)
      journal->committed_thru(committing_op_seq);
    committed_op_seq = committing_op_seq;

    map<version_t, vector<Context*> >::iterator p = commit_waiters.begin();
    while (p != commit_waiters.end() &&
	   p->first <= committing_op_seq) {
      finisher.queue(p->second);
      commit_waiters.erase(p++);
    }
  }

  void journal_transaction(ObjectStore::Transaction& t, Context *onjournal, Context *ondisk) {
    Mutex::Locker l(lock);

    ++op_seq;

    if (journal && journal->is_writeable()) {
      bufferlist tbl;
      t.encode(tbl);
      journal->submit_entry(op_seq, tbl, onjournal);
    } else if (onjournal)
      commit_waiters[op_seq].push_back(onjournal);

    if (ondisk)
      commit_waiters[op_seq].push_back(ondisk);
  }
  void journal_transactions(list<ObjectStore::Transaction*>& tls, Context *onjournal, Context *ondisk) {
    Mutex::Locker l(lock);

    ++op_seq;

    if (journal && journal->is_writeable()) {
      bufferlist tbl;
      for (list<ObjectStore::Transaction*>::iterator p = tls.begin(); p != tls.end(); p++)
	(*p)->encode(tbl);
      journal->submit_entry(op_seq, tbl, onjournal);
    } else if (onjournal)
      commit_waiters[op_seq].push_back(onjournal);

    if (ondisk)
      commit_waiters[op_seq].push_back(ondisk);
  }

public:
  JournalingObjectStore() : op_seq(0), committing_op_seq(0), committed_op_seq(0), 
			    journal(0),
			    op_lock("JournalingObjectStore::op_lock"),
			    journal_lock("JournalingObjectStore::journal_lock"),
			    lock("JournalingObjectStore::lock") { }
  
};

#endif
