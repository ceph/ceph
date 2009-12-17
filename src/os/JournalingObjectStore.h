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
  __u64 op_seq, applied_seq;
  __u64 committing_seq, committed_seq;
  map<version_t, vector<Context*> > commit_waiters;

  int open_ops;
  bool blocked;

  Journal *journal;
  Finisher finisher;

  Cond cond;
  Mutex journal_lock;
  Mutex lock;

protected:
  void journal_start();
  void journal_stop();
  int journal_replay(__u64 fs_op_seq);

  // --
  __u64 op_apply_start(__u64 op, Context *ondisk);
  void op_apply_finish();
  __u64 op_journal_start(__u64 op);
  void op_journal_finish();

  void journal_transaction(ObjectStore::Transaction& t, __u64 op, Context *onjournal);
  void journal_transactions(list<ObjectStore::Transaction*>& tls, __u64 op, Context *onjournal);

  bool commit_start();
  void commit_started();  // allow new ops (underlying fs should now be committing all prior ops)
  void commit_finish();


public:
  JournalingObjectStore() : op_seq(0), 
			    applied_seq(0), committing_seq(0), committed_seq(0), 
			    open_ops(0), blocked(false),
			    journal(NULL),
			    journal_lock("JournalingObjectStore::journal_lock"),
			    lock("JournalingObjectStore::lock") { }
  
};

#endif
