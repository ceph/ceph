// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-

#include "JournalingObjectStore.h"

#include "common/errno.h"
#include "common/debug.h"

#define dout_subsys ceph_subsys_journal
#undef dout_prefix
#define dout_prefix *_dout << "journal "



void JournalingObjectStore::journal_start()
{
  dout(10) << "journal_start" << dendl;
  finisher.start();
}
 
void JournalingObjectStore::journal_stop() 
{
  dout(10) << "journal_stop" << dendl;
  finisher.stop();
}

// A journal_replay() makes journal writeable, this closes that out.
void JournalingObjectStore::journal_write_close()
{
  if (journal) {
    journal->close();
    delete journal;
    journal = 0;
  }
  apply_manager.reset();
}

int JournalingObjectStore::journal_replay(uint64_t fs_op_seq)
{
  dout(10) << "journal_replay fs op_seq " << fs_op_seq << dendl;

  if (g_conf->journal_replay_from) {
    dout(0) << "journal_replay forcing replay from " << g_conf->journal_replay_from
	    << " instead of " << fs_op_seq << dendl;
    // the previous op is the last one committed
    fs_op_seq = g_conf->journal_replay_from - 1;
  }

  uint64_t op_seq = fs_op_seq;
  apply_manager.init_seq(fs_op_seq);

  if (!journal)
    return 0;

  int err = journal->open(op_seq);
  if (err < 0) {
    dout(3) << "journal_replay open failed with " 
	    << cpp_strerror(err) << dendl;
    delete journal;
    journal = 0;
    return err;
  }

  replaying = true;

  int count = 0;
  while (1) {
    bufferlist bl;
    uint64_t seq = op_seq + 1;
    if (!journal->read_entry(bl, seq)) {
      dout(3) << "journal_replay: end of journal, done." << dendl;
      break;
    }

    if (seq <= op_seq) {
      dout(3) << "journal_replay: skipping old op seq " << seq << " <= " << op_seq << dendl;
      continue;
    }
    assert(op_seq == seq-1);
    
    dout(3) << "journal_replay: applying op seq " << seq << dendl;
    bufferlist::iterator p = bl.begin();
    list<Transaction*> tls;
    while (!p.end()) {
      Transaction *t = new Transaction(p);
      tls.push_back(t);
    }

    apply_manager.op_apply_start(seq);
    int r = do_transactions(tls, seq);
    apply_manager.op_apply_finish(seq);

    op_seq = seq;

    while (!tls.empty()) {
      delete tls.front(); 
      tls.pop_front();
    }

    dout(3) << "journal_replay: r = " << r << ", op_seq now " << op_seq << dendl;
  }

  replaying = false;

  submit_manager.set_op_seq(op_seq);

  // done reading, make writeable.
  err = journal->make_writeable();
  if (err < 0)
    return err;

  return count;
}


// ------------------------------------

uint64_t JournalingObjectStore::ApplyManager::op_apply_start(uint64_t op)
{
  Mutex::Locker l(apply_lock);
  while (blocked) {
    // note: this only happens during journal replay
    dout(10) << "op_apply_start blocked, waiting" << dendl;
    blocked_cond.Wait(apply_lock);
  }
  dout(10) << "op_apply_start " << op << " open_ops " << open_ops << " -> " << (open_ops+1) << dendl;
  assert(!blocked);
  assert(op > committed_seq);
  open_ops++;
  return op;
}

void JournalingObjectStore::ApplyManager::op_apply_finish(uint64_t op)
{
  Mutex::Locker l(apply_lock);
  dout(10) << "op_apply_finish " << op << " open_ops " << open_ops
	   << " -> " << (open_ops-1)
	   << ", max_applied_seq " << max_applied_seq << " -> " << MAX(op, max_applied_seq)
	   << dendl;
  --open_ops;
  assert(open_ops >= 0);

  // signal a blocked commit_start (only needed during journal replay)
  if (blocked) {
    blocked_cond.Signal();
  }

  // there can be multiple applies in flight; track the max value we
  // note.  note that we can't _read_ this value and learn anything
  // meaningful unless/until we've quiesced all in-flight applies.
  if (op > max_applied_seq)
    max_applied_seq = op;
}

uint64_t JournalingObjectStore::SubmitManager::op_submit_start()
{
  lock.Lock();
  uint64_t op = ++op_seq;
  dout(10) << "op_submit_start " << op << dendl;
  return op;
}

void JournalingObjectStore::SubmitManager::op_submit_finish(uint64_t op)
{
  dout(10) << "op_submit_finish " << op << dendl;
  if (op != op_submitted + 1) {
    dout(0) << "op_submit_finish " << op << " expected " << (op_submitted + 1)
	    << ", OUT OF ORDER" << dendl;
    assert(0 == "out of order op_submit_finish");
  }
  op_submitted = op;
  lock.Unlock();
}


// ------------------------------------------

void JournalingObjectStore::ApplyManager::add_waiter(uint64_t op, Context *c)
{
  Mutex::Locker l(com_lock);
  assert(c);
  commit_waiters[op].push_back(c);
}

bool JournalingObjectStore::ApplyManager::commit_start()
{
  bool ret = false;

  uint64_t _committing_seq = 0;
  {
    Mutex::Locker l(apply_lock);
    dout(10) << "commit_start max_applied_seq " << max_applied_seq
	     << ", open_ops " << open_ops
	     << dendl;
    blocked = true;
    while (open_ops > 0) {
      dout(10) << "commit_start waiting for " << open_ops << " open ops to drain" << dendl;
      blocked_cond.Wait(apply_lock);
    }
    assert(open_ops == 0);
    dout(10) << "commit_start blocked, all open_ops have completed" << dendl;
    {
      Mutex::Locker l(com_lock);
      if (max_applied_seq == committed_seq) {
	dout(10) << "commit_start nothing to do" << dendl;
	blocked = false;
	assert(commit_waiters.empty());
	goto out;
      }

      _committing_seq = committing_seq = max_applied_seq;

      dout(10) << "commit_start committing " << committing_seq
	       << ", still blocked" << dendl;
    }
  }
  ret = true;

 out:
  if (journal)
    journal->commit_start(_committing_seq);  // tell the journal too
  return ret;
}

void JournalingObjectStore::ApplyManager::commit_started()
{
  Mutex::Locker l(apply_lock);
  // allow new ops. (underlying fs should now be committing all prior ops)
  dout(10) << "commit_started committing " << committing_seq << ", unblocking" << dendl;
  blocked = false;
  blocked_cond.Signal();
}

void JournalingObjectStore::ApplyManager::commit_finish()
{
  Mutex::Locker l(com_lock);
  dout(10) << "commit_finish thru " << committing_seq << dendl;
  
  if (journal)
    journal->committed_thru(committing_seq);

  committed_seq = committing_seq;
  
  map<version_t, vector<Context*> >::iterator p = commit_waiters.begin();
  while (p != commit_waiters.end() &&
    p->first <= committing_seq) {
    finisher.queue(p->second);
    commit_waiters.erase(p++);
  }
}

void JournalingObjectStore::_op_journal_transactions(
  list<ObjectStore::Transaction*>& tls, uint64_t op,
  Context *onjournal, TrackedOpRef osd_op)
{
  dout(10) << "op_journal_transactions " << op << " " << tls << dendl;

  if (journal && journal->is_writeable()) {
    bufferlist tbl;
    unsigned data_len = 0;
    int data_align = -1; // -1 indicates that we don't care about the alignment
    for (list<ObjectStore::Transaction*>::iterator p = tls.begin();
	 p != tls.end(); ++p) {
      ObjectStore::Transaction *t = *p;
      if (t->get_data_length() > data_len &&
	(int)t->get_data_length() >= g_conf->journal_align_min_size) {
	data_len = t->get_data_length();
	data_align = (t->get_data_alignment() - tbl.length()) & ~CEPH_PAGE_MASK;
      }
      ::encode(*t, tbl);
    }
    journal->submit_entry(op, tbl, data_align, onjournal, osd_op);
  } else if (onjournal) {
    apply_manager.add_waiter(op, onjournal);
  }
}
