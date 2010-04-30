
#include "JournalingObjectStore.h"

#include "config.h"

#define DOUT_SUBSYS journal
#undef dout_prefix
#define dout_prefix *_dout << dbeginl << "journal "



void JournalingObjectStore::journal_start()
{
  dout(10) << "journal_start" << dendl;
  finisher.start();
}
 
void JournalingObjectStore::journal_stop() 
{
  dout(10) << "journal_stop" << dendl;
  finisher.stop();
  if (journal) {
    journal->close();
    delete journal;
    journal = 0;
  }
}

int JournalingObjectStore::journal_replay(__u64 fs_op_seq)
{
  dout(10) << "journal_replay fs op_seq " << fs_op_seq << dendl;
  op_seq = fs_op_seq;

  if (!journal)
    return 0;

  int err = journal->open(op_seq+1);
  if (err < 0) {
    char buf[80];
    dout(3) << "journal_replay open failed with " 
	    << strerror_r(-err, buf, sizeof(buf)) << dendl;
    delete journal;
    journal = 0;
    return err;
  }

  int count = 0;
  while (1) {
    bufferlist bl;
    __u64 seq = op_seq + 1;
    if (!journal->read_entry(bl, seq)) {
      dout(3) << "journal_replay: end of journal, done." << dendl;
      break;
    }

    if (seq <= op_seq) {
      dout(3) << "journal_replay: skipping old op seq " << seq << " <= " << op_seq << dendl;
      continue;
    }
    assert(op_seq == seq-1);
    
    dout(3) << "journal_replay: applying op seq " << seq << " (op_seq " << op_seq << ")" << dendl;
    bufferlist::iterator p = bl.begin();
    list<Transaction*> tls;
    while (!p.end()) {
      Transaction *t = new Transaction(p);
      tls.push_back(t);
    }
    int r = apply_transactions(tls);
    while (!tls.empty()) {
      delete tls.front(); 
      tls.pop_front();
    }

    dout(3) << "journal_replay: r = " << r << ", op now seq " << op_seq << dendl;
    assert(op_seq == seq);
    seq++;  // we expect the next op
  }

  committed_seq = op_seq;

  // done reading, make writeable.
  journal->make_writeable();

  return count;
}


// ------------------------------------

__u64 JournalingObjectStore::op_apply_start(__u64 op) 
{
  lock.Lock();
  while (blocked) {
    dout(10) << "op_apply_start blocked" << dendl;
    cond.Wait(lock);
  }
  open_ops++;

  if (!op)
    op = ++op_seq;
  dout(10) << "op_apply_start " << op << dendl;

  lock.Unlock();
  return op;
}

void JournalingObjectStore::op_apply_finish() 
{
  dout(10) << "op_apply_finish" << dendl;
  lock.Lock();
  if (--open_ops == 0)
    cond.Signal();
  lock.Unlock();
}

__u64 JournalingObjectStore::op_journal_start(__u64 op)
{
  journal_lock.Lock();
  if (!op) {
    lock.Lock();
    op = ++op_seq;
    lock.Unlock();
  }
  return op;
}

void JournalingObjectStore::op_journal_finish()
{
  journal_lock.Unlock();
}


// ------------------------------------------

bool JournalingObjectStore::commit_start() 
{
  // suspend new ops...
  Mutex::Locker l(lock);

  dout(10) << "commit_start" << dendl;
  blocked = true;
  while (open_ops > 0) {
    dout(10) << "commit_start blocked, waiting for " << open_ops << " open ops" << dendl;
    cond.Wait(lock);
  }
  
  if (op_seq == committed_seq) {
    dout(10) << "commit_start nothing to do" << dendl;
    blocked = false;
    cond.Signal();
    assert(commit_waiters.empty());
    return false;
  }
  committing_seq = op_seq;
  dout(10) << "commit_start committing " << committing_seq << ", blocked" << dendl;
  return true;
}

void JournalingObjectStore::commit_started() 
{
  Mutex::Locker l(lock);
  // allow new ops. (underlying fs should now be committing all prior ops)
  dout(10) << "commit_started committing " << committing_seq << ", unblocking" << dendl;
  blocked = false;
  cond.Signal();
}

void JournalingObjectStore::commit_finish()
{
  Mutex::Locker l(lock);
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

void JournalingObjectStore::journal_transaction(ObjectStore::Transaction& t, __u64 op,
						Context *onjournal)
{
  Mutex::Locker l(lock);
  dout(10) << "journal_transaction " << op << dendl;
  if (journal && journal->is_writeable()) {
    bufferlist tbl;
    t.encode(tbl);
    journal->submit_entry(op, tbl, onjournal);
  } else if (onjournal)
    commit_waiters[op].push_back(onjournal);
}

void JournalingObjectStore::journal_transactions(list<ObjectStore::Transaction*>& tls, __u64 op,
						 Context *onjournal)
{
  Mutex::Locker l(lock);
  dout(10) << "journal_transactions " << op << dendl;
    
  if (journal && journal->is_writeable()) {
    bufferlist tbl;
    for (list<ObjectStore::Transaction*>::iterator p = tls.begin(); p != tls.end(); p++)
      (*p)->encode(tbl);
    journal->submit_entry(op, tbl, onjournal);
  } else if (onjournal)
    commit_waiters[op].push_back(onjournal);
}
