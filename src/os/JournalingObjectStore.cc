
#include "JournalingObjectStore.h"

#include "config.h"

#define DOUT_SUBSYS journal
#undef dout_prefix
#define dout_prefix *_dout << dbeginl << "journal "


int JournalingObjectStore::journal_replay()
{
  if (!journal)
    return 0;

  int err = journal->open(op_seq+1);
  if (err < 0) {
    char buf[80];
    dout(3) << "journal_replay open failed with" << err
	    << " " << strerror_r(err, buf, sizeof(buf)) << dendl;
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

  committed_op_seq = op_seq;

  // done reading, make writeable.
  journal->make_writeable();

  return count;
}
