
#include "JournalingObjectStore.h"

#include "config.h"

#define dout(x) if (x <= g_conf.debug) *_dout << dbeginl << g_clock.now() << " journal "
#define derr(x) if (x <= g_conf.debug) *_derr << dbeginl << g_clock.now() << " journal "

int JournalingObjectStore::journal_replay()
{
  if (!journal)
    return 0;

  int err = journal->open(op_seq);
  if (err < 0) {
    dout(3) << "journal_replay open failed with" << err
	    << " " << strerror(err) << dendl;
    delete journal;
    journal = 0;
    return err;
  }

  int count = 0;
  while (1) {
    bufferlist bl;
    __u64 seq;
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
    Transaction t(bl);
    apply_transaction(t);

    assert(op_seq == seq);
  }

  // done reading, make writeable.
  journal->make_writeable();

  return count;
}
