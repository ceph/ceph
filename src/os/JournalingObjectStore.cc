
#include "JournalingObjectStore.h"

#include "config.h"

#define dout(x) if (x <= g_conf.debug) *_dout << dbeginl << g_clock.now() << " journal "
#define derr(x) if (x <= g_conf.debug) *_derr << dbeginl << g_clock.now() << " journal "

int JournalingObjectStore::journal_replay()
{
  if (!journal)
    return 0;

  int err = journal->open(super_epoch);
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
    epoch_t e;
    if (!journal->read_entry(bl, e)) {
      dout(3) << "journal_replay: end of journal, done." << dendl;
      break;
    }
    
    if (e < super_epoch) {
      dout(3) << "journal_replay: skipping old entry in epoch " << e << " < " << super_epoch << dendl;
      continue;
    }
    if (e == super_epoch+1) {
      super_epoch++;
      dout(3) << "journal_replay: jumped to next epoch " << super_epoch << dendl;
    }
    assert(e == super_epoch);
    
    dout(3) << "journal_replay: applying transaction in epoch " << e << dendl;
    Transaction t(bl);
    apply_transaction(t);
    count++;
  }

  // done reading, make writeable.
  journal->make_writeable();

  return count;
}
