// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2010 Greg Farnum <gregf@hq.newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software 
 * Foundation.  See file COPYING.
 * 
 */

#include "common/errno.h"
#include "osdc/Journaler.h"
#include "mds/JournalPointer.h"

#include "mds/mdstypes.h"
#include "mds/MDCache.h"
#include "mon/MonClient.h"
#include "mds/events/EResetJournal.h"

#include "Resetter.h"

#define dout_subsys ceph_subsys_mds

int Resetter::reset(mds_role_t role)
{
  Mutex mylock("Resetter::reset::lock");
  Cond cond;
  bool done;
  int r;

  auto fs =  fsmap->get_filesystem(role.fscid);
  assert(fs != nullptr);
  int const pool_id = fs->mds_map.get_metadata_pool();

  JournalPointer jp(role.rank, pool_id);
  int jp_load_result = jp.load(objecter);
  if (jp_load_result != 0) {
    std::cerr << "Error loading journal: " << cpp_strerror(jp_load_result) <<
      ", pass --force to forcibly reset this journal" << std::endl;
    return jp_load_result;
  }

  Journaler journaler(jp.front,
      pool_id,
      CEPH_FS_ONDISK_MAGIC,
      objecter, 0, 0, &timer, &finisher);

  lock.Lock();
  journaler.recover(new C_SafeCond(&mylock, &cond, &done, &r));
  lock.Unlock();

  mylock.Lock();
  while (!done)
    cond.Wait(mylock);
  mylock.Unlock();

  if (r != 0) {
    if (r == -ENOENT) {
      cerr << "journal does not exist on-disk. Did you set a bad rank?"
	   << std::endl;
      std::cerr << "Error loading journal: " << cpp_strerror(r) <<
        ", pass --force to forcibly reset this journal" << std::endl;
      return r;
    } else {
      cerr << "got error " << r << "from Journaler, failing" << std::endl;
      return r;
    }
  }

  lock.Lock();
  uint64_t old_start = journaler.get_read_pos();
  uint64_t old_end = journaler.get_write_pos();
  uint64_t old_len = old_end - old_start;
  cout << "old journal was " << old_start << "~" << old_len << std::endl;

  uint64_t new_start = ROUND_UP_TO(old_end+1, journaler.get_layout_period());
  cout << "new journal start will be " << new_start
       << " (" << (new_start - old_end) << " bytes past old end)" << std::endl;

  journaler.set_read_pos(new_start);
  journaler.set_write_pos(new_start);
  journaler.set_expire_pos(new_start);
  journaler.set_trimmed_pos(new_start);
  journaler.set_writeable();

  cout << "writing journal head" << std::endl;
  journaler.write_head(new C_SafeCond(&mylock, &cond, &done, &r));
  lock.Unlock();

  mylock.Lock();
  while (!done)
    cond.Wait(mylock);
  mylock.Unlock();
    
  lock.Lock();
  if (r != 0) {
    return r;
  }

  r = _write_reset_event(&journaler);
  if (r != 0) {
    return r;
  }

  lock.Unlock();

  cout << "done" << std::endl;

  return 0;
}

int Resetter::reset_hard(mds_role_t role)
{
  auto fs =  fsmap->get_filesystem(role.fscid);
  assert(fs != nullptr);
  int const pool_id = fs->mds_map.get_metadata_pool();

  JournalPointer jp(role.rank, pool_id);
  jp.front = role.rank + MDS_INO_LOG_OFFSET;
  jp.back = 0;
  int r = jp.save(objecter);
  if (r != 0) {
    derr << "Error writing journal pointer: " << cpp_strerror(r) << dendl;
    return r;
  }

  Journaler journaler(jp.front,
    pool_id,
    CEPH_FS_ONDISK_MAGIC,
    objecter, 0, 0, &timer, &finisher);
  journaler.set_writeable();

  file_layout_t default_log_layout = MDCache::gen_default_log_layout(
      fsmap->get_filesystem(role.fscid)->mds_map);
  journaler.create(&default_log_layout, g_conf->mds_journal_format);

  C_SaferCond cond;
  {
    Mutex::Locker l(lock);
    journaler.write_head(&cond);
  }
  r = cond.wait();
  if (r != 0) {
    derr << "Error writing journal header: " << cpp_strerror(r) << dendl;
    return r;
  }

  {
    Mutex::Locker l(lock);
    r = _write_reset_event(&journaler);
  }
  if (r != 0) {
    derr << "Error writing EResetJournal: " << cpp_strerror(r) << dendl;
    return r;
  }

  dout(4) << "Successfully wrote new journal pointer and header for rank "
    << role << dendl;
  return 0;
}

int Resetter::_write_reset_event(Journaler *journaler)
{
  assert(journaler != NULL);

  LogEvent *le = new EResetJournal;

  bufferlist bl;
  le->encode_with_header(bl, CEPH_FEATURES_SUPPORTED_DEFAULT);
  
  cout << "writing EResetJournal entry" << std::endl;
  C_SaferCond cond;
  journaler->append_entry(bl);
  journaler->flush(&cond);

  return cond.wait();
}

