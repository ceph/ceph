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


#include "common/debug.h"
#include "common/errno.h"
#include "common/Cond.h"
#include "osdc/Objecter.h"
#include "mds/mdstypes.h"

#include "mds/JournalPointer.h"


#define dout_subsys ceph_subsys_journaler
#undef dout_prefix
#define dout_prefix *_dout << objecter->messenger->get_myname() << ".journalpointer"

/**
 * Blocking read of JournalPointer for this MDS
 */
int JournalPointer::load(Objecter *objecter, Mutex *lock)
{
  assert(lock != NULL);
  assert(objecter != NULL);
  assert(!lock->is_locked_by_me());

  inodeno_t const pointer_ino = MDS_INO_LOG_POINTER_OFFSET + node_id;
  char buf[32];
  snprintf(buf, sizeof(buf), "%llx.%08llx", (long long unsigned)pointer_ino, (long long unsigned)0);

  // Blocking read of data
  dout(4) << "Reading journal pointer '" << buf << "'" << dendl;
  bufferlist data;
  C_SaferCond waiter;
  lock->Lock();
  objecter->read_full(object_t(buf), object_locator_t(pool_id),
      CEPH_NOSNAP, &data, 0, &waiter);
  lock->Unlock();
  int r = waiter.wait();

  // Construct JournalPointer result, null or decoded data
  if (r == 0) {
    bufferlist::iterator q = data.begin();
    decode(q);
  } else {
    dout(1) << "Journal pointer '" << buf << "' read failed: " << cpp_strerror(r) << dendl;
  }
  return r;
}


/**
 * Blocking write of JournalPointer for this MDS
 *
 * @return objecter write op status code
 */
int JournalPointer::save(Objecter *objecter, Mutex *lock) const
{
  assert(lock != NULL);
  assert(objecter != NULL);
  assert(!lock->is_locked_by_me());
  // It is not valid to persist a null pointer
  assert(!is_null());

  // Calculate object ID
  inodeno_t const pointer_ino = MDS_INO_LOG_POINTER_OFFSET + node_id;
  char buf[32];
  snprintf(buf, sizeof(buf), "%llx.%08llx", (long long unsigned)pointer_ino, (long long unsigned)0);
  dout(4) << "Writing pointer object '" << buf << "': 0x"
    << std::hex << front << ":0x" << back << std::dec << dendl;

  // Serialize JournalPointer object
  bufferlist data;
  encode(data);

  // Write to RADOS and wait for durability
  C_SaferCond waiter;
  lock->Lock();
  objecter->write_full(object_t(buf), object_locator_t(pool_id),
      SnapContext(), data, ceph_clock_now(g_ceph_context), 0, NULL, &waiter);
  lock->Unlock();
  int write_result = waiter.wait();
  if (write_result < 0) {
    derr << "Error writing pointer object '" << buf << "': " << cpp_strerror(write_result) << dendl;
  }
  return write_result;
}

