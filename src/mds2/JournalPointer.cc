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
#include "mdstypes.h"

#include "JournalPointer.h"


#define dout_subsys ceph_subsys_journaler
#undef dout_prefix
#define dout_prefix *_dout << objecter->messenger->get_myname() << ".journalpointer "


std::string JournalPointer::get_object_id() const
{
  inodeno_t const pointer_ino = MDS_INO_LOG_POINTER_OFFSET + node_id;
  char buf[32];
  snprintf(buf, sizeof(buf), "%llx.%08llx", (long long unsigned)pointer_ino, (long long unsigned)0);

  return std::string(buf);
}


/**
 * Blocking read of JournalPointer for this MDS
 */
int JournalPointer::load(Objecter *objecter)
{
  assert(objecter != NULL);

  // Blocking read of data
  std::string const object_id = get_object_id();
  dout(4) << "Reading journal pointer '" << object_id << "'" << dendl;
  bufferlist data;
  C_SaferCond waiter;
  objecter->read_full(object_t(object_id), object_locator_t(pool_id),
      CEPH_NOSNAP, &data, 0, &waiter);
  int r = waiter.wait();

  // Construct JournalPointer result, null or decoded data
  if (r == 0) {
    bufferlist::iterator q = data.begin();
    try {
      decode(q);
    } catch (const buffer::error &e) {
      return -EINVAL;
    }
  } else {
    dout(1) << "Journal pointer '" << object_id << "' read failed: " << cpp_strerror(r) << dendl;
  }
  return r;
}


/**
 * Blocking write of JournalPointer for this MDS
 *
 * @return objecter write op status code
 */
int JournalPointer::save(Objecter *objecter) const
{
  assert(objecter != NULL);
  // It is not valid to persist a null pointer
  assert(!is_null());

  // Serialize JournalPointer object
  bufferlist data;
  encode(data);

  // Write to RADOS and wait for durability
  std::string const object_id = get_object_id();
  dout(4) << "Writing pointer object '" << object_id << "': 0x"
    << std::hex << front << ":0x" << back << std::dec << dendl;

  C_SaferCond waiter;
  objecter->write_full(object_t(object_id), object_locator_t(pool_id),
		       SnapContext(), data,
		       ceph::real_clock::now(g_ceph_context), 0, NULL,
		       &waiter);
  int write_result = waiter.wait();
  if (write_result < 0) {
    derr << "Error writing pointer object '" << object_id << "': " << cpp_strerror(write_result) << dendl;
  }
  return write_result;
}


/**
 * Non-blocking variant of save() that assumes objecter lock already held by
 * caller
 */
void JournalPointer::save(Objecter *objecter, Context *completion) const
{
  assert(objecter != NULL);

  bufferlist data;
  encode(data);

  objecter->write_full(object_t(get_object_id()), object_locator_t(pool_id),
		       SnapContext(), data,
		       ceph::real_clock::now(g_ceph_context), 0, NULL,
		       completion);
}

