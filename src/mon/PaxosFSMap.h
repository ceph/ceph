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

#ifndef CEPH_PAXOS_FSMAP_H
#define CEPH_PAXOS_FSMAP_H

#include "mds/FSMap.h"
#include "mds/MDSMap.h"

#include "include/ceph_assert.h"

class PaxosFSMap {
public:
  virtual ~PaxosFSMap() {}

  const FSMap &get_pending_fsmap() const { ceph_assert(is_leader()); return pending_fsmap; }
  const FSMap &get_fsmap() const { return fsmap; }

  virtual bool is_leader() const = 0;

protected:
  FSMap &get_pending_fsmap_writeable() { ceph_assert(is_leader()); return pending_fsmap; }

  FSMap &create_pending() {
    ceph_assert(is_leader());
    pending_fsmap = fsmap;
    pending_fsmap.inc_epoch();
    return pending_fsmap;
  }

  void decode(ceph::buffer::list &bl) {
    fsmap.decode(bl);
    pending_fsmap = FSMap(); /* nuke it to catch invalid access */
  }

private:
  /* Keep these PRIVATE to prevent unprotected manipulation. */
  FSMap fsmap; /* the current epoch */
  FSMap pending_fsmap; /* the next epoch */
};


#endif
