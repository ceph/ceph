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

#include "CInode.h"
#include "MDCache.h"
#include "MDS.h"
#include "Locker.h"
#include "osdc/Filer.h"

#include "RecoveryQueue.h"


#define dout_subsys ceph_subsys_mds


struct C_MDC_Recover : public Context {
  RecoveryQueue *rq;
  CInode *in;
  uint64_t size;
  utime_t mtime;
  C_MDC_Recover(RecoveryQueue *rq_, CInode *i) : rq(rq_), in(i), size(0) {}
  void finish(int r) {
    rq->_recovered(in, r, size, mtime);
  }
};


/**
 * Progress the queue.  Call this after enqueuing something or on
 * completion of something.
 */
void RecoveryQueue::advance()
{
  dout(10) << "RecoveryQueue::advance " << file_recover_queue.size() << " queued, "
	   << file_recovering.size() << " recovering" << dendl;

  while (file_recovering.size() < 5 &&
	 !file_recover_queue.empty()) {
    CInode *in = *file_recover_queue.begin();
    file_recover_queue.erase(in);

    inode_t *pi = in->get_projected_inode();

    // blech
    if (pi->client_ranges.size() && !pi->get_max_size()) {
      mds->clog.warn() << "bad client_range " << pi->client_ranges
	  << " on ino " << pi->ino << "\n";
    }

    if (pi->client_ranges.size() && pi->get_max_size()) {
      dout(10) << "do_file_recover starting " << in->inode.size << " " << pi->client_ranges
	       << " " << *in << dendl;
      file_recovering.insert(in);

      C_MDC_Recover *fin = new C_MDC_Recover(this, in);
      mds->filer->probe(in->inode.ino, &in->inode.layout, in->last,
			pi->get_max_size(), &fin->size, &fin->mtime, false,
			0, fin);
    } else {
      dout(10) << "do_file_recover skipping " << in->inode.size
	       << " " << *in << dendl;
      in->state_clear(CInode::STATE_RECOVERING);
      mds->locker->eval(in, CEPH_LOCK_IFILE);
      in->auth_unpin(this);
    }
  }
}


/**
 * Given an authoritative inode which is in the cache,
 * enqueue it for recovery.
 */
void RecoveryQueue::enqueue(CInode *in)
{
  dout(15) << "RecoveryQueue::enqueue " << *in << dendl;
  assert(in->is_auth());

  in->state_clear(CInode::STATE_NEEDSRECOVER);
  if (!in->state_test(CInode::STATE_RECOVERING)) {
    in->state_set(CInode::STATE_RECOVERING);
    in->auth_pin(this);
  }
  file_recover_queue.insert(in);
}


/**
 * Call back on completion of Filer probe on an inode.
 */
void RecoveryQueue::_recovered(CInode *in, int r, uint64_t size, utime_t mtime)
{
  dout(10) << "_recovered r=" << r << " size=" << size << " mtime=" << mtime
	   << " for " << *in << dendl;

  if (r != 0) {
    dout(0) << "recovery error! " << r << dendl;
    if (r == -EBLACKLISTED) {
      mds->suicide();
      return;
    }
    assert(0 == "unexpected error from osd during recovery");
  }

  file_recovering.erase(in);
  in->state_clear(CInode::STATE_RECOVERING);

  if (!in->get_parent_dn() && !in->get_projected_parent_dn()) {
    dout(10) << " inode has no parents, killing it off" << dendl;
    in->auth_unpin(this);
    mds->mdcache->remove_inode(in);
  } else {
    // journal
    mds->locker->check_inode_max_size(in, true, true, size, false, 0, mtime);
    mds->locker->eval(in, CEPH_LOCK_IFILE);
    in->auth_unpin(this);
  }

  advance();
}

