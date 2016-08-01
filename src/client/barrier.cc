// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 *
 * Copyright (C) 2012 CohortFS, LLC.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#if defined(__FreeBSD__)
#include <sys/param.h>
#endif

#include "include/Context.h"
#include "Client.h"
#include "barrier.h"
#include "include/assert.h"

#undef dout_prefix
#define dout_prefix *_dout << "client." << whoami << " "

#define dout_subsys ceph_subsys_client

#define cldout(cl, v)  dout_impl((cl)->cct, dout_subsys, v) \
  *_dout << "client." << cl->whoami << " "

/* C_Block_Sync */
C_Block_Sync::C_Block_Sync(Client *c, uint64_t i, barrier_interval iv,
			   int *r=0) :
  cl(c), ino(i), iv(iv), rval(r)
{
  state = CBlockSync_State_None;
  barrier = NULL;

  cldout(cl, 1) << "C_Block_Sync for " << ino << dendl;

  if (!cl->barriers[ino]) {
    cl->barriers[ino] = new BarrierContext(cl, ino);
  }
  /* XXX current semantics aren't commit-ordered */
  cl->barriers[ino]->write_nobarrier(*this);
}

void C_Block_Sync::finish(int r) {
  cldout(cl, 1) << "C_Block_Sync::finish() for " << ino << " "
		<< iv << " r==" << r << dendl;
  if (rval)
    *rval = r;
  cl->barriers[ino]->complete(*this);
}

/* Barrier */
Barrier::Barrier()
{ }

Barrier::~Barrier()
{ }

/* BarrierContext */
BarrierContext::BarrierContext(Client *c, uint64_t ino) :
  cl(c), ino(ino), lock("BarrierContext")
{ };

void BarrierContext::write_nobarrier(C_Block_Sync &cbs)
{
  Mutex::Locker locker(lock);
  cbs.state = CBlockSync_State_Unclaimed;
  outstanding_writes.push_back(cbs);
}

void BarrierContext::write_barrier(C_Block_Sync &cbs)
{
  Mutex::Locker locker(lock);
  barrier_interval &iv = cbs.iv;

  { /* find blocking commit--intrusive no help here */
    BarrierList::iterator iter;
    bool done = false;
    for (iter = active_commits.begin();
	 !done && (iter != active_commits.end());
	 ++iter) {
      Barrier &barrier = *iter;
      while (boost::icl::intersects(barrier.span, iv)) {
	/*  wait on this */
	barrier.cond.Wait(lock);
	done = true;
      }
    }
  }

  cbs.state = CBlockSync_State_Unclaimed;
  outstanding_writes.push_back(cbs);

} /* write_barrier */

void BarrierContext::commit_barrier(barrier_interval &civ)
{
    Mutex::Locker locker(lock);

    /* we commit outstanding writes--if none exist, we don't care */
    if (outstanding_writes.size() == 0)
      return;

    boost::icl::interval_set<uint64_t> cvs;
    cvs.insert(civ);

    Barrier *barrier = NULL;
    BlockSyncList::iterator iter, iter2;

    iter = outstanding_writes.begin();
    while (iter != outstanding_writes.end()) {
      barrier_interval &iv = iter->iv;
      if (boost::icl::intersects(cvs, iv)) {
	C_Block_Sync &a_write = *iter;
	if (! barrier)
	  barrier = new Barrier();
	/* mark the callback */
	a_write.state = CBlockSync_State_Committing;
	a_write.barrier = barrier;
	iter2 = iter++;
	outstanding_writes.erase(iter2);
	barrier->write_list.push_back(a_write);
	barrier->span.insert(iv);
	/* avoid iter invalidate */
      } else {
	++iter;
      }
    }

    if (barrier) {
      active_commits.push_back(*barrier);
      /* and wait on this */
      barrier->cond.Wait(lock);
    }

} /* commit_barrier */

void BarrierContext::complete(C_Block_Sync &cbs)
{
    Mutex::Locker locker(lock);
    BlockSyncList::iterator iter =
      BlockSyncList::s_iterator_to(cbs);

    switch (cbs.state) {
    case CBlockSync_State_Unclaimed:
      /* cool, no waiting */
      outstanding_writes.erase(iter);
      break;
    case CBlockSync_State_Committing:
    {
      Barrier *barrier = iter->barrier;
      barrier->write_list.erase(iter);
      /* signal waiters */
      barrier->cond.Signal();
	/* dispose cleared barrier */
      if (barrier->write_list.size() == 0) {
	BarrierList::iterator iter2 =
	  BarrierList::s_iterator_to(*barrier);
	active_commits.erase(iter2);
	delete barrier;
      }
    }
    break;
    default:
      assert(false);
      break;
    }

    cbs.state = CBlockSync_State_Completed;

} /* complete */

BarrierContext::~BarrierContext()
{ }
