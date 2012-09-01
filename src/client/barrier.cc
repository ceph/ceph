// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 *
 * Copyright (C) 2012 Linux Box Corporation.
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

#include <iostream>
using namespace std;

#include "include/Context.h"
#include "Client.h"

#include "common/config.h"

#include "barrier.h"

#include "include/assert.h"

#undef dout_prefix
#define dout_prefix *_dout << "client." << whoami << " "

#define dout_subsys ceph_subsys_client

#define cldout(cl, v)  dout_impl((cl)->cct, dout_subsys, v) \
  *_dout << "client." << cl->whoami << " "


/* C_Block_Sync */
C_Block_Sync::C_Block_Sync(Client *c, uint64_t i, barrier_interval iv) :
    cl(c), ino(i), iv(iv)
  {
      state = CBlockSync_State_None;
      barrier = NULL;

      cldout(cl, 1) << "C_Block_Sync for " << ino << dendl;

      if (! cl->barriers[ino]) {
          cl->barriers[ino] = new BarrierContext(cl, ino);
      }
      /* XXX current semantics aren't commit-ordered */
      cl->barriers[ino]->write_nobarrier(*this);
  }

void C_Block_Sync::finish(int) {
    cldout(cl, 1) << "C_Block_Sync::finish() for " << ino << " "
		  << iv.first << ", " << iv.second << dendl;
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
    cbs.state = CBlockSync_State_Unclaimed;
    outstanding_writes.push_back(cbs);
}

void BarrierContext::write_barrier(C_Block_Sync &cbs)
{
    Mutex::Locker locker(lock);
    barrier_interval &iv = cbs.iv;
    bool done = false;

    { /* find blocking commit--intrusive no help here */
        BarrierList::iterator iter;
        for (iter = active_commits.begin();
             !done && (iter != active_commits.end());
             ++iter) {
            Barrier &barrier = *iter;
            while (barrier.span.intersects(iv.first, iv.second)) {
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

    interval_set<uint64_t> cvs;
    cvs.insert(civ.first, civ.second);

    Barrier *barrier = NULL;
    BlockSyncList::iterator iter, iter2;

    iter = outstanding_writes.begin();
    while (iter != outstanding_writes.end()) {
        barrier_interval &iv = iter->iv;
        if (cvs.intersects(iv.first, iv.second)) {
            if (! barrier)
                barrier = new Barrier();
            /* mark the callback */
            iter->state = CBlockSync_State_Committing;
            iter->barrier = barrier;
            barrier->write_list.push_back(*iter);
            barrier->span.insert(iv.first, iv.second);
            /* avoid iter invalidate */
            iter2 = iter++;
            outstanding_writes.erase(iter2);
        } else {
            iter++;
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
        return;
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
