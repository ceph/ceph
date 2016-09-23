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
#include "CDir.h"
#include "CDentry.h"

#include "MDSRank.h"
#include "MDCache.h"
#include "Locker.h"

#include "osdc/Filer.h"

#include "RecoveryQueue.h"


#define dout_subsys ceph_subsys_mds
#undef dout_prefix
#define dout_prefix *_dout << "mds." << mds->get_nodeid() << " RecoveryQueue::" << __func__ << " "

class C_MDC_Recovered : public MDSAsyncContextBase {
protected:
  RecoveryQueue *rq;
  CInodeRef in;
  void finish(int r) {
    rq->_recovered(in, r, size, mtime);
  }

  MDSRank *get_mds() { return rq->mds; }
public:
  uint64_t size;
  utime_t mtime;

  C_MDC_Recovered(RecoveryQueue *rq_, CInodeRef& i) : rq(rq_), in(i), size(0) {
    assert(rq != NULL);
  }
};

class C_MDC_RecoveryKick : public MDSAsyncContextBase {
protected:
  RecoveryQueue *rq;
  MDSRank *get_mds() { return rq->mds; }
  void finish(int r) {
    rq->advance();
  }
public:
  C_MDC_RecoveryKick(RecoveryQueue *rq_) : rq(rq_) {}
};


RecoveryQueue::RecoveryQueue(MDSRank *mds_) :
  lock("RecoveryQueue::lock"),
  mds(mds_), logger(NULL), kick(NULL)
{}


/**
 * Progress the queue.  Call this after enqueuing something or on
 * completion of something.
 */
void RecoveryQueue::_advance()
{ 
  dout(10) << file_recover_queue.size() << " queued, "
	   << file_recover_queue_front.size() << " prioritized, "
	   << file_recovering.size() << " recovering" << dendl;

  while (file_recovering.size() < g_conf->mds_max_file_recover) {
    CInodeRef in;
    if (!file_recover_queue_front.empty()) {
      auto p = file_recover_queue_front.begin();
      in = *p;
      file_recover_queue_front.erase(p);
      file_recover_queue.erase(in.get());
    } else if (!file_recover_queue.empty()) {
      auto p = file_recover_queue.begin();
      in = *p;
      file_recover_queue.erase(p);
    } else {
      break;
    }

    if (in) {
      file_recovering.insert(in.get());
      lock.Unlock();

      in->mutex_lock();
      bool started = _start(in);
      if (started) {
	in->mutex_unlock();
	in.reset();
	lock.Lock();
      } else {
	lock.Lock();
	file_recovering.erase(in.get());
	in->mutex_unlock();
      }
    }
  }

  /*
  logger->set(l_mdc_num_recovering_processing, file_recovering.size());
  logger->set(l_mdc_num_recovering_enqueued, file_recover_queue.size());
  logger->set(l_mdc_num_recovering_prioritized, file_recover_queue_front.size());
  */
}

void RecoveryQueue::advance()
{
  Mutex::Locker l(lock);
  kick = NULL;
  _advance();
}

bool RecoveryQueue::_start(CInodeRef& in)
{
  const inode_t *pi = in->get_projected_inode();
  bool started = false;

  // blech
  if (pi->client_ranges.size() && !pi->get_max_size()) {
    mds->clog->warn() << "bad client_range " << pi->client_ranges
		      << " on ino " << pi->ino << "\n";
  }

  if (pi->client_ranges.size() && pi->get_max_size()) {
    dout(10) << "starting " << pi->size << " " << pi->client_ranges
	     << " " << *in << dendl;
    started = true;
    C_MDC_Recovered *fin = new C_MDC_Recovered(this, in);
    file_layout_t layout = pi->layout;
    mds->mdcache->get_filer()->probe(in->ino(), &layout, in->last,
		    		     pi->get_max_size(), &fin->size, &fin->mtime,
				     false, 0, fin);
  } else {
    dout(10) << "skipping " << pi->size << " " << *in << dendl;
    in->state_clear(CInode::STATE_RECOVERING);
    in->put(CInode::PIN_RECOVERING);
    mds->locker->eval(in.get(), CEPH_LOCK_IFILE);
  }
  return started;
}

void RecoveryQueue::prioritize(CInode *in)
{
  Mutex::Locker l(lock);
  if (file_recovering.count(in)) {
    dout(10) << "already working on " << *in << dendl;
    return;
  }

  if (file_recover_queue.count(in)) {
    dout(20) << *in << dendl;
    file_recover_queue_front.insert(in);
    //logger->set(l_mdc_num_recovering_prioritized, file_recover_queue_front.size());
    return;
  }

  dout(10) << "not queued " << *in << dendl;
}


/**
 * Given an authoritative inode which is in the cache,
 * enqueue it for recovery.
 */
void RecoveryQueue::enqueue(CInode *in)
{
  in->mutex_assert_locked_by_me();
  Mutex::Locker l(lock);
  dout(15) << "RecoveryQueue::enqueue " << *in << dendl;
  //assert(logger);  // Caller should have done set_logger before using me
  //assert(in->is_auth());

  in->state_clear(CInode::STATE_NEEDSRECOVER);
  if (!in->state_test(CInode::STATE_RECOVERING)) {
    in->get(CInode::PIN_RECOVERING);
    in->state_set(CInode::STATE_RECOVERING);
    //logger->inc(l_mdc_recovery_started);
    file_recover_queue.insert(in);
  }
  //logger->set(l_mdc_num_recovering_enqueued, file_recover_queue.size());
  if (!kick && file_recovering.size() < g_conf->mds_max_file_recover) {
    kick = new C_MDC_RecoveryKick(this);
    mds->queue_context(kick);
  }
}


/**
 * Call back on completion of Filer probe on an inode.
 */
void RecoveryQueue::_recovered(CInodeRef& in, int r, uint64_t size, utime_t mtime)
{
  dout(10) << "_recovered r=" << r << " size=" << size << " mtime=" << mtime
	   << " for " << *in << dendl;

  if (r != 0) {
    dout(0) << "recovery error! " << r << dendl;
    if (r == -EBLACKLISTED) {
      mds->respawn();
      return;
    }
    assert(0 == "unexpected error from osd during recovery");
  }

  in->mutex_lock();
  in->state_clear(CInode::STATE_RECOVERING);
  in->put(CInode::PIN_RECOVERING);

  if (!in->get_parent_dn() && !in->get_projected_parent_dn()) {
    dout(10) << " inode has no parents, killing it off" << dendl;
    assert(0);
    //in->auth_unpin(this);
    //mds->mdcache->remove_inode(in);
  } else {
    // journal
    mds->locker->check_inode_max_size(in.get(), false, true, 0, size, mtime);
    mds->locker->eval(in.get(), CEPH_LOCK_IFILE);
    //in->auth_unpin(this);
  }

  Mutex::Locker l(lock);
  file_recovering.erase(in.get());
  //logger->set(l_mdc_num_recovering_processing, file_recovering.size());
  //logger->inc(l_mdc_recovery_completed);

  in->mutex_unlock();

  _advance();
}

