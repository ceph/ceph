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
#include "MDSRank.h"
#include "Locker.h"
#include "osdc/Filer.h"

#include "RecoveryQueue.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_mds
#undef dout_prefix
#define dout_prefix *_dout << "mds." << mds->get_nodeid() << " RecoveryQueue::" << __func__ << " "

class C_MDC_Recover : public MDSIOContextBase {
public:
  C_MDC_Recover(RecoveryQueue *rq_, CInode *i) :
    MDSIOContextBase(false), rq(rq_), in(i) {
    ceph_assert(rq != NULL);
  }
  void print(ostream& out) const override {
    out << "file_recover(" << in->ino() << ")";
  }

  uint64_t size = 0;
  utime_t mtime;
protected:
  void finish(int r) override {
    rq->_recovered(in, r, size, mtime);
  }

  MDSRank *get_mds() override {
    return rq->mds;
  }

  RecoveryQueue *rq;
  CInode *in;
};

RecoveryQueue::RecoveryQueue(MDSRank *mds_) :
  file_recover_queue(member_offset(CInode, item_dirty_dirfrag_dir)),
  file_recover_queue_front(member_offset(CInode, item_dirty_dirfrag_nest)),
  mds(mds_), filer(mds_->objecter, mds_->finisher)
{ }

/**
 * Progress the queue.  Call this after enqueuing something or on
 * completion of something.
 */
void RecoveryQueue::advance()
{
  dout(10) << file_recover_queue_size << " queued, "
	   << file_recover_queue_front_size << " prioritized, "
	   << file_recovering.size() << " recovering" << dendl;

  while (file_recovering.size() < g_conf()->mds_max_file_recover) {
    if (!file_recover_queue_front.empty()) {
      CInode *in = file_recover_queue_front.front();
      in->item_recover_queue_front.remove_myself();
      file_recover_queue_front_size--;
      _start(in);
    } else if (!file_recover_queue.empty()) {
      CInode *in = file_recover_queue.front();
      in->item_recover_queue.remove_myself();
      file_recover_queue_size--;
      _start(in);
    } else {
      break;
    }
  }

  logger->set(l_mdc_num_recovering_processing, file_recovering.size());
  logger->set(l_mdc_num_recovering_enqueued, file_recover_queue_size + file_recover_queue_front_size);
  logger->set(l_mdc_num_recovering_prioritized, file_recover_queue_front_size);
}

void RecoveryQueue::_start(CInode *in)
{
  const auto& pi = in->get_projected_inode();

  // blech
  if (pi->client_ranges.size() && !pi->get_max_size()) {
    mds->clog->warn() << "bad client_range " << pi->client_ranges
		      << " on ino " << pi->ino;
  }

  auto p = file_recovering.find(in);
  if (pi->client_ranges.size() && pi->get_max_size()) {
    dout(10) << "starting " << pi->size << " " << pi->client_ranges
	     << " " << *in << dendl;
    if (p == file_recovering.end()) {
      file_recovering.insert(make_pair(in, false));

      C_MDC_Recover *fin = new C_MDC_Recover(this, in);
      auto layout = pi->layout;
      filer.probe(in->ino(), &layout, in->last,
		  pi->get_max_size(), &fin->size, &fin->mtime, false,
		  0, fin);
    } else {
      p->second = true;
      dout(10) << "already working on " << *in << ", set need_restart flag" << dendl;
    }
  } else {
    dout(10) << "skipping " << pi->size << " " << *in << dendl;
    if (p == file_recovering.end()) {
      in->state_clear(CInode::STATE_RECOVERING);
      mds->locker->eval(in, CEPH_LOCK_IFILE);
      in->auth_unpin(this);
    }
  }
}

void RecoveryQueue::prioritize(CInode *in)
{
  if (file_recovering.count(in)) {
    dout(10) << "already working on " << *in << dendl;
    return;
  }

  if (!in->item_recover_queue_front.is_on_list()) {
    dout(20) << *in << dendl;

    ceph_assert(in->item_recover_queue.is_on_list());
    in->item_recover_queue.remove_myself();
    file_recover_queue_size--;

    file_recover_queue_front.push_back(&in->item_recover_queue_front);

    file_recover_queue_front_size++;
    logger->set(l_mdc_num_recovering_prioritized, file_recover_queue_front_size);
    return;
  }

  dout(10) << "not queued " << *in << dendl;
}

static bool _is_in_any_recover_queue(CInode *in)
{
  return in->item_recover_queue.is_on_list() ||
	 in->item_recover_queue_front.is_on_list();
}

/**
 * Given an authoritative inode which is in the cache,
 * enqueue it for recovery.
 */
void RecoveryQueue::enqueue(CInode *in)
{
  dout(15) << "RecoveryQueue::enqueue " << *in << dendl;
  ceph_assert(logger);  // Caller should have done set_logger before using me
  ceph_assert(in->is_auth());

  in->state_clear(CInode::STATE_NEEDSRECOVER);
  if (!in->state_test(CInode::STATE_RECOVERING)) {
    in->state_set(CInode::STATE_RECOVERING);
    in->auth_pin(this);
    logger->inc(l_mdc_recovery_started);
  }

  if (!_is_in_any_recover_queue(in)) {
    file_recover_queue.push_back(&in->item_recover_queue);
    file_recover_queue_size++;
    logger->set(l_mdc_num_recovering_enqueued, file_recover_queue_size + file_recover_queue_front_size);
  }
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
      mds->respawn();
      return;
    } else {
      // Something wrong on the OSD side trying to recover the size
      // of this inode.  In principle we could record this as a piece
      // of per-inode damage, but it's actually more likely that
      // this indicates something wrong with the MDS (like maybe
      // it has the wrong auth caps?)
      mds->clog->error() << " OSD read error while recovering size"
          " for inode " << in->ino();
      mds->damaged();
    }
  }

  auto p = file_recovering.find(in);
  ceph_assert(p != file_recovering.end());
  bool restart = p->second;
  file_recovering.erase(p);

  logger->set(l_mdc_num_recovering_processing, file_recovering.size());
  logger->inc(l_mdc_recovery_completed);
  in->state_clear(CInode::STATE_RECOVERING);

  if (restart) {
    if (in->item_recover_queue.is_on_list()) {
      in->item_recover_queue.remove_myself();
      file_recover_queue_size--;
    }
    if (in->item_recover_queue_front.is_on_list()) {
      in->item_recover_queue_front.remove_myself();
      file_recover_queue_front_size--;
    }
    logger->set(l_mdc_num_recovering_enqueued, file_recover_queue_size + file_recover_queue_front_size);
    logger->set(l_mdc_num_recovering_prioritized, file_recover_queue_front_size);
    _start(in);
  } else if (!_is_in_any_recover_queue(in)) {
    // journal
    mds->locker->check_inode_max_size(in, true, 0,  size, mtime);
    mds->locker->eval(in, CEPH_LOCK_IFILE);
    in->auth_unpin(this);
  }

  advance();
}

