// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2014 Red Hat
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef SCRUBSTACK_H_
#define SCRUBSTACK_H_

#include "CDir.h"
#include "CDentry.h"
#include "CInode.h"
#include "MDSContext.h"
#include "ScrubHeader.h"

#include "common/LogClient.h"
#include "include/elist.h"
#include "messages/MMDSScrub.h"
#include "messages/MMDSScrubStats.h"

class MDCache;
class Finisher;

class ScrubStack {
public:
  ScrubStack(MDCache *mdc, LogChannelRef &clog, Finisher *finisher_) :
    mdcache(mdc),
    clog(clog),
    finisher(finisher_),
    scrub_stack(member_offset(MDSCacheObject, item_scrub)),
    scrub_waiting(member_offset(MDSCacheObject, item_scrub)) {}
  ~ScrubStack() {
    ceph_assert(scrub_stack.empty());
    ceph_assert(!scrubs_in_progress);
  }
  /**
   * Put the inode at either the top or bottom of the stack, with the
   * given scrub params, and kick off more scrubbing.
   * @param in The inode to scrub
   * @param header The ScrubHeader propagated from wherever this scrub
   */
  int enqueue(CInode *in, ScrubHeaderRef& header, bool top);
  /**
   * Abort an ongoing scrub operation. The abort operation could be
   * delayed if there are in-progress scrub operations on going. The
   * caller should provide a context which is completed after all
   * in-progress scrub operations are completed and pending inodes
   * are removed from the scrub stack (with the context callbacks for
   * inodes completed with -CEPHFS_ECANCELED).
   * @param on_finish Context callback to invoke after abort
   */
  void scrub_abort(Context *on_finish);

  /**
   * Pause scrub operations. Similar to abort, pause is delayed if
   * there are in-progress scrub operations on going. The caller
   * should provide a context which is completed after all in-progress
   * scrub operations are completed. Subsequent scrub operations are
   * queued until scrub is resumed.
   * @param on_finish Context callback to invoke after pause
   */
  void scrub_pause(Context *on_finish);

  /**
   * Resume a paused scrub. Unlike abort or pause, this is instantaneous.
   * Pending pause operations are cancelled (context callbacks are
   * invoked with -CEPHFS_ECANCELED).
   * @returns 0 (success) if resumed, -CEPHFS_EINVAL if an abort is in-progress.
   */
  bool scrub_resume();

  /**
   * Get the current scrub status as human readable string. Some basic
   * information is returned such as number of inodes pending abort/pause.
   */
  void scrub_status(Formatter *f);

  /**
   * Get a high level scrub status summary such as current scrub state
   * and scrub paths.
   */
  std::string_view scrub_summary();

  static bool is_idle(std::string_view state_str) {
    return state_str == "idle";
  }

  bool is_scrubbing() const { return !scrub_stack.empty(); }

  void advance_scrub_status();

  void handle_mds_failure(mds_rank_t mds);

  void dispatch(const cref_t<Message> &m);

  bool remove_inode_if_stacked(CInode *in);

  MDCache *mdcache;

protected:

  // reference to global cluster log client
  LogChannelRef &clog;

  /// A finisher needed so that we don't re-enter kick_off_scrubs
  Finisher *finisher;

  /// The stack of inodes we want to scrub
  elist<MDSCacheObject*> scrub_stack;
  elist<MDSCacheObject*> scrub_waiting;
  /// current number of dentries we're actually scrubbing
  int scrubs_in_progress = 0;
  int stack_size = 0;

  struct scrub_remote_t {
    std::string tag;
    std::set<mds_rank_t> gather_set;
  };
  std::map<CInode*, scrub_remote_t> remote_scrubs;

  unsigned scrub_epoch = 2;
  unsigned scrub_epoch_fully_acked = 0;
  unsigned scrub_epoch_last_abort = 2;
  // check if any mds is aborting scrub after mds.0 starts
  bool scrub_any_peer_aborting = true;

  struct scrub_stat_t {
    unsigned epoch_acked = 0;
    std::set<std::string> scrubbing_tags;
    bool aborting = false;
  };
  std::vector<scrub_stat_t> mds_scrub_stats;

  std::map<std::string, ScrubHeaderRef> scrubbing_map;

  friend class C_RetryScrub;
private:
  // scrub abort is _not_ a state, rather it's an operation that's
  // performed after in-progress scrubs are finished.
  enum State {
    STATE_RUNNING = 0,
    STATE_IDLE,
    STATE_PAUSING,
    STATE_PAUSED,
  };
  friend std::ostream &operator<<(std::ostream &os, const State &state);

  friend class C_InodeValidated;

  int _enqueue(MDSCacheObject *obj, ScrubHeaderRef& header, bool top);
  /**
   * Remove the inode/dirfrag from the stack.
   */
  inline void dequeue(MDSCacheObject *obj);

  /**
   * Kick off as many scrubs as are appropriate, based on the current
   * state of the stack.
   */
  void kick_off_scrubs();

  /**
   * Move the inode/dirfrag that can't be scrubbed immediately
   * from scrub queue to waiting list.
   */
  void add_to_waiting(MDSCacheObject *obj);
  /**
   * Move the inode/dirfrag back to scrub queue.
   */
  void remove_from_waiting(MDSCacheObject *obj, bool kick=true);
  /**
   * Validate authority of the inode. If current mds is not auth of the inode,
   * forword scrub to auth mds.
   */
  bool validate_inode_auth(CInode *in);

  /**
   * Scrub a file inode.
   * @param in The inode to scrub
   */
  void scrub_file_inode(CInode *in);

  /**
   * Callback from completion of CInode::validate_disk_state
   * @param in The inode we were validating
   * @param r The return status from validate_disk_state
   * @param result Populated results from validate_disk_state
   */
  void _validate_inode_done(CInode *in, int r,
			    const CInode::validated_data &result);

  /**
   * Scrub a directory inode. It queues child dirfrags, then does
   * final scrub of the inode.
   *
   * @param in The directory indoe to scrub
   * @param added_children set to true if we pushed some of our children
   * @param done set to true if we started to do final scrub
   */
  void scrub_dir_inode(CInode *in, bool *added_children, bool *done);
  /**
   * Scrub a dirfrag. It queues child dentries, then does final
   * scrub of the dirfrag.
   *
   * @param dir The dirfrag to scrub (must be auth)
   * @param done set to true if we started to do final scrub
   */
  void scrub_dirfrag(CDir *dir, bool *done);
  /**
   * Scrub a directory-representing dentry.
   *
   * @param in The directory inode we're doing final scrub on.
   */
  void scrub_dir_inode_final(CInode *in);
  /**
   * Set scrub state
   * @param next_state State to move the scrub to.
   */
  void set_state(State next_state);

  /**
   * Is scrub in one of transition states (running, pausing)
   */
  bool scrub_in_transition_state();

  /**
   * complete queued up contexts
   * @param r return value to complete contexts.
   */
  void complete_control_contexts(int r);

  /**
   * ask peer mds (rank > 0) to abort/pause/resume scrubs
   */
  void send_state_message(int op);

  /**
   * Abort pending scrubs for inodes waiting in the inode stack.
   * Completion context is complete with -CEPHFS_ECANCELED.
   */
  void abort_pending_scrubs();

  /**
   * Return path for a given inode.
   * @param in inode to make path entry.
   */
  std::string scrub_inode_path(CInode *in) {
    std::string path;
    in->make_path_string(path, true);
    return (path.empty() ? "/" : path.c_str());
  }

  /**
   * Send scrub information (queued/finished scrub path and summary)
   * to cluster log.
   * @param in inode for which scrub has been queued or finished.
   */
  void clog_scrub_summary(CInode *in=nullptr);

  void handle_scrub(const cref_t<MMDSScrub> &m);
  void handle_scrub_stats(const cref_t<MMDSScrubStats> &m);

  State state = STATE_IDLE;
  bool clear_stack = false;

  // list of pending context completions for asynchronous scrub
  // control operations.
  std::vector<Context *> control_ctxs;
};

#endif /* SCRUBSTACK_H_ */
