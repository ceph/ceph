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

class MDCache;
class Finisher;

class ScrubStack {
public:
  ScrubStack(MDCache *mdc, LogChannelRef &clog, Finisher *finisher_) :
    mdcache(mdc),
    clog(clog),
    finisher(finisher_),
    inode_stack(member_offset(CInode, item_scrub)),
    scrubstack(this),
    scrub_kick(mdc, this) {}
  ~ScrubStack() {
    ceph_assert(inode_stack.empty());
    ceph_assert(!scrubs_in_progress);
  }
  /**
   * Put a inode on the top of the scrub stack, so it is the highest priority.
   * If there are other scrubs in progress, they will not continue scrubbing new
   * entries until this one is completed.
   * @param in The inodey to scrub
   * @param header The ScrubHeader propagated from wherever this scrub
   *               was initiated
   */
  void enqueue_inode_top(CInode *in, ScrubHeaderRef& header,
			 MDSContext *on_finish) {
    enqueue_inode(in, header, on_finish, true);
    scrub_origins.emplace(in);
    clog_scrub_summary(in);
  }
  /** Like enqueue_inode_top, but we wait for all pending scrubs before
   * starting this one.
   */
  void enqueue_inode_bottom(CInode *in, ScrubHeaderRef& header,
			    MDSContext *on_finish) {
    enqueue_inode(in, header, on_finish, false);
    scrub_origins.emplace(in);
    clog_scrub_summary(in);
  }

  /**
   * Abort an ongoing scrub operation. The abort operation could be
   * delayed if there are in-progress scrub operations on going. The
   * caller should provide a context which is completed after all
   * in-progress scrub operations are completed and pending inodes
   * are removed from the scrub stack (with the context callbacks for
   * inodes completed with -ECANCELED).
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
   * invoked with -ECANCELED).
   * @returns 0 (success) if resumed, -EINVAL if an abort is in-progress.
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

  bool is_scrubbing() const { return !inode_stack.empty(); }

  MDCache *mdcache;

protected:
  class C_KickOffScrubs : public MDSInternalContext {
  public:
    C_KickOffScrubs(MDCache *mdcache, ScrubStack *s);
    void finish(int r) override { }
    void complete(int r) override {
      if (r == -ECANCELED) {
        return;
      }

      stack->scrubs_in_progress--;
      stack->kick_off_scrubs();
      // don't delete self
    }
  private:
    ScrubStack *stack;
  };

  // reference to global cluster log client
  LogChannelRef &clog;

  /// A finisher needed so that we don't re-enter kick_off_scrubs
  Finisher *finisher;

  /// The stack of inodes we want to scrub
  elist<CInode*> inode_stack;
  /// current number of dentries we're actually scrubbing
  int scrubs_in_progress = 0;
  ScrubStack *scrubstack; // hack for dout
  int stack_size = 0;

  C_KickOffScrubs scrub_kick;

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

  /**
   * Put the inode at either the top or bottom of the stack, with
   * the given scrub params, and then try and kick off more scrubbing.
   */
  void enqueue_inode(CInode *in, ScrubHeaderRef& header,
                      MDSContext *on_finish, bool top);
  void _enqueue_inode(CInode *in, CDentry *parent, ScrubHeaderRef& header,
                      MDSContext *on_finish, bool top);
  /**
   * Kick off as many scrubs as are appropriate, based on the current
   * state of the stack.
   */
  void kick_off_scrubs();
  /**
   * Push a inode on top of the stack.
   */
  inline void push_inode(CInode *in);
  /**
   * Push a inode to the bottom of the stack.
   */
  inline void push_inode_bottom(CInode *in);
  /**
   * Pop the given inode off the stack.
   */
  inline void pop_inode(CInode *in);

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
   * Make progress on scrubbing a directory-representing dirfrag and
   * its children..
   *
   * 1) Select the next dirfrag which hasn't been scrubbed, and make progress
   * on it if possible.
   *
   * 2) If not, move on to the next dirfrag and start it up, if any.
   *
   * 3) If waiting for results from dirfrag scrubs, do nothing.
   *
   * 4) If all dirfrags have been scrubbed, scrub my inode.
   *
   * @param in The CInode to scrub as a directory
   * @param added_children set to true if we pushed some of our children
   * onto the ScrubStack
   * @param is_terminal set to true if there are no descendant dentries
   * remaining to start scrubbing.
   * @param done set to true if we and all our children have finished scrubbing
   */
  void scrub_dir_inode(CInode *in, bool *added_children, bool *is_terminal,
                       bool *done);
  /**
   * Make progress on scrubbing a dirfrag. It may return after each of the
   * following steps, but will report making progress on each one.
   *
   * 1) enqueues the next unscrubbed child directory dentry at the
   * top of the stack.
   *
   * 2) Initiates a scrub on the next unscrubbed file dentry
   *
   * If there are scrubs currently in progress on child dentries, no more child
   * dentries to scrub, and this function is invoked, it will report no
   * progress. Try again later.
   *
   */
  void scrub_dirfrag(CDir *dir, ScrubHeaderRef& header,
		     bool *added_children, bool *is_terminal, bool *done);
  /**
   * Scrub a directory-representing dentry.
   *
   * @param in The directory inode we're doing final scrub on.
   */
  void scrub_dir_inode_final(CInode *in);

  /**
   * Get a CDir into memory, and return it if it's already complete.
   * Otherwise, fetch it and kick off scrubbing when done.
   *
   * @param in The Inode to get the next directory from
   * @param new_dir The CDir we're returning to you. NULL if
   * not ready yet or there aren't any.
   * @returns false if you have to wait, true if there's no work
   * left to do (we returned it, or there are none left in this inode).
   */
  bool get_next_cdir(CInode *in, CDir **new_dir);

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
   * Abort pending scrubs for inodes waiting in the inode stack.
   * Completion context is complete with -ECANCELED.
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

  State state = STATE_IDLE;
  bool clear_inode_stack = false;

  // list of pending context completions for asynchronous scrub
  // control operations.
  std::vector<Context *> control_ctxs;

  // list of inodes for which scrub operations are running -- used
  // to diplay out in `scrub status`.
  std::set<CInode *> scrub_origins;
};

#endif /* SCRUBSTACK_H_ */
