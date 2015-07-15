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

#include <iostream>

#include "ScrubStack.h"
#include "common/Finisher.h"
#include "mds/MDSRank.h"
#include "mds/MDCache.h"
#include "mds/MDSContinuation.h"

#define dout_subsys ceph_subsys_mds
#undef dout_prefix
#define dout_prefix _prefix(_dout, scrubstack->mdcache->mds)
static ostream& _prefix(std::ostream *_dout, MDSRank *mds) {
  return *_dout << "mds." << mds->get_nodeid() << ".scrubstack ";
}

void ScrubStack::push_dentry(CDentry *dentry)
{
  dout(20) << "pushing " << *dentry << " on top of ScrubStack" << dendl;
  dentry->get(CDentry::PIN_SCRUBQUEUE);
  if (!dentry->item_scrub.is_on_list()) {
    stack_size++;
  }
  dentry_stack.push_front(&dentry->item_scrub);
}

void ScrubStack::push_dentry_bottom(CDentry *dentry)
{
  dout(20) << "pushing " << *dentry << " on bottom of ScrubStack" << dendl;
  dentry->get(CDentry::PIN_SCRUBQUEUE);
  if (!dentry->item_scrub.is_on_list()) {
    stack_size++;
  }
  dentry_stack.push_back(&dentry->item_scrub);
}

void ScrubStack::pop_dentry(CDentry *dn)
{
  dout(20) << "popping " << *dn
          << " off of ScrubStack" << dendl;
  assert(dn->item_scrub.is_on_list());
  dn->put(CDentry::PIN_SCRUBQUEUE);
  dn->item_scrub.remove_myself();
  stack_size--;
}

void ScrubStack::_enqueue_dentry(CDentry *dn, CDir *parent, bool recursive,
    bool children, ScrubHeaderRefConst header,
    MDSInternalContextBase *on_finish, bool top)
{
  dout(10) << __func__ << " with {" << *dn << "}"
           << ", recursive=" << recursive << ", children=" << children
           << ", on_finish=" << on_finish << ", top=" << top << dendl;
  assert(mdcache->mds->mds_lock.is_locked_by_me());
  dn->scrub_initialize(parent, recursive, children, header, on_finish);
  if (top)
    push_dentry(dn);
  else
    push_dentry_bottom(dn);
}

void ScrubStack::enqueue_dentry(CDentry *dn, bool recursive, bool children,
                                ScrubHeaderRefConst header,
                                 MDSInternalContextBase *on_finish, bool top)
{
  _enqueue_dentry(dn, NULL, recursive, children, header, on_finish, top);
  kick_off_scrubs();
}

void ScrubStack::kick_off_scrubs()
{
  dout(20) << __func__ << " entering with " << scrubs_in_progress << " in "
              "progress and " << stack_size << " in the stack" << dendl;
  bool can_continue = true;
  elist<CDentry*>::iterator i = dentry_stack.begin();
  while (g_conf->mds_max_scrub_ops_in_progress > scrubs_in_progress &&
      can_continue && !i.end()) {
    CDentry *cur = *i;

    dout(20) << __func__ << " examining dentry " << *cur << dendl;

    CInode *curi = cur->get_projected_inode();
    ++i; // we have our reference, push iterator forward

    if (!curi->is_dir()) {
      // it's a regular file, symlink, or hard link
      pop_dentry(cur); // we only touch it this once, so remove from stack

      if (curi->is_file()) {
        scrub_file_dentry(cur);
        can_continue = true;
      } else {
        // drat, we don't do anything with these yet :(
        dout(5) << "skipping scrub on non-dir, non-file dentry "
                << *cur << dendl;
      }
    } else {
      bool completed; // it's done, so pop it off the stack
      bool terminal; // not done, but we can start ops on other directories
      bool progress; // it added new dentries to the top of the stack
      scrub_dir_dentry(cur, &progress, &terminal, &completed);
      if (completed) {
        dout(20) << __func__ << " dir completed" << dendl;
        pop_dentry(cur);
      } else if (progress) {
        dout(20) << __func__ << " dir progressed" << dendl;
        // we added new stuff to top of stack, so reset ourselves there
        i = dentry_stack.begin();
      } else {
        dout(20) << __func__ << " dir no-op" << dendl;
      }

      can_continue = progress || terminal || completed;
    }
  }
}

void ScrubStack::scrub_dir_dentry(CDentry *dn,
                                  bool *added_children,
                                  bool *terminal,
                                  bool *done)
{
  assert(dn != NULL);
  dout(10) << __func__ << *dn << dendl;

  if (!dn->scrub_info()->scrub_children &&
      !dn->scrub_info()->scrub_recursive) {
    // TODO: we have to scrub the local dentry/inode, but nothing else
  }

  *added_children = false;
  *terminal = false;
  *done = false;

  CInode *in = dn->get_projected_inode();
  // FIXME: greg -- is get_version the appropriate version?  (i.e. is scrub_version
  // meant to be an actual version that we're scrubbing, or something else?)
  if (!in->scrub_info()->scrub_in_progress) {
    // We may come through here more than once on our way up and down
    // the stack... or actually is that right?  Should we perhaps
    // only see ourselves once on the way down and once on the way
    // back up again, and not do this?

    // Hmm, bigger problem, we can only actually 

    in->scrub_initialize(in->get_version());
  }

  list<frag_t> scrubbing_frags;
  list<CDir*> scrubbing_cdirs;
  in->scrub_dirfrags_scrubbing(&scrubbing_frags);
  dout(20) << __func__ << " iterating over " << scrubbing_frags.size()
    << " scrubbing frags" << dendl;
  for (list<frag_t>::iterator i = scrubbing_frags.begin();
      i != scrubbing_frags.end();
      ++i) {
    // turn frags into CDir *
    CDir *dir = in->get_dirfrag(*i);
    scrubbing_cdirs.push_back(dir);
    dout(25) << __func__ << " got CDir " << *dir << " presently scrubbing" << dendl;
  }


  dout(20) << __func__ << " consuming from " << scrubbing_cdirs.size()
    << " scrubbing cdirs" << dendl;

  list<CDir*>::iterator i = scrubbing_cdirs.begin();
  bool all_frags_terminal = true;
  bool all_frags_done = true;
  bool finally_done = false;
  while (g_conf->mds_max_scrub_ops_in_progress > scrubs_in_progress) {
    // select next CDir
    CDir *cur_dir = NULL;
    if (i != scrubbing_cdirs.end()) {
      cur_dir = *i;
      ++i;
      dout(20) << __func__ << " got cur_dir = " << *cur_dir << dendl;
    } else {
      bool ready = get_next_cdir(in, &cur_dir);
      dout(20) << __func__ << " get_next_cdir ready=" << ready << dendl;
      if (cur_dir) {
        cur_dir->scrub_initialize();
      }

      if (ready && cur_dir) {
        scrubbing_cdirs.push_back(cur_dir);
      } else if (!ready) {
        // We are waiting for load of a frag
        all_frags_done = false;
        all_frags_terminal = false;
        break;
      } else {
        // Finished with all frags
        break;
      }
    }
    // scrub that CDir
    bool frag_added_children = false;
    bool frag_terminal = true;
    bool frag_done = false;
    scrub_dirfrag(cur_dir, &frag_added_children, &frag_terminal, &frag_done);
    if (frag_done) {
      // FIXME is this right?  Can we end up hitting this more than
      // once and is that a problem?
      cur_dir->inode->scrub_dirfrag_finished(cur_dir->frag);
    }
    *added_children |= frag_added_children;
    all_frags_terminal = all_frags_terminal && frag_terminal;
    all_frags_done = all_frags_done && frag_done;
  }

  dout(20) << "finished looping; all_frags_terminal=" << all_frags_terminal
           << ", all_frags_done=" << all_frags_done << dendl;
  if (all_frags_done) {
    assert (!*added_children); // can't do this if children are still pending
    scrub_dir_dentry_final(dn, &finally_done);
  }

  *terminal = all_frags_terminal;
  *done = all_frags_done && finally_done;
  dout(10) << __func__ << " is exiting " << *terminal << " " << *done << dendl;
  return;
}

bool ScrubStack::get_next_cdir(CInode *in, CDir **new_dir)
{
  dout(20) << __func__ << " on " << *in << dendl;
  frag_t next_frag;
  int r = in->scrub_dirfrag_next(&next_frag);
  assert (r >= 0);

  if (r == 0) {
    // we got a frag to scrub, otherwise it would be ENOENT
    dout(25) << "looking up new frag " << next_frag << dendl;
    CDir *next_dir = in->get_or_open_dirfrag(mdcache, next_frag);
    if (!next_dir->is_complete()) {
      C_KickOffScrubs *c = new C_KickOffScrubs(mdcache->mds, this);
      next_dir->fetch(c);
      dout(25) << "fetching frag from RADOS" << dendl;
      return false;
    }
    *new_dir = next_dir;
    dout(25) << "returning dir " << *new_dir << dendl;
    return true;
  }
  assert(r == ENOENT);
  // there are no dirfrags left
  *new_dir = NULL;
  return true;
}

void ScrubStack::scrub_dir_dentry_final(CDentry *dn, bool *finally_done)
{
  dout(20) << __func__ << *dn << dendl;
  *finally_done =true;
  // FIXME: greg -- is this the right lifetime from the inode's scrub_info?
  CInode *in = dn->get_projected_inode();

  Context *fin = NULL;
  in->scrub_finished(&fin);
  if (fin) {
    // FIXME: pass some error code in?
    finisher->queue(new MDSIOContextWrapper(mdcache->mds, fin), 0);
  }
  return;
}

void ScrubStack::scrub_dirfrag(CDir *dir, bool *added_children,
                               bool *is_terminal, bool *done)
{
  assert(dir != NULL);

  dout(20) << __func__ << " on " << *dir << dendl;
  *added_children = false;
  *is_terminal = false;
  *done = false;

  // XXX HACK get the frag complete before calling
  // scrub intiialize
  if (!dir->is_complete()) {
    dir->fetch(new C_KickOffScrubs(mdcache->mds, this));
    return;
  }

  if (!dir->scrub_info()->directory_scrubbing) {
    // FIXME: greg - the CDir API seems inconsistent here, as
    // scrub_initialize wants the dir to be complete before
    // we start, but scrub_dentry_next handles incompleteness
    // via its EAGAIN path.
    dir->scrub_initialize();
  }

  int r = 0;
  while(r == 0) {
    MDSInternalContext *kick = new C_KickOffScrubs(mdcache->mds, this);
    CDentry *dn = NULL;
    r = dir->scrub_dentry_next(kick, &dn);
    if (r != EAGAIN) {
      // ctx only used by scrub_dentry_next in EAGAIN case
      // FIXME It's kind of annoying to keep allocating and deleting a ctx here
      delete kick;
    }

    if (r == EAGAIN) {
      // Drop out, CDir fetcher will call back our kicker context
      return;
    }

    if (r == ENOENT) {
      // Nothing left to scrub, are we done?
      std::list<CDentry*> scrubbing;
      dir->scrub_dentries_scrubbing(&scrubbing);
      if (scrubbing.empty()) {
        // FIXME: greg: What's the diff meant to be between done and terminal
        *done = true;
        *is_terminal = true;
        continue;
      } else {
        return;
      }
    }

    if (r < 0) {
      // FIXME: how can I handle an error here?  I can't hold someone up
      // forever, but I can't say "sure you're scrubbed"
      //  -- should change scrub_dentry_next definition to never
      //  give out IO errors (handle them some other way)
      //     
      derr << __func__ << " error from scrub_dentry_next: "
           << r << dendl;
      return;
    }

    // scrub_dentry_next defined to only give -ve, EAGAIN, ENOENT, 0 -- we should
    // never get random IO errors here.
    assert(r == 0);


    CDentry *parent_dn = dir->get_inode()->get_parent_dn();
    ScrubHeaderRefConst header = parent_dn->scrub_info()->header;

    // FIXME: Do I *really* need to construct a kick context for every
    // single dentry I'm going to scrub?
    MDSInternalContext *on_d_scrub = new C_KickOffScrubs(mdcache->mds, this);
    _enqueue_dentry(dn,
        dir,
        parent_dn->scrub_info()->scrub_recursive,
        false,  // We are already recursing so scrub_children not meaningful
        header,
        on_d_scrub,
        true);

    *added_children = true;
  }
}

void ScrubStack::scrub_file_dentry(CDentry *dn)
{
  // No-op:
  // TODO: hook into validate_disk_state
  Context *c = NULL;
  dn->scrub_finished(&c);
  if (c) {
    // FIXME: pass some error code in?
    finisher->queue(new MDSIOContextWrapper(mdcache->mds, c), 0);
  }
}

