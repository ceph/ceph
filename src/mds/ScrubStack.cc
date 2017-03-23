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

void ScrubStack::push_inode(CInode *in)
{
  dout(20) << "pushing " << *in << " on top of ScrubStack" << dendl;
  if (!in->item_scrub.is_on_list()) {
    in->get(CInode::PIN_SCRUBQUEUE);
    stack_size++;
  }
  inode_stack.push_front(&in->item_scrub);
}

void ScrubStack::push_inode_bottom(CInode *in)
{
  dout(20) << "pushing " << *in << " on bottom of ScrubStack" << dendl;
  if (!in->item_scrub.is_on_list()) {
    in->get(CInode::PIN_SCRUBQUEUE);
    stack_size++;
  }
  inode_stack.push_back(&in->item_scrub);
}

void ScrubStack::pop_inode(CInode *in)
{
  dout(20) << "popping " << *in
          << " off of ScrubStack" << dendl;
  assert(in->item_scrub.is_on_list());
  in->put(CInode::PIN_SCRUBQUEUE);
  in->item_scrub.remove_myself();
  stack_size--;
}

void ScrubStack::_enqueue_inode(CInode *in, CDentry *parent,
				const ScrubHeaderRefConst& header,
				MDSInternalContextBase *on_finish, bool top)
{
  dout(10) << __func__ << " with {" << *in << "}"
           << ", on_finish=" << on_finish << ", top=" << top << dendl;
  assert(mdcache->mds->mds_lock.is_locked_by_me());
  in->scrub_initialize(parent, header, on_finish);
  if (top)
    push_inode(in);
  else
    push_inode_bottom(in);
}

void ScrubStack::enqueue_inode(CInode *in, const ScrubHeaderRefConst& header,
                               MDSInternalContextBase *on_finish, bool top)
{
  _enqueue_inode(in, NULL, header, on_finish, top);
  kick_off_scrubs();
}

void ScrubStack::kick_off_scrubs()
{
  dout(20) << __func__ << " entering with " << scrubs_in_progress << " in "
              "progress and " << stack_size << " in the stack" << dendl;
  bool can_continue = true;
  elist<CInode*>::iterator i = inode_stack.begin();
  while (g_conf->mds_max_scrub_ops_in_progress > scrubs_in_progress &&
      can_continue && !i.end()) {
    CInode *curi = *i;
    ++i; // we have our reference, push iterator forward

    dout(20) << __func__ << " examining " << *curi << dendl;

    if (!curi->is_dir()) {
      // it's a regular file, symlink, or hard link
      pop_inode(curi); // we only touch it this once, so remove from stack

      if (!curi->scrub_info()->on_finish) {
	scrubs_in_progress++;
	curi->scrub_set_finisher(&scrub_kick);
      }
      scrub_file_inode(curi);
      can_continue = true;
    } else {
      bool completed; // it's done, so pop it off the stack
      bool terminal; // not done, but we can start ops on other directories
      bool progress; // it added new dentries to the top of the stack
      scrub_dir_inode(curi, &progress, &terminal, &completed);
      if (completed) {
        dout(20) << __func__ << " dir completed" << dendl;
        pop_inode(curi);
      } else if (progress) {
        dout(20) << __func__ << " dir progressed" << dendl;
        // we added new stuff to top of stack, so reset ourselves there
        i = inode_stack.begin();
      } else {
        dout(20) << __func__ << " dir no-op" << dendl;
      }

      can_continue = progress || terminal || completed;
    }
  }
}

void ScrubStack::scrub_dir_inode(CInode *in,
                                 bool *added_children,
                                 bool *terminal,
                                 bool *done)
{
  dout(10) << __func__ << *in << dendl;

  *added_children = false;
  bool all_frags_terminal = true;
  bool all_frags_done = true;

  const ScrubHeaderRefConst& header = in->scrub_info()->header;

  if (header->get_recursive()) {
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
      if (dir) {
	scrubbing_cdirs.push_back(dir);
	dout(25) << __func__ << " got CDir " << *dir << " presently scrubbing" << dendl;
      } else {
	in->scrub_dirfrag_finished(*i);
	dout(25) << __func__ << " missing dirfrag " << *i << " skip scrubbing" << dendl;
      }
    }

    dout(20) << __func__ << " consuming from " << scrubbing_cdirs.size()
	     << " scrubbing cdirs" << dendl;

    list<CDir*>::iterator i = scrubbing_cdirs.begin();
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
      scrub_dirfrag(cur_dir, header,
		    &frag_added_children, &frag_terminal, &frag_done);
      if (frag_done) {
	cur_dir->inode->scrub_dirfrag_finished(cur_dir->frag);
      }
      *added_children |= frag_added_children;
      all_frags_terminal = all_frags_terminal && frag_terminal;
      all_frags_done = all_frags_done && frag_done;
    }

    dout(20) << "finished looping; all_frags_terminal=" << all_frags_terminal
	     << ", all_frags_done=" << all_frags_done << dendl;
  } else {
    dout(20) << "!scrub_recursive" << dendl;
  }

  if (all_frags_done) {
    assert (!*added_children); // can't do this if children are still pending

    // OK, so now I can... fire off a validate on the dir inode, and
    // when it completes, come through here again, noticing that we've
    // set a flag to indicate the the validate happened, and 
    scrub_dir_inode_final(in);
  }

  *terminal = all_frags_terminal;
  *done = all_frags_done;
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
      scrubs_in_progress++;
      next_dir->fetch(&scrub_kick);
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

class C_InodeValidated : public MDSInternalContext
{
  public:
    ScrubStack *stack;
    CInode::validated_data result;
    CInode *target;

    C_InodeValidated(MDSRank *mds, ScrubStack *stack_, CInode *target_)
      : MDSInternalContext(mds), stack(stack_), target(target_)
    {}

    void finish(int r)
    {
      stack->_validate_inode_done(target, r, result);
    }
};


void ScrubStack::scrub_dir_inode_final(CInode *in)
{
  dout(20) << __func__ << *in << dendl;

  // Two passes through this function.  First one triggers inode validation,
  // second one sets finally_done
  // FIXME: kind of overloading scrub_in_progress here, using it while
  // dentry is still on stack to indicate that we have finished
  // doing our validate_disk_state on the inode
  // FIXME: the magic-constructing scrub_info() is going to leave
  // an unneeded scrub_infop lying around here
  if (!in->scrub_info()->children_scrubbed) {
    if (!in->scrub_info()->on_finish) {
      scrubs_in_progress++;
      in->scrub_set_finisher(&scrub_kick);
    }

    in->scrub_children_finished();
    C_InodeValidated *fin = new C_InodeValidated(mdcache->mds, this, in);
    in->validate_disk_state(&fin->result, fin);
  }

  return;
}

void ScrubStack::scrub_dirfrag(CDir *dir,
			       const ScrubHeaderRefConst& header,
			       bool *added_children, bool *is_terminal,
			       bool *done)
{
  assert(dir != NULL);

  dout(20) << __func__ << " on " << *dir << dendl;
  *added_children = false;
  *is_terminal = false;
  *done = false;


  if (!dir->scrub_info()->directory_scrubbing) {
    // Get the frag complete before calling
    // scrub initialize, so that it can populate its lists
    // of dentries.
    if (!dir->is_complete()) {
      scrubs_in_progress++;
      dir->fetch(&scrub_kick);
      return;
    }

    dir->scrub_initialize(header);
  }

  int r = 0;
  while(r == 0) {
    CDentry *dn = NULL;
    scrubs_in_progress++;
    r = dir->scrub_dentry_next(&scrub_kick, &dn);
    if (r != EAGAIN) {
      scrubs_in_progress--;
    }

    if (r == EAGAIN) {
      // Drop out, CDir fetcher will call back our kicker context
      dout(20) << __func__ << " waiting for fetch on " << *dir << dendl;
      return;
    }

    if (r == ENOENT) {
      // Nothing left to scrub, are we done?
      std::list<CDentry*> scrubbing;
      dir->scrub_dentries_scrubbing(&scrubbing);
      if (scrubbing.empty()) {
        dout(20) << __func__ << " dirfrag done: " << *dir << dendl;
        // FIXME: greg: What's the diff meant to be between done and terminal
	dir->scrub_finished();
        *done = true;
        *is_terminal = true;
      } else {
        dout(20) << __func__ << " " << scrubbing.size() << " dentries still "
                   "scrubbing in " << *dir << dendl;
      }
      return;
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

    _enqueue_inode(dn->get_projected_inode(), dn, header, NULL, true);

    *added_children = true;
  }
}

void ScrubStack::scrub_file_inode(CInode *in)
{
  C_InodeValidated *fin = new C_InodeValidated(mdcache->mds, this, in);
  // At this stage the DN is already past scrub_initialize, so
  // it's in the cache, it has PIN_SCRUBQUEUE and it is authpinned
  in->validate_disk_state(&fin->result, fin);
}

void ScrubStack::_validate_inode_done(CInode *in, int r,
				      const CInode::validated_data &result)
{
  LogChannelRef clog = mdcache->mds->clog;
  const ScrubHeaderRefConst header = in->scrub_info()->header;

  std::string path;
  if (!result.passed_validation) {
    // Build path string for use in messages
    in->make_path_string(path, true);
  }

  if (result.backtrace.checked && !result.backtrace.passed) {
    // Record backtrace fails as remote linkage damage, as
    // we may not be able to resolve hard links to this inode
    mdcache->mds->damage_table.notify_remote_damaged(in->inode.ino, path);
  } else if (result.inode.checked && !result.inode.passed) {
    // Record damaged inode structures as damaged dentries as
    // that is where they are stored
    auto parent = in->get_projected_parent_dn();
    if (parent) {
      auto dir = parent->get_dir();
      mdcache->mds->damage_table.notify_dentry(
          dir->inode->ino(), dir->frag, parent->last, parent->name, path);
    }
  }

  // Inform the cluster log if we found an error
  if (!result.passed_validation) {
    clog->warn() << "Scrub error on inode " << *in
                 << " (" << path << ") see " << g_conf->name
                 << " log for details";

    // Put the verbose JSON output into the MDS log for later inspection
    JSONFormatter f;
    result.dump(&f);
    std::ostringstream out;
    f.flush(out);
    derr << __func__ << " scrub error on inode " << *in << ": " << out.str()
         << dendl;
  } else {
    dout(10) << __func__ << " scrub passed on inode " << *in << dendl;
  }

  MDSInternalContextBase *c = NULL;
  in->scrub_finished(&c);

  if (!header->get_recursive() && in == header->get_origin()) {
    if (r >= 0) { // we got into the scrubbing dump it
      result.dump(&(header->get_formatter()));
    } else { // we failed the lookup or something; dump ourselves
      header->get_formatter().open_object_section("results");
      header->get_formatter().dump_int("return_code", r);
      header->get_formatter().close_section(); // results
    }
  }
  if (c) {
    finisher->queue(new MDSIOContextWrapper(mdcache->mds, c), 0);
  }
}

ScrubStack::C_KickOffScrubs::C_KickOffScrubs(MDCache *mdcache, ScrubStack *s)
  : MDSInternalContext(mdcache->mds), stack(s) { }
