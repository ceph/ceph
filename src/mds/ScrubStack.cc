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

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_mds
#undef dout_prefix
#define dout_prefix _prefix(_dout, scrubstack->mdcache->mds)
static ostream& _prefix(std::ostream *_dout, MDSRank *mds) {
  return *_dout << "mds." << mds->get_nodeid() << ".scrubstack ";
}

std::ostream &operator<<(std::ostream &os, const ScrubStack::State &state) {
  switch(state) {
  case ScrubStack::STATE_RUNNING:
    os << "RUNNING";
    break;
  case ScrubStack::STATE_IDLE:
    os << "IDLE";
    break;
  case ScrubStack::STATE_PAUSING:
    os << "PAUSING";
    break;
  case ScrubStack::STATE_PAUSED:
    os << "PAUSED";
    break;
  default:
    ceph_abort();
  }

  return os;
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
  ceph_assert(in->item_scrub.is_on_list());
  in->put(CInode::PIN_SCRUBQUEUE);
  in->item_scrub.remove_myself();
  stack_size--;
}

void ScrubStack::_enqueue_inode(CInode *in, CDentry *parent,
				ScrubHeaderRef& header,
				MDSContext *on_finish, bool top)
{
  dout(10) << __func__ << " with {" << *in << "}"
           << ", on_finish=" << on_finish << ", top=" << top << dendl;
  ceph_assert(ceph_mutex_is_locked_by_me(mdcache->mds->mds_lock));
  in->scrub_initialize(parent, header, on_finish);
  if (top)
    push_inode(in);
  else
    push_inode_bottom(in);
}

void ScrubStack::enqueue_inode(CInode *in, ScrubHeaderRef& header,
                               MDSContext *on_finish, bool top)
{
  // abort in progress
  if (clear_inode_stack) {
    on_finish->complete(-EAGAIN);
    return;
  }

  _enqueue_inode(in, NULL, header, on_finish, top);
  kick_off_scrubs();
}

void ScrubStack::kick_off_scrubs()
{
  ceph_assert(ceph_mutex_is_locked(mdcache->mds->mds_lock));
  dout(20) << __func__ << ": state=" << state << dendl;

  if (clear_inode_stack || state == STATE_PAUSING || state == STATE_PAUSED) {
    if (scrubs_in_progress == 0) {
      dout(10) << __func__ << ": in progress scrub operations finished, "
               << stack_size << " in the stack" << dendl;

      State final_state = state;
      if (clear_inode_stack) {
        abort_pending_scrubs();
        final_state = STATE_IDLE;
      }
      if (state == STATE_PAUSING) {
        final_state = STATE_PAUSED;
      }

      set_state(final_state);
      complete_control_contexts(0);
    }

    return;
  }

  dout(20) << __func__ << " entering with " << scrubs_in_progress << " in "
              "progress and " << stack_size << " in the stack" << dendl;
  bool can_continue = true;
  elist<CInode*>::iterator i = inode_stack.begin();
  while (g_conf()->mds_max_scrub_ops_in_progress > scrubs_in_progress &&
         can_continue) {
    if (i.end()) {
      if (scrubs_in_progress == 0) {
        set_state(STATE_IDLE);
      }

      return;
    }

    assert(state == STATE_RUNNING || state == STATE_IDLE);
    set_state(STATE_RUNNING);

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
  dout(10) << __func__ << " " << *in << dendl;

  *added_children = false;
  bool all_frags_terminal = true;
  bool all_frags_done = true;

  ScrubHeaderRef header = in->get_scrub_header();
  ceph_assert(header != nullptr);

  if (header->get_recursive()) {
    frag_vec_t scrubbing_frags;
    std::queue<CDir*> scrubbing_cdirs;
    in->scrub_dirfrags_scrubbing(&scrubbing_frags);
    dout(20) << __func__ << " iterating over " << scrubbing_frags.size()
      << " scrubbing frags" << dendl;
    for (const auto& fg : scrubbing_frags) {
      // turn frags into CDir *
      CDir *dir = in->get_dirfrag(fg);
      if (dir) {
	scrubbing_cdirs.push(dir);
	dout(25) << __func__ << " got CDir " << *dir << " presently scrubbing" << dendl;
      } else {
	in->scrub_dirfrag_finished(fg);
	dout(25) << __func__ << " missing dirfrag " << fg << " skip scrubbing" << dendl;
      }
    }

    dout(20) << __func__ << " consuming from " << scrubbing_cdirs.size()
	     << " scrubbing cdirs" << dendl;

    while (g_conf()->mds_max_scrub_ops_in_progress > scrubs_in_progress) {
      // select next CDir
      CDir *cur_dir = NULL;
      if (!scrubbing_cdirs.empty()) {
	cur_dir = scrubbing_cdirs.front();
        scrubbing_cdirs.pop();
	dout(20) << __func__ << " got cur_dir = " << *cur_dir << dendl;
      } else {
	bool ready = get_next_cdir(in, &cur_dir);
	dout(20) << __func__ << " get_next_cdir ready=" << ready << dendl;

	if (ready && cur_dir) {
	  scrubbing_cdirs.push(cur_dir);
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
    // set a flag to indicate the validate happened, and 
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
  ceph_assert(r == ENOENT);
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

    void finish(int r) override
    {
      stack->_validate_inode_done(target, r, result);
    }
};


void ScrubStack::scrub_dir_inode_final(CInode *in)
{
  dout(20) << __func__ << " " << *in << dendl;

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
			       ScrubHeaderRef& header,
			       bool *added_children, bool *is_terminal,
			       bool *done)
{
  ceph_assert(dir != NULL);

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
      auto&& scrubbing = dir->scrub_dentries_scrubbing();
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

    // scrub_dentry_next defined to only give EAGAIN, ENOENT, 0 -- we should
    // never get random IO errors here.
    ceph_assert(r == 0);

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

  if (result.backtrace.checked && !result.backtrace.passed &&
      !result.backtrace.repaired)
  {
    // Record backtrace fails as remote linkage damage, as
    // we may not be able to resolve hard links to this inode
    mdcache->mds->damage_table.notify_remote_damaged(in->ino(), path);
  } else if (result.inode.checked && !result.inode.passed &&
             !result.inode.repaired) {
    // Record damaged inode structures as damaged dentries as
    // that is where they are stored
    auto parent = in->get_projected_parent_dn();
    if (parent) {
      auto dir = parent->get_dir();
      mdcache->mds->damage_table.notify_dentry(
          dir->inode->ino(), dir->frag, parent->last, parent->get_name(), path);
    }
  }

  // Inform the cluster log if we found an error
  if (!result.passed_validation) {
    if (result.all_damage_repaired()) {
      clog->info() << "Scrub repaired inode " << in->ino()
                   << " (" << path << ")";
    } else {
      clog->warn() << "Scrub error on inode " << in->ino()
                   << " (" << path << ") see " << g_conf()->name
                   << " log and `damage ls` output for details";
    }

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

  MDSContext *c = NULL;
  in->scrub_finished(&c);

  if (in == header->get_origin()) {
    scrub_origins.erase(in);
    clog_scrub_summary(in);
    if (!header->get_recursive()) {
      if (r >= 0) { // we got into the scrubbing dump it
        result.dump(&(header->get_formatter()));
      } else { // we failed the lookup or something; dump ourselves
        header->get_formatter().open_object_section("results");
        header->get_formatter().dump_int("return_code", r);
        header->get_formatter().close_section(); // results
      }
    }
  }
  if (c) {
    finisher->queue(new MDSIOContextWrapper(mdcache->mds, c), 0);
  }
}

ScrubStack::C_KickOffScrubs::C_KickOffScrubs(MDCache *mdcache, ScrubStack *s)
  : MDSInternalContext(mdcache->mds), stack(s) { }

void ScrubStack::complete_control_contexts(int r) {
  ceph_assert(ceph_mutex_is_locked_by_me(mdcache->mds->mds_lock));

  for (auto &ctx : control_ctxs) {
    ctx->complete(r);
  }
  control_ctxs.clear();
}

void ScrubStack::set_state(State next_state) {
    if (state != next_state) {
      dout(20) << __func__ << ", from state=" << state << ", to state="
               << next_state << dendl;
      state = next_state;
      clog_scrub_summary();
    }
}

bool ScrubStack::scrub_in_transition_state() {
  ceph_assert(ceph_mutex_is_locked_by_me(mdcache->mds->mds_lock));
  dout(20) << __func__ << ": state=" << state << dendl;

  // STATE_RUNNING is considered as a transition state so as to
  // "delay" the scrub control operation.
  if (state == STATE_RUNNING || state == STATE_PAUSING) {
    return true;
  }

  return false;
}

std::string_view ScrubStack::scrub_summary() {
  ceph_assert(ceph_mutex_is_locked_by_me(mdcache->mds->mds_lock));

  bool have_more = false;
  CachedStackStringStream cs;

  if (state == STATE_IDLE) {
    return "idle";
  }

  if (state == STATE_RUNNING) {
    if (clear_inode_stack) {
      *cs << "aborting";
    } else {
      *cs << "active";
    }
  } else {
    if (state == STATE_PAUSING) {
      have_more = true;
      *cs << "pausing";
    } else if (state == STATE_PAUSED) {
      have_more = true;
      *cs << "paused";
    }

    if (clear_inode_stack) {
      if (have_more) {
        *cs << "+";
      }
      *cs << "aborting";
    }
  }

  if (!scrub_origins.empty()) {
    *cs << " [paths:";
    for (auto inode = scrub_origins.begin(); inode != scrub_origins.end(); ++inode) {
      if (inode != scrub_origins.begin()) {
        *cs << ",";
      }

      *cs << scrub_inode_path(*inode);
    }
    *cs << "]";
  }

  return cs->strv();
}

void ScrubStack::scrub_status(Formatter *f) {
  ceph_assert(ceph_mutex_is_locked_by_me(mdcache->mds->mds_lock));

  f->open_object_section("result");

  std::stringstream ss;
  bool have_more = false;

  if (state == STATE_IDLE) {
    ss << "no active scrubs running";
  } else if (state == STATE_RUNNING) {
    if (clear_inode_stack) {
      ss << "ABORTING";
    } else {
      ss << "scrub active";
    }
    ss << " (" << stack_size << " inodes in the stack)";
  } else {
    if (state == STATE_PAUSING || state == STATE_PAUSED) {
      have_more = true;
      ss << state;
    }
    if (clear_inode_stack) {
      if (have_more) {
        ss << "+";
      }
      ss << "ABORTING";
    }

    ss << " (" << stack_size << " inodes in the stack)";
  }
  f->dump_string("status", ss.str());

  f->open_object_section("scrubs");
  for (auto &inode : scrub_origins) {
    have_more = false;
    ScrubHeaderRefConst header = inode->get_scrub_header();

    std::string tag(header->get_tag());
    f->open_object_section(tag.c_str()); // scrub id

    f->dump_string("path", scrub_inode_path(inode));

    std::stringstream optss;
    if (header->get_recursive()) {
      optss << "recursive";
      have_more = true;
    }
    if (header->get_repair()) {
      if (have_more) {
        optss << ",";
      }
      optss << "repair";
      have_more = true;
    }
    if (header->get_force()) {
      if (have_more) {
        optss << ",";
      }
      optss << "force";
    }

    f->dump_string("options", optss.str());
    f->close_section(); // scrub id
  }
  f->close_section(); // scrubs
  f->close_section(); // result
}

void ScrubStack::abort_pending_scrubs() {
  ceph_assert(ceph_mutex_is_locked_by_me(mdcache->mds->mds_lock));
  ceph_assert(clear_inode_stack);

  for (auto inode = inode_stack.begin(); !inode.end(); ++inode) {
    CInode *in = *inode;
    if (in == in->scrub_info()->header->get_origin()) {
      scrub_origins.erase(in);
      clog_scrub_summary(in);
    }

    MDSContext *ctx = nullptr;
    in->scrub_aborted(&ctx);
    if (ctx != nullptr) {
      ctx->complete(-ECANCELED);
    }
  }

  stack_size = 0;
  inode_stack.clear();
  clear_inode_stack = false;
}

void ScrubStack::scrub_abort(Context *on_finish) {
  ceph_assert(ceph_mutex_is_locked_by_me(mdcache->mds->mds_lock));
  ceph_assert(on_finish != nullptr);

  dout(10) << __func__ << ": aborting with " << scrubs_in_progress
           << " scrubs in progress and " << stack_size << " in the"
           << " stack" << dendl;

  clear_inode_stack = true;
  if (scrub_in_transition_state()) {
    control_ctxs.push_back(on_finish);
    return;
  }

  abort_pending_scrubs();
  if (state != STATE_PAUSED) {
    set_state(STATE_IDLE);
  }
  on_finish->complete(0);
}

void ScrubStack::scrub_pause(Context *on_finish) {
  ceph_assert(ceph_mutex_is_locked_by_me(mdcache->mds->mds_lock));
  ceph_assert(on_finish != nullptr);

  dout(10) << __func__ << ": pausing with " << scrubs_in_progress
           << " scrubs in progress and " << stack_size << " in the"
           << " stack" << dendl;

  // abort is in progress
  if (clear_inode_stack) {
    on_finish->complete(-EINVAL);
    return;
  }

  bool done = scrub_in_transition_state();
  if (done) {
    set_state(STATE_PAUSING);
    control_ctxs.push_back(on_finish);
    return;
  }

  set_state(STATE_PAUSED);
  on_finish->complete(0);
}

bool ScrubStack::scrub_resume() {
  ceph_assert(ceph_mutex_is_locked_by_me(mdcache->mds->mds_lock));
  dout(20) << __func__ << ": state=" << state << dendl;

  int r = 0;

  if (clear_inode_stack) {
    r = -EINVAL;
  } else if (state == STATE_PAUSING) {
    set_state(STATE_RUNNING);
    complete_control_contexts(-ECANCELED);
  } else if (state == STATE_PAUSED) {
    set_state(STATE_RUNNING);
    kick_off_scrubs();
  }

  return r;
}

// send current scrub summary to cluster log
void ScrubStack::clog_scrub_summary(CInode *in) {
  if (in) {
    std::string what;
    if (clear_inode_stack) {
      what = "aborted";
    } else if (scrub_origins.count(in)) {
      what = "queued";
    } else {
      what = "completed";
    }
    clog->info() << "scrub " << what << " for path: " << scrub_inode_path(in);
  }

  clog->info() << "scrub summary: " << scrub_summary();
}
