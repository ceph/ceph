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
#define dout_prefix _prefix(_dout, mdcache->mds)
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

void ScrubStack::dequeue(MDSCacheObject *obj)
{
  dout(20) << "dequeue " << *obj << " from ScrubStack" << dendl;
  ceph_assert(obj->item_scrub.is_on_list());
  obj->put(MDSCacheObject::PIN_SCRUBQUEUE);
  obj->item_scrub.remove_myself();
  stack_size--;
}

void ScrubStack::_enqueue(MDSCacheObject *obj, ScrubHeaderRef& header,
			  MDSContext *on_finish, bool top)
{
  ceph_assert(ceph_mutex_is_locked_by_me(mdcache->mds->mds_lock));
  if (CInode *in = dynamic_cast<CInode*>(obj)) {
    dout(10) << __func__ << " with {" << *in << "}"
	     << ", on_finish=" << on_finish << ", top=" << top << dendl;
    in->scrub_initialize(header, on_finish);
  } else if (CDir *dir = dynamic_cast<CDir*>(obj)) {
    dout(10) << __func__ << " with {" << *dir << "}"
	     << ", on_finish=" << on_finish << ", top=" << top << dendl;
    // The edge directory must be in memory
    dir->auth_pin(this);
    dir->scrub_initialize(header, on_finish);
  } else {
    ceph_assert(0 == "queue dentry to scrub stack");
  }

  dout(20) << "enqueue " << *obj << " to " << (top ? "top" : "bottom") << " of ScrubStack" << dendl;
  if (!obj->item_scrub.is_on_list()) {
    obj->get(MDSCacheObject::PIN_SCRUBQUEUE);
    stack_size++;
  }
  if (top)
    scrub_stack.push_front(&obj->item_scrub);
  else
    scrub_stack.push_back(&obj->item_scrub);
}

void ScrubStack::enqueue(CInode *in, ScrubHeaderRef& header,
                         MDSContext *on_finish, bool top)
{
  // abort in progress
  if (clear_stack) {
    on_finish->complete(-EAGAIN);
    return;
  }

  scrub_origins.emplace(in);
  clog_scrub_summary(in);

  _enqueue(in, header, on_finish, top);
  kick_off_scrubs();
}

void ScrubStack::kick_off_scrubs()
{
  ceph_assert(ceph_mutex_is_locked(mdcache->mds->mds_lock));
  dout(20) << __func__ << ": state=" << state << dendl;

  if (clear_stack || state == STATE_PAUSING || state == STATE_PAUSED) {
    if (scrubs_in_progress == 0) {
      dout(10) << __func__ << ": in progress scrub operations finished, "
               << stack_size << " in the stack" << dendl;

      State final_state = state;
      if (clear_stack) {
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
  elist<MDSCacheObject*>::iterator it = scrub_stack.begin();
  while (g_conf()->mds_max_scrub_ops_in_progress > scrubs_in_progress) {
    if (it.end()) {
      if (scrubs_in_progress == 0) {
        set_state(STATE_IDLE);
      }

      return;
    }

    assert(state == STATE_RUNNING || state == STATE_IDLE);
    set_state(STATE_RUNNING);

    if (CInode *in = dynamic_cast<CInode*>(*it)) {
      dout(20) << __func__ << " examining " << *in << dendl;
      ++it;
      if (!in->is_dir()) {
	// it's a regular file, symlink, or hard link
	dequeue(in); // we only touch it this once, so remove from stack

	if (!in->scrub_info()->on_finish) {
	  scrubs_in_progress++;
	  in->scrub_set_finisher(&scrub_kick);
	}
	scrub_file_inode(in);
      } else {
	bool added_children = false;
	bool done = false; // it's done, so pop it off the stack
	scrub_dir_inode(in, &added_children, &done);
	if (done) {
	  dout(20) << __func__ << " dir inode, done" << dendl;
	  dequeue(in);
	}
	if (added_children) {
	  // dirfrags were queued at top of stack
	  it = scrub_stack.begin();
	}
      }
    } else if (CDir *dir = dynamic_cast<CDir*>(*it)) {
      bool done = false; // it's done, so pop it off the stack
      scrub_dirfrag(dir, &done);
      ++it;
      if (done) {
	dout(20) << __func__ << " dirfrag, done" << dendl;
	dequeue(dir);
      }
    } else {
      ceph_assert(0 == "dentry in scrub stack");
    }
  }
}

void ScrubStack::scrub_dir_inode(CInode *in, bool *added_children, bool *done)
{
  dout(10) << __func__ << " " << *in << dendl;

  ScrubHeaderRef header = in->get_scrub_header();
  ceph_assert(header);

  MDSGatherBuilder gather(g_ceph_context);

  frag_vec_t frags;
  in->dirfragtree.get_leaves(frags);
  dout(20) << __func__ << "recursive mode, frags " << frags << dendl;

  for (auto &fg : frags) {
    CDir *dir = in->get_or_open_dirfrag(mdcache, fg);
    if (!dir->is_auth()) {
      dout(20) << __func__ << " not auth " << *dir  << dendl;
      // no-op
    } else if (!dir->can_auth_pin()) {
      dout(20) << __func__ << " freezing/frozen " << *dir  << dendl;
      dir->add_waiter(CDir::WAIT_UNFREEZE, gather.new_sub());
    } else if (dir->get_version() == 0) {
      dout(20) << __func__ << " barebones " << *dir  << dendl;
      dir->fetch(gather.new_sub());
    }
  }
  if (gather.has_subs()) {
    scrubs_in_progress++;
    gather.set_finisher(&scrub_kick);
    gather.activate();
    return;
  }

  std::vector<CDir*> dfs;
  in->get_dirfrags(dfs);
  for (auto &dir : dfs) {
    if (dir->is_auth()){
      _enqueue(dir, header, nullptr, true);
      *added_children = true;
    } else {
      // FIXME: ask auth mds to scrub
    }
  }

  scrub_dir_inode_final(in);

  *done = true;
  dout(10) << __func__ << " done" << dendl;
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

  if (!in->scrub_info()->on_finish) {
    scrubs_in_progress++;
    in->scrub_set_finisher(&scrub_kick);
  }

  C_InodeValidated *fin = new C_InodeValidated(mdcache->mds, this, in);
  in->validate_disk_state(&fin->result, fin);

  return;
}

void ScrubStack::scrub_dirfrag(CDir *dir, bool *done)
{
  ceph_assert(dir != NULL);

  dout(10) << __func__ << " " << *dir << dendl;

  if (!dir->is_complete()) {
    scrubs_in_progress++;
    dir->fetch(&scrub_kick, true); // already auth pinned
    dout(10) << __func__ << " incomplete, fetching" << dendl;
    return;
  }

  ScrubHeaderRef header = dir->get_scrub_header();
  version_t last_scrub = dir->scrub_info()->last_recursive.version;
  if (header->get_recursive()) {
    for (auto it = dir->begin(); it != dir->end(); ++it) {
      if (it->first.snapid != CEPH_NOSNAP)
	continue;
      CDentry *dn = it->second;
      CDentry::linkage_t *dnl = dn->get_linkage();
      if (dn->get_version() <= last_scrub &&
	  dnl->get_remote_d_type() != DT_DIR &&
	  !header->get_force()) {
	dout(15) << __func__ << " skip dentry " << it->first
		 << ", no change since last scrub" << dendl;
	continue;
      }
      if (dnl->is_primary()) {
	_enqueue(dnl->get_inode(), header, nullptr, false);
      } else if (dnl->is_remote()) {
	// TODO: check remote linkage
      }
    }
  }

  dir->scrub_local();

  MDSContext *c = nullptr;
  dir->scrub_finished(&c);
  dir->auth_unpin(this);
  if (c)
    finisher->queue(new MDSIOContextWrapper(mdcache->mds, c), 0);

  *done = true;
  dout(10) << __func__ << " done" << dendl;
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
    CachedStackStringStream css;
    f.flush(*css);
    derr << __func__ << " scrub error on inode " << *in << ": " << css->strv()
         << dendl;
  } else {
    dout(10) << __func__ << " scrub passed on inode " << *in << dendl;
  }

  MDSContext *c = nullptr;
  in->scrub_finished(&c);
  if (c)
    finisher->queue(new MDSIOContextWrapper(mdcache->mds, c), 0);

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
    if (clear_stack) {
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

    if (clear_stack) {
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

  CachedStackStringStream css;
  bool have_more = false;

  if (state == STATE_IDLE) {
    *css << "no active scrubs running";
  } else if (state == STATE_RUNNING) {
    if (clear_stack) {
      *css << "ABORTING";
    } else {
      *css << "scrub active";
    }
    *css << " (" << stack_size << " inodes in the stack)";
  } else {
    if (state == STATE_PAUSING || state == STATE_PAUSED) {
      have_more = true;
      *css << state;
    }
    if (clear_stack) {
      if (have_more) {
        *css << "+";
      }
      *css << "ABORTING";
    }

    *css << " (" << stack_size << " inodes in the stack)";
  }
  f->dump_string("status", css->strv());

  f->open_object_section("scrubs");
  for (auto &inode : scrub_origins) {
    have_more = false;
    ScrubHeaderRefConst header = inode->get_scrub_header();

    std::string tag(header->get_tag());
    f->open_object_section(tag.c_str()); // scrub id

    f->dump_string("path", scrub_inode_path(inode));

    CachedStackStringStream optcss;
    if (header->get_recursive()) {
      *optcss << "recursive";
      have_more = true;
    }
    if (header->get_repair()) {
      if (have_more) {
        *optcss << ",";
      }
      *optcss << "repair";
      have_more = true;
    }
    if (header->get_force()) {
      if (have_more) {
        *optcss << ",";
      }
      *optcss << "force";
    }

    f->dump_string("options", optcss->strv());
    f->close_section(); // scrub id
  }
  f->close_section(); // scrubs
  f->close_section(); // result
}

void ScrubStack::abort_pending_scrubs() {
  ceph_assert(ceph_mutex_is_locked_by_me(mdcache->mds->mds_lock));
  ceph_assert(clear_stack);

  for (auto it = scrub_stack.begin(); !it.end(); ++it) {
    if (CInode *in = dynamic_cast<CInode*>(*it))  {
      if (in == in->scrub_info()->header->get_origin()) {
	scrub_origins.erase(in);
	clog_scrub_summary(in);
      }
      MDSContext *ctx = nullptr;
      in->scrub_aborted(&ctx);
      if (ctx != nullptr) {
	ctx->complete(-ECANCELED);
      }
    } else if (CDir *dir = dynamic_cast<CDir*>(*it)) {
      MDSContext *ctx = nullptr;
      dir->scrub_aborted(&ctx);
      dir->auth_unpin(this);
      if (ctx != nullptr) {
	ctx->complete(-ECANCELED);
      }
    } else {
      ceph_abort(0 == "dentry in scrub stack");
    }
  }

  stack_size = 0;
  scrub_stack.clear();
  clear_stack = false;
}

void ScrubStack::scrub_abort(Context *on_finish) {
  ceph_assert(ceph_mutex_is_locked_by_me(mdcache->mds->mds_lock));
  ceph_assert(on_finish != nullptr);

  dout(10) << __func__ << ": aborting with " << scrubs_in_progress
           << " scrubs in progress and " << stack_size << " in the"
           << " stack" << dendl;

  clear_stack = true;
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
  if (clear_stack) {
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

  if (clear_stack) {
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
    if (clear_stack) {
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
