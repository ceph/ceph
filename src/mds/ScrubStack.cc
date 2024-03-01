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

#include "ScrubStack.h"
#include "common/Finisher.h"
#include "mds/MDSRank.h"
#include "mds/MDCache.h"
#include "mds/MDSContinuation.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_mds
#undef dout_prefix
#define dout_prefix _prefix(_dout, mdcache->mds)

using namespace std;

static std::ostream& _prefix(std::ostream *_dout, MDSRank *mds) {
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

int ScrubStack::_enqueue(MDSCacheObject *obj, ScrubHeaderRef& header, bool top)
{
  ceph_assert(ceph_mutex_is_locked_by_me(mdcache->mds->mds_lock));
  if (CInode *in = dynamic_cast<CInode*>(obj)) {
    if (in->scrub_is_in_progress()) {
      dout(10) << __func__ << " with {" << *in << "}" << ", already in scrubbing" << dendl;
      return -CEPHFS_EBUSY;
    }
    if(in->state_test(CInode::STATE_PURGING)) {
      dout(10) << *obj << " is purging, skip pushing into scrub stack" << dendl;
      // treating this as success since purge will make sure this inode goes away
      return 0;
    }

    dout(10) << __func__ << " with {" << *in << "}" << ", top=" << top << dendl;
    in->scrub_initialize(header);
  } else if (CDir *dir = dynamic_cast<CDir*>(obj)) {
    if (dir->scrub_is_in_progress()) {
      dout(10) << __func__ << " with {" << *dir << "}" << ", already in scrubbing" << dendl;
      return -CEPHFS_EBUSY;
    }
    if(dir->get_inode()->state_test(CInode::STATE_PURGING)) {
      dout(10) << *obj << " is purging, skip pushing into scrub stack" << dendl;
      // treating this as success since purge will make sure this dir inode goes away
      return 0;
    }

    dout(10) << __func__ << " with {" << *dir << "}" << ", top=" << top << dendl;
    // The edge directory must be in memory
    dir->auth_pin(this);
    dir->scrub_initialize(header);
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
  return 0;
}

int ScrubStack::enqueue(CInode *in, ScrubHeaderRef& header, bool top)
{
  // abort in progress
  if (clear_stack)
    return -CEPHFS_EAGAIN;

  header->set_origin(in->ino());
  auto ret = scrubbing_map.emplace(header->get_tag(), header);
  if (!ret.second) {
    dout(10) << __func__ << " with {" << *in << "}"
	     << ", conflicting tag " << header->get_tag() << dendl;
    return -CEPHFS_EEXIST;
  }
  if (header->get_scrub_mdsdir()) {
    filepath fp;
    mds_rank_t rank;
    rank = mdcache->mds->get_nodeid();
    if(rank >= 0 && rank < MAX_MDS) {
      fp.set_path("", MDS_INO_MDSDIR(rank));
    }
    int r = _enqueue(mdcache->get_inode(fp.get_ino()), header, true);
    if (r < 0) {
      return r;
    }
    //to make sure mdsdir is always on the top
    top = false;
  }
  int r = _enqueue(in, header, top);
  if (r < 0)
    return r;

  clog_scrub_summary(in);

  kick_off_scrubs();
  return 0;
}

void ScrubStack::add_to_waiting(MDSCacheObject *obj)
{
  scrubs_in_progress++;
  obj->item_scrub.remove_myself();
  scrub_waiting.push_back(&obj->item_scrub);
}

void ScrubStack::remove_from_waiting(MDSCacheObject *obj, bool kick)
{
  scrubs_in_progress--;
  if (obj->item_scrub.is_on_list()) {
    obj->item_scrub.remove_myself();
    scrub_stack.push_front(&obj->item_scrub);
    if (kick)
      kick_off_scrubs();
  }
}

class C_RetryScrub : public MDSInternalContext {
public:
  C_RetryScrub(ScrubStack *s, MDSCacheObject *o) :
    MDSInternalContext(s->mdcache->mds), stack(s), obj(o) {
    stack->add_to_waiting(obj);
  }
  void finish(int r) override {
    stack->remove_from_waiting(obj);
  }
private:
  ScrubStack *stack;
  MDSCacheObject *obj;
};

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

      if (!validate_inode_auth(in))
	continue;

      if (!in->is_dir()) {
	// it's a regular file, symlink, or hard link
	dequeue(in); // we only touch it this once, so remove from stack

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
      auto next = it;
      ++next;
      bool done = false; // it's done, so pop it off the stack
      scrub_dirfrag(dir, &done);
      if (done) {
	dout(20) << __func__ << " dirfrag, done" << dendl;
	++it; // child inodes were queued at bottom of stack
	dequeue(dir);
      } else {
	it = next;
      }
    } else {
      ceph_assert(0 == "dentry in scrub stack");
    }
  }
}

bool ScrubStack::validate_inode_auth(CInode *in)
{
  if (in->is_auth()) {
    if (!in->can_auth_pin()) {
      dout(10) << __func__ << " can't auth pin" << dendl;
      in->add_waiter(CInode::WAIT_UNFREEZE, new C_RetryScrub(this, in));
      return false;
    }
    return true;
  } else {
    MDSRank *mds = mdcache->mds;
    if (in->is_ambiguous_auth()) {
      dout(10) << __func__ << " ambiguous auth" << dendl;
      in->add_waiter(MDSCacheObject::WAIT_SINGLEAUTH, new C_RetryScrub(this, in));
    } else if (mds->is_cluster_degraded()) {
      dout(20) << __func__ << " cluster degraded" << dendl;
      mds->wait_for_cluster_recovered(new C_RetryScrub(this, in));
    } else {
      ScrubHeaderRef header = in->get_scrub_header();
      ceph_assert(header);

      auto ret = remote_scrubs.emplace(std::piecewise_construct,
				       std::forward_as_tuple(in),
				       std::forward_as_tuple());
      ceph_assert(ret.second); // FIXME: parallel scrubs?
      auto &scrub_r = ret.first->second;
      scrub_r.tag = header->get_tag();

      mds_rank_t auth = in->authority().first;
      dout(10) << __func__ << " forward to mds." << auth << dendl;
      auto r = make_message<MMDSScrub>(MMDSScrub::OP_QUEUEINO, in->ino(),
				       std::move(in->scrub_queued_frags()),
				       header->get_tag(), header->get_origin(),
				       header->is_internal_tag(), header->get_force(),
				       header->get_recursive(), header->get_repair());
      mdcache->mds->send_message_mds(r, auth);

      scrub_r.gather_set.insert(auth);
      // wait for ACK
      add_to_waiting(in);
    }
    return false;
  }
}

void ScrubStack::scrub_dir_inode(CInode *in, bool *added_children, bool *done)
{
  dout(10) << __func__ << " " << *in << dendl;
  ceph_assert(in->is_auth());
  MDSRank *mds = mdcache->mds;

  ScrubHeaderRef header = in->get_scrub_header();
  ceph_assert(header);

  MDSGatherBuilder gather(g_ceph_context);

  auto &queued = in->scrub_queued_frags();
  std::map<mds_rank_t, fragset_t> scrub_remote;

  frag_vec_t frags;
  in->dirfragtree.get_leaves(frags);
  dout(20) << __func__ << " recursive mode, frags " << frags << dendl;
  for (auto &fg : frags) {
    if (queued.contains(fg))
      continue;
    CDir *dir = in->get_or_open_dirfrag(mdcache, fg);
    if (!dir->is_auth()) {
      if (dir->is_ambiguous_auth()) {
	dout(20) << __func__ << " ambiguous auth " << *dir  << dendl;
	dir->add_waiter(MDSCacheObject::WAIT_SINGLEAUTH, gather.new_sub());
      } else if (mds->is_cluster_degraded()) {
	dout(20) << __func__ << " cluster degraded" << dendl;
	mds->wait_for_cluster_recovered(gather.new_sub());
      } else {
	mds_rank_t auth = dir->authority().first;
	scrub_remote[auth].insert_raw(fg);
      }
    } else if (!dir->can_auth_pin()) {
      dout(20) << __func__ << " freezing/frozen " << *dir  << dendl;
      dir->add_waiter(CDir::WAIT_UNFREEZE, gather.new_sub());
    } else if (dir->get_version() == 0) {
      dout(20) << __func__ << " barebones " << *dir  << dendl;
      dir->fetch_keys({}, gather.new_sub());
    } else {
      _enqueue(dir, header, true);
      queued.insert_raw(dir->get_frag());
      *added_children = true;
    }
  }

  queued.simplify();

  if (gather.has_subs()) {
    gather.set_finisher(new C_RetryScrub(this, in));
    gather.activate();
    return;
  }

  if (!scrub_remote.empty()) {
    auto ret = remote_scrubs.emplace(std::piecewise_construct,
				     std::forward_as_tuple(in),
				     std::forward_as_tuple());
    ceph_assert(ret.second); // FIXME: parallel scrubs?
    auto &scrub_r = ret.first->second;
    scrub_r.tag = header->get_tag();

    for (auto& p : scrub_remote) {
      dout(20) << __func__ << " forward " << p.second  << " to mds." << p.first << dendl;
      auto r = make_message<MMDSScrub>(MMDSScrub::OP_QUEUEDIR, in->ino(),
				       std::move(p.second), header->get_tag(),
				       header->get_origin(), header->is_internal_tag(),
				       header->get_force(), header->get_recursive(),
				       header->get_repair());
      mds->send_message_mds(r, p.first);
      scrub_r.gather_set.insert(p.first);
    }
    // wait for ACKs
    add_to_waiting(in);
    return;
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
  {
    stack->scrubs_in_progress++;
  }
  void finish(int r) override {
    stack->_validate_inode_done(target, r, result);
    stack->scrubs_in_progress--;
    stack->kick_off_scrubs();
  }
};

void ScrubStack::scrub_dir_inode_final(CInode *in)
{
  dout(20) << __func__ << " " << *in << dendl;

  C_InodeValidated *fin = new C_InodeValidated(mdcache->mds, this, in);
  in->validate_disk_state(&fin->result, fin);
  return;
}

void ScrubStack::scrub_dirfrag(CDir *dir, bool *done)
{
  ceph_assert(dir != NULL);

  dout(10) << __func__ << " " << *dir << dendl;

  if (!dir->is_complete()) {
    dir->fetch(new C_RetryScrub(this, dir), true); // already auth pinned
    dout(10) << __func__ << " incomplete, fetching" << dendl;
    return;
  }

  ScrubHeaderRef header = dir->get_scrub_header();
  version_t last_scrub = dir->scrub_info()->last_recursive.version;
  if (header->get_recursive()) {
    auto next_seq = mdcache->get_global_snaprealm()->get_newest_seq()+1;
    for (auto it = dir->begin(); it != dir->end(); /* nop */) {
      auto [dnk, dn] = *it;
      ++it; /* trim (in the future) may remove dentry */

      if (dn->scrub(next_seq)) {
        std::string path;
        dir->get_inode()->make_path_string(path, true);
        clog->warn() << "Scrub error on dentry " << *dn
                     << " see " << g_conf()->name
                     << " log and `damage ls` output for details";
      }

      if (dnk.snapid != CEPH_NOSNAP) {
	continue;
      }

      CDentry::linkage_t *dnl = dn->get_linkage();
      if (dn->get_version() <= last_scrub &&
	  dnl->get_remote_d_type() != DT_DIR &&
	  !header->get_force()) {
	dout(15) << __func__ << " skip dentry " << dnk
		 << ", no change since last scrub" << dendl;
	continue;
      }
      if (dnl->is_primary()) {
	_enqueue(dnl->get_inode(), header, false);
      } else if (dnl->is_remote()) {
	// TODO: check remote linkage
      }
    }
  }

  if (!dir->scrub_local()) {
    std::string path;
    dir->get_inode()->make_path_string(path, true);
    clog->warn() << "Scrub error on dir " << dir->ino()
                 << " (" << path << ") see " << g_conf()->name
                 << " log and `damage ls` output for details";
  }

  dir->scrub_finished();
  dir->auth_unpin(this);

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

  in->scrub_finished();
}

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
    if (scrubbing_map.empty())
      return "idle";
    *cs << "idle+waiting";
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

  if (!scrubbing_map.empty()) {
    *cs << " paths [";
    bool first = true;
    for (auto &p : scrubbing_map) {
      if (!first)
	*cs << ",";
      auto& header = p.second;
      if (CInode *in = mdcache->get_inode(header->get_origin()))
	*cs << scrub_inode_path(in);
      else
	*cs << "#" << header->get_origin();
      first = false;
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
    if (scrubbing_map.empty())
      *css << "no active scrubs running";
    else
      *css << state << " (waiting for more scrubs)";
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

  for (auto& p : scrubbing_map) {
    have_more = false;
    auto& header = p.second;

    std::string tag(header->get_tag());
    f->open_object_section(tag.c_str()); // scrub id

    if (CInode *in = mdcache->get_inode(header->get_origin()))
      f->dump_string("path", scrub_inode_path(in));
    else
      f->dump_stream("path") << "#" << header->get_origin();

    f->dump_string("tag", header->get_tag());

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
    if (header->get_scrub_mdsdir()) {
      if (have_more) {
        *optcss << ",";
      }
      *optcss << "scrub_mdsdir";
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

  auto abort_one = [this](MDSCacheObject *obj) {
    if (CInode *in = dynamic_cast<CInode*>(obj))  {
      in->scrub_aborted();
    } else if (CDir *dir = dynamic_cast<CDir*>(obj)) {
      dir->scrub_aborted();
      dir->auth_unpin(this);
    } else {
      ceph_abort(0 == "dentry in scrub stack");
    }
  };
  for (auto it = scrub_stack.begin(); !it.end(); ++it)
    abort_one(*it);
  for (auto it = scrub_waiting.begin(); !it.end(); ++it)
    abort_one(*it);

  stack_size = 0;
  scrub_stack.clear();
  scrub_waiting.clear();

  for (auto& p : remote_scrubs)
    remove_from_waiting(p.first, false);
  remote_scrubs.clear();

  clear_stack = false;
}

void ScrubStack::send_state_message(int op) {
  MDSRank *mds = mdcache->mds;
  set<mds_rank_t> up_mds;
  mds->get_mds_map()->get_up_mds_set(up_mds);
  for (auto& r : up_mds) {
    if (r == 0)
      continue;
    auto m = make_message<MMDSScrub>(op);
    mds->send_message_mds(m, r);
  }
}

void ScrubStack::scrub_abort(Context *on_finish) {
  ceph_assert(ceph_mutex_is_locked_by_me(mdcache->mds->mds_lock));

  dout(10) << __func__ << ": aborting with " << scrubs_in_progress
           << " scrubs in progress and " << stack_size << " in the"
           << " stack" << dendl;

  if (mdcache->mds->get_nodeid() == 0) {
    scrub_epoch_last_abort = scrub_epoch;
    scrub_any_peer_aborting = true;
    send_state_message(MMDSScrub::OP_ABORT);
  }

  clear_stack = true;
  if (scrub_in_transition_state()) {
    if (on_finish)
      control_ctxs.push_back(on_finish);
    return;
  }

  abort_pending_scrubs();
  if (state != STATE_PAUSED)
    set_state(STATE_IDLE);

  if (on_finish)
    on_finish->complete(0);
}

void ScrubStack::scrub_pause(Context *on_finish) {
  ceph_assert(ceph_mutex_is_locked_by_me(mdcache->mds->mds_lock));

  dout(10) << __func__ << ": pausing with " << scrubs_in_progress
           << " scrubs in progress and " << stack_size << " in the"
           << " stack" << dendl;

  if (mdcache->mds->get_nodeid() == 0)
    send_state_message(MMDSScrub::OP_PAUSE);

  // abort is in progress
  if (clear_stack) {
    if (on_finish)
      on_finish->complete(-CEPHFS_EINVAL);
    return;
  }

  bool done = scrub_in_transition_state();
  if (done) {
    set_state(STATE_PAUSING);
    if (on_finish)
      control_ctxs.push_back(on_finish);
    return;
  }

  set_state(STATE_PAUSED);
  if (on_finish)
    on_finish->complete(0);
}

bool ScrubStack::scrub_resume() {
  ceph_assert(ceph_mutex_is_locked_by_me(mdcache->mds->mds_lock));
  dout(20) << __func__ << ": state=" << state << dendl;

  if (mdcache->mds->get_nodeid() == 0)
    send_state_message(MMDSScrub::OP_RESUME);

  int r = 0;

  if (clear_stack) {
    r = -CEPHFS_EINVAL;
  } else if (state == STATE_PAUSING) {
    set_state(STATE_RUNNING);
    complete_control_contexts(-CEPHFS_ECANCELED);
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
    } else if (in->scrub_is_in_progress()) {
      what = "queued";
    } else {
      what = "completed";
    }
    clog->info() << "scrub " << what << " for path: " << scrub_inode_path(in);
  }

  clog->info() << "scrub summary: " << scrub_summary();
}

void ScrubStack::dispatch(const cref_t<Message> &m)
{
  switch (m->get_type()) {
  case MSG_MDS_SCRUB:
    handle_scrub(ref_cast<MMDSScrub>(m));
    break;

  case MSG_MDS_SCRUB_STATS:
    handle_scrub_stats(ref_cast<MMDSScrubStats>(m));
    break;

  default:
    derr << " scrub stack unknown message " << m->get_type() << dendl_impl;
    ceph_abort_msg("scrub stack unknown message");
  }
}

bool ScrubStack::remove_inode_if_stacked(CInode *in) {
  MDSCacheObject *obj = dynamic_cast<MDSCacheObject*>(in);
  if(obj->item_scrub.is_on_list()) {
    dout(20) << "removing inode " << *in << " from scrub_stack" << dendl;
    obj->put(MDSCacheObject::PIN_SCRUBQUEUE);
    obj->item_scrub.remove_myself();
    stack_size--;
    return true;
  }
  return false;
}

void ScrubStack::handle_scrub(const cref_t<MMDSScrub> &m)
{

  mds_rank_t from = mds_rank_t(m->get_source().num());
  dout(10) << __func__ << " " << *m << " from mds." << from << dendl;

  switch (m->get_op()) {
  case MMDSScrub::OP_QUEUEDIR:
    {
      CInode *diri = mdcache->get_inode(m->get_ino());
      ceph_assert(diri);

      std::vector<CDir*> dfs;
      MDSGatherBuilder gather(g_ceph_context);
      frag_vec_t frags;
      diri->dirfragtree.get_leaves(frags);
      for (const auto& fg : m->get_frags()) {
	for (auto f : frags) {
	  if (!fg.contains(f)) {
	    dout(20) << __func__ << " skipping " << f << dendl;
	    continue;
	  }
	  CDir *dir = diri->get_or_open_dirfrag(mdcache, f);
	  if (!dir) {
	    dout(10) << __func__ << " no frag " << f << dendl;
	    continue;
	  }
	  if (!dir->is_auth()) {
	    dout(10) << __func__ << " not auth " << *dir << dendl;
	    continue;
	  }
	  if (!dir->can_auth_pin()) {
	    dout(10) << __func__ << " can't auth pin " << *dir <<  dendl;
	    dir->add_waiter(CDir::WAIT_UNFREEZE, gather.new_sub());
	    continue;
	  }
	  dfs.push_back(dir);
	}
      }

      if (gather.has_subs()) {
	gather.set_finisher(new C_MDS_RetryMessage(mdcache->mds, m));
	gather.activate();
	return;
      }

      fragset_t queued;
      if (!dfs.empty()) {
	ScrubHeaderRef header;
	if (auto it = scrubbing_map.find(m->get_tag()); it != scrubbing_map.end()) {
	  header = it->second;
	} else {
	  header = std::make_shared<ScrubHeader>(m->get_tag(), m->is_internal_tag(),
						 m->is_force(), m->is_recursive(),
						 m->is_repair());
	  header->set_origin(m->get_origin());
	  scrubbing_map.emplace(header->get_tag(), header);
	}
	for (auto dir : dfs) {
	  queued.insert_raw(dir->get_frag());
	  _enqueue(dir, header, true);
	}
	queued.simplify();
	kick_off_scrubs();
      }

      auto r = make_message<MMDSScrub>(MMDSScrub::OP_QUEUEDIR_ACK, m->get_ino(),
				       std::move(queued), m->get_tag());
      mdcache->mds->send_message_mds(r, from);
    }
    break;
  case MMDSScrub::OP_QUEUEDIR_ACK:
    {
      CInode *diri = mdcache->get_inode(m->get_ino());
      ceph_assert(diri);
      auto it = remote_scrubs.find(diri);
      if (it != remote_scrubs.end() &&
	  m->get_tag() == it->second.tag) {
	if (it->second.gather_set.erase(from)) {
	  auto &queued = diri->scrub_queued_frags();
	  for (auto &fg : m->get_frags())
	    queued.insert_raw(fg);
	  queued.simplify();

	  if (it->second.gather_set.empty()) {
	    remote_scrubs.erase(it);

	    const auto& header = diri->get_scrub_header();
	    header->set_epoch_last_forwarded(scrub_epoch);
	    remove_from_waiting(diri);
	  }
	}
      }
    }
    break;
  case MMDSScrub::OP_QUEUEINO:
    {
      CInode *in = mdcache->get_inode(m->get_ino());
      ceph_assert(in);

      ScrubHeaderRef header;
      if (auto it = scrubbing_map.find(m->get_tag()); it != scrubbing_map.end()) {
	header = it->second;
      } else {
	header = std::make_shared<ScrubHeader>(m->get_tag(), m->is_internal_tag(),
					       m->is_force(), m->is_recursive(),
					       m->is_repair());
	header->set_origin(m->get_origin());
	scrubbing_map.emplace(header->get_tag(), header);
      }

      _enqueue(in, header, true);
      in->scrub_queued_frags() = m->get_frags();
      kick_off_scrubs();

      fragset_t queued;
      auto r = make_message<MMDSScrub>(MMDSScrub::OP_QUEUEINO_ACK, m->get_ino(),
				       std::move(queued), m->get_tag());
      mdcache->mds->send_message_mds(r, from);
    }
    break;
  case MMDSScrub::OP_QUEUEINO_ACK:
    {
      CInode *in = mdcache->get_inode(m->get_ino());
      ceph_assert(in);
      auto it = remote_scrubs.find(in);
      if (it != remote_scrubs.end() &&
	  m->get_tag() == it->second.tag &&
	  it->second.gather_set.erase(from)) {
	ceph_assert(it->second.gather_set.empty());
	remote_scrubs.erase(it);

	remove_from_waiting(in, false);
	dequeue(in);

	const auto& header = in->get_scrub_header();
	header->set_epoch_last_forwarded(scrub_epoch);
	in->scrub_finished();

	kick_off_scrubs();
      }
    }
    break;
  case MMDSScrub::OP_ABORT:
    scrub_abort(nullptr);
    break;
  case MMDSScrub::OP_PAUSE:
    scrub_pause(nullptr);
    break;
  case MMDSScrub::OP_RESUME:
    scrub_resume();
    break;
  default:
    derr << " scrub stack unknown scrub operation " << m->get_op() << dendl_impl;
    ceph_abort_msg("scrub stack unknown scrub operation");
  }
}

void ScrubStack::handle_scrub_stats(const cref_t<MMDSScrubStats> &m)
{
  mds_rank_t from = mds_rank_t(m->get_source().num());
  dout(7) << __func__ << " " << *m << " from mds." << from << dendl;

  if (from == 0) {
    if (scrub_epoch != m->get_epoch() - 1) {
      scrub_epoch = m->get_epoch() - 1;
      for (auto& p : scrubbing_map) {
	if (p.second->get_epoch_last_forwarded())
	  p.second->set_epoch_last_forwarded(scrub_epoch);
      }
    }
    bool any_finished = false;
    bool any_repaired = false;
    std::set<std::string> scrubbing_tags;
    for (auto it = scrubbing_map.begin(); it != scrubbing_map.end(); ) {
      auto& header = it->second;
      if (header->get_num_pending() ||
	  header->get_epoch_last_forwarded() >= scrub_epoch) {
	scrubbing_tags.insert(it->first);
	++it;
      } else if (m->is_finished(it->first)) {
	any_finished = true;
	if (header->get_repaired())
	  any_repaired = true;
	scrubbing_map.erase(it++);
      } else {
	++it;
      }
    }

    scrub_epoch = m->get_epoch();

    auto ack = make_message<MMDSScrubStats>(scrub_epoch,
					    std::move(scrubbing_tags), clear_stack);
    mdcache->mds->send_message_mds(ack, 0);

    if (any_finished)
      clog_scrub_summary();
    if (any_repaired)
      mdcache->mds->mdlog->trim_all();
  } else {
    if (scrub_epoch == m->get_epoch() &&
	(size_t)from < mds_scrub_stats.size()) {
      auto& stat = mds_scrub_stats[from];
      stat.epoch_acked = m->get_epoch();
      stat.scrubbing_tags = m->get_scrubbing_tags();
      stat.aborting = m->is_aborting();
    }
  }
}

void ScrubStack::advance_scrub_status()
{
  if (!scrub_any_peer_aborting && scrubbing_map.empty())
    return;

  MDSRank *mds = mdcache->mds;

  set<mds_rank_t> up_mds;
  mds->get_mds_map()->get_up_mds_set(up_mds);
  auto up_max = *up_mds.rbegin();

  bool update_scrubbing = false;
  std::set<std::string> scrubbing_tags;

  if (up_max == 0) {
    update_scrubbing = true;
    scrub_any_peer_aborting = false;
  } else if (mds_scrub_stats.size() > (size_t)(up_max)) {
    bool any_aborting = false;
    bool fully_acked = true;
    for (const auto& stat : mds_scrub_stats) {
      if (stat.aborting || stat.epoch_acked <= scrub_epoch_last_abort)
	any_aborting = true;
      if (stat.epoch_acked != scrub_epoch) {
	fully_acked = false;
	continue;
      }
      scrubbing_tags.insert(stat.scrubbing_tags.begin(),
			    stat.scrubbing_tags.end());
    }
    if (!any_aborting)
      scrub_any_peer_aborting = false;
    if (fully_acked) {
      // handle_scrub_stats() reports scrub is still in-progress if it has
      // forwarded any object to other mds since previous epoch. Let's assume,
      // at time 'A', we got scrub stats from all mds for previous epoch. If
      // a scrub is not reported by any mds, we know there is no forward of
      // the scrub since time 'A'. So we can consider the scrub is finished.
      if (scrub_epoch_fully_acked + 1 == scrub_epoch)
	update_scrubbing = true;
      scrub_epoch_fully_acked = scrub_epoch;
    }
  }

  if (mds_scrub_stats.size() != (size_t)up_max + 1)
    mds_scrub_stats.resize((size_t)up_max + 1);
  mds_scrub_stats.at(0).epoch_acked = scrub_epoch + 1;

  bool any_finished = false;
  bool any_repaired = false;

  for (auto it = scrubbing_map.begin(); it != scrubbing_map.end(); ) {
    auto& header = it->second;
    if (header->get_num_pending() ||
	header->get_epoch_last_forwarded() >= scrub_epoch) {
      if (update_scrubbing && up_max != 0)
	scrubbing_tags.insert(it->first);
      ++it;
    } else if (update_scrubbing && !scrubbing_tags.count(it->first)) {
      // no longer being scrubbed globally
      any_finished = true;
      if (header->get_repaired())
	any_repaired = true;
      scrubbing_map.erase(it++);
    } else {
      ++it;
    }
  }

  ++scrub_epoch;

  for (auto& r : up_mds) {
    if (r == 0)
      continue;
    auto m = update_scrubbing ?
	make_message<MMDSScrubStats>(scrub_epoch, scrubbing_tags) :
	make_message<MMDSScrubStats>(scrub_epoch);
    mds->send_message_mds(m, r);
  }

  if (any_finished)
    clog_scrub_summary();
  if (any_repaired)
    mdcache->mds->mdlog->trim_all();
}

void ScrubStack::handle_mds_failure(mds_rank_t mds)
{
  if (mds == 0) {
    scrub_abort(nullptr);
    return;
  }

  bool kick = false;
  for (auto it = remote_scrubs.begin(); it != remote_scrubs.end(); ) {
    if (it->second.gather_set.erase(mds) &&
	it->second.gather_set.empty()) {
      CInode *in = it->first;
      remote_scrubs.erase(it++);
      remove_from_waiting(in, false);
      kick = true;
    } else {
      ++it;
    }
  }
  if (kick)
    kick_off_scrubs();
}
