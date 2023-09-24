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

#include "common/config.h"
#include "osdc/Journaler.h"
#include "events/ESubtreeMap.h"
#include "events/ESession.h"
#include "events/ESessions.h"

#include "events/EMetaBlob.h"
#include "events/EResetJournal.h"
#include "events/ENoOp.h"

#include "events/EUpdate.h"
#include "events/EPeerUpdate.h"
#include "events/EOpen.h"
#include "events/ECommitted.h"
#include "events/EPurged.h"

#include "events/EExport.h"
#include "events/EImportStart.h"
#include "events/EImportFinish.h"
#include "events/EFragment.h"

#include "events/ETableClient.h"
#include "events/ETableServer.h"
#include "events/ESegment.h"
#include "events/ELid.h"

#include "include/stringify.h"

#include "LogSegment.h"

#include "MDSRank.h"
#include "MDLog.h"
#include "MDCache.h"
#include "Server.h"
#include "Migrator.h"
#include "Mutation.h"

#include "InoTable.h"
#include "MDSTableClient.h"
#include "MDSTableServer.h"

#include "Locker.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_mds
#undef dout_prefix
#define dout_prefix *_dout << "mds." << mds->get_nodeid() << ".journal "

using std::list;
using std::map;
using std::ostream;
using std::pair;
using std::set;
using std::string;
using std::vector;

// -----------------------
// LogSegment

struct BatchStoredBacktrace : public MDSIOContext {
  MDSContext *fin;
  std::vector<CInodeCommitOperations> ops_vec;

  BatchStoredBacktrace(MDSRank *m, MDSContext *f,
		       std::vector<CInodeCommitOperations>&& ops) :
    MDSIOContext(m), fin(f), ops_vec(std::move(ops)) {}
  void finish(int r) override {
    for (auto& op : ops_vec) {
      op.in->_stored_backtrace(r, op.version, nullptr);
    }
    fin->complete(r);
  }
  void print(ostream& out) const override {
    out << "batch backtrace_store";
  }
};

struct BatchCommitBacktrace : public Context {
  MDSRank *mds;
  MDSContext *fin;
  std::vector<CInodeCommitOperations> ops_vec;

  BatchCommitBacktrace(MDSRank *m, MDSContext *f,
		       std::vector<CInodeCommitOperations>&& ops) :
    mds(m), fin(f), ops_vec(std::move(ops)) {}
  void finish(int r) override {
    C_GatherBuilder gather(g_ceph_context);

    for (auto &op : ops_vec) {
      op.in->_commit_ops(r, gather, op.ops_vec, op.bt);
      op.ops_vec.clear();
      op.bt.clear();
    }
    ceph_assert(gather.has_subs());
    gather.set_finisher(new C_OnFinisher(
			  new BatchStoredBacktrace(mds, fin, std::move(ops_vec)),
			  mds->finisher));
    gather.activate();
  }
};

void LogSegment::try_to_expire(MDSRankBase *mdsb, MDSGatherBuilder &gather_bld, int op_prio)
{
  MDSRank* mds = MDSRank::from_base(mdsb);
  set<CDir*> commit;

  dout(6) << "LogSegment(" << seq << "/" << offset << ").try_to_expire" << dendl;

  ceph_assert(g_conf()->mds_kill_journal_expire_at != 1);

  // commit dirs
  for (elist<CDir*>::iterator p = new_dirfrags.begin(); !p.end(); ++p) {
    dout(20) << " new_dirfrag " << **p << dendl;
    ceph_assert((*p)->is_auth());
    commit.insert(*p);
  }
  for (elist<CDir*>::iterator p = dirty_dirfrags.begin(); !p.end(); ++p) {
    dout(20) << " dirty_dirfrag " << **p << dendl;
    ceph_assert((*p)->is_auth());
    commit.insert(*p);
  }
  for (elist<CDentry*>::iterator p = dirty_dentries.begin(); !p.end(); ++p) {
    dout(20) << " dirty_dentry " << **p << dendl;
    ceph_assert((*p)->is_auth());
    commit.insert((*p)->get_dir());
  }
  for (elist<CInode*>::iterator p = dirty_inodes.begin(); !p.end(); ++p) {
    dout(20) << " dirty_inode " << **p << dendl;
    ceph_assert((*p)->is_auth());
    if ((*p)->is_base()) {
      (*p)->store(gather_bld.new_sub());
    } else
      commit.insert((*p)->get_parent_dn()->get_dir());
  }

  if (!commit.empty()) {
    for (set<CDir*>::iterator p = commit.begin();
	 p != commit.end();
	 ++p) {
      CDir *dir = *p;
      ceph_assert(dir->is_auth());
      if (dir->can_auth_pin()) {
	dout(15) << "try_to_expire committing " << *dir << dendl;
	dir->commit(0, gather_bld.new_sub(), false, op_prio);
      } else {
	dout(15) << "try_to_expire waiting for unfreeze on " << *dir << dendl;
	dir->add_waiter(CDir::WAIT_UNFREEZE, gather_bld.new_sub());
      }
    }
  }

  // leader ops with possibly uncommitted peers
  for (set<metareqid_t>::iterator p = uncommitted_leaders.begin();
       p != uncommitted_leaders.end();
       ++p) {
    dout(10) << "try_to_expire waiting for peers to ack commit on " << *p << dendl;
    mds->get_cache()->wait_for_uncommitted_leader(*p, gather_bld.new_sub());
  }

  // peer ops that haven't been committed
  for (set<metareqid_t>::iterator p = uncommitted_peers.begin();
       p != uncommitted_peers.end();
       ++p) {
    dout(10) << "try_to_expire waiting for leader to ack OP_FINISH on " << *p << dendl;
    mds->get_cache()->wait_for_uncommitted_peer(*p, gather_bld.new_sub());
  }

  // uncommitted fragments
  for (set<dirfrag_t>::iterator p = uncommitted_fragments.begin();
       p != uncommitted_fragments.end();
       ++p) {
    dout(10) << "try_to_expire waiting for uncommitted fragment " << *p << dendl;
    mds->get_cache()->wait_for_uncommitted_fragment(*p, gather_bld.new_sub());
  }

  // nudge scatterlocks
  for (elist<CInode*>::iterator p = dirty_dirfrag_dir.begin(); !p.end(); ++p) {
    CInode *in = *p;
    dout(10) << "try_to_expire waiting for dirlock flush on " << *in << dendl;
    mds->locker->scatter_nudge(&in->filelock, gather_bld.new_sub());
  }
  for (elist<CInode*>::iterator p = dirty_dirfrag_dirfragtree.begin(); !p.end(); ++p) {
    CInode *in = *p;
    dout(10) << "try_to_expire waiting for dirfragtreelock flush on " << *in << dendl;
    mds->locker->scatter_nudge(&in->dirfragtreelock, gather_bld.new_sub());
  }
  for (elist<CInode*>::iterator p = dirty_dirfrag_nest.begin(); !p.end(); ++p) {
    CInode *in = *p;
    dout(10) << "try_to_expire waiting for nest flush on " << *in << dendl;
    mds->locker->scatter_nudge(&in->nestlock, gather_bld.new_sub());
  }

  ceph_assert(g_conf()->mds_kill_journal_expire_at != 2);

  // open files and snap inodes 
  if (!open_files.empty()) {
    ceph_assert(!mds->get_log()->is_capped()); // hmm FIXME
    EOpen *le = 0;
    LogSegment *ls = mds->get_log()->get_current_segment();
    ceph_assert(ls != this);
    elist<CInode*>::iterator p = open_files.begin(member_offset(CInode, item_open_file));
    while (!p.end()) {
      CInode *in = *p;
      ++p;
      if (in->last != CEPH_NOSNAP && in->is_auth() && !in->client_snap_caps.empty()) {
	// journal snap inodes that need flush. This simplify the mds failover hanlding
	dout(20) << "try_to_expire requeueing snap needflush inode " << *in << dendl;
	if (!le) {
	  le = new EOpen(mds->get_log());
	}
	le->add_clean_inode(in);
	ls->open_files.push_back(&in->item_open_file);
      } else {
	// open files are tracked by open file table, no need to journal them again
	in->item_open_file.remove_myself();
      }
    }
    if (le) {
      mds->get_log()->submit_entry(le);
      mds->get_log()->wait_for_safe(gather_bld.new_sub());
      dout(10) << "try_to_expire waiting for open files to rejournal" << dendl;
    }
  }

  ceph_assert(g_conf()->mds_kill_journal_expire_at != 3);

  size_t count = 0;
  for (elist<CInode*>::iterator it = dirty_parent_inodes.begin(); !it.end(); ++it)
    count++;

  std::vector<CInodeCommitOperations> ops_vec;
  ops_vec.reserve(count);
  // backtraces to be stored/updated
  for (elist<CInode*>::iterator p = dirty_parent_inodes.begin(); !p.end(); ++p) {
    CInode *in = *p;
    ceph_assert(in->is_auth());
    if (in->can_auth_pin()) {
      dout(15) << "try_to_expire waiting for storing backtrace on " << *in << dendl;
      ops_vec.resize(ops_vec.size() + 1);
      in->store_backtrace(ops_vec.back(), op_prio);
    } else {
      dout(15) << "try_to_expire waiting for unfreeze on " << *in << dendl;
      in->add_waiter(CInode::WAIT_UNFREEZE, gather_bld.new_sub());
    }
  }
  if (!ops_vec.empty())
    mds->finisher->queue(new BatchCommitBacktrace(mds, gather_bld.new_sub(), std::move(ops_vec)));

  ceph_assert(g_conf()->mds_kill_journal_expire_at != 4);

  // idalloc
  if (inotablev > mds->inotable->get_committed_version()) {
    dout(10) << "try_to_expire saving inotable table, need " << inotablev
	      << ", committed is " << mds->inotable->get_committed_version()
	      << " (" << mds->inotable->get_committing_version() << ")"
	      << dendl;
    mds->inotable->save(gather_bld.new_sub(), inotablev);
  }

  // sessionmap
  if (sessionmapv > mds->sessionmap.get_committed()) {
    dout(10) << "try_to_expire saving sessionmap, need " << sessionmapv 
	      << ", committed is " << mds->sessionmap.get_committed()
	      << " (" << mds->sessionmap.get_committing() << ")"
	      << dendl;
    mds->sessionmap.save(gather_bld.new_sub(), sessionmapv);
  }

  // updates to sessions for completed_requests
  mds->sessionmap.save_if_dirty(touched_sessions, &gather_bld);
  touched_sessions.clear();

  // pending commit atids
  for (map<int, ceph::unordered_set<version_t> >::iterator p = pending_commit_tids.begin();
       p != pending_commit_tids.end();
       ++p) {
    MDSTableClient *client = mds->get_table_client(p->first);
    ceph_assert(client);
    for (ceph::unordered_set<version_t>::iterator q = p->second.begin();
	 q != p->second.end();
	 ++q) {
      dout(10) << "try_to_expire " << get_mdstable_name(p->first) << " transaction " << *q 
	       << " pending commit (not yet acked), waiting" << dendl;
      ceph_assert(!client->has_committed(*q));
      client->wait_for_ack(*q, gather_bld.new_sub());
    }
  }
  
  // table servers
  for (map<int, version_t>::iterator p = tablev.begin();
       p != tablev.end();
       ++p) {
    MDSTableServer *server = mds->get_table_server(p->first);
    ceph_assert(server);
    if (p->second > server->get_committed_version()) {
      dout(10) << "try_to_expire waiting for " << get_mdstable_name(p->first) 
	       << " to save, need " << p->second << dendl;
      server->save(gather_bld.new_sub());
    }
  }

  // truncating
  for (set<CInode*>::iterator p = truncating_inodes.begin();
       p != truncating_inodes.end();
       ++p) {
    dout(10) << "try_to_expire waiting for truncate of " << **p << dendl;
    (*p)->add_waiter(CInode::WAIT_TRUNC, gather_bld.new_sub());
  }
  // purge inodes
  dout(10) << "try_to_expire waiting for purge of " << purging_inodes << dendl;
  if (purging_inodes.size())
    set_purged_cb(gather_bld.new_sub());
  
  if (gather_bld.has_subs()) {
    dout(6) << "LogSegment(" << seq << "/" << offset << ").try_to_expire waiting" << dendl;
    mds->get_log()->flush();
  } else {
    ceph_assert(g_conf()->mds_kill_journal_expire_at != 5);
    dout(6) << "LogSegment(" << seq << "/" << offset << ").try_to_expire success" << dendl;
  }
}

// -----------------------
// EMetaBlob

void EMetaBlob::add_dir_context(CDir *dir, int mode)
{
  MDSRank *mds = dir->mdcache->mds;

  list<CDentry*> parents;

  // it may be okay not to include the maybe items, if
  //  - we journaled the maybe child inode in this segment
  //  - that subtree turns out to be unambiguously auth
  list<CDentry*> maybe;
  bool maybenot = false;

  while (true) {
    // already have this dir?  (we must always add in order)
    if (lump_map.count(dir->dirfrag())) {
      dout(20) << "EMetaBlob::add_dir_context(" << dir << ") have lump " << dir->dirfrag() << dendl;
      break;
    }

    // stop at root/stray
    CInode *diri = dir->get_inode();
    CDentry *parent = diri->get_projected_parent_dn();

    if (mode == TO_AUTH_SUBTREE_ROOT) {
      // subtree root?
      if (dir->is_subtree_root()) {
	// match logic in MDCache::create_subtree_map()
	if (dir->get_dir_auth().first == mds->get_nodeid()) {
	  mds_authority_t parent_auth = parent ? parent->authority() : CDIR_AUTH_UNDEF;
	  if (parent_auth.first == dir->get_dir_auth().first) {
	    if (parent_auth.second == CDIR_AUTH_UNKNOWN &&
		!dir->is_ambiguous_dir_auth() &&
		!dir->state_test(CDir::STATE_EXPORTBOUND) &&
		!dir->state_test(CDir::STATE_AUXSUBTREE) &&
		!diri->state_test(CInode::STATE_AMBIGUOUSAUTH)) {
	      dout(0) << "EMetaBlob::add_dir_context unexpected subtree " << *dir << dendl;
	      ceph_abort();
	    }
	    dout(20) << "EMetaBlob::add_dir_context(" << dir << ") ambiguous or transient subtree " << dendl;
	  } else {
	    // it's an auth subtree, we don't need maybe (if any), and we're done.
	    dout(20) << "EMetaBlob::add_dir_context(" << dir << ") reached unambig auth subtree, don't need " << maybe
		     << " at " << *dir << dendl;
	    maybe.clear();
	    break;
	  }
	} else {
	  dout(20) << "EMetaBlob::add_dir_context(" << dir << ") reached ambig or !auth subtree, need " << maybe
		   << " at " << *dir << dendl;
	  // we need the maybe list after all!
	  parents.splice(parents.begin(), maybe);
	  maybenot = false;
	}
      }

      // was the inode journaled in this blob?
      if (touched.contains(diri)) {
	dout(20) << "EMetaBlob::add_dir_context(" << dir << ") already have diri this blob " << *diri << dendl;
	break;
      }

      // have we journaled this inode since the last subtree map?
      auto last_major_segment_seq = mds->get_log()->get_last_major_segment_seq();
      if (!maybenot && diri->last_journaled >= last_major_segment_seq) {
	dout(20) << "EMetaBlob::add_dir_context(" << dir << ") already have diri in this segment (" 
		 << diri->last_journaled << " >= " << last_major_segment_seq << "), setting maybenot flag "
		 << *diri << dendl;
	maybenot = true;
      }
    }

    if (!parent)
      break;

    if (maybenot) {
      dout(25) << "EMetaBlob::add_dir_context(" << dir << ")      maybe " << *parent << dendl;
      maybe.push_front(parent);
    } else {
      dout(25) << "EMetaBlob::add_dir_context(" << dir << ") definitely " << *parent << dendl;
      parents.push_front(parent);
    }
    
    dir = parent->get_dir();
  }
  
  parents.splice(parents.begin(), maybe);

  dout(20) << "EMetaBlob::add_dir_context final: " << parents << dendl;
  for (const auto& dentry : parents) {
    ceph_assert(dentry->get_projected_linkage()->is_primary());
    add_dentry(dentry, false);
  }
}

void EMetaBlob::update_segment(LogSegment *ls)
{
  // dirty inode mtimes
  // -> handled directly by Server.cc, replay()

  // alloc table update?
  if (inotablev)
    ls->inotablev = inotablev;
  if (sessionmapv)
    ls->sessionmapv = sessionmapv;

  // truncated inodes
  // -> handled directly by Server.cc

  // client requests
  //  note the newest request per client
  //if (!client_reqs.empty())
    //    ls->last_client_tid[client_reqs.rbegin()->client] = client_reqs.rbegin()->tid);
}

// EMetaBlob::fullbit

void EMetaBlob::fullbit::update_inode(MDSRank *mds, CInode *in)
{
  in->reset_inode(std::move(inode));
  in->reset_xattrs(std::move(xattrs));
  if (in->is_dir()) {
    if (is_export_ephemeral_random()) {
      dout(15) << "random ephemeral pin on " << *in << dendl;
      in->set_ephemeral_pin(false, true);
    }
    in->maybe_export_pin();
    if (!(in->dirfragtree == dirfragtree)) {
      dout(10) << "EMetaBlob::fullbit::update_inode dft " << in->dirfragtree << " -> "
	       << dirfragtree << " on " << *in << dendl;
      in->dirfragtree = std::move(dirfragtree);
      in->force_dirfrags();
      if (in->get_num_dirfrags() && in->authority() == CDIR_AUTH_UNDEF) {
	auto&& ls = in->get_nested_dirfrags();
	for (const auto& dir : ls) {
	  if (dir->get_num_any() == 0 &&
	      mds->get_cache()->can_trim_non_auth_dirfrag(dir)) {
	    dout(10) << " closing empty non-auth dirfrag " << *dir << dendl;
	    in->close_dirfrag(dir->get_frag());
	  }
	}
      }
    }
  } else if (in->is_symlink()) {
    in->symlink = symlink;
  }
  in->reset_old_inodes(std::move(old_inodes));
  if (in->is_any_old_inodes()) {
    snapid_t min_first = in->get_old_inodes()->rbegin()->first + 1;
    if (min_first > in->first)
      in->first = min_first;
  }

  /*
   * we can do this before linking hte inode bc the split_at would
   * be a no-op.. we have no children (namely open snaprealms) to
   * divy up
   */
  in->oldest_snap = oldest_snap;
  in->decode_snap_blob(snapbl);

  /*
   * In case there was anything malformed in the journal that we are
   * replaying, do sanity checks on the inodes we're replaying and
   * go damaged instead of letting any trash into a live cache
   */
  if (in->is_file()) {
    // Files must have valid layouts with a pool set
    if (in->get_inode()->layout.pool_id == -1 ||
	!in->get_inode()->layout.is_valid()) {
      dout(0) << "EMetaBlob.replay invalid layout on ino " << *in
              << ": " << in->get_inode()->layout << dendl;
      CachedStackStringStream css;
      *css << "Invalid layout for inode " << in->ino() << " in journal";
      mds->get_clog_ref()->error() << css->strv();
      mds->damaged();
      ceph_abort();  // Should be unreachable because damaged() calls respawn()
    }
  }
}

/**
 * Get all inodes touched by this metablob.  Includes the 'bits' within
 * dirlumps, and the inodes of the dirs themselves.
 */
void EMetaBlob::get_inodes(
    std::set<inodeno_t> &inodes) const
{
  // For all dirlumps in this metablob
  for (std::map<dirfrag_t, dirlump>::const_iterator i = lump_map.begin(); i != lump_map.end(); ++i) {
    // Record inode of dirlump
    inodeno_t const dir_ino = i->first.ino;
    inodes.insert(dir_ino);

    // Decode dirlump bits
    dirlump const &dl = i->second;
    dl._decode_bits();

    // Record inodes of fullbits
    for (const auto& iter : dl.get_dfull()) {
      inodes.insert(iter.inode->ino);
    }

    // Record inodes of remotebits
    for (const auto& iter : dl.get_dremote()) {
      inodes.insert(iter.ino);
    }
  }
}


/**
 * Get a map of dirfrag to set of dentries in that dirfrag which are
 * touched in this operation.
 */
void EMetaBlob::get_dentries(std::map<dirfrag_t, std::set<std::string> > &dentries) const
{
  for (std::map<dirfrag_t, dirlump>::const_iterator i = lump_map.begin(); i != lump_map.end(); ++i) {
    dirlump const &dl = i->second;
    dirfrag_t const &df = i->first;

    // Get all bits
    dl._decode_bits();

    // For all bits, store dentry
    for (const auto& iter : dl.get_dfull()) {
      dentries[df].insert(iter.dn);
    }
    for (const auto& iter : dl.get_dremote()) {
      dentries[df].insert(iter.dn);
    }
    for (const auto& iter : dl.get_dnull()) {
      dentries[df].insert(iter.dn);
    }
  }
}



/**
 * Calculate all paths that we can infer are touched by this metablob.  Only uses
 * information local to this metablob so it may only be the path within the
 * subtree.
 */
void EMetaBlob::get_paths(
    std::vector<std::string> &paths) const
{
  // Each dentry has a 'location' which is a 2-tuple of parent inode and dentry name
  typedef std::pair<inodeno_t, std::string> Location;

  // Whenever we see a dentry within a dirlump, we remember it as a child of
  // the dirlump's inode
  std::map<inodeno_t, std::vector<std::string> > children;

  // Whenever we see a location for an inode, remember it: this allows us to
  // build a path given an inode
  std::map<inodeno_t, Location> ino_locations;

  // Special case: operations on root inode populate roots but not dirlumps
  if (lump_map.empty() && !roots.empty()) {
    paths.push_back("/");
    return;
  }

  // First pass
  // ==========
  // Build a tiny local metadata cache for the path structure in this metablob
  for (std::map<dirfrag_t, dirlump>::const_iterator i = lump_map.begin(); i != lump_map.end(); ++i) {
    inodeno_t const dir_ino = i->first.ino;
    dirlump const &dl = i->second;
    dl._decode_bits();

    for (const auto& iter : dl.get_dfull()) {
      std::string_view dentry = iter.dn;
      children[dir_ino].emplace_back(dentry);
      ino_locations[iter.inode->ino] = Location(dir_ino, dentry);
    }

    for (const auto& iter : dl.get_dremote()) {
      std::string_view dentry = iter.dn;
      children[dir_ino].emplace_back(dentry);
    }

    for (const auto& iter : dl.get_dnull()) {
      std::string_view dentry = iter.dn;
      children[dir_ino].emplace_back(dentry);
    }
  }

  std::vector<Location> leaf_locations;

  // Second pass
  // ===========
  // Output paths for all childless nodes in the metablob
  for (std::map<dirfrag_t, dirlump>::const_iterator i = lump_map.begin(); i != lump_map.end(); ++i) {
    inodeno_t const dir_ino = i->first.ino;
    dirlump const &dl = i->second;
    dl._decode_bits();

    for (const auto& iter : dl.get_dfull()) {
      std::string_view dentry = iter.dn;
      if (children.find(iter.inode->ino) == children.end()) {
        leaf_locations.push_back(Location(dir_ino, dentry));
      }
    }

    for (const auto& iter : dl.get_dremote()) {
      std::string_view dentry = iter.dn;
      leaf_locations.push_back(Location(dir_ino, dentry));
    }

    for (const auto& iter : dl.get_dnull()) {
      std::string_view dentry = iter.dn;
      leaf_locations.push_back(Location(dir_ino, dentry));
    }
  }

  // For all the leaf locations identified, generate paths
  for (std::vector<Location>::iterator i = leaf_locations.begin(); i != leaf_locations.end(); ++i) {
    Location const &loc = *i;
    std::string path = loc.second;
    inodeno_t ino = loc.first;
    std::map<inodeno_t, Location>::iterator iter = ino_locations.find(ino);
    while(iter != ino_locations.end()) {
      Location const &loc = iter->second;
      if (!path.empty()) {
        path = loc.second + "/" + path;
      } else {
        path = loc.second + path;
      }
      iter = ino_locations.find(loc.first);
    }

    paths.push_back(path);
  }
}

void EMetaBlob::replay(MDSRankBase *mdsb, LogSegment *logseg, int type, MDPeerUpdate *peerup)
{
  MDSRank* mds = MDSRank::from_base(mdsb);
  dout(10) << "EMetaBlob.replay " << lump_map.size() << " dirlumps by " << client_name << dendl;

  ceph_assert(logseg);

  ceph_assert(g_conf()->mds_kill_journal_replay_at != 1);

  for (auto& p : roots) {
    CInode *in = mds->get_cache()->get_inode(p.inode->ino);
    bool isnew = in ? false:true;
    if (!in)
      in = new CInode(mds->get_cache(), false, 2, CEPH_NOSNAP);
    p.update_inode(mds, in);

    if (isnew)
      mds->get_cache()->add_inode(in);
    if (p.is_dirty()) in->_mark_dirty(logseg);
    dout(10) << "EMetaBlob.replay " << (isnew ? " added root ":" updated root ") << *in << dendl;    
  }

  CInode *renamed_diri = 0;
  CDir *olddir = 0;
  if (renamed_dirino) {
    renamed_diri = mds->get_cache()->get_inode(renamed_dirino);
    if (renamed_diri)
      dout(10) << "EMetaBlob.replay renamed inode is " << *renamed_diri << dendl;
    else
      dout(10) << "EMetaBlob.replay don't have renamed ino " << renamed_dirino << dendl;

    int nnull = 0;
    for (const auto& lp : lump_order) {
      dirlump &lump = lump_map[lp];
      if (lump.nnull) {
	dout(10) << "EMetaBlob.replay found null dentry in dir " << lp << dendl;
	nnull += lump.nnull;
      }
    }
    ceph_assert(nnull <= 1);
  }

  // keep track of any inodes we unlink and don't relink elsewhere
  map<CInode*, CDir*> unlinked;
  set<CInode*> linked;

  // walk through my dirs (in order!)
  int count = 0;
  for (const auto& lp : lump_order) {
    dout(10) << "EMetaBlob.replay dir " << lp << dendl;
    dirlump &lump = lump_map[lp];

    // the dir 
    CDir *dir = mds->get_cache()->get_force_dirfrag(lp, true);
    if (!dir) {
      // hmm.  do i have the inode?
      CInode *diri = mds->get_cache()->get_inode((lp).ino);
      if (!diri) {
	if (MDS_INO_IS_MDSDIR(lp.ino)) {
	  ceph_assert(MDS_INO_MDSDIR(mds->get_nodeid()) != lp.ino);
	  diri = mds->get_cache()->create_system_inode(lp.ino, S_IFDIR|0755);
	  diri->state_clear(CInode::STATE_AUTH);
	  dout(10) << "EMetaBlob.replay created base " << *diri << dendl;
	} else {
	  dout(0) << "EMetaBlob.replay missing dir ino  " << lp.ino << dendl;
          mds->get_clog_ref()->error() << "failure replaying journal (EMetaBlob)";
          mds->damaged();
          ceph_abort();  // Should be unreachable because damaged() calls respawn()
	}
      }

      // create the dirfrag
      dir = diri->get_or_open_dirfrag(mds->get_cache(), lp.frag);

      if (MDS_INO_IS_BASE(lp.ino))
	mds->get_cache()->adjust_subtree_auth(dir, CDIR_AUTH_UNDEF);

      dout(10) << "EMetaBlob.replay added dir " << *dir << dendl;  
    }
    dir->reset_fnode(std::move(lump.fnode));
    dir->update_projected_version();

    if (lump.is_importing()) {
      dir->state_set(CDir::STATE_AUTH);
      dir->state_clear(CDir::STATE_COMPLETE);
    }
    if (lump.is_dirty()) {
      dir->_mark_dirty(logseg);

      if (!(dir->get_fnode()->rstat == dir->get_fnode()->accounted_rstat)) {
	dout(10) << "EMetaBlob.replay      dirty nestinfo on " << *dir << dendl;
	mds->locker->mark_updated_scatterlock(&dir->inode->nestlock);
	logseg->dirty_dirfrag_nest.push_back(&dir->inode->item_dirty_dirfrag_nest);
      } else {
	dout(10) << "EMetaBlob.replay      clean nestinfo on " << *dir << dendl;
      }
      if (!(dir->get_fnode()->fragstat == dir->get_fnode()->accounted_fragstat)) {
	dout(10) << "EMetaBlob.replay      dirty fragstat on " << *dir << dendl;
	mds->locker->mark_updated_scatterlock(&dir->inode->filelock);
	logseg->dirty_dirfrag_dir.push_back(&dir->inode->item_dirty_dirfrag_dir);
      } else {
	dout(10) << "EMetaBlob.replay      clean fragstat on " << *dir << dendl;
      }
    }
    if (lump.is_dirty_dft()) {
      dout(10) << "EMetaBlob.replay      dirty dirfragtree on " << *dir << dendl;
      dir->state_set(CDir::STATE_DIRTYDFT);
      mds->locker->mark_updated_scatterlock(&dir->inode->dirfragtreelock);
      logseg->dirty_dirfrag_dirfragtree.push_back(&dir->inode->item_dirty_dirfrag_dirfragtree);
    }
    if (lump.is_new())
      dir->mark_new(logseg);
    if (lump.is_complete())
      dir->mark_complete();
    
    dout(10) << "EMetaBlob.replay updated dir " << *dir << dendl;  

    // decode bits
    lump._decode_bits();

    // full dentry+inode pairs
    for (auto& fb : lump._get_dfull()) {
      CDentry *dn = dir->lookup_exact_snap(fb.dn, fb.dnlast);
      if (!dn) {
	dn = dir->add_null_dentry(fb.dn, fb.dnfirst, fb.dnlast);
	dn->set_version(fb.dnv);
	if (fb.is_dirty()) dn->_mark_dirty(logseg);
	dout(10) << "EMetaBlob.replay added (full) " << *dn << dendl;
      } else {
	dn->set_version(fb.dnv);
	if (fb.is_dirty()) dn->_mark_dirty(logseg);
	dout(10) << "EMetaBlob.replay for [" << fb.dnfirst << "," << fb.dnlast << "] had " << *dn << dendl;
	dn->first = fb.dnfirst;
	ceph_assert(dn->last == fb.dnlast);
      }
      if (lump.is_importing())
	dn->mark_auth();

      CInode *in = mds->get_cache()->get_inode(fb.inode->ino, fb.dnlast);
      if (!in) {
	in = new CInode(mds->get_cache(), dn->is_auth(), fb.dnfirst, fb.dnlast);
	fb.update_inode(mds, in);
	mds->get_cache()->add_inode(in);
	if (!dn->get_linkage()->is_null()) {
	  if (dn->get_linkage()->is_primary()) {
	    unlinked[dn->get_linkage()->get_inode()] = dir;
	    CachedStackStringStream css;
	    *css << "EMetaBlob.replay FIXME had dentry linked to wrong inode " << *dn
	       << " " << *dn->get_linkage()->get_inode() << " should be " << in->ino();
	    dout(0) << css->strv() << dendl;
	    mds->get_clog_ref()->warn() << css->strv();
	  }
	  dir->unlink_inode(dn, false);
	}
	if (unlinked.count(in))
	  linked.insert(in);
	dir->link_primary_inode(dn, in);
	dout(10) << "EMetaBlob.replay added " << *in << dendl;
      } else {
	in->first = fb.dnfirst;
	fb.update_inode(mds, in);
	if (dn->get_linkage()->get_inode() != in && in->get_parent_dn()) {
	  dout(10) << "EMetaBlob.replay unlinking " << *in << dendl;
	  unlinked[in] = in->get_parent_dir();
	  in->get_parent_dir()->unlink_inode(in->get_parent_dn());
	}
	if (dn->get_linkage()->get_inode() != in) {
	  if (!dn->get_linkage()->is_null()) { // note: might be remote.  as with stray reintegration.
	    if (dn->get_linkage()->is_primary()) {
	      unlinked[dn->get_linkage()->get_inode()] = dir;
	      CachedStackStringStream css;
	      *css << "EMetaBlob.replay FIXME had dentry linked to wrong inode " << *dn
		 << " " << *dn->get_linkage()->get_inode() << " should be " << in->ino();
	      dout(0) << css->strv() << dendl;
	      mds->get_clog_ref()->warn() << css->strv();
	    }
	    dir->unlink_inode(dn, false);
	  }
	  if (unlinked.count(in))
	    linked.insert(in);
	  dir->link_primary_inode(dn, in);
	  dout(10) << "EMetaBlob.replay linked " << *in << dendl;
	} else {
	  dout(10) << "EMetaBlob.replay for [" << fb.dnfirst << "," << fb.dnlast << "] had " << *in << dendl;
	}
	ceph_assert(in->first == fb.dnfirst ||
	       (in->is_multiversion() && in->first > fb.dnfirst));
      }
      if (fb.is_dirty())
	in->_mark_dirty(logseg);
      if (fb.is_dirty_parent())
	in->mark_dirty_parent(logseg, fb.is_dirty_pool());
      if (fb.need_snapflush())
	logseg->open_files.push_back(&in->item_open_file);
      if (dn->is_auth())
	in->state_set(CInode::STATE_AUTH);
      else
	in->state_clear(CInode::STATE_AUTH);
      ceph_assert(g_conf()->mds_kill_journal_replay_at != 2);

      {
        auto do_corruption = mds->get_inject_journal_corrupt_dentry_first();
        if (unlikely(do_corruption > 0.0)) {
          auto r = ceph::util::generate_random_number(0.0, 1.0);
          if (r < do_corruption) {
            dout(0) << "corrupting dn: " << *dn << dendl;
            dn->first = -10;
          }
        }
      }

      if (!(++count % mds->heartbeat_reset_grace()))
        mds->heartbeat_reset();
    }

    // remote dentries
    for (const auto& rb : lump.get_dremote()) {
      CDentry *dn = dir->lookup_exact_snap(rb.dn, rb.dnlast);
      if (!dn) {
	dn = dir->add_remote_dentry(rb.dn, rb.ino, rb.d_type, mempool::mds_co::string(rb.alternate_name), rb.dnfirst, rb.dnlast);
	dn->set_version(rb.dnv);
	if (rb.dirty) dn->_mark_dirty(logseg);
	dout(10) << "EMetaBlob.replay added " << *dn << dendl;
      } else {
	if (!dn->get_linkage()->is_null()) {
	  dout(10) << "EMetaBlob.replay unlinking " << *dn << dendl;
	  if (dn->get_linkage()->is_primary()) {
	    unlinked[dn->get_linkage()->get_inode()] = dir;
	    CachedStackStringStream css;
	    *css << "EMetaBlob.replay FIXME had dentry linked to wrong inode " << *dn
	       << " " << *dn->get_linkage()->get_inode() << " should be remote " << rb.ino;
	    dout(0) << css->strv() << dendl;
	  }
	  dir->unlink_inode(dn, false);
	}
        dn->set_alternate_name(mempool::mds_co::string(rb.alternate_name));
	dir->link_remote_inode(dn, rb.ino, rb.d_type);
	dn->set_version(rb.dnv);
	if (rb.dirty) dn->_mark_dirty(logseg);
	dout(10) << "EMetaBlob.replay for [" << rb.dnfirst << "," << rb.dnlast << "] had " << *dn << dendl;
	dn->first = rb.dnfirst;
	ceph_assert(dn->last == rb.dnlast);
      }
      if (lump.is_importing())
	dn->mark_auth();

      if (!(++count % mds->heartbeat_reset_grace()))
        mds->heartbeat_reset();
    }

    // null dentries
    for (const auto& nb : lump.get_dnull()) {
      CDentry *dn = dir->lookup_exact_snap(nb.dn, nb.dnlast);
      if (!dn) {
	dn = dir->add_null_dentry(nb.dn, nb.dnfirst, nb.dnlast);
	dn->set_version(nb.dnv);
	if (nb.dirty) dn->_mark_dirty(logseg);
	dout(10) << "EMetaBlob.replay added (nullbit) " << *dn << dendl;
      } else {
	dn->first = nb.dnfirst;
	if (!dn->get_linkage()->is_null()) {
	  dout(10) << "EMetaBlob.replay unlinking " << *dn << dendl;
	  CInode *in = dn->get_linkage()->get_inode();
	  // For renamed inode, We may call CInode::force_dirfrag() later.
	  // CInode::force_dirfrag() doesn't work well when inode is detached
	  // from the hierarchy.
	  if (!renamed_diri || renamed_diri != in) {
	    if (dn->get_linkage()->is_primary())
	      unlinked[in] = dir;
	    dir->unlink_inode(dn);
	  }
	}
	dn->set_version(nb.dnv);
	if (nb.dirty) dn->_mark_dirty(logseg);
	dout(10) << "EMetaBlob.replay had " << *dn << dendl;
	ceph_assert(dn->last == nb.dnlast);
      }
      olddir = dir;
      if (lump.is_importing())
	dn->mark_auth();

      // Make null dentries the first things we trim
      dout(10) << "EMetaBlob.replay pushing to bottom of lru " << *dn << dendl;

      if (!(++count % mds->heartbeat_reset_grace()))
        mds->heartbeat_reset();
    }
  }

  ceph_assert(g_conf()->mds_kill_journal_replay_at != 3);

  if (renamed_dirino) {
    if (renamed_diri) {
      ceph_assert(unlinked.count(renamed_diri));
      ceph_assert(linked.count(renamed_diri));
      olddir = unlinked[renamed_diri];
    } else {
      // we imported a diri we haven't seen before
      renamed_diri = mds->get_cache()->get_inode(renamed_dirino);
      ceph_assert(renamed_diri);  // it was in the metablob
    }

    if (olddir) {
      if (olddir->authority() != CDIR_AUTH_UNDEF &&
	  renamed_diri->authority() == CDIR_AUTH_UNDEF) {
	ceph_assert(peerup); // auth to non-auth, must be peer prepare
        frag_vec_t leaves;
	renamed_diri->dirfragtree.get_leaves(leaves);
	for (const auto& leaf : leaves) {
	  CDir *dir = renamed_diri->get_dirfrag(leaf);
	  ceph_assert(dir);
	  if (dir->get_dir_auth() == CDIR_AUTH_UNDEF)
	    // preserve subtree bound until peer commit
	    peerup->olddirs.insert(dir->inode);
	  else
	    dir->state_set(CDir::STATE_AUTH);

          if (!(++count % mds->heartbeat_reset_grace()))
            mds->heartbeat_reset();
	}
      }

      mds->get_cache()->adjust_subtree_after_rename(renamed_diri, olddir, false);
      
      // see if we can discard the subtree we renamed out of
      CDir *root = mds->get_cache()->get_subtree_root(olddir);
      if (root->get_dir_auth() == CDIR_AUTH_UNDEF) {
	if (peerup) // preserve the old dir until peer commit
	  peerup->olddirs.insert(olddir->inode);
	else
	  mds->get_cache()->try_trim_non_auth_subtree(root);
      }
    }

    // if we are the srci importer, we'll also have some dirfrags we have to open up...
    if (renamed_diri->authority() != CDIR_AUTH_UNDEF) {
      for (const auto& p : renamed_dir_frags) {
	CDir *dir = renamed_diri->get_dirfrag(p);
	if (dir) {
	  // we already had the inode before, and we already adjusted this subtree accordingly.
	  dout(10) << " already had+adjusted rename import bound " << *dir << dendl;
	  ceph_assert(olddir); 
	  continue;
	}
	dir = renamed_diri->get_or_open_dirfrag(mds->get_cache(), p);
	dout(10) << " creating new rename import bound " << *dir << dendl;
	dir->state_clear(CDir::STATE_AUTH);
	mds->get_cache()->adjust_subtree_auth(dir, CDIR_AUTH_UNDEF);

        if (!(++count % mds->heartbeat_reset_grace()))
          mds->heartbeat_reset();
      }
    }

    // rename may overwrite an empty directory and move it into stray dir.
    unlinked.erase(renamed_diri);
    for (map<CInode*, CDir*>::iterator p = unlinked.begin(); p != unlinked.end(); ++p) {
      if (!linked.count(p->first))
	continue;
      ceph_assert(p->first->is_dir());
      mds->get_cache()->adjust_subtree_after_rename(p->first, p->second, false);

      if (!(++count % mds->heartbeat_reset_grace()))
        mds->heartbeat_reset();
    }
  }

  if (!unlinked.empty()) {
    for (set<CInode*>::iterator p = linked.begin(); p != linked.end(); ++p)
      unlinked.erase(*p);
    dout(10) << " unlinked set contains " << unlinked << dendl;
    for (map<CInode*, CDir*>::iterator p = unlinked.begin(); p != unlinked.end(); ++p) {
      CInode *in = p->first;
      if (peerup) { // preserve unlinked inodes until peer commit
	peerup->unlinked.insert(in);
	if (in->snaprealm)
	  in->snaprealm->adjust_parent();
      } else
	mds->get_cache()->remove_inode_recursive(in);

      if (!(++count % mds->heartbeat_reset_grace()))
        mds->heartbeat_reset();
    }
  }

  // table client transactions
  for (const auto& p : table_tids) {
    dout(10) << "EMetaBlob.replay noting " << get_mdstable_name(p.first)
	     << " transaction " << p.second << dendl;
    MDSTableClient *client = mds->get_table_client(p.first);
    if (client)
      client->got_journaled_agree(p.second, logseg);

    if (!(++count % mds->heartbeat_reset_grace()))
      mds->heartbeat_reset();
  }

  // opened ino?
  if (opened_ino) {
    CInode *in = mds->get_cache()->get_inode(opened_ino);
    ceph_assert(in);
    dout(10) << "EMetaBlob.replay noting opened inode " << *in << dendl;
    logseg->open_files.push_back(&in->item_open_file);
  }

  bool skip_replaying_inotable = g_conf()->mds_inject_skip_replaying_inotable;

  // allocated_inos
  if (inotablev) {
    if (mds->inotable->get_version() >= inotablev ||
	unlikely(type == EVENT_UPDATE && skip_replaying_inotable)) {
      dout(10) << "EMetaBlob.replay inotable tablev " << inotablev
	       << " <= table " << mds->inotable->get_version() << dendl;
      if (allocated_ino)
        mds->get_cache()->insert_taken_inos(allocated_ino);
    } else {
      dout(10) << "EMetaBlob.replay inotable v " << inotablev
	       << " - 1 == table " << mds->inotable->get_version()
	       << " allocated+used " << allocated_ino
	       << " prealloc " << preallocated_inos
	       << dendl;
      if (allocated_ino)
	mds->inotable->replay_alloc_id(allocated_ino);
      if (preallocated_inos.size())
	mds->inotable->replay_alloc_ids(preallocated_inos);

      // repair inotable updates in case inotable wasn't persist in time
      if (inotablev > mds->inotable->get_version()) {
        mds->get_clog_ref()->error() << "journal replay inotablev mismatch "
            << mds->inotable->get_version() << " -> " << inotablev
            << ", will force replay it.";
        mds->inotable->force_replay_version(inotablev);
      }

      ceph_assert(inotablev == mds->inotable->get_version());
    }
  }
  if (sessionmapv) {
    if (mds->sessionmap.get_version() >= sessionmapv ||
	unlikely(type == EVENT_UPDATE && skip_replaying_inotable)) {
      dout(10) << "EMetaBlob.replay sessionmap v " << sessionmapv
	       << " <= table " << mds->sessionmap.get_version() << dendl;
      if (used_preallocated_ino)
        mds->get_cache()->insert_taken_inos(used_preallocated_ino);
    } else {
      dout(10) << "EMetaBlob.replay sessionmap v " << sessionmapv
	       << ", table " << mds->sessionmap.get_version()
	       << " prealloc " << preallocated_inos
	       << " used " << used_preallocated_ino
	       << dendl;
      Session *session = mds->sessionmap.get_session(client_name);
      if (session) {
	dout(20) << " (session prealloc " << session->info.prealloc_inos << ")" << dendl;
	if (used_preallocated_ino) {
	  if (!session->info.prealloc_inos.empty()) {
	    inodeno_t ino = session->take_ino(used_preallocated_ino);
            dout(5) "received ino " << ino << " from the session" << dendl;
	    ceph_assert(ino == used_preallocated_ino);
	    session->info.prealloc_inos.erase(ino);
	  }
          mds->sessionmap.replay_dirty_session(session);
	}
	if (!preallocated_inos.empty()) {
	  session->free_prealloc_inos.insert(preallocated_inos);
	  session->info.prealloc_inos.insert(preallocated_inos);
          mds->sessionmap.replay_dirty_session(session);
	}
      } else {
	dout(10) << "EMetaBlob.replay no session for " << client_name << dendl;
	if (used_preallocated_ino)
	  mds->sessionmap.replay_advance_version();

	if (!preallocated_inos.empty())
	  mds->sessionmap.replay_advance_version();
      }

      // repair sessionmap updates in case sessionmap wasn't persist in time
      if (sessionmapv > mds->sessionmap.get_version()) {
        mds->get_clog_ref()->error() << "EMetaBlob.replay sessionmapv mismatch "
            << sessionmapv << " -> " << mds->sessionmap.get_version()
            << ", will force replay it.";
        if (g_conf()->mds_wipe_sessions) {
          mds->sessionmap.wipe();
        }
        // force replay sessionmap version
        mds->sessionmap.set_version(sessionmapv);
      }
      ceph_assert(sessionmapv == mds->sessionmap.get_version());
    }
  }

  // truncating inodes
  for (const auto& ino : truncate_start) {
    CInode *in = mds->get_cache()->get_inode(ino);
    ceph_assert(in);
    mds->get_cache()->add_recovered_truncate(in, logseg);

    if (!(++count % mds->heartbeat_reset_grace()))
      mds->heartbeat_reset();
  }
  for (const auto& p : truncate_finish) {
    LogSegment *ls = mds->get_log()->get_segment(p.second);
    if (ls) {
      CInode *in = mds->get_cache()->get_inode(p.first);
      ceph_assert(in);
      mds->get_cache()->remove_recovered_truncate(in, ls);
    }

    if (!(++count % mds->heartbeat_reset_grace()))
      mds->heartbeat_reset();
  }

  // destroyed inodes
  if (!destroyed_inodes.empty()) {
    for (vector<inodeno_t>::iterator p = destroyed_inodes.begin();
	p != destroyed_inodes.end();
	++p) {
      CInode *in = mds->get_cache()->get_inode(*p);
      if (in) {
	dout(10) << "EMetaBlob.replay destroyed " << *p << ", dropping " << *in << dendl;
	CDentry *parent = in->get_parent_dn();
	mds->get_cache()->remove_inode(in);
	if (parent) {
	  dout(10) << "EMetaBlob.replay unlinked from dentry " << *parent << dendl;
	  ceph_assert(parent->get_linkage()->is_null());
	}
      } else {
	dout(10) << "EMetaBlob.replay destroyed " << *p << ", not in cache" << dendl;
      }

      if (!(++count % mds->heartbeat_reset_grace()))
        mds->heartbeat_reset();
    }
    mds->get_cache()->open_file_table.note_destroyed_inos(logseg->seq, destroyed_inodes);
  }

  // client requests
  for (const auto& p : client_reqs) {
    if (p.first.name.is_client()) {
      dout(10) << "EMetaBlob.replay request " << p.first << " trim_to " << p.second << dendl;
      inodeno_t created = allocated_ino ? allocated_ino : used_preallocated_ino;
      // if we allocated an inode, there should be exactly one client request id.
      ceph_assert(created == inodeno_t() || client_reqs.size() == 1);

      Session *session = mds->sessionmap.get_session(p.first.name);
      if (session) {
	session->add_completed_request(p.first.tid, created);
	if (p.second)
	  session->trim_completed_requests(p.second);
      }
    }

    if (!(++count % mds->heartbeat_reset_grace()))
      mds->heartbeat_reset();
  }

  // client flushes
  for (const auto& p : client_flushes) {
    if (p.first.name.is_client()) {
      dout(10) << "EMetaBlob.replay flush " << p.first << " trim_to " << p.second << dendl;
      Session *session = mds->sessionmap.get_session(p.first.name);
      if (session) {
	session->add_completed_flush(p.first.tid);
	if (p.second)
	  session->trim_completed_flushes(p.second);
      }
    }

    if (!(++count % mds->heartbeat_reset_grace()))
      mds->heartbeat_reset();
  }

  // update segment
  update_segment(logseg);

  ceph_assert(g_conf()->mds_kill_journal_replay_at != 4);
}

// -----------------------
// EPurged
void EPurged::update_segment()
{
  if (inos.size() && inotablev)
    get_segment()->inotablev = inotablev;
  return;
}

void EPurged::replay(MDSRankBase *mdsb)
{
  MDSRank* mds = MDSRank::from_base(mdsb);
  if (inos.size()) {
    LogSegment *ls = mds->get_log()->get_segment(seq);
    if (ls)
      ls->purging_inodes.subtract(inos);

    if (mds->inotable->get_version() >= inotablev) {
      dout(10) << "EPurged.replay inotable " << mds->inotable->get_version()
	       << " >= " << inotablev << ", noop" << dendl;
    } else {
      dout(10) << "EPurged.replay inotable " << mds->inotable->get_version()
	       << " < " << inotablev << " " << dendl;
      mds->inotable->replay_release_ids(inos);
      ceph_assert(mds->inotable->get_version() == inotablev);
    }
  }
  update_segment();
}

// -----------------------
// ESession

void ESession::update_segment()
{
  get_segment()->sessionmapv = cmapv;
  if (inos_to_free.size() && inotablev)
    get_segment()->inotablev = inotablev;
}

void ESession::replay(MDSRankBase *mdsb)
{
  MDSRank* mds = MDSRank::from_base(mdsb);
  if (inos_to_purge.size())
    get_segment()->purging_inodes.insert(inos_to_purge);
  
  if (mds->sessionmap.get_version() >= cmapv) {
    dout(10) << "ESession.replay sessionmap " << mds->sessionmap.get_version() 
	     << " >= " << cmapv << ", noop" << dendl;
  } else if (mds->sessionmap.get_version() + 1 == cmapv) {
    dout(10) << "ESession.replay sessionmap " << mds->sessionmap.get_version()
	     << " < " << cmapv << " " << (open ? "open":"close") << " " << client_inst << dendl;
    Session *session;
    if (open) {
      session = mds->sessionmap.get_or_add_session(client_inst);
      mds->sessionmap.set_state(session, Session::STATE_OPEN);
      session->set_client_metadata(client_metadata);
      dout(10) << " opened session " << session->info.inst << dendl;
    } else {
      session = mds->sessionmap.get_session(client_inst.name);
      if (session) { // there always should be a session, but there's a bug
	if (session->get_connection() == NULL) {
	  dout(10) << " removed session " << session->info.inst << dendl;
	  mds->sessionmap.remove_session(session);
          session = NULL;
	} else {
	  session->clear();    // the client has reconnected; keep the Session, but reset
	  dout(10) << " reset session " << session->info.inst << " (they reconnected)" << dendl;
	}
      } else {
	mds->get_clog_ref()->error() << "replayed stray Session close event for " << client_inst
			  << " from time " << stamp << ", ignoring";
      }
    }
    if (session) {
      mds->sessionmap.replay_dirty_session(session);
    } else {
      mds->sessionmap.replay_advance_version();
    }
    ceph_assert(mds->sessionmap.get_version() == cmapv);
  } else {
    mds->get_clog_ref()->error() << "ESession.replay sessionmap v " << cmapv
		       << " - 1 > table " << mds->sessionmap.get_version();
    ceph_assert(g_conf()->mds_wipe_sessions);
    mds->sessionmap.wipe();
    mds->sessionmap.set_version(cmapv);
  }
  
  if (inos_to_free.size() && inotablev) {
    if (mds->inotable->get_version() >= inotablev) {
      dout(10) << "ESession.replay inotable " << mds->inotable->get_version()
	       << " >= " << inotablev << ", noop" << dendl;
    } else {
      dout(10) << "ESession.replay inotable " << mds->inotable->get_version()
	       << " < " << inotablev << " " << (open ? "add":"remove") << dendl;
      ceph_assert(!open);  // for now
      mds->inotable->replay_release_ids(inos_to_free);
      ceph_assert(mds->inotable->get_version() == inotablev);
    }
  }

  update_segment();
}

// -----------------------
// ESessions

void ESessions::update_segment()
{
  get_segment()->sessionmapv = cmapv;
}

void ESessions::replay(MDSRankBase *mdsb)
{
  MDSRank* mds = MDSRank::from_base(mdsb);
  if (mds->sessionmap.get_version() >= cmapv) {
    dout(10) << "ESessions.replay sessionmap " << mds->sessionmap.get_version()
	     << " >= " << cmapv << ", noop" << dendl;
  } else {
    dout(10) << "ESessions.replay sessionmap " << mds->sessionmap.get_version()
	     << " < " << cmapv << dendl;
    mds->sessionmap.replay_open_sessions(cmapv, client_map, client_metadata_map);
  }
  update_segment();
}


// -----------------------
// ETableServer

void ETableServer::update_segment()
{
  get_segment()->tablev[table] = version;
}

void ETableServer::replay(MDSRankBase *mdsb)
{
  MDSRank *mds = MDSRank::from_base(mdsb);
  MDSTableServer *server = mds->get_table_server(table);
  if (!server)
    return;

  if (server->get_version() >= version) {
    dout(10) << "ETableServer.replay " << get_mdstable_name(table)
	     << " " << get_mdstableserver_opname(op)
	     << " event " << version
	     << " <= table " << server->get_version() << dendl;
    return;
  }
  
  dout(10) << " ETableServer.replay " << get_mdstable_name(table)
	   << " " << get_mdstableserver_opname(op)
	   << " event " << version << " - 1 == table " << server->get_version() << dendl;
  ceph_assert(version-1 == server->get_version());

  switch (op) {
  case TABLESERVER_OP_PREPARE: {
    server->_note_prepare(bymds, reqid, true);
    bufferlist out;
    server->_prepare(mutation, reqid, bymds, out);
    mutation = std::move(out);
    break;
  }
  case TABLESERVER_OP_COMMIT:
    server->_commit(tid, ref_t<MMDSTableRequest>());
    server->_note_commit(tid, true);
    break;
  case TABLESERVER_OP_ROLLBACK:
    server->_rollback(tid);
    server->_note_rollback(tid, true);
    break;
  case TABLESERVER_OP_SERVER_UPDATE:
    server->_server_update(mutation);
    server->_note_server_update(mutation, true);
    break;
  default:
    mds->get_clog_ref()->error() << "invalid tableserver op in ETableServer";
    mds->damaged();
    ceph_abort();  // Should be unreachable because damaged() calls respawn()
  }
  
  ceph_assert(version == server->get_version());
  update_segment();
}


// ---------------------
// ETableClient

void ETableClient::replay(MDSRankBase *mds)
{
  dout(10) << " ETableClient.replay " << get_mdstable_name(table)
	   << " op " << get_mdstableserver_opname(op)
	   << " tid " << tid << dendl;
    
  MDSTableClient *client = mds->get_table_client(table);
  if (!client)
    return;

  ceph_assert(op == TABLESERVER_OP_ACK);
  client->got_journaled_ack(tid);
}


// -----------------------
// ESnap
/*
void ESnap::update_segment()
{
  get_segment()->tablev[TABLE_SNAP] = version;
}

void ESnap::replay(MDSRankBase *mdsb)
{
  MDSRank* mds = MDSRank::from_base(mdsb);
  if (mds->snaptable->get_version() >= version) {
    dout(10) << "ESnap.replay event " << version
	     << " <= table " << mds->snaptable->get_version() << dendl;
    return;
  } 
  
  dout(10) << " ESnap.replay event " << version
	   << " - 1 == table " << mds->snaptable->get_version() << dendl;
  ceph_assert(version-1 == mds->snaptable->get_version());

  if (create) {
    version_t v;
    snapid_t s = mds->snaptable->create(snap.dirino, snap.name, snap.stamp, &v);
    ceph_assert(s == snap.snapid);
  } else {
    mds->snaptable->remove(snap.snapid);
  }

  ceph_assert(version == mds->snaptable->get_version());
}
*/



// -----------------------
// EUpdate

void EUpdate::update_segment()
{
  auto&& segment = get_segment();
  metablob.update_segment(segment);

  if (client_map.length())
    segment->sessionmapv = cmapv;

  if (had_peers)
    segment->uncommitted_leaders.insert(reqid);
}

void EUpdate::replay(MDSRankBase *mdsb)
{
  MDSRank* mds = MDSRank::from_base(mdsb);
  auto&& segment = get_segment();
  dout(10) << "EUpdate::replay" << dendl;
  metablob.replay(mds, segment, EVENT_UPDATE);
  
  if (had_peers) {
    dout(10) << "EUpdate.replay " << reqid << " had peers, expecting a matching ECommitted" << dendl;
    segment->uncommitted_leaders.insert(reqid);
    set<mds_rank_t> peers;
    mds->get_cache()->add_uncommitted_leader(reqid, segment, peers, true);
  }
  
  if (client_map.length()) {
    if (mds->sessionmap.get_version() >= cmapv) {
      dout(10) << "EUpdate.replay sessionmap v " << cmapv
	       << " <= table " << mds->sessionmap.get_version() << dendl;
    } else {
      dout(10) << "EUpdate.replay sessionmap " << mds->sessionmap.get_version()
	       << " < " << cmapv << dendl;
      // open client sessions?
      map<client_t,entity_inst_t> cm;
      map<client_t,client_metadata_t> cmm;
      auto blp = client_map.cbegin();
      using ceph::decode;
      decode(cm, blp);
      if (!blp.end())
	decode(cmm, blp);
      mds->sessionmap.replay_open_sessions(cmapv, cm, cmm);
    }
  }
  update_segment();
}


// ------------------------
// EOpen

void EOpen::update_segment()
{
  // ??
}

void EOpen::replay(MDSRankBase *mds)
{
  dout(10) << "EOpen.replay " << dendl;
  auto&& segment = get_segment();
  metablob.replay(mds, segment, EVENT_OPEN);

  // note which segments inodes belong to, so we don't have to start rejournaling them
  for (const auto &ino : inos) {
    CInode *in = mds->get_cache()->get_inode(ino);
    if (!in) {
      dout(0) << "EOpen.replay ino " << ino << " not in metablob" << dendl;
      ceph_assert(in);
    }
    segment->open_files.push_back(&in->item_open_file);
  }
  for (const auto &vino : snap_inos) {
    CInode *in = mds->get_cache()->get_inode(vino);
    if (!in) {
      dout(0) << "EOpen.replay ino " << vino << " not in metablob" << dendl;
      ceph_assert(in);
    }
    segment->open_files.push_back(&in->item_open_file);
  }
}


// -----------------------
// ECommitted

void ECommitted::replay(MDSRankBase *mds)
{
  if (mds->get_cache()->uncommitted_leaders.count(reqid)) {
    dout(10) << "ECommitted.replay " << reqid << dendl;
    mds->get_cache()->uncommitted_leaders[reqid].ls->uncommitted_leaders.erase(reqid);
    mds->get_cache()->uncommitted_leaders.erase(reqid);
  } else {
    dout(10) << "ECommitted.replay " << reqid << " -- didn't see original op" << dendl;
  }
}


// -----------------------
// EPeerUpdate

void EPeerUpdate::replay(MDSRankBase *mds)
{
  MDPeerUpdate *su;
  auto&& segment = get_segment();
  switch (op) {
  case EPeerUpdate::OP_PREPARE:
    dout(10) << "EPeerUpdate.replay prepare " << reqid << " for mds." << leader
	     << ": applying commit, saving rollback info" << dendl;
    su = new MDPeerUpdate(origop, rollback);
    commit.replay(mds, segment, EVENT_PEERUPDATE, su);
    mds->get_cache()->add_uncommitted_peer(reqid, segment, leader, su);
    break;

  case EPeerUpdate::OP_COMMIT:
    dout(10) << "EPeerUpdate.replay commit " << reqid << " for mds." << leader << dendl;
    mds->get_cache()->finish_uncommitted_peer(reqid, false);
    break;

  case EPeerUpdate::OP_ROLLBACK:
    dout(10) << "EPeerUpdate.replay abort " << reqid << " for mds." << leader
	     << ": applying rollback commit blob" << dendl;
    commit.replay(mds, segment, EVENT_PEERUPDATE);
    mds->get_cache()->finish_uncommitted_peer(reqid, false);
    break;

  default:
    mds->get_clog_ref()->error() << "invalid op in EPeerUpdate";
    mds->damaged();
    ceph_abort();  // Should be unreachable because damaged() calls respawn()
  }
}


// -----------------------
// ESubtreeMap

void ESubtreeMap::replay(MDSRankBase *mdsb) 
{
  MDSRank* mds = MDSRank::from_base(mdsb);
  if (expire_pos && expire_pos > mds->get_log()->journaler->get_expire_pos())
    mds->get_log()->journaler->set_expire_pos(expire_pos);

  // suck up the subtree map?
  if (mds->get_cache()->is_subtrees()) {
    dout(10) << "ESubtreeMap.replay -- i already have import map; verifying" << dendl;
    int errors = 0;

    for (map<dirfrag_t, vector<dirfrag_t> >::iterator p = subtrees.begin();
	 p != subtrees.end();
	 ++p) {
      CDir *dir = mds->get_cache()->get_dirfrag(p->first);
      if (!dir) {
	mds->get_clog_ref()->error() << " replayed ESubtreeMap at " << get_start_off()
			  << " subtree root " << p->first << " not in cache";
	++errors;
	continue;
      }
      
      if (!mds->get_cache()->is_subtree(dir)) {
	mds->get_clog_ref()->error() << " replayed ESubtreeMap at " << get_start_off()
			  << " subtree root " << p->first << " not a subtree in cache";
	++errors;
	continue;
      }
      if (dir->get_dir_auth().first != mds->get_nodeid()) {
	mds->get_clog_ref()->error() << " replayed ESubtreeMap at " << get_start_off()
			  << " subtree root " << p->first
			  << " is not mine in cache (it's " << dir->get_dir_auth() << ")";
	++errors;
	continue;
      }

      for (vector<dirfrag_t>::iterator q = p->second.begin(); q != p->second.end(); ++q)
	mds->get_cache()->get_force_dirfrag(*q, true);

      set<CDir*> bounds;
      mds->get_cache()->get_subtree_bounds(dir, bounds);
      for (vector<dirfrag_t>::iterator q = p->second.begin(); q != p->second.end(); ++q) {
	CDir *b = mds->get_cache()->get_dirfrag(*q);
	if (!b) {
	  mds->get_clog_ref()->error() << " replayed ESubtreeMap at " << get_start_off()
			    << " subtree " << p->first << " bound " << *q << " not in cache";
	++errors;
	  continue;
	}
	if (bounds.count(b) == 0) {
	  mds->get_clog_ref()->error() << " replayed ESubtreeMap at " << get_start_off()
			    << " subtree " << p->first << " bound " << *q << " not a bound in cache";
	++errors;
	  continue;
	}
	bounds.erase(b);
      }
      for (set<CDir*>::iterator q = bounds.begin(); q != bounds.end(); ++q) {
	mds->get_clog_ref()->error() << " replayed ESubtreeMap at " << get_start_off()
			  << " subtree " << p->first << " has extra bound in cache " << (*q)->dirfrag();
	++errors;
      }
      
      if (ambiguous_subtrees.count(p->first)) {
	if (!mds->get_cache()->have_ambiguous_import(p->first)) {
	  mds->get_clog_ref()->error() << " replayed ESubtreeMap at " << get_start_off()
			    << " subtree " << p->first << " is ambiguous but is not in our cache";
	  ++errors;
	}
      } else {
	if (mds->get_cache()->have_ambiguous_import(p->first)) {
	  mds->get_clog_ref()->error() << " replayed ESubtreeMap at " << get_start_off()
			    << " subtree " << p->first << " is not ambiguous but is in our cache";
	  ++errors;
	}
      }
    }
    
    std::vector<CDir*> dirs;
    mds->get_cache()->get_subtrees(dirs);
    for (const auto& dir : dirs) {
      if (dir->get_dir_auth().first != mds->get_nodeid())
	continue;
      if (subtrees.count(dir->dirfrag()) == 0) {
	mds->get_clog_ref()->error() << " replayed ESubtreeMap at " << get_start_off()
			  << " does not include cache subtree " << dir->dirfrag();
	++errors;
      }
    }

    if (errors) {
      dout(0) << "journal subtrees: " << subtrees << dendl;
      dout(0) << "journal ambig_subtrees: " << ambiguous_subtrees << dendl;
      mds->get_cache()->show_subtrees();
      ceph_assert(!mds->get_log()->get_debug_subtrees() || errors == 0);
    }
    return;
  }

  dout(10) << "ESubtreeMap.replay -- reconstructing (auth) subtree spanning tree" << dendl;
  
  // first, stick the spanning tree in my cache
  //metablob.print(*_dout);
  metablob.replay(mds, get_segment(), EVENT_SUBTREEMAP);
  
  // restore import/export maps
  for (map<dirfrag_t, vector<dirfrag_t> >::iterator p = subtrees.begin();
       p != subtrees.end();
       ++p) {
    CDir *dir = mds->get_cache()->get_dirfrag(p->first);
    ceph_assert(dir);
    if (ambiguous_subtrees.count(p->first)) {
      // ambiguous!
      mds->get_cache()->add_ambiguous_import(p->first, p->second);
      mds->get_cache()->adjust_bounded_subtree_auth(dir, p->second,
						mds_authority_t(mds->get_nodeid(), mds->get_nodeid()));
    } else {
      // not ambiguous
      mds->get_cache()->adjust_bounded_subtree_auth(dir, p->second, mds->get_nodeid());
    }
  }

  mds->get_cache()->recalc_auth_bits(true);

  mds->get_cache()->show_subtrees();
}



// -----------------------
// EFragment

void EFragment::replay(MDSRankBase *mdsb)
{
  MDSRank* mds = MDSRank::from_base(mdsb);
  dout(10) << "EFragment.replay " << op_name(op) << " " << ino << " " << basefrag << " by " << bits << dendl;

  std::vector<CDir*> resultfrags;
  MDSContext::vec waiters;

  // in may be NULL if it wasn't in our cache yet.  if it's a prepare
  // it will be once we replay the metablob , but first we need to
  // refragment anything we already have in the cache.
  CInode *in = mds->get_cache()->get_inode(ino);

  auto&& segment = get_segment();
  switch (op) {
  case OP_PREPARE:
    mds->get_cache()->add_uncommitted_fragment(dirfrag_t(ino, basefrag), bits, orig_frags, segment, &rollback);

    if (in)
      mds->get_cache()->adjust_dir_fragments(in, basefrag, bits, &resultfrags, waiters, true);
    break;

  case OP_ROLLBACK: {
    frag_vec_t old_frags;
    if (in) {
      in->dirfragtree.get_leaves_under(basefrag, old_frags);
      if (orig_frags.empty()) {
	// old format EFragment
	mds->get_cache()->adjust_dir_fragments(in, basefrag, -bits, &resultfrags, waiters, true);
      } else {
	for (const auto& fg : orig_frags)
	  mds->get_cache()->force_dir_fragment(in, fg);
      }
    }
    mds->get_cache()->rollback_uncommitted_fragment(dirfrag_t(ino, basefrag), std::move(old_frags));
    break;
  }

  case OP_COMMIT:
  case OP_FINISH:
    mds->get_cache()->finish_uncommitted_fragment(dirfrag_t(ino, basefrag), op);
    break;

  default:
    ceph_abort();
  }

  metablob.replay(mds, segment, EVENT_FRAGMENT);
  if (in && g_conf()->mds_debug_frag)
    in->verify_dirfrags();
}

// =========================================================================

// -----------------------
// EExport

void EExport::replay(MDSRankBase *mds)
{
  dout(10) << "EExport.replay " << base << dendl;
  auto&& segment = get_segment();
  metablob.replay(mds, segment, EVENT_EXPORT);
  
  CDir *dir = mds->get_cache()->get_dirfrag(base);
  ceph_assert(dir);
  
  set<CDir*> realbounds;
  for (set<dirfrag_t>::iterator p = bounds.begin();
       p != bounds.end();
       ++p) {
    CDir *bd = mds->get_cache()->get_dirfrag(*p);
    ceph_assert(bd);
    realbounds.insert(bd);
  }

  // adjust auth away
  mds->get_cache()->adjust_bounded_subtree_auth(dir, realbounds, CDIR_AUTH_UNDEF);

  mds->get_cache()->try_trim_non_auth_subtree(dir);
}

// -----------------------
// EImportStart

void EImportStart::update_segment()
{
  get_segment()->sessionmapv = cmapv;
}

void EImportStart::replay(MDSRankBase *mdsb)
{
  MDSRank* mds = MDSRank::from_base(mdsb);
  dout(10) << "EImportStart.replay " << base << " bounds " << bounds << dendl;
  //metablob.print(*_dout);
  auto&& segment = get_segment();
  metablob.replay(mds, segment, EVENT_IMPORTSTART);

  // put in ambiguous import list
  mds->get_cache()->add_ambiguous_import(base, bounds);

  // set auth partially to us so we don't trim it
  CDir *dir = mds->get_cache()->get_dirfrag(base);
  ceph_assert(dir);

  set<CDir*> realbounds;
  for (vector<dirfrag_t>::iterator p = bounds.begin();
       p != bounds.end();
       ++p) {
    CDir *bd = mds->get_cache()->get_dirfrag(*p);
    ceph_assert(bd);
    if (!bd->is_subtree_root())
      bd->state_clear(CDir::STATE_AUTH);
    realbounds.insert(bd);
  }

  mds->get_cache()->adjust_bounded_subtree_auth(dir, realbounds,
					    mds_authority_t(mds->get_nodeid(), mds->get_nodeid()));

  // open client sessions?
  if (mds->sessionmap.get_version() >= cmapv) {
    dout(10) << "EImportStart.replay sessionmap " << mds->sessionmap.get_version() 
	     << " >= " << cmapv << ", noop" << dendl;
  } else {
    dout(10) << "EImportStart.replay sessionmap " << mds->sessionmap.get_version() 
	     << " < " << cmapv << dendl;
    map<client_t,entity_inst_t> cm;
    map<client_t,client_metadata_t> cmm;
    auto blp = client_map.cbegin();
    using ceph::decode;
    decode(cm, blp);
    if (!blp.end())
      decode(cmm, blp);
    mds->sessionmap.replay_open_sessions(cmapv, cm, cmm);
  }
  update_segment();
}

// -----------------------
// EImportFinish

void EImportFinish::replay(MDSRankBase *mds)
{
  if (mds->get_cache()->have_ambiguous_import(base)) {
    dout(10) << "EImportFinish.replay " << base << " success=" << success << dendl;
    if (success) {
      mds->get_cache()->finish_ambiguous_import(base);
    } else {
      CDir *dir = mds->get_cache()->get_dirfrag(base);
      ceph_assert(dir);
      vector<dirfrag_t> bounds;
      mds->get_cache()->get_ambiguous_import_bounds(base, bounds);
      mds->get_cache()->adjust_bounded_subtree_auth(dir, bounds, CDIR_AUTH_UNDEF);
      mds->get_cache()->cancel_ambiguous_import(dir);
      mds->get_cache()->try_trim_non_auth_subtree(dir);
   }
  } else {
    // this shouldn't happen unless this is an old journal
    dout(10) << "EImportFinish.replay " << base << " success=" << success
	     << " on subtree not marked as ambiguous" 
	     << dendl;
    mds->get_clog_ref()->error() << "failure replaying journal (EImportFinish)";
    mds->damaged();
    ceph_abort();  // Should be unreachable because damaged() calls respawn()
  }
}


// ------------------------
// EResetJournal

void EResetJournal::replay(MDSRankBase *mdsb)
{
  MDSRank* mds = MDSRank::from_base(mdsb);
  dout(1) << "EResetJournal" << dendl;

  mds->sessionmap.wipe();
  mds->inotable->replay_reset();

  if (mds->mdsmap->get_root() == mds->get_nodeid()) {
    CDir *rootdir = mds->get_cache()->get_root()->get_or_open_dirfrag(mds->get_cache(), frag_t());
    mds->get_cache()->adjust_subtree_auth(rootdir, mds->get_nodeid());
  }

  CDir *mydir = mds->get_cache()->get_myin()->get_or_open_dirfrag(mds->get_cache(), frag_t());
  mds->get_cache()->adjust_subtree_auth(mydir, mds->get_nodeid());

  mds->get_cache()->recalc_auth_bits(true);

  mds->get_cache()->show_subtrees();
}

void ESegment::replay(MDSRankBase *mds)
{
  dout(4) << "ESegment::replay, seq " << seq << dendl;
}

void ELid::replay(MDSRankBase *mds)
{
  dout(4) << "ELid::replay, seq " << seq << dendl;
}

void ENoOp::replay(MDSRankBase *mds)
{
  dout(4) << "ENoOp::replay, " << pad_size << " bytes skipped in journal" << dendl;
}

/**
 * If re-formatting an old journal that used absolute log position
 * references as segment sequence numbers, use this function to update
 * it.
 *
 * @param mds
 * MDSRank instance, just used for logging
 * @param old_to_new
 * Map of old journal segment sequence numbers to new journal segment sequence numbers
 *
 * @return
 * True if the event was modified.
 */
bool EMetaBlob::rewrite_truncate_finish(MDSRankBase const *mds,
    std::map<LogSegment::seq_t, LogSegment::seq_t> const &old_to_new)
{
  bool modified = false;
  map<inodeno_t, LogSegment::seq_t> new_trunc_finish;
  for (const auto& p : truncate_finish) {
    auto q = old_to_new.find(p.second);
    if (q != old_to_new.end()) {
      dout(20) << __func__ << " applying segment seq mapping "
        << p.second << " -> " << q->second << dendl;
      new_trunc_finish.emplace(p.first, q->second);
      modified = true;
    } else {
      dout(20) << __func__ << " no segment seq mapping found for "
        << p.second << dendl;
      new_trunc_finish.insert(p);
    }
  }
  truncate_finish.swap(new_trunc_finish);

  return modified;
}
