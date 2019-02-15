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
#include "events/ESlaveUpdate.h"
#include "events/EOpen.h"
#include "events/ECommitted.h"

#include "events/EExport.h"
#include "events/EImportStart.h"
#include "events/EImportFinish.h"
#include "events/EFragment.h"

#include "events/ETableClient.h"
#include "events/ETableServer.h"

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


// -----------------------
// LogSegment

void LogSegment::try_to_expire(MDSRank *mds, MDSGatherBuilder &gather_bld, int op_prio)
{
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

  // master ops with possibly uncommitted slaves
  for (set<metareqid_t>::iterator p = uncommitted_masters.begin();
       p != uncommitted_masters.end();
       ++p) {
    dout(10) << "try_to_expire waiting for slaves to ack commit on " << *p << dendl;
    mds->mdcache->wait_for_uncommitted_master(*p, gather_bld.new_sub());
  }

  // uncommitted fragments
  for (set<dirfrag_t>::iterator p = uncommitted_fragments.begin();
       p != uncommitted_fragments.end();
       ++p) {
    dout(10) << "try_to_expire waiting for uncommitted fragment " << *p << dendl;
    mds->mdcache->wait_for_uncommitted_fragment(*p, gather_bld.new_sub());
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
    ceph_assert(!mds->mdlog->is_capped()); // hmm FIXME
    EOpen *le = 0;
    LogSegment *ls = mds->mdlog->get_current_segment();
    ceph_assert(ls != this);
    elist<CInode*>::iterator p = open_files.begin(member_offset(CInode, item_open_file));
    while (!p.end()) {
      CInode *in = *p;
      ++p;
      if (in->last != CEPH_NOSNAP && in->is_auth() && !in->client_snap_caps.empty()) {
	// journal snap inodes that need flush. This simplify the mds failover hanlding
	dout(20) << "try_to_expire requeueing snap needflush inode " << *in << dendl;
	if (!le) {
	  le = new EOpen(mds->mdlog);
	  mds->mdlog->start_entry(le);
	}
	le->add_clean_inode(in);
	ls->open_files.push_back(&in->item_open_file);
      } else {
	// open files are tracked by open file table, no need to journal them again
	in->item_open_file.remove_myself();
      }
    }
    if (le) {
      mds->mdlog->submit_entry(le);
      mds->mdlog->wait_for_safe(gather_bld.new_sub());
      dout(10) << "try_to_expire waiting for open files to rejournal" << dendl;
    }
  }

  ceph_assert(g_conf()->mds_kill_journal_expire_at != 3);

  // backtraces to be stored/updated
  for (elist<CInode*>::iterator p = dirty_parent_inodes.begin(); !p.end(); ++p) {
    CInode *in = *p;
    ceph_assert(in->is_auth());
    if (in->can_auth_pin()) {
      dout(15) << "try_to_expire waiting for storing backtrace on " << *in << dendl;
      in->store_backtrace(gather_bld.new_sub(), op_prio);
    } else {
      dout(15) << "try_to_expire waiting for unfreeze on " << *in << dendl;
      in->add_waiter(CInode::WAIT_UNFREEZE, gather_bld.new_sub());
    }
  }

  ceph_assert(g_conf()->mds_kill_journal_expire_at != 4);

  // slave updates
  for (elist<MDSlaveUpdate*>::iterator p = slave_updates.begin(member_offset(MDSlaveUpdate,
									     item));
       !p.end(); ++p) {
    MDSlaveUpdate *su = *p;
    dout(10) << "try_to_expire waiting on slave update " << su << dendl;
    ceph_assert(su->waiter == 0);
    su->waiter = gather_bld.new_sub();
  }

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
  
  if (gather_bld.has_subs()) {
    dout(6) << "LogSegment(" << seq << "/" << offset << ").try_to_expire waiting" << dendl;
    mds->mdlog->flush();
  } else {
    ceph_assert(g_conf()->mds_kill_journal_expire_at != 5);
    dout(6) << "LogSegment(" << seq << "/" << offset << ").try_to_expire success" << dendl;
  }
}


// -----------------------
// EMetaBlob

void EMetaBlob::add_dir_context(CDir *dir, int mode)
{
  MDSRank *mds = dir->cache->mds;

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
      if (event_seq && diri->last_journaled == event_seq) {
	dout(20) << "EMetaBlob::add_dir_context(" << dir << ") already have diri this blob " << *diri << dendl;
	break;
      }

      // have we journaled this inode since the last subtree map?
      if (!maybenot && last_subtree_map && diri->last_journaled >= last_subtree_map) {
	dout(20) << "EMetaBlob::add_dir_context(" << dir << ") already have diri in this segment (" 
		 << diri->last_journaled << " >= " << last_subtree_map << "), setting maybenot flag "
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
  for (list<CDentry*>::iterator p = parents.begin(); p != parents.end(); ++p) {
    ceph_assert((*p)->get_projected_linkage()->is_primary());
    add_dentry(*p, false);
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

void EMetaBlob::fullbit::encode(bufferlist& bl, uint64_t features) const {
  ENCODE_START(8, 5, bl);
  encode(dn, bl);
  encode(dnfirst, bl);
  encode(dnlast, bl);
  encode(dnv, bl);
  encode(inode, bl, features);
  encode(xattrs, bl);
  if (inode.is_symlink())
    encode(symlink, bl);
  if (inode.is_dir()) {
    encode(dirfragtree, bl);
    encode(snapbl, bl);
  }
  encode(state, bl);
  if (old_inodes.empty()) {
    encode(false, bl);
  } else {
    encode(true, bl);
    encode(old_inodes, bl, features);
  }
  if (!inode.is_dir())
    encode(snapbl, bl);
  encode(oldest_snap, bl);
  ENCODE_FINISH(bl);
}

void EMetaBlob::fullbit::decode(bufferlist::const_iterator &bl) {
  DECODE_START_LEGACY_COMPAT_LEN(7, 5, 5, bl);
  decode(dn, bl);
  decode(dnfirst, bl);
  decode(dnlast, bl);
  decode(dnv, bl);
  decode(inode, bl);
  decode(xattrs, bl);
  if (inode.is_symlink())
    decode(symlink, bl);
  if (inode.is_dir()) {
    decode(dirfragtree, bl);
    decode(snapbl, bl);
    if ((struct_v == 2) || (struct_v == 3)) {
      bool dir_layout_exists;
      decode(dir_layout_exists, bl);
      if (dir_layout_exists) {
	__u8 dir_struct_v;
	decode(dir_struct_v, bl); // default_file_layout version
	decode(inode.layout, bl); // and actual layout, that we care about
      }
    }
  }
  if (struct_v >= 6) {
    decode(state, bl);
  } else {
    bool dirty;
    decode(dirty, bl);
    state = dirty ? EMetaBlob::fullbit::STATE_DIRTY : 0;
  }

  if (struct_v >= 3) {
    bool old_inodes_present;
    decode(old_inodes_present, bl);
    if (old_inodes_present) {
      decode(old_inodes, bl);
    }
  }
  if (!inode.is_dir()) {
    if (struct_v >= 7)
      decode(snapbl, bl);
  }
  if (struct_v >= 8)
    decode(oldest_snap, bl);
  else
    oldest_snap = CEPH_NOSNAP;

  DECODE_FINISH(bl);
}

void EMetaBlob::fullbit::dump(Formatter *f) const
{
  f->dump_string("dentry", dn);
  f->dump_stream("snapid.first") << dnfirst;
  f->dump_stream("snapid.last") << dnlast;
  f->dump_int("dentry version", dnv);
  f->open_object_section("inode");
  inode.dump(f);
  f->close_section(); // inode
  f->open_object_section("xattrs");
  for (const auto &p : xattrs) {
    std::string s(p.second.c_str(), p.second.length());
    f->dump_string(p.first.c_str(), s);
  }
  f->close_section(); // xattrs
  if (inode.is_symlink()) {
    f->dump_string("symlink", symlink);
  }
  if (inode.is_dir()) {
    f->dump_stream("frag tree") << dirfragtree;
    f->dump_string("has_snapbl", snapbl.length() ? "true" : "false");
    if (inode.has_layout()) {
      f->open_object_section("file layout policy");
      // FIXME
      f->dump_string("layout", "the layout exists");
      f->close_section(); // file layout policy
    }
  }
  f->dump_string("state", state_string());
  if (!old_inodes.empty()) {
    f->open_array_section("old inodes");
    for (const auto &p : old_inodes) {
      f->open_object_section("inode");
      f->dump_int("snapid", p.first);
      p.second.dump(f);
      f->close_section(); // inode
    }
    f->close_section(); // old inodes
  }
}

void EMetaBlob::fullbit::generate_test_instances(list<EMetaBlob::fullbit*>& ls)
{
  CInode::mempool_inode inode;
  fragtree_t fragtree;
  CInode::mempool_xattr_map empty_xattrs;
  bufferlist empty_snapbl;
  fullbit *sample = new fullbit("/testdn", 0, 0, 0,
                                inode, fragtree, empty_xattrs, "", 0, empty_snapbl,
                                false, NULL);
  ls.push_back(sample);
}

void EMetaBlob::fullbit::update_inode(MDSRank *mds, CInode *in)
{
  in->inode = inode;
  in->xattrs = xattrs;
  in->maybe_export_pin();
  if (in->inode.is_dir()) {
    if (!(in->dirfragtree == dirfragtree)) {
      dout(10) << "EMetaBlob::fullbit::update_inode dft " << in->dirfragtree << " -> "
	       << dirfragtree << " on " << *in << dendl;
      in->dirfragtree = dirfragtree;
      in->force_dirfrags();
      if (in->has_dirfrags() && in->authority() == CDIR_AUTH_UNDEF) {
	list<CDir*> ls;
	in->get_nested_dirfrags(ls);
	for (list<CDir*>::iterator p = ls.begin(); p != ls.end(); ++p) {
	  CDir *dir = *p;
	  if (dir->get_num_any() == 0 &&
	      mds->mdcache->can_trim_non_auth_dirfrag(dir)) {
	    dout(10) << " closing empty non-auth dirfrag " << *dir << dendl;
	    in->close_dirfrag(dir->get_frag());
	  }
	}
      }
    }
  } else if (in->inode.is_symlink()) {
    in->symlink = symlink;
  }
  in->old_inodes = old_inodes;
  if (!in->old_inodes.empty()) {
    snapid_t min_first = in->old_inodes.rbegin()->first + 1;
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
    if (in->inode.layout.pool_id == -1 || !in->inode.layout.is_valid()) {
      dout(0) << "EMetaBlob.replay invalid layout on ino " << *in
              << ": " << in->inode.layout << dendl;
      std::ostringstream oss;
      oss << "Invalid layout for inode " << in->ino() << " in journal";
      mds->clog->error() << oss.str();
      mds->damaged();
      ceph_abort();  // Should be unreachable because damaged() calls respawn()
    }
  }
}

// EMetaBlob::remotebit

void EMetaBlob::remotebit::encode(bufferlist& bl) const
{
  ENCODE_START(2, 2, bl);
  encode(dn, bl);
  encode(dnfirst, bl);
  encode(dnlast, bl);
  encode(dnv, bl);
  encode(ino, bl);
  encode(d_type, bl);
  encode(dirty, bl);
  ENCODE_FINISH(bl);
}

void EMetaBlob::remotebit::decode(bufferlist::const_iterator &bl)
{
  DECODE_START_LEGACY_COMPAT_LEN(2, 2, 2, bl);
  decode(dn, bl);
  decode(dnfirst, bl);
  decode(dnlast, bl);
  decode(dnv, bl);
  decode(ino, bl);
  decode(d_type, bl);
  decode(dirty, bl);
  DECODE_FINISH(bl);
}

void EMetaBlob::remotebit::dump(Formatter *f) const
{
  f->dump_string("dentry", dn);
  f->dump_int("snapid.first", dnfirst);
  f->dump_int("snapid.last", dnlast);
  f->dump_int("dentry version", dnv);
  f->dump_int("inodeno", ino);
  uint32_t type = DTTOIF(d_type) & S_IFMT; // convert to type entries
  string type_string;
  switch(type) {
  case S_IFREG:
    type_string = "file"; break;
  case S_IFLNK:
    type_string = "symlink"; break;
  case S_IFDIR:
    type_string = "directory"; break;
  case S_IFIFO:
    type_string = "fifo"; break;
  case S_IFCHR:
    type_string = "chr"; break;
  case S_IFBLK:
    type_string = "blk"; break;
  case S_IFSOCK:
    type_string = "sock"; break;
  default:
    assert (0 == "unknown d_type!");
  }
  f->dump_string("d_type", type_string);
  f->dump_string("dirty", dirty ? "true" : "false");
}

void EMetaBlob::remotebit::
generate_test_instances(list<EMetaBlob::remotebit*>& ls)
{
  remotebit *remote = new remotebit("/test/dn", 0, 10, 15, 1, IFTODT(S_IFREG), false);
  ls.push_back(remote);
}

// EMetaBlob::nullbit

void EMetaBlob::nullbit::encode(bufferlist& bl) const
{
  ENCODE_START(2, 2, bl);
  encode(dn, bl);
  encode(dnfirst, bl);
  encode(dnlast, bl);
  encode(dnv, bl);
  encode(dirty, bl);
  ENCODE_FINISH(bl);
}

void EMetaBlob::nullbit::decode(bufferlist::const_iterator &bl)
{
  DECODE_START_LEGACY_COMPAT_LEN(2, 2, 2, bl);
  decode(dn, bl);
  decode(dnfirst, bl);
  decode(dnlast, bl);
  decode(dnv, bl);
  decode(dirty, bl);
  DECODE_FINISH(bl);
}

void EMetaBlob::nullbit::dump(Formatter *f) const
{
  f->dump_string("dentry", dn);
  f->dump_int("snapid.first", dnfirst);
  f->dump_int("snapid.last", dnlast);
  f->dump_int("dentry version", dnv);
  f->dump_string("dirty", dirty ? "true" : "false");
}

void EMetaBlob::nullbit::generate_test_instances(list<nullbit*>& ls)
{
  nullbit *sample = new nullbit("/test/dentry", 0, 10, 15, false);
  nullbit *sample2 = new nullbit("/test/dirty", 10, 20, 25, true);
  ls.push_back(sample);
  ls.push_back(sample2);
}

// EMetaBlob::dirlump

void EMetaBlob::dirlump::encode(bufferlist& bl, uint64_t features) const
{
  ENCODE_START(2, 2, bl);
  encode(fnode, bl);
  encode(state, bl);
  encode(nfull, bl);
  encode(nremote, bl);
  encode(nnull, bl);
  _encode_bits(features);
  encode(dnbl, bl);
  ENCODE_FINISH(bl);
}

void EMetaBlob::dirlump::decode(bufferlist::const_iterator &bl)
{
  DECODE_START_LEGACY_COMPAT_LEN(2, 2, 2, bl)
  decode(fnode, bl);
  decode(state, bl);
  decode(nfull, bl);
  decode(nremote, bl);
  decode(nnull, bl);
  decode(dnbl, bl);
  dn_decoded = false;      // don't decode bits unless we need them.
  DECODE_FINISH(bl);
}

void EMetaBlob::dirlump::dump(Formatter *f) const
{
  if (!dn_decoded) {
    dirlump *me = const_cast<dirlump*>(this);
    me->_decode_bits();
  }
  f->open_object_section("fnode");
  fnode.dump(f);
  f->close_section(); // fnode
  f->dump_string("state", state_string());
  f->dump_int("nfull", nfull);
  f->dump_int("nremote", nremote);
  f->dump_int("nnull", nnull);

  f->open_array_section("full bits");
  for (const auto& iter : dfull) {
    f->open_object_section("fullbit");
    iter.dump(f);
    f->close_section(); // fullbit
  }
  f->close_section(); // full bits
  f->open_array_section("remote bits");
  for (const auto& iter : dremote) {
    f->open_object_section("remotebit");
    iter.dump(f);
    f->close_section(); // remotebit
  }
  f->close_section(); // remote bits
  f->open_array_section("null bits");
  for (const auto& iter : dnull) {
    f->open_object_section("null bit");
    iter.dump(f);
    f->close_section(); // null bit
  }
  f->close_section(); // null bits
}

void EMetaBlob::dirlump::generate_test_instances(list<dirlump*>& ls)
{
  ls.push_back(new dirlump());
}

/**
 * EMetaBlob proper
 */
void EMetaBlob::encode(bufferlist& bl, uint64_t features) const
{
  ENCODE_START(8, 5, bl);
  encode(lump_order, bl);
  encode(lump_map, bl, features);
  encode(roots, bl, features);
  encode(table_tids, bl);
  encode(opened_ino, bl);
  encode(allocated_ino, bl);
  encode(used_preallocated_ino, bl);
  encode(preallocated_inos, bl);
  encode(client_name, bl);
  encode(inotablev, bl);
  encode(sessionmapv, bl);
  encode(truncate_start, bl);
  encode(truncate_finish, bl);
  encode(destroyed_inodes, bl);
  encode(client_reqs, bl);
  encode(renamed_dirino, bl);
  encode(renamed_dir_frags, bl);
  {
    // make MDSRank use v6 format happy
    int64_t i = -1;
    bool b = false;
    encode(i, bl);
    encode(b, bl);
  }
  encode(client_flushes, bl);
  ENCODE_FINISH(bl);
}
void EMetaBlob::decode(bufferlist::const_iterator &bl)
{
  DECODE_START_LEGACY_COMPAT_LEN(7, 5, 5, bl);
  decode(lump_order, bl);
  decode(lump_map, bl);
  if (struct_v >= 4) {
    decode(roots, bl);
  } else {
    bufferlist rootbl;
    decode(rootbl, bl);
    if (rootbl.length()) {
      auto p = rootbl.cbegin();
      roots.emplace_back(p);
    }
  }
  decode(table_tids, bl);
  decode(opened_ino, bl);
  decode(allocated_ino, bl);
  decode(used_preallocated_ino, bl);
  decode(preallocated_inos, bl);
  decode(client_name, bl);
  decode(inotablev, bl);
  decode(sessionmapv, bl);
  decode(truncate_start, bl);
  decode(truncate_finish, bl);
  decode(destroyed_inodes, bl);
  if (struct_v >= 2) {
    decode(client_reqs, bl);
  } else {
    list<metareqid_t> r;
    decode(r, bl);
    while (!r.empty()) {
	client_reqs.push_back(pair<metareqid_t,uint64_t>(r.front(), 0));
	r.pop_front();
    }
  }
  if (struct_v >= 3) {
    decode(renamed_dirino, bl);
    decode(renamed_dir_frags, bl);
  }
  if (struct_v >= 6) {
    // ignore
    int64_t i;
    bool b;
    decode(i, bl);
    decode(b, bl);
  }
  if (struct_v >= 8) {
    decode(client_flushes, bl);
  }
  DECODE_FINISH(bl);
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
      inodes.insert(iter.inode.ino);
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
  std::map<inodeno_t, std::list<std::string> > children;

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
      ino_locations[iter.inode.ino] = Location(dir_ino, dentry);
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
      if (children.find(iter.inode.ino) == children.end()) {
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


void EMetaBlob::dump(Formatter *f) const
{
  f->open_array_section("lumps");
  for (const auto& d : lump_order) {
    f->open_object_section("lump");
    f->open_object_section("dirfrag");
    f->dump_stream("dirfrag") << d;
    f->close_section(); // dirfrag
    f->open_object_section("dirlump");
    lump_map.at(d).dump(f);
    f->close_section(); // dirlump
    f->close_section(); // lump
  }
  f->close_section(); // lumps
  
  f->open_array_section("roots");
  for (const auto& iter : roots) {
    f->open_object_section("root");
    iter.dump(f);
    f->close_section(); // root
  }
  f->close_section(); // roots

  f->open_array_section("tableclient tranactions");
  for (const auto& p : table_tids) {
    f->open_object_section("transaction");
    f->dump_int("tid", p.first);
    f->dump_int("version", p.second);
    f->close_section(); // transaction
  }
  f->close_section(); // tableclient transactions
  
  f->dump_int("renamed directory inodeno", renamed_dirino);
  
  f->open_array_section("renamed directory fragments");
  for (const auto& p : renamed_dir_frags) {
    f->dump_int("frag", p);
  }
  f->close_section(); // renamed directory fragments

  f->dump_int("inotable version", inotablev);
  f->dump_int("SessionMap version", sessionmapv);
  f->dump_int("allocated ino", allocated_ino);
  
  f->dump_stream("preallocated inos") << preallocated_inos;
  f->dump_int("used preallocated ino", used_preallocated_ino);

  f->open_object_section("client name");
  client_name.dump(f);
  f->close_section(); // client name

  f->open_array_section("inodes starting a truncate");
  for(const auto& ino : truncate_start) {
    f->dump_int("inodeno", ino);
  }
  f->close_section(); // truncate inodes
  f->open_array_section("inodes finishing a truncated");
  for(const auto& p : truncate_finish) {
    f->open_object_section("inode+segment");
    f->dump_int("inodeno", p.first);
    f->dump_int("truncate starting segment", p.second);
    f->close_section(); // truncated inode
  }
  f->close_section(); // truncate finish inodes

  f->open_array_section("destroyed inodes");
  for(vector<inodeno_t>::const_iterator i = destroyed_inodes.begin();
      i != destroyed_inodes.end(); ++i) {
    f->dump_int("inodeno", *i);
  }
  f->close_section(); // destroyed inodes

  f->open_array_section("client requests");
  for(const auto& p : client_reqs) {
    f->open_object_section("Client request");
    f->dump_stream("request ID") << p.first;
    f->dump_int("oldest request on client", p.second);
    f->close_section(); // request
  }
  f->close_section(); // client requests
}

void EMetaBlob::generate_test_instances(list<EMetaBlob*>& ls)
{
  ls.push_back(new EMetaBlob());
}

void EMetaBlob::replay(MDSRank *mds, LogSegment *logseg, MDSlaveUpdate *slaveup)
{
  dout(10) << "EMetaBlob.replay " << lump_map.size() << " dirlumps by " << client_name << dendl;

  ceph_assert(logseg);

  ceph_assert(g_conf()->mds_kill_journal_replay_at != 1);

  for (auto& p : roots) {
    CInode *in = mds->mdcache->get_inode(p.inode.ino);
    bool isnew = in ? false:true;
    if (!in)
      in = new CInode(mds->mdcache, false, 2, CEPH_NOSNAP);
    p.update_inode(mds, in);

    if (isnew)
      mds->mdcache->add_inode(in);
    if (p.is_dirty()) in->_mark_dirty(logseg);
    dout(10) << "EMetaBlob.replay " << (isnew ? " added root ":" updated root ") << *in << dendl;    
  }

  CInode *renamed_diri = 0;
  CDir *olddir = 0;
  if (renamed_dirino) {
    renamed_diri = mds->mdcache->get_inode(renamed_dirino);
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
  for (const auto& lp : lump_order) {
    dout(10) << "EMetaBlob.replay dir " << lp << dendl;
    dirlump &lump = lump_map[lp];

    // the dir 
    CDir *dir = mds->mdcache->get_force_dirfrag(lp, true);
    if (!dir) {
      // hmm.  do i have the inode?
      CInode *diri = mds->mdcache->get_inode((lp).ino);
      if (!diri) {
	if (MDS_INO_IS_MDSDIR(lp.ino)) {
	  ceph_assert(MDS_INO_MDSDIR(mds->get_nodeid()) != lp.ino);
	  diri = mds->mdcache->create_system_inode(lp.ino, S_IFDIR|0755);
	  diri->state_clear(CInode::STATE_AUTH);
	  dout(10) << "EMetaBlob.replay created base " << *diri << dendl;
	} else {
	  dout(0) << "EMetaBlob.replay missing dir ino  " << lp.ino << dendl;
          mds->clog->error() << "failure replaying journal (EMetaBlob)";
          mds->damaged();
          ceph_abort();  // Should be unreachable because damaged() calls respawn()
	}
      }

      // create the dirfrag
      dir = diri->get_or_open_dirfrag(mds->mdcache, lp.frag);

      if (MDS_INO_IS_BASE(lp.ino))
	mds->mdcache->adjust_subtree_auth(dir, CDIR_AUTH_UNDEF);

      dout(10) << "EMetaBlob.replay added dir " << *dir << dendl;  
    }
    dir->set_version( lump.fnode.version );
    dir->fnode = lump.fnode;

    if (lump.is_importing()) {
      dir->state_set(CDir::STATE_AUTH);
      dir->state_clear(CDir::STATE_COMPLETE);
    }
    if (lump.is_dirty()) {
      dir->_mark_dirty(logseg);

      if (!(dir->fnode.rstat == dir->fnode.accounted_rstat)) {
	dout(10) << "EMetaBlob.replay      dirty nestinfo on " << *dir << dendl;
	mds->locker->mark_updated_scatterlock(&dir->inode->nestlock);
	logseg->dirty_dirfrag_nest.push_back(&dir->inode->item_dirty_dirfrag_nest);
      } else {
	dout(10) << "EMetaBlob.replay      clean nestinfo on " << *dir << dendl;
      }
      if (!(dir->fnode.fragstat == dir->fnode.accounted_fragstat)) {
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
	dn->state_set(CDentry::STATE_AUTH);

      CInode *in = mds->mdcache->get_inode(fb.inode.ino, fb.dnlast);
      if (!in) {
	in = new CInode(mds->mdcache, dn->is_auth(), fb.dnfirst, fb.dnlast);
	fb.update_inode(mds, in);
	mds->mdcache->add_inode(in);
	if (!dn->get_linkage()->is_null()) {
	  if (dn->get_linkage()->is_primary()) {
	    unlinked[dn->get_linkage()->get_inode()] = dir;
	    stringstream ss;
	    ss << "EMetaBlob.replay FIXME had dentry linked to wrong inode " << *dn
	       << " " << *dn->get_linkage()->get_inode() << " should be " << fb.inode.ino;
	    dout(0) << ss.str() << dendl;
	    mds->clog->warn(ss);
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
	      stringstream ss;
	      ss << "EMetaBlob.replay FIXME had dentry linked to wrong inode " << *dn
		 << " " << *dn->get_linkage()->get_inode() << " should be " << fb.inode.ino;
	      dout(0) << ss.str() << dendl;
	      mds->clog->warn(ss);
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
    }

    // remote dentries
    for (const auto& rb : lump.get_dremote()) {
      CDentry *dn = dir->lookup_exact_snap(rb.dn, rb.dnlast);
      if (!dn) {
	dn = dir->add_remote_dentry(rb.dn, rb.ino, rb.d_type, rb.dnfirst, rb.dnlast);
	dn->set_version(rb.dnv);
	if (rb.dirty) dn->_mark_dirty(logseg);
	dout(10) << "EMetaBlob.replay added " << *dn << dendl;
      } else {
	if (!dn->get_linkage()->is_null()) {
	  dout(10) << "EMetaBlob.replay unlinking " << *dn << dendl;
	  if (dn->get_linkage()->is_primary()) {
	    unlinked[dn->get_linkage()->get_inode()] = dir;
	    stringstream ss;
	    ss << "EMetaBlob.replay FIXME had dentry linked to wrong inode " << *dn
	       << " " << *dn->get_linkage()->get_inode() << " should be remote " << rb.ino;
	    dout(0) << ss.str() << dendl;
	  }
	  dir->unlink_inode(dn, false);
	}
	dir->link_remote_inode(dn, rb.ino, rb.d_type);
	dn->set_version(rb.dnv);
	if (rb.dirty) dn->_mark_dirty(logseg);
	dout(10) << "EMetaBlob.replay for [" << rb.dnfirst << "," << rb.dnlast << "] had " << *dn << dendl;
	dn->first = rb.dnfirst;
	ceph_assert(dn->last == rb.dnlast);
      }
      if (lump.is_importing())
	dn->state_set(CDentry::STATE_AUTH);
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
	dn->state_set(CDentry::STATE_AUTH);

      // Make null dentries the first things we trim
      dout(10) << "EMetaBlob.replay pushing to bottom of lru " << *dn << dendl;
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
      renamed_diri = mds->mdcache->get_inode(renamed_dirino);
      ceph_assert(renamed_diri);  // it was in the metablob
    }

    if (olddir) {
      if (olddir->authority() != CDIR_AUTH_UNDEF &&
	  renamed_diri->authority() == CDIR_AUTH_UNDEF) {
	ceph_assert(slaveup); // auth to non-auth, must be slave prepare
        frag_vec_t leaves;
	renamed_diri->dirfragtree.get_leaves(leaves);
	for (const auto& leaf : leaves) {
	  CDir *dir = renamed_diri->get_dirfrag(leaf);
	  ceph_assert(dir);
	  if (dir->get_dir_auth() == CDIR_AUTH_UNDEF)
	    // preserve subtree bound until slave commit
	    slaveup->olddirs.insert(dir->inode);
	  else
	    dir->state_set(CDir::STATE_AUTH);
	}
      }

      mds->mdcache->adjust_subtree_after_rename(renamed_diri, olddir, false);
      
      // see if we can discard the subtree we renamed out of
      CDir *root = mds->mdcache->get_subtree_root(olddir);
      if (root->get_dir_auth() == CDIR_AUTH_UNDEF) {
	if (slaveup) // preserve the old dir until slave commit
	  slaveup->olddirs.insert(olddir->inode);
	else
	  mds->mdcache->try_trim_non_auth_subtree(root);
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
	dir = renamed_diri->get_or_open_dirfrag(mds->mdcache, p);
	dout(10) << " creating new rename import bound " << *dir << dendl;
	dir->state_clear(CDir::STATE_AUTH);
	mds->mdcache->adjust_subtree_auth(dir, CDIR_AUTH_UNDEF);
      }
    }

    // rename may overwrite an empty directory and move it into stray dir.
    unlinked.erase(renamed_diri);
    for (map<CInode*, CDir*>::iterator p = unlinked.begin(); p != unlinked.end(); ++p) {
      if (!linked.count(p->first))
	continue;
      ceph_assert(p->first->is_dir());
      mds->mdcache->adjust_subtree_after_rename(p->first, p->second, false);
    }
  }

  if (!unlinked.empty()) {
    for (set<CInode*>::iterator p = linked.begin(); p != linked.end(); ++p)
      unlinked.erase(*p);
    dout(10) << " unlinked set contains " << unlinked << dendl;
    for (map<CInode*, CDir*>::iterator p = unlinked.begin(); p != unlinked.end(); ++p) {
      CInode *in = p->first;
      if (slaveup) { // preserve unlinked inodes until slave commit
	slaveup->unlinked.insert(in);
	if (in->snaprealm)
	  in->snaprealm->adjust_parent();
      } else
	mds->mdcache->remove_inode_recursive(in);
    }
  }

  // table client transactions
  for (const auto& p : table_tids) {
    dout(10) << "EMetaBlob.replay noting " << get_mdstable_name(p.first)
	     << " transaction " << p.second << dendl;
    MDSTableClient *client = mds->get_table_client(p.first);
    if (client)
      client->got_journaled_agree(p.second, logseg);
  }

  // opened ino?
  if (opened_ino) {
    CInode *in = mds->mdcache->get_inode(opened_ino);
    ceph_assert(in);
    dout(10) << "EMetaBlob.replay noting opened inode " << *in << dendl;
    logseg->open_files.push_back(&in->item_open_file);
  }

  // allocated_inos
  if (inotablev) {
    if (mds->inotable->get_version() >= inotablev) {
      dout(10) << "EMetaBlob.replay inotable tablev " << inotablev
	       << " <= table " << mds->inotable->get_version() << dendl;
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

      // [repair bad inotable updates]
      if (inotablev > mds->inotable->get_version()) {
	mds->clog->error() << "journal replay inotablev mismatch "
	    << mds->inotable->get_version() << " -> " << inotablev;
	mds->inotable->force_replay_version(inotablev);
      }

      ceph_assert(inotablev == mds->inotable->get_version());
    }
  }
  if (sessionmapv) {
    if (mds->sessionmap.get_version() >= sessionmapv) {
      dout(10) << "EMetaBlob.replay sessionmap v " << sessionmapv
	       << " <= table " << mds->sessionmap.get_version() << dendl;
    } else if (mds->sessionmap.get_version() + 2 >= sessionmapv) {
      dout(10) << "EMetaBlob.replay sessionmap v " << sessionmapv
	       << " -(1|2) == table " << mds->sessionmap.get_version()
	       << " prealloc " << preallocated_inos
	       << " used " << used_preallocated_ino
	       << dendl;
      Session *session = mds->sessionmap.get_session(client_name);
      if (session) {
	dout(20) << " (session prealloc " << session->info.prealloc_inos << ")" << dendl;
	if (used_preallocated_ino) {
	  if (!session->info.prealloc_inos.empty()) {
	    inodeno_t next = session->next_ino();
	    inodeno_t i = session->take_ino(used_preallocated_ino);
	    if (next != i)
	      mds->clog->warn() << " replayed op " << client_reqs << " used ino " << i
			       << " but session next is " << next;
	    ceph_assert(i == used_preallocated_ino);
	    session->info.used_inos.clear();
	  }
          mds->sessionmap.replay_dirty_session(session);
	}
	if (!preallocated_inos.empty()) {
	  session->info.prealloc_inos.insert(preallocated_inos);
          mds->sessionmap.replay_dirty_session(session);
	}

      } else {
	dout(10) << "EMetaBlob.replay no session for " << client_name << dendl;
	if (used_preallocated_ino) {
	  mds->sessionmap.replay_advance_version();
        }
	if (!preallocated_inos.empty())
	  mds->sessionmap.replay_advance_version();
      }
      ceph_assert(sessionmapv == mds->sessionmap.get_version());
    } else {
      mds->clog->error() << "journal replay sessionmap v " << sessionmapv
			<< " -(1|2) > table " << mds->sessionmap.get_version();
      ceph_assert(g_conf()->mds_wipe_sessions);
      mds->sessionmap.wipe();
      mds->sessionmap.set_version(sessionmapv);
    }
  }

  // truncating inodes
  for (const auto& ino : truncate_start) {
    CInode *in = mds->mdcache->get_inode(ino);
    ceph_assert(in);
    mds->mdcache->add_recovered_truncate(in, logseg);
  }
  for (const auto& p : truncate_finish) {
    LogSegment *ls = mds->mdlog->get_segment(p.second);
    if (ls) {
      CInode *in = mds->mdcache->get_inode(p.first);
      ceph_assert(in);
      mds->mdcache->remove_recovered_truncate(in, ls);
    }
  }

  // destroyed inodes
  if (!destroyed_inodes.empty()) {
    for (vector<inodeno_t>::iterator p = destroyed_inodes.begin();
	p != destroyed_inodes.end();
	++p) {
      CInode *in = mds->mdcache->get_inode(*p);
      if (in) {
	dout(10) << "EMetaBlob.replay destroyed " << *p << ", dropping " << *in << dendl;
	CDentry *parent = in->get_parent_dn();
	mds->mdcache->remove_inode(in);
	if (parent) {
	  dout(10) << "EMetaBlob.replay unlinked from dentry " << *parent << dendl;
	  ceph_assert(parent->get_linkage()->is_null());
	}
      } else {
	dout(10) << "EMetaBlob.replay destroyed " << *p << ", not in cache" << dendl;
      }
    }
    mds->mdcache->open_file_table.note_destroyed_inos(logseg->seq, destroyed_inodes);
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
  }

  // update segment
  update_segment(logseg);

  ceph_assert(g_conf()->mds_kill_journal_replay_at != 4);
}

// -----------------------
// ESession

void ESession::update_segment()
{
  get_segment()->sessionmapv = cmapv;
  if (inos.size() && inotablev)
    get_segment()->inotablev = inotablev;
}

void ESession::replay(MDSRank *mds)
{
  if (mds->sessionmap.get_version() >= cmapv) {
    dout(10) << "ESession.replay sessionmap " << mds->sessionmap.get_version() 
	     << " >= " << cmapv << ", noop" << dendl;
  } else {
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
	mds->clog->error() << "replayed stray Session close event for " << client_inst
			  << " from time " << stamp << ", ignoring";
      }
    }
    if (session) {
      mds->sessionmap.replay_dirty_session(session);
    } else {
      mds->sessionmap.replay_advance_version();
    }
    ceph_assert(mds->sessionmap.get_version() == cmapv);
  }
  
  if (inos.size() && inotablev) {
    if (mds->inotable->get_version() >= inotablev) {
      dout(10) << "ESession.replay inotable " << mds->inotable->get_version()
	       << " >= " << inotablev << ", noop" << dendl;
    } else {
      dout(10) << "ESession.replay inotable " << mds->inotable->get_version()
	       << " < " << inotablev << " " << (open ? "add":"remove") << dendl;
      ceph_assert(!open);  // for now
      mds->inotable->replay_release_ids(inos);
      ceph_assert(mds->inotable->get_version() == inotablev);
    }
  }

  update_segment();
}

void ESession::encode(bufferlist &bl, uint64_t features) const
{
  ENCODE_START(5, 5, bl);
  encode(stamp, bl);
  encode(client_inst, bl, features);
  encode(open, bl);
  encode(cmapv, bl);
  encode(inos, bl);
  encode(inotablev, bl);
  encode(client_metadata, bl);
  ENCODE_FINISH(bl);
}

void ESession::decode(bufferlist::const_iterator &bl)
{
  DECODE_START_LEGACY_COMPAT_LEN(5, 3, 3, bl);
  if (struct_v >= 2)
    decode(stamp, bl);
  decode(client_inst, bl);
  decode(open, bl);
  decode(cmapv, bl);
  decode(inos, bl);
  decode(inotablev, bl);
  if (struct_v == 4) {
    decode(client_metadata.kv_map, bl);
  } else if (struct_v >= 5) {
    decode(client_metadata, bl);
  }
  DECODE_FINISH(bl);
}

void ESession::dump(Formatter *f) const
{
  f->dump_stream("client instance") << client_inst;
  f->dump_string("open", open ? "true" : "false");
  f->dump_int("client map version", cmapv);
  f->dump_stream("inos") << inos;
  f->dump_int("inotable version", inotablev);
  f->open_object_section("client_metadata");
  client_metadata.dump(f);
  f->close_section();  // client_metadata
}

void ESession::generate_test_instances(list<ESession*>& ls)
{
  ls.push_back(new ESession);
}

// -----------------------
// ESessions

void ESessions::encode(bufferlist &bl, uint64_t features) const
{
  ENCODE_START(2, 1, bl);
  encode(client_map, bl, features);
  encode(cmapv, bl);
  encode(stamp, bl);
  encode(client_metadata_map, bl);
  ENCODE_FINISH(bl);
}

void ESessions::decode_old(bufferlist::const_iterator &bl)
{
  using ceph::decode;
  decode(client_map, bl);
  decode(cmapv, bl);
  if (!bl.end())
    decode(stamp, bl);
}

void ESessions::decode_new(bufferlist::const_iterator &bl)
{
  DECODE_START(2, bl);
  decode(client_map, bl);
  decode(cmapv, bl);
  decode(stamp, bl);
  if (struct_v >= 2)
    decode(client_metadata_map, bl);
  DECODE_FINISH(bl);
}

void ESessions::dump(Formatter *f) const
{
  f->dump_int("client map version", cmapv);

  f->open_array_section("client map");
  for (map<client_t,entity_inst_t>::const_iterator i = client_map.begin();
       i != client_map.end(); ++i) {
    f->open_object_section("client");
    f->dump_int("client id", i->first.v);
    f->dump_stream("client entity") << i->second;
    f->close_section(); // client
  }
  f->close_section(); // client map
}

void ESessions::generate_test_instances(list<ESessions*>& ls)
{
  ls.push_back(new ESessions());
}

void ESessions::update_segment()
{
  get_segment()->sessionmapv = cmapv;
}

void ESessions::replay(MDSRank *mds)
{
  if (mds->sessionmap.get_version() >= cmapv) {
    dout(10) << "ESessions.replay sessionmap " << mds->sessionmap.get_version()
	     << " >= " << cmapv << ", noop" << dendl;
  } else {
    dout(10) << "ESessions.replay sessionmap " << mds->sessionmap.get_version()
	     << " < " << cmapv << dendl;
    mds->sessionmap.replay_open_sessions(client_map, client_metadata_map);
    ceph_assert(mds->sessionmap.get_version() == cmapv);
  }
  update_segment();
}


// -----------------------
// ETableServer

void ETableServer::encode(bufferlist& bl, uint64_t features) const
{
  ENCODE_START(3, 3, bl);
  encode(stamp, bl);
  encode(table, bl);
  encode(op, bl);
  encode(reqid, bl);
  encode(bymds, bl);
  encode(mutation, bl);
  encode(tid, bl);
  encode(version, bl);
  ENCODE_FINISH(bl);
}

void ETableServer::decode(bufferlist::const_iterator &bl)
{
  DECODE_START_LEGACY_COMPAT_LEN(3, 3, 3, bl);
  if (struct_v >= 2)
    decode(stamp, bl);
  decode(table, bl);
  decode(op, bl);
  decode(reqid, bl);
  decode(bymds, bl);
  decode(mutation, bl);
  decode(tid, bl);
  decode(version, bl);
  DECODE_FINISH(bl);
}

void ETableServer::dump(Formatter *f) const
{
  f->dump_int("table id", table);
  f->dump_int("op", op);
  f->dump_int("request id", reqid);
  f->dump_int("by mds", bymds);
  f->dump_int("tid", tid);
  f->dump_int("version", version);
}

void ETableServer::generate_test_instances(list<ETableServer*>& ls)
{
  ls.push_back(new ETableServer());
}


void ETableServer::update_segment()
{
  get_segment()->tablev[table] = version;
}

void ETableServer::replay(MDSRank *mds)
{
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
    server->_commit(tid, MMDSTableRequest::ref());
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
    mds->clog->error() << "invalid tableserver op in ETableServer";
    mds->damaged();
    ceph_abort();  // Should be unreachable because damaged() calls respawn()
  }
  
  ceph_assert(version == server->get_version());
  update_segment();
}


// ---------------------
// ETableClient

void ETableClient::encode(bufferlist& bl, uint64_t features) const
{
  ENCODE_START(3, 3, bl);
  encode(stamp, bl);
  encode(table, bl);
  encode(op, bl);
  encode(tid, bl);
  ENCODE_FINISH(bl);
}

void ETableClient::decode(bufferlist::const_iterator &bl)
{
  DECODE_START_LEGACY_COMPAT_LEN(3, 3, 3, bl);
  if (struct_v >= 2)
    decode(stamp, bl);
  decode(table, bl);
  decode(op, bl);
  decode(tid, bl);
  DECODE_FINISH(bl);
}

void ETableClient::dump(Formatter *f) const
{
  f->dump_int("table", table);
  f->dump_int("op", op);
  f->dump_int("tid", tid);
}

void ETableClient::generate_test_instances(list<ETableClient*>& ls)
{
  ls.push_back(new ETableClient());
}

void ETableClient::replay(MDSRank *mds)
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

void ESnap::replay(MDSRank *mds)
{
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

void EUpdate::encode(bufferlist &bl, uint64_t features) const
{
  ENCODE_START(4, 4, bl);
  encode(stamp, bl);
  encode(type, bl);
  encode(metablob, bl, features);
  encode(client_map, bl);
  encode(cmapv, bl);
  encode(reqid, bl);
  encode(had_slaves, bl);
  ENCODE_FINISH(bl);
}
 
void EUpdate::decode(bufferlist::const_iterator &bl)
{
  DECODE_START_LEGACY_COMPAT_LEN(4, 4, 4, bl);
  if (struct_v >= 2)
    decode(stamp, bl);
  decode(type, bl);
  decode(metablob, bl);
  decode(client_map, bl);
  if (struct_v >= 3)
    decode(cmapv, bl);
  decode(reqid, bl);
  decode(had_slaves, bl);
  DECODE_FINISH(bl);
}

void EUpdate::dump(Formatter *f) const
{
  f->open_object_section("metablob");
  metablob.dump(f);
  f->close_section(); // metablob

  f->dump_string("type", type);
  f->dump_int("client map length", client_map.length());
  f->dump_int("client map version", cmapv);
  f->dump_stream("reqid") << reqid;
  f->dump_string("had slaves", had_slaves ? "true" : "false");
}

void EUpdate::generate_test_instances(list<EUpdate*>& ls)
{
  ls.push_back(new EUpdate());
}


void EUpdate::update_segment()
{
  auto&& segment = get_segment();
  metablob.update_segment(segment);

  if (client_map.length())
    segment->sessionmapv = cmapv;

  if (had_slaves)
    segment->uncommitted_masters.insert(reqid);
}

void EUpdate::replay(MDSRank *mds)
{
  auto&& segment = get_segment();
  metablob.replay(mds, segment);
  
  if (had_slaves) {
    dout(10) << "EUpdate.replay " << reqid << " had slaves, expecting a matching ECommitted" << dendl;
    segment->uncommitted_masters.insert(reqid);
    set<mds_rank_t> slaves;
    mds->mdcache->add_uncommitted_master(reqid, segment, slaves, true);
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
      mds->sessionmap.replay_open_sessions(cm, cmm);

      ceph_assert(mds->sessionmap.get_version() == cmapv);
    }
  }
  update_segment();
}


// ------------------------
// EOpen

void EOpen::encode(bufferlist &bl, uint64_t features) const {
  ENCODE_START(4, 3, bl);
  encode(stamp, bl);
  encode(metablob, bl, features);
  encode(inos, bl);
  encode(snap_inos, bl);
  ENCODE_FINISH(bl);
} 

void EOpen::decode(bufferlist::const_iterator &bl) {
  DECODE_START_LEGACY_COMPAT_LEN(3, 3, 3, bl);
  if (struct_v >= 2)
    decode(stamp, bl);
  decode(metablob, bl);
  decode(inos, bl);
  if (struct_v >= 4)
    decode(snap_inos, bl);
  DECODE_FINISH(bl);
}

void EOpen::dump(Formatter *f) const
{
  f->open_object_section("metablob");
  metablob.dump(f);
  f->close_section(); // metablob
  f->open_array_section("inos involved");
  for (vector<inodeno_t>::const_iterator i = inos.begin();
       i != inos.end(); ++i) {
    f->dump_int("ino", *i);
  }
  f->close_section(); // inos
}

void EOpen::generate_test_instances(list<EOpen*>& ls)
{
  ls.push_back(new EOpen());
  ls.push_back(new EOpen());
  ls.back()->add_ino(0);
}

void EOpen::update_segment()
{
  // ??
}

void EOpen::replay(MDSRank *mds)
{
  dout(10) << "EOpen.replay " << dendl;
  auto&& segment = get_segment();
  metablob.replay(mds, segment);

  // note which segments inodes belong to, so we don't have to start rejournaling them
  for (const auto &ino : inos) {
    CInode *in = mds->mdcache->get_inode(ino);
    if (!in) {
      dout(0) << "EOpen.replay ino " << ino << " not in metablob" << dendl;
      ceph_assert(in);
    }
    segment->open_files.push_back(&in->item_open_file);
  }
  for (const auto &vino : snap_inos) {
    CInode *in = mds->mdcache->get_inode(vino);
    if (!in) {
      dout(0) << "EOpen.replay ino " << vino << " not in metablob" << dendl;
      ceph_assert(in);
    }
    segment->open_files.push_back(&in->item_open_file);
  }
}


// -----------------------
// ECommitted

void ECommitted::replay(MDSRank *mds)
{
  if (mds->mdcache->uncommitted_masters.count(reqid)) {
    dout(10) << "ECommitted.replay " << reqid << dendl;
    mds->mdcache->uncommitted_masters[reqid].ls->uncommitted_masters.erase(reqid);
    mds->mdcache->uncommitted_masters.erase(reqid);
  } else {
    dout(10) << "ECommitted.replay " << reqid << " -- didn't see original op" << dendl;
  }
}

void ECommitted::encode(bufferlist& bl, uint64_t features) const
{
  ENCODE_START(3, 3, bl);
  encode(stamp, bl);
  encode(reqid, bl);
  ENCODE_FINISH(bl);
} 

void ECommitted::decode(bufferlist::const_iterator& bl)
{
  DECODE_START_LEGACY_COMPAT_LEN(3, 3, 3, bl);
  if (struct_v >= 2)
    decode(stamp, bl);
  decode(reqid, bl);
  DECODE_FINISH(bl);
}

void ECommitted::dump(Formatter *f) const {
  f->dump_stream("stamp") << stamp;
  f->dump_stream("reqid") << reqid;
}

void ECommitted::generate_test_instances(list<ECommitted*>& ls)
{
  ls.push_back(new ECommitted);
  ls.push_back(new ECommitted);
  ls.back()->stamp = utime_t(1, 2);
  ls.back()->reqid = metareqid_t(entity_name_t::CLIENT(123), 456);
}

// -----------------------
// ESlaveUpdate

void link_rollback::encode(bufferlist &bl) const
{
  ENCODE_START(3, 2, bl);
  encode(reqid, bl);
  encode(ino, bl);
  encode(was_inc, bl);
  encode(old_ctime, bl);
  encode(old_dir_mtime, bl);
  encode(old_dir_rctime, bl);
  encode(snapbl, bl);
  ENCODE_FINISH(bl);
}

void link_rollback::decode(bufferlist::const_iterator &bl)
{
  DECODE_START_LEGACY_COMPAT_LEN(3, 2, 2, bl);
  decode(reqid, bl);
  decode(ino, bl);
  decode(was_inc, bl);
  decode(old_ctime, bl);
  decode(old_dir_mtime, bl);
  decode(old_dir_rctime, bl);
  if (struct_v >= 3)
    decode(snapbl, bl);
  DECODE_FINISH(bl);
}

void link_rollback::dump(Formatter *f) const
{
  f->dump_stream("metareqid") << reqid;
  f->dump_int("ino", ino);
  f->dump_string("was incremented", was_inc ? "true" : "false");
  f->dump_stream("old_ctime") << old_ctime;
  f->dump_stream("old_dir_mtime") << old_dir_mtime;
  f->dump_stream("old_dir_rctime") << old_dir_rctime;
}

void link_rollback::generate_test_instances(list<link_rollback*>& ls)
{
  ls.push_back(new link_rollback());
}

void rmdir_rollback::encode(bufferlist& bl) const
{
  ENCODE_START(3, 2, bl);
  encode(reqid, bl);
  encode(src_dir, bl);
  encode(src_dname, bl);
  encode(dest_dir, bl);
  encode(dest_dname, bl);
  encode(snapbl, bl);
  ENCODE_FINISH(bl);
}

void rmdir_rollback::decode(bufferlist::const_iterator& bl)
{
  DECODE_START_LEGACY_COMPAT_LEN(3, 2, 2, bl);
  decode(reqid, bl);
  decode(src_dir, bl);
  decode(src_dname, bl);
  decode(dest_dir, bl);
  decode(dest_dname, bl);
  if (struct_v >= 3)
    decode(snapbl, bl);
  DECODE_FINISH(bl);
}

void rmdir_rollback::dump(Formatter *f) const
{
  f->dump_stream("metareqid") << reqid;
  f->dump_stream("source directory") << src_dir;
  f->dump_string("source dname", src_dname);
  f->dump_stream("destination directory") << dest_dir;
  f->dump_string("destination dname", dest_dname);
}

void rmdir_rollback::generate_test_instances(list<rmdir_rollback*>& ls)
{
  ls.push_back(new rmdir_rollback());
}

void rename_rollback::drec::encode(bufferlist &bl) const
{
  ENCODE_START(2, 2, bl);
  encode(dirfrag, bl);
  encode(dirfrag_old_mtime, bl);
  encode(dirfrag_old_rctime, bl);
  encode(ino, bl);
  encode(remote_ino, bl);
  encode(dname, bl);
  encode(remote_d_type, bl);
  encode(old_ctime, bl);
  ENCODE_FINISH(bl);
}

void rename_rollback::drec::decode(bufferlist::const_iterator &bl)
{
  DECODE_START_LEGACY_COMPAT_LEN(2, 2, 2, bl);
  decode(dirfrag, bl);
  decode(dirfrag_old_mtime, bl);
  decode(dirfrag_old_rctime, bl);
  decode(ino, bl);
  decode(remote_ino, bl);
  decode(dname, bl);
  decode(remote_d_type, bl);
  decode(old_ctime, bl);
  DECODE_FINISH(bl);
}

void rename_rollback::drec::dump(Formatter *f) const
{
  f->dump_stream("directory fragment") << dirfrag;
  f->dump_stream("directory old mtime") << dirfrag_old_mtime;
  f->dump_stream("directory old rctime") << dirfrag_old_rctime;
  f->dump_int("ino", ino);
  f->dump_int("remote ino", remote_ino);
  f->dump_string("dname", dname);
  uint32_t type = DTTOIF(remote_d_type) & S_IFMT; // convert to type entries
  string type_string;
  switch(type) {
  case S_IFREG:
    type_string = "file"; break;
  case S_IFLNK:
    type_string = "symlink"; break;
  case S_IFDIR:
    type_string = "directory"; break;
  default:
    type_string = "UNKNOWN-" + stringify((int)type); break;
  }
  f->dump_string("remote dtype", type_string);
  f->dump_stream("old ctime") << old_ctime;
}

void rename_rollback::drec::generate_test_instances(list<drec*>& ls)
{
  ls.push_back(new drec());
  ls.back()->remote_d_type = IFTODT(S_IFREG);
}

void rename_rollback::encode(bufferlist &bl) const
{
  ENCODE_START(3, 2, bl);
  encode(reqid, bl);
  encode(orig_src, bl);
  encode(orig_dest, bl);
  encode(stray, bl);
  encode(ctime, bl);
  encode(srci_snapbl, bl);
  encode(desti_snapbl, bl);
  ENCODE_FINISH(bl);
}

void rename_rollback::decode(bufferlist::const_iterator &bl)
{
  DECODE_START_LEGACY_COMPAT_LEN(3, 2, 2, bl);
  decode(reqid, bl);
  decode(orig_src, bl);
  decode(orig_dest, bl);
  decode(stray, bl);
  decode(ctime, bl);
  if (struct_v >= 3) {
    decode(srci_snapbl, bl);
    decode(desti_snapbl, bl);
  }
  DECODE_FINISH(bl);
}

void rename_rollback::dump(Formatter *f) const
{
  f->dump_stream("request id") << reqid;
  f->open_object_section("original src drec");
  orig_src.dump(f);
  f->close_section(); // original src drec
  f->open_object_section("original dest drec");
  orig_dest.dump(f);
  f->close_section(); // original dest drec
  f->open_object_section("stray drec");
  stray.dump(f);
  f->close_section(); // stray drec
  f->dump_stream("ctime") << ctime;
}

void rename_rollback::generate_test_instances(list<rename_rollback*>& ls)
{
  ls.push_back(new rename_rollback());
  ls.back()->orig_src.remote_d_type = IFTODT(S_IFREG);
  ls.back()->orig_dest.remote_d_type = IFTODT(S_IFREG);
  ls.back()->stray.remote_d_type = IFTODT(S_IFREG);
}

void ESlaveUpdate::encode(bufferlist &bl, uint64_t features) const
{
  ENCODE_START(3, 3, bl);
  encode(stamp, bl);
  encode(type, bl);
  encode(reqid, bl);
  encode(master, bl);
  encode(op, bl);
  encode(origop, bl);
  encode(commit, bl, features);
  encode(rollback, bl);
  ENCODE_FINISH(bl);
} 

void ESlaveUpdate::decode(bufferlist::const_iterator &bl)
{
  DECODE_START_LEGACY_COMPAT_LEN(3, 3, 3, bl);
  if (struct_v >= 2)
    decode(stamp, bl);
  decode(type, bl);
  decode(reqid, bl);
  decode(master, bl);
  decode(op, bl);
  decode(origop, bl);
  decode(commit, bl);
  decode(rollback, bl);
  DECODE_FINISH(bl);
}

void ESlaveUpdate::dump(Formatter *f) const
{
  f->open_object_section("metablob");
  commit.dump(f);
  f->close_section(); // metablob

  f->dump_int("rollback length", rollback.length());
  f->dump_string("type", type);
  f->dump_stream("metareqid") << reqid;
  f->dump_int("master", master);
  f->dump_int("op", op);
  f->dump_int("original op", origop);
}

void ESlaveUpdate::generate_test_instances(list<ESlaveUpdate*>& ls)
{
  ls.push_back(new ESlaveUpdate());
}


void ESlaveUpdate::replay(MDSRank *mds)
{
  MDSlaveUpdate *su;
  auto&& segment = get_segment();
  switch (op) {
  case ESlaveUpdate::OP_PREPARE:
    dout(10) << "ESlaveUpdate.replay prepare " << reqid << " for mds." << master 
	     << ": applying commit, saving rollback info" << dendl;
    su = new MDSlaveUpdate(origop, rollback, segment->slave_updates);
    commit.replay(mds, segment, su);
    mds->mdcache->add_uncommitted_slave_update(reqid, master, su);
    break;

  case ESlaveUpdate::OP_COMMIT:
    su = mds->mdcache->get_uncommitted_slave_update(reqid, master);
    if (su) {
      dout(10) << "ESlaveUpdate.replay commit " << reqid << " for mds." << master << dendl;
      mds->mdcache->finish_uncommitted_slave_update(reqid, master);
    } else {
      dout(10) << "ESlaveUpdate.replay commit " << reqid << " for mds." << master 
	       << ": ignoring, no previously saved prepare" << dendl;
    }
    break;

  case ESlaveUpdate::OP_ROLLBACK:
    dout(10) << "ESlaveUpdate.replay abort " << reqid << " for mds." << master
	     << ": applying rollback commit blob" << dendl;
    commit.replay(mds, segment);
    su = mds->mdcache->get_uncommitted_slave_update(reqid, master);
    if (su)
      mds->mdcache->finish_uncommitted_slave_update(reqid, master);
    break;

  default:
    mds->clog->error() << "invalid op in ESlaveUpdate";
    mds->damaged();
    ceph_abort();  // Should be unreachable because damaged() calls respawn()
  }
}


// -----------------------
// ESubtreeMap

void ESubtreeMap::encode(bufferlist& bl, uint64_t features) const
{
  ENCODE_START(6, 5, bl);
  encode(stamp, bl);
  encode(metablob, bl, features);
  encode(subtrees, bl);
  encode(ambiguous_subtrees, bl);
  encode(expire_pos, bl);
  encode(event_seq, bl);
  ENCODE_FINISH(bl);
}
 
void ESubtreeMap::decode(bufferlist::const_iterator &bl)
{
  DECODE_START_LEGACY_COMPAT_LEN(6, 5, 5, bl);
  if (struct_v >= 2)
    decode(stamp, bl);
  decode(metablob, bl);
  decode(subtrees, bl);
  if (struct_v >= 4)
    decode(ambiguous_subtrees, bl);
  if (struct_v >= 3)
    decode(expire_pos, bl);
  if (struct_v >= 6)
    decode(event_seq, bl);
  DECODE_FINISH(bl);
}

void ESubtreeMap::dump(Formatter *f) const
{
  f->open_object_section("metablob");
  metablob.dump(f);
  f->close_section(); // metablob
  
  f->open_array_section("subtrees");
  for(map<dirfrag_t,vector<dirfrag_t> >::const_iterator i = subtrees.begin();
      i != subtrees.end(); ++i) {
    f->open_object_section("tree");
    f->dump_stream("root dirfrag") << i->first;
    for (vector<dirfrag_t>::const_iterator j = i->second.begin();
	 j != i->second.end(); ++j) {
      f->dump_stream("bound dirfrag") << *j;
    }
    f->close_section(); // tree
  }
  f->close_section(); // subtrees

  f->open_array_section("ambiguous subtrees");
  for(set<dirfrag_t>::const_iterator i = ambiguous_subtrees.begin();
      i != ambiguous_subtrees.end(); ++i) {
    f->dump_stream("dirfrag") << *i;
  }
  f->close_section(); // ambiguous subtrees

  f->dump_int("expire position", expire_pos);
}

void ESubtreeMap::generate_test_instances(list<ESubtreeMap*>& ls)
{
  ls.push_back(new ESubtreeMap());
}

void ESubtreeMap::replay(MDSRank *mds) 
{
  if (expire_pos && expire_pos > mds->mdlog->journaler->get_expire_pos())
    mds->mdlog->journaler->set_expire_pos(expire_pos);

  // suck up the subtree map?
  if (mds->mdcache->is_subtrees()) {
    dout(10) << "ESubtreeMap.replay -- i already have import map; verifying" << dendl;
    int errors = 0;

    for (map<dirfrag_t, vector<dirfrag_t> >::iterator p = subtrees.begin();
	 p != subtrees.end();
	 ++p) {
      CDir *dir = mds->mdcache->get_dirfrag(p->first);
      if (!dir) {
	mds->clog->error() << " replayed ESubtreeMap at " << get_start_off()
			  << " subtree root " << p->first << " not in cache";
	++errors;
	continue;
      }
      
      if (!mds->mdcache->is_subtree(dir)) {
	mds->clog->error() << " replayed ESubtreeMap at " << get_start_off()
			  << " subtree root " << p->first << " not a subtree in cache";
	++errors;
	continue;
      }
      if (dir->get_dir_auth().first != mds->get_nodeid()) {
	mds->clog->error() << " replayed ESubtreeMap at " << get_start_off()
			  << " subtree root " << p->first
			  << " is not mine in cache (it's " << dir->get_dir_auth() << ")";
	++errors;
	continue;
      }

      for (vector<dirfrag_t>::iterator q = p->second.begin(); q != p->second.end(); ++q)
	mds->mdcache->get_force_dirfrag(*q, true);

      set<CDir*> bounds;
      mds->mdcache->get_subtree_bounds(dir, bounds);
      for (vector<dirfrag_t>::iterator q = p->second.begin(); q != p->second.end(); ++q) {
	CDir *b = mds->mdcache->get_dirfrag(*q);
	if (!b) {
	  mds->clog->error() << " replayed ESubtreeMap at " << get_start_off()
			    << " subtree " << p->first << " bound " << *q << " not in cache";
	++errors;
	  continue;
	}
	if (bounds.count(b) == 0) {
	  mds->clog->error() << " replayed ESubtreeMap at " << get_start_off()
			    << " subtree " << p->first << " bound " << *q << " not a bound in cache";
	++errors;
	  continue;
	}
	bounds.erase(b);
      }
      for (set<CDir*>::iterator q = bounds.begin(); q != bounds.end(); ++q) {
	mds->clog->error() << " replayed ESubtreeMap at " << get_start_off()
			  << " subtree " << p->first << " has extra bound in cache " << (*q)->dirfrag();
	++errors;
      }
      
      if (ambiguous_subtrees.count(p->first)) {
	if (!mds->mdcache->have_ambiguous_import(p->first)) {
	  mds->clog->error() << " replayed ESubtreeMap at " << get_start_off()
			    << " subtree " << p->first << " is ambiguous but is not in our cache";
	  ++errors;
	}
      } else {
	if (mds->mdcache->have_ambiguous_import(p->first)) {
	  mds->clog->error() << " replayed ESubtreeMap at " << get_start_off()
			    << " subtree " << p->first << " is not ambiguous but is in our cache";
	  ++errors;
	}
      }
    }
    
    std::vector<CDir*> dirs;
    mds->mdcache->get_subtrees(dirs);
    for (const auto& dir : dirs) {
      if (dir->get_dir_auth().first != mds->get_nodeid())
	continue;
      if (subtrees.count(dir->dirfrag()) == 0) {
	mds->clog->error() << " replayed ESubtreeMap at " << get_start_off()
			  << " does not include cache subtree " << dir->dirfrag();
	++errors;
      }
    }

    if (errors) {
      dout(0) << "journal subtrees: " << subtrees << dendl;
      dout(0) << "journal ambig_subtrees: " << ambiguous_subtrees << dendl;
      mds->mdcache->show_subtrees();
      ceph_assert(!g_conf()->mds_debug_subtrees || errors == 0);
    }
    return;
  }

  dout(10) << "ESubtreeMap.replay -- reconstructing (auth) subtree spanning tree" << dendl;
  
  // first, stick the spanning tree in my cache
  //metablob.print(*_dout);
  metablob.replay(mds, get_segment());
  
  // restore import/export maps
  for (map<dirfrag_t, vector<dirfrag_t> >::iterator p = subtrees.begin();
       p != subtrees.end();
       ++p) {
    CDir *dir = mds->mdcache->get_dirfrag(p->first);
    ceph_assert(dir);
    if (ambiguous_subtrees.count(p->first)) {
      // ambiguous!
      mds->mdcache->add_ambiguous_import(p->first, p->second);
      mds->mdcache->adjust_bounded_subtree_auth(dir, p->second,
						mds_authority_t(mds->get_nodeid(), mds->get_nodeid()));
    } else {
      // not ambiguous
      mds->mdcache->adjust_bounded_subtree_auth(dir, p->second, mds->get_nodeid());
    }
  }

  mds->mdcache->recalc_auth_bits(true);

  mds->mdcache->show_subtrees();
}



// -----------------------
// EFragment

void EFragment::replay(MDSRank *mds)
{
  dout(10) << "EFragment.replay " << op_name(op) << " " << ino << " " << basefrag << " by " << bits << dendl;

  list<CDir*> resultfrags;
  MDSContext::vec waiters;

  // in may be NULL if it wasn't in our cache yet.  if it's a prepare
  // it will be once we replay the metablob , but first we need to
  // refragment anything we already have in the cache.
  CInode *in = mds->mdcache->get_inode(ino);

  auto&& segment = get_segment();
  switch (op) {
  case OP_PREPARE:
    mds->mdcache->add_uncommitted_fragment(dirfrag_t(ino, basefrag), bits, orig_frags, segment, &rollback);

    if (in)
      mds->mdcache->adjust_dir_fragments(in, basefrag, bits, resultfrags, waiters, true);
    break;

  case OP_ROLLBACK: {
    frag_vec_t old_frags;
    if (in) {
      in->dirfragtree.get_leaves_under(basefrag, old_frags);
      if (orig_frags.empty()) {
	// old format EFragment
	mds->mdcache->adjust_dir_fragments(in, basefrag, -bits, resultfrags, waiters, true);
      } else {
	for (const auto& fg : orig_frags)
	  mds->mdcache->force_dir_fragment(in, fg);
      }
    }
    mds->mdcache->rollback_uncommitted_fragment(dirfrag_t(ino, basefrag), std::move(old_frags));
    break;
  }

  case OP_COMMIT:
  case OP_FINISH:
    mds->mdcache->finish_uncommitted_fragment(dirfrag_t(ino, basefrag), op);
    break;

  default:
    ceph_abort();
  }

  metablob.replay(mds, segment);
  if (in && g_conf()->mds_debug_frag)
    in->verify_dirfrags();
}

void EFragment::encode(bufferlist &bl, uint64_t features) const {
  ENCODE_START(5, 4, bl);
  encode(stamp, bl);
  encode(op, bl);
  encode(ino, bl);
  encode(basefrag, bl);
  encode(bits, bl);
  encode(metablob, bl, features);
  encode(orig_frags, bl);
  encode(rollback, bl);
  ENCODE_FINISH(bl);
}

void EFragment::decode(bufferlist::const_iterator &bl) {
  DECODE_START_LEGACY_COMPAT_LEN(5, 4, 4, bl);
  if (struct_v >= 2)
    decode(stamp, bl);
  if (struct_v >= 3)
    decode(op, bl);
  decode(ino, bl);
  decode(basefrag, bl);
  decode(bits, bl);
  decode(metablob, bl);
  if (struct_v >= 5) {
    decode(orig_frags, bl);
    decode(rollback, bl);
  }
  DECODE_FINISH(bl);
}

void EFragment::dump(Formatter *f) const
{
  /*f->open_object_section("Metablob");
  metablob.dump(f); // sadly we don't have this; dunno if we'll get it
  f->close_section();*/
  f->dump_string("op", op_name(op));
  f->dump_stream("ino") << ino;
  f->dump_stream("base frag") << basefrag;
  f->dump_int("bits", bits);
}

void EFragment::generate_test_instances(list<EFragment*>& ls)
{
  ls.push_back(new EFragment);
  ls.push_back(new EFragment);
  ls.back()->op = OP_PREPARE;
  ls.back()->ino = 1;
  ls.back()->bits = 5;
}

void dirfrag_rollback::encode(bufferlist &bl) const
{
  ENCODE_START(1, 1, bl);
  encode(fnode, bl);
  ENCODE_FINISH(bl);
}

void dirfrag_rollback::decode(bufferlist::const_iterator &bl)
{
  DECODE_START(1, bl);
  decode(fnode, bl);
  DECODE_FINISH(bl);
}



// =========================================================================

// -----------------------
// EExport

void EExport::replay(MDSRank *mds)
{
  dout(10) << "EExport.replay " << base << dendl;
  auto&& segment = get_segment();
  metablob.replay(mds, segment);
  
  CDir *dir = mds->mdcache->get_dirfrag(base);
  ceph_assert(dir);
  
  set<CDir*> realbounds;
  for (set<dirfrag_t>::iterator p = bounds.begin();
       p != bounds.end();
       ++p) {
    CDir *bd = mds->mdcache->get_dirfrag(*p);
    ceph_assert(bd);
    realbounds.insert(bd);
  }

  // adjust auth away
  mds->mdcache->adjust_bounded_subtree_auth(dir, realbounds, CDIR_AUTH_UNDEF);

  mds->mdcache->try_trim_non_auth_subtree(dir);
}

void EExport::encode(bufferlist& bl, uint64_t features) const
{
  ENCODE_START(4, 3, bl);
  encode(stamp, bl);
  encode(metablob, bl, features);
  encode(base, bl);
  encode(bounds, bl);
  encode(target, bl);
  ENCODE_FINISH(bl);
}

void EExport::decode(bufferlist::const_iterator &bl)
{
  DECODE_START_LEGACY_COMPAT_LEN(3, 3, 3, bl);
  if (struct_v >= 2)
    decode(stamp, bl);
  decode(metablob, bl);
  decode(base, bl);
  decode(bounds, bl);
  if (struct_v >= 4)
    decode(target, bl);
  DECODE_FINISH(bl);
}

void EExport::dump(Formatter *f) const
{
  f->dump_float("stamp", (double)stamp);
  /*f->open_object_section("Metablob");
  metablob.dump(f); // sadly we don't have this; dunno if we'll get it
  f->close_section();*/
  f->dump_stream("base dirfrag") << base;
  f->open_array_section("bounds dirfrags");
  for (set<dirfrag_t>::const_iterator i = bounds.begin();
      i != bounds.end(); ++i) {
    f->dump_stream("dirfrag") << *i;
  }
  f->close_section(); // bounds dirfrags
}

void EExport::generate_test_instances(list<EExport*>& ls)
{
  EExport *sample = new EExport();
  ls.push_back(sample);
}


// -----------------------
// EImportStart

void EImportStart::update_segment()
{
  get_segment()->sessionmapv = cmapv;
}

void EImportStart::replay(MDSRank *mds)
{
  dout(10) << "EImportStart.replay " << base << " bounds " << bounds << dendl;
  //metablob.print(*_dout);
  auto&& segment = get_segment();
  metablob.replay(mds, segment);

  // put in ambiguous import list
  mds->mdcache->add_ambiguous_import(base, bounds);

  // set auth partially to us so we don't trim it
  CDir *dir = mds->mdcache->get_dirfrag(base);
  ceph_assert(dir);

  set<CDir*> realbounds;
  for (vector<dirfrag_t>::iterator p = bounds.begin();
       p != bounds.end();
       ++p) {
    CDir *bd = mds->mdcache->get_dirfrag(*p);
    ceph_assert(bd);
    if (!bd->is_subtree_root())
      bd->state_clear(CDir::STATE_AUTH);
    realbounds.insert(bd);
  }

  mds->mdcache->adjust_bounded_subtree_auth(dir, realbounds,
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
    mds->sessionmap.replay_open_sessions(cm, cmm);

    if (mds->sessionmap.get_version() != cmapv) {
      derr << "sessionmap version " << mds->sessionmap.get_version()
           << " != cmapv " << cmapv << dendl;
      mds->clog->error() << "failure replaying journal (EImportStart)";
      mds->damaged();
      ceph_abort();  // Should be unreachable because damaged() calls respawn()
    }
  }
  update_segment();
}

void EImportStart::encode(bufferlist &bl, uint64_t features) const {
  ENCODE_START(4, 3, bl);
  encode(stamp, bl);
  encode(base, bl);
  encode(metablob, bl, features);
  encode(bounds, bl);
  encode(cmapv, bl);
  encode(client_map, bl);
  encode(from, bl);
  ENCODE_FINISH(bl);
}

void EImportStart::decode(bufferlist::const_iterator &bl) {
  DECODE_START_LEGACY_COMPAT_LEN(3, 3, 3, bl);
  if (struct_v >= 2)
    decode(stamp, bl);
  decode(base, bl);
  decode(metablob, bl);
  decode(bounds, bl);
  decode(cmapv, bl);
  decode(client_map, bl);
  if (struct_v >= 4)
    decode(from, bl);
  DECODE_FINISH(bl);
}

void EImportStart::dump(Formatter *f) const
{
  f->dump_stream("base dirfrag") << base;
  f->open_array_section("boundary dirfrags");
  for (vector<dirfrag_t>::const_iterator iter = bounds.begin();
      iter != bounds.end(); ++iter) {
    f->dump_stream("frag") << *iter;
  }
  f->close_section();
}

void EImportStart::generate_test_instances(list<EImportStart*>& ls)
{
  ls.push_back(new EImportStart);
}

// -----------------------
// EImportFinish

void EImportFinish::replay(MDSRank *mds)
{
  if (mds->mdcache->have_ambiguous_import(base)) {
    dout(10) << "EImportFinish.replay " << base << " success=" << success << dendl;
    if (success) {
      mds->mdcache->finish_ambiguous_import(base);
    } else {
      CDir *dir = mds->mdcache->get_dirfrag(base);
      ceph_assert(dir);
      vector<dirfrag_t> bounds;
      mds->mdcache->get_ambiguous_import_bounds(base, bounds);
      mds->mdcache->adjust_bounded_subtree_auth(dir, bounds, CDIR_AUTH_UNDEF);
      mds->mdcache->cancel_ambiguous_import(dir);
      mds->mdcache->try_trim_non_auth_subtree(dir);
   }
  } else {
    // this shouldn't happen unless this is an old journal
    dout(10) << "EImportFinish.replay " << base << " success=" << success
	     << " on subtree not marked as ambiguous" 
	     << dendl;
    mds->clog->error() << "failure replaying journal (EImportFinish)";
    mds->damaged();
    ceph_abort();  // Should be unreachable because damaged() calls respawn()
  }
}

void EImportFinish::encode(bufferlist& bl, uint64_t features) const
{
  ENCODE_START(3, 3, bl);
  encode(stamp, bl);
  encode(base, bl);
  encode(success, bl);
  ENCODE_FINISH(bl);
}

void EImportFinish::decode(bufferlist::const_iterator &bl)
{
  DECODE_START_LEGACY_COMPAT_LEN(3, 3, 3, bl);
  if (struct_v >= 2)
    decode(stamp, bl);
  decode(base, bl);
  decode(success, bl);
  DECODE_FINISH(bl);
}

void EImportFinish::dump(Formatter *f) const
{
  f->dump_stream("base dirfrag") << base;
  f->dump_string("success", success ? "true" : "false");
}
void EImportFinish::generate_test_instances(list<EImportFinish*>& ls)
{
  ls.push_back(new EImportFinish);
  ls.push_back(new EImportFinish);
  ls.back()->success = true;
}


// ------------------------
// EResetJournal

void EResetJournal::encode(bufferlist& bl, uint64_t features) const
{
  ENCODE_START(2, 2, bl);
  encode(stamp, bl);
  ENCODE_FINISH(bl);
}
 
void EResetJournal::decode(bufferlist::const_iterator &bl)
{
  DECODE_START_LEGACY_COMPAT_LEN(2, 2, 2, bl);
  decode(stamp, bl);
  DECODE_FINISH(bl);
}

void EResetJournal::dump(Formatter *f) const
{
  f->dump_stream("timestamp") << stamp;
}

void EResetJournal::generate_test_instances(list<EResetJournal*>& ls)
{
  ls.push_back(new EResetJournal());
}

void EResetJournal::replay(MDSRank *mds)
{
  dout(1) << "EResetJournal" << dendl;

  mds->sessionmap.wipe();
  mds->inotable->replay_reset();

  if (mds->mdsmap->get_root() == mds->get_nodeid()) {
    CDir *rootdir = mds->mdcache->get_root()->get_or_open_dirfrag(mds->mdcache, frag_t());
    mds->mdcache->adjust_subtree_auth(rootdir, mds->get_nodeid());   
  }

  CDir *mydir = mds->mdcache->get_myin()->get_or_open_dirfrag(mds->mdcache, frag_t());
  mds->mdcache->adjust_subtree_auth(mydir, mds->get_nodeid());   

  mds->mdcache->recalc_auth_bits(true);

  mds->mdcache->show_subtrees();
}


void ENoOp::encode(bufferlist &bl, uint64_t features) const
{
  ENCODE_START(2, 2, bl);
  encode(pad_size, bl);
  uint8_t const pad = 0xff;
  for (unsigned int i = 0; i < pad_size; ++i) {
    encode(pad, bl);
  }
  ENCODE_FINISH(bl);
}


void ENoOp::decode(bufferlist::const_iterator &bl)
{
  DECODE_START(2, bl);
  decode(pad_size, bl);
  if (bl.get_remaining() != pad_size) {
    // This is spiritually an assertion, but expressing in a way that will let
    // journal debug tools catch it and recognise a malformed entry.
    throw buffer::end_of_buffer();
  } else {
    bl.advance(pad_size);
  }
  DECODE_FINISH(bl);
}


void ENoOp::replay(MDSRank *mds)
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
bool EMetaBlob::rewrite_truncate_finish(MDSRank const *mds,
    std::map<log_segment_seq_t, log_segment_seq_t> const &old_to_new)
{
  bool modified = false;
  map<inodeno_t, log_segment_seq_t> new_trunc_finish;
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
