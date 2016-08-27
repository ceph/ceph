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
#include "events/EOpen.h"

#include "events/ETableClient.h"
#include "events/ETableServer.h"

#include "include/stringify.h"


#include "CInode.h"
#include "CDir.h"
#include "CDentry.h"

#include "MDSRank.h"
#include "MDCache.h"
#include "Server.h"
#include "Locker.h"

#include "MDLog.h"
#include "SessionMap.h"
#include "InoTable.h"
#include "LogEvent.h"
#include "LogSegment.h"

#define dout_subsys ceph_subsys_mds
#undef DOUT_COND
#define DOUT_COND(cct, l) (l<=cct->_conf->debug_mds || l <= cct->_conf->debug_mds_log \
			      || l <= cct->_conf->debug_mds_log_expire)
#undef dout_prefix
#define dout_prefix *_dout << "mds." << mds->get_nodeid() << ".journal "


// -----------------------
// LogSegment

void LogSegment::try_to_expire(MDSRank *mds, MDSGatherBuilder &gather_bld, int op_prio)
{
  set<CObjectRef> commit;

  dout(6) << "LogSegment(" << seq << "/" << offset << ").try_to_expire" << dendl;

  assert(g_conf->mds_kill_journal_expire_at != 1);

  mds->mdcache->lock_log_segments();
  // commit dirs
  for (elist<CDir*>::iterator p = new_dirfrags.begin(); !p.end(); ++p) {
    dout(20) << " new_dirfrag " << **p << dendl;
    commit.insert(*p);
  }
  for (elist<CDir*>::iterator p = dirty_dirfrags.begin(); !p.end(); ++p) {
    dout(20) << " dirty_dirfrag " << **p << dendl;
    commit.insert(*p);
  }
  for (elist<CDentry*>::iterator p = dirty_dentries.begin(); !p.end(); ++p) {
    dout(20) << " dirty_dentry " << **p << dendl;
    commit.insert((*p)->get_dir());
  }
  for (elist<CInode*>::iterator p = dirty_inodes.begin(); !p.end(); ++p) {
    dout(20) << " dirty_inode " << **p << dendl;
    if ((*p)->is_base())
      commit.insert(*p);
    else
      commit.insert((*p)->get_parent_dn()->get_dir());
  }
  mds->mdcache->unlock_log_segments();

  for (auto p = commit.begin(); p != commit.end(); ++p) {
    if (CDir *dir = dynamic_cast<CDir*>(p->get())) {
      dir->get_inode()->mutex_lock();
      dout(15) << "try_to_expire committing " << *dir << dendl;
      dir->commit(gather_bld.new_sub(), op_prio);
      dir->get_inode()->mutex_unlock();
    } else if (CInode *in = dynamic_cast<CInode*>(p->get())) {
      dout(15) << "try_to_expire committing " << *in << dendl;
      in->mutex_lock();
      in->store(gather_bld.new_sub());
      in->mutex_unlock();
    } else {
      assert(0);
    }
  }

#if 0
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
#endif

  map<CInodeRef, int> dirty_locks;
  mds->mdcache->lock_log_segments();
  // nudge scatterlocks
  for (elist<CInode*>::iterator p = dirty_dirfrag_dir.begin(); !p.end(); ++p) {
    dirty_locks[*p] |= CEPH_LOCK_IFILE;
  }
  for (elist<CInode*>::iterator p = dirty_dirfrag_nest.begin(); !p.end(); ++p) {
    dirty_locks[*p] |= CEPH_LOCK_INEST;
  }
#if 0
  for (elist<CInode*>::iterator p = dirty_dirfrag_dirfragtree.begin(); !p.end(); ++p) {
    CInode *in = *p;
    dout(10) << "try_to_expire waiting for dirfragtreelock flush on " << *in << dendl;
    mds->locker->scatter_nudge(&in->dirfragtreelock, gather_bld.new_sub());
  }
#endif
  mds->mdcache->unlock_log_segments();

  for (auto p = dirty_locks.begin(); p != dirty_locks.end(); ++p) {
    CInode *in = p->first.get();
    in->mutex_lock();
    if (p->second & CEPH_LOCK_IFILE) {
      dout(10) << "try_to_expire waiting for dirlock flush on " << *in << dendl;
      mds->locker->scatter_nudge(&in->filelock, gather_bld.new_sub());
    }
    if (p->second & CEPH_LOCK_INEST) {
      dout(10) << "try_to_expire waiting for nest flush on " << *in << dendl;
      mds->locker->scatter_nudge(&in->nestlock, gather_bld.new_sub());
    }
    in->mutex_unlock();
  }

  assert(g_conf->mds_kill_journal_expire_at != 2);

#if 0
  // open files and snap inodes 
  if (!open_files.empty()) {
    assert(!mds->mdlog->is_capped()); // hmm FIXME
    EOpen *le = 0;
    LogSegment *ls = mds->mdlog->get_current_segment();
    assert(ls != this);
    elist<CInode*>::iterator p = open_files.begin(member_offset(CInode, item_open_file));
    while (!p.end()) {
      CInode *in = *p;
      ++p;
      if (in->last == CEPH_NOSNAP && in->is_auth() &&
	  !in->is_ambiguous_auth() && in->is_any_caps()) {
	if (in->is_any_caps_wanted()) {
	  dout(20) << "try_to_expire requeueing open file " << *in << dendl;
	  if (!le) {
	    le = new EOpen(mds->mdlog);
	    mds->mdlog->start_entry(le);
	  }
	  le->add_clean_inode(in);
	  ls->open_files.push_back(&in->item_open_file);
	} else {
	  // drop inodes that aren't wanted
	  dout(20) << "try_to_expire not requeueing and delisting unwanted file " << *in << dendl;
	  in->item_open_file.remove_myself();
	}
      } else if (in->last != CEPH_NOSNAP && !in->client_snap_caps.empty()) {
	// journal snap inodes that need flush. This simplify the mds failover hanlding
	dout(20) << "try_to_expire requeueing snap needflush inode " << *in << dendl;
	if (!le) {
	  le = new EOpen(mds->mdlog);
	  mds->mdlog->start_entry(le);
	}
	le->add_clean_inode(in);
	ls->open_files.push_back(&in->item_open_file);
      } else {
	/*
	 * we can get a capless inode here if we replay an open file, the client fails to
	 * reconnect it, but does REPLAY an open request (that adds it to the logseg).  AFAICS
	 * it's ok for the client to replay an open on a file it doesn't have in it's cache
	 * anymore.
	 *
	 * this makes the mds less sensitive to strict open_file consistency, although it does
	 * make it easier to miss subtle problems.
	 */
	dout(20) << "try_to_expire not requeueing and delisting capless file " << *in << dendl;
	in->item_open_file.remove_myself();
      }
    }
    if (le) {
      mds->mdlog->submit_entry(le, gather_bld.new_sub());
      dout(10) << "try_to_expire waiting for open files to rejournal" << dendl;
    }
  }
#endif

  assert(g_conf->mds_kill_journal_expire_at != 3);

#if 0
  // backtraces to be stored/updated
  for (elist<CInode*>::iterator p = dirty_parent_inodes.begin(); !p.end(); ++p) {
    CInode *in = *p;
    assert(in->is_auth());
    if (in->can_auth_pin()) {
      dout(15) << "try_to_expire waiting for storing backtrace on " << *in << dendl;
      in->store_backtrace(gather_bld.new_sub(), op_prio);
    } else {
      dout(15) << "try_to_expire waiting for unfreeze on " << *in << dendl;
      in->add_waiter(CInode::WAIT_UNFREEZE, gather_bld.new_sub());
    }
  }

  assert(g_conf->mds_kill_journal_expire_at != 4);

  // slave updates
  for (elist<MDSlaveUpdate*>::iterator p = slave_updates.begin(member_offset(MDSlaveUpdate,
									     item));
       !p.end(); ++p) {
    MDSlaveUpdate *su = *p;
    dout(10) << "try_to_expire waiting on slave update " << su << dendl;
    assert(su->waiter == 0);
    su->waiter = gather_bld.new_sub();
  }
#endif

  // idalloc
  mds->inotable->mutex_lock();
  if (inotablev > mds->inotable->get_committed_version()) {
    dout(10) << "try_to_expire saving inotable table, need " << inotablev
	      << ", committed is " << mds->inotable->get_committed_version()
	      << " (" << mds->inotable->get_committing_version() << ")"
	      << dendl;
    mds->inotable->save(gather_bld.new_sub(), inotablev);
  }
  mds->inotable->mutex_unlock();

  // sessionmap
  mds->sessionmap->mutex_lock();
  if (sessionmapv > mds->sessionmap->get_committed()) {
    dout(10) << "try_to_expire saving sessionmap, need " << sessionmapv 
	      << ", committed is " << mds->sessionmap->get_committed()
	      << " (" << mds->sessionmap->get_committing() << ")"
	      << dendl;
    mds->sessionmap->save(gather_bld.new_sub(), sessionmapv);
  }

  // updates to sessions for completed_requests
  //mds->sessionmap->save_if_dirty(touched_sessions, &gather_bld);
  //touched_sessions.clear();
  mds->sessionmap->mutex_unlock();

#if 0

  // pending commit atids
  for (map<int, ceph::unordered_set<version_t> >::iterator p = pending_commit_tids.begin();
       p != pending_commit_tids.end();
       ++p) {
    MDSTableClient *client = mds->get_table_client(p->first);
    assert(client);
    for (ceph::unordered_set<version_t>::iterator q = p->second.begin();
	 q != p->second.end();
	 ++q) {
      dout(10) << "try_to_expire " << get_mdstable_name(p->first) << " transaction " << *q 
	       << " pending commit (not yet acked), waiting" << dendl;
      assert(!client->has_committed(*q));
      client->wait_for_ack(*q, gather_bld.new_sub());
    }
  }
  
  // table servers
  for (map<int, version_t>::iterator p = tablev.begin();
       p != tablev.end();
       ++p) {
    MDSTableServer *server = mds->get_table_server(p->first);
    assert(server);
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
    assert(g_conf->mds_kill_journal_expire_at != 5);
    dout(6) << "LogSegment(" << seq << "/" << offset << ").try_to_expire success" << dendl;
  }
#endif
}

#undef DOUT_COND
#define DOUT_COND(cct, l) (l<=cct->_conf->debug_mds || l <= cct->_conf->debug_mds_log)


// -----------------------
// EMetaBlob

EMetaBlob::EMetaBlob(MDLog *mdlog) : opened_ino(0), renamed_dirino(0),
				     inotablev(0), sessionmapv(0), allocated_ino(0),
				     last_subtree_map(0), event_seq(0)
{ }

void EMetaBlob::update_segment(LogSegment *ls)
{
#if 0
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
#endif
}

// EMetaBlob::fullbit

void EMetaBlob::fullbit::encode(bufferlist& bl, uint64_t features) const {
  ENCODE_START(8, 5, bl);
  ::encode(dn, bl);
  ::encode(dnfirst, bl);
  ::encode(dnlast, bl);
  ::encode(dnv, bl);
  ::encode(inode, bl, features);
  ::encode(xattrs, bl);
  if (inode.is_symlink())
    ::encode(symlink, bl);
  if (inode.is_dir()) {
    ::encode(dirfragtree, bl);
    ::encode(snapbl, bl);
  }
  ::encode(state, bl);
  if (old_inodes.empty()) {
    ::encode(false, bl);
  } else {
    ::encode(true, bl);
    ::encode(old_inodes, bl, features);
  }
  if (!inode.is_dir())
    ::encode(snapbl, bl);
  ::encode(oldest_snap, bl);
  ENCODE_FINISH(bl);
}

void EMetaBlob::fullbit::decode(bufferlist::iterator &bl) {
  DECODE_START_LEGACY_COMPAT_LEN(7, 5, 5, bl);
  ::decode(dn, bl);
  ::decode(dnfirst, bl);
  ::decode(dnlast, bl);
  ::decode(dnv, bl);
  ::decode(inode, bl);
  ::decode(xattrs, bl);
  if (inode.is_symlink())
    ::decode(symlink, bl);
  if (inode.is_dir()) {
    ::decode(dirfragtree, bl);
    ::decode(snapbl, bl);
    if ((struct_v == 2) || (struct_v == 3)) {
      bool dir_layout_exists;
      ::decode(dir_layout_exists, bl);
      if (dir_layout_exists) {
	__u8 dir_struct_v;
	::decode(dir_struct_v, bl); // default_file_layout version
	::decode(inode.layout, bl); // and actual layout, that we care about
      }
    }
  }
  if (struct_v >= 6) {
    ::decode(state, bl);
  } else {
    bool dirty;
    ::decode(dirty, bl);
    state = dirty ? EMetaBlob::fullbit::STATE_DIRTY : 0;
  }

  if (struct_v >= 3) {
    bool old_inodes_present;
    ::decode(old_inodes_present, bl);
    if (old_inodes_present) {
      ::decode(old_inodes, bl);
    }
  }
  if (!inode.is_dir()) {
    if (struct_v >= 7)
      ::decode(snapbl, bl);
  }
  if (struct_v >= 8)
    ::decode(oldest_snap, bl);
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
  f->open_array_section("xattrs");
  for (map<string, bufferptr>::const_iterator iter = xattrs.begin();
      iter != xattrs.end(); ++iter) {
    f->dump_string(iter->first.c_str(), iter->second.c_str());
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
    for (old_inodes_t::const_iterator iter = old_inodes.begin();
	 iter != old_inodes.end();
	 ++iter) {
      f->open_object_section("inode");
      f->dump_int("snapid", iter->first);
      iter->second.dump(f);
      f->close_section(); // inode
    }
    f->close_section(); // old inodes
  }
}

void EMetaBlob::fullbit::generate_test_instances(list<EMetaBlob::fullbit*>& ls)
{
  inode_t inode;
  fragtree_t fragtree;
  map<string,bufferptr> empty_xattrs;
  bufferlist empty_snapbl;
  fullbit *sample = new fullbit("/testdn", 0, 0, 0,
                                inode, fragtree, empty_xattrs, "", 0, empty_snapbl,
                                false);
  ls.push_back(sample);
}

void EMetaBlob::fullbit::update_inode(MDSRank *mds, CInode *in)
{
  *in->__get_inode() = inode;
  *in->__get_xattrs() = xattrs;
  if (in->is_dir()) {
    assert(in->dirfragtree == dirfragtree);
  } else if (in->is_symlink()) {
    in->__set_symlink(symlink);
  }

  assert(old_inodes.empty());
  assert(oldest_snap == CEPH_NOSNAP);;
  assert(snapbl.length() == 0);

  /*
   * In case there was anything malformed in the journal that we are
   * replaying, do sanity checks on the inodes we're replaying and
   * go damaged instead of letting any trash into a live cache
   */
  if (in->is_file()) {
    const file_layout_t &layout = in->get_inode()->layout;

    // Files must have valid layouts with a pool set
    if (layout.pool_id == -1 || !layout.is_valid()) {
      dout(0) << "EMetaBlob.replay invalid layout on ino " << *in << ": " << layout << dendl;
      std::ostringstream oss;
      oss << "Invalid layout for inode 0x" << std::hex << in->ino()
          << std::dec << " in journal";
      mds->clog->error() << oss.str();
      mds->damaged();
      assert(0);  // Should be unreachable because damaged() calls respawn()
    }
  }
}

// EMetaBlob::remotebit

void EMetaBlob::remotebit::encode(bufferlist& bl) const
{
  ENCODE_START(2, 2, bl);
  ::encode(dn, bl);
  ::encode(dnfirst, bl);
  ::encode(dnlast, bl);
  ::encode(dnv, bl);
  ::encode(ino, bl);
  ::encode(d_type, bl);
  ::encode(dirty, bl);
  ENCODE_FINISH(bl);
}

void EMetaBlob::remotebit::decode(bufferlist::iterator &bl)
{
  DECODE_START_LEGACY_COMPAT_LEN(2, 2, 2, bl);
  ::decode(dn, bl);
  ::decode(dnfirst, bl);
  ::decode(dnlast, bl);
  ::decode(dnv, bl);
  ::decode(ino, bl);
  ::decode(d_type, bl);
  ::decode(dirty, bl);
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
  ::encode(dn, bl);
  ::encode(dnfirst, bl);
  ::encode(dnlast, bl);
  ::encode(dnv, bl);
  ::encode(dirty, bl);
  ENCODE_FINISH(bl);
}

void EMetaBlob::nullbit::decode(bufferlist::iterator &bl)
{
  DECODE_START_LEGACY_COMPAT_LEN(2, 2, 2, bl);
  ::decode(dn, bl);
  ::decode(dnfirst, bl);
  ::decode(dnlast, bl);
  ::decode(dnv, bl);
  ::decode(dirty, bl);
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
  ::encode(fnode, bl);
  ::encode(state, bl);
  ::encode(nfull, bl);
  ::encode(nremote, bl);
  ::encode(nnull, bl);
  _encode_bits(features);
  ::encode(dnbl, bl);
  ENCODE_FINISH(bl);
}

void EMetaBlob::dirlump::decode(bufferlist::iterator &bl)
{
  DECODE_START_LEGACY_COMPAT_LEN(2, 2, 2, bl)
  ::decode(fnode, bl);
  ::decode(state, bl);
  ::decode(nfull, bl);
  ::decode(nremote, bl);
  ::decode(nnull, bl);
  ::decode(dnbl, bl);
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
  for (list<ceph::shared_ptr<fullbit> >::const_iterator
      iter = dfull.begin(); iter != dfull.end(); ++iter) {
    f->open_object_section("fullbit");
    (*iter)->dump(f);
    f->close_section(); // fullbit
  }
  f->close_section(); // full bits
  f->open_array_section("remote bits");
  for (list<remotebit>::const_iterator
      iter = dremote.begin(); iter != dremote.end(); ++iter) {
    f->open_object_section("remotebit");
    (*iter).dump(f);
    f->close_section(); // remotebit
  }
  f->close_section(); // remote bits
  f->open_array_section("null bits");
  for (list<nullbit>::const_iterator
      iter = dnull.begin(); iter != dnull.end(); ++iter) {
    f->open_object_section("null bit");
    (*iter).dump(f);
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
  ::encode(lump_order, bl);
  ::encode(lump_map, bl, features);
  ::encode(roots, bl, features);
  ::encode(table_tids, bl);
  ::encode(opened_ino, bl);
  ::encode(allocated_ino, bl);
  ::encode(used_preallocated_ino, bl);
  ::encode(preallocated_inos, bl);
  ::encode(client_name, bl);
  ::encode(inotablev, bl);
  ::encode(sessionmapv, bl);
  ::encode(truncate_start, bl);
  ::encode(truncate_finish, bl);
  ::encode(destroyed_inodes, bl);
  ::encode(client_reqs, bl);
  ::encode(renamed_dirino, bl);
  ::encode(renamed_dir_frags, bl);
  {
    // make MDSRank use v6 format happy
    int64_t i = -1;
    bool b = false;
    ::encode(i, bl);
    ::encode(b, bl);
  }
  ::encode(client_flushes, bl);
  ENCODE_FINISH(bl);
}
void EMetaBlob::decode(bufferlist::iterator &bl)
{
  DECODE_START_LEGACY_COMPAT_LEN(7, 5, 5, bl);
  ::decode(lump_order, bl);
  ::decode(lump_map, bl);
  if (struct_v >= 4) {
    ::decode(roots, bl);
  } else {
    bufferlist rootbl;
    ::decode(rootbl, bl);
    if (rootbl.length()) {
      bufferlist::iterator p = rootbl.begin();
      roots.push_back(ceph::shared_ptr<fullbit>(new fullbit(p)));
    }
  }
  ::decode(table_tids, bl);
  ::decode(opened_ino, bl);
  ::decode(allocated_ino, bl);
  ::decode(used_preallocated_ino, bl);
  ::decode(preallocated_inos, bl);
  ::decode(client_name, bl);
  ::decode(inotablev, bl);
  ::decode(sessionmapv, bl);
  ::decode(truncate_start, bl);
  ::decode(truncate_finish, bl);
  ::decode(destroyed_inodes, bl);
  if (struct_v >= 2) {
    ::decode(client_reqs, bl);
  } else {
    list<metareqid_t> r;
    ::decode(r, bl);
    while (!r.empty()) {
	client_reqs.push_back(pair<metareqid_t,uint64_t>(r.front(), 0));
	r.pop_front();
    }
  }
  if (struct_v >= 3) {
    ::decode(renamed_dirino, bl);
    ::decode(renamed_dir_frags, bl);
  }
  if (struct_v >= 6) {
    // ignore
    int64_t i;
    bool b;
    ::decode(i, bl);
    ::decode(b, bl);
  }
  if (struct_v >= 8) {
    ::decode(client_flushes, bl);
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
    list<ceph::shared_ptr<fullbit> > const &fb_list = dl.get_dfull();
    for (list<ceph::shared_ptr<fullbit> >::const_iterator
        iter = fb_list.begin(); iter != fb_list.end(); ++iter) {
      inodes.insert((*iter)->inode.ino);
    }

    // Record inodes of remotebits
    list<remotebit> const &rb_list = dl.get_dremote();
    for (list<remotebit>::const_iterator
	iter = rb_list.begin(); iter != rb_list.end(); ++iter) {
      inodes.insert(iter->ino);
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
    list<ceph::shared_ptr<fullbit> > const &fb_list = dl.get_dfull();
    list<nullbit> const &nb_list = dl.get_dnull();
    list<remotebit> const &rb_list = dl.get_dremote();

    // For all bits, store dentry
    for (list<ceph::shared_ptr<fullbit> >::const_iterator
        iter = fb_list.begin(); iter != fb_list.end(); ++iter) {
      dentries[df].insert((*iter)->dn);

    }
    for (list<nullbit>::const_iterator
	iter = nb_list.begin(); iter != nb_list.end(); ++iter) {
      dentries[df].insert(iter->dn);
    }
    for (list<remotebit>::const_iterator
	iter = rb_list.begin(); iter != rb_list.end(); ++iter) {
      dentries[df].insert(iter->dn);
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

    list<ceph::shared_ptr<fullbit> > const &fb_list = dl.get_dfull();
    list<nullbit> const &nb_list = dl.get_dnull();
    list<remotebit> const &rb_list = dl.get_dremote();

    for (list<ceph::shared_ptr<fullbit> >::const_iterator
        iter = fb_list.begin(); iter != fb_list.end(); ++iter) {
      std::string const &dentry = (*iter)->dn;
      children[dir_ino].push_back(dentry);
      ino_locations[(*iter)->inode.ino] = Location(dir_ino, dentry);
    }

    for (list<nullbit>::const_iterator
	iter = nb_list.begin(); iter != nb_list.end(); ++iter) {
      std::string const &dentry = iter->dn;
      children[dir_ino].push_back(dentry);
    }

    for (list<remotebit>::const_iterator
	iter = rb_list.begin(); iter != rb_list.end(); ++iter) {
      std::string const &dentry = iter->dn;
      children[dir_ino].push_back(dentry);
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

    list<ceph::shared_ptr<fullbit> > const &fb_list = dl.get_dfull();
    for (list<ceph::shared_ptr<fullbit> >::const_iterator
        iter = fb_list.begin(); iter != fb_list.end(); ++iter) {
      std::string const &dentry = (*iter)->dn;
      children[dir_ino].push_back(dentry);
      ino_locations[(*iter)->inode.ino] = Location(dir_ino, dentry);
      if (children.find((*iter)->inode.ino) == children.end()) {
        leaf_locations.push_back(Location(dir_ino, dentry));

      }
    }

    list<nullbit> const &nb_list = dl.get_dnull();
    for (list<nullbit>::const_iterator
	iter = nb_list.begin(); iter != nb_list.end(); ++iter) {
      std::string const &dentry = iter->dn;
      leaf_locations.push_back(Location(dir_ino, dentry));
    }

    list<remotebit> const &rb_list = dl.get_dremote();
    for (list<remotebit>::const_iterator
	iter = rb_list.begin(); iter != rb_list.end(); ++iter) {
      std::string const &dentry = iter->dn;
      leaf_locations.push_back(Location(dir_ino, dentry));
    }
  }

  // For all the leaf locations identified, generate paths
  for (std::vector<Location>::iterator i = leaf_locations.begin(); i != leaf_locations.end(); ++i) {
    Location const &loc = *i;
    std::string path = loc.second;
    inodeno_t ino = loc.first;
    while(ino_locations.find(ino) != ino_locations.end()) {
      Location const &loc = ino_locations[ino];
      if (!path.empty()) {
        path = loc.second + "/" + path;
      } else {
        path = loc.second + path;
      }
      ino = loc.first;
    }

    paths.push_back(path);
  }
}


void EMetaBlob::dump(Formatter *f) const
{
  f->open_array_section("lumps");
  for (list<dirfrag_t>::const_iterator i = lump_order.begin();
       i != lump_order.end(); ++i) {
    f->open_object_section("lump");
    f->open_object_section("dirfrag");
    f->dump_stream("dirfrag") << *i;
    f->close_section(); // dirfrag
    f->open_object_section("dirlump");
    lump_map.at(*i).dump(f);
    f->close_section(); // dirlump
    f->close_section(); // lump
  }
  f->close_section(); // lumps
  
  f->open_array_section("roots");
  for (list<ceph::shared_ptr<fullbit> >::const_iterator i = roots.begin();
       i != roots.end(); ++i) {
    f->open_object_section("root");
    (*i)->dump(f);
    f->close_section(); // root
  }
  f->close_section(); // roots

  f->open_array_section("tableclient tranactions");
  for (list<pair<__u8,version_t> >::const_iterator i = table_tids.begin();
       i != table_tids.end(); ++i) {
    f->open_object_section("transaction");
    f->dump_int("tid", i->first);
    f->dump_int("version", i->second);
    f->close_section(); // transaction
  }
  f->close_section(); // tableclient transactions
  
  f->dump_int("renamed directory inodeno", renamed_dirino);
  
  f->open_array_section("renamed directory fragments");
  for (list<frag_t>::const_iterator i = renamed_dir_frags.begin();
       i != renamed_dir_frags.end(); ++i) {
    f->dump_int("frag", *i);
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
  for(list<inodeno_t>::const_iterator i = truncate_start.begin();
      i != truncate_start.end(); ++i) {
    f->dump_int("inodeno", *i);
  }
  f->close_section(); // truncate inodes
  f->open_array_section("inodes finishing a truncated");
  for(map<inodeno_t,uint64_t>::const_iterator i = truncate_finish.begin();
      i != truncate_finish.end(); ++i) {
    f->open_object_section("inode+segment");
    f->dump_int("inodeno", i->first);
    f->dump_int("truncate starting segment", i->second);
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
  for(list<pair<metareqid_t,uint64_t> >::const_iterator i = client_reqs.begin();
      i != client_reqs.end(); ++i) {
    f->open_object_section("Client request");
    f->dump_stream("request ID") << i->first;
    f->dump_int("oldest request on client", i->second);
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

  assert(logseg);

  assert(g_conf->mds_kill_journal_replay_at != 1);

  for (list<ceph::shared_ptr<fullbit> >::iterator p = roots.begin(); p != roots.end(); ++p) {
    CInodeRef in = mds->mdcache->get_inode((*p)->inode.ino);
    bool isnew = in ? false:true;
    if (!in)
      in = new CInode(mds->mdcache);
    (*p)->update_inode(mds, in.get());

    if (isnew)
      mds->mdcache->add_inode(in.get());
    if ((*p)->is_dirty()) in->_mark_dirty(logseg);
    dout(10) << "EMetaBlob.replay " << (isnew ? " added root ":" updated root ") << *in << dendl;    
  }

  // keep track of any inodes we unlink and don't relink elsewhere
  set<CInode*> unlinked;

  // walk through my dirs (in order!)
  for (list<dirfrag_t>::iterator lp = lump_order.begin();
       lp != lump_order.end();
       ++lp) {
    dout(10) << "EMetaBlob.replay dir " << *lp << dendl;
    dirlump &lump = lump_map[*lp];

    CInodeRef diri = mds->mdcache->get_inode((*lp).ino);
    if (!diri) {
      dout(0) << "EMetaBlob.replay missing dir ino  " << (*lp).ino << dendl;
      mds->clog->error() << "failure replaying journal (EMetaBlob)";
      mds->damaged();
      assert(0);  // Should be unreachable because damaged() calls respawn()
    }

    diri->mutex_lock();
    CDirRef dir = diri->get_or_open_dirfrag((*lp).frag);

    // the dir 
    *dir->__get_fnode() = lump.fnode;
    dir->__set_version(lump.fnode.version);

    if (lump.is_dirty()) {
      dir->_mark_dirty(logseg);

      const fnode_t *pf = dir->get_fnode();
      int mask = 0;
      if (pf->rstat != pf->accounted_rstat) {
	mask |= CEPH_LOCK_INEST;
	dout(10) << "EMetaBlob.replay      dirty nestinfo on " << *dir << dendl;
      } else {
	dout(10) << "EMetaBlob.replay      clean nestinfo on " << *dir << dendl;
      }
      if (pf->fragstat != pf->accounted_fragstat) {
	mask |= CEPH_LOCK_IFILE;
	dout(10) << "EMetaBlob.replay      dirty fragstat on " << *dir << dendl;
      } else {
	dout(10) << "EMetaBlob.replay      clean fragstat on " << *dir << dendl;
      }
      if (mask)
	diri->mark_dirty_scattered(logseg, mask);
    }
    /*
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
    */
    
    dout(10) << "EMetaBlob.replay updated dir " << *dir << dendl;  

    // decode bits
    lump._decode_bits();

    // full dentry+inode pairs
    for (list<ceph::shared_ptr<fullbit> >::const_iterator pp = lump.get_dfull().begin();
	 pp != lump.get_dfull().end();
	 ++pp) {
      ceph::shared_ptr<fullbit> p = *pp;
      assert(p->dnfirst == 2);
      assert(p->dnlast == CEPH_NOSNAP);

      CDentryRef dn = dir->lookup(p->dn);
      if (!dn) {
	dn = dir->add_null_dentry(p->dn);
	dn->set_version(p->dnv);
	if (p->is_dirty()) dn->_mark_dirty(logseg);
	dout(10) << "EMetaBlob.replay added " << *dn << dendl;
      } else {
	dn->set_version(p->dnv);
	if (p->is_dirty()) dn->_mark_dirty(logseg);
	dout(10) << "EMetaBlob.replay for [" << p->dnfirst << "," << p->dnlast << "] had " << *dn << dendl;
      }

      CInodeRef in = mds->mdcache->get_inode(p->inode.ino);
      if (!in) {
	in = new CInode(mds->mdcache);
	in->mutex_lock();
	p->update_inode(mds, in.get());
	mds->mdcache->add_inode(in.get());
      } else {
	in->mutex_lock();
	p->update_inode(mds, in.get());
	if (in->get_parent_dn() != dn) {
	  dout(10) << "EMetaBlob.replay unlinking " << *in << dendl;
	  CDentryRef other_dn = in->get_parent_dn();
	  if (other_dn->get_dir_inode() != diri.get()) {
	    assert(other_dn->get_dir_inode()->mutex_trylock()); // single thread, 
	  }
	  unlinked.insert(in.get());
	  other_dn->get_dir()->unlink_inode(other_dn.get());
	  if (other_dn->get_dir_inode() != diri.get()) {
	    other_dn->get_dir_inode()->mutex_unlock(); // single thread, 
	  }
	}
      }

      if (dn->get_linkage()->get_inode() != in) {
	if (!dn->get_linkage()->is_null()) { // note: might be remote.  as with stray reintegration.
	  if (dn->get_linkage()->is_primary()) {
	    CInodeRef other_in = dn->get_linkage()->get_inode();
	    assert(other_in->mutex_trylock()); // single thread, 
	     unlinked.insert(other_in.get());
	    stringstream ss;
	    ss << "EMetaBlob.replay FIXME had dentry linked to wrong inode " << *dn
	       << " " << *other_in << " should be " << p->inode.ino;
	    dout(0) << ss.str() << dendl;
	    mds->clog->warn(ss);
	    dir->unlink_inode(dn.get());
	    other_in->mutex_unlock();
	  } else {
	    dir->unlink_inode(dn.get());
	  }
	}
	unlinked.erase(in.get());
	dir->link_primary_inode(dn.get(), in.get());
	dout(10) << "EMetaBlob.replay linked " << *in << dendl;
      } else {
	dout(10) << "EMetaBlob.replay for [" << p->dnfirst << "," << p->dnlast << "] had " << *in << dendl;
      }
      if (p->is_dirty())
	in->_mark_dirty(logseg);
  //    if (p->is_dirty_parent())
//	in->_mark_dirty_parent(logseg, p->is_dirty_pool());
      assert(g_conf->mds_kill_journal_replay_at != 2);

      in->mutex_unlock();
    }

    // remote dentries
    for (list<remotebit>::const_iterator p = lump.get_dremote().begin();
	 p != lump.get_dremote().end();
	 ++p) {
      assert(p->dnfirst == 2);
      assert(p->dnlast == CEPH_NOSNAP);

      CDentryRef dn = dir->lookup(p->dn);
      if (!dn) {
	dn = dir->add_remote_dentry(p->dn, p->ino, p->d_type);
	dn->set_version(p->dnv);
	if (p->dirty) dn->_mark_dirty(logseg);
	dout(10) << "EMetaBlob.replay added " << *dn << dendl;
      } else {
	if (!dn->get_linkage()->is_null()) {
	  dout(10) << "EMetaBlob.replay unlinking " << *dn << dendl;
	  if (dn->get_linkage()->is_primary()) {
	    CInodeRef other_in = dn->get_linkage()->get_inode();
	    assert(other_in->mutex_trylock()); // single thread, 
	    unlinked.insert(other_in.get());
	    stringstream ss;
	    ss << "EMetaBlob.replay FIXME had dentry linked to wrong inode " << *dn
	       << " " << *other_in << " should be remote " << p->ino;
	    dout(0) << ss.str() << dendl;
	    dir->unlink_inode(dn.get());
	    other_in->mutex_unlock();
	  } else {
	    dir->unlink_inode(dn.get());
	  }
	}
	dir->link_remote_inode(dn.get(), p->ino, p->d_type);
	dn->set_version(p->dnv);
	if (p->dirty) dn->_mark_dirty(logseg);
	dout(10) << "EMetaBlob.replay for [" << p->dnfirst << "," << p->dnlast << "] had " << *dn << dendl;
      }
    }

    // null dentries
    for (list<nullbit>::const_iterator p = lump.get_dnull().begin();
	 p != lump.get_dnull().end();
	 ++p) {
      assert(p->dnfirst == 2);
      assert(p->dnlast == CEPH_NOSNAP);

      CDentryRef dn = dir->lookup(p->dn);
      if (!dn) {
	dn = dir->add_null_dentry(p->dn);
	dn->set_version(p->dnv);
	if (p->dirty) dn->_mark_dirty(logseg);
	dout(10) << "EMetaBlob.replay added " << *dn << dendl;
      } else {
	if (!dn->get_linkage()->is_null()) {
	  dout(10) << "EMetaBlob.replay unlinking " << *dn << dendl;
	  if (dn->get_linkage()->is_primary()) {
	    CInodeRef other_in = dn->get_linkage()->get_inode();
	    assert(other_in->mutex_trylock()); // single thread, 
	    unlinked.insert(other_in.get());
	    dir->unlink_inode(dn.get());
	    other_in->mutex_unlock();
	  } else {
	    dir->unlink_inode(dn.get());
	  }
	}
	dn->set_version(p->dnv);
	if (p->dirty) dn->_mark_dirty(logseg);
	dout(10) << "EMetaBlob.replay had " << *dn << dendl;
      }
    }

    diri->mutex_unlock();
  }


  assert(g_conf->mds_kill_journal_replay_at != 3);

  if (!unlinked.empty()) {
    dout(10) << " unlinked set contains " << unlinked << dendl;
    for (auto p : unlinked) {
      p->mutex_lock();
      mds->mdcache->remove_inode_recursive(p);
    }
  }

#if 0
  // table client transactions
  for (list<pair<__u8,version_t> >::iterator p = table_tids.begin();
       p != table_tids.end();
       ++p) {
    dout(10) << "EMetaBlob.replay noting " << get_mdstable_name(p->first)
	     << " transaction " << p->second << dendl;
    MDSTableClient *client = mds->get_table_client(p->first);
    if (client)
      client->got_journaled_agree(p->second, logseg);
  }

  // opened ino?
  if (opened_ino) {
    CInode *in = mds->mdcache->get_inode(opened_ino);
    assert(in);
    dout(10) << "EMetaBlob.replay noting opened inode " << *in << dendl;
    logseg->open_files.push_back(&in->item_open_file);
  }
#endif

  // allocated_inos
  if (inotablev) {
    mds->inotable->mutex_lock();
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
	    << mds->inotable->get_version() << " -> " << inotablev << "\n";
	mds->inotable->force_replay_version(inotablev);
      }

      assert(inotablev == mds->inotable->get_version());
    }
    mds->inotable->mutex_unlock();
  }
  if (sessionmapv) {
    mds->sessionmap->mutex_lock();
    if (mds->sessionmap->get_version() >= sessionmapv) {
      dout(10) << "EMetaBlob.replay sessionmap v " << sessionmapv
	       << " <= table " << mds->sessionmap->get_version() << dendl;
    } else if (mds->sessionmap->get_version() + 2 >= sessionmapv) {
      dout(10) << "EMetaBlob.replay sessionmap v " << sessionmapv
	       << " -(1|2) == table " << mds->sessionmap->get_version()
	       << " prealloc " << preallocated_inos
	       << " used " << used_preallocated_ino
	       << dendl;
      Session *session = mds->sessionmap->get_session(client_name);
      if (session) {
	dout(20) << " (session prealloc " << session->info.prealloc_inos << ")" << dendl;
	if (used_preallocated_ino) {
	  if (session->info.prealloc_inos.empty()) {
	    // HRM: badness in the journal
	    mds->clog->warn() << " replayed op " << client_reqs << " on session for "
			     << client_name << " with empty prealloc_inos\n";
	  } else {
	    inodeno_t next = session->next_ino();
	    inodeno_t i = session->take_ino(used_preallocated_ino);
	    if (next != i)
	      mds->clog->warn() << " replayed op " << client_reqs << " used ino " << i
			       << " but session next is " << next << "\n";
	    assert(i == used_preallocated_ino);
	    session->info.used_inos.clear();
	  }
          mds->sessionmap->replay_dirty_session(session);
	}
	if (!preallocated_inos.empty()) {
	  session->info.prealloc_inos.insert(preallocated_inos);
          mds->sessionmap->replay_dirty_session(session);
	}

      } else {
	dout(10) << "EMetaBlob.replay no session for " << client_name << dendl;
	if (used_preallocated_ino) {
	  mds->sessionmap->replay_advance_version();
        }
	if (!preallocated_inos.empty())
	  mds->sessionmap->replay_advance_version();
      }
      assert(sessionmapv == mds->sessionmap->get_version());
    } else {
      mds->clog->error() << "journal replay sessionmap v " << sessionmapv
			<< " -(1|2) > table " << mds->sessionmap->get_version() << "\n";
      assert(g_conf->mds_wipe_sessions);
      mds->sessionmap->wipe();
      mds->sessionmap->set_version(sessionmapv);
    }
    mds->sessionmap->mutex_unlock();
  }

  /*
  // truncating inodes
  for (list<inodeno_t>::iterator p = truncate_start.begin();
       p != truncate_start.end();
       ++p) {
    CInode *in = mds->mdcache->get_inode(*p);
    assert(in);
    mds->mdcache->add_recovered_truncate(in, logseg);
  }
  for (map<inodeno_t,uint64_t>::iterator p = truncate_finish.begin();
       p != truncate_finish.end();
       ++p) {
    LogSegment *ls = mds->mdlog->get_segment(p->second);
    if (ls) {
      CInode *in = mds->mdcache->get_inode(p->first);
      assert(in);
      mds->mdcache->remove_recovered_truncate(in, ls);
    }
  }

  // destroyed inodes
  for (vector<inodeno_t>::iterator p = destroyed_inodes.begin();
       p != destroyed_inodes.end();
       ++p) {
    CInode *in = mds->mdcache->get_inode(*p);
    if (in) {
      dout(10) << "EMetaBlob.replay destroyed " << *p << ", dropping " << *in << dendl;
      mds->mdcache->remove_inode(in);
    } else {
      dout(10) << "EMetaBlob.replay destroyed " << *p << ", not in cache" << dendl;
    }
  }

  // client requests
  for (list<pair<metareqid_t, uint64_t> >::iterator p = client_reqs.begin();
       p != client_reqs.end();
       ++p) {
    if (p->first.name.is_client()) {
      dout(10) << "EMetaBlob.replay request " << p->first << " trim_to " << p->second << dendl;
      inodeno_t created = allocated_ino ? allocated_ino : used_preallocated_ino;
      // if we allocated an inode, there should be exactly one client request id.
      assert(created == inodeno_t() || client_reqs.size() == 1);

      Session *session = mds->sessionmap.get_session(p->first.name);
      if (session) {
	session->add_completed_request(p->first.tid, created);
	if (p->second)
	  session->trim_completed_requests(p->second);
      }
    }
  }

  // client flushes
  for (list<pair<metareqid_t, uint64_t> >::iterator p = client_flushes.begin();
       p != client_flushes.end();
       ++p) {
    if (p->first.name.is_client()) {
      dout(10) << "EMetaBlob.replay flush " << p->first << " trim_to " << p->second << dendl;
      Session *session = mds->sessionmap.get_session(p->first.name);
      if (session) {
	session->add_completed_flush(p->first.tid);
	if (p->second)
	  session->trim_completed_flushes(p->second);
      }
    }
  }
  */

  // update segment
  update_segment(logseg);

  assert(g_conf->mds_kill_journal_replay_at != 4);
}

// -----------------------
// ESession

void ESession::update_segment()
{
  _segment->sessionmapv = cmapv;
  if (inos.size() && inotablev)
    _segment->inotablev = inotablev;
}

void ESession::replay(MDSRank *mds)
{
  mds->sessionmap->mutex_lock();
  if (mds->sessionmap->get_version() >= cmapv) {
    dout(10) << "ESession.replay sessionmap " << mds->sessionmap->get_version() 
	     << " >= " << cmapv << ", noop" << dendl;
  } else {
    dout(10) << "ESession.replay sessionmap " << mds->sessionmap->get_version()
	     << " < " << cmapv << " " << (open ? "open":"close") << " " << client_inst << dendl;
    Session *session;
    if (open) {
      session = mds->sessionmap->get_or_add_session(client_inst);
      mds->sessionmap->set_state(session, Session::STATE_OPEN);
      session->set_client_metadata(client_metadata);
      dout(10) << " opened session " << session->info.inst << dendl;
    } else {
      session = mds->sessionmap->get_session(client_inst.name);
      if (session) { // there always should be a session, but there's a bug
	if (session->connection == NULL) {
	  dout(10) << " removed session " << session->info.inst << dendl;
	  mds->sessionmap->remove_session(session);
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
      mds->sessionmap->replay_dirty_session(session);
    } else {
      mds->sessionmap->replay_advance_version();
    }
    assert(mds->sessionmap->get_version() == cmapv);
  }
  
  if (inos.size() && inotablev) {
    mds->inotable->mutex_lock();
    if (mds->inotable->get_version() >= inotablev) {
      dout(10) << "ESession.replay inotable " << mds->inotable->get_version()
	       << " >= " << inotablev << ", noop" << dendl;
    } else {
      dout(10) << "ESession.replay inotable " << mds->inotable->get_version()
	       << " < " << inotablev << " " << (open ? "add":"remove") << dendl;
      assert(!open);  // for now
      mds->inotable->replay_release_ids(inos);
      assert(mds->inotable->get_version() == inotablev);
    }
    mds->inotable->mutex_unlock();
  }
  mds->sessionmap->mutex_unlock();

  update_segment();
}

void ESession::encode(bufferlist &bl, uint64_t features) const
{
  ENCODE_START(4, 3, bl);
  ::encode(stamp, bl);
  ::encode(client_inst, bl, features);
  ::encode(open, bl);
  ::encode(cmapv, bl);
  ::encode(inos, bl);
  ::encode(inotablev, bl);
  ::encode(client_metadata, bl);
  ENCODE_FINISH(bl);
}

void ESession::decode(bufferlist::iterator &bl)
{
  DECODE_START_LEGACY_COMPAT_LEN(4, 3, 3, bl);
  if (struct_v >= 2)
    ::decode(stamp, bl);
  ::decode(client_inst, bl);
  ::decode(open, bl);
  ::decode(cmapv, bl);
  ::decode(inos, bl);
  ::decode(inotablev, bl);
  if (struct_v >= 4) {
    ::decode(client_metadata, bl);
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
  for (map<string, string>::const_iterator i = client_metadata.begin();
      i != client_metadata.end(); ++i) {
    f->dump_string(i->first.c_str(), i->second);
  }
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
  ENCODE_START(1, 1, bl);
  ::encode(client_map, bl, features);
  ::encode(cmapv, bl);
  ::encode(stamp, bl);
  ENCODE_FINISH(bl);
}

void ESessions::decode_old(bufferlist::iterator &bl)
{
  ::decode(client_map, bl);
  ::decode(cmapv, bl);
  if (!bl.end())
    ::decode(stamp, bl);
}

void ESessions::decode_new(bufferlist::iterator &bl)
{
  DECODE_START(1, bl);
  ::decode(client_map, bl);
  ::decode(cmapv, bl);
  if (!bl.end())
    ::decode(stamp, bl);
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
  _segment->sessionmapv = cmapv;
}

void ESessions::replay(MDSRank *mds)
{
#if 0
  if (mds->sessionmap.get_version() >= cmapv) {
    dout(10) << "ESessions.replay sessionmap " << mds->sessionmap.get_version()
	     << " >= " << cmapv << ", noop" << dendl;
  } else {
    dout(10) << "ESessions.replay sessionmap " << mds->sessionmap.get_version()
	     << " < " << cmapv << dendl;
    mds->sessionmap.open_sessions(client_map);
    assert(mds->sessionmap.get_version() == cmapv);
    mds->sessionmap.set_projected(mds->sessionmap.get_version());
  }
  update_segment();
#endif
}


// -----------------------
// ETableServer

void ETableServer::encode(bufferlist& bl, uint64_t features) const
{
  ENCODE_START(3, 3, bl);
  ::encode(stamp, bl);
  ::encode(table, bl);
  ::encode(op, bl);
  ::encode(reqid, bl);
  ::encode(bymds, bl);
  ::encode(mutation, bl);
  ::encode(tid, bl);
  ::encode(version, bl);
  ENCODE_FINISH(bl);
}

void ETableServer::decode(bufferlist::iterator &bl)
{
  DECODE_START_LEGACY_COMPAT_LEN(3, 3, 3, bl);
  if (struct_v >= 2)
    ::decode(stamp, bl);
  ::decode(table, bl);
  ::decode(op, bl);
  ::decode(reqid, bl);
  ::decode(bymds, bl);
  ::decode(mutation, bl);
  ::decode(tid, bl);
  ::decode(version, bl);
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
  _segment->tablev[table] = version;
}

void ETableServer::replay(MDSRank *mds)
{
#if 0
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
  assert(version-1 == server->get_version());

  switch (op) {
  case TABLESERVER_OP_PREPARE:
    server->_prepare(mutation, reqid, bymds);
    server->_note_prepare(bymds, reqid);
    break;
  case TABLESERVER_OP_COMMIT:
    server->_commit(tid);
    server->_note_commit(tid);
    break;
  case TABLESERVER_OP_ROLLBACK:
    server->_rollback(tid);
    server->_note_rollback(tid);
    break;
  case TABLESERVER_OP_SERVER_UPDATE:
    server->_server_update(mutation);
    break;
  default:
    mds->clog->error() << "invalid tableserver op in ETableServer";
    mds->damaged();
    assert(0);  // Should be unreachable because damaged() calls respawn()
  }
  
  assert(version == server->get_version());
  update_segment();
#endif
}


// ---------------------
// ETableClient

void ETableClient::encode(bufferlist& bl, uint64_t features) const
{
  ENCODE_START(3, 3, bl);
  ::encode(stamp, bl);
  ::encode(table, bl);
  ::encode(op, bl);
  ::encode(tid, bl);
  ENCODE_FINISH(bl);
}

void ETableClient::decode(bufferlist::iterator &bl)
{
  DECODE_START_LEGACY_COMPAT_LEN(3, 3, 3, bl);
  if (struct_v >= 2)
    ::decode(stamp, bl);
  ::decode(table, bl);
  ::decode(op, bl);
  ::decode(tid, bl);
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
#if 0
  dout(10) << " ETableClient.replay " << get_mdstable_name(table)
	   << " op " << get_mdstableserver_opname(op)
	   << " tid " << tid << dendl;
    
  MDSTableClient *client = mds->get_table_client(table);
  if (!client)
    return;

  assert(op == TABLESERVER_OP_ACK);
  client->got_journaled_ack(tid);
#endif
}


// -----------------------
// ESnap
/*
void ESnap::update_segment()
{
  _segment->tablev[TABLE_SNAP] = version;
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
  assert(version-1 == mds->snaptable->get_version());

  if (create) {
    version_t v;
    snapid_t s = mds->snaptable->create(snap.dirino, snap.name, snap.stamp, &v);
    assert(s == snap.snapid);
  } else {
    mds->snaptable->remove(snap.snapid);
  }

  assert(version == mds->snaptable->get_version());
}
*/



// -----------------------
// EUpdate

void EUpdate::encode(bufferlist &bl, uint64_t features) const
{
  ENCODE_START(4, 4, bl);
  ::encode(stamp, bl);
  ::encode(type, bl);
  ::encode(metablob, bl, features);
  ::encode(client_map, bl);
  ::encode(cmapv, bl);
  ::encode(reqid, bl);
  ::encode(had_slaves, bl);
  ENCODE_FINISH(bl);
}
 
void EUpdate::decode(bufferlist::iterator &bl)
{
  DECODE_START_LEGACY_COMPAT_LEN(4, 4, 4, bl);
  if (struct_v >= 2)
    ::decode(stamp, bl);
  ::decode(type, bl);
  ::decode(metablob, bl);
  ::decode(client_map, bl);
  if (struct_v >= 3)
    ::decode(cmapv, bl);
  ::decode(reqid, bl);
  ::decode(had_slaves, bl);
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
#if 0
  metablob.update_segment(_segment);

  if (had_slaves)
    _segment->uncommitted_masters.insert(reqid);
#endif
}

void EUpdate::replay(MDSRank *mds)
{
  metablob.replay(mds, _segment);
  
#if 0
  if (had_slaves) {
    dout(10) << "EUpdate.replay " << reqid << " had slaves, expecting a matching ECommitted" << dendl;
    _segment->uncommitted_masters.insert(reqid);
    set<mds_rank_t> slaves;
    mds->mdcache->add_uncommitted_master(reqid, _segment, slaves, true);
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
      map<client_t, uint64_t> seqm;
      bufferlist::iterator blp = client_map.begin();
      ::decode(cm, blp);
      mds->server->prepare_force_open_sessions(cm, seqm);
      mds->server->finish_force_open_sessions(cm, seqm);

      assert(mds->sessionmap.get_version() == cmapv);
      mds->sessionmap.set_projected(mds->sessionmap.get_version());
    }
  }
#endif
}


// ------------------------
// EOpen

void EOpen::encode(bufferlist &bl, uint64_t features) const {
  ENCODE_START(4, 3, bl);
  ::encode(stamp, bl);
  ::encode(metablob, bl, features);
  ::encode(inos, bl);
  ::encode(snap_inos, bl);
  ENCODE_FINISH(bl);
} 

void EOpen::decode(bufferlist::iterator &bl) {
  DECODE_START_LEGACY_COMPAT_LEN(3, 3, 3, bl);
  if (struct_v >= 2)
    ::decode(stamp, bl);
  ::decode(metablob, bl);
  ::decode(inos, bl);
  if (struct_v >= 4)
    ::decode(snap_inos, bl);
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
  metablob.replay(mds, _segment);

#if 0
  // note which segments inodes belong to, so we don't have to start rejournaling them
  for (const auto &ino : inos) {
    CInode *in = mds->mdcache->get_inode(ino);
    if (!in) {
      dout(0) << "EOpen.replay ino " << ino << " not in metablob" << dendl;
      assert(in);
    }
    _segment->open_files.push_back(&in->item_open_file);
  }
  for (const auto &vino : snap_inos) {
    CInode *in = mds->mdcache->get_inode(vino);
    if (!in) {
      dout(0) << "EOpen.replay ino " << vino << " not in metablob" << dendl;
      assert(in);
    }
    _segment->open_files.push_back(&in->item_open_file);
  }
#endif
}

// -----------------------
// ESubtreeMap

void ESubtreeMap::encode(bufferlist& bl, uint64_t features) const
{
  ENCODE_START(6, 5, bl);
  ::encode(stamp, bl);
  ::encode(metablob, bl, features);
  ::encode(subtrees, bl);
  ::encode(ambiguous_subtrees, bl);
  ::encode(expire_pos, bl);
  ::encode(event_seq, bl);
  ENCODE_FINISH(bl);
}
 
void ESubtreeMap::decode(bufferlist::iterator &bl)
{
  DECODE_START_LEGACY_COMPAT_LEN(6, 5, 5, bl);
  if (struct_v >= 2)
    ::decode(stamp, bl);
  ::decode(metablob, bl);
  ::decode(subtrees, bl);
  if (struct_v >= 4)
    ::decode(ambiguous_subtrees, bl);
  if (struct_v >= 3)
    ::decode(expire_pos, bl);
  if (struct_v >= 6)
    ::decode(event_seq, bl);
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
#if 0
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
    
    list<CDir*> subs;
    mds->mdcache->list_subtrees(subs);
    for (list<CDir*>::iterator p = subs.begin(); p != subs.end(); ++p) {
      CDir *dir = *p;
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
      assert(!g_conf->mds_debug_subtrees || errors == 0);
    }
    return;
  }

  dout(10) << "ESubtreeMap.replay -- reconstructing (auth) subtree spanning tree" << dendl;
  
  // first, stick the spanning tree in my cache
  //metablob.print(*_dout);
  metablob.replay(mds, _segment);
  
  // restore import/export maps
  for (map<dirfrag_t, vector<dirfrag_t> >::iterator p = subtrees.begin();
       p != subtrees.end();
       ++p) {
    CDir *dir = mds->mdcache->get_dirfrag(p->first);
    assert(dir);
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
#endif
}

// ------------------------
// EResetJournal

void EResetJournal::encode(bufferlist& bl, uint64_t features) const
{
  ENCODE_START(2, 2, bl);
  ::encode(stamp, bl);
  ENCODE_FINISH(bl);
}
 
void EResetJournal::decode(bufferlist::iterator &bl)
{
  DECODE_START_LEGACY_COMPAT_LEN(2, 2, 2, bl);
  ::decode(stamp, bl);
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
#if 0
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
#endif
}


void ENoOp::encode(bufferlist &bl, uint64_t features) const
{
  ENCODE_START(2, 2, bl);
  ::encode(pad_size, bl);
  uint8_t const pad = 0xff;
  for (unsigned int i = 0; i < pad_size; ++i) {
    ::encode(pad, bl);
  }
  ENCODE_FINISH(bl);
}


void ENoOp::decode(bufferlist::iterator &bl)
{
  DECODE_START(2, bl);
  ::decode(pad_size, bl);
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
#if 0
  map<inodeno_t, log_segment_seq_t> new_trunc_finish;
  for (std::map<inodeno_t, log_segment_seq_t>::iterator i = truncate_finish.begin();
      i != truncate_finish.end(); ++i) {
    if (old_to_new.count(i->second)) {
      dout(20) << __func__ << " applying segment seq mapping "
        << i->second << " -> " << old_to_new.find(i->second)->second << dendl;
      new_trunc_finish[i->first] = old_to_new.find(i->second)->second;
      modified = true;
    } else {
      dout(20) << __func__ << " no segment seq mapping found for "
        << i->second << dendl;
      new_trunc_finish[i->first] = i->second;
    }
  }
  truncateo_finish = new_trunc_finish;
#endif 
  return modified;
}
