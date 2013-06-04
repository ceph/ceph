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

#include "MDS.h"
#include "MDCache.h"
#include "CInode.h"
#include "CDir.h"
#include "CDentry.h"
#include "Migrator.h"
#include "Locker.h"
#include "Server.h"

#include "MDBalancer.h"
#include "MDLog.h"
#include "MDSMap.h"

#include "include/filepath.h"

#include "events/EExport.h"
#include "events/EImportStart.h"
#include "events/EImportFinish.h"
#include "events/ESessions.h"

#include "msg/Messenger.h"

#include "messages/MClientCaps.h"

#include "messages/MExportDirDiscover.h"
#include "messages/MExportDirDiscoverAck.h"
#include "messages/MExportDirCancel.h"
#include "messages/MExportDirPrep.h"
#include "messages/MExportDirPrepAck.h"
#include "messages/MExportDir.h"
#include "messages/MExportDirAck.h"
#include "messages/MExportDirNotify.h"
#include "messages/MExportDirNotifyAck.h"
#include "messages/MExportDirFinish.h"

#include "messages/MExportCaps.h"
#include "messages/MExportCapsAck.h"


/*
 * this is what the dir->dir_auth values look like
 *
 *   dir_auth  authbits  
 * export
 *   me         me      - before
 *   me, me     me      - still me, but preparing for export
 *   me, them   me      - send MExportDir (peer is preparing)
 *   them, me   me      - journaled EExport
 *   them       them    - done
 *
 * import:
 *   them       them    - before
 *   me, them   me      - journaled EImportStart
 *   me         me      - done
 *
 * which implies:
 *  - auth bit is set if i am listed as first _or_ second dir_auth.
 */

#include "common/config.h"


#define dout_subsys ceph_subsys_mds
#undef DOUT_COND
#define DOUT_COND(cct, l) (l <= cct->_conf->debug_mds || l <= cct->_conf->debug_mds_migrator)
#undef dout_prefix
#define dout_prefix *_dout << "mds." << mds->get_nodeid() << ".migrator "

/* This function DOES put the passed message before returning*/
void Migrator::dispatch(Message *m)
{
  switch (m->get_type()) {
    // import
  case MSG_MDS_EXPORTDIRDISCOVER:
    handle_export_discover(static_cast<MExportDirDiscover*>(m));
    break;
  case MSG_MDS_EXPORTDIRPREP:
    handle_export_prep(static_cast<MExportDirPrep*>(m));
    break;
  case MSG_MDS_EXPORTDIR:
    handle_export_dir(static_cast<MExportDir*>(m));
    break;
  case MSG_MDS_EXPORTDIRFINISH:
    handle_export_finish(static_cast<MExportDirFinish*>(m));
    break;
  case MSG_MDS_EXPORTDIRCANCEL:
    handle_export_cancel(static_cast<MExportDirCancel*>(m));
    break;

    // export 
  case MSG_MDS_EXPORTDIRDISCOVERACK:
    handle_export_discover_ack(static_cast<MExportDirDiscoverAck*>(m));
    break;
  case MSG_MDS_EXPORTDIRPREPACK:
    handle_export_prep_ack(static_cast<MExportDirPrepAck*>(m));
    break;
  case MSG_MDS_EXPORTDIRACK:
    handle_export_ack(static_cast<MExportDirAck*>(m));
    break;
  case MSG_MDS_EXPORTDIRNOTIFYACK:
    handle_export_notify_ack(static_cast<MExportDirNotifyAck*>(m));
    break;    

    // export 3rd party (dir_auth adjustments)
  case MSG_MDS_EXPORTDIRNOTIFY:
    handle_export_notify(static_cast<MExportDirNotify*>(m));
    break;

    // caps
  case MSG_MDS_EXPORTCAPS:
    handle_export_caps(static_cast<MExportCaps*>(m));
    break;
  case MSG_MDS_EXPORTCAPSACK:
    handle_export_caps_ack(static_cast<MExportCapsAck*>(m));
    break;

  default:
    assert(0);
  }
}


class C_MDC_EmptyImport : public Context {
  Migrator *mig;
  CDir *dir;
public:
  C_MDC_EmptyImport(Migrator *m, CDir *d) : mig(m), dir(d) {}
  void finish(int r) {
    mig->export_empty_import(dir);
  }
};


void Migrator::export_empty_import(CDir *dir)
{
  dout(7) << "export_empty_import " << *dir << dendl;
  assert(dir->is_subtree_root());

  if (dir->inode->is_auth()) {
    dout(7) << " inode is auth" << dendl;
    return;
  }
  if (!dir->is_auth()) {
    dout(7) << " not auth" << dendl;
    return;
  }
  if (dir->is_freezing() || dir->is_frozen()) {
    dout(7) << " freezing or frozen" << dendl;
    return;
  }
  if (dir->get_num_head_items() > 0) {
    dout(7) << " not actually empty" << dendl;
    return;
  }
  if (dir->inode->is_root()) {
    dout(7) << " root" << dendl;
    return;
  }
  
  int dest = dir->inode->authority().first;
  //if (mds->is_shutting_down()) dest = 0;  // this is more efficient.
  
  dout(7) << " really empty, exporting to " << dest << dendl;
  assert (dest != mds->get_nodeid());
  
  dout(7) << "exporting to mds." << dest 
           << " empty import " << *dir << dendl;
  export_dir( dir, dest );
}




// ==========================================================
// mds failure handling

void Migrator::handle_mds_failure_or_stop(int who)
{
  dout(5) << "handle_mds_failure_or_stop mds." << who << dendl;

  // check my exports

  // first add an extra auth_pin on any freezes, so that canceling a
  // nested freeze doesn't complete one further up the hierarchy and
  // confuse the shit out of us.  we'll remove it after canceling the
  // freeze.  this way no freeze completions run before we want them
  // to.
  list<CDir*> pinned_dirs;
  for (map<CDir*,int>::iterator p = export_state.begin();
       p != export_state.end();
       ++p) {
    if (p->second == EXPORT_FREEZING) {
      CDir *dir = p->first;
      dout(10) << "adding temp auth_pin on freezing " << *dir << dendl;
      dir->auth_pin(this);
      pinned_dirs.push_back(dir);
    }
  }

  map<CDir*,int>::iterator p = export_state.begin();
  while (p != export_state.end()) {
    map<CDir*,int>::iterator next = p;
    ++next;
    CDir *dir = p->first;
    
    // abort exports:
    //  - that are going to the failed node
    //  - that aren't frozen yet (to avoid auth_pin deadlock)
    //  - they havne't prepped yet (they may need to discover bounds to do that)
    if (export_peer[dir] == who ||
	p->second == EXPORT_DISCOVERING ||
	p->second == EXPORT_FREEZING ||
	p->second == EXPORT_PREPPING) { 
      // the guy i'm exporting to failed, or we're just freezing.
      dout(10) << "cleaning up export state (" << p->second << ")" << get_export_statename(p->second)
	       << " of " << *dir << dendl;
      
      switch (p->second) {
      case EXPORT_DISCOVERING:
	dout(10) << "export state=discovering : canceling freeze and removing auth_pin" << dendl;
	dir->unfreeze_tree();  // cancel the freeze
	dir->auth_unpin(this);
	export_state.erase(dir); // clean up
	export_unlock(dir);
	export_locks.erase(dir);
	dir->state_clear(CDir::STATE_EXPORTING);
	if (mds->mdsmap->is_clientreplay_or_active_or_stopping(export_peer[dir])) // tell them.
	  mds->send_message_mds(new MExportDirCancel(dir->dirfrag()), export_peer[dir]);
	break;
	
      case EXPORT_FREEZING:
	dout(10) << "export state=freezing : canceling freeze" << dendl;
	dir->unfreeze_tree();  // cancel the freeze
	export_state.erase(dir); // clean up
	dir->state_clear(CDir::STATE_EXPORTING);
	if (mds->mdsmap->is_clientreplay_or_active_or_stopping(export_peer[dir])) // tell them.
	  mds->send_message_mds(new MExportDirCancel(dir->dirfrag()), export_peer[dir]);
	break;

	// NOTE: state order reversal, warning comes after prepping
      case EXPORT_WARNING:
	dout(10) << "export state=warning : unpinning bounds, unfreezing, notifying" << dendl;
	// fall-thru

      case EXPORT_PREPPING:
	if (p->second != EXPORT_WARNING) 
	  dout(10) << "export state=prepping : unpinning bounds, unfreezing" << dendl;
	{
	  // unpin bounds
	  set<CDir*> bounds;
	  cache->get_subtree_bounds(dir, bounds);
	  for (set<CDir*>::iterator q = bounds.begin();
	       q != bounds.end();
	       ++q) {
	    CDir *bd = *q;
	    bd->put(CDir::PIN_EXPORTBOUND);
	    bd->state_clear(CDir::STATE_EXPORTBOUND);
	  }
	  // notify bystanders
	  if (p->second == EXPORT_WARNING)
	    export_notify_abort(dir, bounds);
	}
	dir->unfreeze_tree();
	export_state.erase(dir); // clean up
	cache->adjust_subtree_auth(dir, mds->get_nodeid());
	cache->try_subtree_merge(dir);  // NOTE: this may journal subtree_map as side effect
	export_unlock(dir);
	export_locks.erase(dir);
	dir->state_clear(CDir::STATE_EXPORTING);
	if (mds->mdsmap->is_clientreplay_or_active_or_stopping(export_peer[dir])) // tell them.
	  mds->send_message_mds(new MExportDirCancel(dir->dirfrag()), export_peer[dir]);
	break;
	
      case EXPORT_EXPORTING:
	dout(10) << "export state=exporting : reversing, and unfreezing" << dendl;
	export_reverse(dir);
	export_state.erase(dir); // clean up
	export_locks.erase(dir);
	dir->state_clear(CDir::STATE_EXPORTING);
	break;

      case EXPORT_LOGGINGFINISH:
      case EXPORT_NOTIFYING:
	dout(10) << "export state=loggingfinish|notifying : ignoring dest failure, we were successful." << dendl;
	// leave export_state, don't clean up now.
	break;

      default:
	assert(0);
      }

      // finish clean-up?
      if (export_state.count(dir) == 0) {
	export_peer.erase(dir);
	export_warning_ack_waiting.erase(dir);
	export_notify_ack_waiting.erase(dir);
	
	// wake up any waiters
	mds->queue_waiters(export_finish_waiters[dir]);
	export_finish_waiters.erase(dir);
	
	// send pending import_maps?  (these need to go out when all exports have finished.)
	cache->maybe_send_pending_resolves();

	cache->show_subtrees();

	maybe_do_queued_export();	
      }
    } else {
      // bystander failed.
      if (export_warning_ack_waiting.count(dir) &&
	  export_warning_ack_waiting[dir].count(who)) {
	export_warning_ack_waiting[dir].erase(who);
	export_notify_ack_waiting[dir].erase(who);   // they won't get a notify either.
	if (p->second == EXPORT_WARNING) {
	  // exporter waiting for warning acks, let's fake theirs.
	  dout(10) << "faking export_warning_ack from mds." << who
		   << " on " << *dir << " to mds." << export_peer[dir] 
		   << dendl;
	  if (export_warning_ack_waiting[dir].empty()) 
	    export_go(dir);
	}
      }
      if (export_notify_ack_waiting.count(dir) &&
	  export_notify_ack_waiting[dir].count(who)) {
	export_notify_ack_waiting[dir].erase(who);
	if (p->second == EXPORT_NOTIFYING) {
	  // exporter is waiting for notify acks, fake it
	  dout(10) << "faking export_notify_ack from mds." << who
		   << " on " << *dir << " to mds." << export_peer[dir] 
		   << dendl;
	  if (export_notify_ack_waiting[dir].empty()) 
	    export_finish(dir);
	}
      }
    }
    
    // next!
    p = next;
  }


  // check my imports
  map<dirfrag_t,int>::iterator q = import_state.begin();
  while (q != import_state.end()) {
    map<dirfrag_t,int>::iterator next = q;
    ++next;
    dirfrag_t df = q->first;
    CInode *diri = mds->mdcache->get_inode(df.ino);
    CDir *dir = mds->mdcache->get_dirfrag(df);

    if (import_peer[df] == who) {
      if (dir)
	dout(10) << "cleaning up import state (" << q->second << ")" << get_import_statename(q->second)
		 << " of " << *dir << dendl;
      else
	dout(10) << "cleaning up import state (" << q->second << ")" << get_import_statename(q->second)
		 << " of " << df << dendl;

      switch (q->second) {
      case IMPORT_DISCOVERING:
	dout(10) << "import state=discovering : clearing state" << dendl;
	import_reverse_discovering(df);
	break;

      case IMPORT_DISCOVERED:
	assert(diri);
	dout(10) << "import state=discovered : unpinning inode " << *diri << dendl;
	import_reverse_discovered(df, diri);
	break;

      case IMPORT_PREPPING:
	assert(dir);
	dout(10) << "import state=prepping : unpinning base+bounds " << *dir << dendl;
	import_reverse_prepping(dir);
	break;

      case IMPORT_PREPPED:
	assert(dir);
	dout(10) << "import state=prepped : unpinning base+bounds, unfreezing " << *dir << dendl;
	{
	  set<CDir*> bounds;
	  cache->get_subtree_bounds(dir, bounds);
	  import_remove_pins(dir, bounds);
	  
	  // adjust auth back to the exporter
	  cache->adjust_subtree_auth(dir, import_peer[df]);
	  cache->try_subtree_merge(dir);   // NOTE: may journal subtree_map as side-effect

	  // bystanders?
	  if (import_bystanders[dir].empty()) {
	    import_reverse_unfreeze(dir);
	  } else {
	    // notify them; wait in aborting state
	    import_notify_abort(dir, bounds);
	    import_state[df] = IMPORT_ABORTING;
	    assert(g_conf->mds_kill_import_at != 10);
	  }
	}
	break;

      case IMPORT_LOGGINGSTART:
	assert(dir);
	dout(10) << "import state=loggingstart : reversing import on " << *dir << dendl;
	import_reverse(dir);
	break;

      case IMPORT_ACKING:
	assert(dir);
	// hrm.  make this an ambiguous import, and wait for exporter recovery to disambiguate
	dout(10) << "import state=acking : noting ambiguous import " << *dir << dendl;
	{
	  set<CDir*> bounds;
	  cache->get_subtree_bounds(dir, bounds);
	  cache->add_ambiguous_import(dir, bounds);
	}
	break;
	
      case IMPORT_ABORTING:
	assert(dir);
	dout(10) << "import state=aborting : ignoring repeat failure " << *dir << dendl;
	break;
      }
    } else {
      if (q->second == IMPORT_ABORTING &&
	  import_bystanders[dir].count(who)) {
	assert(dir);
	dout(10) << "faking export_notify_ack from mds." << who
		 << " on aborting import " << *dir << " from mds." << import_peer[df] 
		 << dendl;
	import_bystanders[dir].erase(who);
	if (import_bystanders[dir].empty()) {
	  import_bystanders.erase(dir);
	  import_reverse_unfreeze(dir);
	}
      }
    }

    // next!
    q = next;
  }

  while (!pinned_dirs.empty()) {
    CDir *dir = pinned_dirs.front();
    dout(10) << "removing temp auth_pin on " << *dir << dendl;
    dir->auth_unpin(this);
    pinned_dirs.pop_front();
  }  
}



void Migrator::show_importing()
{  
  dout(10) << "show_importing" << dendl;
  for (map<dirfrag_t,int>::iterator p = import_state.begin();
       p != import_state.end();
       ++p) {
    CDir *dir = mds->mdcache->get_dirfrag(p->first);
    if (dir) {
      dout(10) << " importing from " << import_peer[p->first]
	       << ": (" << p->second << ") " << get_import_statename(p->second) 
	       << " " << p->first
	       << " " << *dir
	       << dendl;
    } else {
      dout(10) << " importing from " << import_peer[p->first]
	       << ": (" << p->second << ") " << get_import_statename(p->second) 
	       << " " << p->first 
	       << dendl;
    }
  }
}

void Migrator::show_exporting() 
{
  dout(10) << "show_exporting" << dendl;
  for (map<CDir*,int>::iterator p = export_state.begin();
       p != export_state.end();
       ++p) 
    dout(10) << " exporting to " << export_peer[p->first]
	     << ": (" << p->second << ") " << get_export_statename(p->second) 
	     << " " << p->first->dirfrag()
	     << " " << *p->first
	     << dendl;
}



void Migrator::audit()
{
  if (!g_conf->subsys.should_gather(ceph_subsys_mds, 5))
    return;  // hrm.

  // import_state
  show_importing();
  for (map<dirfrag_t,int>::iterator p = import_state.begin();
       p != import_state.end();
       ++p) {
    if (p->second == IMPORT_DISCOVERING) 
      continue;
    if (p->second == IMPORT_DISCOVERED) {
      CInode *in = cache->get_inode(p->first.ino);
      assert(in);
      continue;
    }
    CDir *dir = cache->get_dirfrag(p->first);
    assert(dir);
    if (p->second == IMPORT_PREPPING) 
      continue;
    if (p->second == IMPORT_ABORTING) {
      assert(!dir->is_ambiguous_dir_auth());
      assert(dir->get_dir_auth().first != mds->get_nodeid());
      continue;
    }
    assert(dir->is_ambiguous_dir_auth());
    assert(dir->authority().first  == mds->get_nodeid() ||
	   dir->authority().second == mds->get_nodeid());
  }

  // export_state
  show_exporting();
  for (map<CDir*,int>::iterator p = export_state.begin();
       p != export_state.end();
       ++p) {
    CDir *dir = p->first;
    if (p->second == EXPORT_DISCOVERING ||
	p->second == EXPORT_FREEZING) continue;
    assert(dir->is_ambiguous_dir_auth());
    assert(dir->authority().first  == mds->get_nodeid() ||
	   dir->authority().second == mds->get_nodeid());
  }

  // ambiguous+me subtrees should be importing|exporting

  // write me
}





// ==========================================================
// EXPORT

void Migrator::export_dir_nicely(CDir *dir, int dest)
{
  // enqueue
  dout(7) << "export_dir_nicely " << *dir << " to " << dest << dendl;
  export_queue.push_back(pair<dirfrag_t,int>(dir->dirfrag(), dest));

  maybe_do_queued_export();
}

void Migrator::maybe_do_queued_export()
{
  while (!export_queue.empty() &&
	 export_state.size() <= 4) {
    dirfrag_t df = export_queue.front().first;
    int dest = export_queue.front().second;
    export_queue.pop_front();
    
    CDir *dir = mds->mdcache->get_dirfrag(df);
    if (!dir) continue;
    if (!dir->is_auth()) continue;

    dout(0) << "nicely exporting to mds." << dest << " " << *dir << dendl;

    export_dir(dir, dest);
  }
}




class C_MDC_ExportFreeze : public Context {
  Migrator *mig;
  CDir *ex;   // dir i'm exporting

public:
  C_MDC_ExportFreeze(Migrator *m, CDir *e) :
	mig(m), ex(e) {}
  virtual void finish(int r) {
    if (r >= 0)
      mig->export_frozen(ex);
  }
};


void Migrator::get_export_lock_set(CDir *dir, set<SimpleLock*>& locks)
{
  // path
  vector<CDentry*> trace;
  cache->make_trace(trace, dir->inode);
  for (vector<CDentry*>::iterator it = trace.begin();
       it != trace.end();
       ++it)
    locks.insert(&(*it)->lock);

  // bound dftlocks:
  // NOTE: We need to take an rdlock on bounding dirfrags during
  //  migration for a rather irritating reason: when we export the
  //  bound inode, we need to send scatterlock state for the dirfrags
  //  as well, so that the new auth also gets the correct info.  If we
  //  race with a refragment, this info is useless, as we can't
  //  redivvy it up.  And it's needed for the scatterlocks to work
  //  properly: when the auth is in a sync/lock state it keeps each
  //  dirfrag's portion in the local (auth OR replica) dirfrag.
  set<CDir*> wouldbe_bounds;
  cache->get_wouldbe_subtree_bounds(dir, wouldbe_bounds);
  for (set<CDir*>::iterator p = wouldbe_bounds.begin(); p != wouldbe_bounds.end(); ++p)
    locks.insert(&(*p)->get_inode()->dirfragtreelock);
}


/** export_dir(dir, dest)
 * public method to initiate an export.
 * will fail if the directory is freezing, frozen, unpinnable, or root. 
 */
void Migrator::export_dir(CDir *dir, int dest)
{
  dout(7) << "export_dir " << *dir << " to " << dest << dendl;
  assert(dir->is_auth());
  assert(dest != mds->get_nodeid());
   
  if (mds->mdsmap->is_degraded()) {
    dout(7) << "cluster degraded, no exports for now" << dendl;
    return;
  }
  if (dir->inode->is_system()) {
    dout(7) << "i won't export system dirs (root, mdsdirs, stray, /.ceph, etc.)" << dendl;
    //assert(0);
    return;
  }

  if (!dir->inode->is_base() && dir->get_parent_dir()->get_inode()->is_stray() &&
      dir->get_parent_dir()->get_parent_dir()->ino() != MDS_INO_MDSDIR(dest)) {
    dout(7) << "i won't export anything in stray" << dendl;
    return;
  }

  if (dir->is_frozen() ||
      dir->is_freezing()) {
    dout(7) << " can't export, freezing|frozen.  wait for other exports to finish first." << dendl;
    return;
  }
  if (dir->state_test(CDir::STATE_EXPORTING)) {
    dout(7) << "already exporting" << dendl;
    return;
  }
  
  // locks?
  set<SimpleLock*> locks;
  get_export_lock_set(dir, locks);
  if (!mds->locker->rdlock_try_set(locks)) {
    dout(7) << "export_dir can't rdlock needed locks, failing." << dendl;
    return;
  }
  mds->locker->rdlock_take_set(locks);
  export_locks[dir].swap(locks);

  // ok.
  assert(export_state.count(dir) == 0);
  export_state[dir] = EXPORT_DISCOVERING;
  export_peer[dir] = dest;

  dir->state_set(CDir::STATE_EXPORTING);
  assert(g_conf->mds_kill_export_at != 1);

  // send ExportDirDiscover (ask target)
  filepath path;
  dir->inode->make_path(path);
  mds->send_message_mds(new MExportDirDiscover(mds->get_nodeid(), path, dir->dirfrag()), dest);
  assert(g_conf->mds_kill_export_at != 2);

  // start the freeze, but hold it up with an auth_pin.
  dir->auth_pin(this);
  dir->freeze_tree();
  assert(dir->is_freezing_tree());
  dir->add_waiter(CDir::WAIT_FROZEN, new C_MDC_ExportFreeze(this, dir));
}


/*
 * called on receipt of MExportDirDiscoverAck
 * the importer now has the directory's _inode_ in memory, and pinned.
 *
 * This function DOES put the passed message before returning
 */
void Migrator::handle_export_discover_ack(MExportDirDiscoverAck *m)
{
  CDir *dir = cache->get_dirfrag(m->get_dirfrag());
  assert(dir);
  
  dout(7) << "export_discover_ack from " << m->get_source()
	  << " on " << *dir << dendl;

  if (export_state.count(dir) == 0 ||
      export_state[dir] != EXPORT_DISCOVERING ||
      export_peer[dir] != m->get_source().num()) {
    dout(7) << "must have aborted" << dendl;
  } else {
    // release locks to avoid deadlock
    export_unlock(dir);
    export_locks.erase(dir);
    // freeze the subtree
    export_state[dir] = EXPORT_FREEZING;
    dir->auth_unpin(this);
    assert(g_conf->mds_kill_export_at != 3);
  }
  
  m->put();  // done
}

void Migrator::export_frozen(CDir *dir)
{
  dout(7) << "export_frozen on " << *dir << dendl;
  assert(dir->is_frozen());
  assert(dir->get_cum_auth_pins() == 0);

  int dest = export_peer[dir];
  CInode *diri = dir->inode;

  // ok, try to grab all my locks.
  set<SimpleLock*> locks;
  get_export_lock_set(dir, locks);
  if (!mds->locker->can_rdlock_set(locks)) {
    dout(7) << "export_dir couldn't rdlock all needed locks, failing. " 
	    << *diri << dendl;

    // .. unwind ..
    export_peer.erase(dir);
    export_state.erase(dir);
    dir->unfreeze_tree();
    dir->state_clear(CDir::STATE_EXPORTING);

    mds->queue_waiters(export_finish_waiters[dir]);
    export_finish_waiters.erase(dir);

    mds->send_message_mds(new MExportDirCancel(dir->dirfrag()), dest);
    return;
  }
  mds->locker->rdlock_take_set(locks);
  export_locks[dir].swap(locks);
  
  cache->show_subtrees();

  // note the bounds.
  //  force it into a subtree by listing auth as <me,me>.
  cache->adjust_subtree_auth(dir, mds->get_nodeid(), mds->get_nodeid());
  set<CDir*> bounds;
  cache->get_subtree_bounds(dir, bounds);

  // generate prep message, log entry.
  MExportDirPrep *prep = new MExportDirPrep(dir->dirfrag());

  // include list of bystanders
  for (map<int,int>::iterator p = dir->replicas_begin();
       p != dir->replicas_end();
       ++p) {
    if (p->first != dest) {
      dout(10) << "bystander mds." << p->first << dendl;
      prep->add_bystander(p->first);
    }
  }

  // include base dirfrag
  cache->replicate_dir(dir, dest, prep->basedir);
  
  /*
   * include spanning tree for all nested exports.
   * these need to be on the destination _before_ the final export so that
   * dir_auth updates on any nested exports are properly absorbed.
   * this includes inodes and dirfrags included in the subtree, but
   * only the inodes at the bounds.
   *
   * each trace is: df ('-' | ('f' dir | 'd') dentry inode (dir dentry inode)*)
   */
  set<inodeno_t> inodes_added;
  set<dirfrag_t> dirfrags_added;

  // check bounds
  for (set<CDir*>::iterator it = bounds.begin();
       it != bounds.end();
       ++it) {
    CDir *bound = *it;

    // pin it.
    bound->get(CDir::PIN_EXPORTBOUND);
    bound->state_set(CDir::STATE_EXPORTBOUND);
    
    dout(7) << "  export bound " << *bound << dendl;
    prep->add_bound( bound->dirfrag() );

    // trace to bound
    bufferlist tracebl;
    CDir *cur = bound;
    
    char start = '-';
    while (1) {
      // don't repeat inodes
      if (inodes_added.count(cur->inode->ino()))
	break;
      inodes_added.insert(cur->inode->ino());

      // prepend dentry + inode
      assert(cur->inode->is_auth());
      bufferlist bl;
      cache->replicate_dentry(cur->inode->parent, dest, bl);
      dout(7) << "  added " << *cur->inode->parent << dendl;
      cache->replicate_inode(cur->inode, dest, bl);
      dout(7) << "  added " << *cur->inode << dendl;
      bl.claim_append(tracebl);
      tracebl.claim(bl);

      cur = cur->get_parent_dir();

      // don't repeat dirfrags
      if (dirfrags_added.count(cur->dirfrag()) ||
	  cur == dir) {
	start = 'd';  // start with dentry
	break;
      }
      dirfrags_added.insert(cur->dirfrag());

      // prepend dir
      cache->replicate_dir(cur, dest, bl);
      dout(7) << "  added " << *cur << dendl;
      bl.claim_append(tracebl);
      tracebl.claim(bl);

      start = 'f';  // start with dirfrag
    }
    bufferlist final;
    dirfrag_t df = cur->dirfrag();
    ::encode(df, final);
    ::encode(start, final);
    final.claim_append(tracebl);
    prep->add_trace(final);
  }

  // send.
  export_state[dir] = EXPORT_PREPPING;
  mds->send_message_mds(prep, dest);
  assert (g_conf->mds_kill_export_at != 4);
}

/* This function DOES put the passed message before returning*/
void Migrator::handle_export_prep_ack(MExportDirPrepAck *m)
{
  CDir *dir = cache->get_dirfrag(m->get_dirfrag());
  assert(dir);

  dout(7) << "export_prep_ack " << *dir << dendl;

  if (export_state.count(dir) == 0 ||
      export_state[dir] != EXPORT_PREPPING) {
    // export must have aborted.  
    dout(7) << "export must have aborted" << dendl;
    m->put();
    return;
  }

  assert (g_conf->mds_kill_export_at != 5);
  // send warnings
  int dest = export_peer[dir];
  set<CDir*> bounds;
  cache->get_subtree_bounds(dir, bounds);

  assert(export_peer.count(dir));
  assert(export_warning_ack_waiting.count(dir) == 0);
  assert(export_notify_ack_waiting.count(dir) == 0);

  for (map<int,int>::iterator p = dir->replicas_begin();
       p != dir->replicas_end();
       ++p) {
    if (p->first == dest) continue;
    if (!mds->mdsmap->is_clientreplay_or_active_or_stopping(p->first))
      continue;  // only if active
    export_warning_ack_waiting[dir].insert(p->first);
    export_notify_ack_waiting[dir].insert(p->first);  // we'll eventually get a notifyack, too!

    MExportDirNotify *notify = new MExportDirNotify(dir->dirfrag(), true,
						    pair<int,int>(mds->get_nodeid(),CDIR_AUTH_UNKNOWN),
						    pair<int,int>(mds->get_nodeid(),export_peer[dir]));
    for (set<CDir*>::iterator i = bounds.begin(); i != bounds.end(); ++i)
      notify->get_bounds().push_back((*i)->dirfrag());
    mds->send_message_mds(notify, p->first);
    
  }
  export_state[dir] = EXPORT_WARNING;

  assert(g_conf->mds_kill_export_at != 6);
  // nobody to warn?
  if (export_warning_ack_waiting.count(dir) == 0) 
    export_go(dir);  // start export.
    
  // done.
  m->put();
}


class C_M_ExportGo : public Context {
  Migrator *migrator;
  CDir *dir;
public:
  C_M_ExportGo(Migrator *m, CDir *d) : migrator(m), dir(d) {}
  void finish(int r) {
    migrator->export_go_synced(dir);
  }
};

void Migrator::export_go(CDir *dir)
{
  assert(export_peer.count(dir));
  int dest = export_peer[dir];
  dout(7) << "export_go " << *dir << " to " << dest << dendl;

  // first sync log to flush out e.g. any cap imports
  mds->mdlog->wait_for_safe(new C_M_ExportGo(this, dir));
  mds->mdlog->flush();
}

void Migrator::export_go_synced(CDir *dir)
{  
  if (export_state.count(dir) == 0 ||
      export_state[dir] != EXPORT_WARNING) {
    // export must have aborted.  
    dout(7) << "export must have aborted on " << dir << dendl;
    return;
  }

  assert(export_peer.count(dir));
  int dest = export_peer[dir];
  dout(7) << "export_go_synced " << *dir << " to " << dest << dendl;

  cache->show_subtrees();
  
  export_warning_ack_waiting.erase(dir);
  export_state[dir] = EXPORT_EXPORTING;
  assert(g_conf->mds_kill_export_at != 7);

  assert(dir->get_cum_auth_pins() == 0);

  // set ambiguous auth
  cache->adjust_subtree_auth(dir, mds->get_nodeid(), dest);

  // take away the popularity we're sending.
  utime_t now = ceph_clock_now(g_ceph_context);
  mds->balancer->subtract_export(dir, now);
  
  // fill export message with cache data
  MExportDir *req = new MExportDir(dir->dirfrag());
  map<client_t,entity_inst_t> exported_client_map;
  int num_exported_inodes = encode_export_dir(req->export_data,
					      dir,   // recur start point
					      exported_client_map,
					      now);
  ::encode(exported_client_map, req->client_map);

  // add bounds to message
  set<CDir*> bounds;
  cache->get_subtree_bounds(dir, bounds);
  for (set<CDir*>::iterator p = bounds.begin();
       p != bounds.end();
       ++p)
    req->add_export((*p)->dirfrag());

  // send
  mds->send_message_mds(req, dest);
  assert(g_conf->mds_kill_export_at != 8);

  // stats
  if (mds->logger) mds->logger->inc(l_mds_ex);
  if (mds->logger) mds->logger->inc(l_mds_iexp, num_exported_inodes);

  cache->show_subtrees();
}


/** encode_export_inode
 * update our local state for this inode to export.
 * encode relevant state to be sent over the wire.
 * used by: encode_export_dir, file_rename (if foreign)
 *
 * FIXME: the separation between CInode.encode_export and these methods 
 * is pretty arbitrary and dumb.
 */
void Migrator::encode_export_inode(CInode *in, bufferlist& enc_state, 
				   map<client_t,entity_inst_t>& exported_client_map)
{
  dout(7) << "encode_export_inode " << *in << dendl;
  assert(!in->is_replica(mds->get_nodeid()));

  // relax locks?
  if (!in->is_replicated()) {
    in->replicate_relax_locks();
    dout(20) << " did replicate_relax_locks, now " << *in << dendl;
  }

  ::encode(in->inode.ino, enc_state);
  ::encode(in->last, enc_state);
  in->encode_export(enc_state);

  // caps 
  encode_export_inode_caps(in, enc_state, exported_client_map);
}

void Migrator::encode_export_inode_caps(CInode *in, bufferlist& bl, 
					map<client_t,entity_inst_t>& exported_client_map)
{
  dout(20) << "encode_export_inode_caps " << *in << dendl;

  // encode caps
  map<client_t,Capability::Export> cap_map;
  in->export_client_caps(cap_map);
  ::encode(cap_map, bl);
  ::encode(in->get_mds_caps_wanted(), bl);

  in->state_set(CInode::STATE_EXPORTINGCAPS);
  in->get(CInode::PIN_EXPORTINGCAPS);

  // make note of clients named by exported capabilities
  for (map<client_t, Capability*>::iterator it = in->client_caps.begin();
       it != in->client_caps.end();
       ++it) 
    exported_client_map[it->first] = mds->sessionmap.get_inst(entity_name_t::CLIENT(it->first.v));
}

void Migrator::finish_export_inode_caps(CInode *in)
{
  dout(20) << "finish_export_inode_caps " << *in << dendl;

  in->state_clear(CInode::STATE_EXPORTINGCAPS);
  in->put(CInode::PIN_EXPORTINGCAPS);

  // tell (all) clients about migrating caps.. 
  for (map<client_t, Capability*>::iterator it = in->client_caps.begin();
       it != in->client_caps.end();
       ++it) {
    Capability *cap = it->second;
    dout(7) << "finish_export_inode telling client." << it->first
	    << " exported caps on " << *in << dendl;
    MClientCaps *m = new MClientCaps(CEPH_CAP_OP_EXPORT,
				     in->ino(),
				     in->find_snaprealm()->inode->ino(),
				     cap->get_cap_id(), cap->get_last_seq(), 
				     cap->pending(), cap->wanted(), 0,
				     cap->get_mseq());
    mds->send_message_client_counted(m, it->first);
  }
  in->clear_client_caps_after_export();
  mds->locker->eval(in, CEPH_CAP_LOCKS);
}

void Migrator::finish_export_inode(CInode *in, utime_t now, list<Context*>& finished)
{
  dout(12) << "finish_export_inode " << *in << dendl;

  // clean
  if (in->is_dirty())
    in->mark_clean();
  
  // clear/unpin cached_by (we're no longer the authority)
  in->clear_replica_map();
  
  // twiddle lock states for auth -> replica transition
  in->authlock.export_twiddle();
  in->linklock.export_twiddle();
  in->dirfragtreelock.export_twiddle();
  in->filelock.export_twiddle();
  in->nestlock.export_twiddle();
  in->xattrlock.export_twiddle();
  in->snaplock.export_twiddle();
  in->flocklock.export_twiddle();
  in->policylock.export_twiddle();
  
  // mark auth
  assert(in->is_auth());
  in->state_clear(CInode::STATE_AUTH);
  in->replica_nonce = CInode::EXPORT_NONCE;
  
  in->clear_dirty_rstat();

  // no more auth subtree? clear scatter dirty
  if (!in->has_subtree_root_dirfrag(mds->get_nodeid()))
    in->clear_scatter_dirty();

  in->item_open_file.remove_myself();

  in->clear_dirty_parent();

  // waiters
  in->take_waiting(CInode::WAIT_ANY_MASK, finished);

  in->finish_export(now);
  
  finish_export_inode_caps(in);

  // *** other state too?

  // move to end of LRU so we drop out of cache quickly!
  if (in->get_parent_dn()) 
    cache->lru.lru_bottouch(in->get_parent_dn());

}

int Migrator::encode_export_dir(bufferlist& exportbl,
				CDir *dir,
				map<client_t,entity_inst_t>& exported_client_map,
				utime_t now)
{
  int num_exported = 0;

  dout(7) << "encode_export_dir " << *dir << " " << dir->get_num_head_items() << " head items" << dendl;
  
  assert(dir->get_projected_version() == dir->get_version());

#ifdef MDS_VERIFY_FRAGSTAT
  if (dir->is_complete())
    dir->verify_fragstat();
#endif

  // dir 
  dirfrag_t df = dir->dirfrag();
  ::encode(df, exportbl);
  dir->encode_export(exportbl);
  
  __u32 nden = dir->items.size();
  ::encode(nden, exportbl);
  
  // dentries
  list<CDir*> subdirs;
  CDir::map_t::iterator it;
  for (it = dir->begin(); it != dir->end(); ++it) {
    CDentry *dn = it->second;
    CInode *in = dn->get_linkage()->get_inode();
    
    if (!dn->is_replicated())
      dn->lock.replicate_relax();

    num_exported++;
    
    // -- dentry
    dout(7) << "encode_export_dir exporting " << *dn << dendl;
    
    // dn name
    ::encode(dn->name, exportbl);
    ::encode(dn->last, exportbl);
    
    // state
    dn->encode_export(exportbl);
    
    // points to...
    
    // null dentry?
    if (dn->get_linkage()->is_null()) {
      exportbl.append("N", 1);  // null dentry
      continue;
    }
    
    if (dn->get_linkage()->is_remote()) {
      // remote link
      exportbl.append("L", 1);  // remote link
      
      inodeno_t ino = dn->get_linkage()->get_remote_ino();
      unsigned char d_type = dn->get_linkage()->get_remote_d_type();
      ::encode(ino, exportbl);
      ::encode(d_type, exportbl);
      continue;
    }
    
    // primary link
    // -- inode
    exportbl.append("I", 1);    // inode dentry
    
    encode_export_inode(in, exportbl, exported_client_map);  // encode, and (update state for) export
    
    // directory?
    list<CDir*> dfs;
    in->get_dirfrags(dfs);
    for (list<CDir*>::iterator p = dfs.begin(); p != dfs.end(); ++p) {
      CDir *t = *p;
      if (!t->state_test(CDir::STATE_EXPORTBOUND)) {
	// include nested dirfrag
	assert(t->get_dir_auth().first == CDIR_AUTH_PARENT);
	subdirs.push_back(t);  // it's ours, recurse (later)
      }
    }
  }

  // subdirs
  for (list<CDir*>::iterator it = subdirs.begin(); it != subdirs.end(); ++it)
    num_exported += encode_export_dir(exportbl, *it, exported_client_map, now);

  return num_exported;
}

void Migrator::finish_export_dir(CDir *dir, list<Context*>& finished, utime_t now)
{
  dout(10) << "finish_export_dir " << *dir << dendl;

  // release open_by 
  dir->clear_replica_map();

  // mark
  assert(dir->is_auth());
  dir->state_clear(CDir::STATE_AUTH);
  dir->remove_bloom();
  dir->replica_nonce = CDir::NONCE_EXPORT;

  if (dir->is_dirty())
    dir->mark_clean();

  // suck up all waiters
  dir->take_waiting(CDir::WAIT_ANY_MASK, finished);    // all dir waiters
  
  // pop
  dir->finish_export(now);

  // dentries
  list<CDir*> subdirs;
  CDir::map_t::iterator it;
  for (it = dir->begin(); it != dir->end(); ++it) {
    CDentry *dn = it->second;
    CInode *in = dn->get_linkage()->get_inode();

    // dentry
    dn->finish_export();

    // inode?
    if (dn->get_linkage()->is_primary()) {
      finish_export_inode(in, now, finished);

      // subdirs?
      in->get_nested_dirfrags(subdirs);
    }
  }

  // subdirs
  for (list<CDir*>::iterator it = subdirs.begin(); it != subdirs.end(); ++it) 
    finish_export_dir(*it, finished, now);
}

class C_MDS_ExportFinishLogged : public Context {
  Migrator *migrator;
  CDir *dir;
public:
  C_MDS_ExportFinishLogged(Migrator *m, CDir *d) : migrator(m), dir(d) {}
  void finish(int r) {
    migrator->export_logged_finish(dir);
  }
};


/*
 * i should get an export_ack from the export target.
 *
 * This function DOES put the passed message before returning
 */
void Migrator::handle_export_ack(MExportDirAck *m)
{
  CDir *dir = cache->get_dirfrag(m->get_dirfrag());
  assert(dir);
  assert(dir->is_frozen_tree_root());  // i'm exporting!

  // yay!
  dout(7) << "handle_export_ack " << *dir << dendl;

  export_warning_ack_waiting.erase(dir);
  
  export_state[dir] = EXPORT_LOGGINGFINISH;
  assert (g_conf->mds_kill_export_at != 9);
  set<CDir*> bounds;
  cache->get_subtree_bounds(dir, bounds);

  // list us second, them first.
  // this keeps authority().first in sync with subtree auth state in the journal.
  int target = export_peer[dir];
  cache->adjust_subtree_auth(dir, target, mds->get_nodeid());

  // log completion. 
  //  include export bounds, to ensure they're in the journal.
  EExport *le = new EExport(mds->mdlog, dir);
  mds->mdlog->start_entry(le);

  le->metablob.add_dir_context(dir, EMetaBlob::TO_ROOT);
  le->metablob.add_dir( dir, false );
  for (set<CDir*>::iterator p = bounds.begin();
       p != bounds.end();
       ++p) {
    CDir *bound = *p;
    le->get_bounds().insert(bound->dirfrag());
    le->metablob.add_dir_context(bound);
    le->metablob.add_dir(bound, false);
  }

  // log export completion, then finish (unfreeze, trigger finish context, etc.)
  mds->mdlog->submit_entry(le);
  mds->mdlog->wait_for_safe(new C_MDS_ExportFinishLogged(this, dir));
  mds->mdlog->flush();
  assert (g_conf->mds_kill_export_at != 10);
  
  m->put();
}

void Migrator::export_notify_abort(CDir *dir, set<CDir*>& bounds)
{
  dout(7) << "export_notify_abort " << *dir << dendl;

  for (set<int>::iterator p = export_notify_ack_waiting[dir].begin();
       p != export_notify_ack_waiting[dir].end();
       ++p) {
    MExportDirNotify *notify = new MExportDirNotify(dir->dirfrag(), false,
						    pair<int,int>(mds->get_nodeid(),export_peer[dir]),
						    pair<int,int>(mds->get_nodeid(),CDIR_AUTH_UNKNOWN));
    for (set<CDir*>::iterator i = bounds.begin(); i != bounds.end(); ++i)
      notify->get_bounds().push_back((*i)->dirfrag());
    mds->send_message_mds(notify, *p);
  }
}

/*
 * this happens if hte dest failes after i send teh export data but before it is acked
 * that is, we don't know they safely received and logged it, so we reverse our changes
 * and go on.
 */
void Migrator::export_reverse(CDir *dir)
{
  dout(7) << "export_reverse " << *dir << dendl;
  
  assert(export_state[dir] == EXPORT_EXPORTING);
  
  set<CDir*> bounds;
  cache->get_subtree_bounds(dir, bounds);

  // remove exporting pins
  list<CDir*> rq;
  rq.push_back(dir);
  while (!rq.empty()) {
    CDir *t = rq.front(); 
    rq.pop_front();
    t->abort_export();
    for (CDir::map_t::iterator p = t->items.begin(); p != t->items.end(); ++p) {
      p->second->abort_export();
      if (!p->second->get_linkage()->is_primary())
	continue;
      CInode *in = p->second->get_linkage()->get_inode();
      in->abort_export();
      if (in->is_dir())
	in->get_nested_dirfrags(rq);
    }
  }
  
  // unpin bounds
  for (set<CDir*>::iterator p = bounds.begin();
       p != bounds.end();
       ++p) {
    CDir *bd = *p;
    bd->put(CDir::PIN_EXPORTBOUND);
    bd->state_clear(CDir::STATE_EXPORTBOUND);
  }

  // adjust auth, with possible subtree merge.
  cache->adjust_subtree_auth(dir, mds->get_nodeid());
  cache->try_subtree_merge(dir);  // NOTE: may journal subtree_map as side-effect

  // notify bystanders
  export_notify_abort(dir, bounds);

  // process delayed expires
  cache->process_delayed_expire(dir);
  
  // some clean up
  export_warning_ack_waiting.erase(dir);
  export_notify_ack_waiting.erase(dir);

  // unfreeze
  dir->unfreeze_tree();

  export_unlock(dir);

  cache->show_cache();
}


/*
 * once i get the ack, and logged the EExportFinish(true),
 * send notifies (if any), otherwise go straight to finish.
 * 
 */
void Migrator::export_logged_finish(CDir *dir)
{
  dout(7) << "export_logged_finish " << *dir << dendl;

  // send notifies
  int dest = export_peer[dir];

  set<CDir*> bounds;
  cache->get_subtree_bounds(dir, bounds);

  for (set<int>::iterator p = export_notify_ack_waiting[dir].begin();
       p != export_notify_ack_waiting[dir].end();
       ++p) {
    MExportDirNotify *notify = new MExportDirNotify(dir->dirfrag(), true,
						    pair<int,int>(mds->get_nodeid(), dest),
						    pair<int,int>(dest, CDIR_AUTH_UNKNOWN));

    for (set<CDir*>::iterator i = bounds.begin(); i != bounds.end(); ++i)
      notify->get_bounds().push_back((*i)->dirfrag());
    
    mds->send_message_mds(notify, *p);
  }

  // wait for notifyacks
  export_state[dir] = EXPORT_NOTIFYING;
  assert (g_conf->mds_kill_export_at != 11);
  
  // no notifies to wait for?
  if (export_notify_ack_waiting[dir].empty())
    export_finish(dir);  // skip notify/notify_ack stage.
}

/*
 * warning:
 *  i'll get an ack from each bystander.
 *  when i get them all, do the export.
 * notify:
 *  i'll get an ack from each bystander.
 *  when i get them all, unfreeze and send the finish.
 *
 * This function DOES put the passed message before returning
 */
void Migrator::handle_export_notify_ack(MExportDirNotifyAck *m)
{
  CDir *dir = cache->get_dirfrag(m->get_dirfrag());
  assert(dir);
  int from = m->get_source().num();
    
  if (export_state.count(dir) && export_state[dir] == EXPORT_WARNING) {
    // exporting. process warning.
    dout(7) << "handle_export_notify_ack from " << m->get_source()
	    << ": exporting, processing warning on "
	    << *dir << dendl;
    assert(export_warning_ack_waiting.count(dir));
    export_warning_ack_waiting[dir].erase(from);
    
    if (export_warning_ack_waiting[dir].empty()) 
      export_go(dir);     // start export.
  } 
  else if (export_state.count(dir) && export_state[dir] == EXPORT_NOTIFYING) {
    // exporting. process notify.
    dout(7) << "handle_export_notify_ack from " << m->get_source()
	    << ": exporting, processing notify on "
	    << *dir << dendl;
    assert(export_notify_ack_waiting.count(dir));
    export_notify_ack_waiting[dir].erase(from);
    
    if (export_notify_ack_waiting[dir].empty())
      export_finish(dir);
  }
  else if (import_state.count(dir->dirfrag()) && import_state[dir->dirfrag()] == IMPORT_ABORTING) {
    // reversing import
    dout(7) << "handle_export_notify_ack from " << m->get_source()
	    << ": aborting import on "
	    << *dir << dendl;
    assert(import_bystanders[dir].count(from));
    import_bystanders[dir].erase(from);
    if (import_bystanders[dir].empty()) {
      import_bystanders.erase(dir);
      import_reverse_unfreeze(dir);
    }
  }

  m->put();
}

void Migrator::export_unlock(CDir *dir)
{
  dout(10) << "export_unlock " << *dir << dendl;

  mds->locker->rdlock_finish_set(export_locks[dir]);

  list<Context*> ls;
  mds->queue_waiters(ls);
}

void Migrator::export_finish(CDir *dir)
{
  dout(5) << "export_finish " << *dir << dendl;

  assert (g_conf->mds_kill_export_at != 12);
  if (export_state.count(dir) == 0) {
    dout(7) << "target must have failed, not sending final commit message.  export succeeded anyway." << dendl;
    return;
  }

  // send finish/commit to new auth
  if (mds->mdsmap->is_clientreplay_or_active_or_stopping(export_peer[dir])) {
    mds->send_message_mds(new MExportDirFinish(dir->dirfrag()), export_peer[dir]);
  } else {
    dout(7) << "not sending MExportDirFinish, dest has failed" << dendl;
  }
  assert(g_conf->mds_kill_export_at != 13);
  
  // finish export (adjust local cache state)
  C_Contexts *fin = new C_Contexts(g_ceph_context);
  finish_export_dir(dir, fin->contexts, ceph_clock_now(g_ceph_context));
  dir->add_waiter(CDir::WAIT_UNFREEZE, fin);

  // unfreeze
  dout(7) << "export_finish unfreezing" << dendl;
  dir->unfreeze_tree();
  
  // unpin bounds
  set<CDir*> bounds;
  cache->get_subtree_bounds(dir, bounds);
  for (set<CDir*>::iterator p = bounds.begin();
       p != bounds.end();
       ++p) {
    CDir *bd = *p;
    bd->put(CDir::PIN_EXPORTBOUND);
    bd->state_clear(CDir::STATE_EXPORTBOUND);
  }

  // adjust auth, with possible subtree merge.
  //  (we do this _after_ removing EXPORTBOUND pins, to allow merges)
  cache->adjust_subtree_auth(dir, export_peer[dir]);
  cache->try_subtree_merge(dir);  // NOTE: may journal subtree_map as sideeffect

  // no more auth subtree? clear scatter dirty
  if (!dir->get_inode()->is_auth() &&
      !dir->get_inode()->has_subtree_root_dirfrag(mds->get_nodeid()))
    dir->get_inode()->clear_scatter_dirty();

  // unpin path
  export_unlock(dir);

  // discard delayed expires
  cache->discard_delayed_expire(dir);

  // remove from exporting list, clean up state
  dir->state_clear(CDir::STATE_EXPORTING);
  export_state.erase(dir);
  export_locks.erase(dir);
  export_peer.erase(dir);
  export_notify_ack_waiting.erase(dir);

  // queue finishers
  mds->queue_waiters(export_finish_waiters[dir]);
  export_finish_waiters.erase(dir);

  cache->show_subtrees();
  audit();

  // send pending import_maps?
  mds->mdcache->maybe_send_pending_resolves();
  
  maybe_do_queued_export();
}








// ==========================================================
// IMPORT

void Migrator::handle_export_discover(MExportDirDiscover *m)
{
  int from = m->get_source_mds();
  assert(from != mds->get_nodeid());

  dout(7) << "handle_export_discover on " << m->get_path() << dendl;

  // note import state
  dirfrag_t df = m->get_dirfrag();
  // only start discovering on this message once.
  if (!m->started) {
    m->started = true;
    import_pending_msg[df] = m;
    import_state[df] = IMPORT_DISCOVERING;
    import_peer[df] = from;
  } else {
    // am i retrying after ancient path_traverse results?
    if (import_pending_msg.count(df) == 0 || import_pending_msg[df] != m) {
      dout(7) << " dropping obsolete message" << dendl;
      m->put();
      return;
    }
  }

  if (!mds->mdcache->is_open()) {
    dout(5) << " waiting for root" << dendl;
    mds->mdcache->wait_for_open(new C_MDS_RetryMessage(mds, m));
    return;
  }

  assert (g_conf->mds_kill_import_at != 1);

  // do we have it?
  CInode *in = cache->get_inode(m->get_dirfrag().ino);
  if (!in) {
    // must discover it!
    filepath fpath(m->get_path());
    vector<CDentry*> trace;
    int r = cache->path_traverse(NULL, m, NULL, fpath, &trace, NULL, MDS_TRAVERSE_DISCOVER);
    if (r > 0) return;
    if (r < 0) {
      dout(7) << "handle_export_discover_2 failed to discover or not dir " << m->get_path() << ", NAK" << dendl;
      assert(0);    // this shouldn't happen if the auth pins his path properly!!!! 
    }

    assert(0); // this shouldn't happen; the get_inode above would have succeeded.
  }

  // yay
  dout(7) << "handle_export_discover have " << df << " inode " << *in << dendl;
  
  import_state[m->get_dirfrag()] = IMPORT_DISCOVERED;
  import_pending_msg.erase(m->get_dirfrag());

  // pin inode in the cache (for now)
  assert(in->is_dir());
  in->get(CInode::PIN_IMPORTING);

  // reply
  dout(7) << " sending export_discover_ack on " << *in << dendl;
  mds->send_message_mds(new MExportDirDiscoverAck(df), import_peer[df]);
  m->put();
  assert (g_conf->mds_kill_import_at != 2);  
}

void Migrator::import_reverse_discovering(dirfrag_t df)
{
  import_pending_msg.erase(df);
  import_state.erase(df);
  import_peer.erase(df);
}

void Migrator::import_reverse_discovered(dirfrag_t df, CInode *diri)
{
  // unpin base
  diri->put(CInode::PIN_IMPORTING);
  import_state.erase(df);
  import_peer.erase(df);
}

void Migrator::import_reverse_prepping(CDir *dir)
{
  import_pending_msg.erase(dir->dirfrag());
  set<CDir*> bounds;
  cache->map_dirfrag_set(import_bound_ls[dir], bounds);
  import_remove_pins(dir, bounds);
  import_reverse_final(dir);
}

/* This function DOES put the passed message before returning*/
void Migrator::handle_export_cancel(MExportDirCancel *m)
{
  dout(7) << "handle_export_cancel on " << m->get_dirfrag() << dendl;
  dirfrag_t df = m->get_dirfrag();
  if (import_state[df] == IMPORT_DISCOVERING) {
    import_reverse_discovering(df);
  } else if (import_state[df] == IMPORT_DISCOVERED) {
    CInode *in = cache->get_inode(df.ino);
    assert(in);
    import_reverse_discovered(df, in);
  } else if (import_state[df] == IMPORT_PREPPING) {
    CDir *dir = mds->mdcache->get_dirfrag(df);
    assert(dir);
    import_reverse_prepping(dir);
  } else if (import_state[df] == IMPORT_PREPPED) {
    CDir *dir = mds->mdcache->get_dirfrag(df);
    assert(dir);
    set<CDir*> bounds;
    cache->get_subtree_bounds(dir, bounds);
    import_remove_pins(dir, bounds);
    // adjust auth back to the exportor
    cache->adjust_subtree_auth(dir, import_peer[df]);
    cache->try_subtree_merge(dir);
    import_reverse_unfreeze(dir);
  } else {
    assert(0 == "got export_cancel in weird state");
  }
  m->put();
}

/* This function DOES put the passed message before returning*/
void Migrator::handle_export_prep(MExportDirPrep *m)
{
  int oldauth = m->get_source().num();
  assert(oldauth != mds->get_nodeid());

  CDir *dir;
  CInode *diri;
  list<Context*> finished;

  // assimilate root dir.
  if (!m->did_assim()) {
    diri = cache->get_inode(m->get_dirfrag().ino);
    assert(diri);
    bufferlist::iterator p = m->basedir.begin();
    dir = cache->add_replica_dir(p, diri, oldauth, finished);
    dout(7) << "handle_export_prep on " << *dir << " (first pass)" << dendl;
  } else {
    if (import_pending_msg.count(m->get_dirfrag()) == 0 ||
	import_pending_msg[m->get_dirfrag()] != m) {
      dout(7) << "handle_export_prep obsolete message, dropping" << dendl;
      m->put();
      return;
    }

    dir = cache->get_dirfrag(m->get_dirfrag());
    assert(dir);
    dout(7) << "handle_export_prep on " << *dir << " (subsequent pass)" << dendl;
    diri = dir->get_inode();
  }
  assert(dir->is_auth() == false);

  cache->show_subtrees();

  // build import bound map
  map<inodeno_t, fragset_t> import_bound_fragset;
  for (list<dirfrag_t>::iterator p = m->get_bounds().begin();
       p != m->get_bounds().end();
       ++p) {
    dout(10) << " bound " << *p << dendl;
    import_bound_fragset[p->ino].insert(p->frag);
  }

  // assimilate contents?
  if (!m->did_assim()) {
    dout(7) << "doing assim on " << *dir << dendl;
    m->mark_assim();  // only do this the first time!
    import_pending_msg[dir->dirfrag()] = m;

    // change import state
    import_state[dir->dirfrag()] = IMPORT_PREPPING;
    import_bound_ls[dir] = m->get_bounds();
    assert(g_conf->mds_kill_import_at != 3);

    // move pin to dir
    diri->put(CInode::PIN_IMPORTING);
    dir->get(CDir::PIN_IMPORTING);  
    dir->state_set(CDir::STATE_IMPORTING);
    
    // bystander list
    import_bystanders[dir] = m->get_bystanders();
    dout(7) << "bystanders are " << import_bystanders[dir] << dendl;

    // assimilate traces to exports
    // each trace is: df ('-' | ('f' dir | 'd') dentry inode (dir dentry inode)*)
    for (list<bufferlist>::iterator p = m->traces.begin();
	 p != m->traces.end();
	 ++p) {
      bufferlist::iterator q = p->begin();
      dirfrag_t df;
      ::decode(df, q);
      char start;
      ::decode(start, q);
      dout(10) << " trace from " << df << " start " << start << " len " << p->length() << dendl;

      CInode *in;
      CDir *cur = 0;
      if (start == 'd') {
	cur = cache->get_dirfrag(df);
	assert(cur);
	dout(10) << "  had " << *cur << dendl;
      } else if (start == 'f') {
	in = cache->get_inode(df.ino);
	assert(in);
	dout(10) << "  had " << *in << dendl;
	cur = cache->add_replica_dir(q, in, oldauth, finished);
 	dout(10) << "  added " << *cur << dendl;
      } else if (start == '-') {
	// nothing
      } else
	assert(0 == "unrecognized start char");
      while (start != '-') {
	CDentry *dn = cache->add_replica_dentry(q, cur, finished);
	dout(10) << "  added " << *dn << dendl;
	CInode *in = cache->add_replica_inode(q, dn, finished);
	dout(10) << "  added " << *in << dendl;
	if (q.end())
	  break;
	cur = cache->add_replica_dir(q, in, oldauth, finished);
	dout(10) << "  added " << *cur << dendl;
      }
    }

    // make bound sticky
    for (map<inodeno_t,fragset_t>::iterator p = import_bound_fragset.begin();
	 p != import_bound_fragset.end();
	 ++p) {
      CInode *in = cache->get_inode(p->first);
      assert(in);
      in->get_stickydirs();
      dout(7) << " set stickydirs on bound inode " << *in << dendl;
    }

  } else {
    dout(7) << " not doing assim on " << *dir << dendl;
  }

  if (!finished.empty())
    mds->queue_waiters(finished);


  // open all bounds
  set<CDir*> import_bounds;
  for (map<inodeno_t,fragset_t>::iterator p = import_bound_fragset.begin();
       p != import_bound_fragset.end();
       ++p) {
    CInode *in = cache->get_inode(p->first);
    assert(in);

    // map fragset into a frag_t list, based on the inode fragtree
    list<frag_t> fglist;
    for (set<frag_t>::iterator q = p->second.begin(); q != p->second.end(); ++q)
      in->dirfragtree.get_leaves_under(*q, fglist);
    dout(10) << " bound inode " << p->first << " fragset " << p->second << " maps to " << fglist << dendl;
    
    for (list<frag_t>::iterator q = fglist.begin();
	 q != fglist.end();
	 ++q) {
      CDir *bound = cache->get_dirfrag(dirfrag_t(p->first, *q));
      if (!bound) {
	dout(7) << "  opening bounding dirfrag " << *q << " on " << *in << dendl;
	cache->open_remote_dirfrag(in, *q,
				   new C_MDS_RetryMessage(mds, m));
	return;
      }

      if (!bound->state_test(CDir::STATE_IMPORTBOUND)) {
	dout(7) << "  pinning import bound " << *bound << dendl;
	bound->get(CDir::PIN_IMPORTBOUND);
	bound->state_set(CDir::STATE_IMPORTBOUND);
      } else {
	dout(7) << "  already pinned import bound " << *bound << dendl;
      }
      import_bounds.insert(bound);
    }
  }

  dout(7) << " all ready, noting auth and freezing import region" << dendl;
  
  // note that i am an ambiguous auth for this subtree.
  // specify bounds, since the exporter explicitly defines the region.
  cache->adjust_bounded_subtree_auth(dir, import_bounds, 
				     pair<int,int>(oldauth, mds->get_nodeid()));
  cache->verify_subtree_bounds(dir, import_bounds);
  
  // freeze.
  dir->_freeze_tree();

  // ok!
  dout(7) << " sending export_prep_ack on " << *dir << dendl;
  mds->send_message(new MExportDirPrepAck(dir->dirfrag()), m->get_connection());
  
  // note new state
  import_state[dir->dirfrag()] = IMPORT_PREPPED;
  import_pending_msg.erase(dir->dirfrag());
  assert(g_conf->mds_kill_import_at != 4);
  // done 
  m->put();

}




class C_MDS_ImportDirLoggedStart : public Context {
  Migrator *migrator;
  dirfrag_t df;
  CDir *dir;
  int from;
public:
  map<client_t,entity_inst_t> imported_client_map;
  map<client_t,uint64_t> sseqmap;

  C_MDS_ImportDirLoggedStart(Migrator *m, CDir *d, int f) :
    migrator(m), df(d->dirfrag()), dir(d), from(f) {
  }
  void finish(int r) {
    migrator->import_logged_start(df, dir, from, imported_client_map, sseqmap);
  }
};

/* This function DOES put the passed message before returning*/
void Migrator::handle_export_dir(MExportDir *m)
{
  assert (g_conf->mds_kill_import_at != 5);
  CDir *dir = cache->get_dirfrag(m->dirfrag);
  assert(dir);
  
  utime_t now = ceph_clock_now(g_ceph_context);
  int oldauth = m->get_source().num();
  dout(7) << "handle_export_dir importing " << *dir << " from " << oldauth << dendl;
  assert(dir->is_auth() == false);

  cache->show_subtrees();

  C_MDS_ImportDirLoggedStart *onlogged = new C_MDS_ImportDirLoggedStart(this, dir, m->get_source().num());

  // start the journal entry
  EImportStart *le = new EImportStart(mds->mdlog, dir->dirfrag(), m->bounds);
  mds->mdlog->start_entry(le);

  le->metablob.add_dir_context(dir);
  
  // adjust auth (list us _first_)
  cache->adjust_subtree_auth(dir, mds->get_nodeid(), oldauth);

  // new client sessions, open these after we journal
  // include imported sessions in EImportStart
  bufferlist::iterator cmp = m->client_map.begin();
  ::decode(onlogged->imported_client_map, cmp);
  assert(cmp.end());
  le->cmapv = mds->server->prepare_force_open_sessions(onlogged->imported_client_map, onlogged->sseqmap);
  le->client_map.claim(m->client_map);

  bufferlist::iterator blp = m->export_data.begin();
  int num_imported_inodes = 0;
  while (!blp.end()) {
    num_imported_inodes += 
      decode_import_dir(blp,
			oldauth, 
			dir,                 // import root
			le,
			mds->mdlog->get_current_segment(),
			import_caps[dir],
			import_updated_scatterlocks[dir],
			now);
  }
  dout(10) << " " << m->bounds.size() << " imported bounds" << dendl;
  
  // include bounds in EImportStart
  set<CDir*> import_bounds;
  cache->get_subtree_bounds(dir, import_bounds);
  for (set<CDir*>::iterator it = import_bounds.begin();
       it != import_bounds.end();
       ++it) 
    le->metablob.add_dir(*it, false);  // note that parent metadata is already in the event

  // adjust popularity
  mds->balancer->add_import(dir, now);

  dout(7) << "handle_export_dir did " << *dir << dendl;

  // note state
  import_state[dir->dirfrag()] = IMPORT_LOGGINGSTART;
  assert (g_conf->mds_kill_import_at != 6);

  // log it
  mds->mdlog->submit_entry(le);
  mds->mdlog->wait_for_safe(onlogged);
  mds->mdlog->flush();

  // some stats
  if (mds->logger) {
    mds->logger->inc(l_mds_im);
    mds->logger->inc(l_mds_iim, num_imported_inodes);
  }

  m->put();
}


/*
 * this is an import helper
 *  called by import_finish, and import_reverse and friends.
 */
void Migrator::import_remove_pins(CDir *dir, set<CDir*>& bounds)
{
  // root
  dir->put(CDir::PIN_IMPORTING);
  dir->state_clear(CDir::STATE_IMPORTING);

  // bounding inodes
  set<inodeno_t> did;
  for (list<dirfrag_t>::iterator p = import_bound_ls[dir].begin();
       p != import_bound_ls[dir].end();
       ++p) {
    if (did.count(p->ino))
      continue;
    did.insert(p->ino);
    CInode *in = cache->get_inode(p->ino);
    assert(in);
    in->put_stickydirs();
  }

  if (import_state[dir->dirfrag()] >= IMPORT_PREPPED) {
    // bounding dirfrags
    for (set<CDir*>::iterator it = bounds.begin();
	 it != bounds.end();
	 ++it) {
      CDir *bd = *it;
      bd->put(CDir::PIN_IMPORTBOUND);
      bd->state_clear(CDir::STATE_IMPORTBOUND);
    }
  }
}


/*
 * note: this does teh full work of reversing and import and cleaning up
 *  state.  
 * called by both handle_mds_failure and by handle_resolve (if we are
 *  a survivor coping with an exporter failure+recovery).
 */
void Migrator::import_reverse(CDir *dir)
{
  dout(7) << "import_reverse " << *dir << dendl;

  set<CDir*> bounds;
  cache->get_subtree_bounds(dir, bounds);

  // remove pins
  import_remove_pins(dir, bounds);

  // update auth, with possible subtree merge.
  assert(dir->is_subtree_root());
  if (mds->is_resolve())
    cache->trim_non_auth_subtree(dir);
  cache->adjust_subtree_auth(dir, import_peer[dir->dirfrag()]);

  if (!dir->get_inode()->is_auth() &&
      !dir->get_inode()->has_subtree_root_dirfrag(mds->get_nodeid()))
    dir->get_inode()->clear_scatter_dirty();

  // adjust auth bits.
  list<CDir*> q;
  q.push_back(dir);
  while (!q.empty()) {
    CDir *cur = q.front();
    q.pop_front();
    
    // dir
    assert(cur->is_auth());
    cur->state_clear(CDir::STATE_AUTH);
    cur->remove_bloom();
    cur->clear_replica_map();
    if (cur->is_dirty())
      cur->mark_clean();

    CDir::map_t::iterator it;
    for (it = cur->begin(); it != cur->end(); ++it) {
      CDentry *dn = it->second;

      // dentry
      dn->state_clear(CDentry::STATE_AUTH);
      dn->clear_replica_map();
      if (dn->is_dirty()) 
	dn->mark_clean();

      // inode?
      if (dn->get_linkage()->is_primary()) {
	CInode *in = dn->get_linkage()->get_inode();
	in->state_clear(CDentry::STATE_AUTH);
	in->clear_replica_map();
	if (in->is_dirty()) 
	  in->mark_clean();
	in->clear_dirty_rstat();
	if (!in->has_subtree_root_dirfrag(mds->get_nodeid()))
	  in->clear_scatter_dirty();

	in->clear_dirty_parent();

	in->authlock.clear_gather();
	in->linklock.clear_gather();
	in->dirfragtreelock.clear_gather();
	in->filelock.clear_gather();

	// non-bounding dir?
	list<CDir*> dfs;
	in->get_dirfrags(dfs);
	for (list<CDir*>::iterator p = dfs.begin(); p != dfs.end(); ++p)
	  if (bounds.count(*p) == 0)
	    q.push_back(*p);
      }
    }
  }

  // reexport caps
  for (map<CInode*, map<client_t,Capability::Export> >::iterator p = import_caps[dir].begin();
       p != import_caps[dir].end();
       ++p) {
    CInode *in = p->first;
    dout(20) << " reexporting caps on " << *in << dendl;
    /*
     * bleh.. just export all caps for this inode.  the auth mds
     * will pick them up during recovery.
     */
    bufferlist bl; // throw this away
    map<client_t,entity_inst_t> exported_client_map;  // throw this away too
    encode_export_inode_caps(in, bl, exported_client_map);
    finish_export_inode_caps(in);
  }
	 
  // log our failure
  mds->mdlog->start_submit_entry(new EImportFinish(dir, false));	// log failure

  cache->try_subtree_merge(dir);  // NOTE: this may journal subtree map as side effect

  // bystanders?
  if (import_bystanders[dir].empty()) {
    dout(7) << "no bystanders, finishing reverse now" << dendl;
    import_reverse_unfreeze(dir);
  } else {
    // notify them; wait in aborting state
    dout(7) << "notifying bystanders of abort" << dendl;
    import_notify_abort(dir, bounds);
    import_state[dir->dirfrag()] = IMPORT_ABORTING;
    assert (g_conf->mds_kill_import_at != 10);
  }
}

void Migrator::import_notify_finish(CDir *dir, set<CDir*>& bounds)
{
  dout(7) << "import_notify_finish " << *dir << dendl;

  for (set<int>::iterator p = import_bystanders[dir].begin();
       p != import_bystanders[dir].end();
       ++p) {
    MExportDirNotify *notify =
      new MExportDirNotify(dir->dirfrag(), false,
			   pair<int,int>(import_peer[dir->dirfrag()], mds->get_nodeid()),
			   pair<int,int>(mds->get_nodeid(), CDIR_AUTH_UNKNOWN));
    for (set<CDir*>::iterator i = bounds.begin(); i != bounds.end(); ++i)
      notify->get_bounds().push_back((*i)->dirfrag());
    mds->send_message_mds(notify, *p);
  }
}

void Migrator::import_notify_abort(CDir *dir, set<CDir*>& bounds)
{
  dout(7) << "import_notify_abort " << *dir << dendl;
  
  for (set<int>::iterator p = import_bystanders[dir].begin();
       p != import_bystanders[dir].end();
       ++p) {
    MExportDirNotify *notify =
      new MExportDirNotify(dir->dirfrag(), true,
			   pair<int,int>(import_peer[dir->dirfrag()], mds->get_nodeid()),
			   pair<int,int>(import_peer[dir->dirfrag()], CDIR_AUTH_UNKNOWN));
    for (set<CDir*>::iterator i = bounds.begin(); i != bounds.end(); ++i)
      notify->get_bounds().push_back((*i)->dirfrag());
    mds->send_message_mds(notify, *p);
  }
}

void Migrator::import_reverse_unfreeze(CDir *dir)
{
  assert(dir);
  dout(7) << "import_reverse_unfreeze " << *dir << dendl;
  dir->unfreeze_tree();
  list<Context*> ls;
  mds->queue_waiters(ls);
  cache->discard_delayed_expire(dir);
  import_reverse_final(dir);
}

void Migrator::import_reverse_final(CDir *dir) 
{
  dout(7) << "import_reverse_final " << *dir << dendl;

  // clean up
  import_state.erase(dir->dirfrag());
  import_peer.erase(dir->dirfrag());
  import_bystanders.erase(dir);
  import_bound_ls.erase(dir);
  import_updated_scatterlocks.erase(dir);
  import_caps.erase(dir);

  // send pending import_maps?
  mds->mdcache->maybe_send_pending_resolves();

  cache->show_subtrees();
  //audit();  // this fails, bc we munge up the subtree map during handle_import_map (resolve phase)
}




void Migrator::import_logged_start(dirfrag_t df, CDir *dir, int from,
				   map<client_t,entity_inst_t>& imported_client_map,
				   map<client_t,uint64_t>& sseqmap)
{
  if (import_state.count(df) == 0 ||
      import_state[df] != IMPORT_LOGGINGSTART) {
    dout(7) << "import " << df << " must have aborted" << dendl;
    return;
  }

  dout(7) << "import_logged " << *dir << dendl;

  // note state
  import_state[dir->dirfrag()] = IMPORT_ACKING;

  assert (g_conf->mds_kill_import_at != 7);

  // force open client sessions and finish cap import
  mds->server->finish_force_open_sessions(imported_client_map, sseqmap);
  
  for (map<CInode*, map<client_t,Capability::Export> >::iterator p = import_caps[dir].begin();
       p != import_caps[dir].end();
       ++p) {
    finish_import_inode_caps(p->first, from, p->second);
  }
  
  // send notify's etc.
  dout(7) << "sending ack for " << *dir << " to old auth mds." << from << dendl;

  // test surviving observer of a failed migration that did not complete
  //assert(dir->replica_map.size() < 2 || mds->whoami != 0);

  mds->send_message_mds(new MExportDirAck(dir->dirfrag()), from);
  assert (g_conf->mds_kill_import_at != 8);

  cache->show_subtrees();
}

/* This function DOES put the passed message before returning*/
void Migrator::handle_export_finish(MExportDirFinish *m)
{
  CDir *dir = cache->get_dirfrag(m->get_dirfrag());
  assert(dir);
  dout(7) << "handle_export_finish on " << *dir << dendl;
  import_finish(dir, false);
  m->put();
}

void Migrator::import_finish(CDir *dir, bool notify)
{
  dout(7) << "import_finish on " << *dir << dendl;

  // log finish
  assert(g_conf->mds_kill_import_at != 9);

  // clear updated scatterlocks
  /*
  for (list<ScatterLock*>::iterator p = import_updated_scatterlocks[dir].begin();
       p != import_updated_scatterlocks[dir].end();
       ++p) 
    (*p)->clear_updated();
  */

  // remove pins
  set<CDir*> bounds;
  cache->get_subtree_bounds(dir, bounds);

  if (notify)
    import_notify_finish(dir, bounds);

  import_remove_pins(dir, bounds);

  map<CInode*, map<client_t,Capability::Export> > cap_imports;
  import_caps[dir].swap(cap_imports);

  // clear import state (we're done!)
  import_state.erase(dir->dirfrag());
  import_peer.erase(dir->dirfrag());
  import_bystanders.erase(dir);
  import_bound_ls.erase(dir);
  import_caps.erase(dir);
  import_updated_scatterlocks.erase(dir);

  mds->mdlog->start_submit_entry(new EImportFinish(dir, true));

  // adjust auth, with possible subtree merge.
  cache->adjust_subtree_auth(dir, mds->get_nodeid());
  cache->try_subtree_merge(dir);   // NOTE: this may journal subtree_map as sideffect

  // process delayed expires
  cache->process_delayed_expire(dir);

  // ok now unfreeze (and thus kick waiters)
  dir->unfreeze_tree();
  cache->show_subtrees();
  //audit();  // this fails, bc we munge up the subtree map during handle_import_map (resolve phase)

  list<Context*> ls;
  mds->queue_waiters(ls);

  // re-eval imported caps
  for (map<CInode*, map<client_t,Capability::Export> >::iterator p = cap_imports.begin();
       p != cap_imports.end();
       ++p)
    if (p->first->is_auth())
      mds->locker->eval(p->first, CEPH_CAP_LOCKS, true);

  // send pending import_maps?
  mds->mdcache->maybe_send_pending_resolves();

  // did i just import mydir?
  if (dir->ino() == MDS_INO_MDSDIR(mds->whoami))
    cache->populate_mydir();

  // is it empty?
  if (dir->get_num_head_items() == 0 &&
      !dir->inode->is_auth()) {
    // reexport!
    export_empty_import(dir);
  }
}


void Migrator::decode_import_inode(CDentry *dn, bufferlist::iterator& blp, int oldauth,
				   LogSegment *ls, uint64_t log_offset,
				   map<CInode*, map<client_t,Capability::Export> >& cap_imports,
				   list<ScatterLock*>& updated_scatterlocks)
{  
  dout(15) << "decode_import_inode on " << *dn << dendl;

  inodeno_t ino;
  snapid_t last;
  ::decode(ino, blp);
  ::decode(last, blp);

  bool added = false;
  CInode *in = cache->get_inode(ino, last);
  if (!in) {
    in = new CInode(mds->mdcache, true, 1, last);
    added = true;
  } else {
    in->state_set(CInode::STATE_AUTH);
  }

  // state after link  -- or not!  -sage
  in->decode_import(blp, ls);  // cap imports are noted for later action

  // note that we are journaled at this log offset
  in->last_journaled = log_offset;

  // caps
  decode_import_inode_caps(in, blp, cap_imports);

  // link before state  -- or not!  -sage
  if (dn->get_linkage()->get_inode() != in) {
    assert(!dn->get_linkage()->get_inode());
    dn->dir->link_primary_inode(dn, in);
  }
 
  // add inode?
  if (added) {
    cache->add_inode(in);
    dout(10) << "added " << *in << dendl;
  } else {
    dout(10) << "  had " << *in << dendl;
  }

  if (in->inode.is_dirty_rstat())
    in->mark_dirty_rstat();
  
  // clear if dirtyscattered, since we're going to journal this
  //  but not until we _actually_ finish the import...
  if (in->filelock.is_dirty()) {
    updated_scatterlocks.push_back(&in->filelock);
    mds->locker->mark_updated_scatterlock(&in->filelock);
  }

  // adjust replica list
  //assert(!in->is_replica(oldauth));  // not true on failed export
  in->add_replica(oldauth, CInode::EXPORT_NONCE);
  if (in->is_replica(mds->get_nodeid()))
    in->remove_replica(mds->get_nodeid());
  
}

void Migrator::decode_import_inode_caps(CInode *in,
					bufferlist::iterator &blp,
					map<CInode*, map<client_t,Capability::Export> >& cap_imports)
{
  map<client_t,Capability::Export> cap_map;
  ::decode(cap_map, blp);
  ::decode(in->get_mds_caps_wanted(), blp);
  if (!cap_map.empty() || !in->get_mds_caps_wanted().empty()) {
    cap_imports[in].swap(cap_map);
    in->get(CInode::PIN_IMPORTINGCAPS);
  }
}

void Migrator::finish_import_inode_caps(CInode *in, int from, 
					map<client_t,Capability::Export> &cap_map)
{
  for (map<client_t,Capability::Export>::iterator it = cap_map.begin();
       it != cap_map.end();
       ++it) {
    dout(10) << "finish_import_inode_caps for client." << it->first << " on " << *in << dendl;
    Session *session = mds->sessionmap.get_session(entity_name_t::CLIENT(it->first.v));
    assert(session);

    Capability *cap = in->get_client_cap(it->first);
    if (!cap) {
      cap = in->add_client_cap(it->first, session);
    }
    cap->merge(it->second);

    mds->mdcache->do_cap_import(session, in, cap);
  }

  in->replica_caps_wanted = 0;
  in->put(CInode::PIN_IMPORTINGCAPS);
}

int Migrator::decode_import_dir(bufferlist::iterator& blp,
				int oldauth,
				CDir *import_root,
				EImportStart *le,
				LogSegment *ls,
				map<CInode*, map<client_t,Capability::Export> >& cap_imports,
				list<ScatterLock*>& updated_scatterlocks, utime_t now)
{
  // set up dir
  dirfrag_t df;
  ::decode(df, blp);

  CInode *diri = cache->get_inode(df.ino);
  assert(diri);
  CDir *dir = diri->get_or_open_dirfrag(mds->mdcache, df.frag);
  assert(dir);
  
  dout(7) << "decode_import_dir " << *dir << dendl;

  // assimilate state
  dir->decode_import(blp, now, ls);

  // mark  (may already be marked from get_or_open_dir() above)
  if (!dir->is_auth())
    dir->state_set(CDir::STATE_AUTH);

  // adjust replica list
  //assert(!dir->is_replica(oldauth));    // not true on failed export
  dir->add_replica(oldauth);
  if (dir->is_replica(mds->get_nodeid()))
    dir->remove_replica(mds->get_nodeid());

  // add to journal entry
  if (le) 
    le->metablob.add_import_dir(dir);

  int num_imported = 0;

  // take all waiters on this dir
  // NOTE: a pass of imported data is guaranteed to get all of my waiters because
  // a replica's presense in my cache implies/forces it's presense in authority's.
  list<Context*> waiters;
  
  dir->take_waiting(CDir::WAIT_ANY_MASK, waiters);
  for (list<Context*>::iterator it = waiters.begin();
       it != waiters.end();
       ++it) 
    import_root->add_waiter(CDir::WAIT_UNFREEZE, *it);  // UNFREEZE will get kicked both on success or failure
  
  dout(15) << "doing contents" << dendl;
  
  // contents
  __u32 nden;
  ::decode(nden, blp);
  
  for (; nden>0; nden--) {
    num_imported++;
    
    // dentry
    string dname;
    snapid_t last;
    ::decode(dname, blp);
    ::decode(last, blp);
    
    CDentry *dn = dir->lookup_exact_snap(dname, last);
    if (!dn)
      dn = dir->add_null_dentry(dname, 1, last);
    
    dn->decode_import(blp, ls);

    dn->add_replica(oldauth, CDentry::EXPORT_NONCE);
    if (dn->is_replica(mds->get_nodeid()))
      dn->remove_replica(mds->get_nodeid());

    dout(15) << "decode_import_dir got " << *dn << dendl;
    
    // points to...
    char icode;
    ::decode(icode, blp);
    
    if (icode == 'N') {
      // null dentry
      assert(dn->get_linkage()->is_null());  
      
      // fall thru
    }
    else if (icode == 'L') {
      // remote link
      inodeno_t ino;
      unsigned char d_type;
      ::decode(ino, blp);
      ::decode(d_type, blp);
      if (dn->get_linkage()->is_remote()) {
	assert(dn->get_linkage()->get_remote_ino() == ino);
      } else {
	dir->link_remote_inode(dn, ino, d_type);
      }
    }
    else if (icode == 'I') {
      // inode
      assert(le);
      decode_import_inode(dn, blp, oldauth, ls, le->get_start_off(), cap_imports, updated_scatterlocks);
    }
    
    // add dentry to journal entry
    if (le)
      le->metablob.add_import_dentry(dn);
  }
  
#ifdef MDS_VERIFY_FRAGSTAT
  if (dir->is_complete())
    dir->verify_fragstat();
#endif

  dout(7) << "decode_import_dir done " << *dir << dendl;
  return num_imported;
}





// authority bystander

/* This function DOES put the passed message before returning*/
void Migrator::handle_export_notify(MExportDirNotify *m)
{
  CDir *dir = cache->get_dirfrag(m->get_dirfrag());

  int from = m->get_source().num();
  pair<int,int> old_auth = m->get_old_auth();
  pair<int,int> new_auth = m->get_new_auth();
  
  if (!dir) {
    dout(7) << "handle_export_notify " << old_auth << " -> " << new_auth
	    << " on missing dir " << m->get_dirfrag() << dendl;
  } else if (dir->authority() != old_auth) {
    dout(7) << "handle_export_notify old_auth was " << dir->authority() 
	    << " != " << old_auth << " -> " << new_auth
	    << " on " << *dir << dendl;
  } else {
    dout(7) << "handle_export_notify " << old_auth << " -> " << new_auth
	    << " on " << *dir << dendl;
    // adjust auth
    set<CDir*> have;
    cache->map_dirfrag_set(m->get_bounds(), have);
    cache->adjust_bounded_subtree_auth(dir, have, new_auth);
    
    // induce a merge?
    cache->try_subtree_merge(dir);
  }
  
  // send ack
  if (m->wants_ack()) {
    mds->send_message_mds(new MExportDirNotifyAck(m->get_dirfrag()), from);
  } else {
    // aborted.  no ack.
    dout(7) << "handle_export_notify no ack requested" << dendl;
  }
  
  m->put();
}








/** cap exports **/



void Migrator::export_caps(CInode *in)
{
  int dest = in->authority().first;
  dout(7) << "export_caps to mds." << dest << " " << *in << dendl;

  assert(in->is_any_caps());
  assert(!in->is_auth());
  assert(!in->is_ambiguous_auth());
  assert(!in->state_test(CInode::STATE_EXPORTINGCAPS));

  MExportCaps *ex = new MExportCaps;
  ex->ino = in->ino();

  encode_export_inode_caps(in, ex->cap_bl, ex->client_map);

  mds->send_message_mds(ex, dest);
}

/* This function DOES put the passed message before returning*/
void Migrator::handle_export_caps_ack(MExportCapsAck *ack)
{
  CInode *in = cache->get_inode(ack->ino);
  assert(in);
  dout(10) << "handle_export_caps_ack " << *ack << " from " << ack->get_source() 
	   << " on " << *in
	   << dendl;
  
  finish_export_inode_caps(in);
  ack->put();
}


class C_M_LoggedImportCaps : public Context {
  Migrator *migrator;
  CInode *in;
  int from;
public:
  map<CInode*, map<client_t,Capability::Export> > cap_imports;
  map<client_t,entity_inst_t> client_map;
  map<client_t,uint64_t> sseqmap;

  C_M_LoggedImportCaps(Migrator *m, CInode *i, int f) : migrator(m), in(i), from(f) {}
  void finish(int r) {
    migrator->logged_import_caps(in, from, cap_imports, client_map, sseqmap);
  }  
};

/* This function DOES put the passed message before returning*/
void Migrator::handle_export_caps(MExportCaps *ex)
{
  dout(10) << "handle_export_caps " << *ex << " from " << ex->get_source() << dendl;
  CInode *in = cache->get_inode(ex->ino);
  
  assert(in);
  assert(in->is_auth());
  /*
   * note: i may be frozen, but i won't have been encoded for export (yet)!
   *  see export_go() vs export_go_synced().
   */

  C_M_LoggedImportCaps *finish = new C_M_LoggedImportCaps(this, in, ex->get_source().num());
  finish->client_map = ex->client_map;

  // decode new caps
  bufferlist::iterator blp = ex->cap_bl.begin();
  decode_import_inode_caps(in, blp, finish->cap_imports);
  assert(!finish->cap_imports.empty());   // thus, inode is pinned.

  // journal open client sessions
  version_t pv = mds->server->prepare_force_open_sessions(finish->client_map, finish->sseqmap);
  
  ESessions *le = new ESessions(pv, ex->client_map);
  mds->mdlog->start_entry(le);
  mds->mdlog->submit_entry(le);
  mds->mdlog->wait_for_safe(finish);
  mds->mdlog->flush();

  ex->put();
}


void Migrator::logged_import_caps(CInode *in, 
				  int from,
				  map<CInode*, map<client_t,Capability::Export> >& cap_imports,
				  map<client_t,entity_inst_t>& client_map,
				  map<client_t,uint64_t>& sseqmap) 
{
  dout(10) << "logged_import_caps on " << *in << dendl;

  // force open client sessions and finish cap import
  mds->server->finish_force_open_sessions(client_map, sseqmap);

  assert(cap_imports.count(in));
  finish_import_inode_caps(in, from, cap_imports[in]);  
  mds->locker->eval(in, CEPH_CAP_LOCKS, true);

  mds->send_message_mds(new MExportCapsAck(in->ino()), from);
}

