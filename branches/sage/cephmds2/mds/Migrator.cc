// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
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
#include "Migrator.h"

#include "MDBalancer.h"
#include "MDLog.h"
#include "MDSMap.h"

#include "include/filepath.h"

#include "events/EString.h"
#include "events/EExport.h"
#include "events/EImportStart.h"
#include "events/EImportFinish.h"

#include "msg/Messenger.h"

#include "messages/MClientFileCaps.h"

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

#include "messages/MHashDirDiscover.h"
#include "messages/MHashDirDiscoverAck.h"
#include "messages/MHashDirPrep.h"
#include "messages/MHashDirPrepAck.h"
#include "messages/MHashDir.h"
#include "messages/MHashDirNotify.h"
#include "messages/MHashDirAck.h"

#include "messages/MUnhashDirPrep.h"
#include "messages/MUnhashDirPrepAck.h"
#include "messages/MUnhashDir.h"
#include "messages/MUnhashDirAck.h"
#include "messages/MUnhashDirNotify.h"
#include "messages/MUnhashDirNotifyAck.h"


#include "config.h"
#undef dout
#define  dout(l)    if (l<=g_conf.debug || l <= g_conf.debug_mds) cout << g_clock.now() << " mds" << mds->get_nodeid() << ".migrator "



void Migrator::dispatch(Message *m)
{
  switch (m->get_type()) {
    // import
  case MSG_MDS_EXPORTDIRDISCOVER:
    handle_export_discover((MExportDirDiscover*)m);
    break;
  case MSG_MDS_EXPORTDIRPREP:
    handle_export_prep((MExportDirPrep*)m);
    break;
  case MSG_MDS_EXPORTDIR:
    handle_export_dir((MExportDir*)m);
    break;
  case MSG_MDS_EXPORTDIRFINISH:
    handle_export_finish((MExportDirFinish*)m);
    break;

    // export 
  case MSG_MDS_EXPORTDIRDISCOVERACK:
    handle_export_discover_ack((MExportDirDiscoverAck*)m);
    break;
  case MSG_MDS_EXPORTDIRPREPACK:
    handle_export_prep_ack((MExportDirPrepAck*)m);
    break;
  case MSG_MDS_EXPORTDIRACK:
    handle_export_ack((MExportDirAck*)m);
    break;
  case MSG_MDS_EXPORTDIRNOTIFYACK:
    handle_export_notify_ack((MExportDirNotifyAck*)m);
    break;    

    // export 3rd party (dir_auth adjustments)
  case MSG_MDS_EXPORTDIRNOTIFY:
    handle_export_notify((MExportDirNotify*)m);
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
  dout(7) << "export_empty_import " << *dir << endl;
  
  if (dir->inode->is_auth()) return;
  if (!dir->is_auth()) return;
  
  if (dir->inode->is_freezing() || dir->inode->is_frozen()) return;
  if (dir->is_freezing() || dir->is_frozen()) return;
  
  if (dir->get_size() > 0) {
    dout(7) << "not actually empty" << endl;
    return;
  }

  if (dir->inode->is_root()) {
    dout(7) << "root" << endl;
    return;
  }
  
  // is it really empty?
  if (!dir->is_complete()) {
    dout(7) << "not complete, fetching." << endl;
    dir->fetch(new C_MDC_EmptyImport(this,dir));
    return;
  }
  
  int dest = dir->inode->authority().first;
  
  // comment this out ot wreak havoc?
  //if (mds->is_shutting_down()) dest = 0;  // this is more efficient.
  
  dout(7) << "really empty, exporting to " << dest << endl;
  assert (dest != mds->get_nodeid());
  
  dout(-7) << "exporting to mds" << dest 
           << " empty import " << *dir << endl;
  export_dir( dir, dest );
}




// ==========================================================
// mds failure handling

void Migrator::handle_mds_failure(int who)
{
  dout(5) << "handle_mds_failure mds" << who << endl;

  // check my exports
  map<CDir*,int>::iterator p = export_state.begin();
  while (p != export_state.end()) {
    map<CDir*,int>::iterator next = p;
    next++;
    CDir *dir = p->first;
    
    // abort exports:
    //  - that are going to the failed node
    //  - that aren't frozen yet (to about auth_pin deadlock)
    if (export_peer[dir] == who ||
	p->second == EXPORT_DISCOVERING || p->second == EXPORT_FREEZING) { 
      // the guy i'm exporting to failed, or we're just freezing.
      dout(10) << "cleaning up export state " << p->second << " of " << *dir << endl;
      
      switch (p->second) {
      case EXPORT_DISCOVERING:
	dout(10) << "export state=discovering : canceling freeze and removing auth_pin" << endl;
	dir->unfreeze_tree();  // cancel the freeze
	dir->auth_unpin();
	export_state.erase(dir); // clean up
	dir->state_clear(CDir::STATE_EXPORTING);
	if (export_peer[dir] != who) // tell them.
	  mds->send_message_mds(new MExportDirCancel(dir->dirfrag()), who, MDS_PORT_MIGRATOR);
	break;
	
      case EXPORT_FREEZING:
	dout(10) << "export state=freezing : canceling freeze" << endl;
	dir->unfreeze_tree();  // cancel the freeze
	export_state.erase(dir); // clean up
	dir->state_clear(CDir::STATE_EXPORTING);
	if (export_peer[dir] != who) // tell them.
	  mds->send_message_mds(new MExportDirCancel(dir->dirfrag()), who, MDS_PORT_MIGRATOR);
	break;

	// NOTE: state order reversal, warning comes after loggingstart+prepping
      case EXPORT_WARNING:
	dout(10) << "export state=warning : unpinning bounds, unfreezing, notifying" << endl;
	// fall-thru

	//case EXPORT_LOGGINGSTART:
      case EXPORT_PREPPING:
	if (p->second != EXPORT_WARNING) 
	  dout(10) << "export state=loggingstart|prepping : unpinning bounds, unfreezing" << endl;
	// unpin bounds
	for (set<CDir*>::iterator p = export_bounds[dir].begin();
	     p != export_bounds[dir].end();
	     ++p) {
	  CDir *bd = *p;
	  bd->put(CDir::PIN_EXPORTBOUND);
	  bd->state_clear(CDir::STATE_EXPORTBOUND);
	}
	dir->unfreeze_tree();
	cache->adjust_subtree_auth(dir, mds->get_nodeid());
	cache->try_subtree_merge(dir);
	export_state.erase(dir); // clean up
	dir->state_clear(CDir::STATE_EXPORTING);
	break;
	
      case EXPORT_EXPORTING:
	dout(10) << "export state=exporting : reversing, and unfreezing" << endl;
	export_reverse(dir);
	export_state.erase(dir); // clean up
	dir->state_clear(CDir::STATE_EXPORTING);
	break;

      case EXPORT_LOGGINGFINISH:
      case EXPORT_NOTIFYING:
	dout(10) << "export state=loggingfinish|notifying : ignoring dest failure, we were successful." << endl;
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
	
	// unpin the path
	vector<CDentry*> trace;
	cache->make_trace(trace, dir->inode);
	mds->locker->dentry_anon_rdlock_trace_finish(trace);
	
	// wake up any waiters
	mds->queue_finished(export_finish_waiters[dir]);
	export_finish_waiters.erase(dir);
	
	// send pending import_maps?  (these need to go out when all exports have finished.)
	cache->send_pending_import_maps();
	
	cache->show_subtrees();
      }
    } else {
      // bystander failed.
      if (p->second == EXPORT_WARNING) {
	// exporter waiting for warning acks, let's fake theirs.
	if (export_warning_ack_waiting[dir].count(who)) {
	  dout(10) << "faking export_warning_ack from mds" << who
		   << " on " << *dir << " to mds" << export_peer[dir] 
		   << endl;
	  export_warning_ack_waiting[dir].erase(who);
	  export_notify_ack_waiting[dir].erase(who);   // they won't get a notify either.
	  if (export_warning_ack_waiting[dir].empty()) 
	    export_go(dir);
	}
      }
      if (p->second == EXPORT_NOTIFYING) {
	// exporter is waiting for notify acks, fake it
	if (export_notify_ack_waiting[dir].count(who)) {
	  dout(10) << "faking export_notify_ack from mds" << who
		   << " on " << *dir << " to mds" << export_peer[dir] 
		   << endl;
	  export_notify_ack_waiting[dir].erase(who);
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
    next++;
    dirfrag_t df = q->first;
    CInode *diri = mds->mdcache->get_inode(df.ino);
    CDir *dir = mds->mdcache->get_dirfrag(df);

    if (import_peer[df] == who) {
      switch (import_state[df]) {
      case IMPORT_DISCOVERING:
	dout(10) << "import state=discovering : clearing state" << endl;
	import_state.erase(df);
	import_peer.erase(df);
	break;

      case IMPORT_DISCOVERED:
	dout(10) << "import state=discovered : unpinning inode " << *diri << endl;
	assert(diri);
	// unpin base
	diri->put(CInode::PIN_IMPORTING);
	import_state.erase(df);
	import_peer.erase(df);
	break;

      case IMPORT_PREPPING:
	if (import_state[df] == IMPORT_PREPPING) {
	  dout(10) << "import state=prepping : unpinning base+bounds " << *dir << endl;
	}
	assert(dir);
	import_reverse_unpin(dir);    // unpin
	break;

      case IMPORT_PREPPED:
	dout(10) << "import state=prepping : unpinning base+bounds, unfreezing " << *dir << endl;
	assert(dir);
	
	// adjust auth back to me
	cache->adjust_subtree_auth(dir, import_peer[df]);
	cache->try_subtree_merge(dir);
	
	// bystanders?
	if (import_bystanders[dir].empty()) {
	  import_reverse_unfreeze(dir);
	} else {
	  // notify them; wait in aborting state
	  import_notify_abort(dir);
	  import_state[df] = IMPORT_ABORTING;
	}
	break;

      case IMPORT_LOGGINGSTART:
	dout(10) << "import state=loggingstart : reversing import on " << *dir << endl;
	import_reverse(dir);
	break;

      case IMPORT_ACKING:
	// hrm.  make this an ambiguous import, and wait for exporter recovery to disambiguate
	dout(10) << "import state=acking : noting ambiguous import " << *dir << endl;
	cache->add_ambiguous_import(dir, import_bounds[dir]);
	break;

      case IMPORT_ABORTING:
	dout(10) << "import state=aborting : ignoring repeat failure " << *dir << endl;
	break;
      }
    }

    // next!
    q = next;
  }
}





void Migrator::audit()
{
  if (g_conf.debug_mds < 5) return;  // hrm.

  // import_state
  for (map<dirfrag_t,int>::iterator p = import_state.begin();
       p != import_state.end();
       p++) {
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
    assert(dir->is_ambiguous_dir_auth());
    assert(dir->authority().first  == mds->get_nodeid() ||
	   dir->authority().second == mds->get_nodeid());
  }

  // export_state
  for (map<CDir*,int>::iterator p = export_state.begin();
       p != export_state.end();
       p++) {
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


/** export_dir(dir, dest)
 * public method to initiate an export.
 * will fail if the directory is freezing, frozen, unpinnable, or root. 
 */
void Migrator::export_dir(CDir *dir, int dest)
{
  dout(7) << "export_dir " << *dir << " to " << dest << endl;
  assert(dir->is_auth());
  assert(dest != mds->get_nodeid());
   
  if (mds->mdsmap->is_degraded()) {
    dout(7) << "cluster degraded, no exports for now" << endl;
    return;
  }

  if (dir->inode->is_root()) {
    dout(7) << "i won't export root" << endl;
    //assert(0);
    return;
  }

  if (dir->is_frozen() ||
      dir->is_freezing()) {
    dout(7) << " can't export, freezing|frozen.  wait for other exports to finish first." << endl;
    return;
  }
  if (dir->state_test(CDir::STATE_EXPORTING)) {
    dout(7) << "already exporting" << endl;
    return;
  }
  
  // pin path?
  vector<CDentry*> trace;
  cache->make_trace(trace, dir->inode);
  if (!mds->locker->dentry_can_rdlock_trace(trace, 0)) {
    dout(7) << "export_dir couldn't pin path, failing." << endl;
    return;
  }

  // ok.
  mds->locker->dentry_anon_rdlock_trace_start(trace);
  assert(export_state.count(dir) == 0);
  export_state[dir] = EXPORT_DISCOVERING;
  export_peer[dir] = dest;

  dir->state_set(CDir::STATE_EXPORTING);

  // send ExportDirDiscover (ask target)
  mds->send_message_mds(new MExportDirDiscover(dir), export_peer[dir], MDS_PORT_MIGRATOR);

  // start the freeze, but hold it up with an auth_pin.
  dir->auth_pin();
  dir->freeze_tree(new C_MDC_ExportFreeze(this, dir));
}


/*
 * called on receipt of MExportDirDiscoverAck
 * the importer now has the directory's _inode_ in memory, and pinned.
 */
void Migrator::handle_export_discover_ack(MExportDirDiscoverAck *m)
{
  CDir *dir = cache->get_dirfrag(m->get_dirfrag());
  assert(dir);
  
  dout(7) << "export_discover_ack from " << m->get_source()
	  << " on " << *dir << endl;

  if (export_state.count(dir) == 0 ||
      export_state[dir] != EXPORT_DISCOVERING ||
      export_peer[dir] != m->get_source().num()) {
    dout(7) << "must have aborted" << endl;
  } else {
    // freeze the subtree
    export_state[dir] = EXPORT_FREEZING;
    dir->auth_unpin();
  }
  
  delete m;  // done
}

void Migrator::export_frozen(CDir *dir)
{
  dout(7) << "export_frozen on " << *dir << endl;
  assert(dir->is_frozen());
  int dest = export_peer[dir];

  // ok!
  cache->show_subtrees();

  // note the bounds.
  //  force it into a subtree by listing auth as <me,me>.
  cache->adjust_subtree_auth(dir, mds->get_nodeid(), mds->get_nodeid());
  cache->get_subtree_bounds(dir, export_bounds[dir]);
  set<CDir*> &bounds = export_bounds[dir];

  // generate prep message, log entry.
  MExportDirPrep *prep = new MExportDirPrep(dir->dirfrag());

  // include list of bystanders
  for (map<int,int>::iterator p = dir->replicas_begin();
       p != dir->replicas_end();
       p++) {
    if (p->first != dest) {
      dout(10) << "bystander mds" << p->first << endl;
      prep->add_bystander(p->first);
    }
  }

  // include spanning tree for all nested exports.
  // these need to be on the destination _before_ the final export so that
  // dir_auth updates on any nested exports are properly absorbed.
  set<inodeno_t> inodes_added;

  // include base dir
  prep->add_dir( new CDirDiscover(dir, dir->add_replica(dest)) );
  
  // check bounds
  for (set<CDir*>::iterator it = bounds.begin();
       it != bounds.end();
       it++) {
    CDir *bound = *it;

    // pin it.
    bound->get(CDir::PIN_EXPORTBOUND);
    bound->state_set(CDir::STATE_EXPORTBOUND);
    
    dout(7) << "  export bound " << *bound << endl;

    prep->add_export( bound->dirfrag() );

    /* first assemble each trace, in trace order, and put in message */
    list<CInode*> inode_trace;  

    // trace to dir
    CDir *cur = bound;
    while (cur != dir) {
      // don't repeat ourselves
      if (inodes_added.count(cur->ino())) break;   // did already!
      inodes_added.insert(cur->ino());
      
      CDir *parent_dir = cur->get_parent_dir();

      // inode?
      assert(cur->inode->is_auth());
      inode_trace.push_front(cur->inode);
      dout(7) << "  will add " << *cur->inode << endl;
      
      // include dir?
      // note: don't replicate ambiguous auth items!  they're
      //    frozen anyway.
      if (cur->is_auth() && !cur->is_ambiguous_auth()) {
        prep->add_dir( new CDirDiscover(cur, cur->add_replica(dest)) );  // yay!
        dout(7) << "  added " << *cur << endl;
      }
      
      cur = parent_dir;      
    }

    for (list<CInode*>::iterator it = inode_trace.begin();
         it != inode_trace.end();
         it++) {
      CInode *in = *it;
      dout(7) << "  added " << *in << endl;
      prep->add_inode( in->parent->get_dir()->dirfrag(),
                       in->parent->get_name(),
                       in->replicate_to(dest) );
    }

  }

  // send.
  export_state[dir] = EXPORT_PREPPING;
  mds->send_message_mds(prep, dest, MDS_PORT_MIGRATOR);
}

void Migrator::handle_export_prep_ack(MExportDirPrepAck *m)
{
  CDir *dir = cache->get_dirfrag(m->get_dirfrag());
  assert(dir);

  dout(7) << "export_prep_ack " << *dir << endl;

  if (export_state.count(dir) == 0 ||
      export_state[dir] != EXPORT_PREPPING) {
    // export must have aborted.  
    dout(7) << "export must have aborted" << endl;
    delete m;
    return;
  }

  // send warnings
  assert(export_peer.count(dir));
  int dest = export_peer[dir];
  assert(export_warning_ack_waiting.count(dir) == 0);
  assert(export_notify_ack_waiting.count(dir) == 0);
  for (map<int,int>::iterator p = dir->replicas_begin();
       p != dir->replicas_end();
       ++p) {
    if (p->first == dest) continue;
    if (!mds->mdsmap->is_active_or_stopping(p->first))
      continue;  // only if active
    export_warning_ack_waiting[dir].insert(p->first);
    export_notify_ack_waiting[dir].insert(p->first);  // we'll eventually get a notifyack, too!

    MExportDirNotify *notify = new MExportDirNotify(dir->dirfrag(), true,
						    pair<int,int>(mds->get_nodeid(),CDIR_AUTH_UNKNOWN),
						    pair<int,int>(mds->get_nodeid(),export_peer[dir]));
    notify->copy_bounds(export_bounds[dir]);
    mds->send_message_mds(notify, p->first, MDS_PORT_MIGRATOR);
    
  }
  export_state[dir] = EXPORT_WARNING;

  // nobody to warn?
  if (export_warning_ack_waiting.count(dir) == 0) 
    export_go(dir);  // start export.
    
  // done.
  delete m;
}


void Migrator::export_go(CDir *dir)
{  
  assert(export_peer.count(dir));
  int dest = export_peer[dir];
  dout(7) << "export_go " << *dir << " to " << dest << endl;

  cache->show_subtrees();
  
  export_warning_ack_waiting.erase(dir);
  export_state[dir] = EXPORT_EXPORTING;

  assert(export_bounds.count(dir) == 1);
  assert(export_data.count(dir) == 0);

  assert(dir->get_cum_auth_pins() == 0);

  // set ambiguous auth
  cache->adjust_subtree_auth(dir, dest, mds->get_nodeid());
  cache->verify_subtree_bounds(dir, export_bounds[dir]);
  
  // fill export message with cache data
  C_Contexts *fin = new C_Contexts;       // collect all the waiters
  int num_exported_inodes = encode_export_dir( export_data[dir], 
					       fin, 
					       dir,   // base
					       dir,   // recur start point
					       dest );
  
  // send the export data!
  MExportDir *req = new MExportDir(dir->dirfrag());

  // export state
  req->set_dirstate( export_data[dir] );

  // add bounds to message
  for (set<CDir*>::iterator p = export_bounds[dir].begin();
       p != export_bounds[dir].end();
       ++p)
    req->add_export((*p)->dirfrag());

  //s end
  mds->send_message_mds(req, dest, MDS_PORT_MIGRATOR);

  // queue up the finisher
  dir->add_waiter( CDir::WAIT_UNFREEZE, fin );

  // take away the popularity we're sending.   FIXME: do this later?
  mds->balancer->subtract_export(dir);

  // stats
  if (mds->logger) mds->logger->inc("ex");
  if (mds->logger) mds->logger->inc("iex", num_exported_inodes);

  cache->show_subtrees();
}


/** encode_export_inode
 * update our local state for this inode to export.
 * encode relevant state to be sent over the wire.
 * used by: encode_export_dir, file_rename (if foreign)
 */
void Migrator::encode_export_inode(CInode *in, bufferlist& enc_state, int new_auth)
{
  // tell (all) clients about migrating caps.. mark STALE
  for (map<int, Capability>::iterator it = in->client_caps.begin();
       it != in->client_caps.end();
       it++) {
    dout(7) << "encode_export_inode " << *in << " telling client" << it->first << " stale caps" << endl;
    MClientFileCaps *m = new MClientFileCaps(in->inode, 
                                             it->second.get_last_seq(), 
                                             it->second.pending(),
                                             it->second.wanted(),
                                             MClientFileCaps::FILECAP_STALE);
    mds->messenger->send_message(m, mds->clientmap.get_inst(it->first),
				 0, MDS_PORT_CACHE);
  }

  // relax locks?
  if (!in->is_replicated())
    in->replicate_relax_locks();

  // add inode
  assert(!in->is_replica(mds->get_nodeid()));
  CInodeExport istate( in );
  istate._encode( enc_state );

  // we're export this inode; fix inode state
  dout(7) << "encode_export_inode " << *in << endl;
  
  if (in->is_dirty()) in->mark_clean();
  
  // clear/unpin cached_by (we're no longer the authority)
  in->clear_replicas();
  
  // twiddle lock states for auth -> replica transition
  // auth
  in->authlock.clear_gather();
  if (in->authlock.get_state() == LOCK_GLOCKR)
    in->authlock.set_state(LOCK_LOCK);

  // link
  in->linklock.clear_gather();
  if (in->linklock.get_state() == LOCK_GLOCKR)
    in->linklock.set_state(LOCK_LOCK);

  // dirfragtree
  in->dirfragtreelock.clear_gather();
  if (in->dirfragtreelock.get_state() == LOCK_GLOCKR)
    in->dirfragtreelock.set_state(LOCK_LOCK);

  // file : we lost all our caps, so move to stable state!
  in->filelock.clear_gather();
  if (in->filelock.get_state() == LOCK_GLOCKR ||
      in->filelock.get_state() == LOCK_GLOCKM ||
      in->filelock.get_state() == LOCK_GLOCKL ||
      in->filelock.get_state() == LOCK_GLONERR ||
      in->filelock.get_state() == LOCK_GLONERM ||
      in->filelock.get_state() == LOCK_LONER)
    in->filelock.set_state(LOCK_LOCK);
  if (in->filelock.get_state() == LOCK_GMIXEDR)
    in->filelock.set_state(LOCK_MIXED);
  // this looks like a step backwards, but it's what we want!
  if (in->filelock.get_state() == LOCK_GSYNCM)
    in->filelock.set_state(LOCK_MIXED);
  if (in->filelock.get_state() == LOCK_GSYNCL)
    in->filelock.set_state(LOCK_LOCK);
  if (in->filelock.get_state() == LOCK_GMIXEDL)
    in->filelock.set_state(LOCK_LOCK);
    //in->filelock.set_state(LOCK_MIXED);
  
  // mark auth
  assert(in->is_auth());
  in->state_clear(CInode::STATE_AUTH);
  in->replica_nonce = CInode::EXPORT_NONCE;
  
  // *** other state too?

  // move to end of LRU so we drop out of cache quickly!
  if (in->get_parent_dn()) 
    cache->lru.lru_bottouch(in->get_parent_dn());
}


int Migrator::encode_export_dir(list<bufferlist>& dirstatelist,
			      C_Contexts *fin,
			      CDir *basedir,
			      CDir *dir,
			      int newauth)
{
  int num_exported = 0;

  dout(7) << "encode_export_dir " << *dir << " " << dir->nitems << " items" << endl;
  
  assert(dir->get_projected_version() == dir->get_version());

  // dir 
  bufferlist enc_dir;
  
  CDirExport dstate(dir);
  dstate._encode( enc_dir );
  
  // release open_by 
  dir->clear_replicas();

  // mark
  assert(dir->is_auth());
  dir->state_clear(CDir::STATE_AUTH);
  dir->replica_nonce = CDir::NONCE_EXPORT;

  list<CDir*> subdirs;

  if (dir->is_dirty())
    dir->mark_clean();
  
  // discard most dir state
  dir->state &= CDir::MASK_STATE_EXPORT_KEPT;  // i only retain a few things.
  
  // suck up all waiters
  list<Context*> waiting;
  dir->take_waiting(CDir::WAIT_ANY, waiting);    // all dir waiters
  fin->take(waiting);
  
  // dentries
  CDir_map_t::iterator it;
  for (it = dir->begin(); it != dir->end(); it++) {
    CDentry *dn = it->second;
    CInode *in = dn->get_inode();
    
    num_exported++;
    
    // -- dentry
    dout(7) << "encode_export_dir exporting " << *dn << endl;
    
    // name
    _encode(it->first, enc_dir);
    
    // state
    it->second->encode_export_state(enc_dir);
    
    // points to...
    
    // null dentry?
    if (dn->is_null()) {
      enc_dir.append("N", 1);  // null dentry
      continue;
    }
    
    if (dn->is_remote()) {
      // remote link
      enc_dir.append("L", 1);  // remote link
      
      inodeno_t ino = dn->get_remote_ino();
      enc_dir.append((char*)&ino, sizeof(ino));
      continue;
    }
    
    // primary link
    // -- inode
    enc_dir.append("I", 1);    // inode dentry
    
    encode_export_inode(in, enc_dir, newauth);  // encode, and (update state for) export
    
    // directory?
    list<CDir*> dfs;
    in->get_dirfrags(dfs);
    for (list<CDir*>::iterator p = dfs.begin(); p != dfs.end(); ++p) {
      CDir *dir = *p;
      if (!dir->state_test(CDir::STATE_EXPORTBOUND)) {
	// include nested dirfrag
	assert(dir->get_dir_auth().first == CDIR_AUTH_PARENT);
	subdirs.push_back(dir);  // it's ours, recurse (later)
      }
    }
    
    // waiters
    list<Context*> waiters;
    in->take_waiting(CInode::WAIT_ANY, waiters);
    fin->take(waiters);
  }

  // add to dirstatelist
  bufferlist bl;
  dirstatelist.push_back( bl );
  dirstatelist.back().claim( enc_dir );

  // subdirs
  for (list<CDir*>::iterator it = subdirs.begin(); it != subdirs.end(); it++)
    num_exported += encode_export_dir(dirstatelist, fin, basedir, *it, newauth);

  return num_exported;
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
 */
void Migrator::handle_export_ack(MExportDirAck *m)
{
  CDir *dir = cache->get_dirfrag(m->get_dirfrag());
  assert(dir);
  assert(dir->is_frozen_tree_root());  // i'm exporting!

  // yay!
  dout(7) << "handle_export_ack " << *dir << endl;

  export_warning_ack_waiting.erase(dir);
  
  export_state[dir] = EXPORT_LOGGINGFINISH;
  export_data.erase(dir);
  
  // log completion
  EExport *le = new EExport(dir);
  le->metablob.add_dir( dir, false );
  for (set<CDir*>::iterator p = export_bounds[dir].begin();
       p != export_bounds[dir].end();
       ++p) {
    CDir *bound = *p;
    le->get_bounds().insert(bound->dirfrag());
    le->metablob.add_dir_context(bound);
    le->metablob.add_dir(bound, false);
  }

  // log export completion, then finish (unfreeze, trigger finish context, etc.)
  dir->get(CDir::PIN_LOGGINGEXPORTFINISH);
  mds->mdlog->submit_entry(le,
			   new C_MDS_ExportFinishLogged(this, dir));
  
  delete m;
}



/*
 * this happens if hte dest failes after i send teh export data but before it is acked
 * that is, we don't know they safely received and logged it, so we reverse our changes
 * and go on.
 */
void Migrator::export_reverse(CDir *dir)
{
  dout(7) << "export_reverse " << *dir << endl;
  
  assert(export_state[dir] == EXPORT_EXPORTING);
  assert(export_bounds.count(dir));
  assert(export_data.count(dir));
  
  // adjust auth, with possible subtree merge.
  cache->verify_subtree_bounds(dir, export_bounds[dir]);
  cache->adjust_subtree_auth(dir, mds->get_nodeid());
  cache->try_subtree_merge(dir);

  // unpin bounds
  for (set<CDir*>::iterator p = export_bounds[dir].begin();
       p != export_bounds[dir].end();
       ++p) {
    CDir *bd = *p;
    bd->put(CDir::PIN_EXPORTBOUND);
    bd->state_clear(CDir::STATE_EXPORTBOUND);
  }

  // re-import the metadata
  int num_imported_inodes = 0;

  for (list<bufferlist>::iterator p = export_data[dir].begin();
       p != export_data[dir].end();
       ++p) {
    num_imported_inodes += 
      decode_import_dir(*p, 
                       export_peer[dir], 
                       dir,                 // import root
		       0);
  }

  // process delayed expires
  cache->process_delayed_expire(dir);
  
  // unfreeze
  dir->unfreeze_tree();

  // some clean up
  export_data.erase(dir);
  export_bounds.erase(dir);
  export_warning_ack_waiting.erase(dir);
  export_notify_ack_waiting.erase(dir);

  cache->show_cache();
}


/*
 * once i get the ack, and logged the EExportFinish(true),
 * send notifies (if any), otherwise go straight to finish.
 * 
 */
void Migrator::export_logged_finish(CDir *dir)
{
  dout(7) << "export_logged_finish " << *dir << endl;
  dir->put(CDir::PIN_LOGGINGEXPORTFINISH);

  cache->verify_subtree_bounds(dir, export_bounds[dir]);

  // send notifies
  int dest = export_peer[dir];

  for (set<int>::iterator p = export_notify_ack_waiting[dir].begin();
       p != export_notify_ack_waiting[dir].end();
       ++p) {
    MExportDirNotify *notify;
    if (mds->mdsmap->is_active_or_stopping(export_peer[dir])) 
      // dest is still alive.
      notify = new MExportDirNotify(dir->dirfrag(), true,
				    pair<int,int>(mds->get_nodeid(), dest),
				    pair<int,int>(dest, CDIR_AUTH_UNKNOWN));
    else 
      // dest is dead.  bystanders will think i am only auth, as per mdcache->handle_mds_failure()
      notify = new MExportDirNotify(dir->dirfrag(), true,
				    pair<int,int>(mds->get_nodeid(), CDIR_AUTH_UNKNOWN),
				    pair<int,int>(dest, CDIR_AUTH_UNKNOWN));

    notify->copy_bounds(export_bounds[dir]);
    
    mds->send_message_mds(notify, *p, MDS_PORT_MIGRATOR);
  }

  // wait for notifyacks
  export_state[dir] = EXPORT_NOTIFYING;
  
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
	    << *dir << endl;
    assert(export_warning_ack_waiting.count(dir));
    export_warning_ack_waiting[dir].erase(from);
    
    if (export_warning_ack_waiting[dir].empty()) 
      export_go(dir);     // start export.
  } 
  else if (export_state.count(dir) && export_state[dir] == EXPORT_NOTIFYING) {
    // exporting. process notify.
    dout(7) << "handle_export_notify_ack from " << m->get_source()
	    << ": exporting, processing notify on "
	    << *dir << endl;
    assert(export_notify_ack_waiting.count(dir));
    export_notify_ack_waiting[dir].erase(from);
    
    if (export_notify_ack_waiting[dir].empty())
      export_finish(dir);
  }
  else if (import_state.count(dir->dirfrag()) && import_state[dir->dirfrag()] == IMPORT_ABORTING) {
    // reversing import
    dout(7) << "handle_export_notify_ack from " << m->get_source()
	    << ": aborting import on "
	    << *dir << endl;
    assert(import_bystanders[dir].count(from));
    import_bystanders[dir].erase(from);
    if (import_bystanders[dir].empty()) {
      import_bystanders.erase(dir);
      import_reverse_unfreeze(dir);
    }
  }

  delete m;
}


void Migrator::export_finish(CDir *dir)
{
  dout(7) << "export_finish " << *dir << endl;

  if (export_state.count(dir) == 0) {
    dout(7) << "target must have failed, not sending final commit message.  export succeeded anyway." << endl;
    return;
  }

  // send finish/commit to new auth
  if (mds->mdsmap->is_active_or_stopping(export_peer[dir])) {
    mds->send_message_mds(new MExportDirFinish(dir->dirfrag()), 
			  export_peer[dir], MDS_PORT_MIGRATOR);
  } else {
    dout(7) << "not sending MExportDirFinish, dest has failed" << endl;
  }
  
  // unfreeze
  dout(7) << "export_finish unfreezing" << endl;
  dir->unfreeze_tree();
  
  // unpin bounds
  for (set<CDir*>::iterator p = export_bounds[dir].begin();
       p != export_bounds[dir].end();
       ++p) {
    CDir *bd = *p;
    bd->put(CDir::PIN_EXPORTBOUND);
    bd->state_clear(CDir::STATE_EXPORTBOUND);
  }

  // adjust auth, with possible subtree merge.
  //  (we do this _after_ removing EXPORTBOUND pins, to allow merges)
  cache->adjust_subtree_auth(dir, export_peer[dir]);
  cache->try_subtree_merge(dir);
  
  // unpin path
  dout(7) << "export_finish unpinning path" << endl;
  vector<CDentry*> trace;
  cache->make_trace(trace, dir->inode);
  mds->locker->dentry_anon_rdlock_trace_finish(trace);

  // discard delayed expires
  cache->discard_delayed_expire(dir);

  // remove from exporting list, clean up state
  dir->state_clear(CDir::STATE_EXPORTING);
  export_state.erase(dir);
  export_peer.erase(dir);
  export_bounds.erase(dir);
  export_notify_ack_waiting.erase(dir);

  // queue finishers
  mds->queue_finished(export_finish_waiters[dir]);
  export_finish_waiters.erase(dir);

  // stats
  //if (mds->logger) mds->logger->set("nex", cache->exports.size());

  cache->show_subtrees();
  audit();

  // send pending import_maps?
  mds->mdcache->send_pending_import_maps();
}








// ==========================================================
// IMPORT

void Migrator::handle_export_discover(MExportDirDiscover *m)
{
  assert(m->get_source().num() != mds->get_nodeid());

  dout(7) << "handle_export_discover on " << m->get_path() << endl;

  // note import state
  dirfrag_t df = m->get_dirfrag();
  
  // only start discovering on this message once.
  if (!m->started) {
    m->started = true;
    import_state[df] = IMPORT_DISCOVERING;
    import_peer[df] = m->get_source().num();
  }

  // am i retrying after ancient path_traverse results?
  if (import_state.count(df) == 0 &&
      import_state[df] != IMPORT_DISCOVERING) {
    dout(7) << "hmm import_state is off, i must be obsolete lookup" << endl;
    delete m;
    return;
  }

  // do we have it?
  CInode *in = cache->get_inode(m->get_dirfrag().ino);
  if (!in) {
    // must discover it!
    filepath fpath(m->get_path());
    vector<CDentry*> trace;
    int r = cache->path_traverse(0, 
				 0,
				 fpath, trace, true,
				 m, new C_MDS_RetryMessage(mds, m),       // on delay/retry
				 MDS_TRAVERSE_DISCOVER);
    if (r > 0) return; // wait
    if (r < 0) {
      dout(7) << "handle_export_discover_2 failed to discover or not dir " << m->get_path() << ", NAK" << endl;
      assert(0);    // this shouldn't happen if the auth pins his path properly!!!! 
    }
    
    CInode *in;
    if (trace.empty()) {
      in = cache->get_root();
      if (!in) {
	cache->open_root(new C_MDS_RetryMessage(mds, m));
	return;
      }
    } else {
      in = trace[trace.size()-1]->inode;
    }
  }

  // yay
  import_discovered(in, df);
  delete m;
}

void Migrator::import_discovered(CInode *in, dirfrag_t df)
{
  dout(7) << "import_discovered " << df << " inode " << *in << endl;
  
  // pin inode in the cache (for now)
  assert(in->is_dir());
  in->get(CInode::PIN_IMPORTING);
  
  // reply
  dout(7) << " sending export_discover_ack on " << *in << endl;
  mds->send_message_mds(new MExportDirDiscoverAck(df),
			import_peer[df], MDS_PORT_MIGRATOR);
}

void Migrator::handle_export_cancel(MExportDirCancel *m)
{
  dout(7) << "handle_export_cancel on " << m->get_dirfrag() << endl;

  if (import_state[m->get_dirfrag()] == IMPORT_DISCOVERED) {
    CInode *in = cache->get_inode(m->get_dirfrag().ino);
    assert(in);
    in->put(CInode::PIN_IMPORTING);
  } else {
    assert(import_state[m->get_dirfrag()] == IMPORT_DISCOVERING);
  }

  import_state.erase(m->get_dirfrag());
  import_peer.erase(m->get_dirfrag());

  delete m;
}


void Migrator::handle_export_prep(MExportDirPrep *m)
{
  CInode *diri = cache->get_inode(m->get_dirfrag().ino);
  assert(diri);

  int oldauth = m->get_source().num();
  assert(oldauth != mds->get_nodeid());

  list<Context*> finished;

  // assimilate root dir.
  CDir *dir;

  if (!m->did_assim()) {
    dir = cache->add_replica_dir(diri, 
				 m->get_dirfrag().frag, *m->get_dirfrag_discover(m->get_dirfrag()), 
				 oldauth, finished);
    dout(7) << "handle_export_prep on " << *dir << " (first pass)" << endl;
  } else {
    dir = cache->get_dirfrag(m->get_dirfrag());
    assert(dir);
    dout(7) << "handle_export_prep on " << *dir << " (subsequent pass)" << endl;
  }
  assert(dir->is_auth() == false);
  
  cache->show_subtrees();

  // assimilate contents?
  if (!m->did_assim()) {
    dout(7) << "doing assim on " << *dir << endl;
    m->mark_assim();  // only do this the first time!

    // move pin to dir
    diri->put(CInode::PIN_IMPORTING);
    dir->get(CDir::PIN_IMPORTING);  
    dir->state_set(CDir::STATE_IMPORTING);

    // change import state
    import_state[dir->dirfrag()] = IMPORT_PREPPING;
    
    // bystander list
    import_bystanders[dir] = m->get_bystanders();
    dout(7) << "bystanders are " << import_bystanders[dir] << endl;

    // assimilate traces to exports
    for (list<CInodeDiscover*>::iterator it = m->get_inodes().begin();
         it != m->get_inodes().end();
         it++) {
      // inode
      CInode *in = cache->get_inode( (*it)->get_ino() );
      if (in) {
        (*it)->update_inode(in);
        dout(7) << " updated " << *in << endl;
      } else {
        in = new CInode(mds->mdcache, false);
        (*it)->update_inode(in);
        
        // link to the containing dir
	CDir *condir = cache->get_dirfrag( m->get_containing_dirfrag(in->ino()) );
	assert(condir);
	cache->add_inode( in );
        condir->add_dentry( m->get_dentry(in->ino()), in );
        
        dout(7) << "   added " << *in << endl;
      }
      
      assert( in->get_parent_dir()->dirfrag() == m->get_containing_dirfrag(in->ino()) );
      
      // dirs
      for (list<frag_t>::iterator pf = m->get_inode_dirfrags(in->ino()).begin();
	   pf != m->get_inode_dirfrags(in->ino()).end();
	   ++pf) {
	// add/update
	cache->add_replica_dir(in, *pf, *m->get_dirfrag_discover(dirfrag_t(in->ino(), *pf)),
			       oldauth, finished);
      }
    }

    // open export dirs/bounds?
    assert(import_bound_inos.count(dir->dirfrag()) == 0);
    import_bound_inos[dir->dirfrag()].clear();
    for (list<dirfrag_t>::iterator it = m->get_bounds().begin();
         it != m->get_bounds().end();
         it++) {
      dout(7) << "  checking bound " << hex << *it << dec << endl;
      CInode *in = cache->get_inode(it->ino);
      assert(in);
      
      // note bound.
      import_bound_inos[dir->dirfrag()].push_back(*it);

      CDir *dir = cache->get_dirfrag(*it);
      if (!dir) {
        dout(7) << "  opening nested export on " << *in << endl;
        cache->open_remote_dir(in, it->frag,
			       new C_MDS_RetryMessage(mds, m));
      }
    }
  } else {
    dout(7) << " not doing assim on " << *dir << endl;
  }
  

  // verify we have all bounds
  int waiting_for = 0;
  for (list<dirfrag_t>::iterator it = m->get_bounds().begin();
       it != m->get_bounds().end();
       it++) {
    dirfrag_t df = *it;
    CDir *bound = cache->get_dirfrag(df);
    if (bound) {
      if (!bound->state_test(CDir::STATE_IMPORTBOUND)) {
        dout(7) << "  pinning import bound " << *bound << endl;
        bound->get(CDir::PIN_IMPORTBOUND);
        bound->state_set(CDir::STATE_IMPORTBOUND);
	import_bounds[dir].insert(bound);
      } else {
        dout(7) << "  already pinned import bound " << *bound << endl;
      }
    } else {
      dout(7) << "  waiting for nested export dir on " << *cache->get_inode(df.ino) << endl;
      waiting_for++;
    }
  }

  if (waiting_for) {
    dout(7) << " waiting for " << waiting_for << " nested export dir opens" << endl;
  } else {
    dout(7) << " all ready, noting auth and freezing import region" << endl;

    // note that i am an ambiguous auth for this subtree.
    // specify bounds, since the exporter explicitly defines the region.
    cache->adjust_bounded_subtree_auth(dir, import_bounds[dir], 
				       pair<int,int>(oldauth, mds->get_nodeid()));
    cache->verify_subtree_bounds(dir, import_bounds[dir]);
    
    // freeze.
    dir->_freeze_tree();
    
    // ok!
    dout(7) << " sending export_prep_ack on " << *dir << endl;
    mds->send_message_mds(new MExportDirPrepAck(dir->dirfrag()),
			  m->get_source().num(), MDS_PORT_MIGRATOR);

    // note new state
    import_state[dir->dirfrag()] = IMPORT_PREPPED;

    // done 
    delete m;
  }

  // finish waiters
  finish_contexts(finished, 0);
}




class C_MDS_ImportDirLoggedStart : public Context {
  Migrator *migrator;
  CDir *dir;
  int from;
public:
  C_MDS_ImportDirLoggedStart(Migrator *m, CDir *d, int f) :
    migrator(m), dir(d), from(f) {
  }
  void finish(int r) {
    migrator->import_logged_start(dir, from);
  }
};

void Migrator::handle_export_dir(MExportDir *m)
{
  CDir *dir = cache->get_dirfrag(m->get_dirfrag());
  assert(dir);

  int oldauth = m->get_source().num();
  dout(7) << "handle_export_dir importing " << *dir << " from " << oldauth << endl;
  assert(dir->is_auth() == false);

  cache->show_subtrees();

  // start the journal entry
  EImportStart *le = new EImportStart(dir->dirfrag(), m->get_bounds());
  le->metablob.add_dir_context(dir);
  
  // adjust auth (list us _first_)
  cache->adjust_subtree_auth(dir, mds->get_nodeid(), oldauth);
  cache->verify_subtree_bounds(dir, import_bounds[dir]);

  // add this crap to my cache
  int num_imported_inodes = 0;

  for (list<bufferlist>::iterator p = m->get_dirstate().begin();
       p != m->get_dirstate().end();
       ++p) {
    num_imported_inodes += 
      decode_import_dir(*p, 
                       oldauth, 
                       dir,                 // import root
		       le);
  }
  dout(10) << " " << m->get_bounds().size() << " imported bounds" << endl;
  
  // include bounds in EImportStart
  for (set<CDir*>::iterator it = import_bounds[dir].begin();
       it != import_bounds[dir].end();
       it++) {
    CDir *bd = *it;

    // include bounding dirs in EImportStart
    // (now that the interior metadata is already in the event)
    le->metablob.add_dir(bd, false);
  }

  // adjust popularity
  mds->balancer->add_import(dir);

  dout(7) << "handle_export_dir did " << *dir << endl;

  // log it
  mds->mdlog->submit_entry(le,
			   new C_MDS_ImportDirLoggedStart(this, dir, m->get_source().num()));

  // note state
  import_state[dir->dirfrag()] = IMPORT_LOGGINGSTART;

  // some stats
  if (mds->logger) {
    mds->logger->inc("im");
    mds->logger->inc("iim", num_imported_inodes);
    //mds->logger->set("nim", cache->imports.size());
  }

  delete m;
}


/*
 * note: this does teh full work of reversing and import and cleaning up
 *  state.  
 * called by both handle_mds_failure and by handle_import_map (if we are
 *  a survivor coping with an exporter failure+recovery).
 */
void Migrator::import_reverse(CDir *dir, bool fix_dir_auth)
{
  dout(7) << "import_reverse " << *dir << endl;

  // update auth, with possible subtree merge.
  if (fix_dir_auth) {
    assert(dir->is_subtree_root());
    cache->adjust_subtree_auth(dir, import_peer[dir->dirfrag()]);
    cache->try_subtree_merge(dir);
  }

  // adjust auth bits.
  list<CDir*> q;
  q.push_back(dir);
  while (!q.empty()) {
    CDir *cur = q.front();
    q.pop_front();
    
    // dir
    assert(cur->is_auth());
    cur->state_clear(CDir::STATE_AUTH);
    cur->clear_replicas();
    if (cur->is_dirty())
      cur->mark_clean();

    CDir_map_t::iterator it;
    for (it = cur->begin(); it != cur->end(); it++) {
      CDentry *dn = it->second;

      // dentry
      dn->state_clear(CDentry::STATE_AUTH);
      dn->clear_replicas();
      if (dn->is_dirty()) 
	dn->mark_clean();

      // inode?
      if (dn->is_primary()) {
	CInode *in = dn->get_inode();
	in->state_clear(CDentry::STATE_AUTH);
	in->clear_replicas();
	if (in->is_dirty()) 
	  in->mark_clean();
	in->authlock.clear_gather();
	in->linklock.clear_gather();
	in->dirfragtreelock.clear_gather();
	in->filelock.clear_gather();

	// non-bounding dir?
	list<CDir*> dfs;
	in->get_dirfrags(dfs);
	for (list<CDir*>::iterator p = dfs.begin(); p != dfs.end(); ++p)
	  if (!(*p)->state_test(CDir::STATE_IMPORTBOUND))
	    q.push_back(*p);
      }
    }
  }

  // log our failure
  mds->mdlog->submit_entry(new EImportFinish(dir,false));	// log failure

  // bystanders?
  if (import_bystanders[dir].empty()) {
    dout(7) << "no bystanders, finishing reverse now" << endl;
    import_reverse_unfreeze(dir);
  } else {
    // notify them; wait in aborting state
    dout(7) << "notifying bystanders of abort" << endl;
    import_notify_abort(dir);
    import_state[dir->dirfrag()] = IMPORT_ABORTING;
  }
}

void Migrator::import_notify_abort(CDir *dir)
{
  dout(7) << "import_notify_abort " << *dir << endl;
  
  for (set<int>::iterator p = import_bystanders[dir].begin();
       p != import_bystanders[dir].end();
       ++p) {
    // NOTE: the bystander will think i am _only_ auth, because they will have seen
    // the exporter's failure and updated the subtree auth.  see mdcache->handle_mds_failure().
    MExportDirNotify *notify = 
      new MExportDirNotify(dir->dirfrag(), true,
			   pair<int,int>(mds->get_nodeid(), CDIR_AUTH_UNKNOWN),
			   pair<int,int>(import_peer[dir->dirfrag()], CDIR_AUTH_UNKNOWN));
    notify->copy_bounds(import_bounds[dir]);
    mds->send_message_mds(notify, *p, MDS_PORT_MIGRATOR);
  }
}

void Migrator::import_reverse_unfreeze(CDir *dir)
{
  dout(7) << "import_reverse_unfreeze " << *dir << endl;

  // unfreeze
  dir->unfreeze_tree();

  // discard expire crap
  cache->discard_delayed_expire(dir);
  
  import_reverse_unpin(dir);
}

void Migrator::import_reverse_unpin(CDir *dir) 
{
  dout(7) << "import_reverse_unpin " << *dir << endl;

  // remove importing pin
  dir->put(CDir::PIN_IMPORTING);
  dir->state_clear(CDir::STATE_IMPORTING);

  // remove bound pins
  for (set<CDir*>::iterator it = import_bounds[dir].begin();
       it != import_bounds[dir].end();
       it++) {
    CDir *bd = *it;
    bd->put(CDir::PIN_IMPORTBOUND);
    bd->state_clear(CDir::STATE_IMPORTBOUND);
  }

  // clean up
  import_state.erase(dir->dirfrag());
  import_peer.erase(dir->dirfrag());
  import_bound_inos.erase(dir->dirfrag());
  import_bounds.erase(dir);
  import_bystanders.erase(dir);

  cache->show_subtrees();
  audit();
}


void Migrator::import_logged_start(CDir *dir, int from) 
{
  dout(7) << "import_logged " << *dir << endl;

  // note state
  import_state[dir->dirfrag()] = IMPORT_ACKING;

  // send notify's etc.
  dout(7) << "sending ack for " << *dir << " to old auth mds" << from << endl;
  mds->send_message_mds(new MExportDirAck(dir->dirfrag()),
			from, MDS_PORT_MIGRATOR);

  cache->show_subtrees();
}


void Migrator::handle_export_finish(MExportDirFinish *m)
{
  CDir *dir = cache->get_dirfrag(m->get_dirfrag());
  assert(dir);
  dout(7) << "handle_export_finish on " << *dir << endl;
  import_finish(dir);
  delete m;
}

void Migrator::import_finish(CDir *dir, bool now) 
{
  dout(7) << "import_finish on " << *dir << endl;

  // log finish
  mds->mdlog->submit_entry(new EImportFinish(dir, true));

  // remove pins
  dir->put(CDir::PIN_IMPORTING);
  dir->state_clear(CDir::STATE_IMPORTING);

  for (set<CDir*>::iterator it = import_bounds[dir].begin();
       it != import_bounds[dir].end();
       it++) {
    CDir *bd = *it;

    // remove bound pin
    bd->put(CDir::PIN_IMPORTBOUND);
    bd->state_clear(CDir::STATE_IMPORTBOUND);
  }

  // unfreeze
  dir->unfreeze_tree();

  // adjust auth, with possible subtree merge.
  cache->verify_subtree_bounds(dir, import_bounds[dir]);
  cache->adjust_subtree_auth(dir, mds->get_nodeid());
  cache->try_subtree_merge(dir);
  
  // clear import state (we're done!)
  import_state.erase(dir->dirfrag());
  import_peer.erase(dir->dirfrag());
  import_bound_inos.erase(dir->dirfrag());
  import_bounds.erase(dir);
  import_bystanders.erase(dir);

  // process delayed expires
  cache->process_delayed_expire(dir);

  // ok now finish contexts
  dout(5) << "finishing any waiters on imported data" << endl;
  dir->finish_waiting(CDir::WAIT_IMPORTED);

  // log it
  if (mds->logger) {
    //mds->logger->set("nex", cache->exports.size());
    //mds->logger->set("nim", cache->imports.size());
  }
  cache->show_subtrees();
  audit();

  // is it empty?
  if (dir->get_size() == 0 &&
      !dir->inode->is_auth()) {
    // reexport!
    export_empty_import(dir);
  }
}


void Migrator::decode_import_inode(CDentry *dn, bufferlist& bl, int& off, int oldauth)
{  
  dout(15) << "decode_import_inode on " << *dn << endl;

  CInodeExport istate;
  off = istate._decode(bl, off);
  
  bool added = false;
  CInode *in = cache->get_inode(istate.get_ino());
  if (!in) {
    in = new CInode(mds->mdcache);
    added = true;
  } else {
    in->state_set(CInode::STATE_AUTH);
  }

  // state after link  -- or not!  -sage
  set<int> merged_client_caps;
  istate.update_inode(in, merged_client_caps);
 
  // link before state  -- or not!  -sage
  if (dn->inode != in) {
    assert(!dn->inode);
    dn->dir->link_inode(dn, in);
  }
 
  // add inode?
  if (added) {
    cache->add_inode(in);
    dout(10) << "added " << *in << endl;
  } else {
    dout(10) << "  had " << *in << endl;
  }
  
  
  // adjust replica list
  //assert(!in->is_replica(oldauth));  // not true on failed export
  in->add_replica( oldauth, CInode::EXPORT_NONCE );
  if (in->is_replica(mds->get_nodeid()))
    in->remove_replica(mds->get_nodeid());
  
  // twiddle locks
  if (in->authlock.do_import(oldauth, mds->get_nodeid()))
    mds->locker->simple_eval(&in->authlock);
  if (in->linklock.do_import(oldauth, mds->get_nodeid()))
    mds->locker->simple_eval(&in->linklock);
  if (in->dirfragtreelock.do_import(oldauth, mds->get_nodeid()))
    mds->locker->simple_eval(&in->dirfragtreelock);

  // caps
  for (set<int>::iterator it = merged_client_caps.begin();
       it != merged_client_caps.end();
       it++) {
    MClientFileCaps *caps = new MClientFileCaps(in->inode,
                                                in->client_caps[*it].get_last_seq(),
                                                in->client_caps[*it].pending(),
                                                in->client_caps[*it].wanted(),
                                                MClientFileCaps::FILECAP_REAP);
    caps->set_mds( oldauth ); // reap from whom?
    mds->messenger->send_message(caps, 
				 mds->clientmap.get_inst(*it),
				 0, MDS_PORT_CACHE);
  }

  // filelock
  if (in->filelock.do_import(oldauth, mds->get_nodeid()))
    mds->locker->simple_eval(&in->filelock);
}


int Migrator::decode_import_dir(bufferlist& bl,
				int oldauth,
				CDir *import_root,
				EImportStart *le)
{
  int off = 0;
  
  // set up dir
  CDirExport dstate;
  off = dstate._decode(bl, off);
  
  CInode *diri = cache->get_inode(dstate.get_dirfrag().ino);
  assert(diri);
  CDir *dir = diri->get_or_open_dirfrag(mds->mdcache, dstate.get_dirfrag().frag);
  assert(dir);
  
  dout(7) << "decode_import_dir " << *dir << endl;

  // assimilate state
  dstate.update_dir( dir );

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
    le->metablob.add_dir(dir, true);  // Hmm: false would be okay in some cases

  int num_imported = 0;

  // take all waiters on this dir
  // NOTE: a pass of imported data is guaranteed to get all of my waiters because
  // a replica's presense in my cache implies/forces it's presense in authority's.
  list<Context*> waiters;
  
  dir->take_waiting(CDir::WAIT_ANY, waiters);
  for (list<Context*>::iterator it = waiters.begin();
       it != waiters.end();
       it++) 
    import_root->add_waiter(CDir::WAIT_IMPORTED, *it);
  
  dout(15) << "doing contents" << endl;
  
  // contents
  long nden = dstate.get_nden();
  
  for (; nden>0; nden--) {
    
    num_imported++;
    
    // dentry
    string dname;
    _decode(dname, bl, off);
    
    CDentry *dn = dir->lookup(dname);
    if (!dn)
      dn = dir->add_dentry(dname);  // null
    
    // decode state
    dn->decode_import_state(bl, off, oldauth, mds->get_nodeid());
    dout(15) << "decode_import_dir got " << *dn << endl;
    
    // points to...
    char icode;
    bl.copy(off, 1, &icode);
    off++;
    
    if (icode == 'N') {
      // null dentry
      assert(dn->is_null());  
      
      // fall thru
    }
    else if (icode == 'L') {
      // remote link
      inodeno_t ino;
      bl.copy(off, sizeof(ino), (char*)&ino);
      off += sizeof(ino);
      if (dn->is_remote()) {
	assert(dn->get_remote_ino() == ino);
      } else {
	dir->link_inode(dn, ino);
      }
    }
    else if (icode == 'I') {
      // inode
      decode_import_inode(dn, bl, off, oldauth);
    }
    
    // add dentry to journal entry
    if (le)
      le->metablob.add_dentry(dn, dn->is_dirty());
  }
  
  dout(7) << "decode_import_dir done " << *dir << endl;
  return num_imported;
}





// authority bystander

void Migrator::handle_export_notify(MExportDirNotify *m)
{
  CDir *dir = cache->get_dirfrag(m->get_dirfrag());

  int from = m->get_source().num();
  pair<int,int> old_auth = m->get_old_auth();
  pair<int,int> new_auth = m->get_new_auth();
  
  if (!dir) {
    dout(7) << "handle_export_notify " << old_auth << " -> " << new_auth
	    << " on missing dir " << m->get_dirfrag() << endl;
  } else if (dir->authority() != old_auth) {
    dout(7) << "handle_export_notify old_auth was " << dir->authority() 
	    << " != " << old_auth << " -> " << new_auth
	    << " on " << *dir << endl;
  } else {
    dout(7) << "handle_export_notify " << old_auth << " -> " << new_auth
	    << " on " << *dir << endl;
    // adjust auth
    cache->adjust_bounded_subtree_auth(dir, m->get_bounds(), new_auth);
    
    // induce a merge?
    cache->try_subtree_merge(dir);
  }
  
  // send ack
  if (m->wants_ack()) {
    mds->send_message_mds(new MExportDirNotifyAck(m->get_dirfrag()),
			  from, MDS_PORT_MIGRATOR);
  } else {
    // aborted.  no ack.
    dout(7) << "handle_export_notify no ack requested" << endl;
  }
  
  delete m;
}



