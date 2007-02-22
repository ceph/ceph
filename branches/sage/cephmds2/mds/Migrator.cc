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
#include "MDStore.h"
#include "Migrator.h"

#include "MDBalancer.h"
#include "MDLog.h"
#include "MDSMap.h"

#include "include/filepath.h"

#include "events/EString.h"
#include "events/EExportStart.h"
#include "events/EExportFinish.h"
#include "events/EImportStart.h"
#include "events/EImportFinish.h"

#include "msg/Messenger.h"

#include "messages/MClientFileCaps.h"

#include "messages/MExportDirDiscover.h"
#include "messages/MExportDirDiscoverAck.h"
#include "messages/MExportDirPrep.h"
#include "messages/MExportDirPrepAck.h"
#include "messages/MExportDirWarning.h"
#include "messages/MExportDirWarningAck.h"
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
    handle_export_dir_discover((MExportDirDiscover*)m);
    break;
  case MSG_MDS_EXPORTDIRPREP:
    handle_export_dir_prep((MExportDirPrep*)m);
    break;
  case MSG_MDS_EXPORTDIR:
    handle_export_dir((MExportDir*)m);
    break;
  case MSG_MDS_EXPORTDIRFINISH:
    handle_export_dir_finish((MExportDirFinish*)m);
    break;

    // export 
  case MSG_MDS_EXPORTDIRDISCOVERACK:
    handle_export_dir_discover_ack((MExportDirDiscoverAck*)m);
    break;
  case MSG_MDS_EXPORTDIRPREPACK:
    handle_export_dir_prep_ack((MExportDirPrepAck*)m);
    break;
  case MSG_MDS_EXPORTDIRWARNINGACK:
    handle_export_dir_warning_ack((MExportDirWarningAck*)m);
    break;
  case MSG_MDS_EXPORTDIRACK:
    handle_export_dir_ack((MExportDirAck*)m);
    break;
  case MSG_MDS_EXPORTDIRNOTIFYACK:
    handle_export_dir_notify_ack((MExportDirNotifyAck*)m);
    break;    

    // export 3rd party (dir_auth adjustments)
  case MSG_MDS_EXPORTDIRWARNING:
    handle_export_dir_warning((MExportDirWarning*)m);
    break;
  case MSG_MDS_EXPORTDIRNOTIFY:
    handle_export_dir_notify((MExportDirNotify*)m);
    break;


    // hashing
    /*
  case MSG_MDS_HASHDIRDISCOVER:
    handle_hash_dir_discover((MHashDirDiscover*)m);
    break;
  case MSG_MDS_HASHDIRDISCOVERACK:
    handle_hash_dir_discover_ack((MHashDirDiscoverAck*)m);
    break;
  case MSG_MDS_HASHDIRPREP:
    handle_hash_dir_prep((MHashDirPrep*)m);
    break;
  case MSG_MDS_HASHDIRPREPACK:
    handle_hash_dir_prep_ack((MHashDirPrepAck*)m);
    break;
  case MSG_MDS_HASHDIR:
    handle_hash_dir((MHashDir*)m);
    break;
  case MSG_MDS_HASHDIRACK:
    handle_hash_dir_ack((MHashDirAck*)m);
    break;
  case MSG_MDS_HASHDIRNOTIFY:
    handle_hash_dir_notify((MHashDirNotify*)m);
    break;

    // unhashing
  case MSG_MDS_UNHASHDIRPREP:
    handle_unhash_dir_prep((MUnhashDirPrep*)m);
    break;
  case MSG_MDS_UNHASHDIRPREPACK:
    handle_unhash_dir_prep_ack((MUnhashDirPrepAck*)m);
    break;
  case MSG_MDS_UNHASHDIR:
    handle_unhash_dir((MUnhashDir*)m);
    break;
  case MSG_MDS_UNHASHDIRACK:
    handle_unhash_dir_ack((MUnhashDirAck*)m);
    break;
  case MSG_MDS_UNHASHDIRNOTIFY:
    handle_unhash_dir_notify((MUnhashDirNotify*)m);
    break;
  case MSG_MDS_UNHASHDIRNOTIFYACK:
    handle_unhash_dir_notify_ack((MUnhashDirNotifyAck*)m);
    break;
    */

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
  
  return;  // hack fixme

  if (!dir->is_import()) {
    dout(7) << "not import (anymore?)" << endl;
    return;
  }
  if (dir->inode->is_root()) {
    dout(7) << "root" << endl;
    return;
  }

  if (dir->get_size() > 0) {
    dout(7) << "not actually empty" << endl;
    return;
  }

  // is it really empty?
  if (!dir->is_complete()) {
    dout(7) << "not complete, fetching." << endl;
    mds->mdstore->fetch_dir(dir,
                            new C_MDC_EmptyImport(this,dir));
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
    
    if (export_peer[dir] == who) {
      // the guy i'm exporting to failed.  
      // clean up.
      dout(10) << "cleaning up export state " << p->second << " of " << *dir << endl;
      
      switch (p->second) {
      case EXPORT_DISCOVERING:
	dout(10) << "state discovering : canceling freeze and removing auth_pin" << endl;
	dir->unfreeze_tree();  // cancel the freeze
	dir->auth_unpin();     // remove the auth_pin (that was holding up the freeze)
	break;

      case EXPORT_FREEZING:
	dout(10) << "state freezing : canceling freeze" << endl;
	dir->unfreeze_tree();  // cancel the freeze
	break;

      case EXPORT_LOGGINGSTART:
      case EXPORT_PREPPING:
      case EXPORT_WARNING:
	dout(10) << "state loggingstart|prepping|warning : unfreezing, logging EExportFinish(false)" << endl;
	dir->unfreeze_tree();
	mds->mdlog->submit_entry(new EExportFinish(dir,false));
	break;
	
      case EXPORT_EXPORTING:
	dout(10) << "state exporting : logging EExportFinish(false), reversing, and unfreezing" << endl;
	mds->mdlog->submit_entry(new EExportFinish(dir,false));
	reverse_export(dir);
	dir->unfreeze_tree();
	break;

      case EXPORT_LOGGINGFINISH:
	dout(10) << "state loggingfinish : just cleaning up, we were successful." << endl;
	break;

      case EXPORT_NOTIFYING:
	dout(10) << "state notifying : just cleaning up, we were successful." << endl;
	break;

      default:
	assert(0);
      }

      export_state.erase(dir);
      export_peer.erase(dir);
      export_warning_ack_waiting.erase(dir);
      export_notify_ack_waiting.erase(dir);

      // unpin the path
      vector<CDentry*> trace;
      cache->make_trace(trace, dir->inode);
      cache->path_unpin(trace, 0);

      // wake up any waiters
      mds->queue_finished(export_finish_waiters[dir]);
      export_finish_waiters.erase(dir);
      
      // send pending import_maps?  (these need to go out when all exports have finished.)
      mds->mdcache->send_pending_import_maps();

      mds->mdcache->show_imports();
      mds->mdcache->show_cache();
    } else {
      // third party failed.  potential peripheral damage?
      if (p->second == EXPORT_WARNING) {
	// exporter waiting for warning acks, let's fake theirs.
	if (export_warning_ack_waiting[dir].count(who)) {
	  dout(10) << "faking export_dir_warning_ack from mds" << who
		   << " on " << *dir << " to mds" << export_peer[dir] 
		   << endl;
	  export_warning_ack_waiting[dir].erase(who);
	  export_notify_ack_waiting[dir].erase(who);   // they won't get a notify either.
	  if (export_warning_ack_waiting[dir].empty()) 
	    export_dir_go(dir);
	}
      }
      if (p->second == EXPORT_NOTIFYING) {
	// exporter is waiting for notify acks, fake it
	if (export_notify_ack_waiting[dir].count(who)) {
	  dout(10) << "faking export_dir_notify_ack from mds" << who
		   << " on " << *dir << " to mds" << export_peer[dir] 
		   << endl;
	  export_notify_ack_waiting[dir].erase(who);
	  if (export_notify_ack_waiting[dir].empty()) 
	    export_dir_finish(dir);
	}
      }
    }
    
    // next!
    p = next;
  }


  // check my imports
  map<inodeno_t,int>::iterator q = import_state.begin();
  while (q != import_state.end()) {
    map<inodeno_t,int>::iterator next = q;
    next++;
    inodeno_t dirino = q->first;
    CInode *diri = mds->mdcache->get_inode(dirino);
    CDir *dir = 0;
    if (diri) 
      dir = diri->dir;

    if (import_peer[dirino] == who) {
      switch (import_state[dirino]) {
      case IMPORT_DISCOVERED:
	dout(10) << "state discovered : unpinning " << *diri << endl;
	assert(diri);
	// unpin base
	diri->put(CInode::PIN_IMPORTING);
	break;

	// NOTE: state order reversal + fall-thru, pay attention.

      case IMPORT_PREPPED:
	dout(10) << "state prepping : unpinning base+bounds, unfreezing,  " << *dir << endl;
	assert(dir);
	dir->set_dir_auth_pending(CDIR_AUTH_UNKNOWN);   // not anymore.
	
	// unfreeze
	dir->unfreeze_tree();

	// fall-thru to unpin base+bounds

      case IMPORT_PREPPING:
	if (import_state[dirino] == IMPORT_PREPPING) {
	  dout(10) << "state prepping : unpinning base+bounds " << *dir << endl;
	}
	assert(dir);
	// unpin base
	dir->put(CDir::PIN_IMPORTING);
	// unpin bounds
	for (set<CDir*>::iterator it = import_bounds[dir].begin();
	     it != import_bounds[dir].end();
	     it++) {
	  CDir *bd = *it;
	  assert(bd->state_test(CDIR_STATE_IMPORTBOUND));
	  bd->state_clear(CDIR_STATE_IMPORTBOUND);
	  bd->put(CDir::PIN_IMPORTBOUND);
	}
	break;


      case IMPORT_LOGGINGSTART:
	dout(10) << "state loggingstart : reversing import on " << *dir << endl;
	assert(dir);
	mds->mdlog->submit_entry(new EImportFinish(dir,false));	// log failure
	reverse_import(dir);
	break;

      case IMPORT_ACKING:
	// hrm.  make this an ambiguous import, and wait for exporter recovery to disambiguate
	// ...
	break;

      case IMPORT_LOGGINGFINISH:
	// do nothing, exporter is no longer involved.
	break;
      }

      import_state.erase(dirino);
      import_peer.erase(dirino);
      import_bound_inos.erase(dirino);
      import_bounds.erase(dir);

      mds->mdcache->show_imports();
      mds->mdcache->show_cache();
    }

    // next!
    q = next;
  }
}






// ==========================================================
// EXPORT


class C_MDC_ExportFreeze : public Context {
  Migrator *mig;
  CDir *ex;   // dir i'm exporting
  int dest;

public:
  C_MDC_ExportFreeze(Migrator *m, CDir *e, int d) :
	mig(m), ex(e), dest(d) {}
  virtual void finish(int r) {
    if (r >= 0)
      mig->export_dir_frozen(ex, dest);
  }
};



/** export_dir(dir, dest)
 * public method to initiate an export.
 * will fail if the directory is freezing, frozen, unpinnable, or root. 
 */
void Migrator::export_dir(CDir *dir,
			  int dest)
{
  dout(7) << "export_dir " << *dir << " to " << dest << endl;
  assert(dest != mds->get_nodeid());
  assert(!dir->is_hashed());
   
  if (mds->mdsmap->is_degraded()) {
    dout(7) << "cluster degraded, no exports for now" << endl;
    return;
  }

  if (dir->inode->is_root()) {
    dout(7) << "i won't export root" << endl;
    assert(0);
    return;
  }

  if (dir->is_frozen() ||
      dir->is_freezing()) {
    dout(7) << " can't export, freezing|frozen.  wait for other exports to finish first." << endl;
    return;
  }
  if (dir->is_hashed()) {
    dout(7) << "can't export hashed dir right now.  implement me carefully later." << endl;
    return;
  }
  

  // pin path?
  vector<CDentry*> trace;
  cache->make_trace(trace, dir->inode);
  if (!cache->path_pin(trace, 0, 0)) {
    dout(7) << "export_dir couldn't pin path, failing." << endl;
    return;
  }

  // ok, let's go.
  assert(export_state.count(dir) == 0);
  export_state[dir] = EXPORT_DISCOVERING;
  export_peer[dir] = dest;

  // send ExportDirDiscover (ask target)
  mds->send_message_mds(new MExportDirDiscover(dir->inode), dest, MDS_PORT_MIGRATOR);
  dir->auth_pin();   // pin dir, to hang up our freeze  (unpin on prep ack)

  // take away the popularity we're sending.   FIXME: do this later?
  mds->balancer->subtract_export(dir);
  
  // freeze the subtree
  dir->freeze_tree(new C_MDC_ExportFreeze(this, dir, dest));
}


/*
 * called on receipt of MExportDirDiscoverAck
 * the importer now has the directory's _inode_ in memory, and pinned.
 */
void Migrator::handle_export_dir_discover_ack(MExportDirDiscoverAck *m)
{
  CInode *in = cache->get_inode(m->get_ino());
  assert(in);
  CDir *dir = in->dir;
  assert(dir);
  
  dout(7) << "export_dir_discover_ack from " << m->get_source()
	  << " on " << *dir << ", releasing auth_pin" << endl;

  export_state[dir] = EXPORT_FREEZING;

  dir->auth_unpin();   // unpin to allow freeze to complete
  
  delete m;  // done
}

class C_MDC_ExportStartLogged : public Context {
  Migrator *mig;
  CDir *ex;   // dir i'm exporting
  int dest;
  MExportDirPrep *prep;

public:
  C_MDC_ExportStartLogged(Migrator *m, CDir *e, int d, MExportDirPrep *p) :
	mig(m), ex(e), dest(d), prep(p) {}
  virtual void finish(int r) {
    mig->export_dir_frozen_logged(ex, prep, dest);
  }
};

void Migrator::export_dir_frozen(CDir *dir,
                                int dest)
{
  // subtree is now frozen!
  dout(7) << "export_dir_frozen on " << *dir << " to " << dest << endl;
  export_state[dir] = EXPORT_LOGGINGSTART;

  show_imports();

  // -- note/mark subtree bounds --
  // also include traces to all nested exports.
  cache->find_nested_exports(dir, export_bounds[dir]);
  set<CDir*> &bounds = export_bounds[dir];
  
  // note that dest an ambiguous auth for this subtree.
  dir->set_dir_auth_pending(export_peer[dir]);
  
  // generate prep message, log entry.
  EExportStart *le = new EExportStart(dir, dest);
  MExportDirPrep *prep = new MExportDirPrep(dir->inode);

  // include spanning tree for all nested exports.
  // these need to be on the destination _before_ the final export so that
  // dir_auth updates on any nested exports are properly absorbed.
  
  set<inodeno_t> inodes_added;

  // include base dir
  prep->add_dir( new CDirDiscover(dir, dir->add_replica(dest)) );
  le->metablob.add_dir( dir, false );
  
  // check bounds
  for (set<CDir*>::iterator it = bounds.begin();
       it != bounds.end();
       it++) {
    CDir *exp = *it;

    // pin it.
    exp->get(CDir::PIN_EXPORTBOUND);
    
    dout(7) << " including nested export " << *exp << " in prep" << endl;

    prep->add_export( exp->ino() );
    le->get_bounds().insert(exp->ino());
    le->metablob.add_dir_context( exp );
    le->metablob.add_dir( exp, false );

    /* first assemble each trace, in trace order, and put in message */
    list<CInode*> inode_trace;  

    // trace to dir
    CDir *cur = exp;
    while (cur != dir) {
      // don't repeat ourselves
      if (inodes_added.count(cur->ino())) break;   // did already!
      inodes_added.insert(cur->ino());
      
      CDir *parent_dir = cur->get_parent_dir();

      // inode?
      assert(cur->inode->is_auth());
      inode_trace.push_front(cur->inode);
      dout(7) << "  will add " << *cur->inode << endl;
      
      // include dir? note: this'll include everything except the nested exports themselves, 
      // since someone else is obviously auth.
      if (cur->is_auth()) {
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
      prep->add_inode( in->parent->get_dir()->ino(),
                       in->parent->get_name(),
                       in->replicate_to(dest) );
    }

  }
  
  // log our intentions
  dout(7) << " logging EExportStart" << endl;
  mds->mdlog->submit_entry(le, new C_MDC_ExportStartLogged(this, dir, dest, prep));
}

void Migrator::export_dir_frozen_logged(CDir *dir, MExportDirPrep *prep, int dest)
{
  dout(7) << "export_dir_frozen_logged " << *dir << endl;

  if (export_state.count(dir) == 0 ||
      export_state[dir] != EXPORT_LOGGINGSTART) {
    // export must have aborted.  
    dout(7) << "export must have aborted, unfreezing and deleting me old prep message" << endl;
    delete prep;
    dir->unfreeze_tree();  // cancel the freeze
    return;
  }

  export_state[dir] = EXPORT_PREPPING;
  mds->send_message_mds(prep, dest, MDS_PORT_MIGRATOR);
}

void Migrator::handle_export_dir_prep_ack(MExportDirPrepAck *m)
{
  CInode *in = cache->get_inode(m->get_ino());
  assert(in);
  CDir *dir = in->dir;
  assert(dir);

  dout(7) << "export_dir_prep_ack " << *dir << ", sending notifies" << endl;
  
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
    if (!mds->mdsmap->is_active(p->first) || 
	!mds->mdsmap->is_stopping(p->first)) 
      continue;  // only if active
    export_warning_ack_waiting[dir].insert(p->first);
    export_notify_ack_waiting[dir].insert(p->first);  // we'll eventually get a notifyack, too!
    mds->send_message_mds(new MExportDirWarning(dir->ino(), export_peer[dir]),
			  p->first, MDS_PORT_MIGRATOR);
  }
  export_state[dir] = EXPORT_WARNING;

  // nobody to warn?
  if (export_warning_ack_waiting.count(dir) == 0) 
    export_dir_go(dir);  // start export.
    
  // done.
  delete m;
}


void Migrator::handle_export_dir_warning_ack(MExportDirWarningAck *m)
{
  CInode *in = cache->get_inode(m->get_ino());
  assert(in);
  CDir *dir = in->dir;
  assert(dir);
  
  dout(7) << "export_dir_warning_ack " << *dir << " from " << m->get_source() << endl;

  if (export_state.count(dir) == 0 ||
      export_state[dir] != EXPORT_WARNING) {
    // export must have aborted.  
    dout(7) << "export must have aborted" << endl;
    delete m;
    return;
  }

  // process the warning_ack
  int from = m->get_source().num();
  assert(export_warning_ack_waiting.count(dir));
  export_warning_ack_waiting[dir].erase(from);
  
  if (export_warning_ack_waiting[dir].empty()) 
    export_dir_go(dir);     // start export.
    
  // done
  delete m;
}


void Migrator::export_dir_go(CDir *dir)
{  
  assert(export_peer.count(dir));
  int dest = export_peer[dir];
  dout(7) << "export_dir_go " << *dir << " to " << dest << endl;

  show_imports();
  
  export_warning_ack_waiting.erase(dir);
  export_state[dir] = EXPORT_EXPORTING;

  assert(export_bounds.count(dir) == 1);
  assert(export_data.count(dir) == 0);

  assert(dir->get_cum_auth_pins() == 0);

  // update imports/exports
  cache->export_subtree(dir, export_bounds[dir], dest);

  // fill export message with cache data
  C_Contexts *fin = new C_Contexts;       // collect all the waiters
  int num_exported_inodes = encode_export_dir( export_data[dir], 
                                             fin, 
                                             dir,   // base
                                             dir,   // recur start point
                                             dest );
  
  // send the export data!
  MExportDir *req = new MExportDir(dir->ino());

  // export state
  req->set_dirstate( export_data[dir] );

  // add bounds to message
  for (set<CDir*>::iterator p = export_bounds[dir].begin();
       p != export_bounds[dir].end();
       ++p)
    req->add_export((*p)->ino());

  //s end
  mds->send_message_mds(req, dest, MDS_PORT_MIGRATOR);

  // queue up the finisher
  dir->add_waiter( CDIR_WAIT_UNFREEZE, fin );


  // stats
  if (mds->logger) mds->logger->inc("ex");
  if (mds->logger) mds->logger->inc("iex", num_exported_inodes);

  show_imports();
}


/** encode_export_inode
 * update our local state for this inode to export.
 * encode relevant state to be sent over the wire.
 * used by: export_dir_walk, file_rename (if foreign)
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
  // hard
  in->hardlock.clear_gather();
  if (in->hardlock.get_state() == LOCK_GLOCKR)
    in->hardlock.set_state(LOCK_LOCK);

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
  in->set_auth(false);
  in->replica_nonce = CINODE_EXPORT_NONCE;
  
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

  dout(7) << "export_dir_walk " << *dir << " " << dir->nitems << " items" << endl;
  
  assert(dir->get_projected_version() == dir->get_version());

  // dir 
  bufferlist enc_dir;
  
  CDirExport dstate(dir);
  dstate._encode( enc_dir );
  
  // release open_by 
  dir->clear_replicas();

  // mark
  assert(dir->is_auth());
  dir->state_clear(CDIR_STATE_AUTH);
  dir->replica_nonce = CDIR_NONCE_EXPORT;

  // proxy
  //dir->state_set(CDIR_STATE_PROXY);
  //dir->get(CDir::PIN_PROXY);
  //export_proxy_dirinos[basedir].push_back(dir->ino());

  list<CDir*> subdirs;

  if (dir->is_hashed()) {
    // fix state
    dir->state_clear( CDIR_STATE_AUTH );

  } else {
    
    if (dir->is_dirty())
      dir->mark_clean();
    
    // discard most dir state
    dir->state &= CDIR_MASK_STATE_EXPORT_KEPT;  // i only retain a few things.
    
    // suck up all waiters
    list<Context*> waiting;
    dir->take_waiting(CDIR_WAIT_ANY, waiting);    // all dir waiters
    fin->take(waiting);
    
    // inodes
    
    CDir_map_t::iterator it;
    for (it = dir->begin(); it != dir->end(); it++) {
      CDentry *dn = it->second;
      CInode *in = dn->get_inode();
      
      num_exported++;
      
      // -- dentry
      dout(7) << "export_dir_walk exporting " << *dn << endl;

      // name
      _encode(it->first, enc_dir);
      
      // state
      it->second->encode_export_state(enc_dir);

      // points to...

      // null dentry?
      if (dn->is_null()) {
        enc_dir.append("N", 1);  // null dentry
        assert(dn->is_sync());
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
      if (in->is_dir() && in->dir) { 
        if (in->dir->is_auth()) {
          // nested subdir
          assert(in->dir->get_dir_auth().first == CDIR_AUTH_PARENT);
          subdirs.push_back(in->dir);  // it's ours, recurse (later)
	}
      }
      
      // add to proxy
      //export_proxy_inos[basedir].push_back(in->ino());
      //in->state_set(CInode::STATE_PROXY);
      //in->get(CInode::PIN_PROXY);
      
      // waiters
      list<Context*> waiters;
      in->take_waiting(CINODE_WAIT_ANY, waiters);
      fin->take(waiters);
    }
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
    migrator->export_dir_logged_finish(dir);
  }
};


/*
 * i should get an export_dir_ack from the export target.
 */
void Migrator::handle_export_dir_ack(MExportDirAck *m)
{
  CInode *diri = cache->get_inode(m->get_ino());
  CDir *dir = diri->dir;
  assert(dir);
  assert(dir->is_frozen_tree_root());  // i'm exporting!

  // yay!
  dout(7) << "handle_export_dir_ack " << *dir << endl;

  export_warning_ack_waiting.erase(dir);
  
  export_state[dir] = EXPORT_LOGGINGFINISH;
  export_data.erase(dir);
  export_bounds.erase(dir);
  
  // log export completion, then finish (unfreeze, trigger finish context, etc.)
  dir->get(CDir::PIN_LOGGINGEXPORTFINISH);
  mds->mdlog->submit_entry(new EExportFinish(dir, true),
			   new C_MDS_ExportFinishLogged(this, dir));
  
  delete m;
}



/*
 * this happens if hte dest failes after i send teh export data but before it is acked
 * that is, we don't know they safely received and logged it, so we reverse our changes
 * and go on.
 */
void Migrator::reverse_export(CDir *dir)
{
  dout(7) << "reverse_export " << *dir << endl;
  
  assert(export_state[dir] == EXPORT_EXPORTING);
  assert(export_bounds.count(dir));
  assert(export_data.count(dir));
  
  // adjust dir_auth, exports
  cache->import_subtree(dir, export_bounds[dir]);

  // unpin bounds
  for (set<CDir*>::iterator p = export_bounds[dir].begin();
       p != export_bounds[dir].end();
       ++p) {
    CDir *bd = *p;
    bd->put(CDir::PIN_EXPORTBOUND);
  }

  // re-import the metadata
  list<inodeno_t> imported_subdirs;
  int num_imported_inodes = 0;

  for (list<bufferlist>::iterator p = export_data[dir].begin();
       p != export_data[dir].end();
       ++p) {
    num_imported_inodes += 
      decode_import_dir(*p, 
                       export_peer[dir], 
                       dir,                 // import root
                       imported_subdirs,
		       0);
  }

  // remove proxy bits
  //clear_export_proxy_pins(dir);

  // process delayed expires
  cache->process_delayed_expire(dir);
  
  // send out notify(abort) to bystanders.  no ack necessary.
  for (set<int>::iterator p = export_notify_ack_waiting[dir].begin();
       p != export_notify_ack_waiting[dir].end();
       ++p) {
    MExportDirNotify *notify = new MExportDirNotify(dir->ino(), 
						    mds->get_nodeid(), mds->get_nodeid());
    notify->copy_exports(export_bounds[dir]);
    mds->send_message_mds(notify, *p, MDS_PORT_MIGRATOR);
  }
  
  // some clean up
  export_data.erase(dir);
  export_bounds.erase(dir);
  export_warning_ack_waiting.erase(dir);
  export_notify_ack_waiting.erase(dir);
}



/*
 * once i get the ack, and logged the EExportFinish(true),
 * send notifies (if any), otherwise go straight to finish.
 * 
 */
void Migrator::export_dir_logged_finish(CDir *dir)
{
  dout(7) << "export_dir_commit " << *dir << endl;
  dir->put(CDir::PIN_LOGGINGEXPORTFINISH);

  if (export_state.count(dir) == 0||
      export_state[dir] != EXPORT_LOGGINGFINISH) {
    dout(7) << "target must have failed, not sending final commit message.  export succeeded anyway." << endl;
    return;
  }

  // send notifies
  int dest = export_peer[dir];

  for (set<int>::iterator p = export_notify_ack_waiting[dir].begin();
       p != export_notify_ack_waiting[dir].end();
       ++p) {
    MExportDirNotify *notify = new MExportDirNotify(dir->ino(), 
						    mds->get_nodeid(), dest);
    notify->copy_exports(export_bounds[dir]);
    
    mds->send_message_mds(notify, *p, MDS_PORT_MIGRATOR);
  }

  // wait for notifyacks
  export_state[dir] = EXPORT_NOTIFYING;
  
  // no notifies to wait for?
  if (export_notify_ack_waiting[dir].empty())
    export_dir_finish(dir);  // skip notify/notify_ack stage.
}

/*
 * i'll get an ack from each bystander.
 * when i get them all, unfreeze and send the finish.
 */
void Migrator::handle_export_dir_notify_ack(MExportDirNotifyAck *m)
{
  CInode *in = cache->get_inode(m->get_ino());
  CDir *dir = in ? in->dir : 0;

  if (dir) {
    dout(7) << "handle_export_dir_notify_ack from " << m->get_source()
	    << " on " << *dir << endl;
  } else {
    dout(7) << "handle_export_dir_notify_ack from " << m->get_source()
	    << " on dir " << m->get_ino() << endl;
  }
  
  // aborted?
  if (!dir ||
      export_state.count(dir) == 0 ||
      export_state[dir] != EXPORT_NOTIFYING) {
    dout(7) << "target must have failed, not sending finish message.  export succeeded anyway." << endl;
    
    delete m;
    return;
  }

  // process.
  int from = m->get_source().num();
  assert(export_notify_ack_waiting.count(dir));
  export_notify_ack_waiting[dir].erase(from);
  
  if (export_notify_ack_waiting[dir].empty())
    export_dir_finish(dir);

  delete m;
}


void Migrator::export_dir_finish(CDir *dir)
{
  dout(7) << "export_dir_finish " << *dir << endl;

  export_notify_ack_waiting.erase(dir);

  if (export_state.count(dir)) {
    // send finish/commit to new auth
    mds->send_message_mds(new MExportDirFinish(dir->ino()), export_peer[dir], MDS_PORT_MIGRATOR);

    // remove from exporting list
    export_state.erase(dir);
    export_peer.erase(dir);
  } else {
    dout(7) << "target must have failed, not sending final commit message.  export succeeded anyway." << endl;
  }
    
  // unfreeze
  dout(7) << "export_dir_finish unfreezing" << endl;
  dir->unfreeze_tree();
  
  // unpin bounds
  for (set<CDir*>::iterator p = export_bounds[dir].begin();
       p != export_bounds[dir].end();
       ++p) {
    CDir *bd = *p;
    bd->put(CDir::PIN_EXPORTBOUND);
  }

  // unpin path
  dout(7) << "export_dir_finish unpinning path" << endl;
  vector<CDentry*> trace;
  cache->make_trace(trace, dir->inode);
  cache->path_unpin(trace, 0);

  // unpin proxies
  //clear_export_proxy_pins(dir);

  // discard delayed expires
  cache->discard_delayed_expire(dir);

  // queue finishers
  mds->queue_finished(export_finish_waiters[dir]);
  export_finish_waiters.erase(dir);

  // stats
  if (mds->logger) mds->logger->set("nex", cache->exports.size());

  show_imports();

  // send pending import_maps?
  mds->mdcache->send_pending_import_maps();
}


/*
void Migrator::clear_export_proxy_pins(CDir *dir)
{
  dout(10) << "clear_export_proxy_pins " << *dir << endl;

  // inodes
  for (list<inodeno_t>::iterator it = export_proxy_inos[dir].begin();
       it != export_proxy_inos[dir].end();
       it++) {
    CInode *in = cache->get_inode(*it);
    dout(15) << " " << *in << endl;
    in->put(CInode::PIN_PROXY);
    assert(in->state_test(CInode::STATE_PROXY));
    in->state_clear(CInode::STATE_PROXY);
  }
  export_proxy_inos.erase(dir);
  
  // dirs
  for (list<inodeno_t>::iterator it = export_proxy_dirinos[dir].begin();
       it != export_proxy_dirinos[dir].end();
       it++) {
    CDir *dir = cache->get_inode(*it)->dir;    
    dout(15) << " " << *dir << endl;
    dir->put(CDir::PIN_PROXY);
    assert(dir->state_test(CDIR_STATE_PROXY));
    dir->state_clear(CDIR_STATE_PROXY);
    
    // hose neg dentries, too, since we're no longer auth
    CDir_map_t::iterator it;
    for (it = dir->begin(); it != dir->end(); ) {
      CDentry *dn = it->second;
      it++;
      if (dn->is_null()) {
	assert(dn->is_sync());
	dir->remove_dentry(dn);
      } else {
	//dout(10) << "export_dir_notify_ack leaving xlocked neg " << *dn << endl;
	if (dn->is_dirty())
	  dn->mark_clean();
      }
    }
  }
  export_proxy_dirinos.erase(dir);
}    
*/






// ==========================================================
// IMPORT


class C_MDC_ExportDirDiscover : public Context {
  Migrator *mig;
  MExportDirDiscover *m;
public:
  vector<CDentry*> trace;
  C_MDC_ExportDirDiscover(Migrator *mig_, MExportDirDiscover *m_) :
	mig(mig_), m(m_) {}
  void finish(int r) {
    CInode *in = 0;
    if (r >= 0) in = trace[trace.size()-1]->get_inode();
    mig->handle_export_dir_discover_2(m, in, r);
  }
};  

void Migrator::handle_export_dir_discover(MExportDirDiscover *m)
{
  assert(m->get_source().num() != mds->get_nodeid());

  dout(7) << "handle_export_dir_discover on " << m->get_path() << endl;

  // must discover it!
  C_MDC_ExportDirDiscover *onfinish = new C_MDC_ExportDirDiscover(this, m);
  filepath fpath(m->get_path());
  cache->path_traverse(fpath, onfinish->trace, true,
		       m, new C_MDS_RetryMessage(mds,m),       // on delay/retry
		       MDS_TRAVERSE_DISCOVER,
		       onfinish);  // on completion|error
}

void Migrator::handle_export_dir_discover_2(MExportDirDiscover *m, CInode *in, int r)
{
  // yay!
  if (in) {
    dout(7) << "handle_export_dir_discover_2 has " << *in << endl;
  }

  if (r < 0 || !in->is_dir()) {
    dout(7) << "handle_export_dir_discover_2 failed to discover or not dir " << m->get_path() << ", NAK" << endl;

    assert(0);    // this shouldn't happen if the auth pins his path properly!!!! 

    mds->send_message_mds(new MExportDirDiscoverAck(m->get_ino(), false),
			  m->get_source().num(), MDS_PORT_MIGRATOR);    
    delete m;
    return;
  }
  
  assert(in->is_dir());

  /*
  if (in->is_frozen()) {
    dout(7) << "frozen, waiting." << endl;
    in->add_waiter(CINODE_WAIT_AUTHPINNABLE,
                   new C_MDS_RetryMessage(mds,m));
    return;
  }
  
  // pin auth too, until the import completes.
  in->auth_pin();
  */

  // pin inode in the cache (for now)
  in->get(CInode::PIN_IMPORTING);

  import_state[in->ino()] = IMPORT_DISCOVERED;
  import_peer[in->ino()] = m->get_source().num();

  
  // reply
  dout(7) << " sending export_dir_discover_ack on " << *in << endl;
  mds->send_message_mds(new MExportDirDiscoverAck(in->ino()),
			m->get_source().num(), MDS_PORT_MIGRATOR);
  delete m;
}



void Migrator::handle_export_dir_prep(MExportDirPrep *m)
{
  assert(m->get_source().num() != mds->get_nodeid());

  CInode *diri = cache->get_inode(m->get_ino());
  assert(diri);

  list<Context*> finished;

  // assimilate root dir.
  CDir *dir = diri->dir;
  if (dir) {
    dout(7) << "handle_export_dir_prep on " << *dir << " (had dir)" << endl;

    if (!m->did_assim())
      m->get_dir(diri->ino())->update_dir(dir);
  } else {
    assert(!m->did_assim());

    // open dir i'm importing.
    diri->set_dir( new CDir(diri, mds->mdcache, false) );
    dir = diri->dir;
    m->get_dir(diri->ino())->update_dir(dir);
    
    dout(7) << "handle_export_dir_prep on " << *dir << " (opening dir)" << endl;

    diri->take_waiting(CINODE_WAIT_DIR, finished);
  }
  assert(dir->is_auth() == false);
  
  show_imports();

  // assimilate contents?
  if (!m->did_assim()) {
    dout(7) << "doing assim on " << *dir << endl;
    m->mark_assim();  // only do this the first time!

    // move pin to dir
    diri->put(CInode::PIN_IMPORTING);
    dir->get(CDir::PIN_IMPORTING);  

    // change import state
    import_state[diri->ino()] = IMPORT_PREPPING;
    
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
        CInode *condiri = cache->get_inode( m->get_containing_dirino(in->ino()) );
        assert(condiri && condiri->dir);
		cache->add_inode( in );
        condiri->dir->add_dentry( m->get_dentry(in->ino()), in );
        
        dout(7) << "   added " << *in << endl;
      }
      
      assert( in->get_parent_dir()->ino() == m->get_containing_dirino(in->ino()) );
      
      // dir
      if (m->have_dir(in->ino())) {
        if (in->dir) {
          m->get_dir(in->ino())->update_dir(in->dir);
          dout(7) << " updated " << *in->dir << endl;
        } else {
          in->set_dir( new CDir(in, mds->mdcache, false) );
          m->get_dir(in->ino())->update_dir(in->dir);
          dout(7) << "   added " << *in->dir << endl;
          in->take_waiting(CINODE_WAIT_DIR, finished);
        }
      }
    }

    // open export dirs/bounds?
    assert(import_bound_inos.count(diri->ino()) == 0);
    for (list<inodeno_t>::iterator it = m->get_exports().begin();
         it != m->get_exports().end();
         it++) {
      dout(7) << "  checking dir " << hex << *it << dec << endl;
      CInode *in = cache->get_inode(*it);
      assert(in);
      
      // note bound.
      import_bound_inos[dir->ino()].insert(*it);

      if (!in->dir) {
        dout(7) << "  opening nested export on " << *in << endl;
        cache->open_remote_dir(in,
			       new C_MDS_RetryMessage(mds, m));
      }
    }
  } else {
    dout(7) << " not doing assim on " << *dir << endl;
  }
  

  // verify we have all exports
  int waiting_for = 0;
  for (list<inodeno_t>::iterator it = m->get_exports().begin();
       it != m->get_exports().end();
       it++) {
    inodeno_t ino = *it;
    CInode *in = cache->get_inode(ino);
    assert(in);
    if (in->dir) {
      if (!in->dir->state_test(CDIR_STATE_IMPORTBOUND)) {
        dout(7) << "  pinning import bound " << *in->dir << endl;
        in->dir->get(CDir::PIN_IMPORTBOUND);
        in->dir->state_set(CDIR_STATE_IMPORTBOUND);
	import_bounds[dir].insert(in->dir);
      } else {
        dout(7) << "  already pinned import bound " << *in << endl;
      }
    } else {
      dout(7) << "  waiting for nested export dir on " << *in << endl;
      waiting_for++;
    }
  }

  if (waiting_for) {
    dout(7) << " waiting for " << waiting_for << " nested export dir opens" << endl;
  } else {
    // freeze import region
    // (note: this is a manual freeze.. hack hack hack!)
    dout(7) << " all ready, freezing import region" << endl;

    // then, note that i am an ambiguous auth for this subtree.
    dir->set_dir_auth_pending(mds->get_nodeid());

    // mark import point frozen
    dir->_freeze_tree();
    
    // ok!
    dout(7) << " sending export_dir_prep_ack on " << *dir << endl;
    mds->send_message_mds(new MExportDirPrepAck(dir->ino()),
			  m->get_source().num(), MDS_PORT_MIGRATOR);

    // note new state
    import_state[diri->ino()] = IMPORT_PREPPED;

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
  list<inodeno_t> imported_subdirs;
  list<inodeno_t> exports;
public:
  C_MDS_ImportDirLoggedStart(Migrator *m, CDir *d, int f, 
			     list<inodeno_t>& is, list<inodeno_t>& e) :
    migrator(m), dir(d), from(f) {
    imported_subdirs.swap(is);
    exports.swap(e);
  }
  void finish(int r) {
    migrator->import_dir_logged_start(dir, from, imported_subdirs, exports);
  }
};

void Migrator::handle_export_dir(MExportDir *m)
{
  CInode *diri = cache->get_inode(m->get_ino());
  assert(diri);
  CDir *dir = diri->dir;
  assert(dir);

  int oldauth = m->get_source().num();
  dout(7) << "handle_export_dir importing " << *dir << " from " << oldauth << endl;
  assert(dir->is_auth() == false);

  show_imports();
  
  // start the journal entry
  EImportStart *le = new EImportStart(dir->ino(), m->get_exports());
  le->metablob.add_dir_context(dir);
  
  // update dir_auth, import maps
  cache->import_subtree(dir, import_bounds[dir]);

  // take out my importing pin
  dir->put(CDir::PIN_IMPORTING);

  // add this crap to my cache
  list<inodeno_t> imported_subdirs;
  int num_imported_inodes = 0;

  for (list<bufferlist>::iterator p = m->get_dirstate().begin();
       p != m->get_dirstate().end();
       ++p) {
    num_imported_inodes += 
      decode_import_dir(*p, 
                       oldauth, 
                       dir,                 // import root
                       imported_subdirs,
		       le);
  }
  dout(10) << " " << imported_subdirs.size() << " imported subdirs" << endl;
  dout(10) << " " << m->get_exports().size() << " imported nested exports" << endl;
  
  // remove bound pins
  // include bounds in EImportStart
  for (set<CDir*>::iterator it = import_bounds[dir].begin();
       it != import_bounds[dir].end();
       it++) {
    CDir *bd = *it;

    // remove bound pin
    bd->put(CDir::PIN_IMPORTBOUND);
    bd->state_clear(CDIR_STATE_IMPORTBOUND);
    
    // include bounding dirs in EImportStart
    // (now that the interior metadata is already in the event)
    le->metablob.add_dir(bd, false);
  }

  // adjust popularity
  mds->balancer->add_import(dir);

  dout(7) << "handle_export_dir did " << *dir << endl;

  // log it
  mds->mdlog->submit_entry(le,
			   new C_MDS_ImportDirLoggedStart(this, dir, m->get_source().num(), 
							  imported_subdirs, m->get_exports()));

  // note state
  import_state[dir->ino()] = IMPORT_LOGGINGSTART;

  // some stats
  if (mds->logger) {
    mds->logger->inc("im");
    mds->logger->inc("iim", num_imported_inodes);
    mds->logger->set("nim", cache->imports.size());
  }

  delete m;
}


void Migrator::reverse_import(CDir *dir)
{
  dout(7) << "reverse_import " << *dir << endl;

  assert(0); // implement me.

  // update dir_auth, import maps
  cache->export_subtree(dir, import_bounds[dir], import_peer[dir->ino()]);
  
  // remove bound pins
  for (set<CDir*>::iterator it = import_bounds[dir].begin();
       it != import_bounds[dir].end();
       it++) {
    CDir *bd = *it;
    bd->put(CDir::PIN_IMPORTBOUND);
    bd->state_clear(CDIR_STATE_IMPORTBOUND);
  }

  // ...
}


void Migrator::import_dir_logged_start(CDir *dir, int from,
				       list<inodeno_t> &imported_subdirs,
				       list<inodeno_t> &exports)
{
  dout(7) << "import_dir_logged " << *dir << endl;

  // note state
  import_state[dir->ino()] = IMPORT_ACKING;

  // send notify's etc.
  dout(7) << "sending ack for " << *dir << " to old auth mds" << from << endl;
  mds->send_message_mds(new MExportDirAck(dir->inode->ino()),
			from, MDS_PORT_MIGRATOR);

  show_imports();
}


class C_MDS_ImportDirLoggedFinish : public Context {
  Migrator *migrator;
  CDir *dir;
public:
  C_MDS_ImportDirLoggedFinish(Migrator *m, CDir *d) : migrator(m), dir(d) { }
  void finish(int r) {
    migrator->import_dir_logged_finish(dir);
  }
};

void Migrator::handle_export_dir_finish(MExportDirFinish *m)
{
  CInode *diri = cache->get_inode(m->get_ino());
  CDir *dir = diri->dir;
  assert(dir);

  dout(7) << "handle_export_dir_finish logging import_finish on " << *dir << endl;

  // note state
  import_state[dir->ino()] = IMPORT_LOGGINGFINISH;

  // log finish
  mds->mdlog->submit_entry(new EImportFinish(dir, true),
			   new C_MDS_ImportDirLoggedFinish(this,dir));

  delete m;
}


void Migrator::import_dir_logged_finish(CDir *dir)
{
  dout(7) << "import_dir_logged_finish " << *dir << endl;

  // unfreeze!
  dir->unfreeze_tree();
  
  // clear import state (we're done!)
  import_state.erase(dir->ino());
  import_peer.erase(dir->ino());
  import_bound_inos.erase(dir->ino());
  import_bounds.erase(dir);

  // process delayed expires
  cache->process_delayed_expire(dir);

  // ok now finish contexts
  dout(5) << "finishing any waiters on imported data" << endl;
  dir->finish_waiting(CDIR_WAIT_IMPORTED);

  // log it
  if (mds->logger) {
    mds->logger->set("nex", cache->exports.size());
    mds->logger->set("nim", cache->imports.size());
  }
  show_imports();

  // is it empty?
  if (dir->get_size() == 0 &&
      !dir->inode->is_auth()) {
    // reexport!
    export_empty_import(dir);
  }
}


void Migrator::decode_import_inode(CDentry *dn, bufferlist& bl, int& off, int oldauth)
{  
  CInodeExport istate;
  off = istate._decode(bl, off);
  dout(15) << "got a cinodeexport " << endl;
  
  bool added = false;
  CInode *in = cache->get_inode(istate.get_ino());
  if (!in) {
    in = new CInode(mds->mdcache);
    added = true;
  } else {
    in->set_auth(true);
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
  in->add_replica( oldauth, CINODE_EXPORT_NONCE );
  if (in->is_replica(mds->get_nodeid()))
    in->remove_replica(mds->get_nodeid());
  
  // twiddle locks
  // hard
  if (in->hardlock.get_state() == LOCK_GLOCKR) {
    in->hardlock.gather_set.erase(mds->get_nodeid());
    in->hardlock.gather_set.erase(oldauth);
    if (in->hardlock.gather_set.empty())
      mds->locker->inode_hard_eval(in);
  }

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
  if (!in->filelock.is_stable()) {
    // take me and old auth out of gather set
    in->filelock.gather_set.erase(mds->get_nodeid());
    in->filelock.gather_set.erase(oldauth);
    if (in->filelock.gather_set.empty())  // necessary but not suffient...
      mds->locker->inode_file_eval(in);    
  }
}


int Migrator::decode_import_dir(bufferlist& bl,
			       int oldauth,
			       CDir *import_root,
			       list<inodeno_t>& imported_subdirs,
			       EImportStart *le)
{
  int off = 0;
  
  // set up dir
  CDirExport dstate;
  off = dstate._decode(bl, off);
  
  CInode *diri = cache->get_inode(dstate.get_ino());
  assert(diri);
  CDir *dir = diri->get_or_open_dir(mds->mdcache);
  assert(dir);
  
  dout(7) << "decode_import_dir " << *dir << endl;

  // add to list
  if (dir != import_root)
    imported_subdirs.push_back(dir->ino());

  // assimilate state
  dstate.update_dir( dir );

  // mark  (may already be marked from get_or_open_dir() above)
  if (!dir->is_auth())
    dir->state_set(CDIR_STATE_AUTH);

  // adjust replica list
  //assert(!dir->is_replica(oldauth));    // not true on failed export
  dir->add_replica(oldauth);
  if (dir->is_replica(mds->get_nodeid()))
    dir->remove_replica(mds->get_nodeid());

  // add to journal entry
  if (le) 
    le->metablob.add_dir(dir, true);  // Hmm: false would be okay in some cases

  int num_imported = 0;

  if (dir->is_hashed()) {

    // do nothing; dir is hashed
  } else {
    // take all waiters on this dir
    // NOTE: a pass of imported data is guaranteed to get all of my waiters because
    // a replica's presense in my cache implies/forces it's presense in authority's.
    list<Context*> waiters;
    
    dir->take_waiting(CDIR_WAIT_ANY, waiters);
    for (list<Context*>::iterator it = waiters.begin();
         it != waiters.end();
         it++) 
      import_root->add_waiter(CDIR_WAIT_IMPORTED, *it);
    
    dout(15) << "doing contents" << endl;
    
    // contents
    long nden = dstate.get_nden();

    for (; nden>0; nden--) {
      
      num_imported++;
      
      // dentry
      string dname;
      _decode(dname, bl, off);
      dout(15) << "dname is " << dname << endl;

      CDentry *dn = dir->lookup(dname);
      if (!dn)
        dn = dir->add_dentry(dname);  // null

      // decode state
      dn->decode_import_state(bl, off, oldauth, mds->get_nodeid());

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
        dir->link_inode(dn, ino);
      }
      else if (icode == 'I') {
        // inode
        decode_import_inode(dn, bl, off, oldauth);
      }

      // add dentry to journal entry
      if (le) 
	le->metablob.add_dentry(dn, true);  // Hmm: might we do dn->is_dirty() here instead?  
    }

  }

  dout(7) << "decode_import_dir done " << *dir << endl;
  return num_imported;
}





// authority bystander

void Migrator::handle_export_dir_warning(MExportDirWarning *m)
{
  CInode *diri = cache->get_inode(m->get_ino());
  CDir *dir = 0;
  if (diri) dir = diri->dir;

  if (dir) {
    dout(7) << "handle_export_dir_warning " << m->get_source() 
	    << " -> mds" << m->get_new_dir_auth()
	    << " on " << *dir << endl;
    dir->set_dir_auth_pending(m->get_new_dir_auth());
  } else {
    dout(7) << "handle_export_dir_warning on dir " << m->get_ino() << ", acking" << endl;
  }

  // send the ack
  mds->send_message_mds(new MExportDirWarningAck(m->get_ino()),
			m->get_source().num(), MDS_PORT_MIGRATOR);

  delete m;

  // hack: trim now, to flush out cacheexpire bugs
  cache->trim(0);
}


void Migrator::handle_export_dir_notify(MExportDirNotify *m)
{
  CInode *diri = cache->get_inode(m->get_ino());
  CDir *dir = 0;
  if (diri) dir = diri->dir;

  if (!dir) {
    dout(7) << "handle_export_dir_notify mds" << m->get_old_auth()
	    << " -> mds" << m->get_new_auth()
	    << " on missing dir " << m->get_ino() << endl;
  } else if (m->get_old_auth() == m->get_new_auth()) {
    dout(7) << "handle_export_dir_notify mds" << m->get_old_auth()
	    << " aborted export on "
	    << *dir << endl;
    // clear dir_auth_pending
    dir->set_dir_auth_pending(CDIR_AUTH_UNKNOWN);

    // no ack necessary.
    delete m;
    return;
  } else {
    dout(7) << "handle_export_dir_notify mds" << m->get_old_auth()
	    << " -> mds" << m->get_new_auth()
	    << " on " << *dir << endl;
    
    // update bounds first
    for (list<inodeno_t>::iterator it = m->get_exports().begin();
	 it != m->get_exports().end();
	 it++) {
      CInode *n = cache->get_inode(*it);
      if (!n) continue;
      CDir *ndir = n->dir;
      if (!ndir) continue;
      
      int boundauth = ndir->authority().first;
      dout(7) << "export_dir_notify bound " << *ndir << " was dir_auth " << ndir->get_dir_auth() << " (" << boundauth << ")" << endl;
      if (ndir->get_dir_auth().first == CDIR_AUTH_PARENT) {
	if (boundauth != m->get_new_auth())
	  ndir->set_dir_auth( boundauth );
	else 
	  assert(dir->authority().first == m->get_new_auth());  // apparently we already knew!
      } else {
	if (boundauth == m->get_new_auth())
	  ndir->set_dir_auth( CDIR_AUTH_PARENT );
      }
    }
    
    // update dir_auth
    if (diri->authority().first == m->get_new_auth()) {
      dout(7) << "handle_export_dir_notify on " << *diri << ": inode auth is the same, setting dir_auth -1" << endl;
      dir->set_dir_auth( CDIR_AUTH_PARENT );
      assert(!diri->is_auth());
      assert(!dir->is_auth());
    } else {
      dir->set_dir_auth( m->get_new_auth() );
    }
    assert(dir->authority().first != mds->get_nodeid());
    assert(!dir->is_auth());
    
  // DEBUG: verify subdirs
  /*
  if (g_conf.mds_verify_export_dirauth) {
    
    dout(7) << "handle_export_dir_notify on " << *dir << " checking " << m->num_subdirs() << " subdirs" << endl;
    for (list<inodeno_t>::iterator it = m->subdirs_begin();
	 it != m->subdirs_end();
	 it++) {
      CInode *diri = cache->get_inode(*it);
      if (!diri) continue;  // don't have it, don't care
      if (!diri->dir) continue;
      dout(10) << "handle_export_dir_notify checking subdir " << *diri->dir << " is auth " << diri->dir->get_dir_auth() << endl;
      assert(diri->dir != dir);      // base shouldn't be in subdir list
      if (diri->dir->get_dir_auth() != CDIR_AUTH_PARENT) {
	dout(7) << "*** weird value for dir_auth " << diri->dir->get_dir_auth() << " on " << *diri->dir << ", should have been -1 probably??? ******************" << endl;
	assert(0);  // bad news!
	//dir->set_dir_auth( CDIR_AUTH_PARENT );
      }
      assert(diri->dir->authority() == m->get_new_auth());
    }
  }
  */
  }
  
  // send ack
  mds->send_message_mds(new MExportDirNotifyAck(m->get_ino()),
			m->get_old_auth(), MDS_PORT_MIGRATOR);
  
  delete m;
}








void Migrator::show_imports()
{
  mds->balancer->show_imports();
}
