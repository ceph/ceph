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
#include "messages/MExportDir.h"
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
  case MSG_MDS_EXPORTDIRNOTIFYACK:
    handle_export_dir_notify_ack((MExportDirNotifyAck*)m);
    break;    

    // export 3rd party (inode authority)
  case MSG_MDS_EXPORTDIRWARNING:
    handle_export_dir_warning((MExportDirWarning*)m);
    break;
  case MSG_MDS_EXPORTDIRNOTIFY:
    handle_export_dir_notify((MExportDirNotify*)m);
    break;


    // hashing
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

  int dest = dir->inode->authority();

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
	dout(10) << "state loggingstart|prepping : logging EExportFinish(false)" << endl;
	mds->mdlog->submit_entry(new EExportFinish(dir,false));
	// logger will unfreeze.
	break;

      case EXPORT_EXPORTING:
	dout(10) << "state exporting : logging EExportFinish(false), reversing, and unfreezing" << endl;
	mds->mdlog->submit_entry(new EExportFinish(dir,false));
	reverse_export(dir);
	dir->unfreeze_tree();
	break;

      case EXPORT_LOGGINGFINISH:
	dout(10) << "state loggingfinish : doing nothing, we were successful." << endl;
	break;

      default:
	assert(0);
      }

      export_state.erase(dir);
      export_peer.erase(dir);

      // unpin the path
      vector<CDentry*> trace;
      cache->make_trace(trace, dir->inode);
      cache->path_unpin(trace, 0);

      // wake up any waiters
      mds->queue_finished(export_finish_waiters[dir]);
      export_finish_waiters.erase(dir);
      
      // send pending import_maps?
      mds->mdcache->send_pending_import_maps();

      mds->mdcache->show_imports();
      mds->mdcache->show_cache();
    } else {
      // third party failed.  potential peripheral damage?
      if (p->second == EXPORT_EXPORTING) {
	// yeah, i'm waiting for acks, let's fake theirs.
	if (export_notify_ack_waiting[dir].count(who)) {
	  dout(10) << "faking export_dir_notify_ack from mds" << who
		   << " on " << *dir << " to mds" << export_peer[dir] 
		   << endl;
	  export_notify_ack_waiting[dir].erase(who);
	  if (export_notify_ack_waiting[dir].empty())
	    export_dir_acked(dir);
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
      switch (import_peer[dirino]) {
      case IMPORT_DISCOVERED:

	break;

      case IMPORT_PREPPING:

	break;

      case IMPORT_PREPPED:

	break;

      case IMPORT_LOGGINGSTART:

	break;

      case IMPORT_ACKING:
	// hrm.  make this an ambiguous import, and wait for exporter recovery to disambiguate
	// ...
	break;

      case IMPORT_LOGGINGFINISH:
	// do nothing, exporter is no longer involved.
	break;
      }
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

  EExportStart *le = new EExportStart(dir, dest);
  MExportDirPrep *prep = new MExportDirPrep(dir->inode);

  // include spanning tree for all nested exports.
  // these need to be on the destination _before_ the final export so that
  // dir_auth updates on any nested exports are properly absorbed.
  
  set<inodeno_t> inodes_added;

  // include base dir
  prep->add_dir( new CDirDiscover(dir, dir->add_replica(dest)) );
  le->metablob.add_dir( dir, false );
  
  // also include traces to all nested exports.
  set<CDir*> my_nested;
  cache->find_nested_exports(dir, my_nested);
  for (set<CDir*>::iterator it = my_nested.begin();
       it != my_nested.end();
       it++) {
    CDir *exp = *it;
    
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

  dout(7) << "export_dir_prep_ack " << *dir << ", starting export" << endl;
  
  if (export_state.count(dir) == 0 ||
      export_state[dir] != EXPORT_PREPPING) {
    // export must have aborted.  
    dout(7) << "export must have aborted, unfreezing" << endl;
    dir->unfreeze_tree();
    return;
  }

  // start export.
  export_state[dir] = EXPORT_EXPORTING;
  export_dir_go(dir, m->get_source().num());

  // done
  delete m;
}


void Migrator::export_dir_go(CDir *dir,
			     int dest)
{  
  dout(7) << "export_dir_go " << *dir << " to " << dest << endl;

  show_imports();

  assert(export_bounds.count(dir) == 0);
  assert(export_data.count(dir) == 0);

  // update imports/exports
  CDir *containing_import = cache->get_auth_container(dir);

  if (containing_import == dir) {
    dout(7) << " i'm rexporting a previous import" << endl;
    assert(dir->is_import());
    cache->imports.erase(dir);
    dir->state_clear(CDIR_STATE_IMPORT);
    dir->put(CDir::PIN_IMPORT);                  // unpin, no longer an import
    
    // discard nested exports (that we're handing off
    for (set<CDir*>::iterator p = cache->nested_exports[dir].begin();
         p != cache->nested_exports[dir].end(); ) {
      CDir *nested = *p;
      p++;

      // add to export message
      export_bounds[dir].insert(nested);
      
      // nested beneath our new export *in; remove!
      dout(7) << " export " << *nested << " was nested beneath us; removing from export list(s)" << endl;
      assert(cache->exports.count(nested) == 1);
      cache->nested_exports[dir].erase(nested);
    }
    
  } else {
    dout(7) << " i'm a subdir nested under import " << *containing_import << endl;
    cache->exports.insert(dir);
    cache->nested_exports[containing_import].insert(dir);
    
    dir->state_set(CDIR_STATE_EXPORT);
    dir->get(CDir::PIN_EXPORT);                  // i must keep it pinned
    
    // discard nested exports (that we're handing off)
    for (set<CDir*>::iterator p = cache->nested_exports[containing_import].begin();
         p != cache->nested_exports[containing_import].end(); ) {
      CDir *nested = *p;
      p++;
      if (nested == dir) continue;  // ignore myself
      
      // container of parent; otherwise we get ourselves.
      CDir *containing_export = nested->get_parent_dir();
      while (containing_export && !containing_export->is_export())
        containing_export = containing_export->get_parent_dir();
      if (!containing_export) continue;

      if (containing_export == dir) {
        // nested beneath our new export *in; remove!
        dout(7) << " export " << *nested << " was nested beneath us; removing from nested_exports" << endl;
        cache->nested_exports[containing_import].erase(nested);
        // exports.erase(nested); _walk does this

        // add to msg
	export_bounds[dir].insert(nested);
      } else {
        dout(12) << " export " << *nested << " is under other export " << *containing_export << ", which is unrelated" << endl;
        assert(cache->get_auth_container(containing_export) != containing_import);
      }
    }
  }

  // note new authority (locally)
  if (dir->inode->authority() == dest)
    dir->set_dir_auth( CDIR_AUTH_PARENT );
  else
    dir->set_dir_auth( dest );


  // make list of nodes i expect an export_dir_notify_ack from
  //  (everyone w/ this dir open, but me!)
  assert(export_notify_ack_waiting[dir].empty());
  for (map<int,int>::iterator it = dir->replicas_begin();
       it != dir->replicas_end();
       it++) {
    if (it->first == mds->get_nodeid()) continue;
    export_notify_ack_waiting[dir].insert( it->first );

    // send warning to all but dest
    if (it->first != dest) {
      dout(10) << " sending export_dir_warning to mds" << it->first << endl;
      mds->send_message_mds(new MExportDirWarning( dir->ino() ), it->first, MDS_PORT_MIGRATOR);
    }
  }
  assert(export_notify_ack_waiting[dir].count( dest ));

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

  // add bounds
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
  dir->state_set(CDIR_STATE_PROXY);
  dir->get(CDir::PIN_PROXY);
  export_proxy_dirinos[basedir].push_back(dir->ino());

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
      _encode(it->first, enc_dir);
      
      if (dn->is_dirty()) 
        enc_dir.append("D", 1);  // dirty
      else 
        enc_dir.append("C", 1);  // clean

      version_t dnv = dn->get_version();
      enc_dir.append((char*)&dnv, sizeof(dnv));

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
          assert(in->dir->get_dir_auth() == CDIR_AUTH_PARENT);
          subdirs.push_back(in->dir);  // it's ours, recurse (later)
          
        } else {
          // nested export
          assert(in->dir->get_dir_auth() >= 0);
          dout(7) << " encountered nested export " << *in->dir << " dir_auth " << in->dir->get_dir_auth() << "; removing from exports" << endl;
          assert(cache->exports.count(in->dir) == 1); 
          cache->exports.erase(in->dir);                    // discard nested export   (nested_exports updated above)
          
          in->dir->state_clear(CDIR_STATE_EXPORT);
          in->dir->put(CDir::PIN_EXPORT);
          
          // simplify dir_auth?
          if (in->dir->get_dir_auth() == newauth)
            in->dir->set_dir_auth( CDIR_AUTH_PARENT );
        } 
      }
      
      // add to proxy
      export_proxy_inos[basedir].push_back(in->ino());
      in->state_set(CInode::STATE_PROXY);
      in->get(CInode::PIN_PROXY);
      
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
    migrator->export_dir_finish(dir);
  }
};


/*
 * i should get an export_dir_notify_ack from every mds that had me open, including the new auth (an ack)
 */
void Migrator::handle_export_dir_notify_ack(MExportDirNotifyAck *m)
{
  CInode *diri = cache->get_inode(m->get_ino());
  CDir *dir = diri->dir;
  assert(dir);
  assert(dir->is_frozen_tree_root());  // i'm exporting!

  // remove from waiting list
  int from = m->get_source().num();
  assert(export_notify_ack_waiting[dir].count(from));
  export_notify_ack_waiting[dir].erase(from);

  dout(7) << "handle_export_dir_notify_ack on " << *dir << " from " << from 
	  << ", still need (" << export_notify_ack_waiting[dir] << ")" << endl;
  
  // done?
  if (export_notify_ack_waiting[dir].empty()) {
    export_dir_acked(dir);
  } else {
    dout(7) << "handle_export_dir_notify_ack on " << *dir << " from " << from 
            << ", still waiting for " << export_notify_ack_waiting[dir] << endl;
  }
  
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
  
  // re-import it.
  set<CDir*> bounds;
  bounds.swap(export_bounds[dir]);
  export_bounds.erase(dir);

  // -- adjust dir_auth --
  // base
  CDir *im = dir;
  if (dir->get_inode()->authority() == mds->get_nodeid()) {
    // parent is already me.  was export, adding back to existing import.
    im = mds->mdcache->get_auth_container(dir);
    assert(im);
    mds->mdcache->nested_exports[im].erase(dir);
    mds->mdcache->exports.erase(dir);
    dir->set_dir_auth( CDIR_AUTH_PARENT );     
    dir->state_clear(CDIR_STATE_EXPORT);
    dir->put(CDir::PIN_EXPORT);
  } else {
    // parent isn't me.  new import.
    mds->mdcache->imports.insert(dir);
    dir->set_dir_auth( mds->get_nodeid() );               
    dir->state_set(CDIR_STATE_IMPORT);
    dir->get(CDir::PIN_IMPORT);
  }

  dout(10) << "  base " << *dir << endl;
  if (dir != im)
    dout(10) << "  under " << *im << endl;
  
  // bounds
  for (set<CDir*>::iterator p = bounds.begin();
       p != bounds.end();
       ++p) {
    CDir *bd = *p;
    
    if (bd->get_dir_auth() == mds->get_nodeid()) {
      // still me.  was an import. 
      mds->mdcache->imports.erase(bd);
      bd->set_dir_auth( CDIR_AUTH_PARENT );   
      bd->state_clear(CDIR_STATE_IMPORT);
      bd->put(CDir::PIN_IMPORT);
      // move nested exports.
      for (set<CDir*>::iterator q = mds->mdcache->nested_exports[bd].begin();
	   q != mds->mdcache->nested_exports[bd].end();
	   ++q) 
	mds->mdcache->nested_exports[im].insert(*q);
      mds->mdcache->nested_exports.erase(bd);	
    } else {
      // not me anymore.  now an export.
      mds->mdcache->exports.insert(bd);
      mds->mdcache->nested_exports[im].insert(bd);
      assert(bd->get_dir_auth() != CDIR_AUTH_PARENT);
      bd->set_dir_auth( CDIR_AUTH_UNKNOWN );
      bd->state_set(CDIR_STATE_EXPORT);
      bd->get(CDir::PIN_EXPORT);
    }
    
    dout(10) << "  bound " << *bd << endl;
  }


  // reimport the dirs
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
  clear_export_proxy_pins(dir);

  // some clean up
  export_data.erase(dir);
  export_bounds.erase(dir);
  export_notify_ack_waiting.erase(dir);
}


void Migrator::export_dir_acked(CDir *dir)
{
  dout(7) << "export_dir_acked " << *dir << endl;
  export_notify_ack_waiting.erase(dir);
  
  export_state[dir] = EXPORT_LOGGINGFINISH;
  export_data.erase(dir);
  export_bounds.erase(dir);
  
  // log export completion, then finish (unfreeze, trigger finish context, etc.)
  mds->mdlog->submit_entry(new EExportFinish(dir, true),
			   new C_MDS_ExportFinishLogged(this, dir));
}  


/*
 * once i get all teh notify_acks i can finish
 */
void Migrator::export_dir_finish(CDir *dir)
{
  dout(7) << "export_dir_finish " << *dir << endl;

  if (export_state.count(dir)) {
    // send finish/commit to new auth
    mds->send_message_mds(new MExportDirFinish(dir->ino()), dir->authority(), MDS_PORT_MIGRATOR);

    // remove from exporting list
    export_state.erase(dir);
    export_peer.erase(dir);
  } else {
    dout(7) << "target must have failed, not sending final commit message.  export succeeded anyway." << endl;
  }
    
  // unfreeze
  dout(7) << "export_dir_finish unfreezing" << endl;
  dir->unfreeze_tree();
  
  // unpin path
  dout(7) << "export_dir_finish unpinning path" << endl;
  vector<CDentry*> trace;
  cache->make_trace(trace, dir->inode);
  cache->path_unpin(trace, 0);

  // unpin proxies
  clear_export_proxy_pins(dir);

  // queue finishers
  mds->queue_finished(export_finish_waiters[dir]);
  export_finish_waiters.erase(dir);

  // stats
  if (mds->logger) mds->logger->set("nex", cache->exports.size());

  show_imports();

  // send pending import_maps?
  mds->mdcache->send_pending_import_maps();
}


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

  if (in->is_frozen()) {
    dout(7) << "frozen, waiting." << endl;
    in->add_waiter(CINODE_WAIT_AUTHPINNABLE,
                   new C_MDS_RetryMessage(mds,m));
    return;
  }
  
  // pin inode in the cache (for now)
  in->get(CInode::PIN_IMPORTING);
  
  // pin auth too, until the import completes.
  in->auth_pin();

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

    // auth pin too
    dir->auth_pin();
    diri->auth_unpin();

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

    // open export dirs?
    for (list<inodeno_t>::iterator it = m->get_exports().begin();
         it != m->get_exports().end();
         it++) {
      dout(7) << "  checking dir " << hex << *it << dec << endl;
      CInode *in = cache->get_inode(*it);
      assert(in);
      
      // note bound.
      import_bounds[dir->ino()].insert(*it);

      if (!in->dir) {
        dout(7) << "  opening nested export on " << *in << endl;
        cache->open_remote_dir(in,
			       new C_MDS_RetryMessage(mds, m));

        // pin it!
        in->get(CInode::PIN_OPENINGDIR);
        in->state_set(CInode::STATE_OPENINGDIR);
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
    if (!in) dout(0) << "** missing ino " << hex << ino << dec << endl;
    assert(in);
    if (in->dir) {
      if (!in->dir->state_test(CDIR_STATE_IMPORTINGEXPORT)) {
        dout(7) << "  pinning nested export " << *in->dir << endl;
        in->dir->get(CDir::PIN_IMPORTINGEXPORT);
        in->dir->state_set(CDIR_STATE_IMPORTINGEXPORT);

        if (in->state_test(CInode::STATE_OPENINGDIR)) {
          in->put(CInode::PIN_OPENINGDIR);
          in->state_clear(CInode::STATE_OPENINGDIR);
        }
      } else {
        dout(7) << "  already pinned nested export " << *in << endl;
      }
    } else {
      dout(7) << "  waiting for nested export dir on " << *in << endl;
      waiting_for++;
    }
  }
  if (waiting_for) {
    dout(7) << " waiting for " << waiting_for << " nested export dir opens" << endl;
  } else {
    // ok!
    dout(7) << " all ready, sending export_dir_prep_ack on " << *dir << endl;
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




/* this guy waits for the pre-import discovers on hashed directory dir inodes to finish.
 * if it's the last one on the dir, it reprocessed the import.
 */
/*
class C_MDS_ImportPrediscover : public Context {
public:
  MDS *mds;
  MExportDir *m;
  inodeno_t dir_ino;
  string dentry;
  C_MDS_ImportPrediscover(MDS *mds, MExportDir *m, inodeno_t dir_ino, const string& dentry) {
    this->mds = mds;
    this->m = m;
    this->dir_ino = dir_ino;
    this->dentry = dentry;
  }
  virtual void finish(int r) {
    assert(r == 0);  // should never fail!
    
    m->remove_prediscover(dir_ino, dentry);
    
    if (!m->any_prediscovers()) 
      mds->mdcache->handle_export_dir(m);
  }
};
*/

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
  
  // note new authority (locally)
  CDir *im = dir;
  if (dir->inode->is_auth()) {
    // parent is already me.  was export, adding back to existing import.
    im = mds->mdcache->get_auth_container(dir);
    assert(im);
    mds->mdcache->nested_exports[im].erase(dir);
    mds->mdcache->exports.erase(dir);
    dir->set_dir_auth( CDIR_AUTH_PARENT );     
    dir->state_clear(CDIR_STATE_EXPORT);
    dir->put(CDir::PIN_EXPORT);
  } else {
    // parent isn't me.  new import.
    mds->mdcache->imports.insert(dir);
    dir->set_dir_auth( mds->get_nodeid() );               
    dir->state_set(CDIR_STATE_IMPORT);
    dir->get(CDir::PIN_IMPORT);
  }

  // take out my temp pin
  dir->put(CDir::PIN_IMPORTING);

  // mark import point frozen
  // (note: this is a manual freeze.. hack hack hack!)
  dir->get_inode()->auth_pin();
  dir->state_set(CDIR_STATE_FROZENTREE);

  dout(10) << "  base " << *dir << endl;
  if (dir != im)
    dout(10) << "  under " << *im << endl;

  // bounds
  for (list<inodeno_t>::iterator it = m->get_exports().begin();
       it != m->get_exports().end();
       it++) {
    CInode *bdi = cache->get_inode(*it);
    CDir *bd = bdi->dir;
    
    if (bd->get_dir_auth() == mds->get_nodeid()) {
      // still me.  was an import. 
      assert(bd->is_import());
      mds->mdcache->imports.erase(bd);
      bd->set_dir_auth( CDIR_AUTH_PARENT );   
      bd->state_clear(CDIR_STATE_IMPORT);
      bd->put(CDir::PIN_IMPORT);
      // move nested exports.
      for (set<CDir*>::iterator q = mds->mdcache->nested_exports[bd].begin();
	   q != mds->mdcache->nested_exports[bd].end();
	   ++q) 
	mds->mdcache->nested_exports[im].insert(*q);
      mds->mdcache->nested_exports.erase(bd);	
    } else {
      // not me anymore.  now an export.
      mds->mdcache->exports.insert(bd);
      mds->mdcache->nested_exports[im].insert(bd);
      assert(bd->get_dir_auth() != CDIR_AUTH_PARENT);
      bd->set_dir_auth( CDIR_AUTH_UNKNOWN );
      bd->state_set(CDIR_STATE_EXPORT);
      bd->get(CDir::PIN_EXPORT);
    }

    // mark export point frozenleaf
    bd->get(CDir::PIN_FREEZELEAF);
    bd->state_set(CDIR_STATE_FROZENTREELEAF);
    assert(import_bounds[dir->ino()].count(*it));    // we took note during prep stage

    // remove our pin
    bd->put(CDir::PIN_IMPORTINGEXPORT);
    bd->state_clear(CDIR_STATE_IMPORTINGEXPORT);

    dout(10) << "  bound " << *bd << endl;
  }
 
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


void Migrator::import_dir_logged_start(CDir *dir, int from,
				       list<inodeno_t> &imported_subdirs,
				       list<inodeno_t> &exports)
{
  dout(7) << "import_dir_logged " << *dir << endl;

  // note state
  import_state[dir->ino()] = IMPORT_ACKING;

  // send notify's etc.
  dout(7) << "sending notifyack for " << *dir << " to old auth mds" << from << endl;
  mds->send_message_mds(new MExportDirNotifyAck(dir->inode->ino()),
			from, MDS_PORT_MIGRATOR);
  
  dout(7) << "sending notify to others" << endl;
  for (map<int,int>::iterator it = dir->replicas_begin();
       it != dir->replicas_end();
       it++) {
    assert( it->first != mds->get_nodeid() );
    if ( it->first == from ) continue;  // not to old auth.
    
    MExportDirNotify *notify = new MExportDirNotify(dir->ino(), from, mds->get_nodeid());
    notify->copy_exports(exports);

    if (g_conf.mds_verify_export_dirauth)
      notify->copy_subdirs(imported_subdirs);   // copy subdir list (DEBUG)
    
    mds->send_message_mds(notify, it->first, MDS_PORT_MIGRATOR);
  }
  
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
  assert(dir->is_auth());

  // note state
  import_state[dir->ino()] = IMPORT_LOGGINGFINISH;

  // log
  mds->mdlog->submit_entry(new EImportFinish(dir, true),
			   new C_MDS_ImportDirLoggedFinish(this,dir));
  delete m;
}

void Migrator::import_dir_logged_finish(CDir *dir)
{
  dout(7) << "import_dir_logged_finish " << *dir << endl;

  // un auth pin (other exports can now proceed)
  dir->auth_unpin();  
  
  // unfreeze!
  for (set<inodeno_t>::iterator p = import_bounds[dir->ino()].begin();
       p != import_bounds[dir->ino()].end();
       ++p) {
    CInode *diri = mds->mdcache->get_inode(*p);
    CDir *dir = diri->dir;
    assert(dir->state_test(CDIR_STATE_FROZENTREELEAF));
    dir->put(CDir::PIN_FREEZELEAF);
    dir->state_clear(CDIR_STATE_FROZENTREELEAF);
  }

  dir->unfreeze_tree();
      
  // clear import state (we're done!)
  import_state.erase(dir->ino());
  import_peer.erase(dir->ino());
  import_bounds.erase(dir->ino());

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
      
      char dirty;
      bl.copy(off, 1, &dirty);
      off++;

      version_t dnv;
      bl.copy(off, sizeof(dnv), (char*)&dnv);
      off += sizeof(dnv);
      
      char icode;
      bl.copy(off, 1, &icode);
      off++;
      
      CDentry *dn = dir->lookup(dname);
      if (!dn)
        dn = dir->add_dentry(dname);  // null

      // mark dentry dirty?
      if (dirty == 'D') 
	dn->_mark_dirty();
      
      dn->set_version( dnv );
      dn->set_projected_version( dnv );

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
  // add to warning list
  stray_export_warnings.insert( m->get_ino() );
  
  // did i already see the notify?
  if (stray_export_notifies.count(m->get_ino())) {
    // i did, we're good.
    dout(7) << "handle_export_dir_warning on " << m->get_ino() << ".  already got notify." << endl;
    
    // process the notify
    map<inodeno_t, MExportDirNotify*>::iterator it = stray_export_notifies.find(m->get_ino());
    handle_export_dir_notify(it->second);
    stray_export_notifies.erase(it);
  } else {
    dout(7) << "handle_export_dir_warning on " << m->get_ino() << ".  waiting for notify." << endl;
  }
  
  // done
  delete m;
}


void Migrator::handle_export_dir_notify(MExportDirNotify *m)
{
  CDir *dir = 0;
  CInode *in = cache->get_inode(m->get_ino());
  if (in) dir = in->dir;

  // did i see the warning yet?
  if (!stray_export_warnings.count(m->get_ino())) {
    // wait for it.
    dout(7) << "export_dir_notify on " << m->get_ino() << ", waiting for warning." << endl;
    stray_export_notifies.insert(pair<inodeno_t, MExportDirNotify*>( m->get_ino(), m ));
    return;
  }

  // i did, we're all good.
  dout(7) << "export_dir_notify on " << m->get_ino() << ", already saw warning." << endl;
  
  // update dir_auth!
  if (dir) {
    dout(7) << "export_dir_notify on " << *dir << " new_auth " << m->get_new_auth() << " (old_auth " << m->get_old_auth() << ")" << endl;

    // update bounds first
    for (list<inodeno_t>::iterator it = m->get_exports().begin();
         it != m->get_exports().end();
         it++) {
      CInode *n = cache->get_inode(*it);
      if (!n) continue;
      CDir *ndir = n->dir;
      if (!ndir) continue;

      int boundauth = ndir->authority();
      dout(7) << "export_dir_notify bound " << *ndir << " was dir_auth " << ndir->get_dir_auth() << " (" << boundauth << ")" << endl;
      if (ndir->get_dir_auth() == CDIR_AUTH_PARENT) {
        if (boundauth != m->get_new_auth())
          ndir->set_dir_auth( boundauth );
        else assert(dir->authority() == m->get_new_auth());  // apparently we already knew!
      } else {
        if (boundauth == m->get_new_auth())
          ndir->set_dir_auth( CDIR_AUTH_PARENT );
      }
    }
    
    // update dir_auth
    if (in->authority() == m->get_new_auth()) {
      dout(7) << "handle_export_dir_notify on " << *in << ": inode auth is the same, setting dir_auth -1" << endl;
      dir->set_dir_auth( CDIR_AUTH_PARENT );
      assert(!in->is_auth());
      assert(!dir->is_auth());
    } else {
      dir->set_dir_auth( m->get_new_auth() );
    }
    assert(dir->authority() != mds->get_nodeid());
    assert(!dir->is_auth());
    
    // DEBUG: verify subdirs
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
  }
  
  // send notify ack to old auth
  dout(7) << "handle_export_dir_notify sending ack to old_auth " << m->get_old_auth() << endl;
  mds->send_message_mds(new MExportDirNotifyAck(m->get_ino()),
			m->get_old_auth(), MDS_PORT_MIGRATOR);
  

  // done
  stray_export_warnings.erase( m->get_ino() );
  delete m;
}





// =======================================================================
// HASHING


void Migrator::import_hashed_content(CDir *dir, bufferlist& bl, int nden, int oldauth)
{
  int off = 0;
  
  for (; nden>0; nden--) {
    // dentry
    string dname;
    _decode(dname, bl, off);
    dout(15) << "dname is " << dname << endl;
    
    char icode;
    bl.copy(off, 1, &icode);
    off++;
    
    CDentry *dn = dir->lookup(dname);
    if (!dn)
      dn = dir->add_dentry(dname);  // null
    
    // mark dn dirty _after_ we link the inode (scroll down)
    
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
      
      // fix up subdir export?
      if (dn->inode->dir) {
        assert(dn->inode->dir->state_test(CDIR_STATE_IMPORTINGEXPORT));
        dn->inode->dir->put(CDir::PIN_IMPORTINGEXPORT);
        dn->inode->dir->state_clear(CDIR_STATE_IMPORTINGEXPORT);

        if (dn->inode->dir->is_auth()) {
          // mine.  must have been an import.
          assert(dn->inode->dir->is_import());
          dout(7) << "unimporting subdir now that inode is mine " << *dn->inode->dir << endl;
          dn->inode->dir->set_dir_auth( CDIR_AUTH_PARENT );
          cache->imports.erase(dn->inode->dir);
          dn->inode->dir->put(CDir::PIN_IMPORT);
          dn->inode->dir->state_clear(CDIR_STATE_IMPORT);
          
          // move nested under hashdir
          for (set<CDir*>::iterator it = cache->nested_exports[dn->inode->dir].begin();
               it != cache->nested_exports[dn->inode->dir].end();
               it++) 
            cache->nested_exports[dir].insert(*it);
          cache->nested_exports.erase(dn->inode->dir);

          // now it matches the inode
          dn->inode->dir->set_dir_auth( CDIR_AUTH_PARENT );
        }
        else {
          // not mine.  make it an export.
          dout(7) << "making subdir into export " << *dn->inode->dir << endl;
          dn->inode->dir->get(CDir::PIN_EXPORT);
          dn->inode->dir->state_set(CDIR_STATE_EXPORT);
          cache->exports.insert(dn->inode->dir);
          cache->nested_exports[dir].insert(dn->inode->dir);
          
          if (dn->inode->dir->get_dir_auth() == CDIR_AUTH_PARENT)
            dn->inode->dir->set_dir_auth( oldauth );          // no longer matches inode
          assert(dn->inode->dir->get_dir_auth() >= 0);
        }
      }
    }
    
    // mark dentry dirty?  (only _after_ we link the inode!)
    dn->_mark_dirty(); // fixme
  }
}

/*
 
 notes on interaction of hashing and export/import:

  - dir->is_auth() is completely independent of hashing.  for a hashed dir,
     - all nodes are partially authoritative
     - all nodes dir->is_hashed() == true
     - all nodes dir->inode->dir_is_hashed() == true
     - one node dir->is_auth() == true, the rest == false
  - dir_auth for all subdirs in a hashed dir will (likely?) be explicit.

  - remember simple rule: dir auth follows inode, unless dir_auth is explicit.

  - export_dir_walk and decode_import_dir take care with dir_auth:   (for import/export)
     - on export, -1 is changed to mds->get_nodeid()
     - on import, nothing special, actually.

  - hashed dir files aren't included in export; subdirs are converted to imports 
    or exports as necessary.
  - hashed dir subdirs are discovered on export. this is important
    because dirs are needed to tie together auth hierarchy, for auth to know about
    imports/exports, etc.

  - dir state is maintained on auth.
    - COMPLETE and HASHED are transfered to importers.
    - DIRTY is set everywhere.

  - hashed dir is like an import: hashed dir used for nested_exports map.
    - nested_exports is updated appropriately on auth and replicas.
    - a subtree terminates as a hashed dir, since the hashing explicitly
      redelegates all inodes.  thus export_dir_walk includes hashed dirs, but 
      not their inodes.
*/

// HASH on auth

class C_MDC_HashFreeze : public Context {
public:
  Migrator *mig;
  CDir *dir;
  C_MDC_HashFreeze(Migrator *m, CDir *d) : mig(m), dir(d) {}
  virtual void finish(int r) {
    mig->hash_dir_frozen(dir);
  }
};

class C_MDC_HashComplete : public Context {
public:
  Migrator *mig;
  CDir *dir;
  C_MDC_HashComplete(Migrator *mig, CDir *dir) {
    this->mig = mig;
    this->dir = dir;
  }
  virtual void finish(int r) {
    mig->hash_dir_complete(dir);
  }
};


/** hash_dir(dir)
 * start hashing a directory.
 */
void Migrator::hash_dir(CDir *dir)
{
  dout(-7) << "hash_dir " << *dir << endl;

  assert(!dir->is_hashed());
  assert(dir->is_auth());
  
  if (dir->is_frozen() ||
      dir->is_freezing()) {
    dout(7) << " can't hash, freezing|frozen." << endl;
    return;
  }

  // pin path?
  vector<CDentry*> trace;
  cache->make_trace(trace, dir->inode);
  if (!cache->path_pin(trace, 0, 0)) {
    dout(7) << "hash_dir couldn't pin path, failing." << endl;
    return;
  }

  // ok, go
  dir->state_set(CDIR_STATE_HASHING);
  dir->get(CDir::PIN_HASHING);
  assert(dir->hashed_subset.empty());

  // discover on all mds
  assert(hash_gather.count(dir) == 0);
  for (int i=0; i<mds->get_mds_map()->get_num_mds(); i++) {
    if (i == mds->get_nodeid()) continue;  // except me
    hash_gather[dir].insert(i);
    mds->send_message_mds(new MHashDirDiscover(dir->inode), i, MDS_PORT_MIGRATOR);
  }
  dir->auth_pin();  // pin until discovers are all acked.
  
  // start freeze
  dir->freeze_dir(new C_MDC_HashFreeze(this, dir));

  // make complete
  if (!dir->is_complete()) {
    dout(7) << "hash_dir " << *dir << " not complete, fetching" << endl;
    mds->mdstore->fetch_dir(dir,
                            new C_MDC_HashComplete(this, dir));
  } else
    hash_dir_complete(dir);
}


/*
 * wait for everybody to discover and open the hashing dir
 *  then auth_unpin, to let the freeze happen
 */
void Migrator::handle_hash_dir_discover_ack(MHashDirDiscoverAck *m)
{
  CInode *in = cache->get_inode(m->get_ino());
  assert(in);
  CDir *dir = in->dir;
  assert(dir);
  
  int from = m->get_source().num();
  assert(hash_gather[dir].count(from));
  hash_gather[dir].erase(from);
  
  if (hash_gather[dir].empty()) {
    hash_gather.erase(dir);
    dout(7) << "hash_dir_discover_ack " << *dir << ", releasing auth_pin" << endl;
    dir->auth_unpin();   // unpin to allow freeze to complete
  } else {
    dout(7) << "hash_dir_discover_ack " << *dir << ", still waiting for " << hash_gather[dir] << endl;
  }
  
  delete m;  // done
}



/*
 * once the dir is completely in memory,
 *  mark all migrating inodes dirty (to pin in cache)
 */
void Migrator::hash_dir_complete(CDir *dir)
{
  dout(7) << "hash_dir_complete " << *dir << ", dirtying inodes" << endl;

  assert(!dir->is_hashed());
  assert(dir->is_auth());
  
  // mark dirty to pin in cache
  for (CDir_map_t::iterator it = dir->begin(); 
       it != dir->end(); 
       it++) {
    CInode *in = it->second->inode;
    in->_mark_dirty(); // fixme
  }
  
  if (dir->is_frozen_dir())
    hash_dir_go(dir);
}


/*
 * once the dir is frozen,
 *  make sure it's complete
 *  send the prep messages!
 */
void Migrator::hash_dir_frozen(CDir *dir)
{
  dout(7) << "hash_dir_frozen " << *dir << endl;
  
  assert(!dir->is_hashed());
  assert(dir->is_auth());
  assert(dir->is_frozen_dir());
  
  if (!dir->is_complete()) {
    dout(7) << "hash_dir_frozen !complete, waiting still on " << *dir << endl;
    return;  
  }

  // send prep messages w/ export directories to open
  vector<MHashDirPrep*> msgs(mds->get_mds_map()->get_num_mds());

  // check for subdirs
  for (CDir_map_t::iterator it = dir->begin(); 
       it != dir->end(); 
       it++) {
    CDentry *dn = it->second;
    CInode *in = dn->inode;
    
    if (!in->is_dir()) continue;
    if (!in->dir) continue;
    
    int dentryhashcode = mds->mdcache->hash_dentry( dir->ino(), it->first );
    if (dentryhashcode == mds->get_nodeid()) continue;

    // msg?
    if (msgs[dentryhashcode] == 0) {
      msgs[dentryhashcode] = new MHashDirPrep(dir->ino());
    }
    msgs[dentryhashcode]->add_inode(it->first, in->replicate_to(dentryhashcode));
  }

  // send them!
  assert(hash_gather[dir].empty());
  for (unsigned i=0; i<msgs.size(); i++) {
    if (msgs[i]) {
      mds->send_message_mds(msgs[i], i, MDS_PORT_MIGRATOR);
      hash_gather[dir].insert(i);
    }
  }
  
  if (hash_gather[dir].empty()) {
    // no subdirs!  continue!
    hash_gather.erase(dir);
    hash_dir_go(dir);
  } else {
    // wait!
  }
}

/* 
 * wait for peers to open all subdirs
 */
void Migrator::handle_hash_dir_prep_ack(MHashDirPrepAck *m)
{
  CInode *in = cache->get_inode(m->get_ino());
  assert(in);
  CDir *dir = in->dir;
  assert(dir);

  int from = m->get_source().num();

  assert(hash_gather[dir].count(from) == 1);
  hash_gather[dir].erase(from);

  if (hash_gather[dir].empty()) {
    hash_gather.erase(dir);
    dout(7) << "handle_hash_dir_prep_ack on " << *dir << ", last one" << endl;
    hash_dir_go(dir);
  } else {
    dout(7) << "handle_hash_dir_prep_ack on " << *dir << ", waiting for " << hash_gather[dir] << endl;    
  }

  delete m;
}


/*
 * once the dir is frozen,
 *  make sure it's complete
 *  do the hashing!
 */
void Migrator::hash_dir_go(CDir *dir)
{
  dout(7) << "hash_dir_go " << *dir << endl;
  
  assert(!dir->is_hashed());
  assert(dir->is_auth());
  assert(dir->is_frozen_dir());

  // get messages to other nodes ready
  vector<MHashDir*> msgs(mds->get_mds_map()->get_num_mds());
  for (int i=0; i<mds->get_mds_map()->get_num_mds(); i++) {
    if (i == mds->get_nodeid()) continue;
    msgs[i] = new MHashDir(dir->ino());
  }

  // pick a hash seed.
  dir->inode->inode.hash_seed = 1;//dir->ino();

  // suck up all waiters
  C_Contexts *fin = new C_Contexts;
  list<Context*> waiting;
  dir->take_waiting(CDIR_WAIT_ANY, waiting);    // all dir waiters
  fin->take(waiting);
  
  // get containing import.  might be me.
  CDir *containing_import = cache->get_auth_container(dir);
  assert(containing_import != dir || dir->is_import());  

  // divy up contents
  for (CDir_map_t::iterator it = dir->begin(); 
       it != dir->end(); 
       it++) {
    CDentry *dn = it->second;
    CInode *in = dn->inode;

    int dentryhashcode = mds->mdcache->hash_dentry( dir->ino(), it->first );
    if (dentryhashcode == mds->get_nodeid()) {
      continue;      // still mine!
    }

    bufferlist *bl = msgs[dentryhashcode]->get_state_ptr();
    assert(bl);
    
    // -- dentry
    dout(7) << "hash_dir_go sending to " << dentryhashcode << " dn " << *dn << endl;
    _encode(it->first, *bl);
    
    // null dentry?
    if (dn->is_null()) {
      bl->append("N", 1);  // null dentry
      assert(dn->is_sync());
      continue;
    }

    if (dn->is_remote()) {
      // remote link
      bl->append("L", 1);  // remote link

      inodeno_t ino = dn->get_remote_ino();
      bl->append((char*)&ino, sizeof(ino));
      continue;
    }

    // primary link
    // -- inode
    bl->append("I", 1);    // inode dentry
    
    encode_export_inode(in, *bl, dentryhashcode);  // encode, and (update state for) export
    msgs[dentryhashcode]->inc_nden();
    
    if (dn->is_dirty()) 
      dn->mark_clean();

    // add to proxy
    hash_proxy_inos[dir].push_back(in);
    in->state_set(CInode::STATE_PROXY);
    in->get(CInode::PIN_PROXY);

    // fix up subdirs
    if (in->dir) {
      if (in->dir->is_auth()) {
        // mine.  make it into an import.
        dout(7) << "making subdir into import " << *in->dir << endl;
        in->dir->set_dir_auth( mds->get_nodeid() );
        cache->imports.insert(in->dir);
        in->dir->get(CDir::PIN_IMPORT);
        in->dir->state_set(CDIR_STATE_IMPORT);

        // fix nested bits
        for (set<CDir*>::iterator it = cache->nested_exports[containing_import].begin();
             it != cache->nested_exports[containing_import].end(); ) {
          CDir *ex = *it;  
          it++;
          if (cache->get_auth_container(ex) == in->dir) {
            dout(10) << "moving nested export " << *ex << endl;
            cache->nested_exports[containing_import].erase(ex);
            cache->nested_exports[in->dir].insert(ex);
          }
        }
      }
      else {
        // not mine.
        dout(7) << "un-exporting subdir that's being hashed away " << *in->dir << endl;
        assert(in->dir->is_export());
        in->dir->put(CDir::PIN_EXPORT);
        in->dir->state_clear(CDIR_STATE_EXPORT);
        cache->exports.erase(in->dir);
        cache->nested_exports[containing_import].erase(in->dir);
        if (in->dir->authority() == dentryhashcode)
          in->dir->set_dir_auth( CDIR_AUTH_PARENT );
        else
          in->dir->set_dir_auth( in->dir->authority() );
      }
    }
    
    // waiters
    list<Context*> waiters;
    in->take_waiting(CINODE_WAIT_ANY, waiters);
    fin->take(waiters);
  }

  // dir state
  dir->state_set(CDIR_STATE_HASHED);
  dir->get(CDir::PIN_HASHED);
  cache->hashdirs.insert(dir);
  dir->mark_dirty(dir->pre_dirty()); // fixme
  mds->mdlog->submit_entry(new EString("dirty dir fixme"));

  // inode state
  if (dir->inode->is_auth()) {
    dir->inode->_mark_dirty(); // fixme
    mds->mdlog->submit_entry(new EString("hash dirty fixme"));
  }

  // fix up nested_exports?
  if (containing_import != dir) {
    dout(7) << "moving nested exports under hashed dir" << endl;
    for (set<CDir*>::iterator it = cache->nested_exports[containing_import].begin();
         it != cache->nested_exports[containing_import].end(); ) {
      CDir *ex = *it;
      it++;
      if (cache->get_auth_container(ex) == dir) {
        dout(7) << " moving nested export under hashed dir: " << *ex << endl;
        cache->nested_exports[containing_import].erase(ex);
        cache->nested_exports[dir].insert(ex);
      } else {
        dout(7) << " NOT moving nested export under hashed dir: " << *ex << endl;
      }
    }
  }

  // send hash messages
  assert(hash_gather[dir].empty());
  assert(hash_notify_gather[dir].empty());
  assert(dir->hashed_subset.empty());
  for (int i=0; i<mds->get_mds_map()->get_num_mds(); i++) {
    // all nodes hashed locally..
    dir->hashed_subset.insert(i);

    if (i == mds->get_nodeid()) continue;

    // init hash_gather and hash_notify_gather sets
    hash_gather[dir].insert(i);
    
    assert(hash_notify_gather[dir][i].empty());
    for (int j=0; j<mds->get_mds_map()->get_num_mds(); j++) {
      if (j == mds->get_nodeid()) continue;
      if (j == i) continue;
      hash_notify_gather[dir][i].insert(j);
    }

    mds->send_message_mds(msgs[i], i, MDS_PORT_MIGRATOR);
  }

  // wait for all the acks.
}


void Migrator::handle_hash_dir_ack(MHashDirAck *m)
{
  CInode *in = cache->get_inode(m->get_ino());
  assert(in);
  CDir *dir = in->dir;
  assert(dir);

  assert(dir->is_hashed());
  assert(dir->is_hashing());

  int from = m->get_source().num();
  assert(hash_gather[dir].count(from) == 1);
  hash_gather[dir].erase(from);
  
  if (hash_gather[dir].empty()) {
    dout(7) << "handle_hash_dir_ack on " << *dir << ", last one" << endl;

    if (hash_notify_gather[dir].empty()) {
      dout(7) << "got notifies too, all done" << endl;
      hash_dir_finish(dir);
    } else {
      dout(7) << "waiting on notifies " << endl;
    }

  } else {
    dout(7) << "handle_hash_dir_ack on " << *dir << ", waiting for " << hash_gather[dir] << endl;    
  }

  delete m;
}


void Migrator::hash_dir_finish(CDir *dir)
{
  dout(7) << "hash_dir_finish finishing " << *dir << endl;
  assert(dir->is_hashed());
  assert(dir->is_hashing());
  
  // dir state
  hash_gather.erase(dir);
  dir->state_clear(CDIR_STATE_HASHING);
  dir->put(CDir::PIN_HASHING);
  dir->hashed_subset.clear();

  // unproxy inodes
  //  this _could_ happen sooner, on a per-peer basis, but no harm in waiting a few more seconds.
  for (list<CInode*>::iterator it = hash_proxy_inos[dir].begin();
       it != hash_proxy_inos[dir].end();
       it++) {
    CInode *in = *it;
    assert(in->state_test(CInode::STATE_PROXY));
    in->state_clear(CInode::STATE_PROXY);
    in->put(CInode::PIN_PROXY);
  }
  hash_proxy_inos.erase(dir);

  // unpin path
  vector<CDentry*> trace;
  cache->make_trace(trace, dir->inode);
  cache->path_unpin(trace, 0);

  // unfreeze
  dir->unfreeze_dir();

  show_imports();
  assert(hash_gather.count(dir) == 0);

  // stats
  //if (mds->logger) mds->logger->inc("nh", 1);

}




// HASH on auth and non-auth

void Migrator::handle_hash_dir_notify(MHashDirNotify *m)
{
  CInode *in = cache->get_inode(m->get_ino());
  assert(in);
  CDir *dir = in->dir;
  assert(dir);
  assert(dir->is_hashing());

  dout(5) << "handle_hash_dir_notify " << *dir << endl;
  int from = m->get_from();

  int source = m->get_source().num();
  if (dir->is_auth()) {
    // gather notifies
    assert(dir->is_hashed());
    
    assert(    hash_notify_gather[dir][from].count(source) );
    hash_notify_gather[dir][from].erase(source);
    
    if (hash_notify_gather[dir][from].empty()) {
      dout(7) << "last notify from " << from << endl;
      hash_notify_gather[dir].erase(from);

      if (hash_notify_gather[dir].empty()) {
        dout(7) << "last notify!" << endl;
        hash_notify_gather.erase(dir);
        
        if (hash_gather[dir].empty()) {
          dout(7) << "got acks too, all done" << endl;
          hash_dir_finish(dir);
        } else {
          dout(7) << "still waiting on acks from " << hash_gather[dir] << endl;
        }
      } else {
        dout(7) << "still waiting for notify gathers from " << hash_notify_gather[dir].size() << " others" << endl;
      }
    } else {
      dout(7) << "still waiting for notifies from " << from << " via " << hash_notify_gather[dir][from] << endl;
    }

    // delete msg
    delete m;
  } else {
    // update dir hashed_subset 
    assert(dir->hashed_subset.count(from) == 0);
    dir->hashed_subset.insert(from);
    
    // update open subdirs
    for (CDir_map_t::iterator it = dir->begin(); 
         it != dir->end(); 
         it++) {
      CDentry *dn = it->second;
      CInode *in = dn->get_inode();
      if (!in) continue;
      if (!in->dir) continue;
      
      int dentryhashcode = mds->mdcache->hash_dentry( dir->ino(), it->first );
      if (dentryhashcode != from) continue;   // we'll import these in a minute
      
      if (in->dir->authority() != dentryhashcode)
        in->dir->set_dir_auth( in->dir->authority() );
      else
        in->dir->set_dir_auth( CDIR_AUTH_PARENT );
    }
    
    // remove from notify gather set
    assert(hash_gather[dir].count(from));
    hash_gather[dir].erase(from);

    // last notify?
    if (hash_gather[dir].empty()) {
      dout(7) << "gathered all the notifies, finishing hash of " << *dir << endl;
      hash_gather.erase(dir);
      
      dir->state_clear(CDIR_STATE_HASHING);
      dir->put(CDir::PIN_HASHING);
      dir->hashed_subset.clear();
    } else {
      dout(7) << "still waiting for notify from " << hash_gather[dir] << endl;
    }

    // fw notify to auth
    mds->send_message_mds(m, dir->authority(), MDS_PORT_MIGRATOR);
  }
}




// HASH on non-auth

/*
 * discover step:
 *  each peer needs to open up the directory and pin it before we start
 */
class C_MDC_HashDirDiscover : public Context {
  Migrator *mig;
  MHashDirDiscover *m;
public:
  vector<CDentry*> trace;
  C_MDC_HashDirDiscover(Migrator *mig, MHashDirDiscover *m) {
    this->mig = mig;
    this->m = m;
  }
  void finish(int r) {
    CInode *in = 0;
    if (r >= 0) {
      if (trace.size())
        in = trace[trace.size()-1]->get_inode();
      else
        in = mig->cache->get_root();
    }
    mig->handle_hash_dir_discover_2(m, in, r);
  }
};  

void Migrator::handle_hash_dir_discover(MHashDirDiscover *m)
{
  assert(m->get_source().num() != mds->get_nodeid());

  dout(7) << "handle_hash_dir_discover on " << m->get_path() << endl;

  // must discover it!
  C_MDC_HashDirDiscover *onfinish = new C_MDC_HashDirDiscover(this, m);
  filepath fpath(m->get_path());
  cache->path_traverse(fpath, onfinish->trace, true,
		       m, new C_MDS_RetryMessage(mds,m),       // on delay/retry
		       MDS_TRAVERSE_DISCOVER,
		       onfinish);  // on completion|error
}

void Migrator::handle_hash_dir_discover_2(MHashDirDiscover *m, CInode *in, int r)
{
  // yay!
  if (in) {
    dout(7) << "handle_hash_dir_discover_2 has " << *in << endl;
  }

  if (r < 0 || !in->is_dir()) {
    dout(7) << "handle_hash_dir_discover_2 failed to discover or not dir " << m->get_path() << ", NAK" << endl;
    assert(0);    // this shouldn't happen if the auth pins his path properly!!!! 
  }
  assert(in->is_dir());

  // is dir open?
  if (!in->dir) {
    dout(7) << "handle_hash_dir_discover_2 opening dir " << *in << endl;
    cache->open_remote_dir(in,
			   new C_MDS_RetryMessage(mds, m));
    return;
  }
  CDir *dir = in->dir;

  // pin dir, set hashing flag
  dir->state_set(CDIR_STATE_HASHING);
  dir->get(CDir::PIN_HASHING);
  assert(dir->hashed_subset.empty());
  
  // inode state
  dir->inode->inode.hash_seed = 1;// dir->ino();
  if (dir->inode->is_auth()) {
    dir->inode->_mark_dirty(); // fixme
    mds->mdlog->submit_entry(new EString("hash dirty fixme"));
  }

  // get gather set ready for notifies
  assert(hash_gather[dir].empty());
  for (int i=0; i<mds->get_mds_map()->get_num_mds(); i++) {
    if (i == mds->get_nodeid()) continue;
    if (i == dir->authority()) continue;
    hash_gather[dir].insert(i);
  }

  // reply
  dout(7) << " sending hash_dir_discover_ack on " << *dir << endl;
  mds->send_message_mds(new MHashDirDiscoverAck(dir->ino()),
			m->get_source().num(), MDS_PORT_MIGRATOR);
  delete m;
}

/*
 * prep step:
 *  peers need to open up all subdirs of the hashed dir
 */

void Migrator::handle_hash_dir_prep(MHashDirPrep *m)
{
  CInode *in = cache->get_inode(m->get_ino());
  assert(in);
  CDir *dir = in->dir;
  assert(dir);
  
  dout(7) << "handle_hash_dir_prep " << *dir << endl;

  if (!m->did_assim()) {
    m->mark_assim();  // only do this the first time!

    // assimilate dentry+inodes for exports
    for (map<string,CInodeDiscover*>::iterator it = m->get_inodes().begin();
         it != m->get_inodes().end();
         it++) {
      CInode *in = cache->get_inode( it->second->get_ino() );
      if (in) {
        it->second->update_inode(in);
        dout(5) << " updated " << *in << endl;
      } else {
        in = new CInode(mds->mdcache, false);
        it->second->update_inode(in);
        cache->add_inode(in);
        
        // link 
        dir->add_dentry( it->first, in );
        dout(5) << "   added " << *in << endl;
      }

      // open!
      if (!in->dir) {
        dout(5) << "  opening nested export on " << *in << endl;
        cache->open_remote_dir(in,
			       new C_MDS_RetryMessage(mds, m));
      }
    }
  }

  // verify!
  int waiting_for = 0;
  for (map<string,CInodeDiscover*>::iterator it = m->get_inodes().begin();
       it != m->get_inodes().end();
       it++) {
    CInode *in = cache->get_inode( it->second->get_ino() );
    assert(in);

    if (in->dir) {
      if (!in->dir->state_test(CDIR_STATE_IMPORTINGEXPORT)) {
        dout(5) << "  pinning nested export " << *in->dir << endl;
        in->dir->get(CDir::PIN_IMPORTINGEXPORT);
        in->dir->state_set(CDIR_STATE_IMPORTINGEXPORT);
      } else {
        dout(5) << "  already pinned nested export " << *in << endl;
      }
    } else {
      dout(5) << "  waiting for nested export dir on " << *in << endl;
      waiting_for++;
    }
  }

  if (waiting_for) {
    dout(5) << "waiting for " << waiting_for << " dirs to open" << endl;
    return;
  } 

  // ack!
  mds->send_message_mds(new MHashDirPrepAck(dir->ino()),
			m->get_source().num(), MDS_PORT_MIGRATOR);
  
  // done.
  delete m;
}


/*
 * hash step:
 */

void Migrator::handle_hash_dir(MHashDir *m)
{
  CInode *in = cache->get_inode(m->get_ino());
  assert(in);
  CDir *dir = in->dir;
  assert(dir);
  assert(!dir->is_auth());
  assert(!dir->is_hashed());
  assert(dir->is_hashing());

  dout(5) << "handle_hash_dir " << *dir << endl;
  int oldauth = m->get_source().num();

  // content
  import_hashed_content(dir, m->get_state(), m->get_nden(), oldauth);

  // dir state
  dir->state_set(CDIR_STATE_HASHED);
  dir->get(CDir::PIN_HASHED);
  cache->hashdirs.insert(dir);
  dir->hashed_subset.insert(mds->get_nodeid());

  // dir is complete
  dir->mark_complete();
  dir->mark_dirty(dir->pre_dirty()); // fixme
  mds->mdlog->submit_entry(new EString("dirty dir fixme"));

  // commit
  mds->mdstore->commit_dir(dir, 0);
  
  // send notifies
  dout(7) << "sending notifies" << endl;
  for (int i=0; i<mds->get_mds_map()->get_num_mds(); i++) {
    if (i == mds->get_nodeid()) continue;
    if (i == m->get_source().num()) continue;
    mds->send_message_mds(new MHashDirNotify(dir->ino(), mds->get_nodeid()),
			  i, MDS_PORT_MIGRATOR);
  }

  // ack
  dout(7) << "acking" << endl;
  mds->send_message_mds(new MHashDirAck(dir->ino()),
			m->get_source().num(), MDS_PORT_MIGRATOR);
  
  // done.
  delete m;

  show_imports();
}





// UNHASH on auth

class C_MDC_UnhashFreeze : public Context {
public:
  Migrator *mig;
  CDir *dir;
  C_MDC_UnhashFreeze(Migrator *m, CDir *d)  : mig(m), dir(d) {}
  virtual void finish(int r) {
    mig->unhash_dir_frozen(dir);
  }
};

class C_MDC_UnhashComplete : public Context {
public:
  Migrator *mig;
  CDir *dir;
  C_MDC_UnhashComplete(Migrator *m, CDir *d) : mig(m), dir(d) {}
  virtual void finish(int r) {
    mig->unhash_dir_complete(dir);
  }
};


void Migrator::unhash_dir(CDir *dir)
{
  dout(-7) << "unhash_dir " << *dir << endl;

  assert(dir->is_hashed());
  assert(!dir->is_unhashing());
  assert(dir->is_auth());
  assert(hash_gather.count(dir)==0);

  // pin path?
  vector<CDentry*> trace;
  cache->make_trace(trace, dir->inode);
  if (!cache->path_pin(trace, 0, 0)) {
    dout(7) << "unhash_dir couldn't pin path, failing." << endl;
    return;
  }

  // twiddle state
  dir->state_set(CDIR_STATE_UNHASHING);

  // first, freeze the dir.
  dir->freeze_dir(new C_MDC_UnhashFreeze(this, dir));

  // make complete
  if (!dir->is_complete()) {
    dout(7) << "unhash_dir " << *dir << " not complete, fetching" << endl;
    mds->mdstore->fetch_dir(dir,
                            new C_MDC_UnhashComplete(this, dir));
  } else
    unhash_dir_complete(dir);

}

void Migrator::unhash_dir_frozen(CDir *dir)
{
  dout(7) << "unhash_dir_frozen " << *dir << endl;
  
  assert(dir->is_hashed());
  assert(dir->is_auth());
  assert(dir->is_frozen_dir());
  
  if (!dir->is_complete()) {
    dout(7) << "unhash_dir_frozen !complete, waiting still on " << *dir << endl;
  } else
    unhash_dir_prep(dir);
}


/*
 * ask peers to freeze and complete hashed dir
 */
void Migrator::unhash_dir_prep(CDir *dir)
{
  dout(7) << "unhash_dir_prep " << *dir << endl;
  assert(dir->is_hashed());
  assert(dir->is_auth());
  assert(dir->is_frozen_dir());
  assert(dir->is_complete());

  if (!hash_gather[dir].empty()) return;  // already been here..freeze must have been instantaneous

  // send unhash prep to all peers
  assert(hash_gather[dir].empty());
  for (int i=0; i<mds->get_mds_map()->get_num_mds(); i++) {
    if (i == mds->get_nodeid()) continue;
    hash_gather[dir].insert(i);
    mds->send_message_mds(new MUnhashDirPrep(dir->ino()),
			  i, MDS_PORT_MIGRATOR);
  }
}

/* 
 * wait for peers to freeze and complete hashed dirs
 */
void Migrator::handle_unhash_dir_prep_ack(MUnhashDirPrepAck *m)
{
  CInode *in = cache->get_inode(m->get_ino());
  assert(in);
  CDir *dir = in->dir;
  assert(dir);
  
  int from = m->get_source().num();
  dout(7) << "handle_unhash_dir_prep_ack from " << from << " " << *dir << endl;

  if (!m->did_assim()) {
    m->mark_assim();  // only do this the first time!
    
    // assimilate dentry+inodes for exports
    for (map<string,CInodeDiscover*>::iterator it = m->get_inodes().begin();
         it != m->get_inodes().end();
         it++) {
      CInode *in = cache->get_inode( it->second->get_ino() );
      if (in) {
        it->second->update_inode(in);
        dout(5) << " updated " << *in << endl;
      } else {
        in = new CInode(mds->mdcache, false);
        it->second->update_inode(in);
        cache->add_inode(in);
        
        // link 
        dir->add_dentry( it->first, in );
        dout(5) << "   added " << *in << endl;
      }
      
      // open!
      if (!in->dir) {
        dout(5) << "  opening nested export on " << *in << endl;
        cache->open_remote_dir(in,
			       new C_MDS_RetryMessage(mds, m));
      }
    }
  }
  
  // verify!
  int waiting_for = 0;
  for (map<string,CInodeDiscover*>::iterator it = m->get_inodes().begin();
       it != m->get_inodes().end();
       it++) {
    CInode *in = cache->get_inode( it->second->get_ino() );
    assert(in);
    
    if (in->dir) {
      if (!in->dir->state_test(CDIR_STATE_IMPORTINGEXPORT)) {
        dout(5) << "  pinning nested export " << *in->dir << endl;
        in->dir->get(CDir::PIN_IMPORTINGEXPORT);
        in->dir->state_set(CDIR_STATE_IMPORTINGEXPORT);
      } else {
        dout(5) << "  already pinned nested export " << *in << endl;
      }
    } else {
      dout(5) << "  waiting for nested export dir on " << *in << endl;
      waiting_for++;
    }
  }
  
  if (waiting_for) {
    dout(5) << "waiting for " << waiting_for << " dirs to open" << endl;
    return;
  } 
  
  // ok, done with this PrepAck
  assert(hash_gather[dir].count(from) == 1);
  hash_gather[dir].erase(from);
  
  if (hash_gather[dir].empty()) {
    hash_gather.erase(dir);
    dout(7) << "handle_unhash_dir_prep_ack on " << *dir << ", last one" << endl;
    unhash_dir_go(dir);
  } else {
    dout(7) << "handle_unhash_dir_prep_ack on " << *dir << ", waiting for " << hash_gather[dir] << endl;    
  }
  
  delete m;
}


/*
 * auth:
 *  send out MHashDir's to peers
 */
void Migrator::unhash_dir_go(CDir *dir)
{
  dout(7) << "unhash_dir_go " << *dir << endl;
  assert(dir->is_hashed());
  assert(dir->is_auth());
  assert(dir->is_frozen_dir());
  assert(dir->is_complete());

  // send unhash prep to all peers
  assert(hash_gather[dir].empty());
  for (int i=0; i<mds->get_mds_map()->get_num_mds(); i++) {
    if (i == mds->get_nodeid()) continue;
    hash_gather[dir].insert(i);
    mds->send_message_mds(new MUnhashDir(dir->ino()),
			  i, MDS_PORT_MIGRATOR);
  }
}

/*
 * auth:
 *  assimilate unhashing content
 */
void Migrator::handle_unhash_dir_ack(MUnhashDirAck *m)
{
  CInode *in = cache->get_inode(m->get_ino());
  assert(in);
  CDir *dir = in->dir;
  assert(dir);
  
  dout(7) << "handle_unhash_dir_ack " << *dir << endl;
  assert(dir->is_hashed());

  // assimilate content
  int from = m->get_source().num();
  import_hashed_content(dir, m->get_state(), m->get_nden(), from);
  delete m;

  // done?
  assert(hash_gather[dir].count(from));
  hash_gather[dir].erase(from);
  
  if (!hash_gather[dir].empty()) {
    dout(7) << "still waiting for unhash acks from " << hash_gather[dir] << endl;
    return;
  } 

  // done!
  
  // fix up nested_exports
  CDir *containing_import = cache->get_auth_container(dir);
  if (containing_import != dir) {
    for (set<CDir*>::iterator it = cache->nested_exports[dir].begin();
         it != cache->nested_exports[dir].end();
         it++) {
      dout(7) << "moving nested export out from under hashed dir : " << **it << endl;
      cache->nested_exports[containing_import].insert(*it);
    }
    cache->nested_exports.erase(dir);
  }
  
  // dir state
  //dir->state_clear(CDIR_STATE_UNHASHING); //later
  dir->state_clear(CDIR_STATE_HASHED);
  dir->put(CDir::PIN_HASHED);
  cache->hashdirs.erase(dir);
  
  // commit!
  assert(dir->is_complete());
  //dir->mark_complete();
  dir->mark_dirty(dir->pre_dirty()); // fixme
  mds->mdstore->commit_dir(dir, 0);

  // inode state
  dir->inode->inode.hash_seed = 0;
  if (dir->inode->is_auth()) {
    dir->inode->_mark_dirty(); // fixme
    mds->mdlog->submit_entry(new EString("hash inode dirty fixme"));
  }
  
  // notify
  assert(hash_gather[dir].empty());
  for (int i=0; i<mds->get_mds_map()->get_num_mds(); i++) {
    if (i == mds->get_nodeid()) continue;

    hash_gather[dir].insert(i);
    
    mds->send_message_mds(new MUnhashDirNotify(dir->ino()),
			  i, MDS_PORT_MIGRATOR);
  }
}


/*
 * sent by peer to flush mds links.  unfreeze when all gathered.
 */
void Migrator::handle_unhash_dir_notify_ack(MUnhashDirNotifyAck *m)
{
  CInode *in = cache->get_inode(m->get_ino());
  assert(in);
  CDir *dir = in->dir;
  assert(dir);
  
  dout(7) << "handle_unhash_dir_ack " << *dir << endl;
  assert(!dir->is_hashed());
  assert(dir->is_unhashing());
  assert(dir->is_frozen_dir());

  // done?
  int from = m->get_source().num();
  assert(hash_gather[dir].count(from));
  hash_gather[dir].erase(from);
  delete m;

  if (!hash_gather[dir].empty()) {
    dout(7) << "still waiting for notifyack from " << hash_gather[dir] << " on " << *dir << endl;
  } else {
    unhash_dir_finish(dir);
  }  
}


/*
 * all mds links are flushed.  unfreeze dir!
 */
void Migrator::unhash_dir_finish(CDir *dir)
{
  dout(7) << "unhash_dir_finish " << *dir << endl;
  hash_gather.erase(dir);

  // unpin path
  vector<CDentry*> trace;
  cache->make_trace(trace, dir->inode);
  cache->path_unpin(trace, 0);

  // state
  dir->state_clear(CDIR_STATE_UNHASHING);

  // unfreeze
  dir->unfreeze_dir();

}



// UNHASH on all

/*
 * hashed dir is complete.  
 *  mark all migrating inodes dirty (to pin in cache)
 *  if frozen too, then go to next step (depending on auth)
 */
void Migrator::unhash_dir_complete(CDir *dir)
{
  dout(7) << "unhash_dir_complete " << *dir << ", dirtying inodes" << endl;
  
  assert(dir->is_hashed());
  assert(dir->is_complete());
  
  // mark dirty to pin in cache
  for (CDir_map_t::iterator it = dir->begin(); 
       it != dir->end(); 
       it++) {
    CInode *in = it->second->inode;
    if (in->is_auth()) {
      in->_mark_dirty(); // fixme
      mds->mdlog->submit_entry(new EString("unhash dirty fixme"));
    }
  }
  
  if (!dir->is_frozen_dir()) {
    dout(7) << "dir complete but !frozen, waiting " << *dir << endl;
  } else {
    if (dir->is_auth())
      unhash_dir_prep(dir);            // auth
    else
      unhash_dir_prep_finish(dir);  // nonauth
  }
}


// UNHASH on non-auth

class C_MDC_UnhashPrepFreeze : public Context {
public:
  Migrator *mig;
  CDir *dir;
  C_MDC_UnhashPrepFreeze(Migrator *m, CDir *d) : mig(m), dir(d) {}
  virtual void finish(int r) {
    mig->unhash_dir_prep_frozen(dir);
  }
};


/*
 * peers need to freeze their dir and make them complete
 */
void Migrator::handle_unhash_dir_prep(MUnhashDirPrep *m)
{
  CInode *in = cache->get_inode(m->get_ino());
  assert(in);
  CDir *dir = in->dir;
  assert(dir);
  
  dout(7) << "handle_unhash_dir_prep " << *dir << endl;
  assert(dir->is_hashed());

  // freeze
  dir->freeze_dir(new C_MDC_UnhashPrepFreeze(this, dir));

  // make complete
  if (!dir->is_complete()) {
    dout(7) << "unhash_dir " << *dir << " not complete, fetching" << endl;
    mds->mdstore->fetch_dir(dir,
                            new C_MDC_UnhashComplete(this, dir));
  } else {
    unhash_dir_complete(dir);
  }
  
  delete m;
}

/*
 * peer has hashed dir frozen.  
 *  complete too?
 */
void Migrator::unhash_dir_prep_frozen(CDir *dir)
{
  dout(7) << "unhash_dir_prep_frozen " << *dir << endl;
  
  assert(dir->is_hashed());
  assert(dir->is_frozen_dir());
  assert(!dir->is_auth());
  
  if (!dir->is_complete()) {
    dout(7) << "unhash_dir_prep_frozen !complete, waiting still on " << *dir << endl;
  } else
    unhash_dir_prep_finish(dir);
}

/*
 * peer has hashed dir complete and frozen.  ack.
 */
void Migrator::unhash_dir_prep_finish(CDir *dir)
{
  dout(7) << "unhash_dir_prep_finish " << *dir << endl;
  assert(dir->is_hashed());
  assert(!dir->is_auth());
  assert(dir->is_frozen());
  assert(dir->is_complete());
  
  // twiddle state
  if (dir->is_unhashing())
    return;  // already replied.
  dir->state_set(CDIR_STATE_UNHASHING);

  // send subdirs back to auth
  MUnhashDirPrepAck *ack = new MUnhashDirPrepAck(dir->ino());
  int auth = dir->authority();
  
  for (CDir_map_t::iterator it = dir->begin(); 
       it != dir->end(); 
       it++) {
    CDentry *dn = it->second;
    CInode *in = dn->inode;
    
    if (!in->is_dir()) continue;
    if (!in->dir) continue;
    
    int dentryhashcode = mds->mdcache->hash_dentry( dir->ino(), it->first );
    if (dentryhashcode != mds->get_nodeid()) continue;
    
    // msg?
    ack->add_inode(it->first, in->replicate_to(auth));
  }
  
  // ack
  mds->send_message_mds(ack, auth, MDS_PORT_MIGRATOR);
}



/*
 * peer needs to send hashed dir content back to auth.
 *  unhash dir.
 */
void Migrator::handle_unhash_dir(MUnhashDir *m)
{
  CInode *in = cache->get_inode(m->get_ino());
  assert(in);
  CDir *dir = in->dir;
  assert(dir);
  
  dout(7) << "handle_unhash_dir " << *dir << endl;//" .. hash_seed is " << dir->inode->inode.hash_seed << endl;
  assert(dir->is_hashed());
  assert(dir->is_unhashing());
  assert(!dir->is_auth());
  
  // get message ready
  bufferlist bl;
  int nden = 0;

  // suck up all waiters
  C_Contexts *fin = new C_Contexts;
  list<Context*> waiting;
  dir->take_waiting(CDIR_WAIT_ANY, waiting);    // all dir waiters
  fin->take(waiting);
  
  // divy up contents
  for (CDir_map_t::iterator it = dir->begin(); 
       it != dir->end(); 
       it++) {
    CDentry *dn = it->second;
    CInode *in = dn->inode;

    int dentryhashcode = mds->mdcache->hash_dentry( dir->ino(), it->first );
    if (dentryhashcode != mds->get_nodeid()) {
      // not mine!
      // twiddle dir_auth?
      if (in->dir) {
        if (in->dir->authority() != dir->authority())
          in->dir->set_dir_auth( in->dir->authority() );
        else
          in->dir->set_dir_auth( CDIR_AUTH_PARENT );
      }
      continue;
    }
    
    // -- dentry
    dout(7) << "unhash_dir_go sending to " << dentryhashcode << " dn " << *dn << endl;
    _encode(it->first, bl);
    
    // null dentry?
    if (dn->is_null()) {
      bl.append("N", 1);  // null dentry
      assert(dn->is_sync());
      continue;
    }

    if (dn->is_remote()) {
      // remote link
      bl.append("L", 1);  // remote link

      inodeno_t ino = dn->get_remote_ino();
      bl.append((char*)&ino, sizeof(ino));
      continue;
    }

    // primary link
    // -- inode
    bl.append("I", 1);    // inode dentry
    
    encode_export_inode(in, bl, dentryhashcode);  // encode, and (update state for) export
    nden++;

    if (dn->is_dirty()) 
      dn->mark_clean();

    // proxy
    in->state_set(CInode::STATE_PROXY);
    in->get(CInode::PIN_PROXY);
    hash_proxy_inos[dir].push_back(in);

    if (in->dir) {
      if (in->dir->is_auth()) {
        // mine.  make it into an import.
        dout(7) << "making subdir into import " << *in->dir << endl;
        in->dir->set_dir_auth( mds->get_nodeid() );
        cache->imports.insert(in->dir);
        in->dir->get(CDir::PIN_IMPORT);
        in->dir->state_set(CDIR_STATE_IMPORT);
      }
      else {
        // not mine.
        dout(7) << "un-exporting subdir that's being unhashed away " << *in->dir << endl;
        assert(in->dir->is_export());
        in->dir->put(CDir::PIN_EXPORT);
        in->dir->state_clear(CDIR_STATE_EXPORT);
        cache->exports.erase(in->dir);
        cache->nested_exports[dir].erase(in->dir);
      }
    }
    
    // waiters
    list<Context*> waiters;
    in->take_waiting(CINODE_WAIT_ANY, waiters);
    fin->take(waiters);
  }

  // we should have no nested exports; we're not auth for the dir!
  assert(cache->nested_exports[dir].empty());
  cache->nested_exports.erase(dir);

  // dir state
  //dir->state_clear(CDIR_STATE_UNHASHING);  // later
  dir->state_clear(CDIR_STATE_HASHED);
  dir->put(CDir::PIN_HASHED);
  cache->hashdirs.erase(dir);
  dir->mark_clean();

  // inode state
  dir->inode->inode.hash_seed = 0;
  if (dir->inode->is_auth()) {
    dir->inode->_mark_dirty(); // fixme
    mds->mdlog->submit_entry(new EString("unhash inode dirty fixme"));
  }

  // init gather set
  mds->get_mds_map()->get_active_mds_set( hash_gather[dir] );
  hash_gather[dir].erase(mds->get_nodeid());

  // send unhash message
  mds->send_message_mds(new MUnhashDirAck(dir->ino(), bl, nden),
			dir->authority(), MDS_PORT_MIGRATOR);
}


/*
 * first notify comes from auth.
 *  send notifies to all other peers, with peer = self
 * if we get notify from peer=other, remove from our gather list.
 * when we've gotten notifies from everyone,
 *  unpin proxies,
 *  send notify_ack to auth.
 * this ensures that all mds links are flushed of cache_expire type messages.
 */
void Migrator::handle_unhash_dir_notify(MUnhashDirNotify *m)
{
  CInode *in = cache->get_inode(m->get_ino());
  assert(in);
  CDir *dir = in->dir;
  assert(dir);
  
  dout(7) << "handle_unhash_dir_finish " << *dir << endl;
  assert(!dir->is_hashed());
  assert(dir->is_unhashing());
  assert(!dir->is_auth());
  
  int from = m->get_source().num();
  assert(hash_gather[dir].count(from) == 1);
  hash_gather[dir].erase(from);
  delete m;

  // did we send our shout out?
  if (from == dir->authority()) {
    // send notify to everyone else in weird chatter storm
    for (int i=0; i<mds->get_mds_map()->get_num_mds(); i++) {
      if (i == from) continue;
      if (i == mds->get_nodeid()) continue;
      mds->send_message_mds(new MUnhashDirNotify(dir->ino()), i, MDS_PORT_MIGRATOR);
    }
  }

  // are we done?
  if (!hash_gather[dir].empty()) {
    dout(7) << "still waiting for notify from " << hash_gather[dir] << endl;
    return;
  }
  hash_gather.erase(dir);

  // all done!
  dout(7) << "all mds links flushed, unpinning unhash proxies" << endl;

  // unpin proxies
  for (list<CInode*>::iterator it = hash_proxy_inos[dir].begin();
       it != hash_proxy_inos[dir].end();
       it++) {
    CInode *in = *it;
    assert(in->state_test(CInode::STATE_PROXY));
    in->state_clear(CInode::STATE_PROXY);
    in->put(CInode::PIN_PROXY);
  }

  // unfreeze
  dir->unfreeze_dir();
  
  // ack
  dout(7) << "sending notify_ack to auth for unhash of " << *dir << endl;
  mds->send_message_mds(new MUnhashDirNotifyAck(dir->ino()), dir->authority(), MDS_PORT_MIGRATOR);
  
}




void Migrator::show_imports()
{
  mds->balancer->show_imports();
}
