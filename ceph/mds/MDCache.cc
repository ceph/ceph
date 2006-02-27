// -*- mode:C++; tab-width:4; c-basic-offset:2; indent-tabs-mode:t -*- 
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



#include "MDCache.h"
#include "MDStore.h"
#include "CInode.h"
#include "CDir.h"
#include "MDS.h"
#include "MDCluster.h"
#include "MDLog.h"
#include "MDBalancer.h"
#include "AnchorTable.h"

#include "include/filepath.h"

#include "msg/Message.h"
#include "msg/Messenger.h"

#include "osd/Filer.h"

#include "events/EInodeUpdate.h"
#include "events/EDirUpdate.h"
#include "events/EUnlink.h"

#include "messages/MGenericMessage.h"
#include "messages/MDiscover.h"
#include "messages/MDiscoverReply.h"

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

//#include "messages/MInodeUpdate.h"
#include "messages/MDirUpdate.h"
#include "messages/MCacheExpire.h"

#include "messages/MInodeFileCaps.h"

#include "messages/MInodeLink.h"
#include "messages/MInodeLinkAck.h"
#include "messages/MInodeUnlink.h"
#include "messages/MInodeUnlinkAck.h"

#include "messages/MLock.h"
#include "messages/MDentryUnlink.h"

#include "messages/MRenameWarning.h"
#include "messages/MRenameNotify.h"
#include "messages/MRenameNotifyAck.h"
#include "messages/MRename.h"
#include "messages/MRenameAck.h"
#include "messages/MRenameReq.h"
#include "messages/MRenamePrep.h"

#include "messages/MClientRequest.h"
#include "messages/MClientFileCaps.h"

#include "IdAllocator.h"

#include "common/Timer.h"

#include <assert.h>
#include <errno.h>
#include <iostream>
#include <string>
#include <map>
using namespace std;

#include "config.h"
#undef dout
#define  dout(l)    if (l<=g_conf.debug || l <= g_conf.debug_mds) cout << "mds" << mds->get_nodeid() << ".cache "





MDCache::MDCache(MDS *m)
{
  mds = m;
  root = NULL;
  lru.lru_set_max(g_conf.mds_cache_size);
  lru.lru_set_midpoint(g_conf.mds_cache_mid);

  did_shutdown_exports = false;

  shutdown_commits = 0;
}

MDCache::~MDCache() 
{
}


// 

bool MDCache::shutdown()
{
  if (lru.lru_get_size() > 0) {
	dout(7) << "WARNING: mdcache shutodwn with non-empty cache" << endl;
	//show_cache();
	show_imports();
	//dump();
  }
  return true;
}


// MDCache

CInode *MDCache::create_inode()
{
  CInode *in = new CInode;

  // zero
  memset(&in->inode, 0, sizeof(inode_t));
  
  // assign ino
  in->inode.ino = mds->idalloc->get_id(ID_INO);

  in->inode.nlink = 1;   // FIXME

  in->inode.layout = g_OSD_FileLayout;

  add_inode(in);  // add
  return in;
}

void MDCache::destroy_inode(CInode *in)
{
  mds->idalloc->reclaim_id(ID_INO, in->ino());
  remove_inode(in);
}


void MDCache::add_inode(CInode *in) 
{
  // add to lru, inode map
  assert(inode_map.size() == lru.lru_get_size());
  lru.lru_insert_mid(in);
  assert(inode_map.count(in->ino()) == 0);  // should be no dup inos!
  inode_map[ in->ino() ] = in;
  assert(inode_map.size() == lru.lru_get_size());
}

void MDCache::remove_inode(CInode *o) 
{ 
  dout(14) << "remove_inode " << *o << endl;
  if (o->get_parent_dn()) {
	// FIXME: multiple parents?
	CDentry *dn = o->get_parent_dn();
	assert(!dn->is_dirty());
	if (dn->is_sync())
	  dn->dir->remove_dentry(dn);  // unlink inode AND hose dentry
	else
	  dn->dir->unlink_inode(dn);   // leave dentry
  }
  inode_map.erase(o->ino());    // remove from map
  lru.lru_remove(o);           // remove from lru
}




void MDCache::rename_file(CDentry *srcdn, 
						  CDentry *destdn)
{
  CInode *in = srcdn->inode;

  // unlink src
  srcdn->dir->unlink_inode(srcdn);
  
  // unlink old inode?
  if (destdn->inode) destdn->dir->unlink_inode(destdn);
  
  // link inode w/ dentry
  destdn->dir->link_inode( destdn, in );
}


/*
 fix_renamed_dir():

 caller has already:
   - relinked inode in new location
   - fixed in->is_auth()
   - set dir_auth, if appropriate

 caller has not:
   - touched in->dir
   - updated import/export tables
*/
void MDCache::fix_renamed_dir(CDir *srcdir,
							  CInode *in,
							  CDir *destdir,
							  bool authchanged,   // _inode_ auth
							  int dir_auth)        // dir auth (for certain cases)
{
  dout(7) << "fix_renamed_dir on " << *in << endl;
  dout(7) << "fix_renamed_dir on " << *in->dir << endl;

  if (in->dir->is_auth()) {
	// dir ours
	dout(7) << "dir is auth" << endl;
	assert(!in->dir->is_export());

	if (in->is_auth()) {
	  // inode now ours

	  if (authchanged) {
		// inode _was_ replica, now ours
		dout(7) << "inode was replica, now ours.  removing from import list." << endl;
		assert(in->dir->is_import());
		
		// not import anymore!
		imports.erase(in->dir);
		in->dir->state_clear(CDIR_STATE_IMPORT);
		in->dir->put(CDIR_PIN_IMPORT);

		in->dir->set_dir_auth( CDIR_AUTH_PARENT );
		dout(7) << " fixing dir_auth to be " << in->dir->get_dir_auth() << endl;

		// move my nested imports to in's containing import
		CDir *con = get_auth_container(in->dir);
		assert(con);
		for (set<CDir*>::iterator p = nested_exports[in->dir].begin();
			 p != nested_exports[in->dir].end();
			 p++) {
		  dout(7) << "moving nested export under new container " << *con << endl;
		  nested_exports[con].insert(*p);
		}
		nested_exports.erase(in->dir);
		
	  } else {
		// inode was ours, still ours.
		dout(7) << "inode was ours, still ours." << endl;
		assert(!in->dir->is_import());
		assert(in->dir->get_dir_auth() == CDIR_AUTH_PARENT);
		
		// move any exports nested beneath me?
		CDir *newcon = get_auth_container(in->dir);
		assert(newcon);
		CDir *oldcon = get_auth_container(srcdir);
		assert(oldcon);
		if (newcon != oldcon) {
		  dout(7) << "moving nested exports under new container" << endl;
		  set<CDir*> nested;
		  find_nested_exports_under(oldcon, in->dir, nested);
		  for (set<CDir*>::iterator it = nested.begin();
			   it != nested.end();
			   it++) {
			dout(7) << "moving nested export " << *it << " under new container" << endl;
			nested_exports[oldcon].erase(*it);
			nested_exports[newcon].insert(*it);
		  }
		}
	  }

	} else {
	  // inode now replica

	  if (authchanged) {
		// inode was ours, but now replica
		dout(7) << "inode was ours, now replica.  adding to import list." << endl;

		// i am now an import
		imports.insert(in->dir);
		in->dir->state_set(CDIR_STATE_IMPORT);
		in->dir->get(CDIR_PIN_IMPORT);

		in->dir->set_dir_auth( mds->get_nodeid() );
		dout(7) << " fixing dir_auth to be " << in->dir->get_dir_auth() << endl;

		// find old import
		CDir *oldcon = get_auth_container(srcdir);
		assert(oldcon);
		dout(7) << " oldcon is " << *oldcon << endl;

		// move nested exports under me 
		set<CDir*> nested;
		find_nested_exports_under(oldcon, in->dir, nested);  
		for (set<CDir*>::iterator it = nested.begin();
			 it != nested.end();
			 it++) {
		  dout(7) << "moving nested export " << *it << " under me" << endl;
		  nested_exports[oldcon].erase(*it);
		  nested_exports[in->dir].insert(*it);
		}

	  } else {
		// inode was replica, still replica
		dout(7) << "inode was replica, still replica.  doing nothing." << endl;
		assert(in->dir->is_import());

		// verify dir_auth
		assert(in->dir->get_dir_auth() == mds->get_nodeid()); // me, because i'm auth for dir.
		assert(in->authority() != in->dir->get_dir_auth());   // inode not me.
	  }

	  assert(in->dir->is_import());
	}

  } else {
	// dir is not ours
	dout(7) << "dir is not auth" << endl;

	if (in->is_auth()) {
	  // inode now ours

	  if (authchanged) {
		// inode was replica, now ours
		dout(7) << "inode was replica, now ours.  now an export." << endl;
		assert(!in->dir->is_export());
		
		// now export
		exports.insert(in->dir);
		in->dir->state_set(CDIR_STATE_EXPORT);
		in->dir->get(CDIR_PIN_EXPORT);
		
		assert(dir_auth >= 0);  // better be defined
		in->dir->set_dir_auth( dir_auth );
		dout(7) << " fixing dir_auth to be " << in->dir->get_dir_auth() << endl;
		
		CDir *newcon = get_auth_container(in->dir);
		assert(newcon);
		nested_exports[newcon].insert(in->dir);

	  } else {
		// inode was ours, still ours
		dout(7) << "inode was ours, still ours.  did my import change?" << endl;

		// sanity
		assert(in->dir->is_export());
		assert(in->dir->get_dir_auth() >= 0);              
		assert(in->dir->get_dir_auth() != in->authority());

		// moved under new import?
		CDir *oldcon = get_auth_container(srcdir);
		CDir *newcon = get_auth_container(in->dir);
		if (oldcon != newcon) {
		  dout(7) << "moving myself under new import " << *newcon << endl;
		  nested_exports[oldcon].erase(in->dir);
		  nested_exports[newcon].insert(in->dir);
		}
	  }

	  assert(in->dir->is_export());
	} else {
	  // inode now replica

	  if (authchanged) {
		// inode was ours, now replica
		dout(7) << "inode was ours, now replica.  removing from export list." << endl;
		assert(in->dir->is_export());

		// remove from export list
		exports.erase(in->dir);
		in->dir->state_clear(CDIR_STATE_EXPORT);
		in->dir->put(CDIR_PIN_EXPORT);
		
		CDir *oldcon = get_auth_container(srcdir);
		assert(oldcon);
		assert(nested_exports[oldcon].count(in->dir) == 1);
		nested_exports[oldcon].erase(in->dir);

		// simplify dir_auth
		if (in->authority() == in->dir->authority()) {
		  in->dir->set_dir_auth( CDIR_AUTH_PARENT );
		  dout(7) << "simplified dir_auth to -1, inode auth is (also) " << in->authority() << endl;
		} else {
		  assert(in->dir->get_dir_auth() >= 0);    // someone else's export,
		}

	  } else {
		// inode was replica, still replica
		dout(7) << "inode was replica, still replica.  do nothing." << endl;
		
		// fix dir_auth?
		if (in->authority() == dir_auth)
		  in->dir->set_dir_auth( CDIR_AUTH_PARENT );
		else
		  in->dir->set_dir_auth( dir_auth );
		dout(7) << " fixing dir_auth to be " << dir_auth << endl;

		// do nothing.
	  }
	  
	  assert(!in->dir->is_export());
	}  
  }

  show_imports();
}

class C_MDC_EmptyImport : public Context {
  MDCache *mdc;
  CDir *dir;
public:
  C_MDC_EmptyImport(MDCache *mdc, CDir *dir) {
	this->mdc = mdc;
	this->dir = dir;
  }
  void finish(int r) {
	mdc->export_empty_import(dir);
  }
};

void MDCache::export_empty_import(CDir *dir)
{
  dout(7) << "export_empty_import " << *dir << endl;
  
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
  
  dout(7) << "exporting empty import " << *dir << " to " << dest << endl;
  export_dir( dir, dest );
}


bool MDCache::trim(int max) {
  if (max < 0) {
	max = lru.lru_get_max();
	if (!max) return false;
  }

  map<int, MCacheExpire*> expiremap;

  dout(7) << "trim max=" << max << "  cur=" << lru.lru_get_size() << endl;
  assert(expiremap.empty());

  while (lru.lru_get_size() > (unsigned)max) {
	CInode *in = (CInode*)lru.lru_expire();
	if (!in) break; //return false;

	if (in->dir) {
	  // notify dir authority?
	  int auth = in->dir->authority();
	  if (auth != mds->get_nodeid()) {
		dout(17) << "sending expire to mds" << auth << " on   " << *in->dir << endl;
		if (expiremap.count(auth) == 0) expiremap[auth] = new MCacheExpire(mds->get_nodeid());
		expiremap[auth]->add_dir(in->ino(), in->dir->replica_nonce);
	  }
	}

	// notify inode authority?
	{
	  int auth = in->authority();
	  if (auth != mds->get_nodeid()) {
		assert(!in->is_auth());
		dout(17) << "sending expire to mds" << auth << " on " << *in << endl;
		if (expiremap.count(auth) == 0) expiremap[auth] = new MCacheExpire(mds->get_nodeid());
		expiremap[auth]->add_inode(in->ino(), in->replica_nonce);
	  }	else {
		assert(in->is_auth());
	  }
	}
	CInode *diri = NULL;
	if (in->parent)
	  diri = in->parent->dir->inode;

	if (in->is_root()) {
	  dout(7) << "just trimmed root, cache now empty." << endl;
	  root = NULL;
	}


	// last link?
	if (in->inode.nlink == 0) {
	  dout(17) << "last link, removing file content " << *in << endl;             // FIXME THIS IS WRONG PLACE FOR THIS!
	  mds->filer->remove(in->inode, 
						 in->inode.size, 
						 NULL, NULL);   // FIXME
	}

	// remove it
	dout(15) << "trim removing " << *in << " " << in << endl;
	remove_inode(in);
	delete in;

	if (diri) {
	  // dir incomplete!
	  diri->dir->state_clear(CDIR_STATE_COMPLETE);

	  // reexport?
	  if (diri->dir->is_import() &&             // import
		  diri->dir->get_size() == 0 &&         // no children
		  !diri->is_root())                   // not root
		export_empty_import(diri->dir);
	  
	} 

	mds->logger->inc("cex");
  }


  /* hack
  if (lru.lru_get_size() == max) {
	int i;
	dout(1) << "lru_top " << lru.lru_ntop << "/" << lru.lru_num << endl;
	CInode *cur = (CInode*)lru.lru_tophead;
	i = 1;
	while (cur) {
	  dout(1) << " top " << i++ << "/" << lru.lru_ntop << " " << cur->lru_is_expireable() << "  " << *cur << endl;
	  cur = (CInode*)cur->lru_next;
	}

	dout(1) << "lru_bot " << lru.lru_nbot << "/" << lru.lru_num << endl;
	cur = (CInode*)lru.lru_bothead;
	i = 1;
	while (cur) {
	  dout(1) << " bot " << i++ << "/" << lru.lru_nbot << " " << cur->lru_is_expireable() << "  " << *cur << endl;
	  cur = (CInode*)cur->lru_next;
	}

  }
  */

  // send expires
  for (map<int, MCacheExpire*>::iterator it = expiremap.begin();
	   it != expiremap.end();
	   it++) {
	dout(7) << "sending cache_expire to " << it->first << endl;
	mds->messenger->send_message(it->second,
								 MSG_ADDR_MDS(it->first), MDS_PORT_CACHE, MDS_PORT_CACHE);
  }


  return true;
}

class C_MDC_ShutdownCommit : public Context {
  MDCache *mdc;
public:
  C_MDC_ShutdownCommit(MDCache *mdc) {
	this->mdc = mdc;
  }
  void finish(int r) {
	mdc->shutdown_commits--;
  }
};

class C_MDC_ShutdownCheck : public Context {
  MDCache *mdc;
public:
  C_MDC_ShutdownCheck(MDCache *m) : mdc(m) {}
  void finish(int) {
	cout << "shutdown_check at " << g_clock.now() << endl;
	int o = g_conf.debug_mds;
	g_conf.debug_mds = 10;
	mdc->show_cache();
	g_conf.debug_mds = o;
	g_timer.add_event_after(g_conf.mds_shutdown_check, new C_MDC_ShutdownCheck(mdc));
  }
};

void MDCache::shutdown_start()
{
  dout(1) << "shutdown_start" << endl;

  if (g_conf.mds_shutdown_check)
	g_timer.add_event_after(g_conf.mds_shutdown_check, new C_MDC_ShutdownCheck(this));
}



bool MDCache::shutdown_pass()
{
  dout(7) << "shutdown_pass" << endl;
  //assert(mds->is_shutting_down());
  if (mds->is_shut_down()) {
	dout(7) << " already shut down" << endl;
	show_cache();
	show_imports();
	return true;
  }

  // unhash dirs?
  if (!hashdirs.empty()) {
	// unhash any of my dirs?
	for (set<CDir*>::iterator it = hashdirs.begin();
		 it != hashdirs.end();
		 it++) {
	  CDir *dir = *it;
	  if (!dir->is_auth()) continue;
	  if (dir->is_unhashing()) continue;
	  unhash_dir(dir);
	}

	dout(7) << "waiting for dirs to unhash" << endl;
	return false;
  }

  // commit dirs?
  if (g_conf.mds_commit_on_shutdown) {
	
	if (shutdown_commits < 0) {
	  dout(1) << "shutdown_pass committing all dirty dirs" << endl;
	  shutdown_commits = 0;
	  
	  for (hash_map<inodeno_t, CInode*>::iterator it = inode_map.begin();
		   it != inode_map.end();
		   it++) {
		CInode *in = it->second;
		
		// commit any dirty dir that's ours
		if (in->is_dir() && in->dir && in->dir->is_auth() && in->dir->is_dirty()) {
		  mds->mdstore->commit_dir(in->dir, new C_MDC_ShutdownCommit(this));
		  shutdown_commits++;
		}
	  }
	}

	// commits?
	if (shutdown_commits > 0) {
	  dout(7) << "shutdown_commits still waiting for " << shutdown_commits << endl;
	  return false;
	}
  }

  // flush anything we can from the cache
  trim(0);
  dout(5) << "cache size now " << lru.lru_get_size() << endl;


  // (wait for) flush log?
  if (g_conf.mds_log_flush_on_shutdown &&
	  mds->mdlog->get_num_events()) {
	dout(7) << "waiting for log to flush .. " << mds->mdlog->get_num_events() << endl;
	return false;
  } 
  
  // send all imports back to 0.
  if (mds->get_nodeid() != 0 && !did_shutdown_exports) {
	// flush what i can from the cache first..
	trim(0);

	// export to root
	for (set<CDir*>::iterator it = imports.begin();
		 it != imports.end();
		 ) {
	  CDir *im = *it;
	  it++;
	  if (im->inode->is_root()) continue;
	  if (im->is_frozen() || im->is_freezing()) continue;
	  
	  dout(7) << "sending " << *im << " back to mds0" << endl;
	  export_dir(im,0);
	}
	did_shutdown_exports = true;
  } 


  // waiting for imports?  (e.g. root?)
  if (exports.size()) {
	dout(7) << "still have " << exports.size() << " exports" << endl;
	//show_cache();
	return false;
  }

  // filer active?
  if (mds->filer->is_active()) {
	dout(7) << "filer still active" << endl;
	return false;
  }
  
  // close root?
  if (mds->get_nodeid() == 0 &&
	  lru.lru_get_size() == 1 &&
	  root && 
	  root->dir && 
	  root->dir->is_import() &&
	  root->dir->get_ref() == 1) {  // 1 is the import!
	// un-import
	dout(7) << "removing root import" << endl;
	imports.erase(root->dir);
	root->dir->state_clear(CDIR_STATE_IMPORT);
	root->dir->put(CDIR_PIN_IMPORT);

	if (root->is_pinned_by(CINODE_PIN_DIRTY)) {
	  dout(7) << "clearing root dirty flag" << endl;
	  root->put(CINODE_PIN_DIRTY);
	}

	trim(0);
	assert(inode_map.size() == lru.lru_get_size());
  }
  
  // imports?
  if (!imports.empty()) {
	dout(7) << "still have " << imports.size() << " imports" << endl;
	//show_cache();
	return false;
  }
  
  // done?
  if (lru.lru_get_size() > 0) {
	dout(7) << "there's still stuff in the cache: " << lru.lru_get_size() << endl;
	show_cache();
	//dump();
	return false;
  }
  
  // done!
  dout(1) << "shutdown done, sending shutdown_finish" << endl;
  if (mds->get_nodeid() != 0) {
	//g_conf.debug = 25;
	mds->messenger->send_message(new MGenericMessage(MSG_MDS_SHUTDOWNFINISH),
								 MSG_ADDR_MDS(0), MDS_PORT_MAIN, MDS_PORT_MAIN);
  } else {
	mds->handle_shutdown_finish(NULL);
  }
  return true;
}







int MDCache::open_root(Context *c)
{
  int whoami = mds->get_nodeid();

  // open root inode
  if (whoami == 0) { 
	// i am root inode
	CInode *root = new CInode();
	memset(&root->inode, 0, sizeof(inode_t));
	root->inode.ino = 1;
	root->inode.hash_seed = 0;   // not hashed!

	// make it up (FIXME)
	root->inode.mode = 0755 | INODE_MODE_DIR;
	root->inode.size = 0;
	root->inode.mtime = 0;

	root->inode.nlink = 1;
	root->inode.layout = g_OSD_MDDirLayout;

	root->state_set(CINODE_STATE_ROOT);

	set_root( root );

	// root directory too
	assert(root->dir == NULL);
	root->set_dir( new CDir(root, mds, true) );
	root->dir->set_dir_auth( 0 );  // me!
	root->dir->dir_rep = CDIR_REP_NONE;

	// root is sort of technically an import (from a vacuum)
	imports.insert( root->dir );
	root->dir->state_set(CDIR_STATE_IMPORT);
	root->dir->get(CDIR_PIN_IMPORT);

	if (c) {
	  c->finish(0);
	  delete c;
	}
  } else {
	// request inode from root mds
	if (waiting_for_root.empty()) {
	  dout(7) << "discovering root" << endl;

	  filepath want;
	  MDiscover *req = new MDiscover(whoami,
									 0,
									 want,
									 false);  // there _is_ no base dir for the root inode
	  mds->messenger->send_message(req,
								   MSG_ADDR_MDS(0), MDS_PORT_CACHE,
								   MDS_PORT_CACHE);
	} else {
	  dout(7) << "waiting for root" << endl;
	}	

	// wait
	waiting_for_root.push_back(c);

  }

  return 0;
}



/*
 * some import/export helpers
 */

/** con = get_auth_container(dir)
 * Returns the directory in which authority is delegated for *dir.  
 * This may be because a directory is an import, or because it is hashed
 * and we are nested underneath an inode in that dir (that hashes to us).
 * Thus do not assume con->is_auth()!  It is_auth() || is_hashed().
 */
CDir *MDCache::get_auth_container(CDir *dir)
{
  CDir *imp = dir;  // might be *dir

  // find the underlying import or hash that delegates dir
  while (true) {
	if (imp->is_import()) break; // import
	imp = imp->get_parent_dir();
	assert(imp);
	if (imp->is_hashed()) break; // hash
  }

  return imp;
}


void MDCache::find_nested_exports(CDir *dir, set<CDir*>& s) 
{
  CDir *import = get_auth_container(dir);
  find_nested_exports_under(import, dir, s);
}

void MDCache::find_nested_exports_under(CDir *import, CDir *dir, set<CDir*>& s)
{
  dout(10) << "find_nested_exports for " << *dir << endl;
  dout(10) << "find_nested_exports_under import " << *import << endl;

  if (import == dir) {
	// yay, my job is easy!
	for (set<CDir*>::iterator p = nested_exports[import].begin();
		 p != nested_exports[import].end();
		 p++) {
	  CDir *nested = *p;
	  s.insert(nested);
	  dout(10) << "find_nested_exports " << *dir << " " << *nested << endl;
	}
	return;
  }

  // ok, my job is annoying.
  for (set<CDir*>::iterator p = nested_exports[import].begin();
	   p != nested_exports[import].end();
	   p++) {
	CDir *nested = *p;
	
	dout(12) << "find_nested_exports checking " << *nested << endl;

	// trace back to import, or dir
	CDir *cur = nested->get_parent_dir();
	while (!cur->is_import() || cur == dir) {
	  if (cur == dir) {
		s.insert(nested);
		dout(10) << "find_nested_exports " << *dir << " " << *nested << endl;
		break;
	  } else {
		cur = cur->get_parent_dir();
	  }
	}
  }
}









// ========= messaging ==============


int MDCache::proc_message(Message *m)
{
  switch (m->get_type()) {
  case MSG_MDS_DISCOVER:
	handle_discover((MDiscover*)m);
	break;
  case MSG_MDS_DISCOVERREPLY:
	handle_discover_reply((MDiscoverReply*)m);
	break;

	/*
  case MSG_MDS_INODEUPDATE:
	handle_inode_update((MInodeUpdate*)m);
	break;
	*/

  case MSG_MDS_INODELINK:
	handle_inode_link((MInodeLink*)m);
	break;
  case MSG_MDS_INODELINKACK:
	handle_inode_link_ack((MInodeLinkAck*)m);
	break;

  case MSG_MDS_DIRUPDATE:
	handle_dir_update((MDirUpdate*)m);
	break;

  case MSG_MDS_CACHEEXPIRE:
	handle_cache_expire((MCacheExpire*)m);
	break;


	// locking
  case MSG_MDS_LOCK:
	handle_lock((MLock*)m);
	break;

	// cache fun
  case MSG_MDS_INODEFILECAPS:
	handle_inode_file_caps((MInodeFileCaps*)m);
	break;

  case MSG_CLIENT_FILECAPS:
	handle_client_file_caps((MClientFileCaps*)m);
	break;

  case MSG_MDS_DENTRYUNLINK:
	handle_dentry_unlink((MDentryUnlink*)m);
	break;

  case MSG_MDS_RENAMEWARNING:
	handle_rename_warning((MRenameWarning*)m);
	break;
  case MSG_MDS_RENAMENOTIFY:
	handle_rename_notify((MRenameNotify*)m);
	break;
  case MSG_MDS_RENAMENOTIFYACK:
	handle_rename_notify_ack((MRenameNotifyAck*)m);
	break;
  case MSG_MDS_RENAME:
	handle_rename((MRename*)m);
	break;
  case MSG_MDS_RENAMEREQ:
	handle_rename_req((MRenameReq*)m);
	break;
  case MSG_MDS_RENAMEPREP:
	handle_rename_prep((MRenamePrep*)m);
	break;
  case MSG_MDS_RENAMEACK:
	handle_rename_ack((MRenameAck*)m);
	break;

	
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
	dout(7) << "cache unknown message " << m->get_type() << endl;
	assert(0);
	break;
  }

  return 0;
}


/* path_traverse
 *
 * return values:
 *   <0 : traverse error (ENOTDIR, ENOENT)
 *    0 : success
 *   >0 : delayed or forwarded
 *
 * Notes:
 *   onfinish context is only needed if you specify MDS_TRAVERSE_DISCOVER _and_
 *   you aren't absolutely certain that the path actually exists.  If it doesn't,
 *   the context is needed to pass a (failure) result code.
 */

class C_MDC_TraverseDiscover : public Context {
  Context *onfinish, *ondelay;
 public:
  C_MDC_TraverseDiscover(Context *onfinish, Context *ondelay) {
	this->ondelay = ondelay;
	this->onfinish = onfinish;
  }
  void finish(int r) {
	//dout(10) << "TraverseDiscover r = " << r << endl;
	if (r < 0 && onfinish) {   // ENOENT on discover, pass back to caller.
	  onfinish->finish(r);
	} else {
	  ondelay->finish(r);      // retry as usual
	}
	delete onfinish;
	delete ondelay;
  }
};

int MDCache::path_traverse(filepath& origpath, 
						   vector<CDentry*>& trace, 
						   bool follow_trailing_symlink,
						   Message *req,
						   Context *ondelay,
						   int onfail,
						   Context *onfinish,
						   bool is_client_req)  // true if req is MClientRequest .. gross, FIXME
{
  int whoami = mds->get_nodeid();
  set< pair<CInode*, string> > symlinks_resolved; // keep a list of symlinks we touch to avoid loops

  bool noperm = false;
  if (onfail == MDS_TRAVERSE_DISCOVER ||
	  onfail == MDS_TRAVERSE_DISCOVERXLOCK) noperm = true;

  // root
  CInode *cur = get_root();
  if (cur == NULL) {
	dout(7) << "traverse: i don't have root" << endl;
	open_root(ondelay);
	if (onfinish) delete onfinish;
	return 1;
  }

  // start trace
  trace.clear();

  // make our own copy, since we'll modify when we hit symlinks
  filepath path = origpath;  

  unsigned depth = 0;
  while (depth < path.depth()) {
	dout(12) << "traverse: path seg depth " << depth << " = " << path[depth] << endl;
	
	// ENOTDIR?
	if (!cur->is_dir()) {
	  dout(7) << "traverse: " << *cur << " not a dir " << endl;
	  delete ondelay;
	  if (onfinish) {
		onfinish->finish(-ENOTDIR);
		delete onfinish;
	  }
	  return -ENOTDIR;
	}

	// open dir
	if (!cur->dir) {
	  if (cur->dir_is_auth()) {
		// parent dir frozen_dir?
		if (cur->is_frozen_dir()) {
		  dout(7) << "traverse: " << *cur->get_parent_dir() << " is frozen_dir, waiting" << endl;
		  cur->get_parent_dir()->add_waiter(CDIR_WAIT_UNFREEZE, ondelay);
		  if (onfinish) delete onfinish;
		  return 1;
		}

		cur->get_or_open_dir(mds);
		assert(cur->dir);
	  } else {
		// discover dir from/via inode auth
		assert(!cur->is_auth());
		if (cur->waiting_for(CINODE_WAIT_DIR)) {
		  dout(10) << "traverse: need dir for " << *cur << ", already doing discover" << endl;
		} else {
		  filepath want = path.postfixpath(depth);
		  dout(10) << "traverse: need dir for " << *cur << ", doing discover, want " << want.get_path() << endl;
		  mds->messenger->send_message(new MDiscover(mds->get_nodeid(),
													 cur->ino(),
													 want,
													 true),  // need this dir too
									   MSG_ADDR_MDS(cur->authority()), MDS_PORT_CACHE,
									   MDS_PORT_CACHE);
		}
		cur->add_waiter(CINODE_WAIT_DIR, ondelay);
		if (onfinish) delete onfinish;
		return 1;
	  }
	}
	
	// frozen?
	/*
	if (cur->dir->is_frozen()) {
	  // doh!
	  // FIXME: traverse is allowed?
	  dout(7) << "traverse: " << *cur->dir << " is frozen, waiting" << endl;
	  cur->dir->add_waiter(CDIR_WAIT_UNFREEZE, ondelay);
	  if (onfinish) delete onfinish;
	  return 1;
	}
	*/

	// must read directory hard data (permissions, x bit) to traverse
	if (!noperm && !inode_hard_read_try(cur, ondelay)) {
	  if (onfinish) delete onfinish;
	  return 1;
	}
	
	// check permissions?
	// XXX
	
	// ..?
	if (path[depth] == "..") {
	  trace.pop_back();
	  depth++;
	  cur = cur->get_parent_inode();
	  dout(10) << "traverse: following .. back to " << *cur << endl;
	  continue;
	}


	// dentry
	CDentry *dn = cur->dir->lookup(path[depth]);

	// null and last_bit and xlocked by me?
	if (dn && dn->is_null() && 
		dn->is_xlockedbyme(req) &&
		depth == path.depth()-1) {
	  dout(10) << "traverse: hit (my) xlocked dentry at tail of traverse, succeeding" << endl;
	  trace.push_back(dn);
	  break; // done!
	}

	if (dn && !dn->is_null()) {
	  // dentry exists.  xlocked?
	  if (!noperm && dn->is_xlockedbyother(req)) {
		dout(10) << "traverse: xlocked dentry at " << *dn << endl;
		cur->dir->add_waiter(CDIR_WAIT_DNREAD,
							 path[depth],
							 ondelay);
		if (onfinish) delete onfinish;
		return 1;
	  }

	  // do we have inode?
	  if (!dn->inode) {
		assert(dn->is_remote());
		// do i have it?
		CInode *in = get_inode(dn->get_remote_ino());
		if (in) {
		  dout(7) << "linking in remote in " << *in << endl;
		  dn->link_remote(in);
		} else {
		  dout(7) << "remote link to " << hex << dn->get_remote_ino() << dec << ", which i don't have" << endl;
		  open_remote_ino(dn->get_remote_ino(), req,
						  ondelay);
		  return 1;
		}		
	  }

	  // symlink?
	  if (dn->inode->is_symlink() &&
		  (follow_trailing_symlink || depth < path.depth()-1)) {
		// symlink, resolve!
		filepath sym = dn->inode->symlink;
		dout(10) << "traverse: hit symlink " << *dn->inode << " to " << sym << endl;

		// break up path components
		// /head/symlink/tail
		filepath head = path.prefixpath(depth);
		filepath tail = path.postfixpath(depth+1);
		dout(10) << "traverse: path head = " << head << endl;
		dout(10) << "traverse: path tail = " << tail << endl;
		
		if (symlinks_resolved.count(pair<CInode*,string>(dn->inode, tail.get_path()))) {
		  dout(10) << "already hit this symlink, bailing to avoid the loop" << endl;
		  return -ELOOP;
		}
		symlinks_resolved.insert(pair<CInode*,string>(dn->inode, tail.get_path()));

		// start at root?
		if (dn->inode->symlink[0] == '/') {
		  // absolute
		  trace.clear();
		  depth = 0;
		  path = tail;
		  dout(10) << "traverse: absolute symlink, path now " << path << " depth " << depth << endl;
		} else {
		  // relative
		  path = head;
		  path.append(sym);
		  path.append(tail);
		  dout(10) << "traverse: relative symlink, path now " << path << " depth " << depth << endl;
		}
		continue;		
	  } else {
		// keep going.

		// forwarder wants replicas?
		if (is_client_req && ((MClientRequest*)req)->get_mds_wants_replica_in_dirino()) {
		  dout(30) << "traverse: REP is here, " << hex << ((MClientRequest*)req)->get_mds_wants_replica_in_dirino() << " vs " << cur->dir->ino() << dec << endl;
		  
		  if (((MClientRequest*)req)->get_mds_wants_replica_in_dirino() == cur->dir->ino() &&
			  cur->dir->is_auth() && 
			  cur->dir->is_rep() &&
			  cur->dir->is_open_by(req->get_source().num())) {
			assert(req->get_source().is_mds());
			int from = req->get_source().num();
			
			if (dn->get_inode()->is_cached_by(from)) {
			  dout(15) << "traverse: REP would replicate to mds" << from << ", but already cached_by " 
					   << MSG_ADDR_NICE(req->get_source()) << " dn " << *dn << endl; 
			} else {
			  dout(10) << "traverse: REP replicating to " << MSG_ADDR_NICE(req->get_source()) << " dn " << *dn << endl;
			  MDiscoverReply *reply = new MDiscoverReply(cur->dir->ino());
			  reply->add_dentry( dn->get_name(), !dn->can_read());
			  reply->add_inode( dn->inode->replicate_to( from ) );
			  mds->messenger->send_message(reply,
										   req->get_source(), MDS_PORT_CACHE, MDS_PORT_CACHE);
			}
		  }
		}
			
		trace.push_back(dn);
		cur = dn->inode;
		touch_inode(cur);
		depth++;
		continue;
	  }
	}
	
	// MISS.  don't have it.

	int dauth = cur->dir->dentry_authority( path[depth] );
	dout(12) << "traverse: miss on dentry " << path[depth] << " dauth " << dauth << " in " << *cur->dir << endl;
	

	if (dauth == whoami) {
	  // dentry is mine.
	  if (cur->dir->is_complete()) {
		// file not found
		delete ondelay;
		if (onfinish) {
		  onfinish->finish(-ENOENT);
		  delete onfinish;
		}
		return -ENOENT;
	  } else {
		
		//wrong?
		//if (onfail == MDS_TRAVERSE_DISCOVER) 
		//  return -1;
		
		// directory isn't complete; reload
		dout(7) << "traverse: incomplete dir contents for " << *cur << ", fetching" << endl;
		touch_inode(cur);
		mds->mdstore->fetch_dir(cur->dir, ondelay);
		
		mds->logger->inc("cmiss");

		if (onfinish) delete onfinish;
		return 1;
	  }
	} else {
	  // dentry is not mine.
	  
	  /* no, let's let auth handle the discovery/replication ..
	  if (onfail == MDS_TRAVERSE_FORWARD && 
		  onfinish == 0 &&   // no funnyness
		  cur->dir->is_rep()) {
		dout(5) << "trying to discover in popular dir " << *cur->dir << endl;
		onfail = MDS_TRAVERSE_DISCOVER;
	  }
	  */

	  if ((onfail == MDS_TRAVERSE_DISCOVER ||
		   onfail == MDS_TRAVERSE_DISCOVERXLOCK)) {
		// discover

		filepath want = path.postfixpath(depth);
		if (cur->dir->waiting_for(CDIR_WAIT_DENTRY, path[depth])) {
		  dout(7) << "traverse: already waiting for discover on " << *cur << " for " << want.get_path() << " to mds" << dauth << endl;
		} else {
		  dout(7) << "traverse: discover on " << *cur << " for " << want.get_path() << " to mds" << dauth << endl;
		  
		  touch_inode(cur);
		
		  mds->messenger->send_message(new MDiscover(mds->get_nodeid(),
													 cur->ino(),
													 want,
													 false),
									   MSG_ADDR_MDS(dauth), MDS_PORT_CACHE,
									   MDS_PORT_CACHE);
		  mds->logger->inc("dis");
		}
		
		// delay processing of current request.
		//  delay finish vs ondelay until result of traverse, so that ENOENT can be 
		//  passed to onfinish if necessary
		cur->dir->add_waiter(CDIR_WAIT_DENTRY, 
							 path[depth], 
							 new C_MDC_TraverseDiscover(onfinish, ondelay));
		
		mds->logger->inc("cmiss");
		return 1;
	  } 
	  if (onfail == MDS_TRAVERSE_FORWARD) {
		// forward
		dout(7) << "traverse: not auth for " << path << " at " << path[depth] << ", fwd to mds" << dauth << endl;

		if (is_client_req && cur->dir->is_rep()) {
		  dout(15) << "traverse: REP fw to mds" << dauth << ", requesting rep under " << *cur->dir << endl;
		  ((MClientRequest*)req)->set_mds_wants_replica_in_dirino(cur->dir->ino());
		  req->clear_payload();  // reencode!
		}

		mds->messenger->send_message(req,
									 MSG_ADDR_MDS(dauth), req->get_dest_port(),
									 req->get_dest_port());
		//show_imports();
		
		mds->logger->inc("cfw");
		if (onfinish) delete onfinish;
		delete ondelay;
		return 2;
	  }	
	  if (onfail == MDS_TRAVERSE_FAIL) {
		delete ondelay;
		if (onfinish) {
		  onfinish->finish(-ENOENT);  // -ENOENT, but only because i'm not the authority!
		  delete onfinish;
		}
		return -ENOENT;  // not necessarily exactly true....
	  }
	}
	
	assert(0);  // i shouldn't get here
  }
  
  // success.
  delete ondelay;
  if (onfinish) {
	onfinish->finish(0);
	delete onfinish;
  }
  return 0;
}



void MDCache::open_remote_dir(CInode *diri,
							  Context *fin) 
{
  dout(10) << "open_remote_dir on " << *diri << endl;
  
  assert(diri->is_dir());
  assert(!diri->dir_is_auth());
  assert(!diri->is_auth());
  assert(diri->dir == 0);

  filepath want;  // no dentries, i just want the dir open
  mds->messenger->send_message(new MDiscover(mds->get_nodeid(),
											 diri->ino(),
											 want,
											 true),  // need the dir open
							   MSG_ADDR_MDS(diri->authority()), MDS_PORT_CACHE,
							   MDS_PORT_CACHE);

  diri->add_waiter(CINODE_WAIT_DIR, fin);
}



class C_MDC_OpenRemoteInoLookup : public Context {
  MDCache *mdc;
  inodeno_t ino;
  Message *req;
  Context *onfinish;
public:
  vector<Anchor*> anchortrace;
  C_MDC_OpenRemoteInoLookup(MDCache *mdc, inodeno_t ino, Message *req, Context *onfinish) {
	this->mdc = mdc;
	this->ino = ino;
	this->req = req;
	this->onfinish = onfinish;
  }
  void finish(int r) {
	assert(r == 0);
	if (r == 0)
	  mdc->open_remote_ino_2(ino, req, anchortrace, onfinish);
	else {
	  onfinish->finish(r);
	  delete onfinish;
	}
  }
};

void MDCache::open_remote_ino(inodeno_t ino,
							  Message *req,
							  Context *onfinish)
{
  dout(7) << "open_remote_ino on " << ino << endl;
  
  C_MDC_OpenRemoteInoLookup *c = new C_MDC_OpenRemoteInoLookup(this, ino, req, onfinish);
  mds->anchormgr->lookup(ino, c->anchortrace, c);
}

void MDCache::open_remote_ino_2(inodeno_t ino,
								Message *req,
								vector<Anchor*>& anchortrace,
								Context *onfinish)
{
  dout(7) << "open_remote_ino_2 on " << ino << ", trace depth is " << anchortrace.size() << endl;
  
  // construct path
  filepath path;
  for (unsigned i=0; i<anchortrace.size(); i++) 
	path.add_dentry(anchortrace[i]->ref_dn);

  dout(7) << " path is " << path << endl;

  vector<CDentry*> trace;
  int r = path_traverse(path, trace, false,
						req,
						onfinish,  // delay actually
						MDS_TRAVERSE_DISCOVER);
  if (r > 0) return;
  
  onfinish->finish(r);
  delete onfinish;
}




// path pins

bool MDCache::path_pin(vector<CDentry*>& trace,
					   Message *m,
					   Context *c)
{
  // verify everything is pinnable
  for (vector<CDentry*>::iterator it = trace.begin();
	   it != trace.end();
	   it++) {
	CDentry *dn = *it;
	if (!dn->is_pinnable(m)) {
	  // wait
	  if (c) {
		dout(10) << "path_pin can't pin " << *dn << ", waiting" << endl;
		dn->dir->add_waiter(CDIR_WAIT_DNPINNABLE,   
							dn->name,
							c);
	  } else {
		dout(10) << "path_pin can't pin, no waiter, failing." << endl;
	  }
	  return false;
	}
  }

  // pin!
  for (vector<CDentry*>::iterator it = trace.begin();
	   it != trace.end();
	   it++) {
	(*it)->pin(m);
	dout(11) << "path_pinned " << *(*it) << endl;
  }

  delete c;
  return true;
}


void MDCache::path_unpin(vector<CDentry*>& trace,
						 Message *m)
{
  for (vector<CDentry*>::iterator it = trace.begin();
	   it != trace.end();
	   it++) {
	CDentry *dn = *it;
	dn->unpin(m);
	dout(11) << "path_unpinned " << *dn << endl;

	// did we completely unpin a waiter?
	if (dn->lockstate == DN_LOCK_UNPINNING && !dn->is_pinned()) {
	  // return state to sync, in case the unpinner flails
	  dn->lockstate = DN_LOCK_SYNC;

	  // run finisher right now to give them a fair shot.
	  dn->dir->finish_waiting(CDIR_WAIT_DNUNPINNED, dn->name);
	}
  }
}


void MDCache::make_trace(vector<CDentry*>& trace, CInode *in)
{
  CInode *parent = in->get_parent_inode();
  if (parent) {
	make_trace(trace, parent);

	CDentry *dn = in->get_parent_dn();
	dout(15) << "make_trace adding " << *dn << endl;
	trace.push_back(dn);
  }
}


bool MDCache::request_start(Message *req,
							CInode *ref,
							vector<CDentry*>& trace)
{
  assert(active_requests.count(req) == 0);

  // pin path
  if (trace.size()) {
	if (!path_pin(trace, req, new C_MDS_RetryMessage(mds,req))) return false;
  }

  dout(7) << "request_start " << *req << endl;

  // add to map
  active_requests[req].ref = ref;
  if (trace.size()) active_requests[req].traces[trace[trace.size()-1]] = trace;

  // request pins
  request_pin_inode(req, ref);
  
  mds->logger->inc("req");

  return true;
}


void MDCache::request_pin_inode(Message *req, CInode *in) 
{
  if (active_requests[req].request_pins.count(in) == 0) {
	in->request_pin_get();
	active_requests[req].request_pins.insert(in);
  }
}

void MDCache::request_pin_dir(Message *req, CDir *dir) 
{
  if (active_requests[req].request_dir_pins.count(dir) == 0) {
	dir->request_pin_get();
	active_requests[req].request_dir_pins.insert(dir);
  }
}


void MDCache::request_cleanup(Message *req)
{
  assert(active_requests.count(req) == 1);

  // leftover xlocks?
  if (active_requests[req].xlocks.size()) {
	set<CDentry*> dns = active_requests[req].xlocks;

	for (set<CDentry*>::iterator it = dns.begin();
		 it != dns.end();
		 it++) {
	  CDentry *dn = *it;
	  
	  dout(7) << "request_cleanup leftover xlock " << *dn << endl;
	  
	  dentry_xlock_finish(dn);
	  
	  // queue finishers
	  dn->dir->take_waiting(CDIR_WAIT_ANY, dn->name, mds->finished_queue);

	  // remove clean, null dentry?  (from a failed rename or whatever)
	  if (dn->is_null() && dn->is_sync() && !dn->is_dirty()) {
		dn->dir->remove_dentry(dn);
	  }
	}
	
	assert(active_requests[req].xlocks.empty());  // we just finished finished them
  }

  // foreign xlocks?
  if (active_requests[req].foreign_xlocks.size()) {
	set<CDentry*> dns = active_requests[req].foreign_xlocks;
	active_requests[req].foreign_xlocks.clear();
	
	for (set<CDentry*>::iterator it = dns.begin();
		 it != dns.end();
		 it++) {
	  CDentry *dn = *it;
	  
	  dout(7) << "request_cleanup sending unxlock for foreign xlock on " << *dn << endl;
	  assert(dn->is_xlocked());
	  int dauth = dn->dir->dentry_authority(dn->name);
	  MLock *m = new MLock(LOCK_AC_UNXLOCK, mds->get_nodeid());
	  m->set_dn(dn->dir->ino(), dn->name);
	  mds->messenger->send_message(m,
								   MSG_ADDR_MDS(dauth), MDS_PORT_CACHE, MDS_PORT_CACHE);
	}
  }

  // unpin paths
  for (map< CDentry*, vector<CDentry*> >::iterator it = active_requests[req].traces.begin();
	   it != active_requests[req].traces.end();
	   it++) {
	path_unpin(it->second, req);
  }
  
  // request pins
  for (set<CInode*>::iterator it = active_requests[req].request_pins.begin();
	   it != active_requests[req].request_pins.end();
	   it++) {
	(*it)->request_pin_put();
  }
  for (set<CDir*>::iterator it = active_requests[req].request_dir_pins.begin();
	   it != active_requests[req].request_dir_pins.end();
	   it++) {
	(*it)->request_pin_put();
  }

  // remove from map
  active_requests.erase(req);


  // log some stats *****
  mds->logger->set("c", lru.lru_get_size());
  mds->logger->set("cpin", lru.lru_get_num_pinned());
  mds->logger->set("ctop", lru.lru_get_top());
  mds->logger->set("cbot", lru.lru_get_bot());
  mds->logger->set("cptail", lru.lru_get_pintail());
  mds->logger->set("buf",buffer_total_alloc);

  if (g_conf.log_pins) {
	// pin
	for (int i=0; i<CINODE_NUM_PINS; i++) {
	  mds->logger2->set(cinode_pin_names[i],
						cinode_pins[i]);
	}
	/*
	  for (map<int,int>::iterator it = cdir_pins.begin();
	  it != cdir_pins.end();
	  it++) {
	  //string s = "D";
	  //s += cdir_pin_names[it->first];
	  mds->logger2->set(//s, 
	  cdir_pin_names[it->first],
	  it->second);
	  }
	*/
  }

}

void MDCache::request_finish(Message *req)
{
  dout(7) << "request_finish " << *req << endl;
  request_cleanup(req);
  delete req;  // delete req
  
  mds->logger->inc("reply");


  //dump();
}


void MDCache::request_forward(Message *req, int who, int port)
{
  if (!port) port = MDS_PORT_SERVER;

  dout(7) << "request_forward to " << who << " req " << *req << endl;
  request_cleanup(req);
  mds->messenger->send_message(req,
							   MSG_ADDR_MDS(who), port,
							   port);

  mds->logger->inc("fw");
}



// ANCHORS

class C_MDC_AnchorInode : public Context {
  CInode *in;
  
public:
  C_MDC_AnchorInode(CInode *in) {
	this->in = in;
  }
  void finish(int r) {
	if (r == 0) {
	  assert(in->inode.anchored == false);
	  in->inode.anchored = true;

	  in->state_clear(CINODE_STATE_ANCHORING);
	  in->put(CINODE_PIN_ANCHORING);
	  
	  in->mark_dirty();
	}

	// trigger
	in->finish_waiting(CINODE_WAIT_ANCHORED, r);
  }
};

void MDCache::anchor_inode(CInode *in, Context *onfinish)
{
  assert(in->is_auth());

  // already anchoring?
  if (in->state_test(CINODE_STATE_ANCHORING)) {
	dout(7) << "anchor_inode already anchoring " << *in << endl;

	// wait
	in->add_waiter(CINODE_WAIT_ANCHORED,
				   onfinish);

  } else {
	dout(7) << "anchor_inode anchoring " << *in << endl;

	// auth: do it
	in->state_set(CINODE_STATE_ANCHORING);
	in->get(CINODE_PIN_ANCHORING);
	
	// wait
	in->add_waiter(CINODE_WAIT_ANCHORED,
				   onfinish);
	
	// make trace
	vector<Anchor*> trace;
	in->make_anchor_trace(trace);
	
	// do it
	mds->anchormgr->create(in->ino(), trace, 
						   new C_MDC_AnchorInode( in ));
  }
}


void MDCache::handle_inode_link(MInodeLink *m)
{
  CInode *in = get_inode(m->get_ino());
  assert(in);

  if (!in->is_auth()) {
	assert(in->is_proxy());
	dout(7) << "handle_inode_link not auth for " << *in << ", fw to auth" << endl;
	mds->messenger->send_message(m,
								 MSG_ADDR_MDS(in->authority()), MDS_PORT_CACHE, MDS_PORT_CACHE);
	return;
  }

  dout(7) << "handle_inode_link on " << *in << endl;

  if (!in->is_anchored()) {
	assert(in->inode.nlink == 1);
	dout(7) << "needs anchor, nlink=" << in->inode.nlink << ", creating anchor" << endl;
	
	anchor_inode(in,
				 new C_MDS_RetryMessage(mds, m));
	return;
  }

  in->inode.nlink++;
  in->mark_dirty();

  // reply
  dout(7) << " nlink++, now " << in->inode.nlink++ << endl;

  mds->messenger->send_message(new MInodeLinkAck(m->get_ino(), true),
							   MSG_ADDR_MDS(m->get_from()), MDS_PORT_CACHE, MDS_PORT_CACHE);
  delete m;
}


void MDCache::handle_inode_link_ack(MInodeLinkAck *m) 
{
  CInode *in = get_inode(m->get_ino());
  assert(in);

  dout(7) << "handle_inode_link_ack success = " << m->is_success() << " on " << *in << endl;
  in->finish_waiting(CINODE_WAIT_LINK,
					 m->is_success() ? 1:-1);
}



// REPLICAS


void MDCache::handle_discover(MDiscover *dis) 
{
  int whoami = mds->get_nodeid();

  // from me to me?
  if (dis->get_asker() == whoami) {
    dout(7) << "discover for " << dis->get_want().get_path() << " bounced back to me, dropping." << endl;
	delete dis;
	return;
  }

  CInode *cur = 0;
  MDiscoverReply *reply = 0;
  //filepath fullpath;

  // get started.
  if (dis->get_base_ino() == 0) {
    // wants root
    dout(7) << "discover from mds" << dis->get_asker() << " wants root + " << dis->get_want().get_path() << endl;

    assert(mds->get_nodeid() == 0);
    assert(root->is_auth());

	//fullpath = dis->get_want();


    // add root
    reply = new MDiscoverReply(0);
    reply->add_inode( root->replicate_to( dis->get_asker() ) );
	dout(10) << "added root " << *root << endl;

    cur = root;
    
  } else {
    // there's a base inode
    cur = get_inode(dis->get_base_ino());
    assert(cur);

	if (dis->wants_base_dir()) {
	  dout(7) << "discover from mds" << dis->get_asker() << " has " << *cur << " wants dir+" << dis->get_want().get_path() << endl;
	} else {
	  dout(7) << "discover from mds" << dis->get_asker() << " has " << *cur->dir << " wants " << dis->get_want().get_path() << endl;
	}
    
    assert(cur->is_dir());
    
	// crazyness?
	if (!cur->dir && !cur->is_auth()) {
	  int iauth = cur->authority();
	  dout(7) << "no dir and not inode auth; fwd to auth " << iauth << endl;
	  mds->messenger->send_message( dis,
									MSG_ADDR_MDS( iauth ), MDS_PORT_CACHE, MDS_PORT_CACHE );
	  return;
	}

	// frozen_dir?
	if (!cur->dir && cur->is_frozen_dir()) {
	  dout(7) << "is frozen_dir, waiting" << endl;
	  cur->get_parent_dir()->add_waiter(CDIR_WAIT_UNFREEZE, 
										new C_MDS_RetryMessage(mds, dis));
	  return;
	}

    if (!cur->dir) 
	  cur->get_or_open_dir(mds);
    assert(cur->dir);

	dout(10) << "dir is " << *cur->dir << endl;
	
    // create reply
    reply = new MDiscoverReply(cur->ino());
  }

  assert(reply);
  assert(cur);
  
  /*
  // first traverse and make sure we won't have to do any waiting
  dout(10) << "traversing full discover path = " << fullpath << endl;
  vector<CInode*> trav;
  int r = path_traverse(fullpath, trav, dis, MDS_TRAVERSE_FAIL);
  if (r > 0) 
	return;  // fw or delay
  dout(10) << "traverse finish w/o blocking, continuing" << endl;
  // ok, now we know we won't block on dentry locks or readdir.
  */


  // add content
  // do some fidgeting to include a dir if they asked for the base dir, or just root.
  for (unsigned i = 0; i < dis->get_want().depth() || dis->get_want().depth() == 0; i++) {
    // add dir
    if (reply->is_empty() && !dis->wants_base_dir()) {
      dout(7) << "they don't want the base dir" << endl;
    } else {
	  // is it actaully a dir at all?
	  if (!cur->is_dir()) {
		dout(7) << "not a dir " << *cur << endl;
		reply->set_flag_error_dir();
		break;
	  }

      // add dir
      if (!cur->dir_is_auth()) {
		dout(7) << *cur << " dir auth is someone else, i'm done" << endl;
        break;
      }
	  
	  // did we hit a frozen_dir?
	  if (!cur->dir && cur->is_frozen_dir()) {
		dout(7) << *cur << " is frozen_dir, stopping" << endl;
		break;
	  }
	  
	  if (!cur->dir) cur->get_or_open_dir(mds);
	  
      reply->add_dir( new CDirDiscover( cur->dir, 
										cur->dir->open_by_add( dis->get_asker() ) ) );
      dout(7) << "added dir " << *cur->dir << endl;
    }
    if (dis->get_want().depth() == 0) break;
    
    // lookup dentry
    int dentry_auth = cur->dir->dentry_authority( dis->get_dentry(i) );
    if (dentry_auth != mds->get_nodeid()) {
      dout(7) << *cur->dir << "dentry " << dis->get_dentry(i) << " auth " << dentry_auth << ", i'm done." << endl;
      break;      // that's it for us!
    }

    // get inode
    CDentry *dn = cur->dir->lookup( dis->get_dentry(i) );
	
	/*
	if (dn && !dn->can_read()) { // xlocked?
	  dout(7) << "waiting on " << *dn << endl;
	  cur->dir->add_waiter(CDIR_WAIT_DNREAD,
						   dn->name,
						   new C_MDS_RetryMessage(mds, dis));
	  return;
	}
	*/
	
    if (dn) {
	  if (!dn->inode && dn->is_sync()) {
        dout(7) << "mds" << whoami << " dentry " << dis->get_dentry(i) << " null in " << *cur->dir << ", returning error" << endl;
		reply->set_flag_error_dn( dis->get_dentry(i) );
		break;   // don't replicate null but non-locked dentries.
	  }
	  
	  reply->add_dentry( dis->get_dentry(i), !dn->can_read() );
	  dout(7) << "added dentry " << *dn << endl;
	  
	  if (!dn->inode) break;  // we're done.
	}

	if (dn && dn->inode) {
		CInode *next = dn->inode;
        assert(next->is_auth());

        // add inode
		//int nonce = next->cached_by_add(dis->get_asker());
		reply->add_inode( next->replicate_to( dis->get_asker() ) );
		dout(7) << "added inode " << *next << endl;// " nonce=" << nonce<< endl;

		// descend
		cur = next;
    } else {
      // don't have inode?
      if (cur->dir->is_complete()) {
        // set error flag in reply
        dout(7) << "mds" << whoami << " dentry " << dis->get_dentry(i) << " not found in " << *cur->dir << ", returning error" << endl;
		reply->set_flag_error_dn( dis->get_dentry(i) );
		break;
      } else {
        // readdir
        dout(7) << "mds" << whoami << " incomplete dir contents for " << *cur->dir << ", fetching" << endl;

        //mds->mdstore->fetch_dir(cur->dir, NULL); //new C_MDS_RetryMessage(mds, dis));
        //break; // send what we have so far

        mds->mdstore->fetch_dir(cur->dir, new C_MDS_RetryMessage(mds, dis));
        return;
      }
    }
  }
       
  // how did we do.
  if (reply->is_empty()) {

    // discard empty reply
    delete reply;

    if ((cur->is_auth() || cur->is_proxy() || cur->dir->is_proxy()) &&
		!cur->dir->is_auth()) {
      // fwd to dir auth
	  int dirauth = cur->dir->authority();
	  if (dirauth == dis->get_asker()) {
		dout(7) << "from (new?) dir auth, dropping (obsolete) discover on floor." << endl;  // XXX FIXME is this right?
		//assert(dis->get_asker() == dis->get_source());  //might be a weird other loop.  either way, asker has it.
		delete dis;
	  } else {
		dout(7) << "fwd to dir auth " << dirauth << endl;
		mds->messenger->send_message( dis,
									  MSG_ADDR_MDS( dirauth ), MDS_PORT_CACHE, MDS_PORT_CACHE );
	  }
      return;
    }
	
    dout(7) << "i'm not auth or proxy, dropping (this empty reply).  i bet i just exported." << endl;
	//assert(0);
    
  } else {
    // send back to asker
    dout(7) << "sending result back to asker mds" << dis->get_asker() << endl;
    mds->messenger->send_message(reply,
                                 MSG_ADDR_MDS(dis->get_asker()), MDS_PORT_CACHE, MDS_PORT_CACHE);
  }

  // done.
  delete dis;
}


void MDCache::handle_discover_reply(MDiscoverReply *m) 
{
  // starting point
  CInode *cur;
  list<Context*> finished, error;
  
  if (m->has_root()) {
	// nowhere!
	dout(7) << "discover_reply root + " << m->get_path() << " " << m->get_num_inodes() << " inodes" << endl;
	assert(!root);
	assert(m->get_base_ino() == 0);
	assert(!m->has_base_dentry());
	assert(!m->has_base_dir());
	
	// add in root
	cur = new CInode(false);
	  
	m->get_inode(0).update_inode(cur);
	
	// root
	cur->state_set(CINODE_STATE_ROOT);
	set_root( cur );
	dout(7) << " got root: " << *cur << endl;

	// take waiters
	finished.swap(waiting_for_root);
  } else {
	// grab inode
	cur = get_inode(m->get_base_ino());
	
	if (!cur) {
	  dout(7) << "discover_reply don't have base ino " << hex << m->get_base_ino() << dec << ", dropping" << endl;
	  delete m;
	  return;
	}
	
	dout(7) << "discover_reply " << *cur << " + " << m->get_path() << ", have " << m->get_num_inodes() << " inodes" << endl;
  }

  // fyi
  if (m->is_flag_error_dir()) dout(7) << " flag error, dir" << endl;
  if (m->is_flag_error_dn()) dout(7) << " flag error, dentry = " << m->get_error_dentry() << endl;
  dout(10) << "depth is " << m->get_depth() << ", has_root = " << m->has_root() << endl;
  
  // loop over discover results.
  // indexese follow each ([[dir] dentry] inode) 
  // can start, end with any type.
  
  for (int i=m->has_root(); i<m->get_depth(); i++) {
	dout(10) << "discover_reply i=" << i << " cur " << *cur << endl;

	// dir
	if ((i >  0) ||
		(i == 0 && m->has_base_dir())) {
	  if (cur->dir) {
		// had it
		/* this is strange, but it happens when:
		   we discover multiple dentries under a dir.
		   bc, no flag to indicate a dir discover is underway, (as there is w/ a dentry one).
		   this is actually good, since (dir aside) they're asking for different information.
		*/
		dout(7) << "had " << *cur->dir;
		m->get_dir(i).update_dir(cur->dir);
		dout2(7) << ", now " << *cur->dir << endl;
	  } else {
		// add it (_replica_)
		cur->set_dir( new CDir(cur, mds, false) );
		m->get_dir(i).update_dir(cur->dir);
		dout(7) << "added " << *cur->dir << " nonce " << cur->dir->replica_nonce << endl;

		// get waiters
		cur->take_waiting(CINODE_WAIT_DIR, finished);
	  }
	}	

	// dentry error?
	if (i == m->get_depth()-1 && 
		m->is_flag_error_dn()) {
	  // error!
	  assert(cur->is_dir());
	  if (cur->dir) {
		dout(7) << " flag_error on dentry " << m->get_error_dentry() << ", triggering dentry?" << endl;
		cur->dir->take_waiting(CDIR_WAIT_DENTRY,
							   m->get_error_dentry(),
							   error);
	  } else {
		dout(7) << " flag_error on dentry " << m->get_error_dentry() << ", triggering dir?" << endl;
		cur->take_waiting(CINODE_WAIT_DIR, error);
	  }
	  break;
	}

	if (i >= m->get_num_dentries()) break;
	
	// dentry
	dout(7) << "i = " << i << " dentry is " << m->get_dentry(i) << endl;

	CDentry *dn = 0;
	if (i > 0 || 
		m->has_base_dentry()) {
	  dn = cur->dir->lookup( m->get_dentry(i) );
	  
	  if (dn) {
		dout(7) << "had " << *dn << endl;
	  } else {
		dn = cur->dir->add_dentry( m->get_dentry(i) );
		if (m->get_dentry_xlock(i)) {
		  dout(7) << " new dentry is xlock " << *dn << endl;
		  dn->lockstate = DN_LOCK_XLOCK;
		  dn->xlockedby = 0;
		}
		dout(7) << "added " << *dn << endl;
	  }

	  cur->dir->take_waiting(CDIR_WAIT_DENTRY,
							 m->get_dentry(i),
							 finished);
	}
	
	if (i >= m->get_num_inodes()) break;

	// inode
	dout(7) << "i = " << i << " ino is " << m->get_ino(i) << endl;
	CInode *in = get_inode( m->get_inode(i).get_ino() );
	assert(dn);
	
	if (in) {
	  dout(7) << "had " << *in << endl;
	  
	  // fix nonce
	  dout(7) << " my nonce is " << in->replica_nonce << ", taking from discover, which  has " << m->get_inode(i).get_replica_nonce() << endl;
	  in->replica_nonce = m->get_inode(i).get_replica_nonce();
	  
	  if (dn && in != dn->inode) {
		dout(7) << " but it's not linked via dentry " << *dn << endl;
		// link
		if (dn->inode) {
		  dout(7) << "dentry WAS linked to " << *dn->inode << endl;
		  assert(0);  // WTF.
		}
		dn->dir->link_inode(dn, in);
	  }
	}
	else {
	  assert(dn->inode == 0);  // better not be something else linked to this dentry...

	  // didn't have it.
	  in = new CInode(false);
	  
	  m->get_inode(i).update_inode(in);
		
	  // link in
	  add_inode( in );
	  dn->dir->link_inode(dn, in);
	  
	  dout(7) << "added " << *in << " nonce " << in->replica_nonce << endl;
	}
	
	// onward!
	cur = in;
  }

  // dir error at the end there?
  if (m->is_flag_error_dir()) {
	dout(7) << " flag_error on dir " << *cur << endl;
	assert(!cur->is_dir());
	cur->take_waiting(CINODE_WAIT_DIR, error);
  }

  // finish errors directly
  finish_contexts(error, -ENOENT);

  mds->queue_finished(finished);

  // done
  delete m;
}








/*
int MDCache::send_inode_updates(CInode *in)
{
  assert(in->is_auth());
  for (set<int>::iterator it = in->cached_by_begin(); 
	   it != in->cached_by_end(); 
	   it++) {
	dout(7) << "sending inode_update on " << *in << " to " << *it << endl;
	assert(*it != mds->get_nodeid());
	mds->messenger->send_message(new MInodeUpdate(in, in->get_cached_by_nonce(*it)),
								 MSG_ADDR_MDS(*it), MDS_PORT_CACHE,
								 MDS_PORT_CACHE);
  }

  return 0;
}


void MDCache::handle_inode_update(MInodeUpdate *m)
{
  inodeno_t ino = m->get_ino();
  CInode *in = get_inode(m->get_ino());
  if (!in) {
	//dout(7) << "inode_update on " << m->get_ino() << ", don't have it, ignoring" << endl;
	dout(7) << "inode_update on " << hex << m->get_ino() << dec << ", don't have it, sending expire" << endl;
	MCacheExpire *expire = new MCacheExpire(mds->get_nodeid());
	expire->add_inode(m->get_ino(), m->get_nonce());
	mds->messenger->send_message(expire,
								 m->get_source(), MDS_PORT_CACHE,
								 MDS_PORT_CACHE);
	goto out;
  }

  if (in->is_auth()) {
	dout(7) << "inode_update on " << *in << ", but i'm the authority!" << endl;
	assert(0); // this should never happen
  }
  
  dout(7) << "inode_update on " << *in << endl;

  // update! NOTE dir_auth is unaffected by this.
  in->decode_basic_state(m->get_payload());

 out:
  // done
  delete m;
}
*/



void MDCache::handle_cache_expire(MCacheExpire *m)
{
  int from = m->get_from();
  int source = MSG_ADDR_NUM(m->get_source());
  map<int, MCacheExpire*> proxymap;
  
  if (m->get_from() == source) {
	dout(7) << "cache_expire from " << from << endl;
  } else {
	dout(7) << "cache_expire from " << from << " via " << source << endl;
  }

  // inodes
  for (map<inodeno_t,int>::iterator it = m->get_inodes().begin();
	   it != m->get_inodes().end();
	   it++) {
	CInode *in = get_inode(it->first);
	int nonce = it->second;
	
	if (!in) {
	  dout(7) << "inode_expire on " << it->first << " from " << from << ", don't have it" << endl;
	  assert(in);  // OOPS  i should be authority, or recent authority (and thus frozen).
	}  
	if (!in->is_auth()) {
	  int newauth = in->authority();
	  dout(7) << "proxy inode expire on " << *in << " to " << newauth << endl;
	  assert(newauth >= 0);
	  assert(in->state_test(CINODE_STATE_PROXY));
	  if (proxymap.count(newauth) == 0) proxymap[newauth] = new MCacheExpire(from);
	  proxymap[newauth]->add_inode(it->first, it->second);
	  continue;
	}
	
	// check nonce
	if (from == mds->get_nodeid()) {
	  // my cache_expire, and the export_dir giving auth back to me crossed paths!  
	  // we can ignore this.  no danger of confusion since the two parties are both me.
	  dout(7) << "inode_expire on " << *in << " from mds" << from << " .. ME!  ignoring." << endl;
	} 
	else if (nonce == in->get_cached_by_nonce(from)) {
	  // remove from our cached_by
	  dout(7) << "inode_expire on " << *in << " from mds" << from << " cached_by was " << in->cached_by << endl;
	  in->cached_by_remove(from);
	  in->mds_caps_wanted.erase(from);
	  
	  // note: this code calls _eval more often than it needs to!
	  // fix lock
	  if (in->hardlock.is_gathering(from)) {
		in->hardlock.gather_set.erase(from);
		if (in->hardlock.gather_set.size() == 0)
		  inode_hard_eval(in);
	  }
	  if (in->filelock.is_gathering(from)) {
		in->filelock.gather_set.erase(from);
		if (in->filelock.gather_set.size() == 0)
		  inode_file_eval(in);
	  }
	  
	  // alone now?
	  if (!in->is_cached_by_anyone()) {
		inode_hard_eval(in);
		inode_file_eval(in);
	  }

	} 
	else {
	  // this is an old nonce, ignore expire.
	  dout(7) << "inode_expire on " << *in << " from mds" << from << " with old nonce " << nonce << " (current " << in->get_cached_by_nonce(from) << "), dropping" << endl;
	  assert(in->get_cached_by_nonce(from) > nonce);
	}
  }

  // dirs
  for (map<inodeno_t,int>::iterator it = m->get_dirs().begin();
	   it != m->get_dirs().end();
	   it++) {
	CInode *diri = get_inode(it->first);
	CDir *dir = diri->dir;
	int nonce = it->second;
	
	if (!dir) {
	  dout(7) << "dir_expire on " << it->first << " from " << from << ", don't have it" << endl;
	  assert(dir);  // OOPS  i should be authority, or recent authority (and thus frozen).
	}  
	if (!dir->is_auth()) {
	  int newauth = dir->authority();
	  dout(7) << "proxy dir expire on " << *dir << " to " << newauth << endl;
	  if (!dir->is_proxy()) dout(0) << "nonproxy dir expire? " << *dir << " .. auth is " << newauth << " .. expire is from " << from << endl;
	  assert(dir->is_proxy());
	  assert(newauth >= 0);
	  assert(dir->state_test(CDIR_STATE_PROXY));
	  if (proxymap.count(newauth) == 0) proxymap[newauth] = new MCacheExpire(from);
	  proxymap[newauth]->add_dir(it->first, it->second);
	  continue;
	}
	
	// check nonce
	if (from == mds->get_nodeid()) {
	  dout(7) << "dir_expire on " << *dir << " from mds" << from << " .. ME!  ignoring" << endl;
	} 
	else if (nonce == dir->get_open_by_nonce(from)) {
	  // remove from our cached_by
	  dout(7) << "dir_expire on " << *dir << " from mds" << from << " open_by was " << dir->open_by << endl;
	  dir->open_by_remove(from);
	} 
	else {
	  // this is an old nonce, ignore expire.
	  dout(7) << "dir_expire on " << *dir << " from mds" << from << " with old nonce " << nonce << " (current " << dir->get_open_by_nonce(from) << "), dropping" << endl;
	  assert(dir->get_open_by_nonce(from) > nonce);
	}
  }

  // send proxy forwards
  for (map<int, MCacheExpire*>::iterator it = proxymap.begin();
	   it != proxymap.end();
	   it++) {
	dout(7) << "sending proxy forward to " << it->first << endl;
	mds->messenger->send_message(it->second,
								 MSG_ADDR_MDS(it->first), MDS_PORT_CACHE, MDS_PORT_CACHE);
  }

  // done
  delete m;
}



int MDCache::send_dir_updates(CDir *dir, bool bcast)
{
  // this is an FYI, re: replication

  set<int> who = dir->open_by;
  if (bcast) 
	who = mds->get_cluster()->get_mds_set();
  
  dout(7) << "sending dir_update on " << *dir << " bcast " << bcast << " to " << who << endl;

  string path;
  dir->inode->make_path(path);

  int whoami = mds->get_nodeid();
  for (set<int>::iterator it = who.begin();
	   it != who.end();
	   it++) {
	if (*it == whoami) continue;
	//if (*it == except) continue;
	dout(7) << "sending dir_update on " << *dir << " to " << *it << endl;

	mds->messenger->send_message(new MDirUpdate(dir->ino(),
												dir->dir_rep,
												dir->dir_rep_by,
												path,
												bcast),
								 MSG_ADDR_MDS(*it), MDS_PORT_CACHE,
								 MDS_PORT_CACHE);
  }

  return 0;
}


void MDCache::handle_dir_update(MDirUpdate *m)
{
  CInode *in = get_inode(m->get_ino());
  if (!in || !in->dir) {
	dout(5) << "dir_update on " << hex << m->get_ino() << dec << ", don't have it" << endl;

	// discover it?
	if (m->should_discover()) {
	  m->tried_discover();  // only once!
	  vector<CDentry*> trace;
	  filepath path = m->get_path();

	  dout(5) << "trying discover on dir_update for " << path << endl;

	  int r = path_traverse(path, trace, true,
							m, new C_MDS_RetryMessage(mds, m),
							MDS_TRAVERSE_DISCOVER);
	  if (r > 0)
		return;
	  if (r == 0) {
		assert(in);
		open_remote_dir(in, new C_MDS_RetryMessage(mds, m));
		return;
	  }
	  assert(0);
	}

	goto out;
  }

  // update
  dout(1) << "dir_update on " << *in->dir << endl;
  in->dir->dir_rep = m->get_dir_rep();
  in->dir->dir_rep_by = m->get_dir_rep_by();
  
  // done
 out:
  delete m;
}





class C_MDC_DentryUnlink : public Context {
public:
  MDCache *mdc;
  CDentry *dn;
  CDir *dir;
  Context *c;
  C_MDC_DentryUnlink(MDCache *mdc, CDentry *dn, CDir *dir, Context *c) {
	this->mdc = mdc;
	this->dn = dn;
	this->dir = dir;
	this->c = c;
  }
  void finish(int r) {
	assert(r == 0);
	mdc->dentry_unlink_finish(dn, dir, c);
  }
};


// NAMESPACE FUN

void MDCache::dentry_unlink(CDentry *dn, Context *c)
{
  CDir *dir = dn->dir;
  string dname = dn->name;

  assert(dn->lockstate == DN_LOCK_XLOCK);

  // i need the inode to do any of this properly
  assert(dn->inode);

  // log it
  if (dn->inode) dn->inode->mark_unsafe();   // XXX ??? FIXME
  mds->mdlog->submit_entry(new EUnlink(dir, dn),
						   NULL);    // FIXME FIXME FIXME

  // tell replicas
  if (dir->is_open_by_anyone()) {
	for (set<int>::iterator it = dir->open_by_begin();
		 it != dir->open_by_end();
		 it++) {
	  dout(7) << "inode_unlink sending DentryUnlink to " << *it << endl;
	  
	  mds->messenger->send_message(new MDentryUnlink(dir->ino(), dn->name),
								   MSG_ADDR_MDS(*it), MDS_PORT_CACHE, MDS_PORT_CACHE);
	}

	// don't need ack.
  }


  // inode deleted?
  if (dn->is_primary()) {
	assert(dn->inode->is_auth());
	dn->inode->inode.nlink--;
	
	if (dn->inode->is_dir()) assert(dn->inode->inode.nlink == 0);  // no hard links on dirs

	// last link?
	if (dn->inode->inode.nlink == 0) {
	  // truly dangling	  
	  if (dn->inode->dir) {
		// mark dir clean too, since it now dne!
		assert(dn->inode->dir->is_auth());
		dn->inode->dir->state_set(CDIR_STATE_DELETED);
		dn->inode->dir->remove_null_dentries();
		dn->inode->dir->mark_clean();
	  }

	  // mark it clean, it's dead
	  if (dn->inode->is_dirty())
		dn->inode->mark_clean();
	  
	} else {
	  // migrate to inode file
	  dout(7) << "removed primary, but there are remote links, moving to inode file: " << *dn->inode << endl;

	  // dangling but still linked.  
	  assert(dn->inode->is_anchored());

	  // unlink locally
	  CInode *in = dn->inode;
	  dn->dir->unlink_inode( dn );
	  dn->mark_dirty();

	  // mark it dirty!
	  in->mark_dirty();

	  // update anchor to point to inode file+mds
	  vector<Anchor*> atrace;
	  in->make_anchor_trace(atrace);
	  assert(atrace.size() == 1);   // it's dangling
	  mds->anchormgr->update(in->ino(), atrace, 
							 new C_MDC_DentryUnlink(this, dn, dir, c));
	  return;
	}
  }
  else if (dn->is_remote()) {
	// need to dec nlink on primary
	if (dn->inode->is_auth()) {
	  // awesome, i can do it
	  dout(7) << "remote target is local, nlink--" << endl;
	  dn->inode->inode.nlink--;
	  dn->inode->mark_dirty();

	  if (( dn->inode->state_test(CINODE_STATE_DANGLING) && dn->inode->inode.nlink == 0) ||
		  (!dn->inode->state_test(CINODE_STATE_DANGLING) && dn->inode->inode.nlink == 1)) {
		dout(7) << "nlink=1+primary or 0+dangling, removing anchor" << endl;

		// remove anchor (async)
		mds->anchormgr->destroy(dn->inode->ino(), NULL);
	  }
	} else {
	  int auth = dn->inode->authority();
	  dout(7) << "remote target is remote, sending unlink request to " << auth << endl;

	  mds->messenger->send_message(new MInodeUnlink(dn->inode->ino(), mds->get_nodeid()),
								   MSG_ADDR_MDS(auth), MDS_PORT_CACHE, MDS_PORT_CACHE);

	  // unlink locally
	  CInode *in = dn->inode;
	  dn->dir->unlink_inode( dn );
	  dn->mark_dirty();

	  // add waiter
	  in->add_waiter(CINODE_WAIT_UNLINK, c);
	  return;
	}
  }
  else 
	assert(0);   // unlink on null dentry??
 
  // unlink locally
  dn->dir->unlink_inode( dn );
  dn->mark_dirty();

  // finish!
  dentry_unlink_finish(dn, dir, c);
}


void MDCache::dentry_unlink_finish(CDentry *dn, CDir *dir, Context *c)
{
  dout(7) << "dentry_unlink_finish on " << *dn << endl;
  string dname = dn->name;

  // unpin dir / unxlock
  dentry_xlock_finish(dn, true); // quiet, no need to bother replicas since they're already unlinking
  
  // did i empty out an imported dir?
  if (dir->is_import() && !dir->inode->is_root() && dir->get_size() == 0) 
	export_empty_import(dir);

  // wake up any waiters
  dir->take_waiting(CDIR_WAIT_ANY, dname, mds->finished_queue);

  c->finish(0);
}




void MDCache::handle_dentry_unlink(MDentryUnlink *m)
{
  CInode *diri = get_inode(m->get_dirino());
  CDir *dir;
  if (diri) dir = diri->dir;
  if (!diri || !dir) {
	dout(7) << "handle_dentry_unlink don't have dir " << hex << m->get_dirino() << dec << endl;
	delete m;
	return;
  }
  
  CDentry *dn = dir->lookup(m->get_dn());
  if (!dn) {
	dout(7) << "handle_dentry_unlink don't have dentry " << *dir << " dn " << m->get_dn() << endl;
  } else {
	dout(7) << "handle_dentry_unlink on " << *dn << endl;

	// dir?
	if (dn->inode) {
	  if (dn->inode->dir) {
		dn->inode->dir->state_set(CDIR_STATE_DELETED);
		dn->inode->dir->remove_null_dentries();
	  }
	}

	string dname = dn->name;
	
	// unlink
	dn->dir->remove_dentry(dn);
	
	// wake up
	//dir->finish_waiting(CDIR_WAIT_DNREAD, dname);
	dir->take_waiting(CDIR_WAIT_DNREAD, dname, mds->finished_queue);
  }

  delete m;
  return;
}


void MDCache::handle_inode_unlink(MInodeUnlink *m)
{
  CInode *in = get_inode(m->get_ino());
  assert(in);

  // proxy?
  if (in->is_proxy()) {
	dout(7) << "handle_inode_unlink proxy on " << *in << endl;
	mds->messenger->send_message(m,
								 MSG_ADDR_MDS(in->authority()), MDS_PORT_CACHE, MDS_PORT_CACHE);
	return;
  }
  assert(in->is_auth());

  // do it.
  dout(7) << "handle_inode_unlink nlink=" << in->inode.nlink << " on " << *in << endl;
  assert(in->inode.nlink > 0);
  in->inode.nlink--;

  if (in->state_test(CINODE_STATE_DANGLING)) {
	// already dangling.
	// last link?
	if (in->inode.nlink == 0) {
	  dout(7) << "last link, marking clean and removing anchor" << endl;
	  
	  in->mark_clean();       // mark it clean.
	  
	  // remove anchor (async)
	  mds->anchormgr->destroy(in->ino(), NULL);
	}
	else {
	  in->mark_dirty();
	}
  } else {
	// has primary link still.
	assert(in->inode.nlink >= 1);
	in->mark_dirty();

	if (in->inode.nlink == 1) {
	  dout(7) << "nlink=1, removing anchor" << endl;
	  
	  // remove anchor (async)
	  mds->anchormgr->destroy(in->ino(), NULL);
	}
  }

  // ack
  mds->messenger->send_message(new MInodeUnlinkAck(m->get_ino()),
							   MSG_ADDR_MDS(m->get_from()), MDS_PORT_CACHE, MDS_PORT_CACHE);
}

void MDCache::handle_inode_unlink_ack(MInodeUnlinkAck *m)
{
  CInode *in = get_inode(m->get_ino());
  assert(in);

  dout(7) << "handle_inode_unlink_ack on " << *in << endl;
  in->finish_waiting(CINODE_WAIT_UNLINK, 0);
}



// renaming!

/*
 * when initiator gets an ack back for a foreign rename
 */

class C_MDC_RenameNotifyAck : public Context {
  MDCache *mdc;
  CInode *in;
  int initiator;

public:
  C_MDC_RenameNotifyAck(MDCache *mdc, 
						CInode *in, int initiator) {
	this->mdc = mdc;
	this->in = in;
	this->initiator = initiator;
  }
  void finish(int r) {
	mdc->file_rename_ack(in, initiator);
  }
};



/************** initiator ****************/

/*
 * when we get MRenameAck (and rename is done, notifies gone out+acked, etc.)
 */
class C_MDC_RenameAck : public Context {
  MDCache *mdc;
  CDir *srcdir;
  CInode *in;
  Context *c;
public:
  C_MDC_RenameAck(MDCache *mdc, CDir *srcdir, CInode *in, Context *c) {
	this->mdc = mdc;
	this->srcdir = srcdir;
	this->in = in;
	this->c = c;
  }
  void finish(int r) {
	mdc->file_rename_finish(srcdir, in, c);
  }
};


void MDCache::file_rename(CDentry *srcdn, CDentry *destdn, Context *onfinish)
{
  assert(srcdn->is_xlocked());  // by me
  assert(destdn->is_xlocked());  // by me

  CDir *srcdir = srcdn->dir;
  string srcname = srcdn->name;
  
  CDir *destdir = destdn->dir;
  string destname = destdn->name;

  CInode *in = srcdn->inode;
  //Message *req = srcdn->xlockedby;


  // determine the players
  int srcauth = srcdir->dentry_authority(srcdn->name);
  int destauth = destdir->dentry_authority(destname);


  // FOREIGN rename?
  if (srcauth != mds->get_nodeid() ||
	  destauth != mds->get_nodeid()) {
	dout(7) << "foreign rename.  srcauth " << srcauth << ", destauth " << destauth << ", isdir " << srcdn->inode->is_dir() << endl;
	
	string destpath;
	destdn->make_path(destpath);

	if (destauth != mds->get_nodeid()) { 
	  // make sure dest has dir open.
	  dout(7) << "file_rename i'm not dest auth.  sending MRenamePrep to " << destauth << endl;
	  
	  // prep dest first, they must have the dir open!  rest will follow.
	  string srcpath;
	  srcdn->make_path(srcpath);
	  
	  MRenamePrep *m = new MRenamePrep(mds->get_nodeid(),  // i'm the initiator
									   srcdir->ino(), srcname, srcpath, 
									   destdir->ino(), destname, destpath,
									   srcauth);  // tell dest who src is (maybe even me)
	  mds->messenger->send_message(m,
								   MSG_ADDR_MDS(destauth), MDS_PORT_CACHE, MDS_PORT_CACHE);
	  
	  show_imports();
	  
	}
	
	else if (srcauth != mds->get_nodeid()) {
	  if (destauth == mds->get_nodeid()) {
		dout(7) << "file_rename dest auth, not src auth.  sending MRenameReq" << endl;	
	  } else {
		dout(7) << "file_rename neither src auth nor dest auth.  sending MRenameReq" << endl;	
	  }
	  
	  // srcdn not important on destauth, just request
	  MRenameReq *m = new MRenameReq(mds->get_nodeid(),  // i'm the initiator
									 srcdir->ino(), srcname, 
									 destdir->ino(), destname, destpath, destauth);  // tell src who dest is (they may not know)
	  mds->messenger->send_message(m,
								   MSG_ADDR_MDS(srcauth), MDS_PORT_CACHE, MDS_PORT_CACHE);
	}
	
	else
	  assert(0);

	// set waiter on the inode (is this the best place?)
	in->add_waiter(CINODE_WAIT_RENAMEACK, 
				   new C_MDC_RenameAck(this, 
									   srcdir, in, onfinish));
	return;
  }

  // LOCAL rename!
  assert(srcauth == mds->get_nodeid() && destauth == mds->get_nodeid());
  dout(7) << "file_rename src and dest auth, renaming locally (easy!)" << endl;
  
  // update our cache
  if (destdn->inode && destdn->inode->is_dirty())
	destdn->inode->mark_clean();

  rename_file(srcdn, destdn);
  
  // update imports/exports?
  if (in->is_dir() && in->dir) 
	fix_renamed_dir(srcdir, in, destdir, false);  // auth didnt change

  // mark dentries dirty
  srcdn->mark_dirty();
  destdn->mark_dirty();
  in->mark_dirty();
 
 
  // local, restrict notify to ppl with open dirs
  set<int> notify = srcdir->get_open_by();
  for (set<int>::iterator it = destdir->open_by_begin();
	   it != destdir->open_by_end();
	   it++)
	if (notify.count(*it) == 0) notify.insert(*it);
  
  if (notify.size()) {
	// warn + notify
	file_rename_warn(in, notify);
	file_rename_notify(in, srcdir, srcname, destdir, destname, notify, mds->get_nodeid());

	// wait for MRenameNotifyAck's
	in->add_waiter(CINODE_WAIT_RENAMENOTIFYACK,
				   new C_MDC_RenameNotifyAck(this, in, mds->get_nodeid()));  // i am initiator

	// wait for finish
	in->add_waiter(CINODE_WAIT_RENAMEACK,
				   new C_MDC_RenameAck(this, srcdir, in, onfinish));
  } else {
	// sweet, no notify necessary, we're done!
	file_rename_finish(srcdir, in, onfinish);
  }
}

void MDCache::handle_rename_ack(MRenameAck *m)
{
  CInode *in = get_inode(m->get_ino());
  assert(in);
  
  dout(7) << "handle_rename_ack on " << *in << endl;

  // all done!
  in->finish_waiting(CINODE_WAIT_RENAMEACK);

  delete m;
}

void MDCache::file_rename_finish(CDir *srcdir, CInode *in, Context *c)
{
  dout(10) << "file_rename_finish on " << *in << endl;

  // did i empty out an imported dir?  FIXME this check should go somewhere else???
  if (srcdir->is_import() && !srcdir->inode->is_root() && srcdir->get_size() == 0) 
	export_empty_import(srcdir);

  // finish our caller
  if (c) {
	c->finish(0);
	delete c;
  }
}


/************* src **************/


/** handle_rename_req
 * received by auth of src dentry (from init, or destauth if dir).  
 * src may not have dest dir open.
 * src will export inode, unlink|rename, and send MRename to dest.
 */
void MDCache::handle_rename_req(MRenameReq *m)
{
  // i am auth, i will have it.
  CInode *srcdiri = get_inode(m->get_srcdirino());
  CDir *srcdir = srcdiri->dir;
  CDentry *srcdn = srcdir->lookup(m->get_srcname());
  assert(srcdn);
  
  // do it
  file_rename_foreign_src(srcdn, 
						  m->get_destdirino(), m->get_destname(), m->get_destpath(), m->get_destauth(), 
						  m->get_initiator());
  delete m;
}


void MDCache::file_rename_foreign_src(CDentry *srcdn, 
									  inodeno_t destdirino, string& destname, string& destpath, int destauth, 
									  int initiator)
{
  dout(7) << "file_rename_foreign_src " << *srcdn << endl;

  CDir *srcdir = srcdn->dir;
  string srcname = srcdn->name;

  // (we're basically exporting this inode)
  CInode *in = srcdn->inode;
  assert(in);
  assert(in->is_auth());

  if (in->is_dir()) show_imports();

  // encode and export inode state
  bufferlist inode_state;
  encode_export_inode(in, inode_state, destauth);

  // send
  MRename *m = new MRename(initiator,
						   srcdir->ino(), srcdn->name, destdirino, destname,
						   inode_state);
  mds->messenger->send_message(m,
							   MSG_ADDR_MDS(destauth), MDS_PORT_CACHE, MDS_PORT_CACHE);
  
  // have dest?
  CInode *destdiri = get_inode(m->get_destdirino());
  CDir *destdir = 0;
  if (destdiri) destdir = destdiri->dir;
  CDentry *destdn = 0;
  if (destdir) destdn = destdir->lookup(m->get_destname());

  // discover src
  if (!destdn) {
	dout(7) << "file_rename_foreign_src doesn't have destdn, discovering " << destpath << endl;

	filepath destfilepath = destpath;
	vector<CDentry*> trace;
	int r = path_traverse(destfilepath, trace, true,
						  m, new C_MDS_RetryMessage(mds, m), 
						  MDS_TRAVERSE_DISCOVER);
	assert(r>0);
	return;
  }

  assert(destdn);

  // update our cache
  rename_file(srcdn, destdn);
  
  // update imports/exports?
  if (in->is_dir() && in->dir) 
	fix_renamed_dir(srcdir, in, destdir, true);  // auth changed

  srcdn->mark_dirty();

  // proxy!
  in->state_set(CINODE_STATE_PROXY);
  in->get(CINODE_PIN_PROXY);
  
  // generate notify list (everybody but src|dst) and send warnings
  set<int> notify;
  for (int i=0; i<mds->get_cluster()->get_num_mds(); i++) {
	if (i != mds->get_nodeid() &&  // except the source
		i != destauth)             // and the dest
	  notify.insert(i);
  }
  file_rename_warn(in, notify);


  // wait for MRenameNotifyAck's
  in->add_waiter(CINODE_WAIT_RENAMENOTIFYACK,
				 new C_MDC_RenameNotifyAck(this, in, initiator));
}

void MDCache::file_rename_warn(CInode *in,
							   set<int>& notify)
{
  // note gather list
  in->rename_waiting_for_ack = notify;

  // send
  for (set<int>::iterator it = notify.begin();
	   it != notify.end();
	   it++) {
	dout(10) << "file_rename_warn to " << *it << " for " << *in << endl;
	mds->messenger->send_message(new MRenameWarning(in->ino()),
								 MSG_ADDR_MDS(*it), MDS_PORT_CACHE, MDS_PORT_CACHE);
  }
}


void MDCache::handle_rename_notify_ack(MRenameNotifyAck *m)
{
  CInode *in = get_inode(m->get_ino());
  assert(in);
  dout(7) << "handle_rename_notify_ack on " << *in << endl;

  int source = MSG_ADDR_NUM(m->get_source());
  in->rename_waiting_for_ack.erase(source);
  if (in->rename_waiting_for_ack.empty()) {
	// last one!
	in->finish_waiting(CINODE_WAIT_RENAMENOTIFYACK, 0);
  } else {
	dout(7) << "still waiting for " << in->rename_waiting_for_ack << endl;
  }
}


void MDCache::file_rename_ack(CInode *in, int initiator) 
{
  // we got all our MNotifyAck's.

  // was i proxy (if not, it's cuz this was a local rename)
  if (in->state_test(CINODE_STATE_PROXY)) {
	dout(10) << "file_rename_ack clearing proxy bit on " << *in << endl;
	in->state_clear(CINODE_STATE_PROXY);
	in->put(CINODE_PIN_PROXY);
  }

  // done!
  if (initiator == mds->get_nodeid()) {
	// it's me, finish
	dout(7) << "file_rename_ack i am initiator, finishing" << endl;
	in->finish_waiting(CINODE_WAIT_RENAMEACK);
  } else {
	// send ack
	dout(7) << "file_rename_ack sending MRenameAck to initiator " << initiator << endl;
	mds->messenger->send_message(new MRenameAck(in->ino()),
								 MSG_ADDR_MDS(initiator), MDS_PORT_CACHE, MDS_PORT_CACHE);
  }  
}




/************ dest *************/

/** handle_rename_prep
 * received by auth of dest dentry to make sure they have src + dir open.
 * this is so that when they get the inode and dir, they can update exports etc properly.
 * will send MRenameReq to src.
 */
void MDCache::handle_rename_prep(MRenamePrep *m)
{
  // open src
  filepath srcpath = m->get_srcpath();
  vector<CDentry*> trace;
  int r = path_traverse(srcpath, trace, false,
						m, new C_MDS_RetryMessage(mds, m), 
						MDS_TRAVERSE_DISCOVER);

  if (r>0) return;

  // ok!
  CInode *srcin = trace[trace.size()-1]->inode;
  assert(srcin);
  
  dout(7) << "handle_rename_prep have srcin " << *srcin << endl;

  if (srcin->is_dir()) {
	if (!srcin->dir) {
	  dout(7) << "handle_rename_prep need to open dir" << endl;
	  open_remote_dir(srcin,
					  new C_MDS_RetryMessage(mds,m));
	  return;
	}

	dout(7) << "handle_rename_prep have dir " << *srcin->dir << endl;	
  }

  // pin
  srcin->get(CINODE_PIN_RENAMESRC);

  // send rename request
  MRenameReq *req = new MRenameReq(m->get_initiator(),  // i'm the initiator
								   m->get_srcdirino(), m->get_srcname(), 
								   m->get_destdirino(), m->get_destname(), m->get_destpath(),
								   mds->get_nodeid());  // i am dest
  mds->messenger->send_message(req,
							   MSG_ADDR_MDS(m->get_srcauth()), MDS_PORT_CACHE, MDS_PORT_CACHE);
  delete m;
  return;
}



/** handle_rename
 * received by auth of dest dentry.   includes exported inode info.
 * dest may not have srcdir open.
 */
void MDCache::handle_rename(MRename *m)
{
  // srcdn (required)
  CInode *srcdiri = get_inode(m->get_srcdirino());
  CDir *srcdir = srcdiri->dir;
  CDentry *srcdn = srcdir->lookup(m->get_srcname());
  string srcname = srcdn->name;
  assert(srcdn && srcdn->inode);

  dout(7) << "handle_rename srcdn " << *srcdn << endl;

  // destdn (required).  i am auth, so i will have it.
  CInode *destdiri = get_inode(m->get_destdirino());
  CDir *destdir = destdiri->dir;
  CDentry *destdn = destdir->lookup(m->get_destname());
  string destname = destdn->name;
  assert(destdn);
  
  dout(7) << "handle_rename destdn " << *destdn << endl;

  // note old dir auth
  int old_dir_auth = -1;
  if (srcdn->inode->dir) old_dir_auth = srcdn->inode->dir->authority();
	
  // rename replica into position
  if (destdn->inode && destdn->inode->is_dirty())
	destdn->inode->mark_clean();

  rename_file(srcdn, destdn);

  // decode + import inode (into new location start)
  int off = 0;
  // HACK
  bufferlist bufstate;
  bufstate.claim_append(m->get_inode_state());
  decode_import_inode(destdn, bufstate, off, MSG_ADDR_NUM(m->get_source()));

  CInode *in = destdn->inode;
  assert(in);

  // update imports/exports?
  if (in->is_dir()) {
	assert(in->dir);  // i had better already ahve it open.. see MRenamePrep
	fix_renamed_dir(srcdir, in, destdir, true,  // auth changed
					old_dir_auth);              // src is possibly new dir auth.
  }
  
  // mark dirty
  destdn->mark_dirty();
  in->mark_dirty();

  // unpin
  in->put(CINODE_PIN_RENAMESRC);

  // ok, send notifies.
  set<int> notify;
  for (int i=0; i<mds->get_cluster()->get_num_mds(); i++) {
	if (i != MSG_ADDR_NUM(m->get_source()) &&  // except the source
		i != mds->get_nodeid())  // and the dest
	  notify.insert(i);
  }
  file_rename_notify(in, srcdir, srcname, destdir, destname, notify, MSG_ADDR_NUM(m->get_source()));

  delete m;
}


void MDCache::file_rename_notify(CInode *in, 
								 CDir *srcdir, string& srcname, CDir *destdir, string& destname,
								 set<int>& notify,
								 int srcauth)
{
  /* NOTE: notify list might include myself */
  
  // tell
  string destdirpath;
  destdir->inode->make_path(destdirpath);
  
  for (set<int>::iterator it = notify.begin();
	   it != notify.end();
	   it++) {
	dout(10) << "file_rename_notify to " << *it << " for " << *in << endl;
	mds->messenger->send_message(new MRenameNotify(in->ino(),
												   srcdir->ino(),
												   srcname,
												   destdir->ino(),
												   destdirpath,
												   destname,
												   srcauth),
								 MSG_ADDR_MDS(*it), MDS_PORT_CACHE, MDS_PORT_CACHE);
  }
}



/************** bystanders ****************/

void MDCache::handle_rename_warning(MRenameWarning *m)
{
  // add to warning list
  stray_rename_warnings.insert( m->get_ino() );
  
  // did i already see the notify?
  if (stray_rename_notifies.count(m->get_ino())) {
	// i did, we're good.
	dout(7) << "handle_rename_warning on " << m->get_ino() << ".  already got notify." << endl;
	
	handle_rename_notify(stray_rename_notifies[m->get_ino()]);
	stray_rename_notifies.erase(m->get_ino());
  } else {
	dout(7) << "handle_rename_warning on " << m->get_ino() << ".  waiting for notify." << endl;
  }
  
  // done
  delete m;
}


void MDCache::handle_rename_notify(MRenameNotify *m)
{
  // FIXME: when we do hard links, i think we need to 
  // have srcdn and destdn both, or neither,  always!

  // did i see the warning yet?
  if (!stray_rename_warnings.count(m->get_ino())) {
	// wait for it.
	dout(7) << "handle_rename_notify on " << m->get_ino() << ", waiting for warning." << endl;
	stray_rename_notifies[m->get_ino()] = m;
	return;
  }

  dout(7) << "handle_rename_notify dir " << m->get_srcdirino() << " dn " << m->get_srcname() << " to dir " << m->get_destdirino() << " dname " << m->get_destname() << endl;
  
  // src
  CInode *srcdiri = get_inode(m->get_srcdirino());
  CDir *srcdir = 0;
  if (srcdiri) srcdir = srcdiri->dir;
  CDentry *srcdn = 0;
  if (srcdir) srcdn = srcdir->lookup(m->get_srcname());

  // dest
  CInode *destdiri = get_inode(m->get_destdirino());
  CDir *destdir = 0;
  if (destdiri) destdir = destdiri->dir;
  CDentry *destdn = 0;
  if (destdir) destdn = destdir->lookup(m->get_destname());

  // have both?
  list<Context*> finished;
  if (srcdn && destdir) {
	CInode *in = srcdn->inode;

	int old_dir_auth = -1;
	if (in && in->dir) old_dir_auth = in->dir->authority();

	if (!destdn) {
	  destdn = destdir->add_dentry(m->get_destname());  // create null dentry
	  destdn->lockstate = DN_LOCK_XLOCK;                // that's xlocked!
	}

	dout(7) << "handle_rename_notify renaming " << *srcdn << " to " << *destdn << endl;
	
	if (in) {
	  rename_file(srcdn, destdn);

	  // update imports/exports?
	  if (in && in->is_dir() && in->dir) {
		fix_renamed_dir(srcdir, in, destdir, false, old_dir_auth);  // auth didnt change
	  }
	} else {
	  dout(7) << " i don't have the inode (just null dentries)" << endl;
	}
	
  }

  else if (srcdn) {
	dout(7) << "handle_rename_notify no dest, but have src" << endl;
	dout(7) << "srcdn is " << *srcdn << endl;

	if (destdiri) {
	  dout(7) << "have destdiri, opening dir " << *destdiri << endl;
	  open_remote_dir(destdiri,
					  new C_MDS_RetryMessage(mds,m));
	} else {
	  filepath destdirpath = m->get_destdirpath();
	  dout(7) << "don't have destdiri even, doing traverse+discover on " << destdirpath << endl;
	  
	  vector<CDentry*> trace;
	  int r = path_traverse(destdirpath, trace, true,
							m, new C_MDS_RetryMessage(mds, m), 
							MDS_TRAVERSE_DISCOVER);
	  assert(r>0);
	}
	return;
  }

  else if (destdn) {
	dout(7) << "handle_rename_notify unlinking dst only " << *destdn << endl;
	if (destdn->inode) {
	  destdir->unlink_inode(destdn);
	}
  }
  
  else {
	dout(7) << "handle_rename_notify didn't have srcdn or destdn" << endl;
	assert(srcdn == 0 && destdn == 0);
  }
  
  mds->queue_finished(finished);


  // ack
  dout(10) << "sending RenameNotifyAck back to srcauth " << m->get_srcauth() << endl;
  MRenameNotifyAck *ack = new MRenameNotifyAck(m->get_ino());
  mds->messenger->send_message(ack,
							   MSG_ADDR_MDS(m->get_srcauth()), MDS_PORT_CACHE, MDS_PORT_CACHE);
  

  stray_rename_warnings.erase( m->get_ino() );
  delete m;
}









// file i/o -----------------------------------------

__uint64_t MDCache::issue_file_data_version(CInode *in)
{
  dout(7) << "issue_file_data_version on " << *in << endl;
  return in->inode.file_data_version;
}


Capability* MDCache::issue_new_caps(CInode *in,
									int mode,
									MClientRequest *req)
{
  dout(7) << "issue_new_caps for mode " << mode << " on " << *in << endl;
  
  // my needs
  int my_client = req->get_client();
  int my_want = 0;
  if (mode & FILE_MODE_R) my_want |= CAP_FILE_RDCACHE  | CAP_FILE_RD;
  if (mode & FILE_MODE_W) my_want |= CAP_FILE_WRBUFFER | CAP_FILE_WR;

  // register a capability
  Capability *cap = in->get_client_cap(my_client);
  if (!cap) {
	// new cap
	Capability c(my_want);
	in->add_client_cap(my_client, c);
	cap = in->get_client_cap(my_client);
  } else {
	// make sure it has sufficient caps
	if (cap->wanted() & ~my_want) {
	  // augment wanted caps for this client
	  cap->set_wanted( cap->wanted() | my_want );
	}
  }

  // suppress file cap messages for this guy for a few moments (we'll bundle with the open() reply)
  cap->set_suppress(true);
  int before = cap->pending();

  if (in->is_auth()) {
	// [auth] twiddle mode?
	inode_file_eval(in);
  } else {
	// [replica] tell auth about any new caps wanted
	request_inode_file_caps(in);
  }
	
  // issue caps (pot. incl new one)
  issue_caps(in);  // note: _eval above may have done this already...

  // re-issue whatever we can
  cap->issue(cap->pending());
  
  // ok, stop suppressing.
  cap->set_suppress(false);

  int now = cap->pending();
  if (before != now &&
	  (before & CAP_FILE_WR) == 0 &&
	  (now & CAP_FILE_WR)) {
	// FIXME FIXME FIXME
  }
  
  // twiddle file_data_version?
  if ((before & CAP_FILE_WRBUFFER) == 0 &&
	  (now & CAP_FILE_WRBUFFER)) {
	in->inode.file_data_version++;
	dout(7) << " incrementing file_data_version, now " << in->inode.file_data_version << " for " << *in << endl;
  }

  return cap;
}



bool MDCache::issue_caps(CInode *in)
{
  int allowed = in->filelock.caps_allowed(in->is_auth());
  dout(7) << "issue_caps filelock allows=" << cap_string(allowed) << " on " << *in << endl;

  // look at what allowed, needed caps conflict with
  int need_conflicts = 0;
  int issued_conflicts = 0;  
  for (map<int, Capability>::iterator it = in->client_caps.begin();
	   it != in->client_caps.end();
	   it++) {
	need_conflicts |= it->second.needed_conflicts();
	issued_conflicts |= it->second.issued_conflicts();
  }
  for (map<int, int>::iterator it = in->mds_caps_wanted.begin();
	   it != in->mds_caps_wanted.end();
	   it++) {
	need_conflicts |= Capability::conflicts( Capability::needed( it->second ) );
  }
  dout(10) << "  need conflicts: " << cap_string(need_conflicts) << endl;
  dout(10) << "issued conflicts: " << cap_string(issued_conflicts) << endl;
  
  // what's allowed, given this mix?
  allowed -= allowed & need_conflicts;
  allowed -= allowed & issued_conflicts;
  dout(10) << "         allowed: " << cap_string(allowed) << endl;

  // count conflicts with
  int nissued = 0;        

  // client caps
  for (map<int, Capability>::iterator it = in->client_caps.begin();
	   it != in->client_caps.end();
	   it++) {
	if (it->second.issued() != (it->second.wanted() & allowed)) {
	  // issue
	  nissued++;

	  int before = it->second.pending();
	  long seq = it->second.issue(it->second.wanted() & allowed);
	  int after = it->second.pending();

	  // twiddle file_data_version?
	  if (!(before & CAP_FILE_WRBUFFER) &&
		  (after & CAP_FILE_WRBUFFER)) {
		dout(7) << "   incrementing file_data_version for " << *in << endl;
		in->inode.file_data_version++;
	  }

	  if (seq > 0 && 
		  !it->second.is_suppress()) {
		dout(7) << "   sending MClientFileCaps to client" << it->first << " seq " << it->second.get_last_seq() << " new pending " << cap_string(it->second.pending()) << " was " << cap_string(before) << endl;
		mds->messenger->send_message(new MClientFileCaps(in->inode,
														 it->second.get_last_seq(),
														 it->second.pending(),
														 it->second.wanted()),
									 MSG_ADDR_CLIENT(it->first), 0, MDS_PORT_CACHE);
	  }
	}
  }

  return (nissued == 0);  // true if no re-issued, no callbacks
}



void MDCache::request_inode_file_caps(CInode *in)
{
  int wanted = in->get_caps_wanted();
  if (wanted != in->replica_caps_wanted) {
	in->replica_caps_wanted = wanted;

	int auth = in->authority();
	dout(7) << "request_inode_file_caps " << cap_string(in->replica_caps_wanted) 
			<< " on " << *in << " to mds" << auth << endl;
	assert(!in->is_auth());
	
	mds->messenger->send_message(new MInodeFileCaps(in->ino(), mds->get_nodeid(),
													in->replica_caps_wanted),
								 MSG_ADDR_MDS(auth), MDS_PORT_CACHE, MDS_PORT_CACHE);
  }
}

void MDCache::handle_inode_file_caps(MInodeFileCaps *m)
{
  CInode *in = get_inode(m->get_ino());
  assert(in);
  assert(in->is_auth() || in->is_proxy());
  
  dout(7) << "handle_inode_file_caps replica mds" << m->get_from() << " wants caps " << cap_string(m->get_caps()) << " on " << *in << endl;

  if (in->is_proxy()) {
	dout(7) << "proxy, fw" << endl;
	mds->messenger->send_message(m, MSG_ADDR_MDS(in->authority()), MDS_PORT_CACHE, MDS_PORT_CACHE);
	return;
  }

  if (m->get_caps())
	in->mds_caps_wanted[m->get_from()] = m->get_caps();
  else
	in->mds_caps_wanted.erase(m->get_from());

  inode_file_eval(in);
  delete m;
}


/*
 * note: we only get these from the client if
 * - we are calling back previously issued caps (fewer than the client previously had)
 * - or if the client releases (any of) its caps on its own
 */
void MDCache::handle_client_file_caps(MClientFileCaps *m)
{
  int client = MSG_ADDR_NUM(m->get_source());
  CInode *in = get_inode(m->get_ino());
  Capability *cap = 0;
  if (in) 
	cap = in->get_client_cap(client);

  if (!in || !cap) {
	if (!in) {
	  dout(7) << "handle_client_file_caps on unknown ino " << m->get_ino() << ", dropping" << endl;
	} else {
	  dout(7) << "handle_client_file_caps no cap for client" << client << " on " << *in << endl;
	}
	delete m;
	return;
  } 
  
  assert(cap);

  // filter wanted based on what we could ever give out (given auth/replica status)
  int wanted = m->get_wanted() & in->filelock.caps_allowed_ever(in->is_auth());
  
  dout(7) << "handle_client_file_caps seq " << m->get_seq() 
		  << " confirms caps " << cap_string(m->get_caps()) 
		  << " wants " << cap_string(wanted)
		  << " from client" << client
		  << " on " << *in 
		  << endl;  
  
  // update wanted
  if (cap->wanted() != wanted)
	cap->set_wanted(wanted);

  // confirm caps
  int had = cap->confirm_receipt(m->get_seq(), m->get_caps());
  int has = cap->confirmed();
  if (cap->is_null()) {
	dout(7) << " cap for client" << client << " is now null, removing from " << *in << endl;
	in->remove_client_cap(client);
	if (!in->is_auth())
	  request_inode_file_caps(in);

	// tell client.
	MClientFileCaps *r = new MClientFileCaps(in->inode, 
											 0, 0, 0,
											 MClientFileCaps::FILECAP_RELEASE);
	mds->messenger->send_message(r, m->get_source(), 0, MDS_PORT_CACHE);
  }

  // merge in atime?
  if (m->get_inode().atime > in->inode.atime) {
	  dout(7) << "  taking atime " << m->get_inode().atime << " > " 
			  << in->inode.atime << " for " << *in << endl;
	in->inode.atime = m->get_inode().atime;
  }
  
  if ((has|had) & CAP_FILE_WR) {
	bool dirty = false;

	// mtime
	if (m->get_inode().mtime > in->inode.mtime) {
	  dout(7) << "  taking mtime " << m->get_inode().mtime << " > " 
			  << in->inode.mtime << " for " << *in << endl;
	  in->inode.mtime = m->get_inode().mtime;
	  dirty = true;
	}
	// size
	if (m->get_inode().size > in->inode.size) {
	  dout(7) << "  taking size " << m->get_inode().size << " > " 
			  << in->inode.size << " for " << *in << endl;
	  in->inode.size = m->get_inode().size;
	  dirty = true;
	}

	if (dirty) 
	  mds->mdlog->submit_entry(new EInodeUpdate(in));
  }  

  // reevaluate, waiters
  inode_file_eval(in);
  in->finish_waiting(CINODE_WAIT_CAPS, 0);

  delete m;
}










// locks ----------------------------------------------------------------

/*


INODES:

= two types of inode metadata:
   hard  - uid/gid, mode
   file  - mtime, size
 ? atime - atime  (*)       <-- we want a lazy update strategy?

= correspondingly, two types of inode locks:
   hardlock - hard metadata
   filelock - file metadata

   -> These locks are completely orthogonal! 

= metadata ops and how they affect inode metadata:
        sma=size mtime atime
   HARD FILE OP
  files:
    R   RRR stat
    RW      chmod/chown
    R    W  touch   ?ctime
    R       openr
          W read    atime
    R       openw
    Wc      openwc  ?ctime
        WW  write   size mtime
            close 

  dirs:
    R     W readdir atime 
        RRR  ( + implied stats on files)
    Rc  WW  mkdir         (ctime on new dir, size+mtime on parent dir)
    R   WW  link/unlink/rename/rmdir  (size+mtime on dir)

  

= relationship to client (writers):

  - ops in question are
    - stat ... need reasonable value for mtime (+ atime?)
      - maybe we want a "quicksync" type operation instead of full lock
    - truncate ... need to stop writers for the atomic truncate operation
      - need a full lock




= modes
  - SYNC
              Rauth  Rreplica  Wauth  Wreplica
        sync
        




ALSO:

  dirlock  - no dir changes (prior to unhashing)
  denlock  - dentry lock    (prior to unlink, rename)

     
*/


void MDCache::handle_lock(MLock *m)
{
  switch (m->get_otype()) {
  case LOCK_OTYPE_IHARD:
	handle_lock_inode_hard(m);
	break;
	
  case LOCK_OTYPE_IFILE:
	handle_lock_inode_file(m);
	break;
	
  case LOCK_OTYPE_DIR:
	handle_lock_dir(m);
	break;
	
  case LOCK_OTYPE_DN:
	handle_lock_dn(m);
	break;

  default:
	dout(7) << "handle_lock got otype " << m->get_otype() << endl;
	assert(0);
	break;
  }
}
 


// ===============================
// hard inode metadata

bool MDCache::inode_hard_read_try(CInode *in, Context *con)
{
  dout(7) << "inode_hard_read_try on " << *in << endl;  

  // can read?  grab ref.
  if (in->hardlock.can_read(in->is_auth())) {
	return true;
  }
  
  if (in->is_auth()) {
	// auth
	assert(0);  // this shouldn't happen.
  } else {
	// replica
	
	// wait!
	dout(7) << "inode_hard_read_try waiting on " << *in << endl;
	in->add_waiter(CINODE_WAIT_HARDR, con);
  }
  
  return false;
}

bool MDCache::inode_hard_read_start(CInode *in, MClientRequest *m)
{
  dout(7) << "inode_hard_read_start  on " << *in << endl;  

  // can read?  grab ref.
  if (in->hardlock.can_read(in->is_auth())) {
	in->hardlock.get_read();
	return true;
  }
  
  // can't read, and replicated.

  if (in->is_auth()) {
	// auth
	assert(0);  // this shouldn't happen.
  } else {
	// replica

	// wait!
	dout(7) << "inode_hard_read_start waiting on " << *in << endl;
	in->add_waiter(CINODE_WAIT_HARDR, new C_MDS_RetryRequest(mds, m, in));
  }
  
  return false;
}


void MDCache::inode_hard_read_finish(CInode *in)
{
  // drop ref
  assert(in->hardlock.can_read(in->is_auth()));
  in->hardlock.put_read();

  dout(7) << "inode_hard_read_finish on " << *in << endl;
  
  //if (in->hardlock.get_nread() == 0) in->finish_waiting(CINODE_WAIT_HARDNORD);
}


bool MDCache::inode_hard_write_start(CInode *in, MClientRequest *m)
{
  dout(7) << "inode_hard_write_start  on " << *in << endl;

  // if not replicated, i can twiddle lock at will
  if (in->is_auth() &&
	  !in->is_cached_by_anyone() &&
	  in->hardlock.get_state() != LOCK_LOCK) 
	in->hardlock.set_state(LOCK_LOCK);
  
  // can write?  grab ref.
  if (in->hardlock.can_write(in->is_auth())) {
	assert(in->is_auth());
	if (!in->can_auth_pin()) {
	  dout(7) << "inode_hard_write_start waiting for authpinnable on " << *in << endl;
	  in->add_waiter(CINODE_WAIT_AUTHPINNABLE, new C_MDS_RetryRequest(mds, m, in));
	  return false;
	}

	in->auth_pin();  // ugh, can't condition this on nwrite==0 bc we twiddle that in handle_lock_*
	in->hardlock.get_write();
	return true;
  }
  
  // can't write, replicated.
  if (in->is_auth()) {
	// auth
	if (in->hardlock.can_write_soon(in->is_auth())) {
	  // just wait
	} else {
	  // initiate lock
	  inode_hard_lock(in);
	}
	
	dout(7) << "inode_hard_write_start waiting on " << *in << endl;
	in->add_waiter(CINODE_WAIT_HARDW, new C_MDS_RetryRequest(mds, m, in));

	return false;
  } else {
	// replica
	// fw to auth
	int auth = in->authority();
	dout(7) << "inode_hard_write_start " << *in << " on replica, fw to auth " << auth << endl;
	assert(auth != mds->get_nodeid());
	request_forward(m, auth);
	return false;
  }
}


void MDCache::inode_hard_write_finish(CInode *in)
{
  // drop ref
  assert(in->hardlock.can_write(in->is_auth()));
  in->hardlock.put_write();
  in->auth_unpin();
  dout(7) << "inode_hard_write_finish on " << *in << endl;
  
  // drop lock?
  if (in->hardlock.get_nwrite() == 0) {

	// auto-sync if alone.
	if (in->is_auth() &&
		!in->is_cached_by_anyone() &&
		in->hardlock.get_state() != LOCK_SYNC) 
	  in->hardlock.set_state(LOCK_SYNC);
	
	inode_hard_eval(in);
  }
}


void MDCache::inode_hard_eval(CInode *in)
{
  // finished gather?
  if (in->is_auth() &&
	  !in->hardlock.is_stable() &&
	  in->hardlock.gather_set.empty()) {
	dout(7) << "inode_hard_eval finished gather on " << *in << endl;
	switch (in->hardlock.get_state()) {
	case LOCK_GLOCKR:
	  in->hardlock.set_state(LOCK_LOCK);
	  
	  // waiters
	  in->hardlock.get_write();
	  in->finish_waiting(CINODE_WAIT_HARDRWB|CINODE_WAIT_HARDSTABLE);
	  in->hardlock.put_write();
	  break;
	  
	default:
	  assert(0);
	}
  }
  if (!in->hardlock.is_stable()) return;
  
  if (in->is_auth()) {

	// sync?
	if (in->is_cached_by_anyone() &&
		in->hardlock.get_nwrite() == 0 &&
		in->hardlock.get_state() != LOCK_SYNC) {
	  dout(7) << "inode_hard_eval stable, syncing " << *in << endl;
	  inode_hard_sync(in);
	}

  } else {
	// replica
  }
}


// mid

void MDCache::inode_hard_sync(CInode *in)
{
  dout(7) << "inode_hard_sync on " << *in << endl;
  assert(in->is_auth());
  
  // check state
  if (in->hardlock.get_state() == LOCK_SYNC)
	return; // already sync
  if (in->hardlock.get_state() == LOCK_GLOCKR) 
	assert(0); // um... hmm!
  assert(in->hardlock.get_state() == LOCK_LOCK);
  
  // hard data
  crope harddata;
  in->encode_hard_state(harddata);
  
  // bcast to replicas
  for (set<int>::iterator it = in->cached_by_begin(); 
	   it != in->cached_by_end(); 
	   it++) {
	MLock *m = new MLock(LOCK_AC_SYNC, mds->get_nodeid());
	m->set_ino(in->ino(), LOCK_OTYPE_IHARD);
	m->set_data(harddata);
	mds->messenger->send_message(m,
								 MSG_ADDR_MDS(*it), MDS_PORT_CACHE,
								 MDS_PORT_CACHE);
  }
  
  // change lock
  in->hardlock.set_state(LOCK_SYNC);
  
  // waiters?
  in->finish_waiting(CINODE_WAIT_HARDSTABLE);
}

void MDCache::inode_hard_lock(CInode *in)
{
  dout(7) << "inode_hard_lock on " << *in << " hardlock=" << in->hardlock << endl;  
  assert(in->is_auth());
  
  // check state
  if (in->hardlock.get_state() == LOCK_LOCK ||
	  in->hardlock.get_state() == LOCK_GLOCKR) 
	return;  // already lock or locking
  assert(in->hardlock.get_state() == LOCK_SYNC);
  
  // bcast to replicas
  for (set<int>::iterator it = in->cached_by_begin(); 
	   it != in->cached_by_end(); 
	   it++) {
	MLock *m = new MLock(LOCK_AC_LOCK, mds->get_nodeid());
	m->set_ino(in->ino(), LOCK_OTYPE_IHARD);
	mds->messenger->send_message(m,
								 MSG_ADDR_MDS(*it), MDS_PORT_CACHE,
								 MDS_PORT_CACHE);
  }
  
  // change lock
  in->hardlock.set_state(LOCK_GLOCKR);
  in->hardlock.init_gather(in->get_cached_by());
}





// messenger

void MDCache::handle_lock_inode_hard(MLock *m)
{
  assert(m->get_otype() == LOCK_OTYPE_IHARD);
  
  mds->logger->inc("lih");

  int from = m->get_asker();
  CInode *in = get_inode(m->get_ino());
  
  if (LOCK_AC_FOR_AUTH(m->get_action())) {
	// auth
	assert(in);
	assert(in->is_auth() || in->is_proxy());
	dout(7) << "handle_lock_inode_hard " << *in << " hardlock=" << in->hardlock << endl;  

	if (in->is_proxy()) {
	  // fw
	  int newauth = in->authority();
	  assert(newauth >= 0);
	  if (from == newauth) {
		dout(7) << "handle_lock " << m->get_ino() << " from " << from << ": proxy, but from new auth, dropping" << endl;
		delete m;
	  } else {
		dout(7) << "handle_lock " << m->get_ino() << " from " << from << ": proxy, fw to " << newauth << endl;
		mds->messenger->send_message(m,
									 MSG_ADDR_MDS(newauth), MDS_PORT_CACHE,
									 MDS_PORT_CACHE);
	  }
	  return;
	}
  } else {
	// replica
	if (!in) {
	  dout(7) << "handle_lock_inode_hard " << m->get_ino() << ": don't have it anymore" << endl;
	  /* do NOT nak.. if we go that route we need to duplicate all the nonce funkiness
	     to keep gather_set a proper/correct subset of cached_by.  better to use the existing
		 cacheexpire mechanism instead!
	  */
	  delete m;
	  return;
	}
	
	assert(!in->is_auth());
  }

  dout(7) << "handle_lock_inode_hard a=" << m->get_action() << " from " << from << " " << *in << " hardlock=" << in->hardlock << endl;  
 
  CLock *lock = &in->hardlock;
  
  switch (m->get_action()) {
	// -- replica --
  case LOCK_AC_SYNC:
	assert(lock->get_state() == LOCK_LOCK);
	
	{ // assim data
	  int off = 0;
	  in->decode_hard_state(m->get_data(), off);
	}
	
	// update lock
	lock->set_state(LOCK_SYNC);
	
	// no need to reply
	
	// waiters
	in->finish_waiting(CINODE_WAIT_HARDR|CINODE_WAIT_HARDSTABLE);
	break;
	
  case LOCK_AC_LOCK:
	assert(lock->get_state() == LOCK_SYNC);
	//||		   lock->get_state() == LOCK_GLOCKR);
	
	// wait for readers to finish?
	if (lock->get_nread() > 0) {
	  dout(7) << "handle_lock_inode_hard readers, waiting before ack on " << *in << endl;
	  lock->set_state(LOCK_GLOCKR);
	  in->add_waiter(CINODE_WAIT_HARDNORD,
					 new C_MDS_RetryMessage(mds,m));
	  assert(0);  // does this ever happen?  (if so, fix hard_read_finish, and CInodeExport.update_inode!)
	  return;
 	} else {

	  // update lock and reply
	  lock->set_state(LOCK_LOCK);
	  
	  {
		MLock *reply = new MLock(LOCK_AC_LOCKACK, mds->get_nodeid());
		reply->set_ino(in->ino(), LOCK_OTYPE_IHARD);
		mds->messenger->send_message(reply,
									 MSG_ADDR_MDS(from), MDS_PORT_CACHE,
									 MDS_PORT_CACHE);
	  }
	}
	break;
	
	
	// -- auth --
  case LOCK_AC_LOCKACK:
	assert(lock->state == LOCK_GLOCKR);
	assert(lock->gather_set.count(from));
	lock->gather_set.erase(from);

	if (lock->gather_set.size()) {
	  dout(7) << "handle_lock_inode_hard " << *in << " from " << from << ", still gathering " << lock->gather_set << endl;
	} else {
	  dout(7) << "handle_lock_inode_hard " << *in << " from " << from << ", last one" << endl;
	  inode_hard_eval(in);
	}
  }  
  delete m;
}




// =====================
// soft inode metadata


bool MDCache::inode_file_read_start(CInode *in, MClientRequest *m)
{
  dout(7) << "inode_file_read_start " << *in << " filelock=" << in->filelock << endl;  

  // can read?  grab ref.
  if (in->filelock.can_read(in->is_auth())) {
	in->filelock.get_read();
	return true;
  }
  
  // can't read, and replicated.
  if (in->filelock.can_read_soon(in->is_auth())) {
	// wait
	dout(7) << "inode_file_read_start can_read_soon " << *in << endl;
  } else {	
	if (in->is_auth()) {
	  // auth

	  // FIXME or qsync?

	  if (in->filelock.is_stable()) {
		inode_file_lock(in);     // lock, bc easiest to back off

		if (in->filelock.can_read(in->is_auth())) {
		  in->filelock.get_read();
		  
		  in->filelock.get_write();
		  in->finish_waiting(CINODE_WAIT_FILERWB|CINODE_WAIT_FILESTABLE);
		  in->filelock.put_write();
		  return true;
		}
	  } else {
		dout(7) << "inode_file_read_start waiting until stable on " << *in << ", filelock=" << in->filelock << endl;
		in->add_waiter(CINODE_WAIT_FILESTABLE, new C_MDS_RetryRequest(mds, m, in));
		return false;
	  }
	} else {
	  // replica
	  if (in->filelock.is_stable()) {

		// fw to auth
		int auth = in->authority();
		dout(7) << "inode_file_read_start " << *in << " on replica and async, fw to auth " << auth << endl;
		assert(auth != mds->get_nodeid());
		request_forward(m, auth);
		return false;
		
	  } else {
		// wait until stable
		dout(7) << "inode_file_read_start waiting until stable on " << *in << ", filelock=" << in->filelock << endl;
		in->add_waiter(CINODE_WAIT_FILESTABLE, new C_MDS_RetryRequest(mds, m, in));
		return false;
	  }
	}
  }

  // wait
  dout(7) << "inode_file_read_start waiting on " << *in << ", filelock=" << in->filelock << endl;
  in->add_waiter(CINODE_WAIT_FILER, new C_MDS_RetryRequest(mds, m, in));
		
  return false;
}


void MDCache::inode_file_read_finish(CInode *in)
{
  // drop ref
  assert(in->filelock.can_read(in->is_auth()));
  in->filelock.put_read();

  dout(7) << "inode_file_read_finish on " << *in << ", filelock=" << in->filelock << endl;

  if (in->filelock.get_nread() == 0) {
	in->finish_waiting(CINODE_WAIT_FILENORD);
	inode_file_eval(in);
  }
}


bool MDCache::inode_file_write_start(CInode *in, MClientRequest *m)
{
  // can write?  grab ref.
  if (in->filelock.can_write(in->is_auth())) {
	in->filelock.get_write();
	return true;
  }
  
  // can't write, replicated.
  if (in->is_auth()) {
	// auth
	if (in->filelock.can_write_soon(in->is_auth())) {
	  // just wait
	} else {
	  if (!in->filelock.is_stable()) {
		dout(7) << "inode_file_write_start on auth, waiting for stable on " << *in << endl;
		in->add_waiter(CINODE_WAIT_FILESTABLE, new C_MDS_RetryRequest(mds, m, in));
		return false;
	  }
	  
	  // initiate lock 
	  inode_file_lock(in);

	  if (in->filelock.can_write(in->is_auth())) {
		in->filelock.get_write();
		
		in->filelock.get_read();
		in->finish_waiting(CINODE_WAIT_FILERWB|CINODE_WAIT_FILESTABLE);
		in->filelock.put_read();
		return true;
	  }
	}
	
	dout(7) << "inode_file_write_start on auth, waiting for write on " << *in << endl;
	in->add_waiter(CINODE_WAIT_FILEW, new C_MDS_RetryRequest(mds, m, in));
	return false;
  } else {
	// replica
	// fw to auth
	int auth = in->authority();
	dout(7) << "inode_file_write_start " << *in << " on replica, fw to auth " << auth << endl;
	assert(auth != mds->get_nodeid());
	request_forward(m, auth);
	return false;
  }
}


void MDCache::inode_file_write_finish(CInode *in)
{
  // drop ref
  assert(in->filelock.can_write(in->is_auth()));
  in->filelock.put_write();
  dout(7) << "inode_file_write_finish on " << *in << ", filelock=" << in->filelock << endl;
  
  // drop lock?
  if (in->filelock.get_nwrite() == 0) {
	in->finish_waiting(CINODE_WAIT_FILENOWR);
	inode_file_eval(in);
  }
}


/*
 * ...
 *
 * also called after client caps are acked to us
 * - checks if we're in unstable sfot state and can now move on to next state
 * - checks if soft state should change (eg bc last writer closed)
 */

void MDCache::inode_file_eval(CInode *in)
{
  int issued = in->get_caps_issued();

  // [auth] finished gather?
  if (in->is_auth() &&
	  !in->filelock.is_stable() &&
	  in->filelock.gather_set.size() == 0) {
	dout(7) << "inode_file_eval finished mds gather on " << *in << endl;

	switch (in->filelock.get_state()) {
	  // to lock
	case LOCK_GLOCKR:
	case LOCK_GLOCKM:
	case LOCK_GLOCKW:
	  if (issued == 0) {
		in->filelock.set_state(LOCK_LOCK);
		
		// waiters
		in->filelock.get_read();
		in->filelock.get_write();
		in->finish_waiting(CINODE_WAIT_FILERWB|CINODE_WAIT_FILESTABLE);
		in->filelock.put_read();
		in->filelock.put_write();
	  }
	  break;
	  
	  // to mixed
	case LOCK_GMIXEDR:
	  if ((issued & ~(CAP_FILE_RD)) == 0) {
		in->filelock.set_state(LOCK_MIXED);
		in->finish_waiting(CINODE_WAIT_FILESTABLE);
	  }
	  break;

	case LOCK_GMIXEDW:
	  if ((issued & ~(CAP_FILE_WR)) == 0) {
		in->filelock.set_state(LOCK_MIXED);

		if (in->is_cached_by_anyone()) {
		  // data
		  crope softdata;
		  in->encode_file_state(softdata);
		  
		  // bcast to replicas
		  for (set<int>::iterator it = in->cached_by_begin(); 
			   it != in->cached_by_end(); 
			   it++) {
			MLock *m = new MLock(LOCK_AC_MIXED, mds->get_nodeid());
			m->set_ino(in->ino(), LOCK_OTYPE_IFILE);
			m->set_data(softdata);
			mds->messenger->send_message(m,
										 MSG_ADDR_MDS(*it), MDS_PORT_CACHE,
										 MDS_PORT_CACHE);
		  }
		}

		in->finish_waiting(CINODE_WAIT_FILESTABLE);
	  }
	  break;

	  // to wronly
	case LOCK_GWRONLYR:
	  if (issued == 0) {
		in->filelock.set_state(LOCK_WRONLY);
		in->finish_waiting(CINODE_WAIT_FILESTABLE);
	  }
	  break;

	case LOCK_GWRONLYM:
	  if ((issued & ~CAP_FILE_WR) == 0) {
		in->filelock.set_state(LOCK_WRONLY);
		in->finish_waiting(CINODE_WAIT_FILESTABLE);
	  }
	  break;
	  
	  // to sync
	case LOCK_GSYNCW:
	case LOCK_GSYNCM:
	  if ((issued & ~(CAP_FILE_RD)) == 0) {
		in->filelock.set_state(LOCK_SYNC);
		
		{ // bcast data to replicas
		  crope softdata;
		  in->encode_file_state(softdata);
		  
		  for (set<int>::iterator it = in->cached_by_begin(); 
			   it != in->cached_by_end(); 
			   it++) {
			MLock *reply = new MLock(LOCK_AC_SYNC, mds->get_nodeid());
			reply->set_ino(in->ino(), LOCK_OTYPE_IFILE);
			reply->set_data(softdata);
			mds->messenger->send_message(reply,
										 MSG_ADDR_MDS(*it), MDS_PORT_CACHE,
										 MDS_PORT_CACHE);
		  }
		}
		
		// waiters
		in->filelock.get_read();
		in->finish_waiting(CINODE_WAIT_FILER|CINODE_WAIT_FILESTABLE);
		in->filelock.put_read();
	  }
	  break;
	  
	default: 
	  assert(0);
	}
  }
  
  // [replica] finished caps gather?
  if (!in->is_auth() &&
	  !in->filelock.is_stable()) {
	switch (in->filelock.get_state()) {
	case LOCK_GMIXEDR:
	  if ((issued & ~(CAP_FILE_RD)) == 0) {
		in->filelock.set_state(LOCK_MIXED);
		
		// ack
		MLock *reply = new MLock(LOCK_AC_MIXEDACK, mds->get_nodeid());
		reply->set_ino(in->ino(), LOCK_OTYPE_IFILE);
		mds->messenger->send_message(reply,
									 MSG_ADDR_MDS(in->authority()), MDS_PORT_CACHE,
									 MDS_PORT_CACHE);
	  }
	  break;

	case LOCK_GLOCKR:
	  if (issued == 0) {
		in->filelock.set_state(LOCK_LOCK);
		
		// ack
		MLock *reply = new MLock(LOCK_AC_LOCKACK, mds->get_nodeid());
		reply->set_ino(in->ino(), LOCK_OTYPE_IFILE);
		mds->messenger->send_message(reply,
									 MSG_ADDR_MDS(in->authority()), MDS_PORT_CACHE,
									 MDS_PORT_CACHE);
	  }
	  break;

	default:
	  assert(0);
	}
  }

  // !stable -> do nothing.
  if (!in->filelock.is_stable()) return; 


  // stable.
  assert(in->filelock.is_stable());

  if (in->is_auth()) {
	// [auth]
	int wanted = in->get_caps_wanted();
	dout(7) << "inode_file_eval wanted=" << cap_string(wanted) << "  filelock=" << in->filelock << endl;

	// * -> wronly?
	if (in->filelock.get_nread() == 0 &&
		in->filelock.get_nwrite() == 0 &&
		!(wanted & CAP_FILE_RD) &&
		(wanted & CAP_FILE_WR) &&
		in->filelock.get_state() != LOCK_WRONLY) {
	  dout(7) << "inode_file_eval stable, bump to wronly " << *in << ", filelock=" << in->filelock << endl;
	  inode_file_wronly(in);
	}

	// * -> mixed?
	else if (in->filelock.get_nread() == 0 &&
			 in->filelock.get_nwrite() == 0 &&
			 (wanted & CAP_FILE_RD) &&
			 (wanted & CAP_FILE_WR) &&
			 in->filelock.get_state() != LOCK_MIXED) {
	  dout(7) << "inode_file_eval stable, bump to mixed " << *in << ", filelock=" << in->filelock << endl;
	  inode_file_mixed(in);
	}

	// * -> sync?
	else if (in->filelock.get_nwrite() == 0 &&
			 !(wanted & CAP_FILE_WR) &&
			 ((wanted & CAP_FILE_RD) || in->is_cached_by_anyone()) &&
			 in->filelock.get_state() != LOCK_SYNC) {
	  dout(7) << "inode_file_eval stable, bump to sync " << *in << ", filelock=" << in->filelock << endl;
	  inode_file_sync(in);
	}

	// * -> lock?  (if not replicated or open)
	else if (!in->is_cached_by_anyone() &&
			 wanted == 0 &&
			 in->filelock.get_state() != LOCK_LOCK) {
	  inode_file_lock(in);
	}
	
  } else {
	// replica
	// recall? check wiaters?  XXX
  }
}


// mid

bool MDCache::inode_file_sync(CInode *in)
{
  dout(7) << "inode_file_sync " << *in << " filelock=" << in->filelock << endl;  

  assert(in->is_auth());

  // check state
  if (in->filelock.get_state() == LOCK_SYNC ||
	  in->filelock.get_state() == LOCK_GSYNCW ||
	  in->filelock.get_state() == LOCK_GSYNCM)
	return true;

  assert(in->filelock.is_stable());

  int issued = in->get_caps_issued();

  assert((in->get_caps_wanted() & CAP_FILE_WR) == 0);

  if (in->filelock.get_state() == LOCK_LOCK) {
	if (in->is_cached_by_anyone()) {
	  // soft data
	  crope softdata;
	  in->encode_file_state(softdata);
	  
	  // bcast to replicas
	  for (set<int>::iterator it = in->cached_by_begin(); 
		   it != in->cached_by_end(); 
		   it++) {
		MLock *m = new MLock(LOCK_AC_SYNC, mds->get_nodeid());
		m->set_ino(in->ino(), LOCK_OTYPE_IFILE);
		m->set_data(softdata);
		mds->messenger->send_message(m,
									 MSG_ADDR_MDS(*it), MDS_PORT_CACHE,
									 MDS_PORT_CACHE);
	  }
	}

	// change lock
	in->filelock.set_state(LOCK_SYNC);

	// reissue caps
	issue_caps(in);
	return true;
  }

  else if (in->filelock.get_state() == LOCK_MIXED) {
	// writers?
	if (issued & CAP_FILE_WR) {
	  // gather client write caps
	  in->filelock.set_state(LOCK_GSYNCM);
	  issue_caps(in);
	} else {
	  // no writers, go straight to sync

	  if (in->is_cached_by_anyone()) {
		// bcast to replicas
		for (set<int>::iterator it = in->cached_by_begin(); 
			 it != in->cached_by_end(); 
			 it++) {
		  MLock *m = new MLock(LOCK_AC_SYNC, mds->get_nodeid());
		  m->set_ino(in->ino(), LOCK_OTYPE_IFILE);
		  mds->messenger->send_message(m,
									   MSG_ADDR_MDS(*it), MDS_PORT_CACHE,
									   MDS_PORT_CACHE);
		}
	  }
	
	  // change lock
	  in->filelock.set_state(LOCK_SYNC);
	}
	return false;
  }

  else if (in->filelock.get_state() == LOCK_WRONLY) {
	// writers?
	if (issued & CAP_FILE_WR) {
	  // gather client write caps
	  in->filelock.set_state(LOCK_GSYNCW);
	  issue_caps(in);
	} else {
	  // no writers, go straight to sync
	  if (in->is_cached_by_anyone()) {
		// bcast to replicas
		for (set<int>::iterator it = in->cached_by_begin(); 
			 it != in->cached_by_end(); 
			 it++) {
		  MLock *m = new MLock(LOCK_AC_SYNC, mds->get_nodeid());
		  m->set_ino(in->ino(), LOCK_OTYPE_IFILE);
		  mds->messenger->send_message(m,
									   MSG_ADDR_MDS(*it), MDS_PORT_CACHE,
									   MDS_PORT_CACHE);
		}
	  }

	  // change lock
	  in->filelock.set_state(LOCK_SYNC);
	}
	return false;
  }
  else 
	assert(0); // wtf.

  return false;
}


void MDCache::inode_file_lock(CInode *in)
{
  dout(7) << "inode_file_lock " << *in << " filelock=" << in->filelock << endl;  

  assert(in->is_auth());
  
  // check state
  if (in->filelock.get_state() == LOCK_LOCK ||
	  in->filelock.get_state() == LOCK_GLOCKR ||
	  in->filelock.get_state() == LOCK_GLOCKM ||
	  in->filelock.get_state() == LOCK_GLOCKW) 
	return;  // lock or locking

  assert(in->filelock.is_stable());

  int issued = in->get_caps_issued();

  if (in->filelock.get_state() == LOCK_SYNC) {
	if (in->is_cached_by_anyone()) {
	  // bcast to replicas
	  for (set<int>::iterator it = in->cached_by_begin(); 
		   it != in->cached_by_end(); 
		   it++) {
		MLock *m = new MLock(LOCK_AC_LOCK, mds->get_nodeid());
		m->set_ino(in->ino(), LOCK_OTYPE_IFILE);
		mds->messenger->send_message(m,
									 MSG_ADDR_MDS(*it), MDS_PORT_CACHE,
									 MDS_PORT_CACHE);
	  }
	  in->filelock.init_gather(in->get_cached_by());
	  
	  // change lock
	  in->filelock.set_state(LOCK_GLOCKR);

	  // call back caps
	  if (issued) 
		issue_caps(in);
	} else {
	  if (issued) {
		// call back caps
		in->filelock.set_state(LOCK_GLOCKR);
		issue_caps(in);
	  } else {
		in->filelock.set_state(LOCK_LOCK);
	  }
	}
  }

  else if (in->filelock.get_state() == LOCK_MIXED) {
	if (in->is_cached_by_anyone()) {
	  // bcast to replicas
	  for (set<int>::iterator it = in->cached_by_begin(); 
		   it != in->cached_by_end(); 
		   it++) {
		MLock *m = new MLock(LOCK_AC_LOCK, mds->get_nodeid());
		m->set_ino(in->ino(), LOCK_OTYPE_IFILE);
		mds->messenger->send_message(m,
									 MSG_ADDR_MDS(*it), MDS_PORT_CACHE,
									 MDS_PORT_CACHE);
	  }
	  in->filelock.init_gather(in->get_cached_by());

	  // change lock
	  in->filelock.set_state(LOCK_GLOCKM);
	  
	  // call back caps
	  issue_caps(in);
	} else {
	  //assert(issued);  // ??? -sage 2/19/06
	  if (issued) {
		// change lock
		in->filelock.set_state(LOCK_GLOCKM);
		
		// call back caps
		issue_caps(in);
	  } else {
		in->filelock.set_state(LOCK_LOCK);
	  }
	}
	  
  }
  else if (in->filelock.get_state() == LOCK_WRONLY) {
	if (issued & CAP_FILE_WR) {
	  // change lock
	  in->filelock.set_state(LOCK_GLOCKW);
  
	  // call back caps
	  issue_caps(in);
	} else {
	  in->filelock.set_state(LOCK_LOCK);
	}
  }
  else 
	assert(0); // wtf.
}


void MDCache::inode_file_mixed(CInode *in)
{
  dout(7) << "inode_file_mixed " << *in << " filelock=" << in->filelock << endl;  

  assert(in->is_auth());
  
  // check state
  if (in->filelock.get_state() == LOCK_GMIXEDR ||
	  in->filelock.get_state() == LOCK_GMIXEDW)
	return;     // mixed or mixing

  assert(in->filelock.is_stable());

  int issued = in->get_caps_issued();

  if (in->filelock.get_state() == LOCK_SYNC) {
	if (in->is_cached_by_anyone()) {
	  // bcast to replicas
	  for (set<int>::iterator it = in->cached_by_begin(); 
		   it != in->cached_by_end(); 
		   it++) {
		MLock *m = new MLock(LOCK_AC_MIXED, mds->get_nodeid());
		m->set_ino(in->ino(), LOCK_OTYPE_IFILE);
		mds->messenger->send_message(m,
									 MSG_ADDR_MDS(*it), MDS_PORT_CACHE,
									 MDS_PORT_CACHE);
	  }
	  in->filelock.init_gather(in->get_cached_by());
	
	  in->filelock.set_state(LOCK_GMIXEDR);
	  issue_caps(in);
	} else {
	  if (issued) {
		in->filelock.set_state(LOCK_GMIXEDR);
		issue_caps(in);
	  } else {
		in->filelock.set_state(LOCK_MIXED);
	  }
	}
  }

  else if (in->filelock.get_state() == LOCK_LOCK) {
	if (in->is_cached_by_anyone()) {
	  // data
	  crope softdata;
	  in->encode_file_state(softdata);
	  
	  // bcast to replicas
	  for (set<int>::iterator it = in->cached_by_begin(); 
		   it != in->cached_by_end(); 
		   it++) {
		MLock *m = new MLock(LOCK_AC_MIXED, mds->get_nodeid());
		m->set_ino(in->ino(), LOCK_OTYPE_IFILE);
		m->set_data(softdata);
		mds->messenger->send_message(m,
									 MSG_ADDR_MDS(*it), MDS_PORT_CACHE,
									 MDS_PORT_CACHE);
	  }
	}

	// change lock
	in->filelock.set_state(LOCK_MIXED);
	issue_caps(in);
  }

  else if (in->filelock.get_state() == LOCK_WRONLY) {
	if (issued & CAP_FILE_WRBUFFER) {
	  // gather up WRBUFFER caps
	  in->filelock.set_state(LOCK_GMIXEDW);
	  issue_caps(in);
	}
	else if (in->is_cached_by_anyone()) {
	  // bcast to replicas
	  for (set<int>::iterator it = in->cached_by_begin(); 
		   it != in->cached_by_end(); 
		   it++) {
		MLock *m = new MLock(LOCK_AC_MIXED, mds->get_nodeid());
		m->set_ino(in->ino(), LOCK_OTYPE_IFILE);
		mds->messenger->send_message(m,
									 MSG_ADDR_MDS(*it), MDS_PORT_CACHE,
									 MDS_PORT_CACHE);
	  }
	  in->filelock.set_state(LOCK_MIXED);
	  issue_caps(in);
	} else {
	  in->filelock.set_state(LOCK_MIXED);
	  issue_caps(in);
	}
  }

  else 
	assert(0); // wtf.
}


void MDCache::inode_file_wronly(CInode *in)
{
  dout(7) << "inode_file_wronly " << *in << " filelock=" << in->filelock << endl;  

  assert(in->is_auth());

  // check state
  if (in->filelock.get_state() == LOCK_WRONLY ||
	  in->filelock.get_state() == LOCK_GWRONLYR ||
	  in->filelock.get_state() == LOCK_GWRONLYM)
	return; 

  assert(in->filelock.is_stable());

  int issued = in->get_caps_issued();
  assert((in->get_caps_wanted() & CAP_FILE_RD) == 0);

  if (in->filelock.get_state() == LOCK_SYNC) {
	if (in->is_cached_by_anyone()) {
	  // bcast to replicas
	  for (set<int>::iterator it = in->cached_by_begin(); 
		   it != in->cached_by_end(); 
		   it++) {
		MLock *m = new MLock(LOCK_AC_LOCK, mds->get_nodeid());
		m->set_ino(in->ino(), LOCK_OTYPE_IFILE);
		mds->messenger->send_message(m,
									 MSG_ADDR_MDS(*it), MDS_PORT_CACHE,
									 MDS_PORT_CACHE);
	  }
	  in->filelock.init_gather(in->get_cached_by());
	  
	  // change lock
	  in->filelock.set_state(LOCK_GWRONLYR);

	  // call back caps
	  if (issued & CAP_FILE_RD)
		issue_caps(in);
	} else {
	  if (issued & CAP_FILE_RD) {
		in->filelock.set_state(LOCK_GWRONLYR);
		issue_caps(in);
	  } else {
		in->filelock.set_state(LOCK_WRONLY);
		issue_caps(in);
	  }
	}
  }

  else if (in->filelock.get_state() == LOCK_LOCK) {
	// change lock.  ignore replicas; they don't know about WRONLY.
	in->filelock.set_state(LOCK_WRONLY);
	issue_caps(in);
  }

  else if (in->filelock.get_state() == LOCK_MIXED) {
	if (in->is_cached_by_anyone()) {
	  // bcast to replicas
	  for (set<int>::iterator it = in->cached_by_begin(); 
		   it != in->cached_by_end(); 
		   it++) {
		MLock *m = new MLock(LOCK_AC_LOCK, mds->get_nodeid());
		m->set_ino(in->ino(), LOCK_OTYPE_IFILE);
		mds->messenger->send_message(m,
									 MSG_ADDR_MDS(*it), MDS_PORT_CACHE,
									 MDS_PORT_CACHE);
	  }
	  in->filelock.init_gather(in->get_cached_by());
	  
	  // change lock
	  in->filelock.set_state(LOCK_GWRONLYM);
	} else {
	  in->filelock.set_state(LOCK_WRONLY);
	  issue_caps(in);
	}
  }

  else 
	assert(0);
}

// messenger

void MDCache::handle_lock_inode_file(MLock *m)
{
  assert(m->get_otype() == LOCK_OTYPE_IFILE);
  
  mds->logger->inc("lif");

  CInode *in = get_inode(m->get_ino());
  int from = m->get_asker();

  if (LOCK_AC_FOR_AUTH(m->get_action())) {
	// auth
	assert(in);
	assert(in->is_auth() || in->is_proxy());
	dout(7) << "handle_lock_inode_file " << *in << " hardlock=" << in->hardlock << endl;  
		
	if (in->is_proxy()) {
	  // fw
	  int newauth = in->authority();
	  assert(newauth >= 0);
	  if (from == newauth) {
		dout(7) << "handle_lock " << m->get_ino() << " from " << from << ": proxy, but from new auth, dropping" << endl;
		delete m;
	  } else {
		dout(7) << "handle_lock " << m->get_ino() << " from " << from << ": proxy, fw to " << newauth << endl;
		mds->messenger->send_message(m,
									 MSG_ADDR_MDS(newauth), MDS_PORT_CACHE,
									 MDS_PORT_CACHE);
	  }
	  return;
	}
  } else {
	// replica
	if (!in) {
	  // drop it.  don't nak.
	  dout(7) << "handle_lock " << m->get_ino() << ": don't have it anymore" << endl;
	  delete m;
	  return;
	}
	
	assert(!in->is_auth());
  }

  dout(7) << "handle_lock_inode_file a=" << m->get_action() << " from " << from << " " << *in << " filelock=" << in->filelock << endl;  
  
  CLock *lock = &in->filelock;
  int issued = in->get_caps_issued();

  switch (m->get_action()) {
	// -- replica --
  case LOCK_AC_SYNC:
	assert(lock->get_state() == LOCK_LOCK ||
		   lock->get_state() == LOCK_MIXED);
	
	{ // assim data
	  int off = 0;
	  in->decode_file_state(m->get_data(), off);
	}
	
	// update lock
	lock->set_state(LOCK_SYNC);
	
	// no need to reply.
	
	// waiters
	in->filelock.get_read();
	in->finish_waiting(CINODE_WAIT_FILER|CINODE_WAIT_FILESTABLE);
	in->filelock.put_read();
	inode_file_eval(in);
	break;
	
  case LOCK_AC_LOCK:
	assert(lock->get_state() == LOCK_SYNC ||
		   lock->get_state() == LOCK_MIXED);
	
	// call back caps?
	if (issued & CAP_FILE_RD) {
	  dout(7) << "handle_lock_inode_file client readers, gathering caps on " << *in << endl;
	  issue_caps(in);
	}
	if (lock->get_nread() > 0) {
	  dout(7) << "handle_lock_inode_file readers, waiting before ack on " << *in << endl;
	  in->add_waiter(CINODE_WAIT_FILENORD,
					 new C_MDS_RetryMessage(mds,m));
	  lock->set_state(LOCK_GLOCKR);
	  assert(0);// i am broken.. why retry message when state captures all the info i need?
	  return;
	} 
	if (issued & CAP_FILE_RD) {
	  lock->set_state(LOCK_GLOCKR);
	  break;
	}

	// nothing to wait for, lock and ack.
	{
	  lock->set_state(LOCK_LOCK);

	  MLock *reply = new MLock(LOCK_AC_LOCKACK, mds->get_nodeid());
	  reply->set_ino(in->ino(), LOCK_OTYPE_IFILE);
	  mds->messenger->send_message(reply,
								   MSG_ADDR_MDS(from), MDS_PORT_CACHE,
								   MDS_PORT_CACHE);
	}
	break;
	
  case LOCK_AC_MIXED:
	assert(lock->get_state() == LOCK_SYNC ||
		   lock->get_state() == LOCK_LOCK);
	
	if (lock->get_state() == LOCK_SYNC) {
	  // MIXED
	  if (issued & CAP_FILE_RD) {
		// call back client caps
		lock->set_state(LOCK_GMIXEDR);
		issue_caps(in);
		break;
	  } else {
		// no clients, go straight to mixed
		lock->set_state(LOCK_MIXED);

		// ack
		MLock *reply = new MLock(LOCK_AC_MIXEDACK, mds->get_nodeid());
		reply->set_ino(in->ino(), LOCK_OTYPE_IFILE);
		mds->messenger->send_message(reply,
									 MSG_ADDR_MDS(from), MDS_PORT_CACHE,
									 MDS_PORT_CACHE);
	  }
	} else {
	  // LOCK
	  lock->set_state(LOCK_MIXED);
	  
	  // no ack needed.
	}

	issue_caps(in);
	
	// waiters
	in->filelock.get_write();
	in->finish_waiting(CINODE_WAIT_FILEW|CINODE_WAIT_FILESTABLE);
	in->filelock.put_write();
	inode_file_eval(in);
	break;

 
	

	// -- auth --
  case LOCK_AC_LOCKACK:
	assert(lock->state == LOCK_GLOCKR ||
		   lock->state == LOCK_GLOCKM ||
		   lock->state == LOCK_GWRONLYM ||
		   lock->state == LOCK_GWRONLYR);
	assert(lock->gather_set.count(from));
	lock->gather_set.erase(from);

	if (lock->gather_set.size()) {
	  dout(7) << "handle_lock_inode_file " << *in << " from " << from << ", still gathering " << lock->gather_set << endl;
	} else {
	  dout(7) << "handle_lock_inode_file " << *in << " from " << from << ", last one" << endl;
	  inode_file_eval(in);
	}
	break;
	
  case LOCK_AC_SYNCACK:
	assert(lock->state == LOCK_GSYNCM);
	assert(lock->gather_set.count(from));
	lock->gather_set.erase(from);
	
	/* not used currently
    {
	  // merge data  (keep largest size, mtime, etc.)
	  int off = 0;
	  in->decode_merge_file_state(m->get_data(), off);
	}
	*/

	if (lock->gather_set.size()) {
	  dout(7) << "handle_lock_inode_file " << *in << " from " << from << ", still gathering " << lock->gather_set << endl;
	} else {
	  dout(7) << "handle_lock_inode_file " << *in << " from " << from << ", last one" << endl;
	  inode_file_eval(in);
	}
	break;

  case LOCK_AC_MIXEDACK:
	assert(lock->state == LOCK_GMIXEDR);
	assert(lock->gather_set.count(from));
	lock->gather_set.erase(from);
	
	if (lock->gather_set.size()) {
	  dout(7) << "handle_lock_inode_file " << *in << " from " << from << ", still gathering " << lock->gather_set << endl;
	} else {
	  dout(7) << "handle_lock_inode_file " << *in << " from " << from << ", last one" << endl;
	  inode_file_eval(in);
	}
	break;


  default:
	assert(0);
  }  
  
  delete m;
}














void MDCache::handle_lock_dir(MLock *m) 
{

}



// DENTRY

bool MDCache::dentry_xlock_start(CDentry *dn, Message *m, CInode *ref)
{
  dout(7) << "dentry_xlock_start on " << *dn << endl;

  // locked?
  if (dn->lockstate == DN_LOCK_XLOCK) {
	if (dn->xlockedby == m) return true;  // locked by me!

	// not by me, wait
	dout(7) << "dentry " << *dn << " xlock by someone else" << endl;
	dn->dir->add_waiter(CDIR_WAIT_DNREAD, dn->name,
						new C_MDS_RetryRequest(mds,m,ref));
	return false;
  }

  // prelock?
  if (dn->lockstate == DN_LOCK_PREXLOCK) {
	if (dn->xlockedby == m) {
	  dout(7) << "dentry " << *dn << " prexlock by me" << endl;
	  dn->dir->add_waiter(CDIR_WAIT_DNLOCK, dn->name,
						  new C_MDS_RetryRequest(mds,m,ref));
	} else {
	  dout(7) << "dentry " << *dn << " prexlock by someone else" << endl;
	  dn->dir->add_waiter(CDIR_WAIT_DNREAD, dn->name,
						  new C_MDS_RetryRequest(mds,m,ref));
	}
	return false;
  }


  // lockable!
  assert(dn->lockstate == DN_LOCK_SYNC ||
		 dn->lockstate == DN_LOCK_UNPINNING);
  
  // dir auth pinnable?
  if (!dn->dir->can_auth_pin()) {
	dout(7) << "dentry " << *dn << " dir not pinnable, waiting" << endl;
	dn->dir->add_waiter(CDIR_WAIT_AUTHPINNABLE,
						new C_MDS_RetryRequest(mds,m,ref));
	return false;
  }

  // is dentry path pinned?
  if (dn->is_pinned()) {
	dout(7) << "dentry " << *dn << " pinned, waiting" << endl;
	dn->lockstate = DN_LOCK_UNPINNING;
	dn->dir->add_waiter(CDIR_WAIT_DNUNPINNED,
						dn->name,
						new C_MDS_RetryRequest(mds,m,ref));
	return false;
  }

  // pin path up to dentry!            (if success, point of no return)
  CDentry *pdn = dn->dir->inode->get_parent_dn();
  if (pdn) {
	if (active_requests[m].traces.count(pdn)) {
	  dout(7) << "already path pinned parent dentry " << *pdn << endl;
	} else {
	  dout(7) << "pinning parent dentry " << *pdn << endl;
	  vector<CDentry*> trace;
	  make_trace(trace, pdn->inode);
	  assert(trace.size());

	  if (!path_pin(trace, m, new C_MDS_RetryRequest(mds, m, ref))) return false;
	  
	  active_requests[m].traces[trace[trace.size()-1]] = trace;
	}
  }

  // pin dir!
  dn->dir->auth_pin();
  
  // mine!
  dn->xlockedby = m;

  if (dn->dir->is_open_by_anyone()) {
	dn->lockstate = DN_LOCK_PREXLOCK;
	
	// xlock with whom?
	set<int> who = dn->dir->get_open_by();
	dn->gather_set = who;

	// make path
	string path;
	dn->make_path(path);
	dout(10) << "path is " << path << " for " << *dn << endl;

	for (set<int>::iterator it = who.begin();
		 it != who.end();
		 it++) {
	  MLock *m = new MLock(LOCK_AC_LOCK, mds->get_nodeid());
	  m->set_dn(dn->dir->ino(), dn->name);
	  m->set_path(path);
	  mds->messenger->send_message(m,
								   MSG_ADDR_MDS(*it), MDS_PORT_CACHE,
								   MDS_PORT_CACHE);
	}

	// wait
	dout(7) << "dentry_xlock_start locking, waiting for replicas " << endl;
	dn->dir->add_waiter(CDIR_WAIT_DNLOCK, dn->name,
						new C_MDS_RetryRequest(mds, m, ref));
	return false;
  } else {
	dn->lockstate = DN_LOCK_XLOCK;
	active_requests[dn->xlockedby].xlocks.insert(dn);
	return true;
  }
}

void MDCache::dentry_xlock_finish(CDentry *dn, bool quiet)
{
  dout(7) << "dentry_xlock_finish on " << *dn << endl;
  
  assert(dn->xlockedby);
  if (dn->xlockedby == DN_XLOCK_FOREIGN) {
	dout(7) << "this was a foreign xlock" << endl;
  } else {
	// remove from request record
	assert(active_requests[dn->xlockedby].xlocks.count(dn) == 1);
	active_requests[dn->xlockedby].xlocks.erase(dn);
  }

  dn->xlockedby = 0;
  dn->lockstate = DN_LOCK_SYNC;

  // unpin parent dir?
  // -> no?  because we might have xlocked 2 things in this dir.
  //         instead, we let request_finish clean up the mess.
    
  // tell replicas?
  if (!quiet) {
	// tell even if dn is null.
	if (dn->dir->is_open_by_anyone()) {
	  for (set<int>::iterator it = dn->dir->open_by_begin();
		   it != dn->dir->open_by_end();
		   it++) {
		MLock *m = new MLock(LOCK_AC_SYNC, mds->get_nodeid());
		m->set_dn(dn->dir->ino(), dn->name);
		mds->messenger->send_message(m,
									 MSG_ADDR_MDS(*it), MDS_PORT_CACHE,
									 MDS_PORT_CACHE);
	  }
	}
  }
  
  // unpin dir
  dn->dir->auth_unpin();
}

/*
 * onfinish->finish() will be called with 
 * 0 on successful xlock,
 * -1 on failure
 */

class C_MDC_XlockRequest : public Context {
  MDCache *mdc;
  CDir *dir;
  string dname;
  Message *req;
  Context *finisher;
public:
  C_MDC_XlockRequest(MDCache *mdc, 
					 CDir *dir, string& dname, 
					 Message *req,
					 Context *finisher) {
	this->mdc = mdc;
	this->dir = dir;
	this->dname = dname;
	this->req = req;
	this->finisher = finisher;
  }

  void finish(int r) {
	cout << "xlockrequest->finish r = " << r << endl;
	if (r == 1) {  // 1 for xlock request success
	  CDentry *dn = dir->lookup(dname);
	  if (dn && dn->xlockedby == 0) {
		// success
		dn->xlockedby = req;   // our request was the winner
		cout << "xlock request success, now xlocked by req " << req << " dn " << *dn << endl;

		// remember!
		mdc->active_requests[req].foreign_xlocks.insert(dn);
	  }		
	}

	// retry request (or whatever)
	finisher->finish(0);
	delete finisher;
  }
};

void MDCache::dentry_xlock_request(CDir *dir, string& dname, bool create,
								   Message *req, Context *onfinish)
{
  dout(10) << "dentry_xlock_request on dn " << dname << " create=" << create << " in " << *dir << endl; 
  // send request
  int dauth = dir->dentry_authority(dname);
  MLock *m = new MLock(create ? LOCK_AC_REQXLOCKC:LOCK_AC_REQXLOCK, mds->get_nodeid());
  m->set_dn(dir->ino(), dname);
  mds->messenger->send_message(m,
							   MSG_ADDR_MDS(dauth), MDS_PORT_CACHE,
							   MDS_PORT_CACHE);
  
  // add waiter
  dir->add_waiter(CDIR_WAIT_DNREQXLOCK, dname, 
				  new C_MDC_XlockRequest(this, 
										 dir, dname, req,
										 onfinish));
}




void MDCache::handle_lock_dn(MLock *m)
{
  assert(m->get_otype() == LOCK_OTYPE_DN);
  
  CInode *diri = get_inode(m->get_ino());  // may be null 
  CDir *dir = 0;
  if (diri) dir = diri->dir;           // may be null
  string dname = m->get_dn();
  int from = m->get_asker();
  CDentry *dn = 0;

  if (LOCK_AC_FOR_AUTH(m->get_action())) {
	// auth

	// normally we have it always
	if (diri && dir) {
	  int dauth = dir->dentry_authority(dname);
	  assert(dauth == mds->get_nodeid() || dir->is_proxy() ||  // mine or proxy,
			 m->get_action() == LOCK_AC_REQXLOCKACK ||         // or we did a REQXLOCK and this is our ack/nak
			 m->get_action() == LOCK_AC_REQXLOCKNAK);
	  
	  if (dir->is_proxy()) {

		assert(dauth >= 0);

		if (dauth == m->get_asker() && 
			(m->get_action() == LOCK_AC_REQXLOCK ||
			 m->get_action() == LOCK_AC_REQXLOCKC)) {
		  dout(7) << "handle_lock_dn got reqxlock from " << dauth << " and they are auth.. dropping on floor (their import will have woken them up)" << endl;
		  if (active_requests.count(m)) 
			request_finish(m);
		  else
			delete m;
		  return;
		}

		dout(7) << "handle_lock_dn " << m << " " << m->get_ino() << " dname " << dname << " from " << from << ": proxy, fw to " << dauth << endl;

		// forward
		if (active_requests.count(m)) {
		  // xlock requests are requests, use request_* functions!
		  assert(m->get_action() == LOCK_AC_REQXLOCK ||
				 m->get_action() == LOCK_AC_REQXLOCKC);
		  // forward as a request
		  request_forward(m, dauth, MDS_PORT_CACHE);
		} else {
		  // not an xlock req, or it is and we just didn't register the request yet
		  // forward normally
		  mds->messenger->send_message(m,
									   MSG_ADDR_MDS(dauth), MDS_PORT_CACHE,
									   MDS_PORT_CACHE);
		}
		return;
	  }
	  
	  dn = dir->lookup(dname);
	}

	// except with.. an xlock request?
	if (!dn) {
	  assert(dir);  // we should still have the dir, though!  the requester has the dir open.
	  switch (m->get_action()) {

	  case LOCK_AC_LOCK:
		dout(7) << "handle_lock_dn xlock on " << dname << ", adding (null)" << endl;
		dn = dir->add_dentry(dname);
		break;

	  case LOCK_AC_REQXLOCK:
		// send nak
		if (dir->state_test(CDIR_STATE_DELETED)) {
		  dout(7) << "handle_lock_dn reqxlock on deleted dir " << *dir << ", nak" << endl;
		} else {
		  dout(7) << "handle_lock_dn reqxlock on " << dname << " in " << *dir << " dne, nak" << endl;
		}
		{
		  MLock *reply = new MLock(LOCK_AC_REQXLOCKNAK, mds->get_nodeid());
		  reply->set_dn(dir->ino(), dname);
		  reply->set_path(m->get_path());
		  mds->messenger->send_message(reply,
									   MSG_ADDR_MDS(m->get_asker()), MDS_PORT_CACHE,
									   MDS_PORT_CACHE);
		}
		 
		// finish request (if we got that far)
		if (active_requests.count(m)) request_finish(m);

		delete m;
		return;

	  case LOCK_AC_REQXLOCKC:
		dout(7) << "handle_lock_dn reqxlockc on " << dname << " in " << *dir << " dne (yet!)" << endl;
		break;

	  default:
		assert(0);
	  }
	}
  } else {
	// replica
	if (dir) dn = dir->lookup(dname);
	if (!dn) {
	  dout(7) << "handle_lock_dn " << m << " don't have " << m->get_ino() << " dname " << dname << endl;
	  
	  if (m->get_action() == LOCK_AC_REQXLOCKACK ||
		  m->get_action() == LOCK_AC_REQXLOCKNAK) {
		dout(7) << "handle_lock_dn got reqxlockack/nak, but don't have dn " << m->get_path() << ", discovering" << endl;
		//assert(0);  // how can this happen?  tell me now!
		
		vector<CDentry*> trace;
		filepath path = m->get_path();
		int r = path_traverse(path, trace, true,
							  m, new C_MDS_RetryMessage(mds,m), 
							  MDS_TRAVERSE_DISCOVER);
		assert(r>0);
		return;
	  } 

	  if (m->get_action() == LOCK_AC_LOCK) {
		if (0) { // not anymore
		  dout(7) << "handle_lock_dn don't have " << m->get_path() << ", discovering" << endl;
		  
		  vector<CDentry*> trace;
		  filepath path = m->get_path();
		  int r = path_traverse(path, trace, true,
								m, new C_MDS_RetryMessage(mds,m), 
								MDS_TRAVERSE_DISCOVER);
		  assert(r>0);
		}
		if (1) {
		  // NAK
		  MLock *reply = new MLock(LOCK_AC_LOCKNAK, mds->get_nodeid());
		  reply->set_dn(m->get_ino(), dname);
		  mds->messenger->send_message(reply,
									   MSG_ADDR_MDS(m->get_asker()), MDS_PORT_CACHE,
									   MDS_PORT_CACHE);
		}
	  } else {
		dout(7) << "safely ignoring." << endl;
		delete m;
	  }
	  return;
	}

	assert(dn);
  }

  if (dn) {
	dout(7) << "handle_lock_dn a=" << m->get_action() << " from " << from << " " << *dn << endl;
  } else {
	dout(7) << "handle_lock_dn a=" << m->get_action() << " from " << from << " " << dname << " in " << *dir << endl;
  }
  
  switch (m->get_action()) {
	// -- replica --
  case LOCK_AC_LOCK:
	assert(dn->lockstate == DN_LOCK_SYNC ||
		   dn->lockstate == DN_LOCK_UNPINNING ||
		   dn->lockstate == DN_LOCK_XLOCK);   // <-- bc the handle_lock_dn did the discover!

	if (dn->is_pinned()) {
	  dn->lockstate = DN_LOCK_UNPINNING;

	  // wait
	  dout(7) << "dn pinned, waiting " << *dn << endl;
	  dn->dir->add_waiter(CDIR_WAIT_DNUNPINNED,
						  dn->name,
						  new C_MDS_RetryMessage(mds, m));
	  return;
	} else {
	  dn->lockstate = DN_LOCK_XLOCK;
	  dn->xlockedby = 0;

	  // ack now
	  MLock *reply = new MLock(LOCK_AC_LOCKACK, mds->get_nodeid());
	  reply->set_dn(diri->ino(), dname);
	  mds->messenger->send_message(reply,
								   MSG_ADDR_MDS(from), MDS_PORT_CACHE,
								   MDS_PORT_CACHE);
	}

	// wake up waiters
	dir->finish_waiting(CDIR_WAIT_DNLOCK, dname);   // ? will this happen on replica ? 
	break;

  case LOCK_AC_SYNC:
	assert(dn->lockstate == DN_LOCK_XLOCK);
	dn->lockstate = DN_LOCK_SYNC;
	dn->xlockedby = 0;

	// null?  hose it.
	if (dn->is_null()) {
	  dout(7) << "hosing null (and now sync) dentry " << *dn << endl;
	  dir->remove_dentry(dn);
	}

	// wake up waiters
	dir->finish_waiting(CDIR_WAIT_DNREAD, dname);   // will this happen either?  YES: if a rename lock backs out
	break;

  case LOCK_AC_REQXLOCKACK:
  case LOCK_AC_REQXLOCKNAK:
	{
	  dout(10) << "handle_lock_dn got ack/nak on a reqxlock for " << *dn << endl;
	  list<Context*> finished;
	  dir->take_waiting(CDIR_WAIT_DNREQXLOCK, m->get_dn(), finished, 1);  // TAKE ONE ONLY!
	  finish_contexts(finished, 
					  (m->get_action() == LOCK_AC_REQXLOCKACK) ? 1:-1);
	}
	break;


	// -- auth --
  case LOCK_AC_LOCKACK:
  case LOCK_AC_LOCKNAK:
	assert(dn->gather_set.count(from) == 1);
	dn->gather_set.erase(from);
	if (dn->gather_set.size() == 0) {
	  dout(7) << "handle_lock_dn finish gather, now xlock on " << *dn << endl;
	  dn->lockstate = DN_LOCK_XLOCK;
	  active_requests[dn->xlockedby].xlocks.insert(dn);
	  dir->finish_waiting(CDIR_WAIT_DNLOCK, dname);
	}
	break;


  case LOCK_AC_REQXLOCKC:
	// make sure it's a _file_, if it exists.
	if (dn && dn->inode && dn->inode->is_dir()) {
	  dout(7) << "handle_lock_dn failing, reqxlockc on dir " << *dn->inode << endl;
	  
	  // nak
	  string path;
	  dn->make_path(path);

	  MLock *reply = new MLock(LOCK_AC_REQXLOCKNAK, mds->get_nodeid());
	  reply->set_dn(dir->ino(), dname);
	  reply->set_path(path);
	  mds->messenger->send_message(reply,
								   MSG_ADDR_MDS(m->get_asker()), MDS_PORT_CACHE,
								   MDS_PORT_CACHE);
	  
	  // done
	  if (active_requests.count(m)) 
		request_finish(m);
	  else
		delete m;
	  return;
	}

  case LOCK_AC_REQXLOCK:
	if (dn) {
	  dout(7) << "handle_lock_dn reqxlock on " << *dn << endl;
	} else {
	  dout(7) << "handle_lock_dn reqxlock on " << dname << " in " << *dir << endl;	  
	}
	

	// start request?
	if (!active_requests.count(m)) {
	  vector<CDentry*> trace;
	  if (!request_start(m, dir->inode, trace))
		return;  // waiting for pin
	}
	
	// try to xlock!
	if (!dn) {
	  assert(m->get_action() == LOCK_AC_REQXLOCKC);
	  dn = dir->add_dentry(dname);
	}

	if (dn->xlockedby != m) {
	  if (!dentry_xlock_start(dn, m, dir->inode)) {
		// hose null dn if we're waiting on something
		if (dn->is_clean() && dn->is_null() && dn->is_sync()) dir->remove_dentry(dn);
		return;    // waiting for xlock
	  }
	} else {
	  // successfully xlocked!  on behalf of requestor.
	  string path;
	  dn->make_path(path);

	  dout(7) << "handle_lock_dn reqxlock success for " << m->get_asker() << " on " << *dn << ", acking" << endl;
	  
	  // ACK xlock request
	  MLock *reply = new MLock(LOCK_AC_REQXLOCKACK, mds->get_nodeid());
	  reply->set_dn(dir->ino(), dname);
	  reply->set_path(path);
	  mds->messenger->send_message(reply,
								   MSG_ADDR_MDS(m->get_asker()), MDS_PORT_CACHE,
								   MDS_PORT_CACHE);

	  // note: keep request around in memory (to hold the xlock/pins on behalf of requester)
	  return;
	}
	break;

  case LOCK_AC_UNXLOCK:
	dout(7) << "handle_lock_dn unxlock on " << *dn << endl;
	{
	  string dname = dn->name;
	  Message *m = dn->xlockedby;

	  // finish request
	  request_finish(m);  // this will drop the locks (and unpin paths!)
	  return;
	}
	break;

  default:
	assert(0);
  }

  delete m;
}






















// ==========================================================
// IMPORT/EXPORT


class C_MDC_ExportFreeze : public Context {
  MDS *mds;
  CDir *ex;   // dir i'm exporting
  int dest;

public:
  C_MDC_ExportFreeze(MDS *mds, CDir *ex, int dest) {
	this->mds = mds;
	this->ex = ex;
	this->dest = dest;
  }
  virtual void finish(int r) {
	mds->mdcache->export_dir_frozen(ex, dest);
  }
};



/** export_dir(dir, dest)
 * public method to initiate an export.
 * will fail if the directory is freezing, frozen, unpinnable, or root. 
 */
void MDCache::export_dir(CDir *dir,
						 int dest)
{
  dout(7) << "export_dir " << *dir << " to " << dest << endl;
  assert(dest != mds->get_nodeid());
  assert(!dir->is_hashed());
   
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
  make_trace(trace, dir->inode);
  if (!path_pin(trace, 0, 0)) {
	dout(7) << "export_dir couldn't pin path, failing." << endl;
	return;
  }

  // ok, let's go.

  // send ExportDirDiscover (ask target)
  export_gather[dir].insert(dest);
  mds->messenger->send_message(new MExportDirDiscover(dir->inode),
							   MSG_ADDR_MDS(dest), MDS_PORT_CACHE, MDS_PORT_CACHE);
  dir->auth_pin();   // pin dir, to hang up our freeze  (unpin on prep ack)

  // take away the popularity we're sending.   FIXME: do this later?
  mds->balancer->subtract_export(dir);
  
  
  // freeze the subtree
  dir->freeze_tree(new C_MDC_ExportFreeze(mds, dir, dest));
}


/*
 * called on receipt of MExportDirDiscoverAck
 * the importer now has the directory's _inode_ in memory, and pinned.
 */
void MDCache::handle_export_dir_discover_ack(MExportDirDiscoverAck *m)
{
  CInode *in = get_inode(m->get_ino());
  assert(in);
  CDir *dir = in->dir;
  assert(dir);
  
  int from = MSG_ADDR_NUM(m->get_source());
  assert(export_gather[dir].count(from));
  export_gather[dir].erase(from);

  if (export_gather[dir].empty()) {
	dout(7) << "export_dir_discover_ack " << *dir << ", releasing auth_pin" << endl;
	dir->auth_unpin();   // unpin to allow freeze to complete
  } else {
	dout(7) << "export_dir_discover_ack " << *dir << ", still waiting for " << export_gather[dir] << endl;
  }
  
  delete m;  // done
}


void MDCache::export_dir_frozen(CDir *dir,
								int dest)
{
  // subtree is now frozen!
  dout(7) << "export_dir_frozen on " << *dir << " to " << dest << endl;

  show_imports();

  MExportDirPrep *prep = new MExportDirPrep(dir->inode);

  // include spanning tree for all nested exports.
  // these need to be on the destination _before_ the final export so that
  // dir_auth updates on any nested exports are properly absorbed.
  
  set<inodeno_t> inodes_added;
  
  // include base dir
  prep->add_dir( new CDirDiscover(dir, dir->open_by_add(dest)) );
  
  // also include traces to all nested exports.
  set<CDir*> my_nested;
  find_nested_exports(dir, my_nested);
  for (set<CDir*>::iterator it = my_nested.begin();
	   it != my_nested.end();
	   it++) {
	CDir *exp = *it;
    
	dout(7) << " including nested export " << *exp << " in prep" << endl;

	prep->add_export( exp->ino() );

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
        prep->add_dir( new CDirDiscover(cur, cur->open_by_add(dest)) );  // yay!
        dout(7) << "  added " << *cur << endl;
      }
      
      cur = parent_dir;      
    }

	for (list<CInode*>::iterator it = inode_trace.begin();
		 it != inode_trace.end();
		 it++) {
	  CInode *in = *it;
      dout(7) << "  added " << *in << endl;
      prep->add_inode( in->parent->dir->ino(),
					   in->parent->name,
					   in->replicate_to(dest) );
	}

  }
  
  // send it!
  mds->messenger->send_message(prep,
							   MSG_ADDR_MDS(dest), MDS_PORT_CACHE, MDS_PORT_CACHE);
}

void MDCache::handle_export_dir_prep_ack(MExportDirPrepAck *m)
{
  CInode *in = get_inode(m->get_ino());
  assert(in);
  CDir *dir = in->dir;
  assert(dir);

  dout(7) << "export_dir_prep_ack " << *dir << ", starting export" << endl;
  
  // start export.
  export_dir_go(dir, MSG_ADDR_NUM(m->get_source()));

  // done
  delete m;
}


void MDCache::export_dir_go(CDir *dir,
							int dest)
{  
  dout(7) << "export_dir_go " << *dir << " to " << dest << endl;

  show_imports();


  // build export message
  MExportDir *req = new MExportDir(dir->inode);  // include pop


  // update imports/exports
  CDir *containing_import = get_auth_container(dir);

  if (containing_import == dir) {
	dout(7) << " i'm rexporting a previous import" << endl;
	assert(dir->is_import());
	imports.erase(dir);
	dir->state_clear(CDIR_STATE_IMPORT);
	dir->put(CDIR_PIN_IMPORT);                  // unpin, no longer an import
	
	// discard nested exports (that we're handing off
	for (set<CDir*>::iterator p = nested_exports[dir].begin();
		 p != nested_exports[dir].end(); ) {
	  CDir *nested = *p;
	  p++;

	  // add to export message
	  req->add_export(nested);
	  
	  // nested beneath our new export *in; remove!
	  dout(7) << " export " << *nested << " was nested beneath us; removing from export list(s)" << endl;
	  assert(exports.count(nested) == 1);
	  nested_exports[dir].erase(nested);
	}
	
  } else {
	dout(7) << " i'm a subdir nested under import " << *containing_import << endl;
	exports.insert(dir);
	nested_exports[containing_import].insert(dir);
	
	dir->state_set(CDIR_STATE_EXPORT);
	dir->get(CDIR_PIN_EXPORT);                  // i must keep it pinned
	
	// discard nested exports (that we're handing off)
	for (set<CDir*>::iterator p = nested_exports[containing_import].begin();
		 p != nested_exports[containing_import].end(); ) {
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
		nested_exports[containing_import].erase(nested);
		// exports.erase(nested); _walk does this

		// add to msg
		req->add_export(nested);
	  } else {
		dout(12) << " export " << *nested << " is under other export " << *containing_export << ", which is unrelated" << endl;
		assert(get_auth_container(containing_export) != containing_import);
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
  for (set<int>::iterator it = dir->open_by.begin();
	   it != dir->open_by.end();
	   it++) {
	if (*it == mds->get_nodeid()) continue;
	export_notify_ack_waiting[dir].insert( *it );

	// send warning to all but dest
	if (*it != dest) {
	  dout(10) << " sending export_dir_warning to mds" << *it << endl;
	  mds->messenger->send_message(new MExportDirWarning( dir->ino() ),
								   MSG_ADDR_MDS( *it ), MDS_PORT_CACHE, MDS_PORT_CACHE);
	}
  }
  assert(export_notify_ack_waiting[dir].count( dest ));

  // fill export message with cache data
  C_Contexts *fin = new C_Contexts;
  int num_exported_inodes = export_dir_walk( req, 
											 fin, 
											 dir,   // base
											 dir,   // recur start point
											 dest );
  
  // send the export data!
  mds->messenger->send_message(req,
							   MSG_ADDR_MDS(dest), MDS_PORT_CACHE,
							   MDS_PORT_CACHE);

  // queue up the finisher
  dir->add_waiter( CDIR_WAIT_UNFREEZE, fin );


  // stats
  mds->logger->inc("ex");
  mds->logger->inc("iex", num_exported_inodes);

  show_imports();
}


/** encode_export_inode
 * update our local state for this inode to export.
 * encode relevant state to be sent over the wire.
 * used by: export_dir_walk, file_rename (if foreign)
 */
void MDCache::encode_export_inode(CInode *in, bufferlist& enc_state, int new_auth)
{
  in->version++;  // so local log entries are ignored, etc.  (FIXME ??)
  
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
	mds->messenger->send_message(m, MSG_ADDR_CLIENT(it->first), 0, MDS_PORT_CACHE);
  }

  // relax locks?
  if (!in->is_cached_by_anyone())
	in->replicate_relax_locks();

  // add inode
  assert(in->cached_by.count(mds->get_nodeid()) == 0);
  CInodeExport istate( in );
  istate._encode( enc_state );

  // we're export this inode; fix inode state
  dout(7) << "encode_export_inode " << *in << endl;
  
  if (in->is_dirty()) in->mark_clean();
  
  // clear/unpin cached_by (we're no longer the authority)
  in->cached_by_clear();
  
  // twiddle lock states for auth -> replica transition
  // hard
  in->hardlock.clear_gather();
  if (in->hardlock.get_state() == LOCK_GLOCKR)
	in->hardlock.set_state(LOCK_LOCK);

  // file : we lost all our caps, so move to stable state!
  in->filelock.clear_gather();
  if (in->filelock.get_state() == LOCK_GLOCKR ||
	  in->filelock.get_state() == LOCK_GLOCKM ||
	  in->filelock.get_state() == LOCK_GLOCKW ||
	  in->filelock.get_state() == LOCK_GWRONLYR ||
	  in->filelock.get_state() == LOCK_GWRONLYM ||
	  in->filelock.get_state() == LOCK_WRONLY)
	in->filelock.set_state(LOCK_LOCK);
  if (in->filelock.get_state() == LOCK_GMIXEDR)
	in->filelock.set_state(LOCK_MIXED);
  // this looks like a step backwards, but it's what we want!
  if (in->filelock.get_state() == LOCK_GSYNCM)
	in->filelock.set_state(LOCK_MIXED);
  if (in->filelock.get_state() == LOCK_GSYNCW)
	in->filelock.set_state(LOCK_LOCK);
  if (in->filelock.get_state() == LOCK_GMIXEDW)
	in->filelock.set_state(LOCK_LOCK);
	//in->filelock.set_state(LOCK_MIXED);
  
  // mark auth
  assert(in->is_auth());
  in->set_auth(false);
  in->replica_nonce = CINODE_EXPORT_NONCE;
  
  // *** other state too?

  // move to end of LRU so we drop it out of cache quickly!
  lru.lru_bottouch(in);
}


int MDCache::export_dir_walk(MExportDir *req,
							 C_Contexts *fin,
							 CDir *basedir,
							 CDir *dir,
							 int newauth)
{
  int num_exported = 0;

  dout(7) << "export_dir_walk " << *dir << " " << dir->nitems << " items" << endl;
  
  // dir 
  bufferlist enc_dir;
  
  CDirExport dstate(dir);
  dstate._encode( enc_dir );
  
  // release open_by 
  dir->open_by_clear();

  // mark
  assert(dir->is_auth());
  dir->state_clear(CDIR_STATE_AUTH);
  dir->replica_nonce = CDIR_NONCE_EXPORT;

  // proxy
  dir->state_set(CDIR_STATE_PROXY);
  dir->get(CDIR_PIN_PROXY);
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
	  CInode *in = dn->inode;
	  
	  num_exported++;
	  
	  // -- dentry
	  dout(7) << "export_dir_walk exporting " << *dn << endl;
	  _encode(it->first, enc_dir);
	  
	  if (dn->is_dirty()) 
		enc_dir.append("D", 1);  // dirty
	  else 
		enc_dir.append("C", 1);  // clean
	  
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
		  assert(exports.count(in->dir) == 1); 
		  exports.erase(in->dir);                    // discard nested export   (nested_exports updated above)
		  
		  in->dir->state_clear(CDIR_STATE_EXPORT);
		  in->dir->put(CDIR_PIN_EXPORT);
		  
		  // simplify dir_auth?
		  if (in->dir->get_dir_auth() == newauth)
			in->dir->set_dir_auth( CDIR_AUTH_PARENT );
		} 
	  }
	  
	  // add to proxy
	  export_proxy_inos[basedir].push_back(in->ino());
	  in->state_set(CINODE_STATE_PROXY);
	  in->get(CINODE_PIN_PROXY);
	  
	  // waiters
	  list<Context*> waiters;
	  in->take_waiting(CINODE_WAIT_ANY, waiters);
	  fin->take(waiters);
	}
  }

  req->add_dir( enc_dir );

  // subdirs
  for (list<CDir*>::iterator it = subdirs.begin(); it != subdirs.end(); it++)
	num_exported += export_dir_walk(req, fin, basedir, *it, newauth);

  return num_exported;
}


/*
 * i should get an export_dir_notify_ack from every mds that had me open, including the new auth (an ack)
 */
void MDCache::handle_export_dir_notify_ack(MExportDirNotifyAck *m)
{
  CInode *diri = get_inode(m->get_ino());
  CDir *dir = diri->dir;
  assert(dir);
  assert(dir->is_frozen_tree_root());  // i'm exporting!

  // remove from waiting list
  int from = MSG_ADDR_NUM(m->get_source());
  assert(export_notify_ack_waiting[dir].count(from));
  export_notify_ack_waiting[dir].erase(from);

  // done?
  if (!export_notify_ack_waiting[dir].empty()) {
	dout(7) << "handle_export_dir_notify_ack on " << *dir << " from " << from 
			<< ", still waiting for " << export_notify_ack_waiting[dir] << endl;
	
  } else {
	dout(7) << "handle_export_dir_notify_ack on " << *dir << " from " << from 
			<< ", last one!" << endl;

	// ok, we're finished!
	export_notify_ack_waiting.erase(dir);

	// finish export  (unfreeze, trigger finish context, etc.)
	export_dir_finish(dir);

	// unpin proxies
	// inodes
	for (list<inodeno_t>::iterator it = export_proxy_inos[dir].begin();
		 it != export_proxy_inos[dir].end();
		 it++) {
	  CInode *in = get_inode(*it);
	  in->put(CINODE_PIN_PROXY);
	  assert(in->state_test(CINODE_STATE_PROXY));
	  in->state_clear(CINODE_STATE_PROXY);
	}
	export_proxy_inos.erase(dir);

	// dirs
	for (list<inodeno_t>::iterator it = export_proxy_dirinos[dir].begin();
		 it != export_proxy_dirinos[dir].end();
		 it++) {
	  CDir *dir = get_inode(*it)->dir;
	  dir->put(CDIR_PIN_PROXY);
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

  delete m;
}


/*
 * once i get all teh notify_acks i can finish
 */
void MDCache::export_dir_finish(CDir *dir)
{
  // exported!

  
  // FIXME log it
  
  // send finish to new auth
  mds->messenger->send_message(new MExportDirFinish(dir->ino()),
							   MSG_ADDR_MDS(dir->authority()),
							   MDS_PORT_CACHE, MDS_PORT_CACHE);
  
  // unfreeze
  dout(7) << "export_dir_finish " << *dir << ", unfreezing" << endl;
  dir->unfreeze_tree();

  // unpin path
  dout(7) << "export_dir_finish unpinning path" << endl;
  vector<CDentry*> trace;
  make_trace(trace, dir->inode);
  path_unpin(trace, 0);


  // stats
  mds->logger->set("nex", exports.size());

  show_imports();
}












//  IMPORTS

class C_MDC_ExportDirDiscover : public Context {
  MDCache *mdc;
  MExportDirDiscover *m;
public:
  vector<CDentry*> trace;
  C_MDC_ExportDirDiscover(MDCache *mdc, MExportDirDiscover *m) {
	this->mdc = mdc;
	this->m = m;
  }
  void finish(int r) {
	CInode *in = 0;
	if (r >= 0) in = trace[trace.size()-1]->get_inode();
	mdc->handle_export_dir_discover_2(m, in, r);
  }
};  

void MDCache::handle_export_dir_discover(MExportDirDiscover *m)
{
  assert(MSG_ADDR_NUM(m->get_source()) != mds->get_nodeid());

  dout(7) << "handle_export_dir_discover on " << m->get_path() << endl;

  // must discover it!
  C_MDC_ExportDirDiscover *onfinish = new C_MDC_ExportDirDiscover(this, m);
  filepath fpath(m->get_path());
  path_traverse(fpath, onfinish->trace, true,
				m, new C_MDS_RetryMessage(mds,m),       // on delay/retry
				MDS_TRAVERSE_DISCOVER,
				onfinish);  // on completion|error
}

void MDCache::handle_export_dir_discover_2(MExportDirDiscover *m, CInode *in, int r)
{
  // yay!
  if (in) {
	dout(7) << "handle_export_dir_discover_2 has " << *in << endl;
  }

  if (r < 0 || !in->is_dir()) {
	dout(7) << "handle_export_dir_discover_2 failed to discover or not dir " << m->get_path() << ", NAK" << endl;

	assert(0);	// this shouldn't happen if the auth pins his path properly!!!! 

	mds->messenger->send_message(new MExportDirDiscoverAck(m->get_ino(), false),
								 m->get_source(), MDS_PORT_CACHE, MDS_PORT_CACHE);
	
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
  in->get(CINODE_PIN_IMPORTING);
  
  // pin auth too, until the import completes.
  in->auth_pin();
  
  // reply
  dout(7) << " sending export_dir_discover_ack on " << *in << endl;
  mds->messenger->send_message(new MExportDirDiscoverAck(in->ino()),
							   m->get_source(), MDS_PORT_CACHE, MDS_PORT_CACHE);
  delete m;
}



void MDCache::handle_export_dir_prep(MExportDirPrep *m)
{
  assert(MSG_ADDR_NUM(m->get_source()) != mds->get_nodeid());

  CInode *diri = get_inode(m->get_ino());
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
    diri->set_dir( new CDir(diri, mds, false) );
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
	diri->put(CINODE_PIN_IMPORTING);
	dir->get(CDIR_PIN_IMPORTING);  

	// auth pin too
	dir->auth_pin();
	diri->auth_unpin();
	
	// assimilate traces to exports
    for (list<CInodeDiscover*>::iterator it = m->get_inodes().begin();
         it != m->get_inodes().end();
         it++) {
      // inode
      CInode *in = get_inode( (*it)->get_ino() );
      if (in) {
        (*it)->update_inode(in);
        dout(7) << " updated " << *in << endl;
      } else {
        in = new CInode(false);
        (*it)->update_inode(in);
        
        // link to the containing dir
        CInode *condiri = get_inode( m->get_containing_dirino(in->ino()) );
        assert(condiri && condiri->dir);
        add_inode( in );
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
          in->set_dir( new CDir(in, mds, false) );
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
      CInode *in = get_inode(*it);
      assert(in);
      
      if (!in->dir) {
        dout(7) << "  opening nested export on " << *in << endl;
		open_remote_dir(in,
						new C_MDS_RetryMessage(mds, m));

		// pin it!
		in->get(CINODE_PIN_OPENINGDIR);
		in->state_set(CINODE_STATE_OPENINGDIR);
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
    CInode *in = get_inode(ino);
	if (!in) dout(0) << "** missing ino " << hex << ino << dec << endl;
	assert(in);
    if (in->dir) {
	  if (!in->dir->state_test(CDIR_STATE_IMPORTINGEXPORT)) {
		dout(7) << "  pinning nested export " << *in->dir << endl;
		in->dir->get(CDIR_PIN_IMPORTINGEXPORT);
		in->dir->state_set(CDIR_STATE_IMPORTINGEXPORT);

		if (in->state_test(CINODE_STATE_OPENINGDIR)) {
		  in->put(CINODE_PIN_OPENINGDIR);
		  in->state_clear(CINODE_STATE_OPENINGDIR);
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
	mds->messenger->send_message(new MExportDirPrepAck(dir->ino()),
								 m->get_source(), MDS_PORT_CACHE, MDS_PORT_CACHE);
	
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



void MDCache::handle_export_dir(MExportDir *m)
{
  CInode *diri = get_inode(m->get_ino());
  assert(diri);
  CDir *dir = diri->dir;
  assert(dir);

  int oldauth = MSG_ADDR_NUM(m->get_source());
  dout(7) << "handle_export_dir, import " << *dir << " from " << oldauth << endl;
  assert(dir->is_auth() == false);



  show_imports();
  
  // note new authority (locally)
  if (dir->inode->is_auth())
	dir->set_dir_auth( CDIR_AUTH_PARENT );
  else
	dir->set_dir_auth( mds->get_nodeid() );
  dout(10) << " set dir_auth to " << dir->get_dir_auth() << endl;

  // update imports/exports
  CDir *containing_import;
  if (exports.count(dir)) {
	// reimporting
	dout(7) << " i'm reimporting " << *dir << endl;
	exports.erase(dir);

	dir->state_clear(CDIR_STATE_EXPORT);
	dir->put(CDIR_PIN_EXPORT);                // unpin, no longer an export
	
	containing_import = get_auth_container(dir);  
	dout(7) << "  it is nested under import " << *containing_import << endl;
	nested_exports[containing_import].erase(dir);
  } else {
	// new import
	imports.insert(dir);
	dir->state_set(CDIR_STATE_IMPORT);
	dir->get(CDIR_PIN_IMPORT);                // must keep it pinned
	
	containing_import = dir;  // imported exports nested under *in

	dout(7) << " new import at " << *dir << endl;
  }


  // take out my temp pin
  dir->put(CDIR_PIN_IMPORTING);

  // add any inherited exports
  for (list<inodeno_t>::iterator it = m->get_exports().begin();
       it != m->get_exports().end();
       it++) {
    CInode *exi = get_inode(*it);
    assert(exi && exi->dir);
	CDir *ex = exi->dir;

	dout(15) << " nested export " << *ex << endl;

	// remove our pin
	ex->put(CDIR_PIN_IMPORTINGEXPORT);
	ex->state_clear(CDIR_STATE_IMPORTINGEXPORT);


	// add...
    if (ex->is_import()) {
      dout(7) << " importing my import " << *ex << endl;
      imports.erase(ex);
      ex->state_clear(CDIR_STATE_IMPORT);

      mds->logger->inc("imex");

      // move nested exports under containing_import
	  for (set<CDir*>::iterator it = nested_exports[ex].begin();
		   it != nested_exports[ex].end();
		   it++) {
        dout(7) << "     moving nested export " << **it << " under " << *containing_import << endl;
		nested_exports[containing_import].insert(*it);
	  }
      nested_exports.erase(ex);	      // de-list under old import
      
	  ex->set_dir_auth( CDIR_AUTH_PARENT );
      ex->put(CDIR_PIN_IMPORT);       // imports are pinned, no longer import

    } else {
      dout(7) << " importing export " << *ex << endl;

      // add it
	  ex->state_set(CDIR_STATE_EXPORT);
      ex->get(CDIR_PIN_EXPORT);           // all exports are pinned
      exports.insert(ex);
      nested_exports[containing_import].insert(ex);
      mds->logger->inc("imex");
    }
    
  }


  // add this crap to my cache
  list<inodeno_t> imported_subdirs;
  bufferlist dir_state;
  dir_state.claim( m->get_state() );
  int off = 0;
  int num_imported_inodes = 0;

  for (int i = 0; i < m->get_ndirs(); i++) {
	num_imported_inodes += 
	  import_dir_block(dir_state, 
					   off,
					   oldauth, 
					   dir,                 // import root
					   imported_subdirs);
  }
  dout(10) << " " << imported_subdirs.size() << " imported subdirs" << endl;
  dout(10) << " " << m->get_exports().size() << " imported nested exports" << endl;
  

  // adjust popularity
  mds->balancer->add_import(dir);

  // send notify's etc.
  dout(7) << "sending notifyack for " << *dir << " to old auth " << MSG_ADDR_NUM(m->get_source()) << endl;
  mds->messenger->send_message(new MExportDirNotifyAck(dir->inode->ino()),
							   m->get_source(), MDS_PORT_CACHE,
							   MDS_PORT_CACHE);

  dout(7) << "sending notify to others" << endl;
  for (set<int>::iterator it = dir->open_by.begin();
	   it != dir->open_by.end();
	   it++) {
	assert( *it != mds->get_nodeid() );
	if ( *it == MSG_ADDR_NUM(m->get_source()) ) continue;  // not to old auth.

	MExportDirNotify *notify = new MExportDirNotify(dir->ino(), MSG_ADDR_NUM(m->get_source()), mds->get_nodeid());
	notify->copy_exports(m->get_exports());

	if (g_conf.mds_verify_export_dirauth)
	  notify->copy_subdirs(imported_subdirs);   // copy subdir list (DEBUG)

	mds->messenger->send_message(notify,
								 MSG_ADDR_MDS( *it ), MDS_PORT_CACHE,
								 MDS_PORT_CACHE);
  }
  
  // done
  delete m;

  show_imports();


  // is it empty?
  if (dir->get_size() == 0 &&
	  !dir->inode->is_auth()) {
	// reexport!
	export_empty_import(dir);
  }


  // some stats
  mds->logger->inc("im");
  mds->logger->inc("iim", num_imported_inodes);
  mds->logger->set("nim", imports.size());


  // FIXME LOG IT

  /*
	stupid hashing crap, FIXME

  // wait for replicas in hashed dirs?
  if (import_hashed_replicate_waiting.count(m->get_ino())) {
	// it'll happen later!, when i get my inodegetreplicaack's back
  } else {
	// finish now
	//not anymoreimport_dir_finish(dir);
  }
  */

}



void MDCache::handle_export_dir_finish(MExportDirFinish *m)
{
  CInode *diri = get_inode(m->get_ino());
  CDir *dir = diri->dir;
  assert(dir);

  dout(7) << "handle_export_dir_finish on " << *dir << endl;
  assert(dir->is_auth());

  dout(5) << "done with import of " << *dir << endl;
  show_imports();
  mds->logger->set("nex", exports.size());
  mds->logger->set("nim", imports.size());

  // un auth pin (other exports can now proceed)
  dir->auth_unpin();  
  
  // ok now finish contexts
  dout(5) << "finishing any waiters on imported data" << endl;
  dir->finish_waiting(CDIR_WAIT_IMPORTED);

  delete m;
}


void MDCache::decode_import_inode(CDentry *dn, bufferlist& bl, int& off, int oldauth)
{  
  CInodeExport istate;
  off = istate._decode(bl, off);
  dout(15) << "got a cinodeexport " << endl;
  
  bool added = false;
  CInode *in = get_inode(istate.get_ino());
  if (!in) {
	in = new CInode;
	added = true;
  } else {
	in->set_auth(true);
  }

  // link before state
  if (dn->inode != in) {
	assert(!dn->inode);
	dn->dir->link_inode(dn, in);
  }

  // state after link
  set<int> merged_client_caps;
  istate.update_inode(in, merged_client_caps);
 
 
  // add inode?
  if (added) {
	add_inode(in);
	dout(10) << "added " << *in << endl;
  } else {
	dout(10) << "  had " << *in << endl;
  }
  
  
  // cached_by
  assert(!in->is_cached_by(oldauth));
  in->cached_by_add( oldauth, CINODE_EXPORT_NONCE );
  if (in->is_cached_by(mds->get_nodeid()))
	in->cached_by_remove(mds->get_nodeid());
  
  // twiddle locks
  // hard
  if (in->hardlock.get_state() == LOCK_GLOCKR) {
	in->hardlock.gather_set.erase(mds->get_nodeid());
	in->hardlock.gather_set.erase(oldauth);
	if (in->hardlock.gather_set.empty())
	  inode_hard_eval(in);
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
								 MSG_ADDR_CLIENT(*it), 0, MDS_PORT_CACHE);
  }

  // filelock
  if (!in->filelock.is_stable()) {
	// take me and old auth out of gather set
	in->filelock.gather_set.erase(mds->get_nodeid());
	in->filelock.gather_set.erase(oldauth);
	if (in->filelock.gather_set.empty())  // necessary but not suffient...
	  inode_file_eval(in);	
  }

  // other
  if (in->is_dirty()) {
	dout(10) << "logging dirty import " << *in << endl;
	mds->mdlog->submit_entry(new EInodeUpdate(in));
  }
}


int MDCache::import_dir_block(bufferlist& bl,
							  int& off,
							  int oldauth,
							  CDir *import_root,
							  list<inodeno_t>& imported_subdirs)
{
  // set up dir
  CDirExport dstate;
  off = dstate._decode(bl, off);

  CInode *diri = get_inode(dstate.get_ino());
  assert(diri);
  CDir *dir = diri->get_or_open_dir(mds);
  assert(dir);
 
  dout(7) << " import_dir_block " << *dir << " have " << dir->nitems << " items, importing " << dstate.get_nden() << " dentries" << endl;

  // add to list
  if (dir != import_root)
    imported_subdirs.push_back(dir->ino());

  // assimilate state
  dstate.update_dir( dir );
  if (diri->is_auth()) 
	dir->set_dir_auth( CDIR_AUTH_PARENT );   // update_dir may hose dir_auth

  // mark  (may already be marked from get_or_open_dir() above)
  if (!dir->is_auth())
	dir->state_set(CDIR_STATE_AUTH);

  // open_by
  assert(!dir->is_open_by(oldauth));
  dir->open_by_add(oldauth);
  if (dir->is_open_by(mds->get_nodeid()))
	dir->open_by_remove(mds->get_nodeid());

  if (dir->is_hashed()) {

	// do nothing; dir is hashed
	return 0;
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
	int num_imported = 0;
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
	  }
	  
	  // mark dentry dirty?  (only _after_ we link the inode!)
	  if (dirty == 'D') dn->mark_dirty();
	  
	}

	if (dir->is_dirty()) 
	  mds->mdlog->submit_entry(new EDirUpdate(dir));

	return num_imported;
  }
}





// authority bystander

void MDCache::handle_export_dir_warning(MExportDirWarning *m)
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


void MDCache::handle_export_dir_notify(MExportDirNotify *m)
{
  CDir *dir = 0;
  CInode *in = get_inode(m->get_ino());
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
	  CInode *n = get_inode(*it);
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
		CInode *diri = get_inode(*it);
		if (!diri) continue;  // don't have it, don't care
		if (!diri->dir) continue;
		dout(10) << "handle_export_dir_notify checking subdir " << *diri->dir << " is auth " << diri->dir->get_dir_auth() << endl;
		assert(diri->dir != dir);	  // base shouldn't be in subdir list
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
  mds->messenger->send_message(new MExportDirNotifyAck(m->get_ino()),
							   MSG_ADDR_MDS(m->get_old_auth()), MDS_PORT_CACHE, MDS_PORT_CACHE);
  

  // done
  stray_export_warnings.erase( m->get_ino() );
  delete m;
}





// =======================================================================
// HASHING


void MDCache::import_hashed_content(CDir *dir, bufferlist& bl, int nden, int oldauth)
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
		dn->inode->dir->put(CDIR_PIN_IMPORTINGEXPORT);
		dn->inode->dir->state_clear(CDIR_STATE_IMPORTINGEXPORT);

		if (dn->inode->dir->is_auth()) {
		  // mine.  must have been an import.
		  assert(dn->inode->dir->is_import());
		  dout(7) << "unimporting subdir now that inode is mine " << *dn->inode->dir << endl;
		  dn->inode->dir->set_dir_auth( CDIR_AUTH_PARENT );
		  imports.erase(dn->inode->dir);
		  dn->inode->dir->put(CDIR_PIN_IMPORT);
		  dn->inode->dir->state_clear(CDIR_STATE_IMPORT);
		  
		  // move nested under hashdir
		  for (set<CDir*>::iterator it = nested_exports[dn->inode->dir].begin();
			   it != nested_exports[dn->inode->dir].end();
			   it++) 
			nested_exports[dir].insert(*it);
		  nested_exports.erase(dn->inode->dir);

		  // now it matches the inode
		  dn->inode->dir->set_dir_auth( CDIR_AUTH_PARENT );
		}
		else {
		  // not mine.  make it an export.
		  dout(7) << "making subdir into export " << *dn->inode->dir << endl;
		  dn->inode->dir->get(CDIR_PIN_EXPORT);
		  dn->inode->dir->state_set(CDIR_STATE_EXPORT);
		  exports.insert(dn->inode->dir);
		  nested_exports[dir].insert(dn->inode->dir);
		  
		  if (dn->inode->dir->get_dir_auth() == CDIR_AUTH_PARENT)
			dn->inode->dir->set_dir_auth( oldauth );		  // no longer matches inode
		  assert(dn->inode->dir->get_dir_auth() >= 0);
		}
	  }
	}
	
	// mark dentry dirty?  (only _after_ we link the inode!)
	dn->mark_dirty();
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

  - export_dir_walk and import_dir_block take care with dir_auth:   (for import/export)
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
  MDS *mds;
  CDir *dir;
  C_MDC_HashFreeze(MDS *mds, CDir *dir) {
	this->mds = mds;
	this->dir = dir;
  }
  virtual void finish(int r) {
	mds->mdcache->hash_dir_frozen(dir);
  }
};

class C_MDC_HashComplete : public Context {
public:
  MDS *mds;
  CDir *dir;
  C_MDC_HashComplete(MDS *mds, CDir *dir) {
	this->mds = mds;
	this->dir = dir;
  }
  virtual void finish(int r) {
	mds->mdcache->hash_dir_complete(dir);
  }
};


/** hash_dir(dir)
 * start hashing a directory.
 */
void MDCache::hash_dir(CDir *dir)
{
  dout(7) << "hash_dir " << *dir << endl;

  assert(!dir->is_hashed());
  assert(dir->is_auth());
  
  if (dir->is_frozen() ||
	  dir->is_freezing()) {
	dout(7) << " can't hash, freezing|frozen." << endl;
	return;
  }

  // pin path?
  vector<CDentry*> trace;
  make_trace(trace, dir->inode);
  if (!path_pin(trace, 0, 0)) {
	dout(7) << "hash_dir couldn't pin path, failing." << endl;
	return;
  }

  // ok, go
  dir->state_set(CDIR_STATE_HASHING);
  dir->get(CDIR_PIN_HASHING);
  assert(dir->hashed_subset.empty());

  // discover on all mds
  assert(hash_gather.count(dir) == 0);
  for (int i=0; i<mds->get_cluster()->get_num_mds(); i++) {
	if (i == mds->get_nodeid()) continue;  // except me
	hash_gather[dir].insert(i);
	mds->messenger->send_message(new MHashDirDiscover(dir->inode),
								 MSG_ADDR_MDS(i), MDS_PORT_CACHE, MDS_PORT_CACHE);
  }
  dir->auth_pin();  // pin until discovers are all acked.
  
  // start freeze
  dir->freeze_dir(new C_MDC_HashFreeze(mds, dir));

  // make complete
  if (!dir->is_complete()) {
	dout(7) << "hash_dir " << *dir << " not complete, fetching" << endl;
	mds->mdstore->fetch_dir(dir,
							new C_MDC_HashComplete(mds, dir));
  } else
	hash_dir_complete(dir);
}


/*
 * wait for everybody to discover and open the hashing dir
 *  then auth_unpin, to let the freeze happen
 */
void MDCache::handle_hash_dir_discover_ack(MHashDirDiscoverAck *m)
{
  CInode *in = get_inode(m->get_ino());
  assert(in);
  CDir *dir = in->dir;
  assert(dir);
  
  int from = MSG_ADDR_NUM(m->get_source());
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
void MDCache::hash_dir_complete(CDir *dir)
{
  dout(7) << "hash_dir_complete " << *dir << ", dirtying inodes" << endl;

  assert(!dir->is_hashed());
  assert(dir->is_auth());
  
  // mark dirty to pin in cache
  for (CDir_map_t::iterator it = dir->begin(); 
	   it != dir->end(); 
	   it++) {
	CInode *in = it->second->inode;
	in->mark_dirty();
  }
  
  if (dir->is_frozen_dir())
	hash_dir_go(dir);
}


/*
 * once the dir is frozen,
 *  make sure it's complete
 *  send the prep messages!
 */
void MDCache::hash_dir_frozen(CDir *dir)
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
  vector<MHashDirPrep*> msgs(mds->get_cluster()->get_num_mds());

  // check for subdirs
  for (CDir_map_t::iterator it = dir->begin(); 
	   it != dir->end(); 
	   it++) {
	CDentry *dn = it->second;
	CInode *in = dn->inode;
	
	if (!in->is_dir()) continue;
	if (!in->dir) continue;
	
	int dentryhashcode = mds->get_cluster()->hash_dentry( dir->ino(), it->first );
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
	  mds->messenger->send_message(msgs[i], MSG_ADDR_MDS(i), MDS_PORT_CACHE, MDS_PORT_CACHE);
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
void MDCache::handle_hash_dir_prep_ack(MHashDirPrepAck *m)
{
  CInode *in = get_inode(m->get_ino());
  assert(in);
  CDir *dir = in->dir;
  assert(dir);

  int from = MSG_ADDR_NUM(m->get_source());

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
void MDCache::hash_dir_go(CDir *dir)
{
  dout(7) << "hash_dir_go " << *dir << endl;
  
  assert(!dir->is_hashed());
  assert(dir->is_auth());
  assert(dir->is_frozen_dir());

  // get messages to other nodes ready
  vector<MHashDir*> msgs(mds->get_cluster()->get_num_mds());
  for (int i=0; i<mds->get_cluster()->get_num_mds(); i++) {
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
  CDir *containing_import = get_auth_container(dir);
  assert(containing_import != dir || dir->is_import());  

  // divy up contents
  for (CDir_map_t::iterator it = dir->begin(); 
	   it != dir->end(); 
	   it++) {
	CDentry *dn = it->second;
	CInode *in = dn->inode;

	int dentryhashcode = mds->get_cluster()->hash_dentry( dir->ino(), it->first );
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
	in->state_set(CINODE_STATE_PROXY);
	in->get(CINODE_PIN_PROXY);

	// fix up subdirs
	if (in->dir) {
	  if (in->dir->is_auth()) {
		// mine.  make it into an import.
		dout(7) << "making subdir into import " << *in->dir << endl;
		in->dir->set_dir_auth( mds->get_nodeid() );
		imports.insert(in->dir);
		in->dir->get(CDIR_PIN_IMPORT);
		in->dir->state_set(CDIR_STATE_IMPORT);

		// fix nested bits
		for (set<CDir*>::iterator it = nested_exports[containing_import].begin();
			 it != nested_exports[containing_import].end(); ) {
		  CDir *ex = *it;  
		  it++;
		  if (get_auth_container(ex) == in->dir) {
			dout(10) << "moving nested export " << *ex << endl;
			nested_exports[containing_import].erase(ex);
			nested_exports[in->dir].insert(ex);
		  }
		}
	  }
	  else {
		// not mine.
		dout(7) << "un-exporting subdir that's being hashed away " << *in->dir << endl;
		assert(in->dir->is_export());
		in->dir->put(CDIR_PIN_EXPORT);
		in->dir->state_clear(CDIR_STATE_EXPORT);
		exports.erase(in->dir);
		nested_exports[containing_import].erase(in->dir);
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
  dir->get(CDIR_PIN_HASHED);
  hashdirs.insert(dir);
  dir->mark_dirty();
  mds->mdlog->submit_entry(new EDirUpdate(dir));

  // inode state
  if (dir->inode->is_auth()) {
	dir->inode->mark_dirty();
	mds->mdlog->submit_entry(new EInodeUpdate(dir->inode));
  }

  // fix up nested_exports?
  if (containing_import != dir) {
	dout(7) << "moving nested exports under hashed dir" << endl;
	for (set<CDir*>::iterator it = nested_exports[containing_import].begin();
		 it != nested_exports[containing_import].end(); ) {
	  CDir *ex = *it;
	  it++;
	  if (get_auth_container(ex) == dir) {
		dout(7) << " moving nested export under hashed dir: " << *ex << endl;
		nested_exports[containing_import].erase(ex);
		nested_exports[dir].insert(ex);
	  } else {
		dout(7) << " NOT moving nested export under hashed dir: " << *ex << endl;
	  }
	}
  }

  // send hash messages
  assert(hash_gather[dir].empty());
  assert(hash_notify_gather[dir].empty());
  assert(dir->hashed_subset.empty());
  for (int i=0; i<mds->get_cluster()->get_num_mds(); i++) {
	// all nodes hashed locally..
	dir->hashed_subset.insert(i);

	if (i == mds->get_nodeid()) continue;

	// init hash_gather and hash_notify_gather sets
	hash_gather[dir].insert(i);
	
	assert(hash_notify_gather[dir][i].empty());
	for (int j=0; j<mds->get_cluster()->get_num_mds(); j++) {
	  if (j == mds->get_nodeid()) continue;
	  if (j == i) continue;
	  hash_notify_gather[dir][i].insert(j);
	}

	mds->messenger->send_message(msgs[i],
								 MSG_ADDR_MDS(i), MDS_PORT_CACHE, MDS_PORT_CACHE);
  }

  // wait for all the acks.
}


void MDCache::handle_hash_dir_ack(MHashDirAck *m)
{
  CInode *in = get_inode(m->get_ino());
  assert(in);
  CDir *dir = in->dir;
  assert(dir);

  assert(dir->is_hashed());
  assert(dir->is_hashing());

  int from = MSG_ADDR_NUM(m->get_source());
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


void MDCache::hash_dir_finish(CDir *dir)
{
  dout(7) << "hash_dir_finish finishing " << *dir << endl;
  assert(dir->is_hashed());
  assert(dir->is_hashing());
  
  // dir state
  hash_gather.erase(dir);
  dir->state_clear(CDIR_STATE_HASHING);
  dir->put(CDIR_PIN_HASHING);
  dir->hashed_subset.clear();

  // unproxy inodes
  //  this _could_ happen sooner, on a per-peer basis, but no harm in waiting a few more seconds.
  for (list<CInode*>::iterator it = hash_proxy_inos[dir].begin();
	   it != hash_proxy_inos[dir].end();
	   it++) {
	CInode *in = *it;
	assert(in->state_test(CINODE_STATE_PROXY));
	in->state_clear(CINODE_STATE_PROXY);
	in->put(CINODE_PIN_PROXY);
  }
  hash_proxy_inos.erase(dir);

  // unpin path
  vector<CDentry*> trace;
  make_trace(trace, dir->inode);
  path_unpin(trace, 0);

  // unfreeze
  dir->unfreeze_dir();

  show_imports();
  assert(hash_gather.count(dir) == 0);

  // stats
  //mds->logger->inc("nh", 1);

}




// HASH on auth and non-auth

void MDCache::handle_hash_dir_notify(MHashDirNotify *m)
{
  CInode *in = get_inode(m->get_ino());
  assert(in);
  CDir *dir = in->dir;
  assert(dir);
  assert(dir->is_hashing());

  dout(5) << "handle_hash_dir_notify " << *dir << endl;
  int from = m->get_from();

  int source = MSG_ADDR_NUM(m->get_source());
  if (dir->is_auth()) {
	// gather notifies
	assert(dir->is_hashed());
	
	assert(	hash_notify_gather[dir][from].count(source) );
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
	  
	  int dentryhashcode = mds->get_cluster()->hash_dentry( dir->ino(), it->first );
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
	  dir->put(CDIR_PIN_HASHING);
	  dir->hashed_subset.clear();
	} else {
	  dout(7) << "still waiting for notify from " << hash_gather[dir] << endl;
	}

	// fw notify to auth
	mds->messenger->send_message(m, MSG_ADDR_MDS(dir->authority()), MDS_PORT_CACHE, MDS_PORT_CACHE);
  }
}




// HASH on non-auth

/*
 * discover step:
 *  each peer needs to open up the directory and pin it before we start
 */
class C_MDC_HashDirDiscover : public Context {
  MDCache *mdc;
  MHashDirDiscover *m;
public:
  vector<CDentry*> trace;
  C_MDC_HashDirDiscover(MDCache *mdc, MHashDirDiscover *m) {
	this->mdc = mdc;
	this->m = m;
  }
  void finish(int r) {
	CInode *in = 0;
	if (r >= 0) {
	  if (trace.size())
		in = trace[trace.size()-1]->get_inode();
	  else
		in = mdc->get_root();
	}
	mdc->handle_hash_dir_discover_2(m, in, r);
  }
};  

void MDCache::handle_hash_dir_discover(MHashDirDiscover *m)
{
  assert(MSG_ADDR_NUM(m->get_source()) != mds->get_nodeid());

  dout(7) << "handle_hash_dir_discover on " << m->get_path() << endl;

  // must discover it!
  C_MDC_HashDirDiscover *onfinish = new C_MDC_HashDirDiscover(this, m);
  filepath fpath(m->get_path());
  path_traverse(fpath, onfinish->trace, true,
				m, new C_MDS_RetryMessage(mds,m),       // on delay/retry
				MDS_TRAVERSE_DISCOVER,
				onfinish);  // on completion|error
}

void MDCache::handle_hash_dir_discover_2(MHashDirDiscover *m, CInode *in, int r)
{
  // yay!
  if (in) {
	dout(7) << "handle_hash_dir_discover_2 has " << *in << endl;
  }

  if (r < 0 || !in->is_dir()) {
	dout(7) << "handle_hash_dir_discover_2 failed to discover or not dir " << m->get_path() << ", NAK" << endl;
	assert(0);	// this shouldn't happen if the auth pins his path properly!!!! 
  }
  assert(in->is_dir());

  // is dir open?
  if (!in->dir) {
	dout(7) << "handle_hash_dir_discover_2 opening dir " << *in << endl;
	open_remote_dir(in,
					new C_MDS_RetryMessage(mds, m));
	return;
  }
  CDir *dir = in->dir;

  // pin dir, set hashing flag
  dir->state_set(CDIR_STATE_HASHING);
  dir->get(CDIR_PIN_HASHING);
  assert(dir->hashed_subset.empty());
  
  // inode state
  dir->inode->inode.hash_seed = 1;// dir->ino();
  if (dir->inode->is_auth()) {
	dir->inode->mark_dirty();
	mds->mdlog->submit_entry(new EInodeUpdate(dir->inode));
  }

  // get gather set ready for notifies
  assert(hash_gather[dir].empty());
  for (int i=0; i<mds->get_cluster()->get_num_mds(); i++) {
	if (i == mds->get_nodeid()) continue;
	if (i == dir->authority()) continue;
	hash_gather[dir].insert(i);
  }

  // reply
  dout(7) << " sending hash_dir_discover_ack on " << *dir << endl;
  mds->messenger->send_message(new MHashDirDiscoverAck(dir->ino()),
							   m->get_source(), MDS_PORT_CACHE, MDS_PORT_CACHE);
  delete m;
}

/*
 * prep step:
 *  peers need to open up all subdirs of the hashed dir
 */

void MDCache::handle_hash_dir_prep(MHashDirPrep *m)
{
  CInode *in = get_inode(m->get_ino());
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
	  CInode *in = get_inode( it->second->get_ino() );
	  if (in) {
		it->second->update_inode(in);
		dout(5) << " updated " << *in << endl;
	  } else {
		in = new CInode(false);
		it->second->update_inode(in);
		add_inode(in);
		
		// link 
		dir->add_dentry( it->first, in );
		dout(5) << "   added " << *in << endl;
	  }

	  // open!
	  if (!in->dir) {
		dout(5) << "  opening nested export on " << *in << endl;
		open_remote_dir(in,
						new C_MDS_RetryMessage(mds, m));
	  }
	}
  }

  // verify!
  int waiting_for = 0;
  for (map<string,CInodeDiscover*>::iterator it = m->get_inodes().begin();
	   it != m->get_inodes().end();
	   it++) {
	CInode *in = get_inode( it->second->get_ino() );
	assert(in);

	if (in->dir) {
	  if (!in->dir->state_test(CDIR_STATE_IMPORTINGEXPORT)) {
		dout(5) << "  pinning nested export " << *in->dir << endl;
		in->dir->get(CDIR_PIN_IMPORTINGEXPORT);
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
  mds->messenger->send_message(new MHashDirPrepAck(dir->ino()),
							   m->get_source(), MDS_PORT_CACHE, MDS_PORT_CACHE);
  
  // done.
  delete m;
}


/*
 * hash step:
 */

void MDCache::handle_hash_dir(MHashDir *m)
{
  CInode *in = get_inode(m->get_ino());
  assert(in);
  CDir *dir = in->dir;
  assert(dir);
  assert(!dir->is_auth());
  assert(!dir->is_hashed());
  assert(dir->is_hashing());

  dout(5) << "handle_hash_dir " << *dir << endl;
  int oldauth = MSG_ADDR_NUM(m->get_source());

  // content
  import_hashed_content(dir, m->get_state(), m->get_nden(), oldauth);

  // dir state
  dir->state_set(CDIR_STATE_HASHED);
  dir->get(CDIR_PIN_HASHED);
  hashdirs.insert(dir);
  dir->hashed_subset.insert(mds->get_nodeid());

  // dir is complete
  dir->mark_complete();
  dir->mark_dirty();
  mds->mdlog->submit_entry(new EDirUpdate(dir));

  // commit
  mds->mdstore->commit_dir(dir, 0);
  
  // send notifies
  dout(7) << "sending notifies" << endl;
  for (int i=0; i<mds->get_cluster()->get_num_mds(); i++) {
	if (i == mds->get_nodeid()) continue;
	if (i == MSG_ADDR_NUM(m->get_source())) continue;
	mds->messenger->send_message(new MHashDirNotify(dir->ino(), mds->get_nodeid()),
								 MSG_ADDR_MDS(i), MDS_PORT_CACHE, MDS_PORT_CACHE);
  }

  // ack
  dout(7) << "acking" << endl;
  mds->messenger->send_message(new MHashDirAck(dir->ino()),
							   m->get_source(), MDS_PORT_CACHE, MDS_PORT_CACHE);
  
  // done.
  delete m;

  show_imports();
}





// UNHASH on auth

class C_MDC_UnhashFreeze : public Context {
public:
  MDS *mds;
  CDir *dir;
  C_MDC_UnhashFreeze(MDS *mds, CDir *dir) {
	this->mds = mds;
	this->dir = dir;
  }
  virtual void finish(int r) {
	mds->mdcache->unhash_dir_frozen(dir);
  }
};

class C_MDC_UnhashComplete : public Context {
public:
  MDS *mds;
  CDir *dir;
  C_MDC_UnhashComplete(MDS *mds, CDir *dir) {
	this->mds = mds;
	this->dir = dir;
  }
  virtual void finish(int r) {
	mds->mdcache->unhash_dir_complete(dir);
  }
};


void MDCache::unhash_dir(CDir *dir)
{
  dout(7) << "unhash_dir " << *dir << endl;

  assert(dir->is_hashed());
  assert(!dir->is_unhashing());
  assert(dir->is_auth());
  assert(hash_gather.count(dir)==0);

  // pin path?
  vector<CDentry*> trace;
  make_trace(trace, dir->inode);
  if (!path_pin(trace, 0, 0)) {
	dout(7) << "unhash_dir couldn't pin path, failing." << endl;
	return;
  }

  // twiddle state
  dir->state_set(CDIR_STATE_UNHASHING);

  // first, freeze the dir.
  dir->freeze_dir(new C_MDC_UnhashFreeze(mds, dir));

  // make complete
  if (!dir->is_complete()) {
	dout(7) << "unhash_dir " << *dir << " not complete, fetching" << endl;
	mds->mdstore->fetch_dir(dir,
							new C_MDC_UnhashComplete(mds, dir));
  } else
	unhash_dir_complete(dir);

}

void MDCache::unhash_dir_frozen(CDir *dir)
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
void MDCache::unhash_dir_prep(CDir *dir)
{
  dout(7) << "unhash_dir_prep " << *dir << endl;
  assert(dir->is_hashed());
  assert(dir->is_auth());
  assert(dir->is_frozen_dir());
  assert(dir->is_complete());

  if (!hash_gather[dir].empty()) return;  // already been here..freeze must have been instantaneous

  // send unhash prep to all peers
  assert(hash_gather[dir].empty());
  for (int i=0; i<mds->get_cluster()->get_num_mds(); i++) {
	if (i == mds->get_nodeid()) continue;
	hash_gather[dir].insert(i);
	mds->messenger->send_message(new MUnhashDirPrep(dir->ino()),
								 MSG_ADDR_MDS(i), MDS_PORT_CACHE, MDS_PORT_CACHE);
  }
}

/* 
 * wait for peers to freeze and complete hashed dirs
 */
void MDCache::handle_unhash_dir_prep_ack(MUnhashDirPrepAck *m)
{
  CInode *in = get_inode(m->get_ino());
  assert(in);
  CDir *dir = in->dir;
  assert(dir);
  
  int from = MSG_ADDR_NUM(m->get_source());
  dout(7) << "handle_unhash_dir_prep_ack from " << from << " " << *dir << endl;

  if (!m->did_assim()) {
    m->mark_assim();  // only do this the first time!
	
	// assimilate dentry+inodes for exports
	for (map<string,CInodeDiscover*>::iterator it = m->get_inodes().begin();
		 it != m->get_inodes().end();
		 it++) {
	  CInode *in = get_inode( it->second->get_ino() );
	  if (in) {
		it->second->update_inode(in);
		dout(5) << " updated " << *in << endl;
	  } else {
		in = new CInode(false);
		it->second->update_inode(in);
		add_inode(in);
		
		// link 
		dir->add_dentry( it->first, in );
		dout(5) << "   added " << *in << endl;
	  }
	  
	  // open!
	  if (!in->dir) {
		dout(5) << "  opening nested export on " << *in << endl;
		open_remote_dir(in,
						new C_MDS_RetryMessage(mds, m));
	  }
	}
  }
  
  // verify!
  int waiting_for = 0;
  for (map<string,CInodeDiscover*>::iterator it = m->get_inodes().begin();
	   it != m->get_inodes().end();
	   it++) {
	CInode *in = get_inode( it->second->get_ino() );
	assert(in);
	
	if (in->dir) {
	  if (!in->dir->state_test(CDIR_STATE_IMPORTINGEXPORT)) {
		dout(5) << "  pinning nested export " << *in->dir << endl;
		in->dir->get(CDIR_PIN_IMPORTINGEXPORT);
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
void MDCache::unhash_dir_go(CDir *dir)
{
  dout(7) << "unhash_dir_go " << *dir << endl;
  assert(dir->is_hashed());
  assert(dir->is_auth());
  assert(dir->is_frozen_dir());
  assert(dir->is_complete());

  // send unhash prep to all peers
  assert(hash_gather[dir].empty());
  for (int i=0; i<mds->get_cluster()->get_num_mds(); i++) {
	if (i == mds->get_nodeid()) continue;
	hash_gather[dir].insert(i);
	mds->messenger->send_message(new MUnhashDir(dir->ino()),
								 MSG_ADDR_MDS(i), MDS_PORT_CACHE, MDS_PORT_CACHE);
  }
}

/*
 * auth:
 *  assimilate unhashing content
 */
void MDCache::handle_unhash_dir_ack(MUnhashDirAck *m)
{
  CInode *in = get_inode(m->get_ino());
  assert(in);
  CDir *dir = in->dir;
  assert(dir);
  
  dout(7) << "handle_unhash_dir_ack " << *dir << endl;
  assert(dir->is_hashed());

  // assimilate content
  int from = MSG_ADDR_NUM(m->get_source());
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
  CDir *containing_import = get_auth_container(dir);
  if (containing_import != dir) {
	for (set<CDir*>::iterator it = nested_exports[dir].begin();
		 it != nested_exports[dir].end();
		 it++) {
	  dout(7) << "moving nested export out from under hashed dir : " << **it << endl;
	  nested_exports[containing_import].insert(*it);
	}
	nested_exports.erase(dir);
  }
  
  // dir state
  //dir->state_clear(CDIR_STATE_UNHASHING); //later
  dir->state_clear(CDIR_STATE_HASHED);
  dir->put(CDIR_PIN_HASHED);
  hashdirs.erase(dir);
  
  // commit!
  assert(dir->is_complete());
  //dir->mark_complete();
  dir->mark_dirty();
  mds->mdstore->commit_dir(dir, 0);

  // inode state
  dir->inode->inode.hash_seed = 0;
  if (dir->inode->is_auth()) {
	dir->inode->mark_dirty();
	mds->mdlog->submit_entry(new EInodeUpdate(dir->inode));
  }
  
  // notify
  assert(hash_gather[dir].empty());
  for (int i=0; i<mds->get_cluster()->get_num_mds(); i++) {
	if (i == mds->get_nodeid()) continue;

	hash_gather[dir].insert(i);
	
	mds->messenger->send_message(new MUnhashDirNotify(dir->ino()),
								 MSG_ADDR_MDS(i), MDS_PORT_CACHE, MDS_PORT_CACHE);
  }
}


/*
 * sent by peer to flush mds links.  unfreeze when all gathered.
 */
void MDCache::handle_unhash_dir_notify_ack(MUnhashDirNotifyAck *m)
{
  CInode *in = get_inode(m->get_ino());
  assert(in);
  CDir *dir = in->dir;
  assert(dir);
  
  dout(7) << "handle_unhash_dir_ack " << *dir << endl;
  assert(!dir->is_hashed());
  assert(dir->is_unhashing());
  assert(dir->is_frozen_dir());

  // done?
  int from = MSG_ADDR_NUM(m->get_source());
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
void MDCache::unhash_dir_finish(CDir *dir)
{
  dout(7) << "unhash_dir_finish " << *dir << endl;
  hash_gather.erase(dir);

  // unpin path
  vector<CDentry*> trace;
  make_trace(trace, dir->inode);
  path_unpin(trace, 0);

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
void MDCache::unhash_dir_complete(CDir *dir)
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
	  in->mark_dirty();
	  mds->mdlog->submit_entry(new EInodeUpdate(in));
	}
  }
  
  if (!dir->is_frozen_dir()) {
	dout(7) << "dir complete but !frozen, waiting " << *dir << endl;
  } else {
	if (dir->is_auth())
	  unhash_dir_prep(dir);	        // auth
	else
	  unhash_dir_prep_finish(dir);  // nonauth
  }
}


// UNHASH on non-auth

class C_MDC_UnhashPrepFreeze : public Context {
public:
  MDS *mds;
  CDir *dir;
  C_MDC_UnhashPrepFreeze(MDS *mds, CDir *dir) {
	this->mds = mds;
	this->dir = dir;
  }
  virtual void finish(int r) {
	mds->mdcache->unhash_dir_prep_frozen(dir);
  }
};


/*
 * peers need to freeze their dir and make them complete
 */
void MDCache::handle_unhash_dir_prep(MUnhashDirPrep *m)
{
  CInode *in = get_inode(m->get_ino());
  assert(in);
  CDir *dir = in->dir;
  assert(dir);
  
  dout(7) << "handle_unhash_dir_prep " << *dir << endl;
  assert(dir->is_hashed());

  // freeze
  dir->freeze_dir(new C_MDC_UnhashPrepFreeze(mds, dir));

  // make complete
  if (!dir->is_complete()) {
	dout(7) << "unhash_dir " << *dir << " not complete, fetching" << endl;
	mds->mdstore->fetch_dir(dir,
							new C_MDC_UnhashComplete(mds, dir));
  } else {
	unhash_dir_complete(dir);
  }
  
  delete m;
}

/*
 * peer has hashed dir frozen.  
 *  complete too?
 */
void MDCache::unhash_dir_prep_frozen(CDir *dir)
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
void MDCache::unhash_dir_prep_finish(CDir *dir)
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
	
	int dentryhashcode = mds->get_cluster()->hash_dentry( dir->ino(), it->first );
	if (dentryhashcode != mds->get_nodeid()) continue;
	
	// msg?
	ack->add_inode(it->first, in->replicate_to(auth));
  }
  
  // ack
  mds->messenger->send_message(ack,
							   MSG_ADDR_MDS(auth), MDS_PORT_CACHE, MDS_PORT_CACHE);
}



/*
 * peer needs to send hashed dir content back to auth.
 *  unhash dir.
 */
void MDCache::handle_unhash_dir(MUnhashDir *m)
{
  CInode *in = get_inode(m->get_ino());
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

	int dentryhashcode = mds->get_cluster()->hash_dentry( dir->ino(), it->first );
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
	in->state_set(CINODE_STATE_PROXY);
	in->get(CINODE_PIN_PROXY);
	hash_proxy_inos[dir].push_back(in);

	if (in->dir) {
	  if (in->dir->is_auth()) {
		// mine.  make it into an import.
		dout(7) << "making subdir into import " << *in->dir << endl;
		in->dir->set_dir_auth( mds->get_nodeid() );
		imports.insert(in->dir);
		in->dir->get(CDIR_PIN_IMPORT);
		in->dir->state_set(CDIR_STATE_IMPORT);
	  }
	  else {
		// not mine.
		dout(7) << "un-exporting subdir that's being unhashed away " << *in->dir << endl;
		assert(in->dir->is_export());
		in->dir->put(CDIR_PIN_EXPORT);
		in->dir->state_clear(CDIR_STATE_EXPORT);
		exports.erase(in->dir);
		nested_exports[dir].erase(in->dir);
	  }
	}
	
	// waiters
	list<Context*> waiters;
	in->take_waiting(CINODE_WAIT_ANY, waiters);
	fin->take(waiters);
  }

  // we should have no nested exports; we're not auth for the dir!
  assert(nested_exports[dir].empty());
  nested_exports.erase(dir);

  // dir state
  //dir->state_clear(CDIR_STATE_UNHASHING);  // later
  dir->state_clear(CDIR_STATE_HASHED);
  dir->put(CDIR_PIN_HASHED);
  hashdirs.erase(dir);
  dir->mark_clean();

  // inode state
  dir->inode->inode.hash_seed = 0;
  if (dir->inode->is_auth()) {
	dir->inode->mark_dirty();
	mds->mdlog->submit_entry(new EInodeUpdate(dir->inode));
  }

  // init gather set
  hash_gather[dir] = mds->get_cluster()->get_mds_set();
  hash_gather[dir].erase(mds->get_nodeid());

  // send unhash message
  mds->messenger->send_message(new MUnhashDirAck(dir->ino(), bl, nden),
							   MSG_ADDR_MDS(dir->authority()), MDS_PORT_CACHE, MDS_PORT_CACHE);
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
void MDCache::handle_unhash_dir_notify(MUnhashDirNotify *m)
{
  CInode *in = get_inode(m->get_ino());
  assert(in);
  CDir *dir = in->dir;
  assert(dir);
  
  dout(7) << "handle_unhash_dir_finish " << *dir << endl;
  assert(!dir->is_hashed());
  assert(dir->is_unhashing());
  assert(!dir->is_auth());
  
  int from = MSG_ADDR_NUM(m->get_source());
  assert(hash_gather[dir].count(from) == 1);
  hash_gather[dir].erase(from);
  delete m;

  // did we send our shout out?
  if (from == dir->authority()) {
	// send notify to everyone else in weird chatter storm
	for (int i=0; i<mds->get_cluster()->get_num_mds(); i++) {
	  if (i == from) continue;
	  if (i == mds->get_nodeid()) continue;
	  mds->messenger->send_message(new MUnhashDirNotify(dir->ino()),
								   MSG_ADDR_MDS(i), MDS_PORT_CACHE, MDS_PORT_CACHE);
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
	assert(in->state_test(CINODE_STATE_PROXY));
	in->state_clear(CINODE_STATE_PROXY);
	in->put(CINODE_PIN_PROXY);
  }

  // unfreeze
  dir->unfreeze_dir();
  
  // ack
  dout(7) << "sending notify_ack to auth for unhash of " << *dir << endl;
  mds->messenger->send_message(new MUnhashDirNotifyAck(dir->ino()),
							   MSG_ADDR_MDS(dir->authority()), MDS_PORT_CACHE, MDS_PORT_CACHE);
  
}














// ==============================================================
// debug crap


void MDCache::show_imports()
{
  mds->balancer->show_imports();
}


void MDCache::show_cache()
{
  dout(7) << "show_cache" << endl;
  for (inode_map_t::iterator it = inode_map.begin();
	   it != inode_map.end();
	   it++) {
	dout(7) << *((*it).second) << endl;
	
	CDentry *dn = (*it).second->get_parent_dn();
	if (dn) 
	  dout(7) << "       dn " << *dn << endl;
	if ((*it).second->dir) 
	  dout(7) << "   subdir " << *(*it).second->dir << endl;
  }
}


// hack
vector<CInode*> MDCache::hack_add_file(string& fn, CInode *in) {
  
  // root?
  if (fn == "/") {
	if (!root) {
	  root = in;
	  add_inode( in );
	  dout(7) << " added root " << root << endl;
	} else {
	  root->inode.ino = in->inode.ino;  // bleh
	}
	vector<CInode*> trace;
	trace.push_back(root);
	return trace;
  } 


  // file.
  int lastslash = fn.rfind("/");
  string dirpart = fn.substr(0,lastslash);
  string file = fn.substr(lastslash+1);

  //dout(7) << "dirpart '" << dirpart << "' filepart '" << file << "' inode " << in << endl;
  
  CInode *diri = hack_get_file(dirpart);
  assert(diri);

  //dout(7) << " got dir " << diri << endl;

  if (diri->dir == NULL) {
	dout(4) << " making " << *diri << " into a dir" << endl;
	diri->inode.mode &= ~INODE_MODE_FILE;
	diri->inode.mode |= INODE_MODE_DIR;
  }

  assert(diri->is_dir());
  diri->get_or_open_dir(mds);
  
  add_inode( in );
  diri->dir->add_dentry( file, in );

  if (in->is_dir())
	in->get_or_open_dir(mds);

  vector<CInode*> trace;
  trace.push_back(diri);
  trace.push_back(in);
  while (diri->parent) {
	diri = diri->parent->dir->inode;
	trace.insert(trace.begin(),diri);
  }
  return trace;
}

CInode* MDCache::hack_get_file(string& fn) {
  unsigned off = 1;
  CInode *cur = root;
  
  // dirs
  while (off < fn.length()) {
	unsigned int slash = fn.find("/", off);
	if (slash == string::npos) 
	  slash = fn.length();	
	string n = fn.substr(off, slash-off);

	//dout(7) << " looking up '" << n << "' in " << cur << endl;

	if (cur->dir == NULL) {
	  //dout(7) << "   not a directory!" << endl;
	  return NULL;  // this isn't a directory.
	}

	CDentry* den = cur->dir->lookup(n);
	if (den == NULL) return NULL;   // file dne!
	cur = den->inode;
	off = slash+1;	
  }

  //dump();
  lru.lru_status();

  return cur;  
}
