
#include "MDCache.h"
#include "MDStore.h"
#include "CInode.h"
#include "CDir.h"
#include "MDS.h"
#include "MDCluster.h"
#include "MDLog.h"
#include "MDBalancer.h"

#include "include/filepath.h"

#include "include/Message.h"
#include "include/Messenger.h"

#include "events/EInodeUpdate.h"
#include "events/EInodeUnlink.h"

#include "messages/MDiscover.h"
#include "messages/MDiscoverReply.h"
#include "messages/MInodeGetReplica.h"
#include "messages/MInodeGetReplicaAck.h"

#include "messages/MExportDirDiscover.h"
#include "messages/MExportDirDiscoverAck.h"
#include "messages/MExportDirPrep.h"
#include "messages/MExportDirPrepAck.h"
#include "messages/MExportDirWarning.h"
#include "messages/MExportDir.h"
#include "messages/MExportDirNotify.h"
#include "messages/MExportDirNotifyAck.h"
#include "messages/MExportDirFinish.h"

#include "messages/MHashDir.h"
#include "messages/MUnhashDir.h"
#include "messages/MUnhashDirAck.h"

#include "messages/MInodeUpdate.h"
#include "messages/MDirUpdate.h"

#include "messages/MInodeExpire.h"
#include "messages/MDirExpire.h"

#include "messages/MInodeUnlink.h"
#include "messages/MInodeUnlinkAck.h"

#include "messages/MInodeSyncStart.h"
#include "messages/MInodeSyncAck.h"
#include "messages/MInodeSyncRelease.h"
#include "messages/MInodeSyncRecall.h"

#include "messages/MInodeLockStart.h"
#include "messages/MInodeLockAck.h"
#include "messages/MInodeLockRelease.h"

#include "messages/MDirSyncStart.h"
#include "messages/MDirSyncAck.h"
#include "messages/MDirSyncRelease.h"
//#include "messages/MDirSyncRecall.h"

#include "InoAllocator.h"

#include <assert.h>
#include <errno.h>
#include <iostream>
#include <string>
#include <map>
using namespace std;

#include "include/config.h"
#undef dout
#define  dout(l)    if (l<=g_conf.debug) cout << "mds" << mds->get_nodeid() << ".cache "



MDCache::MDCache(MDS *m)
{
  mds = m;
  root = NULL;
  lru = new LRU();
  lru->lru_set_max(g_conf.mdcache_size);
  lru->lru_set_midpoint(g_conf.mdcache_mid);

  inoalloc = new InoAllocator(mds);
}

MDCache::~MDCache() 
{
  if (lru) { delete lru; lru = NULL; }
  if (inoalloc) { delete inoalloc; inoalloc = NULL; }
}


// 

bool MDCache::shutdown()
{
  if (lru->lru_get_size() > 0) {
	dout(7) << "WARNING: mdcache shutodwn with non-empty cache" << endl;
	show_cache();
	show_imports();
  }
}


// MDCache

CInode *MDCache::create_inode()
{
  CInode *in = new CInode;

  // zero
  memset(&in->inode, 0, sizeof(inode_t));
  
  // assign ino
  in->inode.ino = inoalloc->get_ino();

  add_inode(in);  // add
  return in;
}

void MDCache::destroy_inode(CInode *in)
{
  inoalloc->reclaim_ino(in->ino());
  remove_inode(in);
}


void MDCache::add_inode(CInode *in) 
{
  // add to lru, inode map
  assert(inode_map.size() == lru->lru_get_size());
  lru->lru_insert_mid(in);
  assert(inode_map.count(in->ino()) == 0);  // should be no dup inos!
  inode_map[ in->ino() ] = in;
  assert(inode_map.size() == lru->lru_get_size());
}

void MDCache::remove_inode(CInode *o) 
{  
  unlink_inode(o);              // unlink
  inode_map.erase(o->ino());    // remove from map
  lru->lru_remove(o);           // remove from lru
}

void MDCache::unlink_inode(CInode *in)
{
  // detach from parents
  if (in->nparents == 1) {
	CDentry *dn = in->parent;

	// unlink auth_pin count
	dn->dir->adjust_nested_auth_pins( 0 - (in->auth_pins + in->nested_auth_pins) );

	// explicitly define auth
	in->dangling_auth = in->authority();
	dout(10) << "unlink_inode " << *in << " dangling_auth now " << in->dangling_auth << endl;

	// detach
	dn->dir->remove_child(dn);
	in->remove_parent(dn);
	delete dn;
	in->nparents = 0;
	in->parent = NULL;
  } 
  else if (in->nparents > 1) {
	assert(in->nparents <= 1);  // not implemented
  } else {
	assert(in->nparents == 0);  // root or dangling.
	assert(in->parent == NULL);
  }
}



bool MDCache::trim(__int32_t max) {
  if (max < 0) {
	max = lru->lru_get_max();
	if (!max) return false;
  }

  while (lru->lru_get_size() > max) {
	CInode *in = (CInode*)lru->lru_expire();
	if (!in) return false;

	if (in->dir) {
	  // notify dir authority?
	  int auth = in->dir->authority();
	  if (auth != mds->get_nodeid()) {
		dout(7) << "sending dir_expire to mds" << auth << " on " << *in->dir << endl;
		mds->messenger->send_message(new MDirExpire(in->ino(), mds->get_nodeid(), in->dir->replica_nonce),
									 MSG_ADDR_MDS(auth), MDS_PORT_CACHE,
									 MDS_PORT_CACHE);
	  }
	}

	// notify inode authority?
	int auth = in->authority();
	if (auth != mds->get_nodeid()) {
	  dout(7) << "sending inode_expire to mds" << auth << " on " << *in << endl;
	  mds->messenger->send_message(new MInodeExpire(in->ino(), mds->get_nodeid(), in->replica_nonce),
								   MSG_ADDR_MDS(auth), MDS_PORT_CACHE,
								   MDS_PORT_CACHE);
	}	

	CInode *idir = NULL;
	if (in->parent)
	  idir = in->parent->dir->inode;

	if (in->is_root()) {
	  dout(7) << "just trimmed root, cache now empty." << endl;
	  root = NULL;
	}

	// remove it
	dout(11) << "trim removing " << *in << " " << in << endl;
	remove_inode(in);
	delete in;

	if (idir) {
	  // dir incomplete!
	  idir->dir->state_clear(CDIR_STATE_COMPLETE);

	  // reexport?
	  if (imports.count(idir->dir) &&                // import
		  idir->dir->get_size() == 0 &&         // no children
		  !idir->is_root() &&                   // not root
		  !(idir->dir->is_freezing() || idir->dir->is_frozen())  // FIXME: can_auth_pin?
		  ) {
		int dest = idir->authority();
		
		// comment this out ot wreak havoc?
		if (mds->is_shutting_down()) dest = 0;  // this is more efficient.

		if (dest != mds->get_nodeid()) {
		  // it's an empty import!
		  dout(7) << "trimmed parent dir is a (now empty) import; rexporting to " << dest << endl;
		  export_dir( idir->dir, dest );
		}
	  }
	} 
  }
  
  return true;
}


void MDCache::shutdown_start()
{
  dout(1) << "shutdown_start: forcing unsync, unlock of everything" << endl;

  // walk cache
  bool didsomething = false;
  for (hash_map<inodeno_t, CInode*>::iterator it = inode_map.begin();
	   it != inode_map.end();
	   it++) {
	CInode *in = it->second;
	if (in->is_auth()) {
	  if (in->is_syncbyme()) inode_sync_release(in);
	  if (in->is_lockbyme()) inode_lock_release(in);
	}
  }

  // make sure sticky sync is off
  // WHY: if sync sticks it may not unravel of its own accord; sticky
  //  relies on additional requests/etc. to trigger an unsync when
  //  needed, but we're just trimming caches.
  g_conf.mdcache_sticky_sync_normal = false;

}

bool MDCache::shutdown_pass()
{
  static bool did_inode_updates = false;

  dout(7) << "shutdown_pass" << endl;
  //assert(mds->is_shutting_down());
  if (mds->is_shut_down()) {
	cout << " already shut down" << endl;
	show_cache();
	show_imports();
	return true;
  }

  // make a pass on the cache
  
  if (mds->mdlog->get_num_events()) {
	dout(7) << "waiting for log to flush" << endl;
	return false;
  } 

  dout(7) << "log is empty; flushing cache" << endl;
  trim(0);
  
  dout(7) << "cache size now " << lru->lru_get_size() << endl;

  // send inode_expire's on all potentially cache pinned items
  //no: expires now reliable; leaves will always expire
  if (false &&
	  !did_inode_updates) {
	did_inode_updates = true;

	for (hash_map<inodeno_t, CInode*>::iterator it = inode_map.begin();
		 it != inode_map.end();
		 it++) {
	  if (it->second->ref_set.count(CINODE_PIN_CACHED)) 
		send_inode_updates(it->second);  // send an update to discover who dropped the ball
	}
  }

  // send all imports back to 0.
  if (mds->get_nodeid() != 0) {
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
  } else {
	// shut down root?
	if (lru->lru_get_size() == 1) {
	  // all i have left is root
	  dout(7) << "wahoo, all i have left is root!" << endl;
	  
	  if (root->is_pinned_by(CINODE_PIN_DIRTY))   // no root storage yet.
		root->put(CINODE_PIN_DIRTY);

	  if (root->dir->ref == 1) { // (that's the import pin)

		// un-import
		imports.erase(root->dir);
		root->dir->state_clear(CDIR_STATE_IMPORT);
		root->dir->put(CDIR_PIN_IMPORT);

		assert(root->ref == 0);

		// and trim!
		trim(0);

		assert(lru->lru_get_size() == 0);
		assert(root == 0);
		
	  } else {
		dout(1) << "ugh wtf, root still has extra pins: " << *root->dir << endl;
	  }
	  
	  show_cache();
	  show_imports();
	}
  }
	
  // sanity
  assert(inode_map.size() == lru->lru_get_size());

  // done?
  if (lru->lru_get_size() == 0) {
	if (mds->get_nodeid() != 0) {
	  dout(7) << "done, sending shutdown_finish" << endl;
	  mds->messenger->send_message(new Message(MSG_MDS_SHUTDOWNFINISH),
								   MSG_ADDR_MDS(0), MDS_PORT_MAIN, MDS_PORT_MAIN);
	} else {
	  mds->handle_shutdown_finish(NULL);
	}
	return true;
  } else {
	dout(7) << "there's still stuff in the cache: " << lru->lru_get_size() << endl;
	show_cache();
  }
  return false;
}




int MDCache::link_inode( CDir *dir, string& dname, CInode *in ) 
{
  assert(dir->lookup(dname) == 0);

  // create dentry
  CDentry* dn = new CDentry(dname, in);
  in->add_parent(dn);

  // add to dir
  dir->add_child(dn);

  // set dir version
  in->parent_dir_version = dir->get_version();

  return 0;
}




int MDCache::open_root(Context *c)
{
  int whoami = mds->get_nodeid();

  // open root inode
  if (whoami == 0) { 
	// i am root inode
	CInode *root = new CInode();
	root->inode.ino = 1;
	root->inode.isdir = true;

	// make it up (FIXME)
	root->inode.mode = 0755;
	root->inode.size = 0;
	root->inode.touched = 0;

	root->state_set(CINODE_STATE_ROOT);

	set_root( root );

	// root directory too
	assert(root->dir == NULL);
	root->set_dir( new CDir(root, mds, true) );
	root->dir->dir_auth = 0;  // me!
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
									 want);
	  mds->messenger->send_message(req,
								   MSG_ADDR_MDS(0), MDS_PORT_CACHE,
								   MDS_PORT_CACHE);
	} else {
	  dout(7) << "waiting for root" << endl;
	}	

	// wait
	waiting_for_root.push_back(c);

  }
}


CDir *MDCache::get_containing_import(CDir *in)
{
  CDir *imp = in;  // might be *in

  // find the underlying import!
  while (imp && 
		 !imp->is_import()) {
	imp = imp->get_parent_dir();
  }

  assert(imp);
  return imp;
}

CDir *MDCache::get_containing_export(CDir *in)
{
  CDir *ex = in;  // might be *in

  // find the underlying import!
  while (ex &&                        // white not at root,
		 exports.count(ex) == 0) {    // we didn't find an export,
	ex = ex->get_parent_dir();
  }

  return ex;
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


  case MSG_MDS_INODEUPDATE:
	handle_inode_update((MInodeUpdate*)m);
	break;

  case MSG_MDS_DIRUPDATE:
	handle_dir_update((MDirUpdate*)m);
	break;

  case MSG_MDS_INODEEXPIRE:
	handle_inode_expire((MInodeExpire*)m);
	break;


  case MSG_MDS_INODEUNLINK:
	handle_inode_unlink((MInodeUnlink*)m);
	break;
  case MSG_MDS_INODEUNLINKACK:
	handle_inode_unlink_ack((MInodeUnlinkAck*)m);
	break;


	// sync
  case MSG_MDS_INODESYNCSTART:
	handle_inode_sync_start((MInodeSyncStart*)m);
	break;
  case MSG_MDS_INODESYNCACK:
	handle_inode_sync_ack((MInodeSyncAck*)m);
	break;
  case MSG_MDS_INODESYNCRELEASE:
	handle_inode_sync_release((MInodeSyncRelease*)m);
	break;
  case MSG_MDS_INODESYNCRECALL:
	handle_inode_sync_recall((MInodeSyncRecall*)m);
	break;
	
	// lock
  case MSG_MDS_INODELOCKSTART:
	handle_inode_lock_start((MInodeLockStart*)m);
	break;
  case MSG_MDS_INODELOCKACK:
	handle_inode_lock_ack((MInodeLockAck*)m);
	break;
  case MSG_MDS_INODELOCKRELEASE:
	handle_inode_lock_release((MInodeLockRelease*)m);
	break;
	


	// import
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
  case MSG_MDS_EXPORTDIRPREPACK:
	handle_export_dir_prep_ack((MExportDirPrepAck*)m);
	break;
  case MSG_MDS_EXPORTDIRNOTIFYACK:
	handle_export_dir_notify_ack((MExportDirNotifyAck*)m);
	break;	

	// export 3rd party (inode authority)
  case MSG_MDS_EXPORTDIRNOTIFY:
	handle_export_dir_notify((MExportDirNotify*)m);
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
 */
int MDCache::path_traverse(filepath& path, 
						   vector<CInode*>& trace, 
						   Message *req,
						   int onfail)
{
  int whoami = mds->get_nodeid();
  
  // root
  CInode *cur = get_root();
  if (cur == NULL) {
	dout(7) << "mds" << whoami << " i don't have root" << endl;
	if (req) 
	  open_root(new C_MDS_RetryMessage(mds, req));
	return 1;
  }

  // start trace
  trace.clear();
  trace.push_back(cur);

  string have_clean;

  for (int depth = 0; depth < path.depth(); depth++) {
	dout(12) << " path seg " << path[depth] << endl;
	
	if (!cur->is_dir()) {
	  dout(7) << *cur << " not a dir " << cur->inode.isdir << endl;
	  return -ENOTDIR;
	}

	// open dir
	if (!cur->dir) {
	  if (cur->dir_is_auth()) {
		cur->get_or_open_dir(mds);
	  } else {
		// discover dir from/via inode auth
		assert(!cur->is_auth());
		filepath want = path.subpath(depth);
		mds->messenger->send_message(new MDiscover(mds->get_nodeid(),
												   cur->ino(),
												   want,
												   true),
									 MSG_ADDR_MDS(cur->authority()), MDS_PORT_CACHE,
									 MDS_PORT_CACHE);
		cur->dir->add_waiter(CINODE_WAIT_DIR, 
							 path[depth], 
							 new C_MDS_RetryMessage(mds, req));
	  }
	}
	
	// frozen?
	if (cur->dir->is_frozen()) {
	  // doh!
	  // FIXME: traverse is allowed?
	  dout(7) << *cur->dir << " is frozen, waiting" << endl;
	  cur->dir->add_waiter(CDIR_WAIT_UNFREEZE,
						   new C_MDS_RetryMessage(mds, req));
	  return 1;
	}
	
	// must read hard data to traverse
	if (!read_hard_try(cur, req))
	  return 1;
	
	// check permissions?
	
	
	// dentry
	CDentry *dn = cur->dir->lookup(path[depth]);
	if (dn && dn->inode) {
	  // have it, keep going.
	  cur = dn->inode;
	  have_clean += "/";
	  have_clean += path[depth];
	  
	  trace.push_back(cur);
	  continue;
	}
	
	// don't have it.
	int dauth = cur->dir->dentry_authority( path[depth] );
	dout(12) << " dentry " << path[depth] << " dauth is " << dauth << endl;
	

	if (dauth == whoami) {
	  // mine.
	  if (cur->dir->is_complete()) {
		// file not found
		return -ENOENT;
	  } else {
		if (onfail == MDS_TRAVERSE_DISCOVER) 
		  return -1;
		
		// directory isn't complete; reload
		dout(7) << " incomplete dir contents for " << *cur << ", fetching" << endl;
		lru->lru_touch(cur);  // touch readdiree
		mds->mdstore->fetch_dir(cur->dir, new C_MDS_RetryMessage(mds, req));
		
		mds->logger->inc("cmiss");
		mds->logger->inc("rdir");
		return 1;		   
	  }
	} else {
	  // not mine.
	  
	  if (onfail == MDS_TRAVERSE_DISCOVER) {
		// discover
		filepath want = path.subpath(depth);
		
		dout(7) << " discover on " << *cur << " for " << want.get_path() << " to mds" << dauth << endl;
		
		lru->lru_touch(cur);  // touch discoveree
		
		mds->messenger->send_message(new MDiscover(mds->get_nodeid(),
												   cur->ino(),
												   want,
												   false),
									 MSG_ADDR_MDS(dauth), MDS_PORT_CACHE,
									 MDS_PORT_CACHE);
		
		// delay processing of current request
		cur->dir->add_waiter(CDIR_WAIT_DENTRY, 
							 path[depth], 
							 new C_MDS_RetryMessage(mds, req));
		
		mds->logger->inc("dis");
		mds->logger->inc("cmiss");
		return 1;
	  } 
	  if (onfail == MDS_TRAVERSE_FORWARD) {
		// forward
		dout(7) << " not auth for " << path[depth] << ", fwd to mds" << dauth << endl;
		mds->messenger->send_message(req,
									 MSG_ADDR_MDS(dauth), req->get_dest_port(),
									 req->get_dest_port());
		//show_imports();
		
		mds->logger->inc("cfw");
		return 1;
	  }	
	  if (onfail == MDS_TRAVERSE_FAIL) {
		return -1;  // -ENOENT, but only because i'm not the authority
	  }
	}
	
	assert(0);  // i shouldn't get here
  }
  
  // success.
  return 0;
}




// REPLICAS


void MDCache::handle_discover(MDiscover *dis) 
{
  int whoami = mds->get_nodeid();
  
  CDir *dir = 0;
  MDiscoverReply *reply = 0;

  // get started.
  if (dis->get_base_ino() == 0) {
    // wants root
    dout(7) << "handle_discover from mds" << dis->get_asker() << " wants root + " << dis->get_want().get_path() << endl;

    assert(mds->get_nodeid() == 0);
    assert(root->is_auth());

    // add root
    reply = new MDiscoverReply(0);
    reply->add_inode( new CInodeDiscover( root, 
										  root->cached_by_add( dis->get_asker() ) ) );

    dir = root->dir;
    
  } else {
    // there's a base inode
    CInode *in = get_inode(dis->get_base_ino());
    assert(in);
    
    dout(7) << "handle_discover from mds" << dis->get_asker() << " has " << *in << " wants " << dis->get_want().get_path() << endl;
    
    assert(in->is_dir());
    
    dir = in->get_or_open_dir(mds);
    assert(dir);

    if (dir->is_proxy() && !dir->is_hashed()) {
      // fwd to dir auth
      dout(7) << "i am proxy, fwd to dir_auth " << dir->authority() << endl;
      mds->messenger->send_message( dis,
                                    MSG_ADDR_MDS( dir->authority() ), MDS_PORT_CACHE, MDS_PORT_CACHE );
      return;
    }

    // create reply
    reply = new MDiscoverReply(in->ino());
  }

  assert(reply);
  assert(dir);
  
  // add content.
  for (int i = 0; i < dis->get_want().depth() || dis->get_want().depth() == 0; i++) {
    // add dir
    if (reply->is_empty() && !dis->wants_base_dir()) {
      dout(7) << "they don't want the base dir" << endl;
    } else {
      // add dir
      if (!dir->is_auth()) {
        dout(7) << *dir << " auth is " << dir->authority() << ", i'm done" << endl;
        break;
      }

	  // frozen?
	  /* hmmm do we care, actually?
	  if (dir->is_frozen()) {
        dout(7) << *dir << " frozen, waiting" << endl;
		dir->add_waiter(new C_MDS_RetryMessage( dis, mds ));
		delete reply;
		return;
	  }
	  */

      dout(7) << "adding dir " << *dir << endl;
      reply->add_dir( new CDirDiscover( dir, 
										dir->open_by_add( dis->get_asker() ) ) );
    }
    if (dis->get_want().depth() == 0) break;
    
    // lookup dentry
    int dentry_auth = dir->dentry_authority( dis->get_dentry(i) );
    if (dentry_auth != mds->get_nodeid()) {
      dout(7) << *dir << "dentry " << dis->get_dentry(i) << " auth " << dentry_auth << ", i'm done." << endl;
      break;      // that's it for us!
    }

    // get inode
    CDentry *dn = dir->lookup( dis->get_dentry(i) );
    if (dn) {
		CInode *next = dn->inode;
        assert(next->is_auth());

        // add dentry + inode
        dout(7) << "adding dentry " << dn << " + " << *next << endl;
        reply->add_dentry( dis->get_dentry(i) );
        reply->add_inode( new CInodeDiscover(next, 
											 next->cached_by_add(dis->get_asker())) );
    } else {
      // don't have it?
      if (dir->is_complete()) {
        // ...
		// i shoudl return an error falg in the reply... FIXME
        assert(0);  // for now
      } else {
        delete reply;

        // readdir
        dout(7) << "mds" << whoami << " incomplete dir contents for " << *dir << ", fetching" << endl;
        mds->mdstore->fetch_dir(dir, new C_MDS_RetryMessage(mds, dis));
        return;
      }
    }
  }
       
  // how did we do.
  if (reply->is_empty()) {
    // discard empty reply
    delete reply;

    // forward?
    if (dir->is_proxy()) {
      // fwd to auth
      dout(7) << "i am dir proxy, fwd to auth " << dir->authority() << endl;
      mds->messenger->send_message( dis,
                                    MSG_ADDR_MDS( dir->authority() ), MDS_PORT_CACHE, MDS_PORT_CACHE );
      return;
    }

    dout(7) << "i'm not auth or proxy, dropping" << endl;
    
  } else {
    // send back to asker
    dout(7) << "sending result back to asker " << dis->get_asker() << endl;
    mds->messenger->send_message(reply,
                                 dis->get_asker(), MDS_PORT_CACHE, MDS_PORT_CACHE);
  }

  // done.
  delete dis;
}


void MDCache::handle_discover_reply(MDiscoverReply *m) 
{
  // starting point
  CInode *cur;
  
  if (m->has_root()) {
	// nowhere!
	dout(7) << "handle_discover_reply root + " << m->get_path() << endl;
	assert(!root);
  } else {
	// grab inode
	cur = get_inode(m->get_base_ino());
	
	if (!cur) {
	  dout(7) << "handle_discover_reply don't have base ino " << m->get_base_ino() << ", dropping" << endl;
	  delete m;
	  return;
	}
	
	dout(7) << "handle_discover_reply " << *cur << " + " << m->get_path() << endl;
  }
  
  list<Context*> finished;

  for (int i=0; i<m->get_num_inodes(); i++) {
	// dir
	if (i || m->has_base_dir()) {
	  if (cur->dir) {
		// had it
		dout(7) << "had " << *cur->dir;
		m->get_dir(i).update_dir(cur->dir);
		dout2(7) << ", now " << *cur->dir << endl;
	  } else {
		// add it (_replica_)
		cur->set_dir( new CDir(cur, mds, false) );
		m->get_dir(i).update_dir(cur->dir);
		dout(7) << "added " << *cur->dir;
	  }
	}	

	// lookup dentry
	CInode *in = 0;
	if (i || m->has_base_dentry()) {
	  CDentry *dn = cur->dir->lookup( m->get_dentry(i) );
	  if (dn) in = dn->get_inode();
	}
	
	// inode
	if (in) {
	  // had it
	  assert(in == get_inode( m->get_ino(i) ));
	  
	  dout(7) << "had " << *in;
	  m->get_inode(i).update_inode(in);
	  dout2(7) << ", now " << *in << endl;

	} else {
	  // add inode
	  in = new CInode(false);

	  m->get_inode(0).update_inode(in);
	  
	  if (!i && m->has_root()) {
		// root
		in->state_set(CINODE_STATE_ROOT);
		set_root( in );
		
		finished.splice(finished.end(), waiting_for_root);
	  } else {
		// link in
		add_inode( in );
		link_inode( cur->dir, m->get_dentry(i), in );
		
		cur->dir->take_waiting(CDIR_WAIT_DENTRY,
							   m->get_dentry(i),
							   finished);
	  }
	  
	  dout(7) << "added " << *in << endl;
	}
	  
	// onward!
	cur = in;
  }

  // finish
  dout(7) << finished.size() << " contexts to finish" << endl;
  list<Context*>::iterator it;
  for (it = finished.begin(); 
	   it != finished.end(); 
	   it++) {
	Context *c = *it;
	c->finish(0);
	delete c;
  }

  // done
  delete m;
}




void MDCache::handle_inode_get_replica(MInodeGetReplica *m)
{
  CInode *in = get_inode(m->get_ino());
  if (!in) {
	dout(7) << "handle_inode_get_replica don't have inode for ino " << m->get_ino() << endl;
	assert(0);
	return;
  }

  dout(7) << "handle_inode_get_replica from " << m->get_source() << " for " << *in << endl;

  // add to cached_by
  int nonce = in->cached_by_add(m->get_source());
  
  // add bit
  //**** hmm do we put any data in the reply?  not for the limited instances
  // when is this used?  FIXME?
  
  // reply
  mds->messenger->send_message(new MInodeGetReplicaAck(in->ino(), nonce),
							   MSG_ADDR_MDS(m->get_source()), MDS_PORT_CACHE, MDS_PORT_CACHE);

  // done.
  delete m;
}


void MDCache::handle_inode_get_replica_ack(MInodeGetReplicaAck *m)
{
  CInode *in = get_inode(m->get_ino());
  assert(in);

  dout(7) << "handle_inode_get_replica_ack from " << m->get_source() << " on " << *in << " nonce " << m->get_nonce() << endl;

  in->replica_nonce = m->get_nonce();

  // waiters
  in->finish_waiting(CINODE_WAIT_GETREPLICA);

  delete m;  
}






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
	dout(7) << "got inode_update on " << m->get_ino() << ", don't have it, sending expire" << endl;
	mds->messenger->send_message(new MInodeExpire(m->get_ino(), mds->get_nodeid(), m->get_nonce()),
								 m->get_source(), MDS_PORT_CACHE,
								 MDS_PORT_CACHE);
	goto out;
  }

  if (in->is_auth()) {
	dout(7) << "got inode_update on " << *in << ", but i'm the authority!" << endl;
	assert(0); // this should never happen
  }
  
  dout(7) << "got inode_update on " << *in << endl;

  // update! NOTE dir_auth is unaffected by this.
  in->decode_basic_state(m->get_payload());

 out:
  // done
  delete m;
}



void MDCache::handle_inode_expire(MInodeExpire *m)
{
  CInode *in = get_inode(m->get_ino());
  int from = m->get_from();
  
  if (!in) {
	dout(7) << "got inode_expire on " << m->get_ino() << " from " << from << ", don't have it" << endl;
	assert(in);  // OOPS  i should be authority, or recent authority (and thus frozen).
  }
  
  if (!in->is_auth()) {
	// check proxy maps
	int newauth = ino_proxy_auth(in->ino(), m->get_from());
	dout(7) << "got inode_expire on " << *in << ", proxy set sez new auth is " << newauth << endl;
	assert(newauth >= 0);     // we should know the new authority!
	assert(in->is_frozen());  // i should be frozen right now!
	assert(in->state_test(CINODE_STATE_PROXY));
	
	// forward
	mds->messenger->send_message(m,
								 MSG_ADDR_MDS(newauth), MDS_PORT_CACHE, MDS_PORT_CACHE);
	mds->logger->inc("iupfw");
	return;
  }

  // check nonce
  if (m->get_nonce() == in->get_cached_by_nonce(from)) {
	// remove from our cached_by
	dout(7) << "got inode_expire on " << *in << " from mds" << from << " cached_by was " << in->cached_by << endl;
	in->cached_by_remove(from);
  } 
  else {
	// this is an old nonce, ignore expire.
	dout(7) << "got inode_expire on " << *in << " from mds" << from << " with old (?) nonce " << m->get_nonce() << ", dropping" << endl;
	assert(0);  // just for now.. this is actually totally normal.
  }

  // done
  delete m;
}


int MDCache::send_dir_updates(CDir *dir, int except)
{
  
  // FIXME   ?

  int whoami = mds->get_nodeid();
  for (set<int>::iterator it = dir->inode->cached_by_begin(); 
	   it != dir->inode->cached_by_end(); 
	   it++) {
	if (*it == whoami) continue;
	if (*it == except) continue;
	dout(7) << "sending dir_update on " << *dir << " to " << *it << endl;
	mds->messenger->send_message(new MDirUpdate(dir->ino(),
												dir->dir_rep,
												dir->dir_rep_by),
								 MSG_ADDR_MDS(*it), MDS_PORT_CACHE,
								 MDS_PORT_CACHE);
  }

  return 0;
}


void MDCache::handle_dir_update(MDirUpdate *m)
{
  CInode *in = get_inode(m->get_ino());
  if (!in) {
	dout(7) << "dir_update on " << m->get_ino() << ", don't have it" << endl;
	goto out;
  }

  // update!
  if (!in->dir) {
	dout(7) << "dropping dir_update on " << m->get_ino() << ", ->dir is null" << endl;	
	goto out;
  } 

  dout(7) << "dir_update on " << m->get_ino() << endl;
  
  in->dir->dir_rep = m->get_dir_rep();
  in->dir->dir_rep_by = m->get_dir_rep_by();

  // done
 out:
  delete m;
}







// NAMESPACE FUN

class C_MDC_InodeLog : public Context {
public:
  MDCache *mdc;
  CInode *in;
  C_MDC_InodeLog(MDCache *mdc, CInode *in) {
	this->mdc = mdc;
	this->in = in;
  }
  virtual void finish(int r) {
	in->mark_safe();
	mdc->inode_unlink_finish(in);
  }
};

void MDCache::inode_unlink(CInode *in, Context *c)
{
  assert(in->is_auth());
  assert(!in->is_presync());
  assert(!in->is_prelock());
  
  // drop any sync, lock on inode
  if (in->is_syncbyme()) inode_sync_release(in);
  assert(!in->is_sync());
  if (in->is_lockbyme()) inode_lock_release(in);
  assert(!in->is_lockbyme());

  // add the waiter
  in->add_waiter(CINODE_WAIT_UNLINK, c);
  
  // log it
  in->mark_unsafe();
  mds->mdlog->submit_entry(new EInodeUnlink(in, in->get_parent_dir()),
						   new C_MDC_InodeLog(this,in));
  
  // unlink
  unlink_inode( in );
  in->state_set(CINODE_STATE_DANGLING);
  in->mark_clean();   // don't care anymore!

  // tell replicas
  if (in->is_cached_by_anyone()) {
	for (set<int>::iterator it = in->cached_by_begin();
		 it != in->cached_by_end();
		 it++) {
	  dout(7) << "handle_client_unlink sending unlinkreplica to " << *it << endl;
  
	  mds->messenger->send_message(new MInodeUnlink(in->ino()),
								   MSG_ADDR_MDS(*it), MDS_PORT_CACHE, MDS_PORT_CACHE);
	}

	in->get(CINODE_PIN_UNLINKING);
	in->state_set(CINODE_STATE_UNLINKING);
	in->unlink_waiting_for_ack = in->cached_by;
  }
}


void MDCache::inode_unlink_finish(CInode *in)
{
  if (in->is_unsafe() ||
	  in->is_unlinking()) {
	dout(7) << "inode_unlink_finish stll waiting on " << *in << endl;
	return;
  }

  // done! finish
  in->finish_waiting(CINODE_WAIT_UNLINK);
}

void MDCache::handle_inode_unlink(MInodeUnlink *m)
{
  CInode *in = get_inode(m->get_ino());
  if (!in) {
	dout(7) << "handle_inode_unlink don't have ino " << m->get_ino() << endl;
	mds->messenger->send_message(new MInodeUnlinkAck(m->get_ino(), false),
								 m->get_source(), MDS_PORT_CACHE, MDS_PORT_CACHE);
	delete m;
	return;
  }
  
  dout(7) << "handle_inode_unlink on " << *in << endl;

  // unlink
  unlink_inode( in );
  in->state_set(CINODE_STATE_DANGLING);

  mds->messenger->send_message(new MInodeUnlinkAck(m->get_ino()),
							   m->get_source(), MDS_PORT_CACHE, MDS_PORT_CACHE);
  delete m;
  return;
}

void MDCache::handle_inode_unlink_ack(MInodeUnlinkAck *m)
{
  CInode *in = get_inode(m->get_ino());
  assert(in);
  assert(in->is_auth());
  assert(in->is_unlinking());
  
  int from = m->get_source();
  dout(7) << "handle_inode_unlink_ack from " << from << " on " << *in << endl;

  assert(in->unlink_waiting_for_ack.count(from));
  in->unlink_waiting_for_ack.erase(from);
  
  if (in->unlink_waiting_for_ack.empty()) {
	// done unlinking!
	in->state_clear(CINODE_STATE_UNLINKING);
	in->put(CINODE_PIN_UNLINKING);
	inode_unlink_finish(in);
  }

  // done
  delete m;
}





// locks ----------------------------------------------------------------

/*

INODES:

 two types of inode metadata:
  hard - uid/gid, mode
  soft - m/c/atime, size

 correspondingly, two types of locks:
  sync -  soft metadata.. no reads/writes can proceed.  (eg no stat)
  lock -  hard(+soft) metadata.. path traversals stop etc.  (??)


 replication consistency modes:
  hard+soft - hard and soft are defined on all replicas.
              all reads proceed (in absense of sync lock)
              writes require sync lock; possibly fw to auth
   -> normal behavior.

  hard      - hard only, soft is undefined
              reads require a sync
              writes proceed if field updates are monotonic (e.g. size, m/c/atime)
   -> 'softasync'

 types of access by cache users:

   hard   soft
    R      -    read_hard_try       path traversal
    R  <=  R    read_soft_start     stat
    R  <=  W    write_soft_start    touch
    W  =>  W    write_hard_start    chmod

   note on those implications:
     read_soft_start() calls read_hard_try()
     write_soft_start() calls read_hard_try()
     a hard lock implies/subsumes a soft sync  (read_soft_start() returns true if a lock is held)


 relationship with frozen directories:

   read_hard_try - can proceed, because any hard changes require a lock, which requires an active
      authority, which implies things are unfrozen.
   write_hard_start - waits (has to; only auth can initiate)
   read_soft_start  - ???? waits for now.  (FIXME: if !softasync & !syncbyauth)
   write_soft_start - ???? waits for now.  (FIXME: if (softasync & !syncbyauth))

   if sticky is on, an export_dir will drop any sync or lock so that the freeze will 
   proceed (otherwise, deadlock!).  likewise, a sync will not stick if is_freezing().
   


NAMESPACE:

 
*/


/* soft sync locks: mtime, size, etc. 
 */

bool MDCache::read_soft_start(CInode *in, Message *m)
{
  if (!read_hard_try(in, m))
	return false;

  // if frozen: i can't proceed (for now, see above)
  if (in->is_frozen()) {
	dout(7) << "read_soft_start " << *in << " is frozen, waiting" << endl;
	in->add_waiter(CDIR_WAIT_UNFREEZE,
				   new C_MDS_RetryMessage(mds, m));
	return false;
  }


  dout(5) << "read_soft_start " << *in << endl;

  // what soft sync mode?

  if (in->is_softasync()) {
	// softasync: hard consistency only

	if (in->is_auth()) {
	  // i am auth: i need sync
	  if (in->is_syncbyme()) goto yes;
	  if (in->is_lockbyme()) goto yes;   // lock => sync
	  if (!in->is_cached_by_anyone() &&
		  !in->is_open_write()) goto yes;  // i'm alone
	} else {
	  // i am replica: fw to auth
	  int auth = in->authority();
	  dout(5) << "read_soft_start " << *in << " is softasync, fw to auth " << auth << endl;
	  assert(auth != mds->get_nodeid());
	  mds->messenger->send_message(m,
								   MSG_ADDR_MDS(auth), m->get_dest_port(),
								   MDS_PORT_CACHE);
	  return false;	  
	}
  } else {
	// normal: soft+hard consistency

	if (in->is_syncbyauth()) {
	  // wait for sync
	} else {
	  // i'm consistent 
	  goto yes;
	}
  }

  // we need sync
  if (in->is_syncbyauth() && !in->is_softasync()) {
    dout(5) << "read_soft_start " << *in << " is normal+replica+syncbyauth" << endl;
  } else if (in->is_softasync() && in->is_auth()) {
    dout(5) << "read_soft_start " << *in << " is softasync+auth, waiting on sync" << endl;
  } else 
	assert(2+2==5);

  if (!in->can_auth_pin()) {
	dout(5) << "read_soft_start " << *in << " waiting to auth_pin" << endl;
	in->add_waiter(CINODE_WAIT_AUTHPINNABLE,
				   new C_MDS_RetryMessage(mds,m));
	return false;
  }

  if (in->is_auth()) {
	// wait for sync
	in->add_waiter(CINODE_WAIT_SYNC,
				   new C_MDS_RetryMessage(mds, m));

	if (!in->is_presync())
	  inode_sync_start(in);
  } else {
	// wait for unsync
	in->add_waiter(CINODE_WAIT_UNSYNC,
				   new C_MDS_RetryMessage(mds, m));

	assert(in->is_syncbyauth());

	if (!in->is_waitonunsync())
	  inode_sync_wait(in);
  }
  
  return false;

 yes:
  mds->balancer->hit_inode(in, MDS_POP_SOFTRD);
  mds->balancer->hit_inode(in, MDS_POP_ANY);
  return true;
}


int MDCache::read_soft_finish(CInode *in)
{
  dout(5) << "read_soft_finish " << *in << endl;   // " soft_sync_count " << in->soft_sync_count << endl;
  return 0;  // do nothing, actually..
}


bool MDCache::write_soft_start(CInode *in, Message *m)
{
  if (!read_hard_try(in, m))
	return false;

  // if frozen: i can't proceed (for now, see above)
  if (in->is_frozen()) {
	dout(7) << "read_soft_start " << *in << " is frozen, waiting" << endl;
	in->add_waiter(CDIR_WAIT_UNFREEZE,
				   new C_MDS_RetryMessage(mds, m));
	return false;
  }

  dout(5) << "write_soft_start " << *in << endl;
  // what soft sync mode?

  if (in->is_softasync()) {
	// softasync: hard consistency only

	if (in->is_syncbyauth()) {
	  // wait for sync release
	} else {
	  // i'm inconsistent; write away!
	  goto yes;
	}

  } else {
	// normal: soft+hard consistency
	
	if (in->is_auth()) {
	  // i am auth: i need sync
	  if (in->is_syncbyme()) goto yes;
	  if (in->is_lockbyme()) goto yes;   // lock => sync
	  if (!in->is_cached_by_anyone() &&
		  !in->is_open_write()) goto yes;  // i'm alone
	} else {
	  // i am replica: fw to auth
	  int auth = in->authority();
	  dout(5) << "write_soft_start " << *in << " is !softasync, fw to auth " << auth << endl;
	  assert(auth != mds->get_nodeid());
	  mds->messenger->send_message(m,
								   MSG_ADDR_MDS(auth), m->get_dest_port(),
								   MDS_PORT_CACHE);
	  return false;	  
	}
  }

  // we need sync
  if (in->is_syncbyauth() && in->is_softasync() && !in->is_auth()) {
    dout(5) << "write_soft_start " << *in << " is softasync+replica+syncbyauth" << endl;
  } else if (!in->is_softasync() && in->is_auth()) {
    dout(5) << "write_soft_start " << *in << " is normal+auth, waiting on sync" << endl;
  } else 
	assert(2+2==5);

  if (!in->can_auth_pin()) {
	dout(5) << "write_soft_start " << *in << " waiting to auth_pin" << endl;
	in->add_waiter(CINODE_WAIT_AUTHPINNABLE,
				   new C_MDS_RetryMessage(mds,m));
	return false;
  }

  if (in->is_auth()) {
	// wait for sync
	in->add_waiter(CINODE_WAIT_SYNC, 
				   new C_MDS_RetryMessage(mds, m));

	if (!in->is_presync())
	  inode_sync_start(in);
  } else {
	// wait for unsync
	in->add_waiter(CINODE_WAIT_UNSYNC, 
				   new C_MDS_RetryMessage(mds, m));

	assert(in->is_syncbyauth());
	assert(in->is_softasync());
	
	if (!in->is_waitonunsync())
	  inode_sync_wait(in);
  }
  
  return false;

 yes:
  mds->balancer->hit_inode(in, MDS_POP_SOFTWR);
  mds->balancer->hit_inode(in, MDS_POP_ANY);
  return true;
}


int MDCache::write_soft_finish(CInode *in)
{
  dout(5) << "write_soft_finish " << *in << endl;  //" soft_sync_count " << in->soft_sync_count << endl;
  return 0;  // do nothing, actually..
}


// sync interface

void MDCache::inode_sync_wait(CInode *in)
{
  assert(!in->is_auth());
  
  int auth = in->authority();
  dout(5) << "inode_sync_wait on " << *in << ", auth " << auth << endl;
  
  assert(in->is_syncbyauth());
  assert(!in->is_waitonunsync());
  
  in->dist_state |= CINODE_DIST_WAITONUNSYNC;
  in->get(CINODE_PIN_WAITONUNSYNC);
  
  if ((in->is_softasync() && g_conf.mdcache_sticky_sync_softasync) ||
	  (!in->is_softasync() && g_conf.mdcache_sticky_sync_normal)) {
	// actually recall; if !sticky, auth will immediately release.
	dout(5) << "inode_sync_wait on " << *in << " sticky, recalling from auth" << endl;
	mds->messenger->send_message(new MInodeSyncRecall(in->inode.ino),
								 MSG_ADDR_MDS(auth), MDS_PORT_CACHE,
								 MDS_PORT_CACHE);
  }
}


void MDCache::inode_sync_start(CInode *in)
{
  // wait for all replicas
  dout(5) << "inode_sync_start on " << *in << ", waiting for " << in->cached_by << " " << in->get_open_write()<< endl;

  assert(in->is_auth());
  assert(!in->is_presync());
  assert(!in->is_sync());

  in->sync_waiting_for_ack.clear();
  in->dist_state |= CINODE_DIST_PRESYNC;
  in->get(CINODE_PIN_PRESYNC);
  in->auth_pin();
  
  in->sync_replicawantback = false;

  // send messages
  for (set<int>::iterator it = in->cached_by_begin(); 
	   it != in->cached_by_end(); 
	   it++) {
	in->sync_waiting_for_ack.insert(MSG_ADDR_MDS(*it));
	mds->messenger->send_message(new MInodeSyncStart(in->inode.ino, mds->get_nodeid()),
								 MSG_ADDR_MDS(*it), MDS_PORT_CACHE,
								 MDS_PORT_CACHE);
  }

  // sync clients
  for (multiset<int>::iterator it = in->get_open_write().begin();
	   it != in->get_open_write().end();
	   it++) {
	in->sync_waiting_for_ack.insert(MSG_ADDR_CLIENT(*it));
	mds->messenger->send_message(new MInodeSyncStart(in->ino(), mds->get_nodeid()),
								 MSG_ADDR_CLIENT(*it), 0,
								 MDS_PORT_CACHE);
  }

}

void MDCache::inode_sync_release(CInode *in)
{
  dout(5) << "inode_sync_release on " << *in << ", messages to " << in->get_cached_by() << " " << in->get_open_write() << endl;
  
  assert(in->is_syncbyme());
  assert(in->is_auth());

  in->dist_state &= ~CINODE_DIST_SYNCBYME;

  // release replicas
  for (set<int>::iterator it = in->cached_by_begin(); 
	   it != in->cached_by_end(); 
	   it++) {
	mds->messenger->send_message(new MInodeSyncRelease(in),
								 MSG_ADDR_MDS(*it), MDS_PORT_CACHE,
								 MDS_PORT_CACHE);
  }
  
  // release writers
  for (multiset<int>::iterator it = in->get_open_write().begin();
	   it != in->get_open_write().end();
	   it++) {
	mds->messenger->send_message(new MInodeSyncRelease(in),
								 MSG_ADDR_CLIENT(*it), 0,
								 MDS_PORT_CACHE);
  }

  in->auth_unpin();
}


/*
void MDCache::update_replica_auth(CInode *in, int realauth)
{
  assert(0); // this is all hashing crap, fixme

  int myauth = in->authority();
  if (myauth != realauth) {
	dout(7) << "update_replica_auth " << *in << " real auth is " << realauth << " not " << myauth << ", fiddling dir_auth" << endl;
	CDir *dir = in->get_parent_dir();
	assert(!dir->is_hashed());
	assert(!dir->is_auth());
	
	// let's just change ownership of this dir.
	assert(!dir->inode->is_auth());    // i would already have correct info if dir inode were mine
	dir->dir_auth = realauth;
	assert(in->authority() == realauth);    // double check our work.
	assert(!dir->is_auth());                                  // make sure i'm still not auth for the dir.
  }
}
*/

int MDCache::ino_proxy_auth(inodeno_t ino, int frommds) 
{
  // check proxy sets for this ino
  for (map<CDir*, set<inodeno_t> >::iterator wit = export_proxy_inos.begin();
	   wit != export_proxy_inos.end();
	   wit++) {
	CDir *dir = wit->first;
	// does this map apply to this node?
	if (export_notify_ack_waiting[dir].count(frommds) == 0) continue;

	// is this ino in the set?
	if (export_proxy_inos[dir].count(ino)) {
	  int dirauth = dir->authority();
	  assert(dirauth >= 0);
	  return dirauth;
	}
  }
  return -1;   // no proxy
}




// messages
void MDCache::handle_inode_sync_start(MInodeSyncStart *m)
{
  // assume asker == authority for now.
  
  // authority is requesting a lock
  CInode *in = get_inode(m->get_ino());
  if (!in) {
	// don't have it anymore!
	dout(7) << "handle_sync_start " << m->get_ino() << ": don't have it anymore, nak" << endl;
	mds->messenger->send_message(new MInodeSyncAck(m->get_ino(), false),
								 MSG_ADDR_MDS(m->get_asker()), MDS_PORT_CACHE,
								 MDS_PORT_CACHE);
	delete m; // done
	return;
  }
  
  // we shouldn't be authoritative...
  assert(!in->is_auth());
  
  // sanity check: make sure we know who _is_ authoritative! 
  assert(m->get_asker() == in->authority());

  // lock it
  in->dist_state |= CINODE_DIST_SYNCBYAUTH;

  // open for write by clients?
  if (in->is_open_write()) {
	dout(7) << "handle_sync_start " << *in << " syncing write clients " << in->get_open_write() << endl;
	
	// sync clients
	in->sync_waiting_for_ack.clear();
	for (multiset<int>::iterator it = in->get_open_write().begin();
		 it != in->get_open_write().end();
		 it++) {
	  in->sync_waiting_for_ack.insert(MSG_ADDR_CLIENT(*it));
	  mds->messenger->send_message(new MInodeSyncStart(in->ino(), mds->get_nodeid()),
								   MSG_ADDR_CLIENT(*it), 0,
								   MDS_PORT_CACHE);
	}

	in->pending_sync_request = m;	
  } else {
	// no writers, ack.
	dout(7) << "handle_sync_start " << *in << ", sending ack" << endl;
  
	inode_sync_ack(in, m);
  }
}

void MDCache::inode_sync_ack(CInode *in, MInodeSyncStart *m, bool wantback)
{
  dout(7) << "sending inode_sync_ack " << *in << endl;
    
  // send ack
  mds->messenger->send_message(new MInodeSyncAck(in->ino(), true, wantback),
							   MSG_ADDR_MDS(m->get_asker()), MDS_PORT_CACHE,
							   MDS_PORT_CACHE);

  delete m;
}

void MDCache::handle_inode_sync_ack(MInodeSyncAck *m)
{
  CInode *in = get_inode(m->get_ino());
  assert(in);

  dout(7) << "handle_sync_ack " << *in << " from " << m->get_source() << endl;

  if (in->is_auth()) {
	assert(in->is_presync());
  } else {
	assert(in->is_syncbyauth());
	assert(in->pending_sync_request);
  }

  // remove it from waiting list
  in->sync_waiting_for_ack.erase(m->get_source());
  
  if (MSG_ADDR_ISCLIENT(m->get_source()) && !m->did_have()) {
	// erase from cached_by too!
	in->cached_by_remove(m->get_source());
  }

  if (m->replica_wantsback())
	in->sync_replicawantback = true;

  if (in->sync_waiting_for_ack.size()) {

	// more coming
	dout(7) << "handle_sync_ack " << *in << " from " << m->get_source() << ", still waiting for " << in->sync_waiting_for_ack << endl;
	
  } else {
	
	// yay!
	dout(7) << "handle_sync_ack " << *in << " from " << m->get_source() << ", last one" << endl;

	if (!in->is_auth()) {
	  // replica, sync ack back to auth
	  assert(in->pending_sync_request);
	  inode_sync_ack(in, in->pending_sync_request, true);
	  in->pending_sync_request = 0;
	  delete m;
	  return;
	}

	in->dist_state &= ~CINODE_DIST_PRESYNC;
	in->dist_state |= CINODE_DIST_SYNCBYME;
	in->put(CINODE_PIN_PRESYNC);

	// do waiters!
	in->finish_waiting(CINODE_WAIT_SYNC);


	// release sync right away?
	if (in->is_syncbyme()) {
	  if (in->is_freezing()) {
		dout(7) << "handle_sync_ack freezing " << *in << ", dropping sync immediately" << endl;
		inode_sync_release(in);
	  } 
	  else if (in->sync_replicawantback) {
		dout(7) << "handle_sync_ack replica wantback, releasing sync immediately" << endl;
		inode_sync_release(in);
	  }
	  else if ((in->is_softasync() && !g_conf.mdcache_sticky_sync_softasync) ||
			   (!in->is_softasync() && !g_conf.mdcache_sticky_sync_normal)) {
		dout(7) << "handle_sync_ack !sticky, releasing sync immediately" << endl;
		inode_sync_release(in);
	  } 
	  else {
		dout(7) << "handle_sync_ack sticky sync is on, keeping sync for now" << endl;
	  }
	} else {
	  dout(7) << "handle_sync_ack don't have sync anymore, something must have just released it?" << endl;
	}
  }

  delete m; // done
}


void MDCache::handle_inode_sync_release(MInodeSyncRelease *m)
{
  CInode *in = get_inode(m->get_ino());

  if (!in) {
	dout(7) << "handle_sync_release " << m->get_ino() << ", don't have it, dropping" << endl;
	delete m;  // done
	return;
  }
  
  if (!in->is_syncbyauth()) {
	dout(7) << "handle_sync_release " << m->get_ino() << ", not flagged as sync, dropping" << endl;
	assert(0);
	delete m;  // done
	return;
  }
  
  dout(7) << "handle_sync_release " << *in << endl;
  assert(!in->is_auth());
  
  // release state
  in->dist_state &= ~CINODE_DIST_SYNCBYAUTH;

  // waiters?
  if (in->is_waitonunsync()) {
	in->put(CINODE_PIN_WAITONUNSYNC);
	in->dist_state &= ~CINODE_DIST_WAITONUNSYNC;

	// finish
	in->finish_waiting(CINODE_WAIT_UNSYNC);
  }

  // client readers?
  if (in->is_open_write()) {
	dout(7) << "handle_sync_release releasing clients " << in->get_open_write() << endl;
	for (multiset<int>::iterator it = in->get_open_write().begin();
		 it != in->get_open_write().end();
		 it++) {
	  mds->messenger->send_message(new MInodeSyncRelease(in),
								   MSG_ADDR_CLIENT(*it), 0,
								   MDS_PORT_CACHE);
	}
  }

  
  // done
  delete m;
}


void MDCache::handle_inode_sync_recall(MInodeSyncRecall *m)
{
  CInode *in = get_inode(m->get_ino());

  if (!in) {
	dout(7) << "handle_sync_recall " << m->get_ino() << ", don't have it, dropping" << endl;
	delete m;  // done
	return;
  }

  if(!in->is_auth()) {
	// we must have exported, and thus released the sync already.  this person is just behind.
	dout(7) << "handle_sync_recall " << m->get_ino() << ", not auth, dropping" << endl;
	delete m;
	return;
  }
  
  if (in->is_syncbyme()) {
	dout(7) << "handle_sync_recall " << *in << ", releasing" << endl;
	inode_sync_release(in);
  }
  else if (in->is_presync()) {
	dout(7) << "handle_sync_recall " << *in << " is presync, flagging" << endl;
	in->sync_replicawantback = true;
  }
  else {
	dout(7) << "handle_sync_recall " << m->get_ino() << ", not flagged as sync or presync, dropping" << endl;
  }
  
  // done
  delete m;
}




/* hard locks: owner, mode 
 */

bool MDCache::read_hard_try(CInode *in,
							Message *m)
{
  //dout(5) << "read_hard_try " << *in << endl;
  
  if (in->is_auth()) {
	// auth
	goto yes;      // fine
  } else {
	// replica
	if (in->is_lockbyauth()) {
	  // locked by auth; wait!
	  dout(7) << "read_hard_try waiting on " << *in << endl;
	  in->add_waiter(CINODE_WAIT_UNLOCK, new C_MDS_RetryMessage(mds, m));
	  if (!in->is_waitonunlock())
		inode_lock_wait(in);
	  return false;
	} else {
	  // not locked.
	  goto yes;
	}
  }

 yes:
  mds->balancer->hit_inode(in, MDS_POP_HARDRD);
  mds->balancer->hit_inode(in, MDS_POP_ANY);
  return true;
}


bool MDCache::write_hard_start(CInode *in, 
							   Message *m)
{
  // if frozen: i can't proceed; only auth can initiate lock
  if (in->is_frozen()) {
	dout(7) << "write_hard_start " << *in << " is frozen, waiting" << endl;
	in->add_waiter(CDIR_WAIT_UNFREEZE,
				   new C_MDS_RetryMessage(mds, m));
	return false;
  }

  // NOTE: if freezing, and locked, we must proceed, to avoid deadlock (where
  // the freeze is waiting for our lock to be released)


  if (in->is_auth()) {
	// auth
	if (in->is_lockbyme()) goto success;
	if (!in->is_cached_by_anyone()) goto success;
	
	// need lock
	if (!in->can_auth_pin()) {
	  dout(5) << "write_hard_start " << *in << " waiting to auth_pin" << endl;
	  in->add_waiter(CINODE_WAIT_AUTHPINNABLE, new C_MDS_RetryMessage(mds, m));
	  return false;
	}
	
	in->add_waiter(CINODE_WAIT_LOCK, new C_MDS_RetryMessage(mds, m));
	
	if (!in->is_prelock())
	  inode_lock_start(in);
	
	return false;
  } else {
	// replica
	// fw to auth
	int auth = in->authority();
	dout(5) << "write_hard_start " << *in << " on replica, fw to auth " << auth << endl;
	assert(auth != mds->get_nodeid());
	mds->messenger->send_message(m,
								 MSG_ADDR_MDS(auth), m->get_dest_port(),
								 MDS_PORT_CACHE);
	return false;
  }

 success:
  in->lock_active_count++;
  dout(5) << "write_hard_start " << *in << " count now " << in->lock_active_count << endl;
  assert(in->lock_active_count > 0);

  mds->balancer->hit_inode(in, MDS_POP_HARDWR);
  mds->balancer->hit_inode(in, MDS_POP_ANY);
  return true;
}

void MDCache::write_hard_finish(CInode *in)
{
  in->lock_active_count--;
  dout(5) << "write_hard_finish " << *in << " count now " << in->lock_active_count << endl;
  assert(in->lock_active_count >= 0);

  // release lock?
  if (in->lock_active_count == 0 &&
	  in->is_lockbyme() &&
	  !g_conf.mdcache_sticky_lock) {
	dout(7) << "write_hard_finish " << *in << " !sticky, releasing lock immediately" << endl;
	inode_lock_release(in);
  }
}


void MDCache::inode_lock_start(CInode *in)
{
  dout(5) << "lock_start on " << *in << ", waiting for " << in->cached_by << endl;

  assert(in->is_auth());
  assert(!in->is_prelock());
  assert(!in->is_lockbyme());
  assert(!in->is_lockbyauth());

  in->lock_waiting_for_ack = in->cached_by;
  in->dist_state |= CINODE_DIST_PRELOCK;
  in->get(CINODE_PIN_PRELOCK);
  in->auth_pin();

  // send messages
  for (set<int>::iterator it = in->cached_by_begin(); 
	   it != in->cached_by_end(); 
	   it++) {
	mds->messenger->send_message(new MInodeLockStart(in->inode.ino, mds->get_nodeid()),
								 MSG_ADDR_MDS(*it), MDS_PORT_CACHE,
								 MDS_PORT_CACHE);
  }
}


void MDCache::inode_lock_release(CInode *in)
{
  dout(5) << "lock_release on " << *in << ", messages to " << in->get_cached_by() << endl;
  
  assert(in->is_lockbyme());
  assert(in->is_auth());

  in->dist_state &= ~CINODE_DIST_LOCKBYME;

  for (set<int>::iterator it = in->cached_by_begin(); 
	   it != in->cached_by_end(); 
	   it++) {
	mds->messenger->send_message(new MInodeLockRelease(in),
								 MSG_ADDR_MDS(*it), MDS_PORT_CACHE,
								 MDS_PORT_CACHE);
  }

  in->auth_unpin();
}

void MDCache::inode_lock_wait(CInode *in)
{
  dout(5) << "lock_wait on " << *in << endl;
  assert(!in->is_auth());
  assert(in->is_lockbyauth());
  
  in->dist_state |= CINODE_DIST_WAITONUNLOCK;
  in->get(CINODE_PIN_WAITONUNLOCK);
}


void MDCache::handle_inode_lock_start(MInodeLockStart *m)
{
  // authority is requesting a lock
  CInode *in = get_inode(m->get_ino());
  if (!in) {
	// don't have it anymore!
	dout(7) << "handle_lock_start " << m->get_ino() << ": don't have it anymore, nak" << endl;
	mds->messenger->send_message(new MInodeLockAck(m->get_ino(), false),
								 MSG_ADDR_MDS(m->get_asker()), MDS_PORT_CACHE,
								 MDS_PORT_CACHE);
	delete m; // done
	return;
  }
  
  // we shouldn't be authoritative...
  assert(!in->is_auth());
  
  dout(7) << "handle_lock_start " << *in << ", sending ack" << endl;
  
  // lock it
  in->dist_state |= CINODE_DIST_LOCKBYAUTH;

  // sanity check: make sure we know who _is_ authoritative! 
  assert(m->get_asker() == in->authority());
  
  // send ack
  mds->messenger->send_message(new MInodeLockAck(in->ino()),
							   MSG_ADDR_MDS(m->get_asker()), MDS_PORT_CACHE,
							   MDS_PORT_CACHE);

  delete m;  // done
}


void MDCache::handle_inode_lock_ack(MInodeLockAck *m)
{
  CInode *in = get_inode(m->get_ino());
  int from = m->get_source();
  dout(7) << "handle_lock_ack from " << from << " on " << *in << endl;

  assert(in);
  assert(in->is_auth());
  assert(in->dist_state & CINODE_DIST_PRELOCK);

  // remove it from waiting list
  in->lock_waiting_for_ack.erase(from);
  
  if (!m->did_have()) {
	// erase from cached_by too!
	in->cached_by_remove(from);
  }

  if (in->lock_waiting_for_ack.size()) {

	// more coming
	dout(7) << "handle_lock_ack " << *in << " from " << from << ", still waiting for " << in->lock_waiting_for_ack << endl;
	
  } else {
	
	// yay!
	dout(7) << "handle_lock_ack " << *in << " from " << from << ", last one" << endl;

	in->dist_state &= ~CINODE_DIST_PRELOCK;
	in->dist_state |= CINODE_DIST_LOCKBYME;
	in->put(CINODE_PIN_PRELOCK);

	// do waiters!
	in->finish_waiting(CINODE_WAIT_LOCK);
  }

  delete m; // done
}


void MDCache::handle_inode_lock_release(MInodeLockRelease *m)
{
  CInode *in = get_inode(m->get_ino());

  if (!in) {
	dout(7) << "handle_lock_release " << m->get_ino() << ", don't have it, dropping" << endl;
	delete m;  // done
	return;
  }
  
  if (!in->is_lockbyauth()) {
	dout(7) << "handle_lock_release " << m->get_ino() << ", not flagged as locked, dropping" << endl;
	assert(0);   // i should have it, locked, or not have it at all!
	delete m;  // done
	return;
  }
  
  dout(7) << "handle_lock_release " << *in << endl;
  assert(!in->is_auth());
  
  // release state
  in->dist_state &= ~CINODE_DIST_LOCKBYAUTH;

  // waiters?
  if (in->is_waitonunlock()) {
	in->put(CINODE_PIN_WAITONUNLOCK);
	in->dist_state &= ~CINODE_DIST_WAITONUNLOCK;
	
	// finish
	in->finish_waiting(CINODE_WAIT_UNLOCK);
  }
  
  // done
  delete m;
}






// DIR SYNC

/*

 dir sync

 - this are used when a directory is HASHED only.  namely,
   - to stat the dir inode we need an accurate directory size  (????)
   - for a readdir 

*/

void MDCache::dir_sync_start(CDir *dir)
{
  // wait for all replicas
  dout(5) << "sync_start on " << *dir << endl;

  assert(dir->is_hashed());
  assert(dir->is_auth());
  assert(!dir->is_presync());
  assert(!dir->is_sync());

  dir->sync_waiting_for_ack = mds->get_cluster()->get_mds_set();
  dir->state_set(CDIR_STATE_PRESYNC);
  dir->auth_pin();
  
  //dir->sync_replicawantback = false;

  // send messages
  for (set<int>::iterator it = dir->sync_waiting_for_ack.begin();
	   it != dir->sync_waiting_for_ack.end();
	   it++) {
	mds->messenger->send_message(new MDirSyncStart(dir->ino(), mds->get_nodeid()),
								 MSG_ADDR_MDS(*it), MDS_PORT_CACHE,
								 MDS_PORT_CACHE);
  }
}


void MDCache::dir_sync_release(CDir *dir)
{


}

void MDCache::dir_sync_wait(CDir *dir)
{

}


void handle_dir_sync_start(MDirSyncStart *m)
{
}










// IMPORT/EXPORT

class C_MDS_ExportFreeze : public Context {
  MDS *mds;
  CDir *ex;   // dir i'm exporting
  int dest;

public:
  C_MDS_ExportFreeze(MDS *mds, CDir *ex, int dest) {
	this->mds = mds;
	this->ex = ex;
	this->dest = dest;
  }
  virtual void finish(int r) {
	mds->mdcache->export_dir_frozen(ex, dest);
  }
};


class C_MDS_ExportGo : public Context {
  MDS *mds;
  CDir *ex;   // dir i'm exporting
  int dest;
  double pop;

public:
  C_MDS_ExportGo(MDS *mds, CDir *ex, int dest, double pop) {
	this->mds = mds;
	this->ex = ex;
	this->dest = dest;
	this->pop = pop;
  }
  virtual void finish(int r) {
	mds->mdcache->export_dir_go(ex, dest, pop);
  }
};


class C_MDS_ExportFinish : public Context {
  MDS *mds;
  CDir *ex;   // dir i'm exporting

public:
  // contexts for waiting operations on the affected subtree
  list<Context*> will_redelegate;
  list<Context*> will_fail;

  C_MDS_ExportFinish(MDS *mds, CDir *ex, int dest) {
	this->mds = mds;
	this->ex = ex;
  }

  // suck up and categorize waitlists 
  void assim_waitlist(list<Context*>& ls) {
	for (list<Context*>::iterator it = ls.begin();
		 it != ls.end();
		 it++) {
	  dout(7) << "assim_waitlist context " << *it << endl;
	  if ((*it)->can_redelegate()) 
		will_redelegate.push_back(*it);
	  else
		will_fail.push_back(*it);
	}
	ls.clear();
  }
  void assim_waitlist(hash_map< string, list<Context*> >& cmap) {
	for (hash_map< string, list<Context*> >::iterator hit = cmap.begin();
		 hit != cmap.end();
		 hit++) {
	  for (list<Context*>::iterator lit = hit->second.begin(); lit != hit->second.end(); lit++) {
		dout(7) << "assim_waitlist context " << *lit << endl;
		if ((*lit)->can_redelegate()) 
		  will_redelegate.push_back(*lit);
		else
		  will_fail.push_back(*lit);
	  }
	}
	cmap.clear();
  }


  virtual void finish(int r) {
	if (r >= 0) { 
	  // redelegate
	  list<Context*>::iterator it;
	  for (it = will_redelegate.begin(); it != will_redelegate.end(); it++) {
		(*it)->redelegate(mds, ex->authority());
		delete *it;  // delete context
	  }

	  // fail
	  // this happens with: 
	  // - commit_dir
	  // - ?
	  for (it = will_fail.begin(); it != will_fail.end(); it++) {
		Context *c = *it;
		dout(7) << "failing context " << c << endl;
		//assert(false);
		c->finish(-1);  // fail
		delete c;   // delete context
	  }	  
	} else {
	  assert(false); // now what?
	}
  }
};


void MDCache::export_dir(CDir *dir,
						 int dest)
{
  assert(dest != mds->get_nodeid());

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

  // send ExportDirDiscover (ask target)
  dout(7) << "export_dir " << *dir << " to " << dest << ", sending ExportDirDiscover" << endl;
  mds->messenger->send_message(new MExportDirDiscover(dir->inode),
							   dest, MDS_PORT_CACHE, MDS_PORT_CACHE);
  dir->auth_pin();   // pin dir, to hang up our freeze
  mds->logger->inc("ex");
  
  // take away popularity (and pass it on to the context, MExportDir request later)
  double pop = dir->inode->popularity[0].get();  // FIXME rest of vector?
  CInode *t = dir->inode;
  while (t) {
	t->popularity[0].adjust(-pop);
	if (t->parent)
	  t = t->parent->dir->inode;
	else 
	  break;
  }

  // freeze the subtree
  dir->freeze_tree(new C_MDS_ExportFreeze(mds, dir, dest));

  // get waiter ready to do actual export
  dir->add_waiter(CDIR_WAIT_EXPORTPREPACK,
				  new C_MDS_ExportGo(mds, dir, dest, pop));
  
  // drop any sync or lock if sticky
  if (g_conf.mdcache_sticky_sync_normal ||
	  g_conf.mdcache_sticky_sync_softasync)
	export_dir_dropsync(dir);

  // NOTE: we don't need to worry about hard locks; those aren't sticky (yet?).
}


void MDCache::export_dir_dropsync(CDir *dir)
{
  dout(7) << "export_dir_dropsync in " << *dir << endl;

  CDir_map_t::iterator it;
  for (it = dir->begin(); it != dir->end(); it++) {
	CInode *in = it->second->inode;

	if (in->is_auth() && in->is_syncbyme()) {
	  dout(7) << "about to export: dropping sticky(?) sync on " << *in << endl;
	  inode_sync_release(in);
	}

	if (in->is_dir() &&
		in->dir &&                        // open
		!in->dir->is_export() &&          // mine
		in->dir->nested_auth_pins > 0)    // might be sync
	  export_dir_dropsync(in->dir);
  }
}



void MDCache::handle_export_dir_discover_ack(MExportDirDiscoverAck *m)
{
  CInode *in = get_inode(m->get_ino());
  assert(in);
  CDir *dir = in->dir;
  assert(dir);

  dout(7) << "export_dir_discover_ack " << *dir << ", releasing auth_pin" << endl;
  
  dir->auth_unpin();   // unpin to allow freeze to complete

  // done
  delete m;
}


void MDCache::export_dir_frozen(CDir *dir,
								int dest)
{
  // subtree is now frozen!
  dout(7) << "export_dir " << *dir << " to " << dest << ", frozen+discover_ack" << endl;

  MExportDirPrep *prep = new MExportDirPrep(dir->inode);

  // include spanning tree for all nested exports.
  // these need to be on the destination _before_ the final export so that
  // dir_auth updates on any nexted exports are properly absorbed.
  
  set<inodeno_t> inodes_added;
  
  // include base dir
  prep->add_dir( new CDirDiscover(dir, dir->open_by_add(dest)) );
  
  // also include traces to all nested exports.
  for (pair< multimap<CDir*,CDir*>::iterator, multimap<CDir*,CDir*>::iterator > p = 
		 nested_exports.equal_range( dir );
	   p.first != p.second;
	   p.first++) {
	CDir *exp = (*p.first).second;
    
	dout(7) << " including nested export " << *exp << " in prep" << endl;

	prep->add_export( exp->ino() );

    // trace to dir
    CDir *cur;
    while (cur != dir) {
      // don't repeat ourselves
      if (inodes_added.count(cur->ino())) break;   // did already!
      inodes_added.insert(cur->ino());
      
	  CDir *parent_dir = cur->get_parent_dir();

      // inode?
      assert(cur->inode->is_auth());
      prep->add_inode( parent_dir->ino(),
					   cur->inode->parent->name,
					   new CInodeDiscover(cur->inode, cur->inode->cached_by_add(dest)) );
      dout(10) << "  added " << *cur->inode << endl;
      
      // include dir? note: this'll include everything except the nested exports
      if (cur->is_auth()) {
        prep->add_dir( new CDirDiscover(cur, cur->open_by_add(dest)) );  // yay!
        dout(10) << "  added " << *cur << endl;
      }
      
      cur = parent_dir;      
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
  
  dir->finish_waiting(CDIR_WAIT_EXPORTPREPACK);

  // done
  delete m;
}


void MDCache::export_dir_go(CDir *dir,
							int dest,
							double pop)
{  
  dout(7) << "export_dir_go " << *dir << " to " << dest << endl;

  show_imports();

  // update imports/exports
  CDir *containing_import = get_containing_import(dir);
  if (containing_import == dir) {
	dout(7) << " i'm rexporting a previous import" << endl;
	imports.erase(dir);
	dir->state_clear(CDIR_STATE_IMPORT);
	dir->put(CDIR_PIN_IMPORT);                  // unpin, no longer an import
	
	// discard nested exports (that we're handing off
	pair<multimap<CDir*,CDir*>::iterator, multimap<CDir*,CDir*>::iterator> p =
	  nested_exports.equal_range(dir);
	while (p.first != p.second) {
	  CDir *nested = (*p.first).second;

	  // nested beneath our new export *in; remove!
	  dout(7) << " export " << *nested << " was nested beneath us; removing from export list(s)" << endl;
	  assert(exports.count(nested) == 1);
	  //exports.erase(nested);  _walk does this
	  nested_exports.erase(p.first++);   // note this increments before call to erase
	}
	
  } else {
	dout(7) << " i'm a subdir nested under import " << *containing_import << endl;
	exports.insert(dir);
	nested_exports.insert(pair<CDir*,CDir*>(containing_import, dir));
	
	dir->get(CDIR_PIN_EXPORT);                  // i must keep it pinned
	
	// discard nested exports (that we're handing off)
	pair<multimap<CDir*,CDir*>::iterator, multimap<CDir*,CDir*>::iterator> p =
	  nested_exports.equal_range(containing_import);
	while (p.first != p.second) {
	  CDir *nested = (*p.first).second;
	  multimap<CDir*,CDir*>::iterator prev = p.first;
	  p.first++;
	  
	  // container of parent; otherwise we get ourselves.
	  CDir *containing_export = get_containing_export(nested->get_parent_dir());
	  if (!containing_export) continue;
	  if (nested == dir) continue;  // ignore myself

	  if (containing_export == dir) {
		// nested beneath our new export *in; remove!
		dout(7) << " export " << *nested << " was nested beneath us; removing from nested_exports" << endl;
		// exports.erase(nested); _walk does this
		nested_exports.erase(prev);  // note this increments before call to erase
	  } else {
		dout(12) << " export " << *nested << " is under other export " << *containing_export << ", which is unrelated" << endl;
		assert(get_containing_import(containing_export) != containing_import);
	  }
	}
  }

  // note new authority (locally)
  if (dir->inode->authority() == dest)
	dir->dir_auth = CDIR_AUTH_PARENT;
  else
	dir->dir_auth = dest;

  // build export message
  MExportDir *req = new MExportDir(dir->inode, pop);  // include pop
  
  // fill with relevant cache data
  C_MDS_ExportFinish *fin = new C_MDS_ExportFinish(mds, dir, dest);

  export_dir_walk( req, 
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

  // make list of nodes i expect an export_dir_notify_ack from
  // (all w/ this dir open, but me!)
  assert(export_notify_ack_waiting[dir].empty());
  for (set<int>::iterator it = dir->open_by.begin();
	   it != dir->open_by.end();
	   it++) {
	if (*it == mds->get_nodeid()) continue;
	export_notify_ack_waiting[dir].insert( *it );
  }
}



void MDCache::export_dir_walk(MExportDir *req,
							  C_MDS_ExportFinish *fin,
							  CDir *basedir,
							  CDir *dir,
							  int newauth)
{
  dout(7) << "export_dir_walk " << *dir << " " << dir->nitems << " items" << endl;
  
  // dir 
  crope dir_rope;
  
  CDirExport dstate(dir);
  dir_rope.append( dstate._rope() );
  
  // mark
  assert(dir->is_auth());
  dir->state_clear(CDIR_STATE_AUTH);
  
  // discard most dir state
  dir->state &= CDIR_MASK_STATE_EXPORT_KEPT;  // i only retain a few things.
  
  // proxy
  dir->state_set(CDIR_STATE_PROXY);
  dir->get(CDIR_PIN_PROXY);

  // suck up all waiters
  list<Context*> waiting;
  dir->take_waiting(CINODE_WAIT_ANY, waiting);    // FIXME ?? actually this is okay?
  fin->assim_waitlist(waiting);
  
  // inodes
  list<CDir*> subdirs;

  CDir_map_t::iterator it;
  for (it = dir->begin(); it != dir->end(); it++) {
	CInode *in = it->second->inode;
	
	in->version++;  // so log entries are ignored, etc.
	
	// dentry
	dir_rope.append( it->first.c_str(), it->first.length()+1 );
	
	// add inode
	CInodeExport istate( in );
	dir_rope.append( istate._rope() );
	
	if (in->is_dir()) { 

	  // recurse?
	  if (in->dir) {
		if (in->dir->is_auth()) {
		  // nested subdir
		  assert(in->dir->dir_auth == CDIR_AUTH_PARENT);
		  subdirs.push_back(in->dir);  // it's ours, recurse.
		  
		} else {
		  // nested export
		  assert(in->dir->dir_auth >= 0);
		  dout(7) << " encountered nested export " << *in->dir << " dir_auth " << in->dir->dir_auth << "; removing from exports" << endl;
		  assert(exports.count(in->dir) == 1); 
		  exports.erase(in->dir);                    // discard nested export   (nested_exports updated above)
		  in->dir->put(CDIR_PIN_EXPORT);
		  
		  // simplify dir_auth?
		  if (in->dir->dir_auth == newauth)
			in->dir->dir_auth = -1;
		}
	  } 
	}
	
	// we're export this inode; fix inode state
	dout(7) << "export_dir_walk exporting " << *in << endl;

	if (in->is_dirty()) in->mark_clean();

	// clear/unpin cached_by (we're no longer the authority)
	in->cached_by_clear();
	  
	// mark auth
	assert(in->is_auth());
	in->set_auth(false);

	in->replica_nonce = CINODE_EXPORT_NONCE;

	// add to proxy
	export_proxy_inos[basedir].insert(in->ino());
	in->state_set(CINODE_STATE_PROXY);
	in->get(CINODE_PIN_PROXY);
	  
	// *** other state too?
	
	// waiters
	list<Context*> waiters;
	dir->take_waiting(CDIR_WAIT_ANY, waiters);
	fin->assim_waitlist(waiters);
  }

  req->add_dir( dir_rope );
  
  // subdirs
  for (list<CDir*>::iterator it = subdirs.begin(); it != subdirs.end(); it++)
	export_dir_walk(req, fin, basedir, *it, newauth);
}


/*
 * i should get an export_dir_notify_ack from every mds that had me open, including the new auth (an ack)
 */
void MDCache::handle_export_dir_notify_ack(MExportDirNotifyAck *m)
{
  CInode *idir = get_inode(m->get_ino());
  CDir *dir = idir->dir;
  assert(dir);
  assert(dir->is_frozen_tree_root());  // i'm exporting!

  // remove from waiting list
  int from = m->get_source();
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

	// unpin proxies!
	// inodes
	for (set<inodeno_t>::iterator it = export_proxy_inos[dir].begin();
		 it != export_proxy_inos[dir].end();
		 it++) {
	  CInode *in = get_inode(*it);
	  in->put(CINODE_PIN_PROXY);
	  in->state_clear(CINODE_STATE_PROXY);
	}
	export_proxy_inos.erase(dir);

	// dirs
	for (set<inodeno_t>::iterator it = export_proxy_dirinos[dir].begin();
		 it != export_proxy_dirinos[dir].end();
		 it++) {
	  CDir *dir = get_inode(*it)->dir;
	  dir->put(CDIR_PIN_PROXY);
	  dir->state_clear(CDIR_STATE_PROXY);
	}
	export_proxy_dirinos.erase(dir);

	// finish export
	export_dir_finish(dir);
  }
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
  
  show_imports();
}












//  IMPORTS

void MDCache::handle_export_dir_discover(MExportDirDiscover *m)
{
  assert(m->get_source() != mds->get_nodeid());

  // must discover it!
  vector<CInode*> trav;
  filepath fpath(m->get_path());
  int r = path_traverse(fpath, trav, m, MDS_TRAVERSE_DISCOVER);   
  if (r > 0) {
	dout(7) << "handle_export_dir_discover on " << m->get_path() << ", doing discover" << endl;
	return;  // fw or delay
  }
  
  // yay!
  CInode *in = trav[trav.size()-1];
  dout(7) << "handle_export_dir_discover on " << *in << endl;
  assert(in->is_dir());

  // pin inode in the cache (for now)
  in->get(CINODE_PIN_IMPORTING);
  
  // reply
  dout(7) << " sending export_dir_discover_ack on " << *in << endl;
  mds->messenger->send_message(new MExportDirDiscoverAck(in->ino()),
							   m->get_source(), MDS_PORT_CACHE, MDS_PORT_CACHE);
  delete m;
}



void MDCache::handle_export_dir_prep(MExportDirPrep *m)
{
  assert(m->get_source() != mds->get_nodeid());

  CInode *idir = get_inode(m->get_ino());
  assert(idir);

  // assimilate root dir.
  CDir *dir = idir->dir;
  if (dir) {
    dout(7) << "handle_export_dir_prep on " << *dir << " (had dir)" << endl;
  } else {
    assert(!m->did_assim());

    // open dir i'm importing.
    idir->set_dir( new CDir(idir, mds, false) );
    dir = idir->dir;

    m->get_dir(idir->ino())->update_dir(dir);
    
    dout(7) << "handle_export_dir_prep on " << *dir << " (opened dir)" << endl;
  }
  assert(dir->is_auth() == false);
  
  // move pin to dir
  idir->put(CINODE_PIN_IMPORTING);
  dir->get(CDIR_PIN_IMPORTING);  
  
  // assimilate contents?
  if (!m->did_assim()) {
    m->mark_assim();  // only do this the first time!

    for (list<CInodeDiscover*>::iterator it = m->get_inodes().begin();
         it != m->get_inodes().end();
         it++) {
      // inode
      CInode *in = get_inode( (*it)->get_ino() );
      if (in) {
        (*it)->update_inode(in);
        dout(10) << " updated " << *in << endl;
      } else {
        in = new CInode;
        (*it)->update_inode(in);
        
        // link to the containing dir
        CInode *condiri = get_inode( m->get_containing_dirino(in->ino()) );
        assert(condiri && condiri->dir);
        add_inode( in );
        link_inode( condiri->dir, m->get_dentry(in->ino()), in );
        
        dout(10) << "   added " << *in << endl;
      }
      
      assert( in->get_parent_dir()->ino() == m->get_containing_dirino(in->ino()) );
      
      // dir
      if (m->have_dir(in->ino())) {
        if (in->dir) {
          m->get_dir(in->ino())->update_dir(in->dir);
          dout(10) << " updated " << *in->dir << endl;
        } else {
          in->set_dir( new CDir(in, mds, false) );
          m->get_dir(in->ino())->update_dir(in->dir);
          dout(10) << "   added " << *in->dir;
        }
      }
    }

    // open export dirs?
    for (list<inodeno_t>::iterator it = m->get_exports().begin();
         it != m->get_exports().end();
         it++) {
      CInode *in = get_inode(*it);
      assert(in);
      
      if (in->dir) {
        in->dir->get(CDIR_PIN_IMPORTINGEXPORT);
        dout(10) << "  pinning nested export " << *in->dir << endl;
      } else {
        dout(10) << "  opening nested export on " << *in << endl;

        // open (send discover back to old auth for fw to dir auth)
        filepath want;
        mds->messenger->send_message(new MDiscover(mds->get_nodeid(),
                                                   in->ino(),
                                                   want,
                                                   true),
                                     MSG_ADDR_MDS(in->authority()), MDS_PORT_CACHE, MDS_PORT_CACHE);
        
        // wait
        in->add_waiter(CINODE_WAIT_DIR,
                       new C_MDS_RetryMessage(mds, m));
      }
    }
  }
  

  // verify we have all exports
  int waiting_for = 0;
  for (list<inodeno_t>::iterator it = m->get_exports().begin();
       it != m->get_exports().end();
       it++) {
    CInode *in = get_inode(*it);
    if (!in->dir) waiting_for++;
  }
  if (waiting_for) {
    dout(7) << " waiting for " << waiting_for << " nested export dir opens" << endl;
    return;
  }

  // ok!
  dout(7) << " all ready, sending export_dir_prep_ack on " << *dir << endl;
  mds->messenger->send_message(new MExportDirPrepAck(dir->ino()),
							   m->get_source(), MDS_PORT_CACHE, MDS_PORT_CACHE);
  
  // done 
  delete m;
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
  CInode *idir = get_inode(m->get_ino());
  assert(idir);
  CDir *dir = idir->dir;
  assert(dir);

  int oldauth = m->get_source();
  dout(7) << "handle_export_dir, import " << *dir << " from " << oldauth << endl;
  assert(dir->is_auth() == false);

  /* do this at prep stage?????
  // prediscovers?
  if (m->any_prediscovers()) {
	dout(7) << "handle_export_dir checking prediscovers" << endl;
	int needed = 0;
	for (map<inodeno_t, set<string> >::iterator it = m->prediscover_begin();
		 it != m->prediscover_end();
		 it++) {
	  CInode *in = get_inode(it->first);
	  assert(in);
	  assert(in->dir);
	  string dirpath;
	  in->make_path(dirpath);	  
	  for (set<string>::iterator dit = it->second.begin();
		   dit != it->second.end();
		   dit++) {
		// do i have it?
		if (in->dir->lookup(*dit)) {
		  dout(7) << "had " << *dit << endl;
		} else {
		  int dauth = in->dir->dentry_authority(*dit, mds->get_cluster());
		  vector<string> *want = new vector<string>;
		  want->push_back(*dit);
		  dout(7) << "discovering " << dirpath << "/" << *dit << " from " << dauth << endl;
		  mds->messenger->send_message(new MDiscover(mds->get_nodeid(), dirpath, want),
									   MSG_ADDR_MDS(dauth), MDS_PORT_CACHE,
									   MDS_PORT_CACHE);
		  in->dir->add_waiter(CDIR_WAIT_DENTRY,
							  *dit,
							  new C_MDS_ImportPrediscover(mds, m, in->ino(), *dit));
		  needed++;
		}
	  }
	}
	if (needed > 0) return;
  }
  */


  show_imports();
  mds->logger->inc("im");
  
  // note new authority (locally) in inode
  if (dir->inode->is_auth())
	dir->dir_auth = CDIR_AUTH_PARENT;
  else
	dir->dir_auth = mds->get_nodeid();

  // update imports/exports
  CDir *containing_import;
  if (exports.count(dir)) {
	// reimporting
	dout(7) << " i'm reimporting " << *dir << endl;
	exports.erase(dir);

	dir->put(CDIR_PIN_EXPORT);                // unpin, no longer an export
	
	containing_import = get_containing_import(dir);  
	dout(7) << "  it is nested under import " << *containing_import << endl;
	for (pair< multimap<CDir*,CDir*>::iterator, multimap<CDir*,CDir*>::iterator > p =
		   nested_exports.equal_range( containing_import );
		 p.first != p.second;
		 p.first++) {
	  if ((*p.first).second == dir) {
		nested_exports.erase(p.first);
		break;
	  }
	}
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

	// remove our pin
	ex->put(CDIR_PIN_IMPORTINGEXPORT);

	// ...
    if (ex->is_import()) {
      dout(7) << " importing my import " << *ex << endl;
      imports.erase(ex);
      ex->state_clear(CDIR_STATE_IMPORT);

      mds->logger->inc("immyex");

      // move nested exports under containing_import
      for (pair<multimap<CDir*,CDir*>::iterator, multimap<CDir*,CDir*>::iterator> p =
             nested_exports.equal_range(ex);
           p.first != p.second;
           p.first++) {
        CDir *nested = (*p.first).second;
        dout(7) << "     moving nested export " << nested << " under " << containing_import << endl;
        nested_exports.insert(pair<CDir*,CDir*>(containing_import, nested));
      }

      // de-list under old import
      nested_exports.erase(ex);	
      
	  ex->dir_auth = CDIR_AUTH_PARENT;
      ex->put(CDIR_PIN_IMPORT);       // imports are pinned, no longer import

    } else {
      dout(7) << " importing export " << *ex << endl;

      // add it
      ex->get(CDIR_PIN_EXPORT);           // all exports are pinned
      exports.insert(ex);
      nested_exports.insert(pair<CDir*,CDir*>(containing_import, ex));
      mds->logger->inc("imex");
    }
    
  }


  // add this crap to my cache
  list<inodeno_t> imported_subdirs;
  int off = 0;
  for (int i = 0; i < m->get_ndirs(); i++) {
	import_dir_block(m->get_state(), 
					 off,
					 oldauth, 
					 dir,                 // import root
					 imported_subdirs);
  }
  dout(7) << " " << imported_subdirs.size() << " imported subdirs" << endl;
  

  // adjust popularity
  // FIXME what about rest of pop vector?  also, i think this is wrong.
  double newpop = m->get_ipop() - idir->popularity[0].get();
  dout(7) << " imported popularity jump by " << newpop << endl;
  if (newpop > 0) {  // duh
	CInode *t = idir;
	while (t) {
	  t->popularity[0].adjust(newpop);
	  if (t->parent) 
		t = t->parent->dir->inode;
	  else break;
	}
  }


  // send notify's etc.
  dout(7) << "sending notifyack for " << *dir << " to old auth " << m->get_source() << endl;
  mds->messenger->send_message(new MExportDirNotifyAck(dir->inode->ino()),
							   m->get_source(), MDS_PORT_CACHE,
							   MDS_PORT_CACHE);

  dout(7) << "sending notify to others" << endl;
  for (set<int>::iterator it = dir->open_by.begin();
	   it != dir->open_by.end();
	   it++) {
	assert( *it != mds->get_nodeid() );
	if ( *it == m->get_source() ) continue;  // not to old auth.

	MExportDirNotify *notify = new MExportDirNotify(dir->ino(), m->get_source(), mds->get_nodeid());
	if (g_conf.mds_verify_export_dirauth)
	  notify->copy_subdirs(imported_subdirs);   // copy subdir list (debug)

	mds->messenger->send_message(notify,
								 MSG_ADDR_MDS( *it ), MDS_PORT_CACHE,
								 MDS_PORT_CACHE);
  }
  
  // done
  delete m;

  show_imports();


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
  CInode *idir = get_inode(m->get_ino());
  CDir *dir = idir->dir;
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



/*
CInode *MDCache::import_dentry_inode(CDir *dir,
									 pchar& p,
									 int from,
									 CDir *import_root,
									 int *would_be_dir_auth)
{
  // we have three cases:
  assert((dir->is_auth() && !dir->is_hashing()) ||  // auth importing (may be hashed or normal)
		 (!dir->is_auth() && dir->is_hashing()) ||  // nonauth hashing (not yet hashed)
		 (dir->is_auth() && dir->is_unhashing()));  // auth reassimilating (currently hashed)
  
  // dentry
  string dname = p;
  p += dname.length()+1;
  
  // inode
  Inode_Export_State_t *istate = (Inode_Export_State_t*)p;
  p += sizeof(*istate);

  CInode *in = get_inode(istate->inode.ino);
  bool importing = true;
  bool had_inode = false;
  if (!in) {
	in = new CInode;
	in->inode = istate->inode;
  } else 
	had_inode = true;
  
  // auth wonkiness
  if (dir->is_unhashing()) {
	// auth reassimilating
	in->inode = istate->inode;
	in->set_auth(true);
  } 
  else if (dir->is_hashed()) {
	// import on hashed dir
	assert(in->is_dir());
	if (in->authority() == mds->get_nodeid())
	  in->set_auth(true);
	else 
	  in->set_auth(false);
	importing = false;
  } 
  else {
	// normal import
	in->inode = istate->inode;
	in->set_auth(true);
  }

  // now that auth is defined we can link
  if (!had_inode) {
	// add
	add_inode(in);
	link_inode(dir, dname, in);	
	dout(7) << "   import_dentry_inode adding " << *in << " istate.dir_auth " << istate->dir_auth << endl;
  } else {
	dout(7) << "   import_dentry_inode already had " << *in << " istate.dir_auth " << istate->dir_auth << endl;
  }

  // assimilate new auth state?
  if (importing) {

	*would_be_dir_auth = istate->dir_auth;
	
	// hashing fyi
	if (in->dir_is_hashed()) {
	  dout(7) << "   imported hashed dir " << *in->dir << endl;
	  assert(!in->dir || in->dir->is_hashed());
	} else
	  assert(!in->dir || !in->dir->is_hashed());

	// update inode state with authoritative info
	in->version = istate->version;
	in->popularity[0] = istate->popularity;   // FIXME rest of vector?

	// cached_by
	in->cached_by.clear(); 
	for (int nby = istate->ncached_by; nby>0; nby--) {
      int node = *((int*)p);
	  p += sizeof(int);
      int nonce = *((int*)p);
      p += sizeof(int);

	  if (node != mds->get_nodeid()) 
		in->cached_by_add( node, nonce );
	}
	
	in->cached_by_add(from, CINODE_EXPORT_NONCE);             // old auth still has it too.
  
	// dist state: new authority inherits softasync state only; sync/lock are dropped for import/export
	in->dist_state = 0;
	if (istate->is_softasync)
	  in->dist_state |= CINODE_DIST_SOFTASYNC;
  
	// other state? ***

    // dirty?
	if (istate->dirty) {
	  in->mark_dirty();
	  
	  dout(10) << "logging dirty import " << *in << endl;
	  mds->mdlog->submit_entry(new EInodeUpdate(in),
							   NULL);   // FIXME pay attention to completion?
	}
  } else {
	// this is a directory; i am importing a hashed dir
	assert(in->is_dir());
	assert(dir->is_hashed());

	dout(10) << "    import_dir_auth !importing.  hashed dir." << endl;

	int auth = in->authority();

	if (in->is_auth()) {
	  assert(in->is_cached_by(from));
	  assert(auth == mds->get_nodeid());
	} else {
	  if (auth == from) {
		// do nothing.  from added us to their cached_by.
	  } else {
		if (had_inode) {
		  dout(7) << "   imported collateral dir " << *in->dir << " auth " << auth << ", had it" << endl;
		} else {
		  dout(7) << "   imported collateral dir " << *in->dir << " auth " << auth << ", discovering it" << endl;
		  assert(0);  // maybe it gto expired already?
		}
	  }
	}
  }
  
  return in;
}
*/

void MDCache::import_dir_block(crope& r,
							   int& off,
							   int oldauth,
							   CDir *import_root,
							   list<inodeno_t>& imported_subdirs)
{
  // set up dir
  CDirExport dstate;
  off = dstate._unrope(r, off);

  CInode *idir = get_inode(dstate.get_ino());
  assert(idir);
  CDir *dir = idir->get_or_open_dir(mds);
  assert(dir);
 
  dout(7) << " import_dir_block " << *dir << " " << dir->nitems << " items" << endl;

  // add to list
  if (dir != import_root)
    imported_subdirs.push_back(dir->ino());

  // assimilate state
  dstate.update_dir( dir );

  // mark  (may already be marked from get_or_open_dir() above)
  if (!dir->is_auth())
	dir->state_set(CDIR_STATE_AUTH);
  

  // take all waiters on this dir
  // NOTE: a pass of imported data is guaranteed to get all of my waiters because
  // a replica's presense in my cache implies/forces it's presense in authority's.
  list<Context*> waiters;
  dir->take_waiting(CDIR_WAIT_ANY, waiters);
  for (list<Context*>::iterator it = waiters.begin();
	   it != waiters.end();
	   it++) 
	import_root->add_waiter(CDIR_WAIT_IMPORTED, *it);


  // contents
  for (long nitems = dir->nitems; nitems>0; nitems--) {
    // dentry
    string dname = r.c_str() + off;
    off += dname.length()+1;
    
    // inode
    CInodeExport istate;
    off = istate._unrope(r, off);
    
    bool added = false;
    CInode *in = get_inode(istate.get_ino());
    if (!in) {
      in = new CInode;
      added = true;
    }

    // state
    istate.update_inode(in);
    in->set_auth(true);

    if (!in->is_cached_by(oldauth))
      in->cached_by_add( oldauth, CINODE_EXPORT_NONCE );

    if (added) {
      add_inode(in);
      link_inode(dir, dname, in);
      dout(10) << "added " << *in << endl;
    } else {
      dout(10) << "  had " << *in << endl;
    }

    // other
    if (in->is_dirty()) {
      dout(10) << "logging dirty import " << *in << endl;
	  mds->mdlog->submit_entry(new EInodeUpdate(in),
							   NULL);   // FIXME pay attention to completion?
    }
    
  }
 
}


void MDCache::got_hashed_replica(CDir *import,
								 inodeno_t dir_ino,
								 inodeno_t replica_ino)
{

  dout(7) << "got_hashed_replica for import " << *import << " ino " << replica_ino << " in dir " << dir_ino << endl;
  
  // remove from import_hashed_replicate_waiting.
  for (multimap<inodeno_t,inodeno_t>::iterator it = import_hashed_replicate_waiting.find(dir_ino);
	   it != import_hashed_replicate_waiting.end();
	   it++) {
	if (it->second == replica_ino) {
	  import_hashed_replicate_waiting.erase(it);
	  break;
	} else 
	  assert(it->first == dir_ino); // it better be here!
  }
  
  // last one for that dir?
  CInode *idir = get_inode(dir_ino);
  assert(idir && idir->dir);
  if (import_hashed_replicate_waiting.count(dir_ino) > 0)
	return;  // still more
  
  // done with this dir!
  idir->dir->unfreeze_dir();
  
  // remove from import_hashed_frozen_waiting
  for (multimap<inodeno_t,inodeno_t>::iterator it = import_hashed_frozen_waiting.find(import->ino());
	   it != import_hashed_frozen_waiting.end();
	   it++) {
	if (it->second == dir_ino) {
	  import_hashed_frozen_waiting.erase(it);
	  break;
	} else 
	  assert(it->first == import->ino()); // it better be here!
  }
  
  // last one for this import?
  if (import_hashed_frozen_waiting.count(import->ino()) == 0) {
	// all done, we can finish import!


	// THISIS BROKEN FOR HASHED... FIXME
	//	mds->mdcache->import_dir_finish(import);
  }
}





// authority bystander

void MDCache::handle_export_dir_warning(MExportDirWarning *m)
{
  CInode *in = get_inode(m->get_ino());
  if (!in) {
	// add to stray warning list
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
	
	delete m;
	return;
  } 
  
  dout(7) << "handle_export_dir_warning on " << *in << endl;
  assert(!(in->dir && in->dir->is_auth()));  // clearly not my dir
  
  // flag inode state.
  in->state_set(CINODE_STATE_EXPORTING);
  
  // kick it now if the notify is already waiting.
  in->finish_waiting(CINODE_WAIT_EXPORTWARNING);

  // done
  delete m;
}


void MDCache::handle_export_dir_notify(MExportDirNotify *m)
{
  CDir *dir = 0;
  CInode *in = get_inode(m->get_ino());
  if (in) dir = in->dir;
  if (!dir) {

	// did i see the warning yet?
	if (stray_export_warnings.count(m->get_ino())) {
	  // i did, we're all good.
	  dout(7) << "handle_export_dir_notify on " << m->get_ino() << ", already saw warning." << endl;
	} else {
	  // wait for it.
	  dout(7) << "handle_export_dir_notify on " << m->get_ino() << ", waiting for warning." << endl;
	  stray_export_notifies.insert(pair<inodeno_t, MExportDirNotify*>( m->get_ino(), m ));
	  return;
	}

  } else {
	// have i heard about it from the exporter yet?
	if (!dir->state_test(CDIR_STATE_AUTHMOVING)) {
	  dout(7) << "handle_export_dir_notify on " << *dir << " haven't seen warning from old auth yet, waiting" << endl;
	  dir->add_waiter(CDIR_WAIT_EXPORTWARNING,
					  new C_MDS_RetryMessage(mds, m));
	  return;
	}

	// ok!
    dout(7) << "handle_export_dir_notify on " << *dir << " new_auth " << m->get_new_auth() << endl;

	// fix state.
	dir->state_clear(CDIR_STATE_AUTHMOVING);  // exported!
  
	// update dir_auth
	if (in->authority() == m->get_new_auth()) {
	  dout(7) << "handle_export_dir_notify on " << *in << ": inode auth is the same, setting dir_auth -1" << endl;
	  dir->dir_auth = -1;
	  assert(!in->is_auth());
	  assert(!dir->is_auth());
	} else {
	  dir->dir_auth = m->get_new_auth();
	}
	assert(dir->authority() != mds->get_nodeid());
	assert(!dir->is_auth());
	
	// debug: verify subdirs
	if (g_conf.mds_verify_export_dirauth) {

	  dout(7) << "handle_export_dir_notify on " << *dir << " checking " << m->num_subdirs() << " subdirs" << endl;
	  for (list<inodeno_t>::iterator it = m->subdirs_begin();
		   it != m->subdirs_end();
		   it++) {
		CInode *idir = get_inode(*it);
		if (!idir) continue;  // don't have it, don't care
		CDir *dir = idir->dir;
		if (!dir) continue;
		dout(10) << "handle_export_dir_notify checking subdir " << *dir << " is auth " << dir->dir_auth << endl;
		assert(dir != dir);	  // base shouldn't be in subdir list
		if (dir->dir_auth != CDIR_AUTH_PARENT) {
		  dout(7) << "*** weird value for dir_auth " << dir->dir_auth << " on " << *dir << ", should have been -1 probably??? ******************" << endl;
		  assert(0);  // bad news!
		  //dir->dir_auth = -1;
		}
		assert(dir->authority() == m->get_new_auth());
	  }
	}
  }

  // send notify ack to old auth
  dout(7) << "handle_export_dir_notify sending ack to old_auth " << m->get_old_auth() << endl;
  mds->messenger->send_message(new MExportDirNotifyAck(m->get_ino()),
							   MSG_ADDR_MDS(m->get_old_auth()), MDS_PORT_CACHE, MDS_PORT_CACHE);
  
  // done
  delete m;
}




// HASHING

/*
 
 interaction of hashing and export/import:

  - dir->is_auth() is completely independent of hashing.  for a hashed dir,
     - all nodes are partially authoritative
     - all nodes dir->is_hashed() == true
     - all nodes dir->inode->dir_is_hashed() == true
     - one node dir->is_auth == true, the rest == false
  - dir_auth for all items in a hashed dir will likely be explicit.

  - export_dir_walk and import_dir_block take care with dir_auth:
     - on export, -1 is changed to mds->get_nodeid()
     - on import, nothing special, actually.

  - hashed dir files aren't included in export
  - hashed dir dirs ARE included in export, but as replicas.  this is important
    because dirs are needed to tie together hierarchy, for auth to know about
    imports/exports, etc.
    - if exporter is auth, adds importer to cached_by
    - if importer is auth, importer will be fine
    - if third party is auth, sends MExportReplicatedHashed to auth
      - auth sends MExportReplicatedHashedAck to importer, who can proceed
        (ie send export ack) when all such messages are received.

  - dir state is preserved
    - COMPLETE and DIRTY aren't transferred
    - new auth should already know the dir is hashed.
  
*/

// HASH on auth

void MDCache::drop_sync_in_dir(CDir *dir)
{
  for (CDir_map_t::iterator it = dir->begin(); it != dir->end(); it++) {
	CInode *in = it->second->inode;
	if (in->is_auth() && 
		in->is_syncbyme()) {
	  dout(7) << "dropping sticky(?) sync on " << *in << endl;
	  inode_sync_release(in);
	}
  }
}


class C_MDS_HashFreeze : public Context {
public:
  MDS *mds;
  CDir *dir;
  C_MDS_HashFreeze(MDS *mds, CDir *dir) {
	this->mds = mds;
	this->dir = dir;
  }
  virtual void finish(int r) {
	mds->mdcache->hash_dir_finish(dir);
  }
};

class C_MDS_HashComplete : public Context {
public:
  MDS *mds;
  CDir *dir;
  C_MDS_HashComplete(MDS *mds, CDir *dir) {
	this->mds = mds;
	this->dir = dir;
  }
  virtual void finish(int r) {
	mds->mdcache->hash_dir_complete(dir);
  }
};

void MDCache::hash_dir(CDir *dir)
{
  assert(!dir->is_hashing());
  assert(!dir->is_hashed());
  assert(dir->is_auth());
  
  if (dir->is_frozen() ||
	  dir->is_freezing()) {
	dout(7) << " can't hash, freezing|frozen." << endl;
	return;
  }
  
  dout(7) << "hash_dir " << *dir << endl;

  // fix state
  dir->state_set(CDIR_STATE_HASHING);
  dir->auth_pin();

  // start freeze
  dir->freeze_dir(new C_MDS_HashFreeze(mds, dir));

  // make complete
  if (!dir->is_complete()) {
	dout(7) << "hash_dir " << *dir << " not complete, fetching" << endl;
	mds->mdstore->fetch_dir(dir,
							new C_MDS_HashComplete(mds, dir));
  } else
	hash_dir_complete(dir);

  // drop any sync or lock if sticky
  if (g_conf.mdcache_sticky_sync_normal ||
	  g_conf.mdcache_sticky_sync_softasync) 
	drop_sync_in_dir(dir);
}

void MDCache::hash_dir_complete(CDir *dir)
{
  assert(dir->is_hashing());
  assert(!dir->is_hashed());
  assert(dir->is_auth());

  // mark dirty to pin in cache
  for (CDir_map_t::iterator it = dir->begin(); it != dir->end(); it++) {
	CInode *in = it->second->inode;
	int dentryhashcode = mds->get_cluster()->hash_dentry( dir->inode->ino(), it->first );
	if (dentryhashcode == mds->get_nodeid()) 
	  in->mark_dirty();
  }
  
  hash_dir_finish(dir);
}

void MDCache::hash_dir_finish(CDir *dir)
{
  /*
  assert(dir->is_hashing());
  assert(!dir->is_hashed());
  assert(dir->is_auth());
  
  if (!dir->is_frozen_dir()) {
	dout(7) << "hash_dir_finish !frozen yet " << *dir->inode << endl;
	return;
  }
  if (!dir->is_complete()) {
	dout(7) << "hash_dir_finish !complete, waiting still " << *dir->inode << endl;
	return;  
  }

  dout(7) << "hash_dir_finish " << *dir << endl;
  
  // get messages to other nodes ready
  vector<MHashDir*> msgs;
  string path;
  dir->inode->make_path(path);
  for (int i=0; i<mds->get_cluster()->get_num_mds(); i++) {
	msgs.push_back(new MHashDir(path));
  }
  
  // divy up contents
  for (CDir_map_t::iterator it = dir->begin(); it != dir->end(); it++) {
	CInode *in = it->second->inode;
	
	int dentryhashcode = mds->get_cluster()->hash_dentry( dir->inode->ino(), it->first );
	if (dentryhashcode == mds->get_nodeid())
	  continue;      // still mine!

	// giving it away.
	in->version++;   // so log entries are ignored, etc.
	
	// mark my children explicitly mine
	if (in->dir_auth == CDIR_AUTH_PARENT)
	  in->dir_auth = mds->get_nodeid();
	
	// add dentry and inode to message
	msgs[dentryhashcode]->dir_rope.append( it->first.c_str(), it->first.length()+1 );
	msgs[dentryhashcode]->dir_rope.append( in->encode_export_state() );
	
	// fix up my state
	if (in->is_dirty()) in->mark_clean();
	in->cached_by_clear();
	
	assert(in->auth == true);
	in->set_auth(false);

	// there should be no waiters.
  }

  // send them
  for (int i=0; i<mds->get_cluster()->get_num_mds(); i++) {
	mds->messenger->send_message(msgs[i],
								 MSG_ADDR_MDS(i), MDS_PORT_CACHE, MDS_PORT_CACHE);
  }

  // inode state
  dir->inode->inode.isdir = INODE_DIR_HASHED;
  if (dir->inode->is_auth())
	dir->inode->mark_dirty();

  // dir state
  dir->state_set(CDIR_STATE_HASHED);
  dir->state_clear(CDIR_STATE_HASHING);
  dir->mark_dirty();

  // FIXME: log!

  // unfreeze
  dir->unfreeze_dir();
*/
}


/*
hmm, not going to need to do this for now!

void handle_hash_dir_ack(MHashDirAck *m)
{
  CInode *in = 
  
  // done
  delete m;
}
*/

void MDCache::handle_hash_dir(MHashDir *m)
{
  /*
  // traverse to node
  vector<CInode*> trav;
  int r = path_traverse(m->get_path(), trav, m, MDS_TRAVERSE_DISCOVER);   
  if (r > 0) return;  // fw or delay

  CInode *idir = trav[trav.size()-1];
  CDir *dir = idir->get_dir(mds->get_nodeid());

  dout(7) << "handle_hash_dir " << *dir << endl;

  assert(!dir->is_auth());
  assert(!dir->is_hashed());

  // dir state
  dir->state_set(CDIR_STATE_HASHING);

  // assimilate contents
  int oldauth = m->get_source();
  const char *p = m->dir_rope.c_str();
  const char *pend = p + m->dir_rope.length();
  while (p < pend) {
	CInode *in = import_dentry_inode(dir, p, oldauth);
	in->mark_dirty();  // pin in cache
  }

  // dir state
  dir->state_clear(CDIR_STATE_HASHING);
  dir->state_set(CDIR_STATE_HASHED);
 
  // dir is complete
  dir->mark_complete();
  dir->mark_dirty();

  // inode state
  idir->inode.isdir = INODE_DIR_HASHED;
  if (idir->is_auth()) 
	idir->mark_dirty();

  // FIXME: log

  // done.
  delete m;
  */
}




// UNHASHING

class C_MDS_UnhashFreeze : public Context {
public:
  MDS *mds;
  CDir *dir;
  C_MDS_UnhashFreeze(MDS *mds, CDir *dir) {
	this->mds = mds;
	this->dir = dir;
  }
  virtual void finish(int r) {
	mds->mdcache->unhash_dir_finish(dir);
  }
};

class C_MDS_UnhashComplete : public Context {
public:
  MDS *mds;
  CDir *dir;
  C_MDS_UnhashComplete(MDS *mds, CDir *dir) {
	this->mds = mds;
	this->dir = dir;
  }
  virtual void finish(int r) {
	mds->mdcache->unhash_dir_complete(dir);
  }
};

/*
void MDCache::unhash_dir(CDir *dir)
{
  assert(dir->is_hashed());
  assert(!dir->is_unhashing());
  assert(dir->is_auth());
  
  if (dir->is_frozen() ||
	  dir->is_freezing()) {
	dout(7) << " can't un_hash, freezing|frozen." << endl;
	return;
  }
  
  dout(7) << "unhash_dir " << *dir << endl;

  // fix state
  dir->state_set(CDIR_STATE_UNHASHING);

  // freeze
  dir->freeze_dir(new C_MDS_UnhashFreeze(mds, dir));

  // request unhash from other nodes
  string path;
  dir->inode->make_path(path);
  for (int i=0; i<mds->get_cluster()->get_num_mds(); i++) {
	if (i == mds->get_nodeid()) continue;
	mds->messenger->send_message(new MUnhashDir(path),
								 MSG_ADDR_MDS(i), MDS_PORT_CACHE, MDS_PORT_CACHE);
	unhash_waiting.insert(pair<CDir*,int>(dir,i));
  }
  
  // make complete
  if (!dir->is_complete()) {
	dout(7) << "hash_dir " << *dir << " not complete, fetching" << endl;
	mds->mdstore->fetch_dir(dir->inode,
							new C_MDS_UnhashComplete(mds, dir));
  } else
	unhash_dir_complete(dir);

  // drop any sync or lock if sticky
  if (g_conf.mdcache_sticky_sync_normal ||
	  g_conf.mdcache_sticky_sync_softasync)
	drop_sync_in_dir(dir);
}

 
void MDCache::unhash_dir_complete(CDir *dir)
{
  // mark all my inodes dirty (to avoid a race)
  for (CDir_map_t::iterator it = dir->begin(); it != dir->end(); it++) {
	CInode *in = it->second->inode;
	int dentryhashcode = mds->get_cluster()->hash_dentry( dir->inode->ino(), it->first );
	if (dentryhashcode == mds->get_nodeid()) 
	  in->mark_dirty();
  }
  
  unhash_dir_finish(dir);
}


void MDCache::unhash_dir_finish(CDir *dir)
{
  if (!dir->is_frozen_dir()) {
	dout(7) << "unhash_dir_finish still waiting for freeze on " << *dir->inode << endl;
	return;
  }
  if (!dir->is_complete()) {
	dout(7) << "unhash_dir_finish still waiting for complete on " << *dir->inode << endl;
	return;
  }
  if (unhash_waiting.count(dir) > 0) {
	dout(7) << "unhash_dir_finish still waiting for all acks on " << *dir->inode << endl;
	return;
  }
  
  dout(7) << "unhash_dir_finish " << *dir << endl;
  
  // dir state
  dir->state_clear(CDIR_STATE_HASHED);
  dir->state_clear(CDIR_STATE_UNHASHING);
  dir->mark_dirty();
  dir->mark_complete();
  
  // inode state
  dir->inode->inode.isdir = INODE_DIR_NORMAL;
  dir->inode->mark_dirty();

  // unfreeze!
  dir->unfreeze_dir();
}
*/

void MDCache::handle_unhash_dir_ack(MUnhashDirAck *m)
{
  /*
  CInode *idir = get_inode(m->get_ino());
  assert(idir && idir->dir);
  assert(idir->dir->is_auth());
  assert(idir->dir->is_hashed());
  assert(idir->dir->is_unhashing());

  dout(7) << "handle_unhash_dir_ack " << *idir->dir << endl;
  
  // assimilate contents
  int oldauth = m->get_source();
  const char *p = m->dir_rope.c_str();
  const char *pend = p + m->dir_rope.length();
  while (p < pend) {
	CInode *in = import_dentry_inode(idir->dir, p, oldauth);
	in->mark_dirty();   // pin in cache
  }

  // remove from waiting list
  multimap<CDir*,int>::iterator it = unhash_waiting.find(idir->dir);
  while (it->second != oldauth) {
	it++;
	assert(it->first == idir->dir);
  }
  unhash_waiting.erase(it);

  unhash_dir_finish(idir->dir);  // try to finish

  // done.
  delete m; 
  */
}


// unhash on non-auth

class C_MDS_HandleUnhashFreeze : public Context {
public:
  MDS *mds;
  CDir *dir;
  int auth;
  C_MDS_HandleUnhashFreeze(MDS *mds, CDir *dir, int auth) {
	this->mds = mds;
	this->dir = dir;
	this->auth = auth;
  }
  virtual void finish(int r) {
	mds->mdcache->handle_unhash_dir_finish(dir, auth);
  }
};

class C_MDS_HandleUnhashComplete : public Context {
public:
  MDS *mds;
  CDir *dir;
  int auth;
  C_MDS_HandleUnhashComplete(MDS *mds, CDir *dir, int auth) {
	this->mds = mds;
	this->dir = dir;
	this->auth = auth;
  }
  virtual void finish(int r) {
	mds->mdcache->handle_unhash_dir_complete(dir, auth);
  }
};


/*
void MDCache::handle_unhash_dir(MUnhashDir *m)
{
  // traverse to node
  vector<CInode*> trav;
  int r = path_traverse(m->get_path(), trav, m, MDS_TRAVERSE_DISCOVER);   
  if (r > 0) return;  // fw or delay

  CInode *idir = trav[trav.size()-1];
  if (!idir->dir) idir->dir = new CDir(idir, mds->get_nodeid());
  CDir *dir = idir->dir;

  dout(7) << "handle_unhash_dir " << *idir->dir << endl;
  
  assert(dir->is_hashed());
  
  int auth = m->get_source();

  // fix state
  dir->state_set(CDIR_STATE_UNHASHING);

  // freeze
  dir->freeze_dir(new C_MDS_HandleUnhashFreeze(mds, dir, auth));

  // make complete
  if (!dir->is_complete()) {
	dout(7) << "handle_unhash_dir " << *dir << " not complete, fetching" << endl;
	mds->mdstore->fetch_dir(dir->inode,
							new C_MDS_HandleUnhashComplete(mds, dir, auth));
  } else
	handle_unhash_dir_complete(dir, auth);

  // drop any sync or lock if sticky
  if (g_conf.mdcache_sticky_sync_normal ||
	  g_conf.mdcache_sticky_sync_softasync) 
	drop_sync_in_dir(dir);

  // done with message
  delete m;
}
*/

void MDCache::handle_unhash_dir_complete(CDir *dir, int auth)
{
  // mark all my inodes dirty (to avoid a race)
  for (CDir_map_t::iterator it = dir->begin(); it != dir->end(); it++) {
	CInode *in = it->second->inode;
	int dentryhashcode = mds->get_cluster()->hash_dentry( dir->inode->ino(), it->first );
	if (dentryhashcode == mds->get_nodeid()) 
	  in->mark_dirty();
  }
  
  handle_unhash_dir_finish(dir, auth);
}

void MDCache::handle_unhash_dir_finish(CDir *dir, int auth)
{
/*
  assert(dir->is_unhashing());
  assert(dir->is_hashed());

  if (!dir->is_complete()) {
	dout(7) << "still waiting for complete on " << *dir->inode << endl;
	return;
  }
  if (!dir->is_frozen_dir()) {
	dout(7) << "still waiting for frozen_dir on " << *dir->inode << endl;
	return;
  }

  assert(dir->is_frozen_dir());
  assert(dir->is_complete());

  dout(7) << "handle_unhash_dir_finish " << *dir->inode << endl;
  // okay, we are complete and frozen.
  
  // get message to auth ready
  MUnhashDirAck *msg = new MUnhashDirAck(dir->inode->ino());
  
  // include contents
  for (CDir_map_t::iterator it = dir->begin(); it != dir->end(); it++) {
	CInode *in = it->second->inode;
	
	int dentryhashcode = mds->get_cluster()->hash_dentry( dir->inode->ino(), it->first );
	
	if (dentryhashcode != mds->get_nodeid())
	  continue;      // not mine

	// give it away.
	in->version++;   // so log entries are ignored, etc.
	
	// add dentry and inode to message
	msg->dir_rope.append( it->first.c_str(), it->first.length()+1 );
	msg->dir_rope.append( in->encode_export_state() );
	
	if (in->dir_auth == auth)
	  in->dir_auth = CDIR_AUTH_PARENT;

	// fix up my state
	if (in->is_dirty()) in->mark_clean();
	in->cached_by_clear();
	
	assert(in->auth == true);
	in->set_auth(false);

	// there should be no waiters.
  }

  // send back to auth
  mds->messenger->send_message(msg,
							   MSG_ADDR_MDS(auth), MDS_PORT_CACHE, MDS_PORT_CACHE);

  // inode state
  dir->inode->inode.isdir = INODE_DIR_NORMAL;
  if (dir->inode->is_auth())
	dir->inode->mark_dirty();

  // dir state
  dir->state_clear(CDIR_STATE_HASHED);
  dir->state_clear(CDIR_STATE_UNHASHING);
  dir->mark_clean();  // it's not mine.

  // FIXME log
  
  // unfreeze
  dir->unfreeze_dir();
*/
}








// debug crap


void MDCache::show_imports()
{
  if (imports.size() == 0) {
	dout(7) << "no imports/exports" << endl;
	return;
  }
  dout(7) << "imports/exports:" << endl;

  set<CDir*> ecopy = exports;

  for (set<CDir*>::iterator it = imports.begin();
	   it != imports.end();
	   it++) {
	CDir *dir = *it;
	dout(7) << "  + import " << *dir << endl;
	assert( dir->is_import() );
	assert( dir->is_auth() );
	
	for (pair< multimap<CDir*,CDir*>::iterator, multimap<CDir*,CDir*>::iterator > p = 
		   nested_exports.equal_range( *it );
		 p.first != p.second;
		 p.first++) {
	  CDir *exp = (*p.first).second;
	  dout(7) << "      - ex " << *exp << " to " << exp->dir_auth << endl;
	  assert( dir->is_export() );

	  if ( get_containing_import(exp) != *it ) {
		dout(7) << "uh oh, containing import is " << get_containing_import(exp) << endl;
		dout(7) << "uh oh, containing import is " << *get_containing_import(exp) << endl;
		assert( get_containing_import(exp) == *it );
	  }

	  if (ecopy.count(exp) != 1) {
		dout(7) << " nested_export " << *exp << " not in exports" << endl;
		assert(0);
	  }
	  ecopy.erase(exp);
	}
  }

  if (ecopy.size()) {
	for (set<CDir*>::iterator it = ecopy.begin();
		 it != ecopy.end();
		 it++) 
	  dout(7) << " stray item in exports: " << **it << endl;
	assert(ecopy.size() == 0);
  }
  

}


void MDCache::show_cache()
{
  for (inode_map_t::iterator it = inode_map.begin();
	   it != inode_map.end();
	   it++) {
	dout(7) << "cache " << *((*it).second);
	if ((*it).second->ref) 
	  dout2(7) << " pin " << (*it).second->ref_set;
	if ((*it).second->cached_by.size())
	  dout2(7) << " cache_by " << (*it).second->cached_by;
	dout2(7) << endl;
  }
}


// hack
vector<CInode*> MDCache::hack_add_file(string& fn, CInode *in) {
  
  // root?
  if (fn == "/") {
	if (!root) {
	  root = in;
	  add_inode( in );
	  //dout(7) << " added root " << root << endl;
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
  
  CInode *idir = hack_get_file(dirpart);
  assert(idir);

  //dout(7) << " got dir " << idir << endl;

  if (idir->dir == NULL) {
	dout(4) << " making " << dirpart << " into a dir" << endl;
	idir->inode.isdir = true;
	idir->dir = new CDir(idir, mds);
  }
  
  add_inode( in );
  link_inode( idir->dir, file, in );

  vector<CInode*> trace;
  trace.push_back(idir);
  trace.push_back(in);
  while (idir->parent) {
	idir = idir->parent->dir->inode;
	trace.insert(trace.begin(),idir);
  }
  return trace;
}

CInode* MDCache::hack_get_file(string& fn) {
  int off = 1;
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
  lru->lru_status();

  return cur;  
}
