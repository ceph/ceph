
#include "MDCache.h"
#include "MDStore.h"
#include "CInode.h"
#include "CDir.h"
#include "MDS.h"
#include "MDCluster.h"
#include "MDLog.h"
#include "MDBalancer.h"

#include "include/filepath.h"

#include "msg/Message.h"
#include "msg/Messenger.h"

#include "events/EInodeUpdate.h"
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

#include "messages/MHashDir.h"
#include "messages/MUnhashDir.h"
#include "messages/MUnhashDirAck.h"

//#include "messages/MInodeUpdate.h"
#include "messages/MDirUpdate.h"
#include "messages/MCacheExpire.h"

#include "messages/MLock.h"
#include "messages/MDentryUnlink.h"

#include "messages/MRenameNotify.h"
#include "messages/MRename.h"
#include "messages/MRenameAck.h"
#include "messages/MRenameReq.h"

#include "messages/MClientRequest.h"

#include "messages/MInodeWriterClosed.h"

#include "IdAllocator.h"

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
  lru.lru_set_max(g_conf.mdcache_size);
  lru.lru_set_midpoint(g_conf.mdcache_mid);
}

MDCache::~MDCache() 
{
}


// 

bool MDCache::shutdown()
{
  if (lru.lru_get_size() > 0) {
	dout(7) << "WARNING: mdcache shutodwn with non-empty cache" << endl;
	show_cache();
	show_imports();
	dump();
  }
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
							  bool authchanged)   // _inode_ auth
{
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

		// move my nested imports to in's containing import
		CDir *con = get_containing_import(in->dir);
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
		
		// move any exports nested beneath me?
		CDir *newcon = get_containing_import(in->dir);
		assert(newcon);
		CDir *oldcon = get_containing_import(srcdir);
		assert(oldcon);
		if (newcon != oldcon) {
		  dout(7) << "moving nested exports under new container" << endl;
		  set<CDir*> nested;
		  find_nested_exports(in->dir, nested);
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

		// find old import
		CDir *oldcon = get_containing_import(srcdir);
		assert(oldcon);

		// move nested exports under me
		set<CDir*> nested;
		find_nested_exports(in->dir, nested);
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

		// do nothing.
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
	  } else {
		// inode was ours, still ours
		dout(7) << "inode was ours, still ours.  doing nothing." << endl;
		
		// do nothing.
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

		CDir *oldcon = get_containing_import(srcdir);
		assert(oldcon);
		assert(nested_exports[oldcon].count(in->dir) == 1);
		nested_exports[oldcon].erase(in->dir);

	  } else {
		// inode was replica, still replica
		dout(7) << "inode was replica, still replica.  do nothing." << endl;

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


bool MDCache::trim(__int32_t max) {
  if (max < 0) {
	max = lru.lru_get_max();
	if (!max) return false;
  }

  map<int, MCacheExpire*> expiremap;

  while (lru.lru_get_size() > max) {
	CInode *in = (CInode*)lru.lru_expire();
	if (!in) break; //return false;

	if (in->dir) {
	  // notify dir authority?
	  int auth = in->dir->authority();
	  if (auth != mds->get_nodeid()) {
		dout(7) << "sending expire to mds" << auth << " on   " << *in->dir << endl;
		if (expiremap.count(auth) == 0) expiremap[auth] = new MCacheExpire(mds->get_nodeid());
		expiremap[auth]->add_dir(in->ino(), in->dir->replica_nonce);
	  }
	}

	// notify inode authority?
	{
	  int auth = in->authority();
	  if (auth != mds->get_nodeid()) {
		dout(7) << "sending expire to mds" << auth << " on " << *in << endl;
		if (expiremap.count(auth) == 0) expiremap[auth] = new MCacheExpire(mds->get_nodeid());
		expiremap[auth]->add_inode(in->ino(), in->replica_nonce);
	  }	
	}
	CInode *diri = NULL;
	if (in->parent)
	  diri = in->parent->dir->inode;

	if (in->is_root()) {
	  dout(7) << "just trimmed root, cache now empty." << endl;
	  root = NULL;
	}

	// remove it
	dout(11) << "trim removing " << *in << " " << in << endl;
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
  }

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


void MDCache::shutdown_start()
{
  dout(1) << "shutdown_start: forcing unsync, unlock of everything" << endl;

  // walk cache
  bool didsomething = false;
  for (hash_map<inodeno_t, CInode*>::iterator it = inode_map.begin();
	   it != inode_map.end();
	   it++) {
	CInode *in = it->second;

	// commit any dirty dir that's ours
	if (in->is_dir() && in->dir && in->dir->is_auth() && in->dir->is_dirty())
	  mds->mdstore->commit_dir(in->dir, NULL);
	
	//drop locks?
	if (in->is_auth()) {
	  //if (in->is_syncbyme()) inode_sync_release(in);
	  //if (in->is_lockbyme()) inode_lock_release(in);
	}
  }

}

bool MDCache::shutdown_pass()
{
  static bool did_inode_updates = false;

  dout(7) << "shutdown_pass" << endl;
  //assert(mds->is_shutting_down());
  if (mds->is_shut_down()) {
	dout(7) << " already shut down" << endl;
	show_cache();
	show_imports();
	return true;
  }

  // (wait for) flush log
  if (mds->mdlog->get_num_events()) {
	dout(7) << "waiting for log to flush" << endl;
	return false;
  } 
  
  // make a pass on the cache
  dout(7) << "log is empty.  flushing cache" << endl;
  trim(0);
  
  dout(7) << "cache size now " << lru.lru_get_size() << endl;
  
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
  } 

  // shut down root?
  if (lru.lru_get_size() == 1) {
	if (root && 
		root->dir && 
		root->dir->is_import() &&
		root->dir->get_ref() == 1) {  // 1 is the import!
	  // un-import
	  dout(7) << "removing root import" << endl;
	  imports.erase(root->dir);
	  root->dir->state_clear(CDIR_STATE_IMPORT);
	  root->dir->put(CDIR_PIN_IMPORT);
	  trim(0);
	}

	if (root && root->is_pinned_by(CINODE_PIN_DIRTY)) {
	  dout(7) << "clearing root dirty flag" << endl;
	  root->put(CINODE_PIN_DIRTY);
	  trim(0);
	}
  }
	
  // sanity
  assert(inode_map.size() == lru.lru_get_size());

  // done?
  if (lru.lru_get_size() == 0) {
	if (mds->get_nodeid() != 0) {
	  dout(7) << "done, sending shutdown_finish" << endl;
	  mds->messenger->send_message(new MGenericMessage(MSG_MDS_SHUTDOWNFINISH),
								   MSG_ADDR_MDS(0), MDS_PORT_MAIN, MDS_PORT_MAIN);
	} else {
	  mds->handle_shutdown_finish(NULL);
	}
	return true;
  } else {
	dout(7) << "there's still stuff in the cache: " << lru.lru_get_size() << endl;
	show_cache();
	dump();
  }
  return false;
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
}


CDir *MDCache::get_containing_import(CDir *dir)
{
  CDir *imp = dir;  // might be *dir

  // find the underlying import!
  while (imp && 
		 !imp->is_import()) {
	imp = imp->get_parent_dir();
  }

  assert(imp);
  return imp;
}

CDir *MDCache::get_containing_export(CDir *dir)
{
  CDir *ex = dir;  // might be *dir

  // find the underlying import!
  while (ex &&                        // while not at root,
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

	/*
  case MSG_MDS_INODEUPDATE:
	handle_inode_update((MInodeUpdate*)m);
	break;
	*/

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
  case MSG_MDS_INODEWRITERCLOSED:
	handle_inode_writer_closed((MInodeWriterClosed*)m);
	break;

  case MSG_MDS_DENTRYUNLINK:
	handle_dentry_unlink((MDentryUnlink*)m);
	break;

  case MSG_MDS_RENAMENOTIFY:
	handle_rename_notify((MRenameNotify*)m);
	break;
  case MSG_MDS_RENAME:
	handle_rename((MRename*)m);
	break;
  case MSG_MDS_RENAMEREQ:
	handle_rename_req((MRenameReq*)m);
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
	if (r < 0) {            // ENOENT on discover, pass back to caller.
	  onfinish->finish(r);
	} else {
	  ondelay->finish(r);   // retry as usual
	}
	delete onfinish;
	delete ondelay;
  }
};

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


int MDCache::path_traverse(filepath& origpath, 
						   vector<CDentry*>& trace, 
						   bool follow_trailing_symlink,
						   Message *req,
						   Context *ondelay,
						   int onfail,
						   Context *onfinish)   // use this instead of return value, if specified
{
  int whoami = mds->get_nodeid();
  set<CInode*> symlinks_resolved;   // keep a list of symlinks we touch to avoid loops

  bool noperm = false;
  if (onfail == MDS_TRAVERSE_DISCOVER) noperm = true;

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

  int depth = 0;
  while (depth < path.depth()) {
	dout(12) << "traverse: path seg depth " << depth << " = " << path[depth] << endl;
	
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
	if (cur->dir->is_frozen()) {
	  // doh!
	  // FIXME: traverse is allowed?
	  dout(7) << "traverse: " << *cur->dir << " is frozen, waiting" << endl;
	  cur->dir->add_waiter(CDIR_WAIT_UNFREEZE, ondelay);
	  if (onfinish) delete onfinish;
	  return 1;
	}
	
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

	// xlocked by me?
	if (dn && !dn->inode && dn->xlockedby == req) {   // hack?
	  trace.push_back(dn);
	  break; // done!
	}

	if (dn && dn->inode) {
	  // have it.  locked?
	  if (!noperm && dn->is_xlockedbyother(req)) {
		dout(10) << "traverse: xlocked dentry at " << *dn << endl;
		cur->dir->add_waiter(CDIR_WAIT_DNREAD,
							 path[depth],
							 ondelay);
		if (onfinish) delete onfinish;
		return 1;
	  }

	  if (dn->inode->is_symlink() &&
		  (follow_trailing_symlink || depth < path.depth()-1)) {
		// symlink, resolve!
		filepath sym = dn->inode->symlink;
		dout(10) << "traverse: hit symlink " << *dn->inode << " to " << sym << endl;

		/* wrong:
		  if (symlinks_resolved.count(dn->inode)) {
			dout(10) << "already hit this symlink, bailing to avoid a loop" << endl;
			return -NOENT; // ??
		  }
		*/

		// start at root?
		if (dn->inode->symlink[0] == '/') {
		  trace.clear();
		  depth = 0;
		  path = sym;
		  continue;
		}

		// incremental
		filepath head = path.prefixpath(depth);
		filepath tail = path.postfixpath(depth+1);
		dout(10) << "traverse: path head = " << head << endl;
		dout(10) << "traverse: path tail = " << tail << endl;
		path = head;
		path.append(sym);
		path.append(tail);
		dout(10) << "traverse: modified path now " << path << " depth " << depth << endl;
		continue;		
	  } else {
		// keep going.
		trace.push_back(dn);
		cur = dn->inode;
		depth++;
		continue;
	  }
	}
	
	// don't have it.
	int dauth = cur->dir->dentry_authority( path[depth] );
	dout(12) << "traverse: miss on dentry " << path[depth] << " dauth " << dauth << " in " << *cur->dir << endl;
	

	if (dauth == whoami) {
	  // mine.
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
		lru.lru_touch(cur);  // touch readdiree
		mds->mdstore->fetch_dir(cur->dir, ondelay);
		
		mds->logger->inc("cmiss");
		mds->logger->inc("rdir");

		if (onfinish) delete onfinish;
		return 1;
	  }
	} else {
	  // not mine.
	  
	  if (onfail == MDS_TRAVERSE_DISCOVER) {
		// discover

		filepath want = path.postfixpath(depth);
		if (cur->dir->waiting_for(CDIR_WAIT_DENTRY, path[depth])) {
		  dout(7) << "traverse: already waiting for discover on " << *cur << " for " << want.get_path() << " to mds" << dauth << endl;
		} else {
		  dout(7) << "traverse: discover on " << *cur << " for " << want.get_path() << " to mds" << dauth << endl;
		  
		  lru.lru_touch(cur);  // touch discoveree
		
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
		dout(7) << "traverse: not auth for " << path[depth] << ", fwd to mds" << dauth << endl;
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
		  onfinish->finish(-ENOENT);  // -ENOENT, but only because i'm not the authority
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
		dout(7) << "path_pin can't pin " << *dn << ", waiting" << endl;
		dn->dir->add_waiter(CDIR_WAIT_DNREAD,
							dn->name,
							c);
	  } else {
		dout(7) << "path_pin can't pin, no waiter, failing." << endl;
	  }
	  return false;
	}
  }

  // pin!
  for (vector<CDentry*>::iterator it = trace.begin();
	   it != trace.end();
	   it++) {
	(*it)->pin(m);
	dout(10) << "path_pinned " << *(*it) << endl;
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
	dout(10) << "path_unpinned " << *dn << endl;

	// did we completely unpin a waiter?
	if (dn->lockstate == DN_LOCK_UNPINNING && !dn->is_pinned())
	  dn->dir->take_waiting(CDIR_WAIT_DNUNPINNED, dn->name, mds->finished_queue);
  }
}


void MDCache::make_trace(vector<CDentry*>& trace, CInode *in)
{
  CInode *parent = in->get_parent_inode();
  if (parent) {
	make_trace(trace, parent);

	CDentry *dn = in->get_parent_dn();
	dout(10) << "make_trace adding " << *dn << endl;
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
  
  return true;
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
  if (active_requests[req].xlocks.size()) {
	set<CDentry*> dns = active_requests[req].foreign_xlocks;
	active_requests[req].foreign_xlocks.clear();
	
	for (set<CDentry*>::iterator it = dns.begin();
		 it != dns.end();
		 it++) {
	  CDentry *dn = *it;
	  
	  dout(7) << "request_cleanup sending unxlock for foreign xlock on " << *dn << endl;
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
  
  // remove from map
  active_requests.erase(req);
}

void MDCache::request_finish(Message *req)
{
  dout(7) << "request_finish " << *req << endl;
  request_cleanup(req);
  delete req;  // delete req
  
  //dump();
}


void MDCache::request_forward(Message *req, int who)
{
  dout(7) << "request_forward to " << who << " req " << *req << endl;
  request_cleanup(req);
  mds->messenger->send_message(req,
							   MSG_ADDR_MDS(who), MDS_PORT_SERVER,
							   MDS_PORT_SERVER);
}



// rename crap




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

	if (!root->is_cached_by_anyone())
	  root->replicate_relax_locks();


    // add root
    reply = new MDiscoverReply(0);
    reply->add_inode( new CInodeDiscover( root, 
										  root->cached_by_add( dis->get_asker() ) ) );
	dout(10) << "added root " << *root << endl;

    cur = root;
    
  } else {
    // there's a base inode
    cur = get_inode(dis->get_base_ino());
    assert(cur);

    /*string p;
	cur->make_path(p);
	p += "/";
	p += dis->get_want().get_path();
	fullpath = p;
	*/

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

    cur->get_or_open_dir(mds);
    assert(cur);

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
  for (int i = 0; i < dis->get_want().depth() || dis->get_want().depth() == 0; i++) {
    // add dir
    if (reply->is_empty() && !dis->wants_base_dir()) {
      dout(7) << "they don't want the base dir" << endl;
    } else {
      // add dir
      if (!cur->dir_is_auth()) {
		dout(7) << *cur << " dir auth is someone else, i'm done" << endl;
        break;
      }
	  
	  cur->get_or_open_dir(mds);
	  
	  // frozen?
	  /* hmmm do we care, actually?
	  if (dir->is_frozen()) {
        dout(7) << *dir << " frozen, waiting" << endl;
		dir->add_waiter(new C_MDS_RetryMessage( dis, mds ));
		delete reply;
		return;
	  }
	  */

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
		reply->set_flag_error( dis->get_dentry(i) );
		break;   // don't replicate null but non-locked dentries.
	  }
	  
	  reply->add_dentry( dis->get_dentry(i), !dn->can_read() );
	  dout(7) << "added dentry " << *dn << endl;
	  
	  if (!dn->inode) break;  // we're done.
	}

	if (dn && dn->inode) {
		CInode *next = dn->inode;
        assert(next->is_auth());

		// relax inode lock before we replicate?
		if (!next->is_cached_by_anyone()) {
		  next->replicate_relax_locks();
		}

        // add inode
		int nonce = next->cached_by_add(dis->get_asker());
		reply->add_inode( new CInodeDiscover(next, 
											 nonce) );
		dout(7) << "added inode " << *next << " nonce=" << nonce<< endl;

		// descend
		cur = next;
    } else {
      // don't have inode?
      if (cur->dir->is_complete()) {
        // set error flag in reply
        dout(7) << "mds" << whoami << " dentry " << dis->get_dentry(i) << " not found in " << *cur->dir << ", returning error" << endl;
		reply->set_flag_error( dis->get_dentry(i) );
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
	  dout(7) << "discover_reply don't have base ino " << m->get_base_ino() << ", dropping" << endl;
	  delete m;
	  return;
	}
	
	dout(7) << "discover_reply " << *cur << " + " << m->get_path() << ", have " << m->get_num_inodes() << " inodes" << endl;
  }

  // fyi
  if (m->is_flag_error()) dout(7) << " flag error, dentry = " << m->get_error_dentry() << endl;
  

  // loop over discover results.
  // indexese follow each ([[dir] dentry] inode) 
  // can start, end with any type.
  
  for (int i=m->has_root(); i<m->get_depth(); i++) {
	
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
	if (m->is_flag_error()) {
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
	dout(7) << "inode_update on " << m->get_ino() << ", don't have it, sending expire" << endl;
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
  map<int, MCacheExpire*> proxymap;
  
  if (m->get_from() == m->get_source()) {
	dout(7) << "cache_expire from " << from << endl;
  } else {
	dout(7) << "cache_expire from " << from << " via " << m->get_source() << endl;
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
	  int newauth = ino_proxy_auth(in->ino(), 
								   from,
								   export_proxy_inos);
	  dout(7) << "proxy inode expire on " << *in << " to " << newauth << endl;
	  in->is_proxy();
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
	  
	  // fix locks
	  if (in->hardlock.is_gathering(from)) {
		in->hardlock.gather_set.erase(from);
		if (in->hardlock.gather_set.size() == 0)
		  inode_hard_eval(in);
	  }
	  if (in->softlock.is_gathering(from)) {
		in->softlock.gather_set.erase(from);
		if (in->softlock.gather_set.size() == 0)
		  inode_soft_eval(in);
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
	  int newauth = ino_proxy_auth(dir->ino(), 
								   from,
								   export_proxy_dirinos);
	  dout(7) << "proxy dir expire on " << *dir << " to " << newauth << endl;
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


void MDCache::handle_inode_writer_closed(MInodeWriterClosed *m)
{
  CInode *in = get_inode(m->get_ino());
  assert(in);
  assert(in->is_auth() || in->is_proxy());
  
  int from = m->get_from();

  if (in->is_proxy()) {
	int newauth = ino_proxy_auth(in->ino(), 
								 from,
								 export_proxy_inos);
	assert(newauth >= 0);
	dout(7) << "handle_inode_writer_closed " << m->get_ino() << " from " << from << ": proxy, fw to " << newauth << endl;
	mds->messenger->send_message(m,
								 MSG_ADDR_MDS(newauth), MDS_PORT_CACHE,
								 MDS_PORT_CACHE);
	return;
  }

  dout(7) << "handle_inode_wrtier_closed " << *in << " from " << from << endl;

  // remove from my set
  in->remove_replica_writer(m->get_from());
  inode_soft_eval(in);

  delete m;
}



int MDCache::send_dir_updates(CDir *dir, int except)
{
  // this is an FYI, re: replication

  int whoami = mds->get_nodeid();
  for (set<int>::iterator it = dir->open_by_begin(); 
	   it != dir->open_by_end(); 
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
  if (!in || !in->dir) {
	dout(7) << "dir_update on " << m->get_ino() << ", don't have it" << endl;
	goto out;
  }

  // update
  dout(7) << "dir_update on " << m->get_ino() << endl;
  in->dir->dir_rep = m->get_dir_rep();
  in->dir->dir_rep_by = m->get_dir_rep_by();
  
  // done
 out:
  delete m;
}







// NAMESPACE FUN

void MDCache::dentry_unlink(CDentry *dn, Context *c)
{
  CDir *dir = dn->dir;
  string dname = dn->name;

  assert(dn->lockstate == DN_LOCK_XLOCK);

  // log it
  if (dn->inode) dn->inode->mark_unsafe();   // XXX ??? FIXME
  mds->mdlog->submit_entry(new EUnlink(dir, dn),
						   c);

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

  if (dn->inode && dn->inode->dir) {
	// mark dir clean, since it dne!
	dn->inode->dir->remove_null_dentries();
	dn->inode->dir->mark_clean();
  }

  // unlink
  dn->dir->unlink_inode( dn );
  dn->mark_dirty();

  // unpin dir / unxlock
  dentry_xlock_finish(dn, true); // quiet, no need to bother replicas since they're already unlinking

  // did i empty out an imported dir?
  if (dir->is_import() && !dir->inode->is_root() && dir->get_size() == 0) 
	export_empty_import(dir);

  // wake up any waiters
  //dir->finish_waiting(CDIR_WAIT_ANY, dname);
  dir->take_waiting(CDIR_WAIT_ANY, dname, mds->finished_queue);
}


void MDCache::handle_dentry_unlink(MDentryUnlink *m)
{
  CInode *diri = get_inode(m->get_dirino());
  CDir *dir;
  if (diri) dir = diri->dir;
  if (!diri || !dir) {
	dout(7) << "handle_dentry_unlink don't have dir " << m->get_dirino() << endl;
	delete m;
	return;
  }
  
  CDentry *dn = dir->lookup(m->get_dn());
  if (!dn) {
	dout(7) << "handle_dentry_unlink don't have dentry " << *dir << " dn " << m->get_dn() << endl;
  } else {
	dout(7) << "handle_dentry_unlink on " << *dn << endl;

	// dir?
	if (dn->inode && dn->inode->dir)
	  dn->inode->dir->remove_null_dentries();

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




// renaming!

/*
class C_MDC_RenameLocalFile : public Context {
  MDCache *mdc;
  CInode *from, *oldin;
  CDir *destdir;
  Context *c;
public:
  C_MDC_RenameLocalFile(MDCache *mdc,
						CInode *from,
						CDir *destdir,
						CInode *oldin,
						Context *c) {
	this->mdc = mdc;
	this->from = from;
	this->destdir = destdir;
	this->oldin = oldin;
	this->c = c;
  }
  void finish(int r) {
	mdc->file_rename_finish(from, destdir, oldin, c);
  }
};
*/

void MDCache::file_rename(CDentry *srcdn, CDentry *destdn, Context *c, bool everyone)
{
  assert(srcdn->is_xlocked());  // by me
  assert(destdn->is_xlocked());  // by me

  CDir *srcdir = srcdn->dir;
  string srcname = srcdn->name;
  bool srcauth = srcdir->dentry_authority(srcdn->name) == mds->get_nodeid();

  CDir *destdir = destdn->dir;
  string destname = destdn->name;
  bool destauth = destdir->dentry_authority(destdn->name) == mds->get_nodeid();
  
  CInode *in = srcdn->inode;
  Message *req = srcdn->xlockedby;

  // foreign rename?
  if (srcauth && !destauth) {
	dout(7) << "file_rename src auth, not dest auth.  sending MRename" << endl;
	
	file_rename_foreign_src(srcdn, destdn, 
							mds->get_nodeid());  // i'm the initiator

	// set waiter on the inode (is this the best place?)
	in->add_waiter(CINODE_WAIT_RENAME, c);
	return;
  }
  else if (!srcauth) {
	if (destauth) {
	  dout(7) << "file_rename dest auth, not src auth.  sending MRenameReq" << endl;	
	} else {
	  dout(7) << "file_rename neither src auth nor dest auth.  sending MRenameReq" << endl;	
	}
	
	MRenameReq *m = new MRenameReq(mds->get_nodeid(),  // i'm the initiator
								   srcdir->ino(), srcname, destdir->ino(), destname);
	int srcauth = srcdir->dentry_authority(srcdn->name);
	mds->messenger->send_message(m,
								 MSG_ADDR_MDS(srcauth), MDS_PORT_CACHE, MDS_PORT_CACHE);

	// set waiter on the inode (is this the best place?)
	in->add_waiter(CINODE_WAIT_RENAME, c);
	return;
  } 

  assert(srcauth && destauth);
  dout(7) << "file_rename src and dest auth, renaming locally (easy!)" << endl;
  
  // update our cache
  rename_file(srcdn, destdn);
  
  // mark dentries dirty
  srcdn->mark_dirty();
  destdn->mark_dirty();
  
  // update imports/exports?
  if (in->is_dir() && in->dir) 
	fix_renamed_dir(srcdir, in, destdir, false);  // auth didnt change

  
  // tell replicas (no need to wait for ack) to do the same, and un-xlock.
  // make list
  set<int> who;
  who = srcdir->get_open_by();
  for (set<int>::iterator it = destdir->open_by_begin();
	   it != destdir->open_by_end();
	   it++)
	if (who.count(*it) == 0) who.insert(*it);

  // tell
  string destdirpath;
  destdir->inode->make_path(destdirpath);

  for (set<int>::iterator it = who.begin();
	   it != who.end();
	   it++) {
	mds->messenger->send_message(new MRenameNotify(srcdir->ino(),
												   srcname,
												   destdir->ino(),
												   destdirpath,
												   destname),
								 MSG_ADDR_MDS(*it), MDS_PORT_CACHE, MDS_PORT_CACHE);
  }

  // log it
  // ***

  // did i empty out an imported dir?
  if (srcdir->is_import() && !srcdir->inode->is_root() && srcdir->get_size() == 0) 
	export_empty_import(srcdir);

  // clean up xlocks
  dentry_xlock_finish(srcdn);
  dentry_xlock_finish(destdn);

  // finish our query off for now.  XXX FIXME (should wait for log?)
  if (c) {
	c->finish(0);
	delete c;
  }

  // other waiters
  srcdir->take_waiting(CDIR_WAIT_ANY, srcname, mds->finished_queue);
  destdir->take_waiting(CDIR_WAIT_DNREAD, destname, mds->finished_queue);
}

void MDCache::file_rename_foreign_src(CDentry *srcdn, CDentry *destdn, int initiator)
{
  dout(7) << "file_rename_foreign_src " << *srcdn << " to " << *destdn << endl;

  CDir *srcdir = srcdn->dir;
  CDir *destdir = destdn->dir;
  string srcname = srcdn->name;

  // (we're basically exporting this inode)
  CInode *in = srcdn->inode;
  assert(in);
  
  // encode and export inode state
  crope inode_state;
  encode_export_inode(in, inode_state);
  
  MRename *m = new MRename(initiator,
						   srcdir->ino(), srcdn->name, destdir->ino(), destdn->name,
						   inode_state);
  int destauth = destdir->dentry_authority(destdn->name);
  mds->messenger->send_message(m,
							   MSG_ADDR_MDS(destauth), MDS_PORT_CACHE, MDS_PORT_CACHE);

  // update our cache
  rename_file(srcdn, destdn);
  srcdn->mark_dirty();
	
  // update imports/exports?
  if (in->is_dir() && in->dir) 
	fix_renamed_dir(srcdir, in, destdir, false);  // auth didnt change

  /* let the initiator do this.
  // drop xlock on src
  dentry_xlock_finish(srcdn);
  srcdir->take_waiting(CDIR_WAIT_ANY, srcname, mds->finished_queue);
  */
}

void MDCache::handle_rename_req(MRenameReq *m)
{
  CInode *srcdiri = get_inode(m->get_srcdirino());
  CDir *srcdir = srcdiri->dir;
  CDentry *srcdn = srcdir->lookup(m->get_srcname());
  assert(srcdn);

  CInode *destdiri = get_inode(m->get_destdirino());
  CDir *destdir = destdiri->dir;
  CDentry *destdn = destdir->lookup(m->get_destname());
  assert(destdn);

  file_rename_foreign_src(srcdn, destdn, m->get_initiator());
  delete m;
}

void MDCache::handle_rename(MRename *m)
{
  CInode *srcdiri = get_inode(m->get_srcdirino());
  CDir *srcdir = srcdiri->dir;
  CDentry *srcdn = srcdir->lookup(m->get_srcname());
  assert(srcdn);

  CInode *destdiri = get_inode(m->get_destdirino());
  CDir *destdir = destdiri->dir;
  CDentry *destdn = destdir->lookup(m->get_destname());
  assert(destdn);
  string destname = destdn->name;

  dout(7) << "handle_rename " << *srcdn << " to " << *destdn << endl;

  // decode + import inode (into _old_ location to start)
  int off = 0;
  decode_import_inode(srcdn, m->get_inode_state(), off, m->get_source());
  
  CInode *in = srcdn->inode;
  assert(in);

  // rename it
  rename_file(srcdn, destdn);
  destdn->mark_dirty();

  // update imports/exports?
  if (in->is_dir() && in->dir) 
	fix_renamed_dir(srcdir, in, destdir, false);  // auth didnt change

  // send notifies to everyone (since it's hard to safely merge open_by from src+dest)
  // include src.  but not me, or initiator.
  set<int> who;
  for (int i=0; i<mds->get_cluster()->get_num_mds(); i++)
	if (i != mds->get_nodeid() &&
		i != m->get_initiator()) who.insert(i);	
  
  string destdirpath;
  destdir->inode->make_path(destdirpath);

  for (set<int>::iterator it = who.begin();
	   it != who.end();
	   it++) {
	mds->messenger->send_message(new MRenameNotify(srcdir->ino(),
												   srcdn->name,
												   destdir->ino(),
												   destdirpath,
												   destdn->name),
								 MSG_ADDR_MDS(*it), MDS_PORT_CACHE, MDS_PORT_CACHE);
  }

  // ok, tell the initiator
  if (m->get_initiator() == mds->get_nodeid()) {
	// it's me!
	in->take_waiting(CINODE_WAIT_RENAME, mds->finished_queue);
  } else {
	MRenameAck *ack = new MRenameAck(srcdir->ino(), srcdn->name, destdir->ino(), destdn->name);
	mds->messenger->send_message(ack,
								 MSG_ADDR_MDS(m->get_initiator()), MDS_PORT_CACHE, MDS_PORT_CACHE);
  }


  /* let the initiator do this.
  // drop xlock on dst
  dentry_xlock_finish(destdn);
  destdir->take_waiting(CDIR_WAIT_DNREAD, destname, mds->finished_queue);
  */

  delete m;
}


void MDCache::handle_rename_ack(MRenameAck *m)
{
  CInode *destdiri = get_inode(m->get_destdirino());
  CDir *destdir = destdiri->dir;
  CDentry *destdn = destdir->lookup(m->get_destname());
  assert(destdn);
  CInode *in = destdn->inode;
  
  dout(7) << "handle_rename_ack on " << *in << endl;

  // all done!
  in->take_waiting(CINODE_WAIT_RENAME, mds->finished_queue);

  delete m;
}


void MDCache::handle_rename_notify(MRenameNotify *m)
{
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

	if (!destdn) {
	  destdn = destdir->add_dentry(m->get_destname());  // create null dentry
	  destdn->lockstate = DN_LOCK_XLOCK;                // that's xlocked!
	  assert(!srcdn->inode->is_dir());
	}

	dout(7) << "handle_rename_notify renaming " << *srcdn << " to " << *destdn << endl;
	
	if (in) {
	  rename_file(srcdn, destdn);
	}
	
	// remove src dentry
	srcdn->dir->remove_dentry(srcdn);
	
	// update imports/exports?
	if (in && in->is_dir() && in->dir) 
	  fix_renamed_dir(srcdir, in, destdir, false);  // auth didnt change
	
	srcdir->take_waiting(CDIR_WAIT_ANY, m->get_srcname(), finished);
	destdir->take_waiting(CDIR_WAIT_ANY, m->get_destname(), finished);
  }

  else if (srcdn) {
	if (srcdn->inode && srcdn->inode->dir) {
	  dout(7) << "handle_rename_notify no dest, but src is an open dir, discovering destdir.  srcdn is " << *srcdn << endl;
	  
	  vector<CDentry*> trace;
	  filepath destdirpath = m->get_destdirpath();
	  int r = path_traverse(destdirpath, trace, true,
							m, new C_MDS_RetryMessage(mds, m), 
							MDS_TRAVERSE_DISCOVER);
	  assert(r>0);
	  return;
	} else {
	  dout(7) << "handle_rename_notify unlinking src only " << *srcdn << endl;
	  assert(!srcdn->inode->is_dir());
	  srcdir->remove_dentry(srcdn);  // will leave inode dangling.
	  srcdir->take_waiting(CDIR_WAIT_ANY, m->get_srcname(), finished);
	}
  }

  else if (destdn) {
	dout(7) << "handle_rename_notify unlinking dst only " << *destdn << endl;
	destdir->remove_dentry(destdn);
	destdir->take_waiting(CDIR_WAIT_ANY, m->get_destname(), finished);
  }
  
  else {
	dout(7) << "handle_rename_notify didn't have srcdn or destdn" << endl;
	assert(srcdn == 0 && destdn == 0);
  }
  
  mds->queue_finished(finished);

  delete m;
}









// locks ----------------------------------------------------------------

/*

LOCKS:

 three states:

   Auth   Replica   State
    R       R       normal/sync    fw writes to auth
    RW      -       lock           ping auth for R/W?
    W       W       async (*)      fw reads to auth  
  
    * only defined for soft inode metadata, right?

 we also remember:
   auth:
    set<int> replicas 
    bool req_r, req_w
    
   replica:
    last_sync - stamp of last time we were sync



   


INODES:

= two types of inode metadata:
   hard  - uid/gid, mode
   soft  - mtime, size
 ? atime - atime  (*)       <-- we want a lazy update strategy?

   * if we want _strict_ atime behavior, atime can be folded into soft.  
     for lazy atime, should we just leave the atime lock in async state?  XXX

= correspondingly, two types of inode locks:
   hardlock - hard metadata
   softlock - soft metadata

   -> These locks are completely orthogonal! 

= metadata ops and how they affect inode metadata:
        sma=size mtime atime
   HARD SOFT OP
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
	
  case LOCK_OTYPE_ISOFT:
	handle_lock_inode_soft(m);
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
  assert(in->hardlock.can_read(in->is_auth()) ||
		 in->hardlock.could_read(in->is_auth()));
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
	dout(5) << "inode_hard_write_start " << *in << " on replica, fw to auth " << auth << endl;
	assert(auth != mds->get_nodeid());
	request_forward(m, auth);
	return false;
  }
}


void MDCache::inode_hard_write_finish(CInode *in)
{
  // drop ref
  assert(in->hardlock.can_write(in->is_auth()) ||
		 in->hardlock.could_write(in->is_auth()));
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
	  in->hardlock.gather_set.size() == 0) {
	dout(7) << "inode_hard_eval finished gather on " << *in << endl;
	switch (in->hardlock.get_state()) {
	case LOCK_PRELOCK:
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
  if (in->hardlock.get_state() == LOCK_PRELOCK) 
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
	  in->hardlock.get_state() == LOCK_PRELOCK) 
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
  in->hardlock.set_state(LOCK_PRELOCK);
  in->hardlock.init_gather(in->get_cached_by());
}





// messenger

void MDCache::handle_lock_inode_hard(MLock *m)
{
  assert(m->get_otype() == LOCK_OTYPE_IHARD);
  
  int from = m->get_asker();
  CInode *in = get_inode(m->get_ino());
  
  if (LOCK_AC_FOR_AUTH(m->get_action())) {
	// auth
	assert(in);
	assert(in->is_auth() || in->is_proxy());
	dout(7) << "handle_lock_inode_hard " << *in << " hardlock=" << in->hardlock << endl;  

	if (in->is_proxy()) {
	  // fw
	  int newauth = ino_proxy_auth(in->ino(), 
								   from,
								   export_proxy_inos);
	  assert(newauth >= 0);
	  dout(7) << "handle_lock " << m->get_ino() << " from " << from << ": proxy, fw to " << newauth << endl;
	  mds->messenger->send_message(m,
								   MSG_ADDR_MDS(newauth), MDS_PORT_CACHE,
								   MDS_PORT_CACHE);
	  return;
	}
  } else {
	// replica
	if (!in) {
	  dout(7) << "handle_lock_inode_hard " << m->get_ino() << ": don't have it anymore" << endl;
	  /*
	   * do NOT nak.. if we go that route we need ot duplicate all the nonce funkiness
	     to keep gather_set a proper/correct subset of cached_by.  better to use the existing
		 cacheexpire mechanism.
	  */
	  /*
		MLock *reply = new MLock(m->get_action() + 3, mds->get_nodeid());
		reply->set_ino(in->ino(), LOCK_OTYPE_IHARD);
		mds->messenger->send_message(reply,
		                             MSG_ADDR_MDS(m->get_asker()), MDS_PORT_CACHE,
                            		 MDS_PORT_CACHE);
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
	assert(lock->get_state() == LOCK_SYNC ||
		   lock->get_state() == LOCK_WLOCKR);
	
	// wait for readers to finish?
	if (lock->get_nread() > 0) {
	  dout(7) << "handle_lock_inode_hard readers, waiting before ack on " << *in << endl;
	  lock->set_state(LOCK_WLOCKR);
	  in->add_waiter(CINODE_WAIT_HARDNORD,
					 new C_MDS_RetryMessage(mds,m));
	  assert(0);  // does this every happen?  (if so, fix hard_read_finish!)
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
  case LOCK_AC_LOCKNAK:
	// do NOT remove from cached_by; we don't know the nonce! 
	// and somewhere out there there's an expire that will take care of it.
	
  case LOCK_AC_LOCKACK:
	assert(lock->state == LOCK_PRELOCK);
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


bool MDCache::inode_soft_read_start(CInode *in, MClientRequest *m)
{
  dout(7) << "inode_soft_read_start " << *in << " softlock=" << in->softlock << endl;  

  if (in->is_auth() && 
	  !in->softlock.can_read(in->is_auth()) &&
	  !in->is_cached_by_anyone()) {
	in->softlock.set_state(LOCK_LOCK); // twiddle lock at will
  }

  // can read?  grab ref.
  if (in->softlock.can_read(in->is_auth())) {
	in->softlock.get_read();
	return true;
  }
  
  // can't read, and replicated.
  if (in->softlock.can_read_soon(in->is_auth())) {
	// wait
	dout(7) << "inode_soft_read_start can_read_soon " << *in << endl;
  } else {	
	if (in->is_auth()) {
	  // auth

	  // FIXME or qsync?

	  if (in->softlock.is_stable()) {
		assert(in->softlock.get_state() == LOCK_ASYNC);  // should be async!
		inode_soft_lock(in);     // lock, easier to back off
	  } else {
		dout(7) << "inode_soft_read_start waiting until stable on " << *in << ", softlock=" << in->softlock << endl;
		in->add_waiter(CINODE_WAIT_SOFTSTABLE, new C_MDS_RetryRequest(mds, m, in));
		return false;
	  }
	} else {
	  // replica
	  if (in->softlock.is_stable()) {
		if (in->softlock.get_mode() == LOCK_MODE_ASYNC) {
		  // fw to auth
		  int auth = in->authority();
		  dout(5) << "inode_soft_read_start " << *in << " on replica and async, fw to auth " << auth << endl;
		  assert(auth != mds->get_nodeid());
		  request_forward(m, auth);
		  return false;
		} else {
		  // wait.
		  // recall maybe?
		}
		
	  } else {
		// wait until stable
		dout(7) << "inode_soft_read_start waiting until stable on " << *in << ", softlock=" << in->softlock << endl;
		in->add_waiter(CINODE_WAIT_SOFTSTABLE, new C_MDS_RetryRequest(mds, m, in));
		return false;
	  }
	}
  }

  // wait
  dout(7) << "inode_soft_read_start waiting on " << *in << ", softlock=" << in->softlock << endl;
  in->add_waiter(CINODE_WAIT_SOFTR, new C_MDS_RetryRequest(mds, m, in));
		
  return false;
}


void MDCache::inode_soft_read_finish(CInode *in)
{
  // drop ref
  assert(in->softlock.can_read(in->is_auth()) ||
		 in->softlock.could_read(in->is_auth()));
  in->softlock.put_read();

  dout(7) << "inode_soft_read_finish on " << *in << ", softlock=" << in->softlock << endl;

  if (in->softlock.get_nread() == 0) {
	in->finish_waiting(CINODE_WAIT_SOFTNORD);
	inode_soft_eval(in);
  }
}


bool MDCache::inode_soft_write_start(CInode *in, MClientRequest *m)
{
  // if no replicated, i can twiddle lock at will
  if (in->is_auth() &&
	  !in->is_cached_by_anyone() &&
	  in->softlock.get_state() != LOCK_LOCK) 
	in->softlock.set_state(LOCK_LOCK);
  
  // can write?  grab ref.
  if (in->softlock.can_write(in->is_auth())) {
	in->softlock.get_write();
	return true;
  }
  
  // can't write, replicated.
  if (in->is_auth()) {
	// auth
	if (in->softlock.can_write_soon(in->is_auth())) {
	  // just wait
	} else {
	  // initiate lock 
	  // OR async ....... FIXME
	  inode_soft_lock(in);
	}
	
	dout(7) << "inode_soft_write_start on auth, waiting for write on " << *in << endl;
	in->add_waiter(CINODE_WAIT_SOFTW, new C_MDS_RetryRequest(mds, m, in));

	return false;
  } else {
	// replica

	if (in->softlock.get_mode() == LOCK_MODE_ASYNC) {
	  // wait
	  dout(5) << "inode_soft_write_start " << *in << " on replica, sync but mode async, waiting " << endl;
	  in->add_waiter(CINODE_WAIT_SOFTW, new C_MDS_RetryRequest(mds, m, in));
	  return false;
	} else {
	  // fw to auth
	  int auth = in->authority();
	  dout(5) << "inode_soft_write_start " << *in << " on replica, fw to auth " << auth << endl;
	  assert(auth != mds->get_nodeid());
	  request_forward(m, auth);
	  return false;
	}
  }
 
}


void MDCache::inode_soft_write_finish(CInode *in)
{
  // drop ref
  assert(in->softlock.can_write(in->is_auth()) ||
		 in->softlock.could_write(in->is_auth()));
  in->softlock.put_write();
  dout(7) << "inode_soft_write_finish on " << *in << ", softlock=" << in->softlock << endl;
  
  // drop lock?
  if (in->softlock.get_nwrite() == 0) {
	in->finish_waiting(CINODE_WAIT_SOFTNOWR);
	inode_soft_eval(in);
  }
}


void MDCache::inode_soft_eval(CInode *in)
{
  // finished gather?
  if (in->is_auth() &&
	  !in->softlock.is_stable() &&
	  in->softlock.gather_set.size() == 0) {
	dout(7) << "inode_soft_eval finished gather on " << *in << endl;
	switch (in->softlock.get_state()) {
	case LOCK_PRELOCK:
	  in->softlock.set_state(LOCK_LOCK);
	  
	  // waiters
	  in->softlock.get_read();
	  in->softlock.get_write();
	  in->finish_waiting(CINODE_WAIT_SOFTRWB|CINODE_WAIT_SOFTSTABLE);
	  in->softlock.put_read();
	  in->softlock.put_write();
	  break;
	  
	case LOCK_GASYNC:
	  in->softlock.set_state(LOCK_ASYNC);

	  for (set<int>::iterator it = in->cached_by_begin(); 
		   it != in->cached_by_end(); 
		   it++) {
		MLock *reply = new MLock(LOCK_AC_ASYNC, mds->get_nodeid());
		reply->set_ino(in->ino(), LOCK_OTYPE_ISOFT);
		mds->messenger->send_message(reply,
									 MSG_ADDR_MDS(*it), MDS_PORT_CACHE,
									 MDS_PORT_CACHE);
	  }
	  
	  // waiters
	  in->softlock.get_write();
	  in->finish_waiting(CINODE_WAIT_SOFTW|CINODE_WAIT_SOFTSTABLE);
	  in->softlock.put_write();
	  break;
	  
	case LOCK_GSYNC:
	  in->softlock.set_state(LOCK_SYNC);

	  { // bcast data to replicas
		crope softdata;
		in->encode_soft_state(softdata);
		
		for (set<int>::iterator it = in->cached_by_begin(); 
			 it != in->cached_by_end(); 
			 it++) {
		  MLock *reply = new MLock(LOCK_AC_SYNC, mds->get_nodeid());
		  reply->set_ino(in->ino(), LOCK_OTYPE_ISOFT);
		  reply->set_data(softdata);
		  mds->messenger->send_message(reply,
									   MSG_ADDR_MDS(*it), MDS_PORT_CACHE,
									   MDS_PORT_CACHE);
		}
	  }
	  
	  // waiters
	  in->softlock.get_read();
	  in->finish_waiting(CINODE_WAIT_SOFTR|CINODE_WAIT_SOFTSTABLE);
	  in->softlock.put_read();
	  break;
	  
	case LOCK_GLOCK:
	  in->softlock.set_state(LOCK_LOCK);
	  
	  // waiters
	  in->softlock.get_read();
	  in->softlock.get_write();
	  in->finish_waiting(CINODE_WAIT_SOFTRWB|CINODE_WAIT_SOFTSTABLE);
	  in->softlock.put_read();
	  in->softlock.put_write();
	  break;
	  
	default: 
	  assert(0);
	}
  }
  if (!in->softlock.is_stable()) return;  // do nothing
  
  if (in->is_auth()) {
	// auth
	
	// check our mode
	if ((in->is_open_write() || in->num_replica_writers()) &&
		in->softlock.get_mode() != LOCK_MODE_ASYNC) {
	  inode_soft_mode(in,LOCK_MODE_ASYNC);
	}
	
	if (!(in->is_open_write() || in->num_replica_writers()) &&
		in->softlock.get_mode() != LOCK_MODE_SYNC) {
	  inode_soft_mode(in,LOCK_MODE_SYNC);
	}

	// check our state
	if (in->softlock.get_mode() == LOCK_MODE_SYNC) {
	  // sync mode.  bump state to sync?
	  if (in->is_cached_by_anyone() &&
		  in->softlock.get_nwrite() == 0 &&
		  in->softlock.get_state() != LOCK_SYNC) {
		dout(7) << "inode_soft_eval stable, syncing " << *in << ", softlock=" << in->softlock << endl;
		inode_soft_sync(in);
	  }
	}
	
	else if (in->softlock.get_mode() == LOCK_MODE_ASYNC) {
	  // async mode.  bump state to async?
	  if (in->is_cached_by_anyone() &&
		  in->softlock.get_nread() == 0 &&
		  in->softlock.get_state() != LOCK_ASYNC) {
		dout(7) << "inode_soft_eval stable, asyncing " << *in << ", softlock=" << in->softlock << endl;
		inode_soft_async(in);
	  }
	}

  } else {
	// replica
	// recall? check wiaters?  XXX
  }
}

// mid

void MDCache::inode_soft_mode(CInode *in, int mode)
{
  assert(in->is_auth());
  
  in->softlock.set_mode(mode);
  dout(7) << "inode_soft_mode mode=" << mode << " " << *in << " softlock=" << in->softlock << endl;  
  
  // tell replicas
  for (set<int>::iterator it = in->cached_by_begin(); 
	   it != in->cached_by_end(); 
	   it++) {
	int ac;
	switch (mode) {
	case LOCK_MODE_SYNC: ac = LOCK_AC_SYNC_MODE; break;
	case LOCK_MODE_ASYNC: ac = LOCK_AC_ASYNC_MODE; break;
	default: assert(0);
	}
	MLock *m = new MLock(ac, mds->get_nodeid());
	m->set_ino(in->ino(), LOCK_OTYPE_ISOFT);
	mds->messenger->send_message(m,
								 MSG_ADDR_MDS(*it), MDS_PORT_CACHE,
								 MDS_PORT_CACHE);
  }

  // caller shoudl probably eval our state
}

bool MDCache::inode_soft_sync(CInode *in)
{
  dout(7) << "inode_soft_sync " << *in << " softlock=" << in->softlock << endl;  

  assert(in->is_auth());

  // check state
  if (in->softlock.get_state() == LOCK_SYNC) return true;
  if (in->softlock.get_state() == LOCK_GSYNC) return false;

  assert(in->softlock.is_stable());
  if (in->softlock.get_state() == LOCK_PRELOCK ||
	  in->softlock.get_state() == LOCK_GLOCK)
	assert(0);  // hmm!
  assert(in->softlock.get_state() == LOCK_LOCK ||
		 in->softlock.get_state() == LOCK_ASYNC);

  if (in->softlock.get_state() == LOCK_LOCK) {
	// soft data
	crope softdata;
	in->encode_soft_state(softdata);
	
	// bcast to replicas
	for (set<int>::iterator it = in->cached_by_begin(); 
		 it != in->cached_by_end(); 
		 it++) {
	  MLock *m = new MLock(LOCK_AC_SYNC, mds->get_nodeid());
	  m->set_ino(in->ino(), LOCK_OTYPE_ISOFT);
	  m->set_data(softdata);
	  mds->messenger->send_message(m,
								   MSG_ADDR_MDS(*it), MDS_PORT_CACHE,
								   MDS_PORT_CACHE);
	}
	
	// change lock
	in->softlock.set_state(LOCK_SYNC);

	return true;
  }

  else if (in->softlock.get_state() == LOCK_ASYNC) {
	// bcast to replicas
	for (set<int>::iterator it = in->cached_by_begin(); 
		 it != in->cached_by_end(); 
		 it++) {
	  MLock *m = new MLock(LOCK_AC_GSYNC, mds->get_nodeid());
	  m->set_ino(in->ino(), LOCK_OTYPE_ISOFT);
	  mds->messenger->send_message(m,
								   MSG_ADDR_MDS(*it), MDS_PORT_CACHE,
								   MDS_PORT_CACHE);
	}
	
	// change lock
	in->softlock.set_state(LOCK_GSYNC);
	in->softlock.init_gather(in->get_cached_by());

	return false;
  }
  else 
	assert(0); // wtf.
}


void MDCache::inode_soft_lock(CInode *in)
{
  dout(7) << "inode_soft_lock " << *in << " softlock=" << in->softlock << endl;  

  assert(in->is_auth());
  
  // check state
  if (in->softlock.get_state() == LOCK_LOCK ||
	  in->softlock.get_state() == LOCK_PRELOCK ||
	  in->softlock.get_state() == LOCK_GLOCK) 
	return;  // lock or locking
  assert(in->softlock.is_stable());
  if (in->softlock.get_state() == LOCK_GSYNC)
	assert(0);  // hmm!
  assert(in->softlock.get_state() == LOCK_SYNC ||
		 in->softlock.get_state() == LOCK_ASYNC);

  if (in->softlock.get_state() == LOCK_SYNC) {
	// bcast to replicas
	for (set<int>::iterator it = in->cached_by_begin(); 
		 it != in->cached_by_end(); 
		 it++) {
	  MLock *m = new MLock(LOCK_AC_LOCK, mds->get_nodeid());
	  m->set_ino(in->ino(), LOCK_OTYPE_ISOFT);
	  mds->messenger->send_message(m,
								   MSG_ADDR_MDS(*it), MDS_PORT_CACHE,
								   MDS_PORT_CACHE);
	}
	
	// change lock
	in->softlock.set_state(LOCK_PRELOCK);
	in->softlock.init_gather(in->get_cached_by());
  }

  else if (in->softlock.get_state() == LOCK_ASYNC) {
	// bcast to replicas
	for (set<int>::iterator it = in->cached_by_begin(); 
		 it != in->cached_by_end(); 
		 it++) {
	  MLock *m = new MLock(LOCK_AC_GLOCK, mds->get_nodeid());
	  m->set_ino(in->ino(), LOCK_OTYPE_ISOFT);
	  mds->messenger->send_message(m,
								   MSG_ADDR_MDS(*it), MDS_PORT_CACHE,
								   MDS_PORT_CACHE);
	}
	
	// change lock
	in->softlock.set_state(LOCK_GLOCK);
	in->softlock.init_gather(in->get_cached_by());
  }
  else 
	assert(0); // wtf.
}


void MDCache::inode_soft_async(CInode *in)
{
  dout(7) << "inode_soft_async " << *in << " softlock=" << in->softlock << endl;  

  assert(in->is_auth());
  
  // check state
  if (in->softlock.get_state() == LOCK_ASYNC)
	return;     // async
  assert(in->softlock.is_stable());
  if (in->softlock.get_state() == LOCK_GSYNC ||
	  in->softlock.get_state() == LOCK_GLOCK ||
	  in->softlock.get_state() == LOCK_PRELOCK)
	assert(0);  // hmm!
  assert(in->softlock.get_state() == LOCK_SYNC ||
		 in->softlock.get_state() == LOCK_LOCK);

  if (in->softlock.get_state() == LOCK_SYNC) {
	// bcast to replicas
	for (set<int>::iterator it = in->cached_by_begin(); 
		 it != in->cached_by_end(); 
		 it++) {
	  MLock *m = new MLock(LOCK_AC_GASYNC, mds->get_nodeid());
	  m->set_ino(in->ino(), LOCK_OTYPE_ISOFT);
	  mds->messenger->send_message(m,
								   MSG_ADDR_MDS(*it), MDS_PORT_CACHE,
								   MDS_PORT_CACHE);
	}
	
	// change lock
	in->softlock.set_state(LOCK_GASYNC);
	in->softlock.init_gather(in->get_cached_by());
  }

  else if (in->softlock.get_state() == LOCK_LOCK) {
	// data
	crope softdata;
	in->encode_soft_state(softdata);
	
	// bcast to replicas
	for (set<int>::iterator it = in->cached_by_begin(); 
		 it != in->cached_by_end(); 
		 it++) {
	  MLock *m = new MLock(LOCK_AC_ASYNC, mds->get_nodeid());
	  m->set_ino(in->ino(), LOCK_OTYPE_ISOFT);
	  m->set_data(softdata);
	  mds->messenger->send_message(m,
								   MSG_ADDR_MDS(*it), MDS_PORT_CACHE,
								   MDS_PORT_CACHE);
	}
	
	// change lock
	in->softlock.set_state(LOCK_ASYNC);
  }
  else 
	assert(0); // wtf.
}


// messenger

void MDCache::handle_lock_inode_soft(MLock *m)
{
  assert(m->get_otype() == LOCK_OTYPE_ISOFT);
  
  CInode *in = get_inode(m->get_ino());
  int from = m->get_asker();

  if (LOCK_AC_FOR_AUTH(m->get_action())) {
	// auth
	assert(in);
	assert(in->is_auth() || in->is_proxy());
	
	if (in->is_proxy()) {
	  // fw
	  int newauth = ino_proxy_auth(in->ino(), 
								   from,
								   export_proxy_inos);
	  assert(newauth >= 0);
	  dout(7) << "handle_lock " << m->get_ino() << " from " << from << ": proxy, fw to " << newauth << endl;
	  mds->messenger->send_message(m,
								   MSG_ADDR_MDS(newauth), MDS_PORT_CACHE,
								   MDS_PORT_CACHE);
	  return;
	}
  } else {
	// replica
	if (!in) {
	  // nack
	  dout(7) << "handle_lock " << m->get_ino() << ": don't have it anymore" << endl;

	  // DONT NAK
	  /*
	  MLock *reply = new MLock(m->get_action() + LOCK_AC_NAKOFFSET, mds->get_nodeid());
	  reply->set_ino(in->ino(), LOCK_OTYPE_ISOFT);
	  mds->messenger->send_message(reply,
								   MSG_ADDR_MDS(m->get_asker()), MDS_PORT_CACHE,
								   MDS_PORT_CACHE);
	  */
	  delete m;
	  return;
	}
	
	assert(!in->is_auth());
  }

  dout(7) << "handle_lock_inode_soft a=" << m->get_action() << " from " << from << " " << *in << " softlock=" << in->softlock << endl;  
  
  CLock *lock = &in->softlock;
  
  switch (m->get_action()) {
	// -- replica --
  case LOCK_AC_SYNC_MODE:
	lock->set_mode(LOCK_MODE_SYNC);
	in->finish_waiting(CINODE_WAIT_SOFTW);
	break;

  case LOCK_AC_ASYNC_MODE:
	lock->set_mode(LOCK_MODE_ASYNC);
	in->finish_waiting(CINODE_WAIT_SOFTR);
	break;


  case LOCK_AC_SYNC:
	assert(lock->get_state() == LOCK_LOCK ||
		   lock->get_state() == LOCK_GSYNC);
	
	{ // assim data
	  int off = 0;
	  in->decode_soft_state(m->get_data(), off);
	}
	
	// update lock
	lock->set_state(LOCK_SYNC);
	
	// no need to reply.
	
	// waiters
	in->softlock.get_read();
	in->finish_waiting(CINODE_WAIT_SOFTR|CINODE_WAIT_SOFTSTABLE);
	in->softlock.put_read();
	inode_soft_eval(in);
	break;
	
  case LOCK_AC_LOCK:
	assert(lock->get_state() == LOCK_SYNC ||
		   lock->get_state() == LOCK_WLOCKR);
	
	if (lock->get_nread() > 0) {
	  dout(7) << "handle_lock_inode_soft readers, waiting before ack on " << *in << endl;
	  lock->set_state(LOCK_WLOCKR);
	  in->add_waiter(CINODE_WAIT_SOFTNORD,
					 new C_MDS_RetryMessage(mds,m));
	  return;
	} else {
	  // update lock
	  lock->set_state(LOCK_LOCK);
	  
	  // ack
	  {
		MLock *reply = new MLock(LOCK_AC_LOCKACK, mds->get_nodeid());
		reply->set_ino(in->ino(), LOCK_OTYPE_ISOFT);
		mds->messenger->send_message(reply,
									 MSG_ADDR_MDS(from), MDS_PORT_CACHE,
									 MDS_PORT_CACHE);
	  }
	}
	break;
	
  case LOCK_AC_ASYNC:
	assert(lock->get_state() == LOCK_GASYNC ||
		   lock->get_state() == LOCK_LOCK);
	
	// update lock
	lock->set_state(LOCK_ASYNC);
	
	// waiters
	in->softlock.get_write();
	in->finish_waiting(CINODE_WAIT_SOFTW|CINODE_WAIT_SOFTSTABLE);
	in->softlock.put_write();
	inode_soft_eval(in);
	break;
	

  case LOCK_AC_GASYNC:
	assert(lock->get_state() == LOCK_SYNC ||
		   lock->get_state() == LOCK_WGASYNC);
	
	// wait for readers to finish?
	if (lock->get_nread() > 0) {
	  dout(7) << "handle_lock_inode_soft readers, waiting before ack on " << *in << endl;
	  lock->set_state(LOCK_WGASYNC);
	  in->add_waiter(CINODE_WAIT_SOFTNORD,
					 new C_MDS_RetryMessage(mds,m));
	  return;
	} else {
	  // update lock
	  lock->set_state(LOCK_GASYNC);
	  
	  // ack
	  {
		MLock *reply = new MLock(LOCK_AC_GASYNCACK, mds->get_nodeid());
		reply->set_ino(in->ino(), LOCK_OTYPE_ISOFT);
		mds->messenger->send_message(reply,
									 MSG_ADDR_MDS(from), MDS_PORT_CACHE,
									 MDS_PORT_CACHE);
	  }
	}
	break;

	
  case LOCK_AC_GSYNC:
	assert(lock->get_state() == LOCK_ASYNC ||
		   lock->get_state() == LOCK_WGSYNC);

	// wait for writers to finish?
	if (lock->get_nwrite() > 0) {
	  dout(7) << "handle_lock_inode_soft writers, waiting before ack on " << *in << endl;
	  lock->set_state(LOCK_WGSYNC);
	  in->add_waiter(CINODE_WAIT_SOFTNOWR,
					 new C_MDS_RetryMessage(mds,m));
	  return;
	} else {
	  // update lock
	  lock->set_state(LOCK_GSYNC);
	  
	  // reply w/ our data
	  {
		MLock *reply = new MLock(LOCK_AC_GSYNCACK, mds->get_nodeid());
		reply->set_ino(in->ino(), LOCK_OTYPE_ISOFT);
		
		// payload
		crope sd;
		in->encode_soft_state(sd);
		reply->set_data(sd);
		
		// mark clean if dirty!
		if (in->is_dirty()) in->mark_clean();
		
		mds->messenger->send_message(reply,
									 MSG_ADDR_MDS(from), MDS_PORT_CACHE,
									 MDS_PORT_CACHE);
	  }
	}
	break;
	
  case LOCK_AC_GLOCK:
	assert(lock->get_state() == LOCK_ASYNC ||
		   lock->get_state() == LOCK_WLOCKW);
	
	// wait for writers to finish?
	if (lock->get_nwrite() > 0) {
	  dout(7) << "handle_lock_inode_soft writers, waiting before ack on " << *in << endl;
	  lock->set_state(LOCK_WLOCKW);
	  in->add_waiter(CINODE_WAIT_SOFTNOWR,
					 new C_MDS_RetryMessage(mds,m));
	  return;
	} else {
	  // update lock
	  lock->set_state(LOCK_LOCK);
	  
	  // reply w/ our data
	  {
		MLock *reply = new MLock(LOCK_AC_GLOCKACK, mds->get_nodeid());
		reply->set_ino(in->ino(), LOCK_OTYPE_ISOFT);
		
		// payload
		crope sd;
		in->encode_soft_state(sd);
		reply->set_data(sd);
		
		// mark clean if dirty!
		if (in->is_dirty()) in->mark_clean();
		
		mds->messenger->send_message(reply,
									 MSG_ADDR_MDS(from), MDS_PORT_CACHE,
									 MDS_PORT_CACHE);
	  }
	}
	break;
	  
 
	
	// -- auth --
  case LOCK_AC_LOCKNAK:
	// do NOT remove from cached_by; we don't know the nonce! 
	// and somewhere out there there's an expire that will take care of it.
	
  case LOCK_AC_LOCKACK:
	assert(lock->state == LOCK_PRELOCK);
	assert(lock->gather_set.count(from));
	lock->gather_set.erase(from);
	
	if (lock->gather_set.size()) {
	  dout(7) << "handle_lock_inode_soft " << *in << " from " << from << ", still gathering " << lock->gather_set << endl;
	} else {
	  dout(7) << "handle_lock_inode_soft " << *in << " from " << from << ", last one" << endl;
	  inode_soft_eval(in);
	}
	break;
	

  case LOCK_AC_GLOCKNAK:
	// do NOT remove from cached_by; we don't know the nonce! 
	// and somewhere out there there's an expire that will take care of it.
	
  case LOCK_AC_GLOCKACK:
	assert(lock->state == LOCK_GLOCK);
	assert(lock->gather_set.count(from));
	lock->gather_set.erase(from);
	
	if (m->get_action() == LOCK_AC_GLOCKACK) {
	  // merge data  (keep largest size, mtime, etc.)
	  int off = 0;
	  in->decode_merge_soft_state(m->get_data(), off);
	}

	if (lock->gather_set.size()) {
	  dout(7) << "handle_lock_inode_soft " << *in << " from " << from << ", still gathering " << lock->gather_set << endl;
	} else {
	  dout(7) << "handle_lock_inode_soft " << *in << " from " << from << ", last one" << endl;
	  inode_soft_eval(in);
	}
	break;

	
  case LOCK_AC_GSYNCNAK:
	// do NOT remove from cached_by; we don't know the nonce! 
	// and somewhere out there there's an expire that will take care of it.
		
  case LOCK_AC_GSYNCACK:
	assert(lock->state == LOCK_GSYNC);
	assert(lock->gather_set.count(from));
	lock->gather_set.erase(from);
	
	if (m->get_action() == LOCK_AC_GSYNCACK) {
	  // merge data  (keep largest size, mtime, etc.)
	  int off = 0;
	  dout(7) << "merging soft state" <<endl;
	  in->decode_merge_soft_state(m->get_data(), off);
	  dout(7) << "done merging soft state" <<endl;
	}

	if (lock->gather_set.size()) {
	  dout(7) << "handle_lock_inode_soft " << *in << " from " << from << ", still gathering " << lock->gather_set << endl;
	} else {
	  dout(7) << "handle_lock_inode_soft " << *in << " from " << from << ", last one" << endl;
	  inode_soft_eval(in);
	}
	break;

  case LOCK_AC_GASYNCNAK:
  case LOCK_AC_GASYNCACK:
	assert(lock->state == LOCK_GASYNC);
	assert(lock->gather_set.count(from));
	lock->gather_set.erase(from);
	
	if (lock->gather_set.size()) {
	  dout(7) << "handle_lock_inode_soft " << *in << " from " << from << ", still gathering " << lock->gather_set << endl;
	} else {
	  dout(7) << "handle_lock_inode_soft " << *in << " from " << from << ", last one" << endl;
	  inode_soft_eval(in);
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

bool MDCache::dentry_xlock_start(CDentry *dn, Message *m, CInode *ref, bool all_nodes)
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

  if (dn->dir->is_open_by_anyone() || all_nodes) {
	dn->lockstate = DN_LOCK_PREXLOCK;
	
	// xlock with whom?
	set<int> who;
	if (all_nodes) {
	  for (int i=0; i<mds->get_cluster()->get_num_mds(); i++)
		if (i != mds->get_nodeid()) who.insert(i);
	} else {
	  who = dn->dir->get_open_by();
	}
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
	if (r == 0) {
	  CDentry *dn = dir->lookup(dname);
	  if (dn && dn->xlockedby == 0) {
		cout << "xlock request success, now xlocked by " << req << endl;
		// success
		dn->xlockedby = req;   // our request was the winner

		// remember!
		mdc->active_requests[req].foreign_xlocks.insert(dn);
	  }		
	}

	// retry request (or whatever)
	cout << "doing finish on " << finisher << endl;
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
		dout(7) << "handle_lock_dn " << m << " " << m->get_ino() << " dname " << dname << " from " << from << ": proxy, fw to " << dauth << endl;

		// fw
		if (active_requests.count(m)) {
		  // xlock requests are requests, use request_* functions!
		  assert(m->get_action() == LOCK_AC_REQXLOCK ||
				 m->get_action() == LOCK_AC_REQXLOCKC);
		  request_forward(m, dauth);
		} else {
		  // not an xlock req (or we just didn't register the request yet)
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

	  case LOCK_AC_REQXLOCK:
		// send nak
		{
		  dout(7) << "handle_lock_dn reqxlock on " << dname << " in " << *dir << " dne, nak" << endl;
		  MLock *reply = new MLock(LOCK_AC_REQXLOCKNAK, mds->get_nodeid());
		  reply->set_dn(dir->ino(), dname);
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
	  dir->take_waiting(CDIR_WAIT_DNREQXLOCK, m->get_dn(), finished);
	  finish_contexts(finished, 
					  (m->get_action() == LOCK_AC_REQXLOCKACK) ? 0:-1);
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

  case LOCK_AC_REQXLOCK:
  case LOCK_AC_REQXLOCKC:
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
	  if (!dentry_xlock_start(dn, m, dir->inode, true)) {
		// hose null dn if we're waiting on something
		if (dn->is_clean() && dn->is_null() && dn->is_sync()) dir->remove_dentry(dn);
		return;    // waiting for xlock
	  }
	} else {
	  // successfully xlocked!  on behalf of requestor.
	  string path;
	  dn->make_path(path);
	  
	  // ACK xlock request
	  MLock *reply = new MLock(LOCK_AC_REQXLOCKACK, mds->get_nodeid());
	  reply->set_dn(dir->ino(), dname);
	  reply->set_path(path);
	  mds->messenger->send_message(reply,
								   MSG_ADDR_MDS(m->get_asker()), MDS_PORT_CACHE,
								   MDS_PORT_CACHE);

	  // disassociate xlock from MLock
	  active_requests[m].xlocks.clear();
	  active_requests[m].traces.clear();
	  dn->xlockedby = DN_XLOCK_FOREIGN;  // not 0, not a valid pointer.

	  // finish request
	  request_finish(m);
	  return;
	}
	break;

  case LOCK_AC_UNXLOCK:
	dout(7) << "handle_lock_dn unxlock on " << *dn << endl;
	{
	  CDir *dir = dn->dir;
	  string dname = dn->name;
	  dentry_xlock_finish(dn);
	  dir->take_waiting(CDIR_WAIT_ANY, dname, mds->finished_queue);
	}
	break;

  default:
	assert(0);
  }

  delete m;
}










// ino proxy

int MDCache::ino_proxy_auth(inodeno_t ino, 
							int frommds,
							map<CDir*, set<inodeno_t> >& inomap) 
{
  // check proxy sets for this ino
  for (map<CDir*, set<inodeno_t> >::iterator wit = inomap.begin();
	   wit != inomap.end();
	   wit++) {
	CDir *dir = wit->first;

	// does this map apply to this node?
	if (export_notify_ack_waiting[dir].count(frommds) == 0) continue;

	// is this ino in the set?
	if (inomap[dir].count(ino)) {
	  int dirauth = dir->authority();
	  assert(dirauth >= 0);
	  return dirauth;
	}
  }
  return -1;   // no proxy
}


void MDCache::do_ino_proxy(CInode *in, Message *m)
{
  // check proxy maps
  int newauth = ino_proxy_auth(in->ino(), 
							   m->get_source(),    // works bc we only every proxy 1 hop
							   export_proxy_inos);
  dout(7) << "inode " << *in << " proxy, new auth is " << newauth << endl;
  assert(newauth >= 0);     // we should know the new authority!
  assert(in->is_frozen());  // i should be frozen right now!
  assert(in->state_test(CINODE_STATE_PROXY));
  
  // forward
  mds->messenger->send_message(m,
							   MSG_ADDR_MDS(newauth), MDS_PORT_CACHE, MDS_PORT_CACHE);
  return;
}


void MDCache::do_dir_proxy(CDir *dir, Message *m)
{
  // check proxy maps
  int newauth = ino_proxy_auth(dir->ino(), 
							   m->get_source(),   // works because we only every proxy 1 hop
							   export_proxy_dirinos);
  dout(7) << "dir " << *dir << " proxy, new auth is " << newauth << endl;
  assert(newauth >= 0);     // we should know the new authority!
  assert(dir->is_frozen());  // i should be frozen right now!
  assert(dir->state_test(CDIR_STATE_PROXY));
  
  // forward
  mds->messenger->send_message(m,
							   MSG_ADDR_MDS(newauth), MDS_PORT_CACHE, MDS_PORT_CACHE);
  return;
}















// IMPORT/EXPORT

void MDCache::find_nested_exports(CDir *dir, set<CDir*>& s)
{
  CDir *import = get_containing_import(dir);

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

	// trace back to import, or dir
	CDir *cur = nested->get_parent_dir();
	while (!cur->is_import()) {
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

	  finish_contexts(will_fail);
	  finish_contexts(will_redelegate);
	  return;

	  // THIS IS ALL STUPID: (???)
	  /*
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
	  */
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

  // pin path?
  vector<CDentry*> trace;
  make_trace(trace, dir->inode);
  if (!path_pin(trace, 0, 0)) {
	dout(7) << "export_dir couldn't pin path, failing." << endl;
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
  /*
  if (g_conf.mdcache_sticky_sync_normal ||
	  g_conf.mdcache_sticky_sync_softasync)
	export_dir_dropsync(dir);
  */
  // NOTE: we don't need to worry about hard locks; those aren't sticky (yet?).
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
  dout(7) << "export_dir_frozen on " << *dir << " to " << dest << endl;

  show_imports();

  MExportDirPrep *prep = new MExportDirPrep(dir->inode);

  // include spanning tree for all nested exports.
  // these need to be on the destination _before_ the final export so that
  // dir_auth updates on any nexted exports are properly absorbed.
  
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
      dout(10) << "  will add " << *cur->inode << endl;
      
      // include dir? note: this'll include everything except the nested exports themselves, 
	  // since someone else is obviously auth.
      if (cur->is_auth()) {
        prep->add_dir( new CDirDiscover(cur, cur->open_by_add(dest)) );  // yay!
        dout(10) << "  added " << *cur << endl;
      }
      
      cur = parent_dir;      
    }

	for (list<CInode*>::iterator it = inode_trace.begin();
		 it != inode_trace.end();
		 it++) {
	  CInode *in = *it;
      dout(10) << "  added " << *in << endl;
      prep->add_inode( in->parent->dir->ino(),
					   in->parent->name,
					   new CInodeDiscover(in, in->cached_by_add(dest)) );
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


  // build export message
  MExportDir *req = new MExportDir(dir->inode, pop);  // include pop


  // update imports/exports
  CDir *containing_import = get_containing_import(dir);

  if (containing_import == dir) {
	dout(7) << " i'm rexporting a previous import" << endl;
	imports.erase(dir);
	dir->state_clear(CDIR_STATE_IMPORT);
	dir->put(CDIR_PIN_IMPORT);                  // unpin, no longer an import
	
	// discard nested exports (that we're handing off
	// NOTE: possible concurrent modification bug?
	for (set<CDir*>::iterator p = nested_exports[dir].begin();
		 p != nested_exports[dir].end();
		 p++) {
	  CDir *nested = *p;

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
	  CDir *containing_export = get_containing_export(nested->get_parent_dir());
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
		assert(get_containing_import(containing_export) != containing_import);
	  }
	}
  }

  // note new authority (locally)
  if (dir->inode->authority() == dest)
	dir->dir_auth = CDIR_AUTH_PARENT;
  else
	dir->dir_auth = dest;

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

  show_imports();
}


/** encode_export_inode
 * update our local state for this inode to export.
 * encode relevant state to be sent over the wire.
 * used by: export_dir_walk, file_rename (if foreign)
 */
void MDCache::encode_export_inode(CInode *in, crope& state_rope)
{
  in->version++;  // so local log entries are ignored, etc.
  
  // relax locks
  if (!in->is_cached_by_anyone())
	in->replicate_relax_locks();
  
  // replica_writers?
  if (in->is_open_write())
	in->add_replica_writer(mds->get_nodeid());  // i am now a replica writer!
  
  // add inode
  CInodeExport istate( in );
  state_rope.append( istate._rope() );

  // we're export this inode; fix inode state
  dout(7) << "encode_export_inode " << *in << endl;
  
  if (in->is_dirty()) in->mark_clean();
  
  // clear/unpin cached_by (we're no longer the authority)
  in->cached_by_clear();
  
  // don't need to know this anymore
  in->clear_replica_writers();
  
  // twiddle lock states
  in->softlock.twiddle_export();
  in->hardlock.twiddle_export();
  
  // mark auth
  assert(in->is_auth());
  in->set_auth(false);
  in->replica_nonce = CINODE_EXPORT_NONCE;
  
  // *** other state too?
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
  
  // release open_by 
  dir->open_by_clear();

  // mark
  assert(dir->is_auth());
  dir->state_clear(CDIR_STATE_AUTH);
  dir->replica_nonce = CDIR_NONCE_EXPORT;
  
  // discard most dir state
  dir->state &= CDIR_MASK_STATE_EXPORT_KEPT;  // i only retain a few things.
  
  // proxy
  dir->state_set(CDIR_STATE_PROXY);
  dir->get(CDIR_PIN_PROXY);
  export_proxy_dirinos[basedir].insert(dir->ino());

  if (!dir->is_clean())
	dir->mark_clean();
  
  // suck up all waiters
  list<Context*> waiting;
  dir->take_waiting(CDIR_WAIT_ANY, waiting);    // all dir waiters
  fin->assim_waitlist(waiting);
  

  // inodes
  list<CDir*> subdirs;

  CDir_map_t::iterator it;
  for (it = dir->begin(); it != dir->end(); it++) {
	CDentry *dn = it->second;
	CInode *in = dn->inode;
	
	// -- dentry
	dout(7) << "export_dir_walk exporting " << *dn << endl;
	dir_rope.append( it->first.c_str(), it->first.length()+1 );
	
	if (dn->is_dirty()) 
	  dir_rope.append((char)'D');  // dirty
	else 
	  dir_rope.append((char)'C');  // clean

	// null dentry?
	if (in == 0) {
	  dir_rope.append((char)'N');  // null dentry
	  assert(dn->is_sync());
	  continue;
	}
	  

	// -- inode
	dir_rope.append((char)'I');    // inode dentry

	encode_export_inode(in, dir_rope);  // encode, and (update state for) export
	
	// directory?
	if (in->is_dir() && in->dir) { 
	  if (in->dir->is_auth()) {
		// nested subdir
		assert(in->dir->dir_auth == CDIR_AUTH_PARENT);
		subdirs.push_back(in->dir);  // it's ours, recurse (later)
		
	  } else {
		// nested export
		assert(in->dir->dir_auth >= 0);
		dout(7) << " encountered nested export " << *in->dir << " dir_auth " << in->dir->dir_auth << "; removing from exports" << endl;
		assert(exports.count(in->dir) == 1); 
		exports.erase(in->dir);                    // discard nested export   (nested_exports updated above)
		
		in->dir->state_clear(CDIR_STATE_EXPORT);
		in->dir->put(CDIR_PIN_EXPORT);
		
		// simplify dir_auth?
		if (in->dir->dir_auth == newauth)
		  in->dir->dir_auth = CDIR_AUTH_PARENT;
	  } 
	}
	
	// add to proxy
	export_proxy_inos[basedir].insert(in->ino());
	in->state_set(CINODE_STATE_PROXY);
	in->get(CINODE_PIN_PROXY);

	// waiters
	list<Context*> waiters;
	in->take_waiting(CINODE_WAIT_ANY, waiters);
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
  CInode *diri = get_inode(m->get_ino());
  CDir *dir = diri->dir;
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

	// finish export  (unfreeze, trigger finish context, etc.)
	export_dir_finish(dir);

	// unpin proxies
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

	  // hose neg dentries, too, since we're no longer auth
	  CDir_map_t::iterator it;
	  for (it = dir->begin(); it != dir->end(); ) {
		CDentry *dn = it->second;
		it++;
		if (dn->is_null()) {
		  assert(dn->is_sync());
		  dir->remove_dentry(dn);
		} else {
		  dout(10) << "export_dir_notify_ack leaving xlocked neg " << *dn << endl;
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
  assert(m->get_source() != mds->get_nodeid());

  dout(7) << "handle_export_dir_discover on " << m->get_path() << endl;

  // must discover it!
  C_MDC_ExportDirDiscover *onfinish = new C_MDC_ExportDirDiscover(this, m);
  filepath fpath(m->get_path());
  int r = path_traverse(fpath, onfinish->trace, true,
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
  assert(m->get_source() != mds->get_nodeid());

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
        dout(10) << " updated " << *in << endl;
      } else {
        in = new CInode(false);
        (*it)->update_inode(in);
        
        // link to the containing dir
        CInode *condiri = get_inode( m->get_containing_dirino(in->ino()) );
        assert(condiri && condiri->dir);
        add_inode( in );
        condiri->dir->add_dentry( m->get_dentry(in->ino()), in );
        
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
          dout(10) << "   added " << *in->dir << endl;
		  in->take_waiting(CINODE_WAIT_DIR, finished);
        }
      }
    }

    // open export dirs?
    for (list<inodeno_t>::iterator it = m->get_exports().begin();
         it != m->get_exports().end();
         it++) {
      CInode *in = get_inode(*it);
      assert(in);
      
      if (!in->dir) {
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
    if (in->dir) {
	  if (!in->dir->state_test(CDIR_STATE_IMPORTINGEXPORT)) {
		dout(10) << "  pinning nested export " << *in->dir << endl;
		in->dir->get(CDIR_PIN_IMPORTINGEXPORT);
		in->dir->state_set(CDIR_STATE_IMPORTINGEXPORT);
	  } else {
		dout(10) << "  already pinned nested export " << *in << endl;
	  }
	} else {
	  dout(10) << "  waiting for nested export dir on " << *in << endl;
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

  int oldauth = m->get_source();
  dout(7) << "handle_export_dir, import " << *dir << " from " << oldauth << endl;
  assert(dir->is_auth() == false);



  show_imports();
  mds->logger->inc("im");
  
  // note new authority (locally) in inode
  if (dir->inode->is_auth())
	dir->dir_auth = CDIR_AUTH_PARENT;
  else
	dir->dir_auth = mds->get_nodeid();
  dout(10) << " set dir_auth to " << dir->dir_auth << endl;

  // update imports/exports
  CDir *containing_import;
  if (exports.count(dir)) {
	// reimporting
	dout(7) << " i'm reimporting " << *dir << endl;
	exports.erase(dir);

	dir->state_clear(CDIR_STATE_EXPORT);
	dir->put(CDIR_PIN_EXPORT);                // unpin, no longer an export
	
	containing_import = get_containing_import(dir);  
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

      mds->logger->inc("immyex");

      // move nested exports under containing_import
	  for (set<CDir*>::iterator it = nested_exports[ex].begin();
		   it != nested_exports[ex].end();
		   it++) {
        dout(7) << "     moving nested export " << **it << " under " << *containing_import << endl;
		nested_exports[containing_import].insert(*it);
	  }
      nested_exports.erase(ex);	      // de-list under old import
      
	  ex->dir_auth = CDIR_AUTH_PARENT;
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
  crope dir_state = m->get_state();
  int off = 0;
  for (int i = 0; i < m->get_ndirs(); i++) {
	import_dir_block(dir_state, 
					 off,
					 oldauth, 
					 dir,                 // import root
					 imported_subdirs);
  }
  dout(10) << " " << imported_subdirs.size() << " imported subdirs" << endl;
  dout(10) << " " << m->get_exports().size() << " imported nested exports" << endl;
  

  // adjust popularity
  // FIXME what about rest of pop vector?  also, i think this is wrong.
  double newpop = m->get_ipop() - diri->popularity[0].get();
  dout(7) << " imported popularity jump by " << newpop << endl;
  if (newpop > 0) {  // duh
	CInode *t = diri;
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


void MDCache::decode_import_inode(CDentry *dn, crope& r, int& off, int oldauth)
{
  
  CInodeExport istate;
  off = istate._unrope(r, off);
  dout(15) << "got a cinodeexport " << endl;
  
  bool added = false;
  CInode *in = get_inode(istate.get_ino());
  if (!in) {
	in = new CInode;
	added = true;
  } else {
	in->set_auth(true);
  }
  
  // state
  istate.update_inode(in);
  
  // link
  if (added) {
	add_inode(in);
	dn->dir->link_inode(dn, in);
	dout(10) << "added " << *in << endl;
  } else {
	dout(10) << "  had " << *in << endl;
	
	if (in->is_replica_writer(mds->get_nodeid()))
	  in->remove_replica_writer(mds->get_nodeid());
  }
  
  // cached_by
  assert(!in->is_cached_by(oldauth));
  in->cached_by_add( oldauth, CINODE_EXPORT_NONCE );
  if (in->is_cached_by(mds->get_nodeid()))
	in->cached_by_remove(mds->get_nodeid());
  
  /* don't do this
	 if (!in->hardlock.is_stable() && 
	 in->hardlock.is_gathering(mds->get_nodeid())) {
	 in->hardlock.gather_set.erase(mds->get_nodeid());
	 if (in->hardlock.gather_set.size() == 0) 
	 inode_hard_eval(in);
	 }
	 if (!in->softlock.is_stable() && 
	 in->softlock.is_gathering(mds->get_nodeid())) {
	 in->softlock.gather_set.erase(mds->get_nodeid());
	 if (in->softlock.gather_set.size() == 0) 
	 inode_soft_eval(in);
	 }
  */
  
  // other
  if (in->is_dirty()) {
	dout(10) << "logging dirty import " << *in << endl;
	mds->mdlog->submit_entry(new EInodeUpdate(in),
							 NULL);   // FIXME pay attention to completion?
  }
}


void MDCache::import_dir_block(crope& r,
							   int& off,
							   int oldauth,
							   CDir *import_root,
							   list<inodeno_t>& imported_subdirs)
{
  // set up dir
  CDirExport dstate;
  off = dstate._unrope(r, off);

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
  if (diri->is_auth()) dir->dir_auth = CDIR_AUTH_PARENT;   // update_dir may hose dir_auth

  // mark  (may already be marked from get_or_open_dir() above)
  if (!dir->is_auth())
	dir->state_set(CDIR_STATE_AUTH);

  // open_by
  assert(!dir->is_open_by(oldauth));
  dir->open_by_add(oldauth);
  if (dir->is_open_by(mds->get_nodeid()))
	dir->open_by_remove(mds->get_nodeid());

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
  for (long nden = dstate.get_nden(); nden>0; nden--) {
    // dentry
    string dname = r.c_str() + off;
    off += dname.length()+1;
    dout(15) << "dname is " << dname << endl;

	char dirty = *(r.c_str() + off);
	off++;

	char icode = *(r.c_str() + off);
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
	else if (icode == 'I') {
	  // inode
	  decode_import_inode(dn, r, off, oldauth);
    }

	// mark dentry dirty?  (only _after_ we link the inode)
	if (dirty == 'D') dn->mark_dirty();
    
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
  CInode *diri = get_inode(dir_ino);
  assert(diri && diri->dir);
  if (import_hashed_replicate_waiting.count(dir_ino) > 0)
	return;  // still more
  
  // done with this dir!
  diri->dir->unfreeze_dir();
  
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
	  dout(7) << "export_dir_notify bound " << *ndir << " was dir_auth " << ndir->dir_auth << " (" << boundauth << ")" << endl;
	  if (ndir->dir_auth == CDIR_AUTH_PARENT) {
		if (boundauth != m->get_new_auth())
		  ndir->dir_auth = boundauth;
		else assert(dir->authority() == m->get_new_auth());  // apparently we already knew!
	  } else {
		if (boundauth == m->get_new_auth())
		  ndir->dir_auth = CDIR_AUTH_PARENT;
	  }
	}
	
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
	
	// DEBUG: verify subdirs
	if (g_conf.mds_verify_export_dirauth) {
	  
	  dout(7) << "handle_export_dir_notify on " << *dir << " checking " << m->num_subdirs() << " subdirs" << endl;
	  for (list<inodeno_t>::iterator it = m->subdirs_begin();
		   it != m->subdirs_end();
		   it++) {
		CInode *diri = get_inode(*it);
		if (!diri) continue;  // don't have it, don't care
		if (!diri->dir) continue;
		dout(10) << "handle_export_dir_notify checking subdir " << *diri->dir << " is auth " << diri->dir->dir_auth << endl;
		assert(diri->dir != dir);	  // base shouldn't be in subdir list
		if (diri->dir->dir_auth != CDIR_AUTH_PARENT) {
		  dout(7) << "*** weird value for dir_auth " << diri->dir->dir_auth << " on " << *diri->dir << ", should have been -1 probably??? ******************" << endl;
		  assert(0);  // bad news!
		  //dir->dir_auth = -1;
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

/*void MDCache::drop_sync_in_dir(CDir *dir)
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
*/

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
  /*
  if (g_conf.mdcache_sticky_sync_normal ||
	  g_conf.mdcache_sticky_sync_softasync) 
	drop_sync_in_dir(dir);
  */
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

  CInode *diri = trav[trav.size()-1];
  CDir *dir = diri->get_dir(mds->get_nodeid());

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
  diri->inode.isdir = INODE_DIR_HASHED;
  if (diri->is_auth()) 
	diri->mark_dirty();

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
  dir->inode->inode.hash_seed = 0;
  dir->inode->mark_dirty();

  // unfreeze!
  dir->unfreeze_dir();
}
*/

void MDCache::handle_unhash_dir_ack(MUnhashDirAck *m)
{
  /*
  CInode *diri = get_inode(m->get_ino());
  assert(diri && diri->dir);
  assert(diri->dir->is_auth());
  assert(diri->dir->is_hashed());
  assert(diri->dir->is_unhashing());

  dout(7) << "handle_unhash_dir_ack " << *diri->dir << endl;
  
  // assimilate contents
  int oldauth = m->get_source();
  const char *p = m->dir_rope.c_str();
  const char *pend = p + m->dir_rope.length();
  while (p < pend) {
	CInode *in = import_dentry_inode(diri->dir, p, oldauth);
	in->mark_dirty();   // pin in cache
  }

  // remove from waiting list
  multimap<CDir*,int>::iterator it = unhash_waiting.find(diri->dir);
  while (it->second != oldauth) {
	it++;
	assert(it->first == diri->dir);
  }
  unhash_waiting.erase(it);

  unhash_dir_finish(diri->dir);  // try to finish

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

  CInode *diri = trav[trav.size()-1];
  if (!diri->dir) diri->dir = new CDir(diri, mds->get_nodeid());
  CDir *dir = diri->dir;

  dout(7) << "handle_unhash_dir " << *diri->dir << endl;
  
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
	CDir *im = *it;
	dout(7) << "  + import " << *im << endl;
	assert( im->is_import() );
	assert( im->is_auth() );
	
	for (set<CDir*>::iterator p = nested_exports[im].begin();
		 p != nested_exports[im].end();
		 p++) {
	  CDir *exp = *p;
	  dout(7) << "      - ex " << *exp << " to " << exp->dir_auth << endl;
	  assert( exp->is_export() );
	  assert( !exp->is_auth() );
	  
	  if ( get_containing_import(exp) != im ) {
		dout(7) << "uh oh, containing import is " << get_containing_import(exp) << endl;
		dout(7) << "uh oh, containing import is " << *get_containing_import(exp) << endl;
		assert( get_containing_import(exp) == im );
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
	/*
	if ((*it).second->ref) 
	  dout2(7) << " pin " << (*it).second->ref_set;
	if ((*it).second->cached_by.size())
	  dout2(7) << " cache_by " << (*it).second->cached_by;
	*/
	if ((*it).second->dir)
	  dout2(7) << " ... " << *(*it).second->dir;
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
  lru.lru_status();

  return cur;  
}
