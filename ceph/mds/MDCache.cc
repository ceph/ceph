
#include "MDCache.h"
#include "MDStore.h"
#include "CInode.h"
#include "CDir.h"
#include "MDS.h"

#include "include/Message.h"
#include "include/Messenger.h"

#include "messages/MDiscover.h"
#include "messages/MExportDir.h"
#include "messages/MExportDirAck.h"
#include "messages/MExportDirNotify.h"

#include "messages/MInodeUpdate.h"
#include "messages/MDirUpdate.h"

#include "messages/MInodeExpire.h"

#include "messages/MInodeSyncStart.h"
#include "messages/MInodeSyncAck.h"
#include "messages/MInodeSyncRelease.h"

#include <assert.h>
#include <errno.h>
#include <iostream>
#include <string>
#include <map>
using namespace std;

MDCache::MDCache(MDS *m)
{
  mds = m;
  root = NULL;
  opening_root = false;
  lru = new LRU();
  lru->lru_set_max(2500);
}

MDCache::~MDCache() 
{
  if (lru) { delete lru; lru = NULL; }
}


// 

bool MDCache::shutdown()
{
  //if (root) clear_dir(root);
}


// MDCache

bool MDCache::add_inode(CInode *in) 
{
  // add to lru, inode map
  lru->lru_insert_mid(in);
  inode_map[ in->inode.ino ] = in;
}

bool MDCache::remove_inode(CInode *o) 
{
  // detach from parents
  if (o->nparents == 1) {
	CDentry *dn = o->parent;
	dn->dir->remove_child(dn);
	delete dn;
  } 
  else if (o->nparents > 1) {
	throw "implement me";  
  }

  // remove from map
  inode_map.erase(o->inode.ino);

  // remove from lru
  lru->lru_remove(o);

  return true;
}

bool MDCache::trim(__int32_t max) {
  if (max < 0) {
	max = lru->lru_get_max();
	if (!max) return false;
  }

  while (lru->lru_get_size() > max) {
	CInode *in = (CInode*)lru->lru_expire();
	if (!in) return false;

	// notify authority?
	int auth = in->authority(mds->get_cluster());
	if (auth != mds->get_nodeid()) {
	  mds->messenger->send_message(new MInodeExpire(in->inode.ino),
								   MSG_ADDR_MDS(auth), MDS_PORT_CACHE,
								   MDS_PORT_CACHE);
	}	


	// dir incomplete!
	in->parent->dir->state_clear(CDIR_MASK_COMPLETE);

	// remove it
	cout << "mds" << mds->get_nodeid() << " trim deleting " << *in << " " << in << endl;
	remove_inode(in);
	delete in;
	
  }

  return true;
}


CInode* MDCache::get_file(string& fn) {
  int off = 1;
  CInode *cur = root;
  
  // dirs
  while (off < fn.length()) {
	unsigned int slash = fn.find("/", off);
	if (slash == string::npos) 
	  slash = fn.length();	
	string n = fn.substr(off, slash-off);

	//cout << " looking up '" << n << "' in " << cur << endl;

	if (cur->dir == NULL) {
	  //cout << "   not a directory!" << endl;
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


int MDCache::link_inode( CInode *parent, string& dname, CInode *in ) 
{
  if (!parent->dir) {
	return -ENOTDIR;  // not a dir
  }

  // create dentry
  CDentry* dn = new CDentry(dname, in);
  in->add_parent(dn);

  // add to dir
  parent->dir->add_child(dn);

  return 0;
}


void MDCache::add_file(string& fn, CInode *in) {
  
  // root?
  if (fn == "/") {
	root = in;
	add_inode( in );
	//cout << " added root " << root << endl;
	return;
  } 


  // file.
  int lastslash = fn.rfind("/");
  string dirpart = fn.substr(0,lastslash);
  string file = fn.substr(lastslash+1);

  //cout << "dirpart '" << dirpart << "' filepart '" << file << "' inode " << in << endl;
  
  CInode *idir = get_file(dirpart);
  if (idir == NULL) return;

  //cout << " got dir " << idir << endl;

  if (idir->dir == NULL) {
	cerr << " making " << dirpart << " into a dir" << endl;
	idir->dir = new CDir(idir); 
	idir->inode.isdir = true;
  }
  
  add_inode( in );
  link_inode( idir, file, in );

  // trim
  trim();

}

int MDCache::open_root(Context *c)
{
  int whoami = mds->get_nodeid();

  // open root inode
  if (whoami == 0) { 
	// i am root
	CInode *root = new CInode();
	root->inode.ino = 1;
	root->inode.isdir = true;

	// make it up (FIXME)
	root->inode.mode = 0755;
	root->inode.size = 0;

	root->dir = new CDir(root);
	root->dir_auth = 0;  // me!
	root->dir->dir_rep = CDIR_REP_ALL;

	set_root( root );

	// root is technically an import (from a vacuum)
	imports.insert( root );

	if (c) {
	  c->finish(0);
	  delete c;
	}
  } else {
	// request inode from root mds
	if (c) 
	  waiting_for_root.push_back(c);
	
	if (!opening_root) {
	  cout << "mds" << mds->get_nodeid() << " discovering root" << endl;
	  opening_root = true;

	  MDiscover *req = new MDiscover(whoami,
									 string(""),
									 NULL);
	  mds->messenger->send_message(req,
								   MSG_ADDR_MDS(0), MDS_PORT_CACHE,
								   MDS_PORT_CACHE);
	} else
	  cout << "mds" << mds->get_nodeid() << " waiting for root" << endl;
	
  }
}


CInode *MDCache::get_containing_import(CInode *in)
{
  CInode *imp = in;  // might be *in

  // find the underlying import!
  while (imp && 
		 imports.count(imp) == 0) {
	imp = imp->get_parent_inode();
  }

  assert(imp);
  return imp;
}

CInode *MDCache::get_containing_export(CInode *in)
{
  CInode *ex = in;  // might be *in

  // find the underlying import!
  while (ex &&                        // white not at root,
		 exports.count(ex) == 0) {    // we didn't find an export,
	ex = ex->get_parent_inode();
  }

  return ex;
}


// SYNC

/*

this all sucks




int MDCache::read_start(CInode *in, Message *m)
{
  // dist writes not implemented.

  return read_wait(in, m);
}

int MDCache::read_wait(CInode *in, Message *m)
{
  if (in->authority(mds->get_cluster()) == mds->get_nodeid())
	return 0;   // all good for me
  
  if (in->get_sync() & CINODE_SYNC_LOCK) {
	// wait!
	cout << "read_wait waiting for read lock" << endl;
	in->add_read_waiter(new C_MDS_RetryMessage(mds, m));
  }
  return 0;
}

int MDCache::read_finish(CInode *in)
{
  return 0;  // nothing
}





int MDCache::write_start(CInode *in, Message *m)
{
  if (in->get_sync() == CINODE_SYNC_LOCK)
	return 0;   // we're locked!

  int auth = in->authority(mds->get_cluster());
  int whoami = mds->get_nodeid();

  if (auth == whoami) {
	// we are the authority.

	if (in->cached_by.size() == 0) {
	  // it's just us!
	  in->sync_set(CINODE_SYNC_LOCK);
	  in->get();
	  return 0;   
	}

	// ok, we need to get the lock.
	
	// queue waiter
	in->add_write_waiter(new C_MDS_RetryMessage(mds, m));
	
	if (in->get_sync() != CINODE_SYNC_START) {
	  
	  // send sync_start
	  set<int>::iterator it;
	  for (it = in->cached_by.begin(); it != in->cached_by.end(); it++) {
		mds->messenger->send_message(new MInodeSyncStart(in->inode.ino, auth),
									 MSG_ADDR_MDS(*it), MDS_PORT_CACHE,
									 MDS_PORT_CACHE);
	  }
	  
	  in->sync_waiting_for_ack = in->cached_by;
	  in->sync_set(CINODE_SYNC_START);
	  in->get();	// pin
	}
  } else {

	throw "not implemented";

  }

  return 1;
}

int MDCache::write_finish(CInode *in)
{
  assert(in->get_sync() == CINODE_SYNC_LOCK);

  in->sync_set(0);   // clear sync state
  in->put();         // unpin

  // 
  if (in->cached_by.size()) {
	// release
	set<int>::iterator it;
	for (it = in->cached_by.begin(); it != in->cached_by.end(); it++) {
	  mds->messenger->send_message(new MInodeSyncRelease(in->inode.ino),
								   MSG_ADDR_MDS(*it), MDS_PORT_CACHE,
								   MDS_PORT_CACHE);
	}
  }
}

*/



// ========= messaging ==============


int MDCache::proc_message(Message *m)
{
  switch (m->get_type()) {
  case MSG_MDS_DISCOVER:
	handle_discover((MDiscover*)m);
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


	// sync
	/*
  case MSG_MDS_INODESYNCSTART:
	handle_inode_sync_start((MInodeSyncStart*)m);
	break;

  case MSG_MDS_INODESYNCACK:
	handle_inode_sync_ack((MInodeSyncAck*)m);
	break;

  case MSG_MDS_INODESYNCRELEASE:
	handle_inode_sync_release((MInodeSyncRelease*)m);
	break;
	*/
	
	// import
  case MSG_MDS_EXPORTDIR:
	handle_export_dir((MExportDir*)m);
	break;

	// export ack
  case MSG_MDS_EXPORTDIRACK:
	handle_export_dir_ack((MExportDirAck*)m);
	break;

	// export notify
  case MSG_MDS_EXPORTDIRNOTIFY:
	handle_export_dir_notify((MExportDirNotify*)m);
	break;

	
  default:
	cout << "mds" << mds->get_nodeid() << " cache unknown message " << m->get_type() << endl;
	throw "asdf";
	break;
  }

  return 0;
}



int MDCache::path_traverse(string& path, 
						   vector<CInode*>& trace, 
						   Message *req,
						   int onfail)
{
  int whoami = mds->get_nodeid();
  
  CInode *cur = get_root();
  if (cur == NULL) {
	cout << "mds" << whoami << " i don't have root" << endl;
	if (req) 
	  open_root(new C_MDS_RetryMessage(mds, req));
	return 1;
  }

  // break path into bits.
  trace.clear();
  trace.push_back(cur);

  // get read access
  //if (read_wait(cur, req))
  //	return 1;   // wait

  string have_clean;

  vector<string> path_bits;
  split_path(path, path_bits);

  for (int depth = 0; depth < path_bits.size(); depth++) {
	string dname = path_bits[depth];
	cout << " path seg " << dname << endl;

	// lookup dentry
	if (cur->is_dir()) {
	  if (!cur->dir)
		cur->dir = new CDir(cur);

	  // frozen?
	  if (cur->dir->is_freeze_root()) {
		// doh!
		cout << "mds" << whoami << " dir " << *cur << " is frozen, waiting" << endl;
		cur->dir->add_freeze_waiter(new C_MDS_RetryMessage(mds, req));
		return 1;
	  }


	  CDentry *dn = cur->dir->lookup(dname);
	  if (dn && dn->inode) {
		// have it, keep going.
		cur = dn->inode;
		have_clean += "/";
		have_clean += dname;
	  } else {
		// don't have it.
		int dauth = cur->dir->dentry_authority( dname, mds->get_cluster() );

		if (dauth == whoami) {
		  // mine.
		  if (cur->dir->is_complete()) {
			// file not found
			return -ENOENT;
		  } else {
			// directory isn't complete; reload
			cout << "mds" << whoami << " incomplete dir contents for " << *cur << ", fetching" << endl;
			lru->lru_touch(cur);  // touch readdiree
			mds->mdstore->fetch_dir(cur, new C_MDS_RetryMessage(mds, req));
			return 1;		   
		  }
		} else {
		  // not mine.

		  if (onfail == MDS_TRAVERSE_DISCOVER) {
			// discover
			cout << "mds" << whoami << " discover on " << have_clean << " for " << dname << "..., to mds" << dauth << endl;

			// assemble+send request
			vector<string> *want = new vector<string>;
			for (int i=depth; i<path_bits.size(); i++)
			  want->push_back(path_bits[i]);

			lru->lru_touch(cur);  // touch discoveree

			mds->messenger->send_message(new MDiscover(whoami, have_clean, want),
									MSG_ADDR_MDS(dauth), MDS_PORT_CACHE,
									MDS_PORT_CACHE);
			
			// delay processing of current request
			cur->dir->add_waiter(dname, new C_MDS_RetryMessage(mds, req));

			return 1;
		  } 
		  if (onfail == MDS_TRAVERSE_FORWARD) {
			// forward
			cout << "mds" << whoami << " not authoritative for " << dname << ", fwd to mds" << dauth << endl;
			mds->messenger->send_message(req,
										 MSG_ADDR_MDS(dauth), req->get_dest_port(),
										 req->get_dest_port());
			//show_imports();
			return 1;
		  }	
		  if (onfail == MDS_TRAVERSE_FAIL) {
			return -1;
		  }
		}
	  }
	} else {
	  cout << *cur << " not a dir " << cur->inode.isdir << endl;
	  return -ENOTDIR;
	}
	
	trace.push_back(cur);
	//read_wait(cur, req);  // wait for read access
  }

  return 0;
}





int MDCache::handle_discover(MDiscover *dis) 
{
  int whoami = mds->get_nodeid();

  if (dis->asker == whoami) {
	// this is a result
	
	if (dis->want == 0) {
	  cout << "mds" << mds->get_nodeid() << " handle_discover got root" << endl;
	  
	  CInode *root = new CInode();
	  root->inode = dis->trace[0].inode;
	  root->cached_by = dis->trace[0].cached_by;
	  root->cached_by.insert(whoami);   // obviously i have it too
	  root->dir_auth = dis->trace[0].dir_auth;
	  root->dir = new CDir(root);
	  root->dir->dir_rep = dis->trace[0].dir_rep;
	  root->dir->dir_rep_by = dis->trace[0].dir_rep_by;
	  
	  set_root( root );

	  opening_root = false;

	  // done
	  delete dis;

	  // finish off.
	  list<Context*> finished;
	  finished.splice(finished.end(), waiting_for_root);

	  list<Context*>::iterator it;
	  for (it = finished.begin(); it != finished.end(); it++) {
		Context *c = *it;
		c->finish(0);
		delete c;
	  }

	  return 0;
	}
	
	// traverse to start point
	vector<CInode*> trav;

	cout << "mds" << mds->get_nodeid() << " handle_discover got result" << endl;
	  
	int r = path_traverse(dis->basepath, trav, NULL, MDS_TRAVERSE_FAIL);   // FIXME BUG
	if (r != 0) throw "wtf";
	
	CInode *cur = trav[trav.size()-1];
	CInode *start = cur;


	list<Context*> finished;

	// add duplicated dentry+inodes
	for (int i=0; i<dis->trace.size(); i++) {

	  if (!cur->dir) cur->dir = new CDir(cur);  // ugly

	  CInode *in;
	  CDentry *dn = cur->dir->lookup( (*dis->want)[i] );
	  if (dn) {
		// already had it?  (parallel discovers?)
		cout << "huh, already had " << (*dis->want)[i] << endl;
		in = dn->inode;
	  } else {
		in = new CInode();

		// assim discover info
		in->inode = dis->trace[i].inode;
		in->cached_by = dis->trace[i].cached_by;
		in->cached_by.insert(whoami);    // obviously i have it too
		in->dir_auth = dis->trace[i].dir_auth;
		if (in->is_dir()) {
		  in->dir = new CDir(in);
		  in->dir->dir_rep = dis->trace[i].dir_rep;
		  in->dir->dir_rep_by = dis->trace[i].dir_rep_by;
		}
		
		// link in
		add_inode( in );
		link_inode( cur, (*dis->want)[i], in );
		cout << " adding " << *in << endl;
	  }
	  
	  cur->dir->take_waiting((*dis->want)[i],
							 finished);
	  
	  cur = in;
	}

	// done
	delete dis;

	// finish off waiting items
	list<Context*>::iterator it;
	for (it = finished.begin(); it != finished.end(); it++) {
	  Context *c = *it;
	  c->finish(0);
	  delete c;				
	}	

  } else {
	
	cout << "mds" << mds->get_nodeid() << " handle_discover from mds" << dis->asker << " current_need() " << dis->current_need() << endl;
	
	// this is a request
	if (!root) {
	  open_root(new C_MDS_RetryMessage(mds, dis));
	  return 0;
	}

	// get to starting point
	vector<CInode*> trav;
	string current_base = dis->current_base();
	int r = path_traverse(current_base, trav, dis, MDS_TRAVERSE_FORWARD);
	if (r > 0) return 0;  // forwarded, i hope!

	CInode *cur = trav[trav.size()-1];

	// just root?
	if (dis->want_root()) {
	  CInode *root = get_root();
	  dis->add_bit( root, 0 );

	  root->cached_by.insert( dis->asker );
	}

	// add bits
	while (!dis->done()) {
	  if (!cur->is_dir()) {
		cout << "woah, discover on non dir " << dis->current_need() << endl;
		throw "implement me";
	  }

	  if (!cur->dir) cur->dir = new CDir(cur);
	  
	  if (cur->dir->is_frozen()) {
		cout << "mds" << whoami << " dir is frozen, waiting" << endl;
		cur->dir->add_freeze_waiter(new C_MDS_RetryMessage(mds, dis));
		return 0;
	  }

	  // lookup next bit
	  CDentry *dn = cur->dir->lookup(dis->next_dentry());
	  if (dn) {	
		// yay!  
		CInode *next = dn->inode;

		// is it mine?
		int auth = next->authority(mds->get_cluster());
		if (auth == whoami) {
		  // add it
		  dis->add_bit( next, whoami );
		  
		  // remember who is caching this!
		  if (next->cached_by.empty()) 
			next->get(CINODE_PIN_CACHED);
		  next->cached_by.insert( dis->asker );
		  
		  cur = next; // continue!
		} else {
		  // fwd to authority
		  mds->messenger->send_message(dis,
									   MSG_ADDR_MDS(auth), MDS_PORT_CACHE,
									   MDS_PORT_CACHE);
		  return 0;
		}
	  } else {

		// ?? might we not be the authority? ??
		assert(cur->authority(mds->get_cluster()) == whoami);

		// don't have it.
		if (cur->dir->is_complete()) {
		  // file not found.
		  throw "implement me";
		} else {
		  // readdir
		  cout << "mds" << whoami << " incomplete dir contents for " << *cur << ", fetching" << endl;
		  mds->mdstore->fetch_dir(cur, new C_MDS_RetryMessage(mds, dis));
		  return 0;
		}
	  }
	}
	
	// success, send result
	cout << "mds" << whoami << " finished discovery, sending back to " << dis->asker << endl;
	mds->messenger->send_message(dis,
								 MSG_ADDR_MDS(dis->asker), MDS_PORT_CACHE,
								 MDS_PORT_CACHE);
	return 0;
  }

}



int MDCache::send_inode_updates(CInode *in)
{
  set<int>::iterator it;
  for (it = in->cached_by.begin(); it != in->cached_by.end(); it++) {
	cout << "mds" << mds->get_nodeid() << " sending inode_update on " << *in << " to " << *it << endl;
	mds->messenger->send_message(new MInodeUpdate(in->inode,
												  in->cached_by,
												  in->dir_auth),
								 MSG_ADDR_MDS(*it), MDS_PORT_CACHE,
								 MDS_PORT_CACHE);
  }

  return 0;
}


void MDCache::handle_inode_update(MInodeUpdate *m)
{
  CInode *in = get_inode(m->inode.ino);
  if (!in) {
	cout << "mds" << mds->get_nodeid() << " inode_update on " << m->inode.ino << ", don't have it, sending expire" << endl;

	mds->messenger->send_message(new MInodeExpire(m->inode.ino),
								 m->get_source(), MDS_PORT_CACHE,
								 MDS_PORT_CACHE);
	
	delete m;
	return;
  }

  // update!
  cout << "mds" << mds->get_nodeid() << " inode_update on " << *in << endl;

  in->inode = m->inode;
  in->cached_by = m->cached_by;
  in->dir_auth = m->dir_auth;

  // done
  delete m;
}

void MDCache::handle_inode_expire(MInodeExpire *m)
{
  CInode *in = get_inode(m->ino);
  if (!in) {
	cout << "mds" << mds->get_nodeid() << " inode_expire on " << m->ino << ", don't have it, ignoring" << endl;
	delete m;
	return;
  }

  int auth = in->authority(mds->get_cluster());
  if (auth != mds->get_nodeid()) {
	cout << "mds" << mds->get_nodeid() << " inode_expire on " << *in << ", not mine" << endl;
	delete m;
	return;
  }

  // remove
  if (in->cached_by.count(m->get_source())) {
	cout << "mds" << mds->get_nodeid() << " inode_expire on " << *in << " from mds" << m->get_source() << endl;
	in->cached_by.erase(m->get_source());
	if (in->cached_by.empty()) 
	  in->put(CINODE_PIN_CACHED);
  } else {
	cout << "mds" << mds->get_nodeid() << " inode_expire on " << *in << " from mds" << m->get_source() << ", but they're not in cached_by"<< endl;
  }
  
  delete m;
}


int MDCache::send_dir_updates(CDir *dir, int except)
{
  int whoami = mds->get_nodeid();
  for (set<int>::iterator it = dir->inode->cached_by.begin(); 
	   it != dir->inode->cached_by.end(); 
	   it++) {
	if (*it == whoami) continue;
	if (*it == except) continue;
	cout << "mds" << whoami << " sending dir_update on " << *(dir->inode) << " to " << *it << endl;
	mds->messenger->send_message(new MDirUpdate(dir->inode->inode.ino,
												dir->dir_rep,
												dir->dir_rep_by),
								 MSG_ADDR_MDS(*it), MDS_PORT_CACHE,
								 MDS_PORT_CACHE);
  }

  return 0;
}

void MDCache::handle_dir_update(MDirUpdate *m)
{
  CInode *in = get_inode(m->ino);
  if (!in) {
	cout << "mds" << mds->get_nodeid() << " dir_update on " << m->ino << ", don't have it" << endl;
	delete m;
	return;
  }

  // update!
  cout << "mds" << mds->get_nodeid() << " dir_update on " << m->ino << endl;

  in->dir->dir_rep = m->dir_rep;
  in->dir->dir_rep_by = m->dir_rep_by;

  // done
  delete m;
}




// SYNC

/*

this all sucks


void MDCache::handle_inode_sync_start(MInodeSyncStart *m)
{
  // authority is requesting a lock
  CInode *in = get_inode(m->ino);
  if (!in) {
	// don't have it anymore!
	cout << "mds" << mds->get_nodeid() << " sync_start " << in->inode.ino << ": don't have it anymore, nak" << endl;
	mds->messenger->send_message(new MInodeSyncAck(m->ino, false),
								 m->authority, MDS_PORT_CACHE,
								 MDS_PORT_CACHE);
	return;
  }

  // we shouldn't be authoritative...
  assert(m->authority != mds->get_nodeid());

  cout << "mds" << mds->get_nodeid() << " sync_start " << *in << ", sending ack" << endl;

  // lock it
  in->get();
  in->sync_set(CINODE_SYNC_LOCK);
  
  // send ack
  mds->messenger->send_message(new MInodeSyncAck(m->ino),
							   m->authority, MDS_PORT_CACHE,
							   MDS_PORT_CACHE);
}

void MDCache::handle_inode_sync_ack(MInodeSyncAck *m)
{
  CInode *in = get_inode(m->ino);
  assert(in);
  assert(in->get_sync() == CINODE_SYNC_START);

  // remove it from waiting list
  in->sync_waiting_for_ack.erase(m->get_source());
	
  if (in->sync_waiting_for_ack.size()) {

	// more coming
	cout << "mds" << mds->get_nodeid() << " sync_ack " << m->ino << " from " << m->get_source() << ", waiting for more" << endl;

  } else {

	// yay!
	cout << "mds" << mds->get_nodeid() << " sync_ack " << m->ino << " from " << m->get_source() << ", last one" << endl;

	in->sync_set(CINODE_SYNC_LOCK);
  }
}


void MDCache::handle_inode_sync_release(MInodeSyncRelease *m)
{
  CInode *in = get_inode(m->ino);

  if (!in) {
	cout << "mds" << mds->get_nodeid() << " sync_release " << m->ino << ", don't have it anymore" << endl;
	return;
  }

  assert(in->get_sync() == CINODE_SYNC_LOCK);
  
  cout << "mds" << mds->get_nodeid() << " sync_release " << m->ino << endl;
  in->sync_set(0);
  
  // finish
  list<Context*> finished;
  in->take_write_waiting(finished);
  list<Context*>::iterator it;
  for (it = finished.begin(); it != finished.end(); it++) {
	Context *c = *it;
	c->finish(0);
	delete c;
  }
}

*/


// IMPORT/EXPORT

class C_MDS_ExportFreeze : public Context {
  MDS *mds;
  CInode *in;   // inode of dir i'm exporting
  int dest;
  double pop;

public:
  C_MDS_ExportFreeze(MDS *mds, CInode *in, int dest, double pop) {
	this->mds = mds;
	this->in = in;
	this->dest = dest;
	this->pop = pop;
  }
  virtual void finish(int r) {
	mds->mdcache->export_dir_frozen(in, dest, pop);
  }
};

class C_MDS_ExportFinish : public Context {
  MDS *mds;
  CInode *in;   // inode of dir i'm exporting

public:
  // contexts for waiting operations on the affected subtree
  list<Context*> will_redelegate;
  list<Context*> will_fail;

  C_MDS_ExportFinish(MDS *mds, CInode *in) {
	this->mds = mds;
	this->in = in;
  }

  // suck up and categorize waitlists 
  void assim_waitlist(list<Context*>& ls) {
	for (list<Context*>::iterator it = ls.begin();
		 it != ls.end();
		 it++) {
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
		if ((*lit)->can_redelegate()) 
		  will_redelegate.push_back(*lit);
		else
		  will_fail.push_back(*lit);
	  }
	}
	cmap.clear();
  }


  virtual void finish(int r) {
	if (r == 0) { // success
	  // redelegate
	  list<Context*>::iterator it;
	  for (it = will_redelegate.begin(); it != will_redelegate.end(); it++) {
		(*it)->redelegate(mds, in->dir_authority(mds->get_cluster()));
	  }

	  // fail
	  for (it = will_fail.begin(); it != will_fail.end(); it++) {
		assert(false);
		(*it)->finish(-1);  // fail
	  }	  
	} else {
	  assert(false); // now what?
	}
  }
};


void MDCache::export_dir(CInode *in,
							int dest)
{
  if (!in->dir) in->dir = new CDir(in);

  if (!in->parent) {
	cout << "i won't export root" << endl;
	throw "asdf";
	return;
  }

  cout << "mds" << mds->get_nodeid() << " export_dir " << *in << " to " << dest << ", freezing" << endl;

  if (in->dir->is_frozen() ||
	  in->dir->is_freezing()) {
	cout << " can't export, freezing|frozen.  wait for other exports to finish first." << endl;
	return;
  }

  // take away popularity (and pass it on to the context, MExportDir request later)
  double pop = in->popularity.get();
  CInode *t = in;
  while (t) {
	t->popularity.adjust(-pop);
	if (t->parent)
	  t = t->parent->dir->inode;
	else 
	  break;
  }

  // freeze the subtree
  in->dir->freeze(new C_MDS_ExportFreeze(mds, in, dest, pop));
}


void MDCache::export_dir_frozen(CInode *in,
								int dest,
								double pop)
{
  // subtree is now frozen!
  cout << "mds" << mds->get_nodeid() << " export_dir " << *in << " to " << dest << ", frozen" << endl;

  //show_imports();

  
  // update imports/exports
  CInode *containing_import = get_containing_import(in);
  if (containing_import == in) {
	cout << " i'm rexporting a previous import" << endl;
	imports.erase(in);

	in->put(CINODE_PIN_IMPORT);                  // unpin, no longer an import

	// discard nested exports (that we're handing off
	pair<multimap<CInode*,CInode*>::iterator, multimap<CInode*,CInode*>::iterator> p =
	  nested_exports.equal_range(in);
	while (p.first != p.second) {
	  CInode *nested = (*p.first).second;

	  // nested beneath our new export *in; remove!
	  cout << " export " << *nested << " was nested beneath us; removing from export list(s)" << endl;
	  //exports.erase(nested);  _walk does this
	  nested_exports.erase(p.first++);   // note this increments before call to erase
	}

  } else {
	cout << " i'm a subdir nested under import " << *containing_import << endl;
	exports.insert(in);
	nested_exports.insert(pair<CInode*,CInode*>(containing_import, in));

	in->get(CINODE_PIN_EXPORT);                  // i must keep it pinned
	
	// discard nested exports (that we're handing off)
	pair<multimap<CInode*,CInode*>::iterator, multimap<CInode*,CInode*>::iterator> p =
	  nested_exports.equal_range(containing_import);
	while (p.first != p.second) {
	  CInode *nested = (*p.first).second;
	  multimap<CInode*,CInode*>::iterator prev = p.first;
	  p.first++;

	  CInode *containing_export = get_containing_export(nested->get_parent_inode());
	  if (!containing_export) continue;

	  if (containing_export == in && nested != in) {
		// nested beneath our new export *in; remove!
		cout << " export " << *nested << " was nested beneath us; removing from nested_exports" << endl;
		// exports.erase(nested); _walk does this
		nested_exports.erase(prev);  // note this increments before call to erase
	  } else {
		cout << " huh, other export " << *nested << " is under export " << *containing_export << ", which is odd" << endl;
		assert(0);
	  }

	}

  }

  // note new authority (locally)
  in->dir_auth = dest;

  // build export message
  MExportDir *req = new MExportDir(in, pop);  // include pop
  
  // fill with relevant cache data
  C_MDS_ExportFinish *fin = new C_MDS_ExportFinish(mds, in);

  export_dir_walk( req, fin, in );

  mds->messenger->send_message(req,
							   MSG_ADDR_MDS(dest), MDS_PORT_CACHE,
							   MDS_PORT_CACHE);

}

void MDCache::export_dir_walk(MExportDir *req,
							  C_MDS_ExportFinish *fin,
							  CInode *idir)
{
  assert(idir->is_dir());
  if (!idir->dir)
	return;  // we don't ahve anything, obviously

  cout << "export_dir_walk on " << *idir << " " << idir->dir->nitems << " items" << endl;

  // dir 
  crope dir_rope;

  Dir_Export_State_t dstate;
  dstate.ino = idir->inode.ino;
  dstate.nitems = idir->dir->nitems;
  dstate.version = idir->dir->version;
  dstate.state = idir->dir->state;
  dstate.dir_rep = idir->dir->dir_rep;
  dstate.ndir_rep_by = idir->dir->dir_rep_by.size();
  dstate.popularity = idir->dir->popularity;
  dir_rope.append( (char*)&dstate, sizeof(dstate) );
  
  for (set<int>::iterator it = idir->dir->dir_rep_by.begin();
	   it != idir->dir->dir_rep_by.end();
	   it++) {
	int i = *it;
	dir_rope.append( (char*)&i, sizeof(int) );
  }

  // waiters
  list<Context*> waiting;
  idir->take_write_waiting(waiting);
  idir->take_read_waiting(waiting);
  fin->assim_waitlist(waiting);


  // inodes
  list<CInode*> subdirs;

  CDir_map_t::iterator it;
  for (it = idir->dir->begin(); it != idir->dir->end(); it++) {
	CInode *in = it->second->inode;

	// dentry
	dir_rope.append( it->first.c_str(), it->first.length()+1 );
	
	// add inode
	Inode_Export_State_t istate;
	istate.inode = in->inode;
	istate.version = in->version;
	istate.popularity = in->popularity;
	//istate.ref = in->ref;
	istate.ncached_by = in->cached_by.size();

	if (in->is_dir()) {
	  istate.dir_auth = in->dir_auth;
	  assert(in->dir_auth != mds->get_nodeid());   // should be -1

	  if (in->dir_auth == -1) {
		subdirs.push_back(in);  // it's ours, recurse.
	  } else {
		cout << " encountered nested export " << *in << "; removing from exports" << endl;
		assert(exports.count(in) == 1); 
		exports.erase(in);                    // discard nested export   (nested_exports updated above)
		in->put(CINODE_PIN_EXPORT);
	  }
	} else 
	  istate.dir_auth = -1;

	dir_rope.append( (char*)&istate, sizeof(istate) );
	
	for (set<int>::iterator it = in->cached_by.begin();
		 it != in->cached_by.end();
		 it++) {
	  int i = *it;
	  dir_rope.append( (char*)&i, sizeof(int) );
	}

	// other state too!.. open files, etc...

	// ***  

	// waiters
	fin->assim_waitlist(idir->dir->waiting_on_all);
	fin->assim_waitlist(idir->dir->waiting_on_dentry);
  }

  req->add_dir( dir_rope );
  
  // subdirs
  for (list<CInode*>::iterator it = subdirs.begin(); it != subdirs.end(); it++)
	export_dir_walk(req, fin, *it);
}


void MDCache::handle_export_dir_ack(MExportDirAck *m)
{
  // exported!
  CInode *in = mds->mdcache->get_inode(m->ino);

  cout << "mds" << mds->get_nodeid() << " export_dir_ack " << *in << endl;
  
  // remove the metadata from the cache
  if (in->dir) 
	export_dir_purge( in );

  // unfreeze
  cout << "mds" << mds->get_nodeid() << " export_dir_ack " << *in << " unfrozen" << endl;
  in->dir->unfreeze();

  show_imports();

  // done
  delete m;
}


// called by handle_expirt_dir_ack
void MDCache::export_dir_purge(CInode *idir)
{
  cout << "export_dir_purge on " << *idir << endl;

  CDir_map_t::iterator it = idir->dir->begin();
  while (it != idir->dir->end()) {
	CInode *in = it->second->inode;
	it++;
	
	if (in->is_dir() && in->dir) 
	  export_dir_purge(in);
	
	// dir incomplete!
	in->parent->dir->state_clear(CDIR_MASK_COMPLETE);

	if (!in->cached_by.empty()) {
	  in->cached_by.clear();      // only get to do this once, because we're newly non-authoritative.
	  in->put(CINODE_PIN_CACHED);  
	}

	if (in->lru_expireable) {
	  lru->lru_remove(in);
	  cout << "mds" << mds->get_nodeid() << " export_dir_purge deleting " << *in << " " << in << endl;
	  remove_inode(in);
	  delete in;
	} else {
	  cout << "mds" << mds->get_nodeid() << " export_dir_purge not deleting non-expireable " << *in << " " << in->ref_set << endl;
	}
  }

  cout << "export_dir_purge on " << *idir << " done" << endl;
}




//  IMPORTS


void MDCache::handle_export_dir(MExportDir *m)
{
  cout << "mds" << mds->get_nodeid() << " import_dir " << m->path << endl;

  show_imports();

  vector<CInode*> trav;
  
  int r = path_traverse(m->path, trav, m, MDS_TRAVERSE_DISCOVER);   // FIXME BUG
  if (r > 0)
	return;  // did something

  CInode *in = trav[trav.size()-1];
  
  in->get(CINODE_PIN_IMPORTING);  // pin for the import process only.

  if (!in->dir) in->dir = new CDir(in);

  // note new authority (locally)
  in->dir_auth = mds->get_nodeid();

  CInode *containing_import;
  if (exports.count(in)) {
	// reimporting
	cout << " i'm reimporting this dir!" << endl;
	exports.erase(in);

	in->put(CINODE_PIN_EXPORT);                // unpin, no longer an export

	containing_import = get_containing_import(in);  
	cout << "  it is nested under import " << *containing_import << endl;
	for (pair< multimap<CInode*,CInode*>::iterator, multimap<CInode*,CInode*>::iterator > p =
		   nested_exports.equal_range( containing_import );
		 p.first != p.second;
		 p.first++) {
	  if ((*p.first).second == in) {
		nested_exports.erase(p.first);
		break;
	  }
	}
  } else {
	// new import
	imports.insert(in);

	in->get(CINODE_PIN_IMPORT);                // must keep it pinned

	containing_import = in;  // imported exports nested under *in
  }

  // add this crap to my cache
  const char *p = m->state.c_str();
  for (int i = 0; i < m->ndirs; i++) 
	import_dir_block(p, containing_import);
  
  // can i simplify dir_auth?
  if (in->authority(mds->get_cluster()) == in->dir_auth)
	in->dir_auth = CDIR_AUTH_PARENT;

  // ignore "frozen" state of the main dir; it's from the authority
  in->dir->state_clear(CDIR_MASK_FROZEN);

  double newpop = m->ipop - in->popularity.get();
  cout << " imported popularity jump by " << newpop << endl;
  if (newpop > 0) {  // duh
	CInode *t = in;
	while (t) {
	  t->popularity.adjust(newpop);
	  if (t->parent) 
		t = t->parent->dir->inode;
	  else break;
	}
  }

  // send ack
  cout << "mds" << mds->get_nodeid() << " sending ack back to " << m->get_source() << endl;
  MExportDirAck *ack = new MExportDirAck(m);
  mds->messenger->send_message(ack,
							   m->get_source(), MDS_PORT_CACHE,
							   MDS_PORT_CACHE);

  // spread the word!
  if (in->authority(mds->get_cluster()) == mds->get_nodeid()) {
	// i am the authority
	send_inode_updates(in);
  } else {
	// tell the authority; they'll spread the word.
	string path;
	in->make_path(path);
	mds->messenger->send_message(new MExportDirNotify(path, in->dir_auth),
								 in->authority(mds->get_cluster()), MDS_PORT_CACHE,
								 MDS_PORT_CACHE);

  }

  in->put(CINODE_PIN_IMPORTING);   // import done, unpin.
  
  show_imports();

  // done
  delete m;
}

void MDCache::import_dir_block(pchar& p, CInode *containing_import)
{
  // set up dir
  Dir_Export_State_t *dstate = (Dir_Export_State_t*)p;
  cout << " import_dir_block " << dstate->ino << " " << dstate->nitems << " items" << endl;
  CInode *idir = get_inode(dstate->ino);
  assert(idir);

  if (!idir->dir) idir->dir = new CDir(idir);

  idir->dir->version = dstate->version;
  idir->dir->state = dstate->state;
  idir->dir->dir_rep = dstate->dir_rep;
  idir->dir->popularity = dstate->popularity;
  
  p += sizeof(*dstate);
  for (int nrep = dstate->ndir_rep_by; nrep > 0; nrep--) {
	idir->dir->dir_rep_by.insert( *((int*)p) );
	p += sizeof(int);
  }

  // contents
  for (long nitems = dstate->nitems; nitems>0; nitems--) {
	// dentry
	string dname = p;
	p += dname.length()+1;

	// inode
	Inode_Export_State_t *istate = (Inode_Export_State_t*)p;
	CInode *in = get_inode(istate->inode.ino);
	if (!in) {
	  in = new CInode;
	  in->inode = istate->inode;

	  // add
	  add_inode(in);
	  link_inode(idir, dname, in);	
	  cout << "   adding " << *in << endl;
	} else {
	  cout << "   already had " << *in << endl;
	  in->inode = istate->inode;
	}
	
	// update inode state with authoritative info
	in->version = istate->version;
	in->popularity = istate->popularity;
	
	p += sizeof(*istate);
	
	in->cached_by.clear();
	for (int nby = istate->ncached_by; nby>0; nby--) {
	  in->cached_by.insert( *((int*)p) );
	  p += sizeof(int);
	}
	if (in->cached_by.size())
	  in->get(CINODE_PIN_CACHED);              // pin bc of cached_by

	// other state? ... ?

	// was this an export?
	if (istate->dir_auth >= 0) {
	  
	  // to us?
	  if (in->dir_auth == mds->get_nodeid()) {
		// adjust the import
		cout << "mds" << mds->get_nodeid() << "  importing nested export " << *in << " to ME!  how fortuitous" << endl;
		imports.erase(in);

		// move nested exports under containing_import
		for (pair<multimap<CInode*,CInode*>::iterator, multimap<CInode*,CInode*>::iterator> p =
			   nested_exports.equal_range(in);
			 p.first != p.second;
			 p.first++) {
		  CInode *nested = (*p.first).second;
		  cout << "     moving nested export " << nested << " under " << containing_import << endl;
		  nested_exports.insert(pair<CInode*,CInode*>(containing_import, nested));
		}

		// de-list under old import
		nested_exports.erase(in);	

		in->dir_auth = CDIR_AUTH_PARENT;
		in->put(CINODE_PIN_IMPORT);       // imports are pinned, no longer import
	  } else {
		cout << "mds" << mds->get_nodeid() << "  importing nested export " << *in << " to " << istate->dir_auth << endl;
		// add this export
		in->dir_auth = istate->dir_auth;
		in->get(CINODE_PIN_EXPORT);           // all exports are pinned
		exports.insert(in);
		nested_exports.insert(pair<CInode*,CInode*>(containing_import, in));
	  }

	}
  }
 
}



// authority bystander

void MDCache::handle_export_dir_notify(MExportDirNotify *m)
{
  cout << "mds" << mds->get_nodeid() << " handle_export_dir_notify on " << m->path << " new_auth " << m->new_auth << endl;
  
  vector<CInode*> trav;
  int r = path_traverse(m->path, trav, m, MDS_TRAVERSE_FORWARD);  
  if (r != 0) {
	cout << " fwd or freeze or something" << endl;
	return;
  }
  
  CInode *in = trav[ trav.size()-1 ];

  int iauth = in->authority(mds->get_cluster());
  if (iauth != mds->get_nodeid()) {
	// or not!
	cout << " we're not the authority" << endl;
	mds->messenger->send_message(m,
								 iauth, MDS_PORT_CACHE,
								 MDS_PORT_CACHE);
	return;
  }

  // yay, we're the authority
  cout << "mds" << mds->get_nodeid() << " handle_export_dir_notify on " << *in << " new_auth " << m->new_auth << " updated, telling replicas" << endl;

  in->dir_auth = m->new_auth;
  send_inode_updates(in);
  
  // done
  delete m;
}



void MDCache::show_imports()
{
  if (imports.size() == 0) {
	cout << "mds" << mds->get_nodeid() << " no imports/exports" << endl;
	return;
  }
  cout << "mds" << mds->get_nodeid() << " imports/exports:" << endl;

  set<CInode*> ecopy = exports;

  for (set<CInode*>::iterator it = imports.begin();
	   it != imports.end();
	   it++) {
	cout << "mds" << mds->get_nodeid() << "   + import " << **it << endl;
	
	for (pair< multimap<CInode*,CInode*>::iterator, multimap<CInode*,CInode*>::iterator > p = 
		   nested_exports.equal_range( *it );
		 p.first != p.second;
		 p.first++) {
	  CInode *exp = (*p.first).second;
	  cout << "mds" << mds->get_nodeid() << "       - ex " << *exp << " to " << exp->dir_auth << endl;
	  assert( get_containing_import(exp) == *it );

	  if (ecopy.count(exp) != 1) {
		cout << " nested_export " << *exp << " not in exports" << endl;
		assert(0);
	  }
	  ecopy.erase(exp);
	}
  }

  if (ecopy.size()) {
	for (set<CInode*>::iterator it = ecopy.begin();
		 it != ecopy.end();
		 it++) 
	  cout << " stray item in exports: " << **it << endl;
	assert(ecopy.size() == 0);
  }
  

}
