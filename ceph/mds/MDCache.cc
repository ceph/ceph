
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

	// remove it
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
	root->dir->dir_auth = 0;  // me!
	root->dir->dir_rep = CDIR_REP_ALL;

	set_root( root );

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
		cout << "mds" << whoami << " dir is frozen, waiting" << endl;
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
			cout << "mds" << whoami << " incomplete dir contents for " << cur->inode.ino << ", fetching" << endl;
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

			cur->get();  // pin discoveree

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
									MSG_ADDR_MDS(dauth), MDS_PORT_SERVER,
									MDS_PORT_SERVER);
			return 1;
		  }	
		  if (onfail == MDS_TRAVERSE_FAIL) {
			return -1;
		  }
		}
	  }
	} else {
	  cout << cur->inode.ino << " not a dir " << cur->inode.isdir << endl;
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
	  cout << "got root" << endl;
	  
	  CInode *root = new CInode();
	  root->inode = dis->trace[0].inode;
	  root->cached_by = dis->trace[0].cached_by;
	  root->cached_by.insert(whoami);
	  root->dir = new CDir(root);
	  root->dir->dir_auth = dis->trace[0].dir_auth;
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

	int r = path_traverse(dis->basepath, trav, NULL, MDS_TRAVERSE_FAIL);   // FIXME BUG
	if (r != 0) throw "wtf";
	
	CInode *cur = trav[trav.size()-1];
	CInode *start = cur;

	cur->put(); // unpin

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
		in->inode = dis->trace[i].inode;
		in->cached_by = dis->trace[i].cached_by;
		in->cached_by.insert(whoami);
		if (in->is_dir()) {
		  in->dir = new CDir(in);
		  in->dir->dir_auth = dis->trace[i].dir_auth;
		  in->dir->dir_rep = dis->trace[i].dir_rep;
		  in->dir->dir_rep_by = dis->trace[i].dir_rep_by;
		}
		
		add_inode( in );
		link_inode( cur, (*dis->want)[i], in );
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
	  MDiscoverRec_t bit;
	  bit.inode = root->inode;
	  bit.cached_by = root->cached_by;
	  bit.cached_by.insert( whoami );
	  bit.dir_auth = root->dir->dir_auth;
	  bit.dir_rep = root->dir->dir_rep;
	  bit.dir_rep_by = root->dir->dir_rep_by;
	  dis->add_bit(bit);

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
		  MDiscoverRec_t bit;
		  bit.inode = next->inode;
		  bit.cached_by = next->cached_by;
		  bit.cached_by.insert( whoami );
		  if (next->is_dir()) {
			bit.dir_auth = next->dir->dir_auth;
			bit.dir_rep = next->dir->dir_rep;
			bit.dir_rep_by = next->dir->dir_rep_by;
		  }
		  dis->add_bit(bit);
		  
		  // remember who is caching this!
		  if (next->cached_by.empty()) 
			next->get();
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
		// don't have it.
		if (cur->dir->is_complete()) {
		  // file not found.
		  throw "implement me";
		} else {
		  // readdir
		  cout << "mds" << whoami << " incomplete dir contents for " << cur->inode.ino << ", fetching" << endl;
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
	cout << "mds" << mds->get_nodeid() << " sending inode_update on " << in->inode.ino << " to " << *it << endl;
	mds->messenger->send_message(new MInodeUpdate(in->inode,
												  in->cached_by),
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
  cout << "mds" << mds->get_nodeid() << " inode_update on " << m->inode.ino << endl;

  in->inode = m->inode;
  in->cached_by = m->cached_by;

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
	cout << "mds" << mds->get_nodeid() << " inode_expire on " << m->ino << ", not mine" << endl;
	delete m;
	return;
  }

  // remove
  cout << "mds" << mds->get_nodeid() << " inode_expire on " << m->ino << " from mds" << m->get_source() << endl;

  in->cached_by.erase(m->get_source());
  if (in->cached_by.empty())
	in->put();
  
  delete m;
}


int MDCache::send_dir_updates(CDir *dir, int except)
{
  int whoami = mds->get_nodeid();
  set<int>::iterator it;
  for (it = dir->inode->cached_by.begin(); it != dir->inode->cached_by.end(); it++) {
	if (*it == whoami) continue;
	if (*it == except) continue;
	cout << "mds" << whoami << " sending dir_update on " << dir->inode->inode.ino << " to " << *it << endl;
	mds->messenger->send_message(new MDirUpdate(dir->inode->inode.ino,
												dir->dir_auth,
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

  in->dir->dir_auth = m->dir_auth;
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

  cout << "mds" << mds->get_nodeid() << " sync_start " << in->inode.ino << ", sending ack" << endl;

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

public:
  C_MDS_ExportFreeze(MDS *mds, CInode *in, int dest) {
	this->mds = mds;
	this->in = in;
	this->dest = dest;
  }
  virtual void finish(int r) {
	mds->mdcache->export_dir_frozen(in,dest);
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

  virtual void finish(int r) {
	if (r == 0) { // success
	  // redelegate
	  list<Context*>::iterator it;
	  for (it = will_redelegate.begin(); it != will_redelegate.end(); it++) {
		(*it)->redelegate(mds, in->dir->dir_authority(mds->get_cluster()));
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

  string path;
  in->make_path(path);
  cout << "mds" << mds->get_nodeid() << " export_dir " << path << " to " << dest << ", freezing" << endl;

  // freeze the subtree
  in->dir->freeze(new C_MDS_ExportFreeze(mds, in, dest));
}

void MDCache::export_dir_frozen(CInode *in,
								int dest)
{
  // subtree is now frozen!
  string path;
  in->make_path(path);
  cout << "mds" << mds->get_nodeid() << " export_dir " << path << " to " << dest << ", frozen" << endl;

  // give it away
  in->dir->dir_auth = dest;

  MExportDir *req = new MExportDir(in);
  
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
  
  list<CInode*> dirs;

  CDir_map_t::iterator it;
  for (it = idir->dir->begin(); it != idir->dir->end(); it++) {
	CInode *in = it->second->inode;
	
	// dir?
	if (in->is_dir()) 
	  dirs.push_back(in);

	// add
		
	
  }
  
}


void MDCache::handle_export_dir_ack(MExportDirAck *m)
{
  // exported!
  CInode *in = mds->mdcache->get_inode(m->ino);

  string path;
  in->make_path(path);
  cout << "mds" << mds->get_nodeid() << " export_dir_ack " << path << endl;
  
  in->get();   // pin it

  // remove the metadata from the cache
  // ...

  // unfreeze
  in->dir->unfreeze();

  // done
  delete m;
}


void MDCache::handle_export_dir(MExportDir *m)
{
  cout << "mds" << mds->get_nodeid() << " import_dir " << m->path << endl;

  vector<CInode*> trav;
  
  int r = path_traverse(m->path, trav, m, MDS_TRAVERSE_DISCOVER);   // FIXME BUG
  if (r > 0)
	return;  // did something

  CInode *in = trav[trav.size()-1];
  
  if (!in->dir) in->dir = new CDir(in);

  // it's mine, now!
  in->dir->dir_auth = mds->get_nodeid();


  // add this crap to my cache

  
  // send ack
  cout << "mds" << mds->get_nodeid() << " sending ack back to " << m->get_source() << endl;
  MExportDirAck *ack = new MExportDirAck(m);
  mds->messenger->send_message(ack,
							   m->get_source(), MDS_PORT_CACHE,
							   MDS_PORT_CACHE);

  // tell everyone the news
  send_dir_updates(in->dir, m->get_source());

  // done
  delete m;
}


