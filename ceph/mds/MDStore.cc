// -*- mode:C++; tab-width:4; c-basic-offset:2; indent-tabs-mode:t -*- 
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 * 
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 * 
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA  02110-1301  USA
 */


#include "MDStore.h"
#include "MDS.h"
#include "MDCache.h"
#include "CInode.h"
#include "CDir.h"
#include "CDentry.h"
#include "MDCluster.h"

#include "osd/Filer.h"
#include "osd/OSDMap.h"

#include "msg/Message.h"

#include <cassert>
#include <iostream>
using namespace std;


#include "config.h"
#undef dout
#define  dout(l)    if (l<=g_conf.debug) cout << "mds" << mds->get_nodeid() << ".store "


/*
 * separate hashed dir slices into "regions"
 */
size_t get_hash_offset(int hashcode) {
  if (hashcode < 0)   
	return 0;  // not hashed
  else
	return (size_t)(1<<30) * (size_t)hashcode;
}




// ==========================================================================
// FETCH


class C_MDS_Fetch : public Context {
 protected:
  MDStore *ms;
  inodeno_t ino;

 public:
  C_MDS_Fetch(MDStore *ms, inodeno_t ino) : Context() {
	this->ms = ms;
	this->ino = ino;
  }
  
  void finish(int result) {
	ms->fetch_dir_2( result, ino );
  }
};

/** fetch_dir(dir, context)
 * public call to fetch a dir.
 */
void MDStore::fetch_dir( CDir *dir,
						 Context *c )
{
  dout(7) << "fetch_dir " << *dir << " context is " << c << endl;
  assert(dir->is_auth() ||
		 dir->is_hashed());

  // wait
  if (c) dir->add_waiter(CDIR_WAIT_COMPLETE, c);
  
  // already fetching?
  if (dir->state_test(CDIR_STATE_FETCHING)) {
	dout(7) << "already fetching " << *dir << "; waiting" << endl;
	return;
  }
  
  // state
  dir->state_set(CDIR_STATE_FETCHING);
  
  // stats
  mds->logger->inc("fdir");
  
  // create return context
  Context *fin = new C_MDS_Fetch( this, dir->ino() );
  if (dir->is_hashed()) 
	fetch_dir_hash( dir, fin, mds->get_nodeid());   // hashed
  else 
	fetch_dir_hash( dir, fin );                     // normal
}

/*
 * called by low level fn when it's fetched.
 * fix up dir state.
 */
void MDStore::fetch_dir_2( int result, 
						   inodeno_t ino)
{
  CInode *idir = mds->mdcache->get_inode(ino);
  
  if (!idir || result < 0) return;  // hmm!  nevermind i guess.

  assert(idir);
  CDir *dir = idir->dir;
  assert(dir);
  
  // dir is now complete
  dir->state_set(CDIR_STATE_COMPLETE);
  dir->state_clear(CDIR_STATE_FETCHING);

  // finish
  list<Context*> finished;
  dir->take_waiting(CDIR_WAIT_COMPLETE|CDIR_WAIT_DENTRY, finished);
  finish_contexts(finished, result);
}


/** low level methods **/

class C_MDS_FetchHash : public Context {
protected:
  MDS *mds;
  inode_t inode;
  int hashcode;
  Context *context;
  
public:
  bufferlist bl;
  bufferlist bl2;
  
  C_MDS_FetchHash(MDS *mds, inode_t inode, Context *c, int hashcode) : Context() {
	this->mds = mds;
	this->inode = inode;
	this->hashcode = hashcode;
	this->context = c;
  }
  
  void finish(int result) {
	assert(result>0);

	// combine bufferlists bl + bl2 -> bl
	bl.claim_append(bl2);

	// did i get the whole thing?
	size_t size;
	bl.copy(0, sizeof(size_t), (char*)&size);
	size_t got = bl.length() - sizeof(size);
	size_t left = size - got;
	size_t from = bl.length();

	// what part of dir are we getting?
	from += get_hash_offset(hashcode);
	
	if (got >= size) {
	  // done.
	  mds->mdstore->fetch_dir_hash_2( bl, inode, context, hashcode );
	}
	else {
	  // read the rest!
	  dout(12) << "fetch_dir_hash_2 dir size is " << size << ", got " << got << ", reading remaniing " << left << " from off " << from << endl;
	  
	  // create return context
	  C_MDS_FetchHash *fin = new C_MDS_FetchHash( mds, inode, context, hashcode );
	  fin->bl.claim( bl );
	  mds->filer->read(inode,
					   left, from,
					   &fin->bl2,
					   fin );
	  return;
	}
  }
};

/** fetch_dir_hash
 * low level method.
 * fetch part of a dir.  either the whole thing if hashcode is -1, or a specific 
 * hash segment.
 */
void MDStore::fetch_dir_hash( CDir *dir,
							  Context *c, 
							  int hashcode)
{
  dout(11) << "fetch_dir_hash hashcode " << hashcode << " " << *dir << endl;
  
  // create return context
  C_MDS_FetchHash *fin = new C_MDS_FetchHash( mds, dir->get_inode()->inode, c, hashcode );
  
  // grab first stripe bit (which had better be more than 16 bytes!)
  assert(dir->get_inode()->inode.layout.stripe_size >= 16);
  mds->filer->read(dir->get_inode()->inode,
				   dir->get_inode()->inode.layout.stripe_size, get_hash_offset(hashcode),  
				   &fin->bl,
				   fin );
}

void MDStore::fetch_dir_hash_2( bufferlist& bl,
								inode_t& inode,
								Context *c,						   
								int hashcode)
{
  CInode *idir = mds->mdcache->get_inode(inode.ino);
  if (!idir) {
	dout(7) << "fetch_dir_hash_2 on ino " << inode.ino << " but no longer in our cache!" << endl;
	c->finish(-1);
	delete c;
	return;
  } 

  if (!idir->dir_is_auth() ||
	  !idir->dir) {
	dout(7) << "fetch_dir_hash_2 on " << *idir << ", but i'm not auth, or dir not open" << endl;
	c->finish(-1);
	delete c;
	return;
  } 

  // make sure we have a CDir
  CDir *dir = idir->get_or_open_dir(mds);
  
  // do it
  dout(7) << "fetch_dir_hash_2 hashcode " << hashcode << " dir " << *dir << endl;
  
  // parse buffer contents into cache
  dout(15) << "bl is " << bl << endl;
  size_t size;
  bl.copy(0, sizeof(size), (char*)&size);
  assert(bl.length() >= size + sizeof(size));  
  
  int n;
  bl.copy(sizeof(size), sizeof(n), (char*)&n);
  
  char *buffer = bl.c_str();      // contiguous ptr to whole buffer(list)  
  size_t buflen = bl.length();
  size_t p = sizeof(size_t);
  
  __uint32_t num = *(__uint32_t*)(buffer + p);
  p += sizeof(num);
  
  dout(10) << "  " << num << " items in " << size << " bytes" << endl;

  unsigned parsed = 0;
  while (parsed < num) {
	assert(p < buflen && num > 0);
	parsed++;
	
	dout(24) << " " << parsed << "/" << num << " pos " << p-8 << endl;

	// dentry
	string dname = buffer+p;
	p += dname.length() + 1;
	dout(24) << "parse filename '" << dname << "'" << endl;
	
	CDentry *dn = dir->lookup(dname);  // existing dentry?
	
	if (*(buffer+p) == 'L') {
	  // hard link, we don't do that yet.
	  p++;

	  inodeno_t ino = *(inodeno_t*)(buffer+p);
	  p += sizeof(ino);

	  // what to do?
	  if (hashcode >= 0) {
		int dentryhashcode = mds->get_cluster()->hash_dentry( dir->ino(), dname );
		assert(dentryhashcode == hashcode);
	  }

	  if (dn) {
		if (dn->get_inode() == 0) {
		  // negative dentry?
		  dout(12) << "readdir had NEG dentry " << dname << endl;
		} else {
		  // had dentry
		  dout(12) << "readdir had dentry " << dname << endl;
		}
		continue;
	  }

	  // (remote) link
	  CDentry *dn = dir->add_dentry( dname, ino );

	  // link to inode?
	  CInode *in = mds->mdcache->get_inode(ino);   // we may or may not have it.
	  if (in) {
		dn->link_remote(in);
		dout(12) << "readdir got remote link " << ino << " which we have " << *in << endl;
	  } else {
		dout(12) << "readdir got remote link " << ino << " (dont' have it)" << endl;
	  }
	} 
	else if (*(buffer+p) == 'I') {
	  // inode
	  p++;
	  
	  // parse out inode
	  inode_t *inode = (inode_t*)(buffer+p);
	  p += sizeof(inode_t);

	  string symlink;
	  if ((inode->mode & INODE_TYPE_MASK) == INODE_MODE_SYMLINK) {
		symlink = (char*)(buffer+p);
		p += symlink.length() + 1;
	  }

	  // what to do?
	  if (hashcode >= 0) {
		int dentryhashcode = mds->get_cluster()->hash_dentry( dir->ino(), dname );
		assert(dentryhashcode == hashcode);
	  }
	  
	  if (dn) {
		if (dn->get_inode() == 0) {
		  // negative dentry?
		  dout(12) << "readdir had NEG dentry " << dname << endl;
		} else {
		  // had dentry
		  dout(12) << "readdir had dentry " << dname << endl;
		}
		continue;
	  }
	  
	  // add inode
	  CInode *in = 0;
	  if (mds->mdcache->have_inode(inode->ino)) {
		in = mds->mdcache->get_inode(inode->ino);
		dout(12) << "readdir got (but i already had) " << *in << " mode " << in->inode.mode << " mtime " << in->inode.mtime << endl;
	  } else {
		// inode
		in = new CInode();
		memcpy(&in->inode, inode, sizeof(inode_t));
		
		// symlink?
		if (in->is_symlink()) {
		  in->symlink = symlink;
		}
		
		// add 
		mds->mdcache->add_inode( in );
	  }

	  // link
	  dir->add_dentry( dname, in );
	  dout(12) << "readdir got " << *in << " mode " << in->inode.mode << " mtime " << in->inode.mtime << endl;
	}
	else {
	  dout(1) << "corrupt directory, i got tag char '" << *(buffer+p) << "' val " << (int)(*(buffer+p)) << " at pos " << p << endl;
	  assert(0);
	}
  }
  dout(15) << "parsed " << parsed << endl;
  
  if (c) {
	c->finish(0);
	delete c;
  }
}




// ==================================================================
// COMMIT

class C_MDS_CommitDirVerify : public Context {
public:
  MDS *mds;
  inodeno_t ino;
  __uint64_t version;
  Context *c;
  
  C_MDS_CommitDirVerify( MDS *mds, 
						inodeno_t ino, 
						__uint64_t version,
						Context *c) {
	this->mds = mds;
	this->c = c;
	this->version = version;
	this->ino = ino;
  }
  
  virtual void finish(int r) {

	if (r >= 0) {
	  CInode *in = mds->mdcache->get_inode(ino);
	  assert(in && in->dir);
	  if (in && in->dir && in->dir->is_auth()) {
		dout(7) << "CommitDirVerify: current version = " << in->dir->get_version() << endl;
		dout(7) << "CommitDirVerify:  last committed = " << in->dir->get_last_committed_version() << endl;
   		dout(7) << "CommitDirVerify:        required = " << version << endl;
		
		if (in->dir->get_last_committed_version() >= version) {
		  dout(7) << "my required version is safe, done." << endl;
		} else { 
		  dout(7) << "my required version is still not safe, committing again." << endl;

		  // what was requested isn't committed yet.
		  mds->mdstore->commit_dir(in->dir, 
								   version,
								   c);
		  return;
		}
	  }
	}
	
	// must have exported ors omethign!
	dout(7) << "can't retry commit dir on " << ino << ", must have exported?" << endl;
	if (c) {
	  c->finish(-1);
	  delete c;
	}
  }
};

class C_MDS_CommitDirFinish : public Context {
 protected:
  MDStore *ms;
  CDir *dir;
  __uint64_t version;

 public:

  C_MDS_CommitDirFinish(MDStore *ms, CDir *dir) : Context() {
	this->ms = ms;
	this->dir = dir;
	this->version = dir->get_version();   // just for sanity check later
  }
  
  void finish(int result) {
	ms->commit_dir_2( result, dir, version );
  }
};


void MDStore::commit_dir( CDir *dir,
						  Context *c )
{
  assert(dir->is_dirty());
  
  // commit thru current version
  commit_dir(dir, dir->get_version(), c);
}

void MDStore::commit_dir( CDir *dir,
						  __uint64_t version,
						  Context *c )
{
  assert(dir->is_auth() ||
		 dir->is_hashed());
  
  // already committing?
  if (dir->state_test(CDIR_STATE_COMMITTING)) {
	// already mid-commit!
	dout(7) << "commit_dir " << *dir << " mid-commit of " << dir->get_committing_version() << endl;
	dout(7) << "  current version = " << dir->get_version() << endl;
	dout(7) << "requested version = " << version << endl;

	assert(version >= dir->get_last_committed_version());  // why would we request _old_ one?

	dir->add_waiter(CDIR_WAIT_COMMITTED, 
					new C_MDS_CommitDirVerify(mds, dir->ino(), version, c) );
	return;
  }

  if (!dir->can_auth_pin()) {
	// something must be frozen up the hiearchy!
	dout(7) << "commit_dir " << *dir << " can't auth_pin, waiting" << endl;
	dir->add_waiter(CDIR_WAIT_AUTHPINNABLE,
					new C_MDS_CommitDirVerify(mds, dir->ino(), version, c) );
	return;
  }


  // is it complete?
  if (!dir->is_complete()) {
	dout(7) << "commit_dir " << *dir << " not complete, fetching first" << endl;
	// fetch dir first
	fetch_dir(dir, 
			  new C_MDS_CommitDirVerify(mds, dir->ino(), version, c) );
	return;
  }


  // ok go
  dout(7) << "commit_dir " << *dir << " version " << dir->get_version() << endl;

  // add waiter
  if (c) dir->add_waiter(CDIR_WAIT_COMMITTED, c);

  // get continuation ready
  Context *fin = new C_MDS_CommitDirFinish(this, dir);
  
  // state
  dir->state_set(CDIR_STATE_COMMITTING);
  dir->set_committing_version(); 

  // stats
  mds->logger->inc("cdir");

  if (dir->is_hashed()) {
	// hashed
	commit_dir_slice( dir, fin, mds->get_nodeid() );
  } else {
	// non-hashed
    commit_dir_slice( dir, fin );
  }
}

void MDStore::commit_dir_2( int result,
							CDir *dir,
							__uint64_t committed_version)
{
  dout(5) << "commit_dir_2 " << *dir << " committed " << committed_version << ", current version " << dir->get_version() << endl;
  assert(committed_version == dir->get_committing_version());

  // remember which version is now safe
  dir->set_last_committed_version(committed_version);
  
  // is the dir now clean?
  if (committed_version == dir->get_version())
	dir->mark_clean();
 
  dir->state_clear(CDIR_STATE_COMMITTING);

  // finish
  dir->finish_waiting(CDIR_WAIT_COMMITTED);
}




// low-level committer (hashed or normal)

class C_MDS_CommitSlice : public Context {
 protected:
  MDStore *ms;
  CDir *dir;
  Context *c;
  int hashcode;
  __uint64_t version;

public:
  bufferlist bl;

  C_MDS_CommitSlice(MDStore *ms, CDir *dir, Context *c, int w) : Context() {
	this->ms = ms;
	this->dir = dir;
	this->c = c;
	this->hashcode = w;
	version = dir->get_version();
  }
  
  void finish(int result) {
	ms->commit_dir_slice_2( result, dir, c, version, hashcode );
  }
};


void MDStore::commit_dir_slice( CDir *dir,
							   Context *c,
							   int hashcode)
{
  if (hashcode >= 0) {
	assert(dir->is_hashed());
	dout(11) << "commit_dir_slice hashcode " << hashcode << " " << *dir << " version " << dir->get_version() << endl;
  } else {
	assert(dir->is_auth());
	dout(11) << "commit_dir_slice (whole dir) " << *dir << " version " << dir->get_version() << endl;
  }
  
  // get continuation ready
  C_MDS_CommitSlice *fin = new C_MDS_CommitSlice(this, dir, c, hashcode);
  
  // fill buffer
  __uint32_t num = 0;
  
  bufferlist dirdata;
  
  for (CDir_map_t::iterator it = dir->begin();
	   it != dir->end();
	   it++) {
	CDentry *dn = it->second;

	if (hashcode >= 0) {
	  int dentryhashcode = mds->get_cluster()->hash_dentry( dir->ino(), it->first );
	  if (dentryhashcode != hashcode) continue;
	}

	// put dentry in this version
	if (dn->is_dirty()) {
	  dn->float_parent_dir_version( dir->get_version() );
	  dout(12) << " dirty dn " << *dn << " now " << dn->get_parent_dir_version() << endl;
	}
	
	if (dn->is_null()) continue;  // skipping negative entry

	// primary or remote?
	if (dn->is_remote()) {

	  inodeno_t ino = dn->get_remote_ino();
	  dout(14) << " pos " << dirdata.length() << " dn '" << it->first << "' remote ino " << ino << endl;

	  // name, marker, ion
	  dirdata.append( it->first.c_str(), it->first.length() + 1);
	  dirdata.append( "L", 1 );         // remote link
	  dirdata.append((char*)&ino, sizeof(ino));

	} else {
	  // primary link
	  CInode *in = dn->get_inode();
	  assert(in);

	  dout(14) << " pos " << dirdata.length() << " dn '" << it->first << "' inode " << *in << endl;
  
	  // name, marker, inode, [symlink string]
	  dirdata.append( it->first.c_str(), it->first.length() + 1);
	  dirdata.append( "I", 1 );         // inode
	  dirdata.append( (char*) &in->inode, sizeof(inode_t));
	  
	  if (in->is_symlink()) {
		// include symlink destination!
		dout(18) << "    inlcuding symlink ptr " << in->symlink << endl;
		dirdata.append( (char*) in->symlink.c_str(), in->symlink.length() + 1);
	  }
	  
	  // put inode in this dir version
	  if (in->is_dirty()) {
		in->float_parent_dir_version( dir->get_version() );
		dout(12) << " dirty inode " << *in << " now " << in->get_parent_dir_version() << endl;
	  }
	}

	num++;
  }
  dout(14) << "num " << num << endl;
  
  // put count in buffer
  //bufferlist bl;
  size_t size = sizeof(num) + dirdata.length();
  fin->bl.append((char*)&size, sizeof(size));
  fin->bl.append((char*)&num, sizeof(num));
  fin->bl.claim_append(dirdata);  //.c_str(), dirdata.length());
  assert(fin->bl.length() == size + sizeof(size));
  
  // pin inode
  dir->auth_pin();
  
  // submit to osd
  mds->filer->write( dir->get_inode()->inode,
					 fin->bl.length(), 0,
					 fin->bl,
					 0, //OSD_OP_FLAGS_TRUNCATE, // truncate file/object after end of this write
					 NULL, fin ); // on safe
}


void MDStore::commit_dir_slice_2( int result,
								 CDir *dir,
								 Context *c,
								 __uint64_t committed_version,
								 int hashcode )
{
  dout(11) << "commit_dir_slice_2 hashcode " << hashcode << " " << *dir << " v " << committed_version << endl;
  
  // mark inodes and dentries clean too (if we committed them!)
  list<CDentry*> null_clean;
  for (CDir_map_t::iterator it = dir->begin();
	   it != dir->end(); ) {
	CDentry *dn = it->second;
	it++;
	
	if (hashcode >= 0) {
	  int dentryhashcode = mds->get_cluster()->hash_dentry( dir->ino(), dn->get_name() );
	  if (dentryhashcode != hashcode) continue;
	}

	// dentry
	if (committed_version > dn->get_parent_dir_version()) {
	  dout(15) << " dir " << committed_version << " > dn " << dn->get_parent_dir_version() << " still clean " << *dn << endl;
	  assert(!dn->is_dirty());
	}
	else if (dn->get_parent_dir_version() == committed_version) {
	  dout(15) << " dir " << committed_version << " == dn " << dn->get_parent_dir_version() << " now clean " << *dn << endl;
	  if (dn->is_dirty())
		dn->mark_clean();     // might not but could be dirty
	  
	  // remove, if it's null and unlocked
	  if (dn->is_null() && dn->is_sync()) {
		dout(15) << "   removing clean and null " << *dn << endl;
		null_clean.push_back(dn);
		continue;
	  }
	} else {
	  dout(15) << " dir " << committed_version << " < dn " << dn->get_parent_dir_version() << " still dirty " << *dn << endl;
	  assert(committed_version < dn->get_parent_dir_version());
	  //assert(dn->is_dirty() || !dn->is_sync());  // -OR- we did a fetch_dir in order to do a newer commit...
	}

	// only do primary...
	if (!dn->is_primary()) continue;
	
	CInode *in = dn->get_inode();
	assert(in);
	assert(in->is_auth());
	
	if (committed_version > in->get_parent_dir_version()) {
	  dout(15) << " dir " << committed_version << " > inode " << in->get_parent_dir_version() << " still clean " << *(in) << endl;
	  assert(!in->is_dirty());
	}
	else if (in->get_parent_dir_version() == committed_version) {
	  dout(15) << " dir " << committed_version << " == inode " << in->get_parent_dir_version() << " now clean " << *(in) << endl;
	  in->mark_clean();     // might not but could be dirty
	} else {
	  dout(15) << " dir " << committed_version << " < inode " << in->get_parent_dir_version() << " still dirty " << *(in) << endl;
	  assert(committed_version < in->get_parent_dir_version());
	  //assert(in->is_dirty());  // -OR- we did a fetch_dir in order to do a newer commit...
	}
  }

  // remove null clean dentries
  for (list<CDentry*>::iterator it = null_clean.begin();
	   it != null_clean.end();
	   it++) 
	dir->remove_dentry(*it);
  
  // unpin
  dir->auth_unpin();

  // finish
  if (c) {
	c->finish(0);
	delete c;
  }
}












