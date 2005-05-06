
#include "MDStore.h"
#include "MDS.h"
#include "MDCache.h"
#include "CInode.h"
#include "CDir.h"
#include "CDentry.h"
#include "MDCluster.h"

#include "msg/Message.h"

#include <cassert>
#include <iostream>
using namespace std;


#include "include/config.h"
#undef dout
#define  dout(l)    if (l<=g_conf.debug) cout << "mds" << mds->get_nodeid() << ".store "



void MDStore::proc_message(Message *m)
{
  switch (m->get_type()) {

  default:
	dout(7) << "store unknown message " << m->get_type() << endl;
	assert(0);
  }
}



// == fetch_dir


class MDFetchDirContext : public Context {
 protected:
  MDStore *ms;
  inodeno_t ino;

 public:
  MDFetchDirContext(MDStore *ms, inodeno_t ino) : Context() {
	this->ms = ms;
	this->ino = ino;
  }
  
  void finish(int result) {
	ms->fetch_dir_2( result, ino );
  }
};


bool MDStore::fetch_dir( CDir *dir,
						 Context *c )
{
  dout(7) << "fetch_dir " << *dir << " context is " << c << endl;
  if (c) 
	dir->add_waiter(CDIR_WAIT_COMPLETE, c);

  assert(dir->is_auth());

  // already fetching?
  if (dir->state_test(CDIR_STATE_FETCHING)) {
	dout(7) << "already fetching " << *dir << "; waiting" << endl;
	return true;
  }
  
  dir->state_set(CDIR_STATE_FETCHING);

  // create return context
  MDFetchDirContext *fin = new MDFetchDirContext( this, dir->ino() );
  
  if (dir->is_hashed()) 
	do_fetch_dir( dir, fin, mds->get_nodeid());   // hashed
  else 
	do_fetch_dir( dir, fin );  // normal
}

bool MDStore::fetch_dir_2( int result, 
						   inodeno_t ino)
{
  CInode *idir = mds->mdcache->get_inode(ino);
  if (result < 0) 
	dout(7) << *mds << "fetch_dir_2 failed on " << ino << endl;
  
  if (!idir) return false;

  assert(idir);
  assert(idir->dir);
  
  // dir is now complete
  if (result >= 0)
	idir->dir->state_set(CDIR_STATE_COMPLETE);
  idir->dir->state_clear(CDIR_STATE_FETCHING);

  // finish
  list<Context*> finished;
  idir->dir->take_waiting(CDIR_WAIT_COMPLETE|CDIR_WAIT_DENTRY, finished);
  
  finish_contexts(finished);
  
  // trim cache?
  mds->mdcache->trim();
}








// ----------------------------
// commit_dir

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
  crope buffer;

  C_MDS_CommitDirFinish(MDStore *ms, CDir *dir) : Context() {
	this->ms = ms;
	this->dir = dir;
	this->version = dir->get_version();   // just for sanity check later
  }
  
  void finish(int result) {
	ms->commit_dir_2( result, dir, version );
  }
};




bool MDStore::commit_dir( CDir *dir,
						  Context *c )
{
  assert(dir->is_dirty());

  if (!dir->is_dirty()) {
	dout(7) << "commit_dir not dirty " << *dir << endl;
	if (c) {
	  c->finish(0);
	  delete c;
	}
  }

  // commit thru current version
  commit_dir(dir, dir->get_version(), c);
}

bool MDStore::commit_dir( CDir *dir,
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
	return false;
  }

  if (!dir->can_auth_pin()) {
	// something must be frozen up the hiearchy!
	dout(7) << "commit_dir " << *dir << " can't auth_pin, waiting" << endl;
	dir->add_waiter(CDIR_WAIT_AUTHPINNABLE,
					new C_MDS_CommitDirVerify(mds, dir->ino(), version, c) );
	return false;
  }


  // is it complete?
  if (!dir->is_complete()) {
	dout(7) << "commit_dir " << *dir << " not complete, fetching first" << endl;
	// fetch dir first
	fetch_dir(dir, 
			  new C_MDS_CommitDirVerify(mds, dir->ino(), version, c) );
	return false;
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

  if (dir->is_hashed()) {
	// hashed
	do_commit_dir( dir, fin, mds->get_nodeid() );
  } else {
	// non-hashed
    do_commit_dir( dir, fin );
  }
}

bool MDStore::commit_dir_2( int result,
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

class MDDoCommitDirContext : public Context {
 protected:
  MDStore *ms;
  CDir *dir;
  Context *c;
  int hashcode;
  __uint64_t version;

public:
  crope buffer;
  
  MDDoCommitDirContext(MDStore *ms, CDir *dir, Context *c, int w) : Context() {
	this->ms = ms;
	this->dir = dir;
	this->c = c;
	this->hashcode = w;
	version = dir->get_version();
  }
  
  void finish(int result) {
	ms->do_commit_dir_2( result, dir, c, version, hashcode );
  }
};


void MDStore::do_commit_dir( CDir *dir,
							 Context *c,
							 int hashcode)
{
  assert(dir->is_auth());
  
  dout(11) << "do_commit_dir hashcode " << hashcode << " " << *dir << " version " << dir->get_version() << endl;
  
  // get continuation ready
  MDDoCommitDirContext *fin = new MDDoCommitDirContext(this, dir, c, hashcode);
  
  // fill buffer
  __uint32_t num = 0;
  
  crope dirdata;
  
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
	
	CInode *in = dn->get_inode();
	if (!in) continue;  // skip negative dentries
	
	// name
	dirdata.append( it->first.c_str(), it->first.length() + 1);
	
	// marker
	dirdata.append( 'I' );         // inode
	
	// inode
	dirdata.append( (char*) &in->inode, sizeof(inode_t));

	if (in->is_symlink()) {
	  // include symlink destination!
	  dirdata.append( (char*) in->symlink.c_str(), in->symlink.length() + 1);
	}
	
	// put inode in this dir version
	if (in->is_dirty()) {
	  in->float_parent_dir_version( dir->get_version() );
	  dout(12) << " dirty inode " << *in << " now " << in->get_parent_dir_version() << endl;
	}
	
	num++;
  }
  
  // put count in buffer
  fin->buffer.append((char*)&num, sizeof(num));
  fin->buffer.append(dirdata);
  
  // pin inode
  dir->auth_pin();
  
  // submit to osd
  int osd;
  object_t oid;
  if (hashcode >= 0) {
	// hashed
	osd = mds->mdcluster->get_hashdir_meta_osd(dir->ino(), hashcode);
	oid = mds->mdcluster->get_hashdir_meta_oid(dir->ino(), hashcode);
  } else {
	// normal
	osd = mds->mdcluster->get_meta_osd(dir->ino());
	oid = mds->mdcluster->get_meta_oid(dir->ino());
  }
  
  mds->osd_write( osd, oid, 
				  fin->buffer.length(), 0,
				  fin->buffer, 
				  0, // flags
				  fin );
}


void MDStore::do_commit_dir_2( int result,
							   CDir *dir,
							   Context *c,
							   __uint64_t committed_version,
							   int hashcode )
{
  dout(11) << "do_commit_dir_2 hashcode " << hashcode << " " << *dir << endl;
  
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
	  dout(5) << " dir " << committed_version << " > dn " << dn->get_parent_dir_version() << " still clean " << *dn << endl;
	  assert(!dn->is_dirty());
	}
	else if (dn->get_parent_dir_version() == committed_version) {
	  dout(5) << " dir " << committed_version << " == dn " << dn->get_parent_dir_version() << " now clean " << *dn << endl;
	  dn->mark_clean();     // might not but could be dirty
	  
	  // remove, if it's null and unlocked
	  if (dn->is_null() && dn->is_sync()) {
		dout(5) << "   removing clean and null " << *dn << endl;
		null_clean.push_back(dn);
		continue;
	  }
	} else {
	  dout(5) << " dir " << committed_version << " < dn " << dn->get_parent_dir_version() << " still dirty " << *dn << endl;
	  assert(committed_version < dn->get_parent_dir_version());
	  assert(dn->is_dirty() || !dn->is_sync());
	}

	// inode  FIXME: if primary link!
	CInode *in = dn->get_inode();
	if (!in) {
	  assert(dn->is_dirty() || !dn->is_sync());
	  continue;
	}
			 
	assert(in->is_auth());
	
	if (committed_version > in->get_parent_dir_version()) {
	  dout(5) << " dir " << committed_version << " > inode " << in->get_parent_dir_version() << " still clean " << *(in) << endl;
	  assert(!in->is_dirty());
	}
	else if (in->get_parent_dir_version() == committed_version) {
	  dout(5) << " dir " << committed_version << " == inode " << in->get_parent_dir_version() << " now clean " << *(in) << endl;
	  in->mark_clean();     // might not but could be dirty
	} else {
	  dout(5) << " dir " << committed_version << " < inode " << in->get_parent_dir_version() << " still dirty " << *(in) << endl;
	  assert(committed_version < in->get_parent_dir_version());
	  assert(in->is_dirty());
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










class MDDoFetchDirContext : public Context {
 protected:
  MDStore *ms;
  inodeno_t ino;
  int hashcode;
  Context *context;

 public:
  crope buffer;

  MDDoFetchDirContext(MDStore *ms, inodeno_t ino, Context *c, int which) : Context() {
	this->ms = ms;
	this->ino = ino;
	this->hashcode = which;
	this->context = c;
  }
  
  void finish(int result) {
	ms->do_fetch_dir_2( result, buffer, ino, context, hashcode );
  }
};


void MDStore::do_fetch_dir( CDir *dir,
							Context *c, 
							int hashcode)
{

  dout(11) << "fetch_hashed_dir hashcode " << hashcode << " " << *dir << " context is " << c << endl;
  
  // create return context
  MDDoFetchDirContext *fin = new MDDoFetchDirContext( this, dir->ino(), c, hashcode );

  // issue osd read
  int osd;
  object_t oid;
  if (hashcode >= 0) {
	// hashed
	osd = mds->mdcluster->get_hashdir_meta_osd(dir->ino(), hashcode);
	oid = mds->mdcluster->get_hashdir_meta_oid(dir->ino(), hashcode);
  } else {
	// normal
	osd = mds->mdcluster->get_meta_osd(dir->ino());
	oid = mds->mdcluster->get_meta_oid(dir->ino());
  }
  
  mds->osd_read( osd, oid, 
				 0, 0,
				 &fin->buffer,
				 fin );
}

void MDStore::do_fetch_dir_2( int result, 
							  crope buffer, 
							  inodeno_t ino,
							  Context *c,						   
							  int hashcode)
{
  CInode *idir = mds->mdcache->get_inode(ino);
  if (!idir) {
	dout(7) << *mds << "do_fetch_dir_2 on ino " << ino << " but no longer in our cache!" << endl;
	c->finish(-1);
	delete c;
	return;
  } 

  if (!idir->dir_is_auth()) {
	dout(7) << *mds << "do_fetch_dir_2 on " << *idir << ", but i'm not auth" << endl;
	c->finish(-1);
	delete c;
	return;
  } 

  // make sure we have a CDir
  CDir *dir = idir->get_or_open_dir(mds);
  
  // do it
  dout(7) << *mds << "do_fetch_dir_2 hashcode " << hashcode << " dir " << *dir << endl;
  
  // parse buffer contents into cache
  const char *buf = buffer.c_str();
  long buflen = buffer.length();
  
  __uint32_t num = *((__uint32_t*)buf);
  dout(10) << "  " << num << " items" << endl;
  size_t p = 4;
  int parsed = 0;
  while (parsed < num) {
	assert(p < buflen && num > 0);
	parsed++;
	
	// dentry
	string dname = buf+p;
	p += dname.length() + 1;
	//dout(7) << "parse filename " << dname << endl;
	
	CDentry *dn = dir->lookup(dname);  // existing dentry?

	
	if (*(buf+p) == 'L') {
	  // hard link, we don't do that yet.
	  assert(0);
	} 
	else if (*(buf+p) == 'I') {
	  // inode
	  p++;
	  
	  // parse out inode
	  inode_t *inode = (inode_t*)(buf+p);
	  p += sizeof(inode_t);

	  string symlink;
	  if (inode->mode & INODE_MODE_SYMLINK) {
		symlink = (char*)(buf+p);
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
  }
  
  c->finish(0);
  delete c;
}




// hashing
/*
class C_MDS_HashDir : public Context {
public:
  MDS *mds;
  CInode *in;
  Context *c;
  set<int> *left;
  int hashcode;

  C_MDS_HashDir(MDS *mds, CInode *in, Context *c, set<int> *left, int hashcode) {
	this->mds = mds;
	this->in = in;
	this->c = c;
	this->left = left;
	this->hashcode = hashcode;
  }

  virtual void finish(int r) {
	mds->mdstore->hash_dir_2( in, c, left, hashcode );
  }

};



void MDStore::hash_dir( CInode *in,
						Context *c )
{
  assert(in->dir->state_test(CDIR_STATE_HASHING));

  // do them all!
  dout(5) << "hash_dir writing all segments of dir " << *in << endl;
  int nummds = mds->get_cluster()->get_num_mds();
  set<int> *left = new set<int>;
  
  for (int i=0; i<nummds; i++)
	left->insert(i); 
  
  for (int i=0; i<nummds; i++) {
	Context *fin = new C_MDS_HashDir(mds, in, c, left, i);
	do_commit_dir(in, fin, i);
  }
}

void MDStore::hash_dir_2( CInode *in,
						  Context *c,
						  set<int> *left,
						  int hashcode) 
{
  assert(in->dir->state_test(CDIR_STATE_HASHING));

  left->erase(hashcode);
  dout(5) << "hash_dir_2 wrote segment " << hashcode << " of dir " << *in << endl;

  if (!left->empty()) return;
  
  // all items written!
  dout(5) << "hash_dir_2 wrote all segments of dir " << *in << endl;
  delete left;

  // done
  if (c) {
	c->finish(0);
	delete c;
  }
}


void MDStore::unhash_dir( CInode *in,
						  Context *c )
{
  assert(in->dir->state_test(CDIR_STATE_UNHASHING));
  
  // do them all!
  dout(5) << "unhash_dir reading all other segments of dir " << *in << endl;
  int nummds = mds->get_cluster()->get_num_mds();
  set<int> *left = new set<int>;
  
  for (int i=0; i<nummds; i++)
	if (i != mds->get_nodeid()) left->insert(i); 
  
  for (int i=0; i<nummds; i++) {
	Context *fin = new C_MDS_HashDir(mds, in, c, left, i);
	do_commit_dir(in, fin, i);
  }

}

void MDStore::unhash_dir_2( CInode *in,
							Context *c,
							set<int> *left,
							int hashcode) 
{
*/
