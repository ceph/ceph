
#include "MDStore.h"
#include "MDS.h"
#include "MDCache.h"
#include "CInode.h"
#include "CDir.h"
#include "CDentry.h"
#include "MDCluster.h"

#include "include/Message.h"

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


bool MDStore::fetch_dir( CInode *in,
						 Context *c )
{
  dout(7) << "fetch_dir " << in->inode.ino << " context is " << c << endl;
  if (c) 
	in->dir->add_waiter(CDIR_WAIT_COMPLETE, c);

  assert(in->dir->is_auth());

  // already fetching?
  if (in->dir->state_test(CDIR_STATE_FETCHING)) {
	dout(7) << "already fetching " << in->inode.ino << "; waiting" << endl;
	return true;
  }
  
  in->dir->state_set(CDIR_STATE_FETCHING);

  // create return context
  MDFetchDirContext *fin = new MDFetchDirContext( this, in->ino() );

  if (in->dir_is_hashed()) 
	do_fetch_dir( in, fin, mds->get_nodeid());   // hashed
  else 
	do_fetch_dir( in, fin );  // normal
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
  
  list<Context*>::iterator it = finished.begin();	
  while (it != finished.end()) {
	Context *c = *(it++);
	if (c) {
	  dout(10) << "readdir finish doing context " << c << endl;
	  c->finish(0);
	  delete c;
	}
  }
  
  // trim cache?
  mds->mdcache->trim();
}








// ----------------------------
// commit_dir

class C_MDS_CommitDirDelay : public Context {
public:
  MDS *mds;
  inodeno_t ino;
  Context *c;
  C_MDS_CommitDirDelay( MDS *mds, inodeno_t ino, Context *c) {
	this->mds = mds;
	this->c = c;
	this->ino = ino;
  }
  virtual void finish(int r) {

	if (r >= 0) {
	  CInode *in = mds->mdcache->get_inode(ino);
	  if (in) {
		mds->mdstore->commit_dir(in, c);
		return;
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

class MDCommitDirContext : public Context {
 protected:
  MDStore *ms;
  CInode *in;
  __uint64_t version;

 public:
  crope buffer;

  MDCommitDirContext(MDStore *ms, CInode *in) : Context() {
	this->ms = ms;
	this->in = in;
	version = in->dir->get_version();
  }
  
  void finish(int result) {
	ms->commit_dir_2( result, in, version );
  }
};


class MDFetchForCommitContext : public Context {
protected:
  MDStore *ms;
  CInode *in;
  Context *co;
public:
  MDFetchForCommitContext(MDStore *m, CInode *i, Context *c) {
	ms = m; in = i; co = c;
  }
  void finish(int result) {
	ms->commit_dir( in, co );
  }
};



bool MDStore::commit_dir( CInode *in,
						  Context *c )
{
  assert(in->dir->is_auth() ||
		 in->dir->is_hashed());
  
  // already committing?
  if (in->dir->state_test(CDIR_STATE_COMMITTING)) {
	// already mid-commit!
	dout(7) << "commit_dir " << *in << " already mid-commit" << endl;
	in->dir->add_waiter(CDIR_WAIT_COMMITTED, c);   
	return false;
  }

  if (!in->dir->can_auth_pin()) {
	// something must be frozen up the hiearchy!
	dout(7) << "commit_dir " << *in << " can't auth_pin, waiting" << endl;
	in->dir->add_waiter(CDIR_WAIT_AUTHPINNABLE,
						new C_MDS_CommitDirDelay(mds, in->inode.ino, c) );
	return false;
  }


  // is it complete?
  if (!in->dir->is_complete()) {
	dout(7) << "commit_dir " << *in << " not complete, fetching first" << endl;
	// fetch dir first
	Context *fin = new MDFetchForCommitContext(this, in, c);
	fetch_dir(in, fin);
	return false;
  }


  // ok go
  dout(7) << "commit_dir " << *in << " version " << in->dir->get_version() << endl;

  // add waiter
  if (c) in->dir->add_waiter(CDIR_WAIT_COMMITTED, c);

  // get continuation ready
  MDCommitDirContext *fin = new MDCommitDirContext(this, in);
  
  // state
  in->dir->state_set(CDIR_STATE_COMMITTING);
  in->dir->set_committing_version(); 

  if (in->dir_is_hashed()) {
	// hashed
	do_commit_dir( in, fin, mds->get_nodeid() );
  } else {
	// non-hashed
    do_commit_dir( in, fin );
  }
}

bool MDStore::commit_dir_2( int result,
							CInode *in,
							__uint64_t committed_version)
{
  dout(5) << "commit_dir_2 " << *in << " committed " << committed_version << ", current version " << in->dir->get_version() << endl;

  assert(committed_version == in->dir->get_committing_version());
  
  // is the dir now clean?
  if (committed_version == in->dir->get_version())
	in->dir->mark_clean();
 
  in->dir->state_clear(CDIR_STATE_COMMITTING);

  // finish
  list<Context*> finished;
  in->dir->take_waiting(CDIR_WAIT_COMMITTED,
						finished);
  
  for (list<Context*>::iterator it = finished.begin();
	   it != finished.end();
	   it++) {
	Context *c = *it;
	if (c) {
	  c->finish(result);
	  delete c;
	}
  }
}




// low-level committer (hashed or normal)

class MDDoCommitDirContext : public Context {
 protected:
  MDStore *ms;
  CInode *in;
  Context *c;
  int hashcode;
  __uint64_t version;

public:
  crope buffer;
  
  MDDoCommitDirContext(MDStore *ms, CInode *in, Context *c, int w) : Context() {
	this->ms = ms;
	this->in = in;
	this->c = c;
	this->hashcode = w;
	version = in->dir->get_version();
  }
  
  void finish(int result) {
	ms->do_commit_dir_2( result, in, c, version, hashcode );
  }
};


void MDStore::do_commit_dir( CInode *in,
							 Context *c,
							 int hashcode)
{
  assert(in->dir->is_auth());
  
  dout(11) << "do_commit_dir hashcode " << hashcode << " dir " << *in << " version " << in->dir->get_version() << endl;
  
  // get continuation ready
  MDDoCommitDirContext *fin = new MDDoCommitDirContext(this, in, c, hashcode);
  
  // fill buffer
  __uint32_t num = 0;
  
  crope dirdata;
  
  for (CDir_map_t::iterator it = in->dir->begin();
	   it != in->dir->end();
	   it++) {
	
	if (hashcode >= 0) {
	  int dentryhashcode = mds->get_cluster()->hash_dentry( in->ino(), it->first );
	  if (dentryhashcode != hashcode) continue;
	}

	// name
	dirdata.append( it->first.c_str(), it->first.length() + 1);
	
	// marker
	dirdata.append( 'I' ); // inode
	
	// inode
	dirdata.append( (char*) &it->second->get_inode()->inode, sizeof(inode_t));
	
	// put inode in this dir version
	if (it->second->get_inode()->is_dirty()) {
	  it->second->get_inode()->float_parent_dir_version(in->dir->get_version());
	  dout(12) << " dirty inode " << in->get_parent_dir_version() << " " << *(in) << endl;
	}
	
	num++;
  }
  
  // put count in buffer
  fin->buffer.append((char*)&num, sizeof(num));
  fin->buffer.append(dirdata);
  
  // pin inode
  in->dir->auth_pin();
  
  // submit to osd
  int osd;
  object_t oid;
  if (hashcode >= 0) {
	// hashed
	osd = mds->mdcluster->get_hashdir_meta_osd(in->inode.ino, hashcode);
	oid = mds->mdcluster->get_hashdir_meta_oid(in->inode.ino, hashcode);
  } else {
	// normal
	osd = mds->mdcluster->get_meta_osd(in->inode.ino);
	oid = mds->mdcluster->get_meta_oid(in->inode.ino);
  }
  
  mds->osd_write( osd, oid, 
				  fin->buffer.length(), 0,
				  fin->buffer, 
				  0, // flags
				  fin );
}


void MDStore::do_commit_dir_2( int result,
							   CInode *in,
							   Context *c,
							   __uint64_t committed_version,
							   int hashcode )
{
  dout(11) << "do_commit_dir_2 hashcode " << hashcode << " dir " << *in << endl;
  
  // mark inodes clean too (if we committed them!)
  for (CDir_map_t::iterator it = in->dir->begin();
	   it != in->dir->end();
	   it++) {
	CInode *in = it->second->get_inode();
	
	if (hashcode >= 0) {
	  int dentryhashcode = mds->get_cluster()->hash_dentry( in->ino(), it->first );
	  if (dentryhashcode != hashcode) continue;
	}
	
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
  
  // unpin
  in->dir->auth_unpin();

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


void MDStore::do_fetch_dir( CInode *in,
							Context *c, 
							int hashcode)
{

  dout(11) << "fetch_hashed_dir hashcode " << hashcode << " dir " << in->inode.ino << " context is " << c << endl;
  
  // create return context
  MDDoFetchDirContext *fin = new MDDoFetchDirContext( this, in->ino(), c, hashcode );

  // issue osd read
  int osd;
  object_t oid;
  if (hashcode >= 0) {
	// hashed
	osd = mds->mdcluster->get_hashdir_meta_osd(in->inode.ino, hashcode);
	oid = mds->mdcluster->get_hashdir_meta_oid(in->inode.ino, hashcode);
  } else {
	// normal
	osd = mds->mdcluster->get_meta_osd(in->inode.ino);
	oid = mds->mdcluster->get_meta_oid(in->inode.ino);
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

  // make sure we have a CDir
  if (idir->dir == NULL) idir->dir = new CDir(idir, mds->get_nodeid());
	
  if (!idir->dir->is_auth()) {
	dout(7) << *mds << "do_fetch_dir_2 on " << *idir << ", but i'm not auth" << endl;
	c->finish(-1);
	delete c;
	return;
  } 
  
  // do it
  dout(7) << *mds << "do_fetch_dir_2 hashcode " << hashcode << " dir " << *idir << endl;
  
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
	
	// just a hard link?
	if (*(buf+p) == 'L') {
	  // yup.  we don't do that yet.
	  assert(0);
	} else {
	  p++;
	  
	  inode_t *inode = (inode_t*)(buf+p);
	  p += sizeof(inode_t);

	  if (hashcode >= 0) {
		int dentryhashcode = mds->get_cluster()->hash_dentry( inode->ino, dname );
		assert(dentryhashcode == hashcode);
	  }
	  
	  if (mds->mdcache->have_inode(inode->ino)) {
		CInode *in = mds->mdcache->get_inode(inode->ino);
		dout(10) << "readdir got (but i already had) " << *in << " isdir " << in->inode.isdir << " touched " << in->inode.touched<< endl;
		continue;
	  }
	  
	  // inode
	  CInode *in = new CInode();
	  memcpy(&in->inode, inode, sizeof(inode_t));
	  
	  // add and link
	  mds->mdcache->add_inode( in );
	  mds->mdcache->link_inode( idir, dname, in );
	  
	  dout(10) << "readdir got " << *in << " isdir " << in->inode.isdir << " touched " << in->inode.touched<< endl;
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
