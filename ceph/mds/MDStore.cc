
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
  crope buffer;

  MDFetchDirContext(MDStore *ms, inodeno_t ino) : Context() {
	this->ms = ms;
	this->ino = ino;
  }
  
  void finish(int result) {
	ms->fetch_dir_2( result, buffer, ino );
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

  // issue osd read
  int osd = mds->mdcluster->get_meta_osd(in->inode.ino);
  object_t oid = mds->mdcluster->get_meta_oid(in->inode.ino);
  
  mds->osd_read( osd, oid, 
				 0, 0,
				 &fin->buffer,
				 fin );
}

bool MDStore::fetch_dir_2( int result, 
						   crope buffer, 
						   inodeno_t ino)
{
  CInode *idir = mds->mdcache->get_inode(ino);
  if (!idir) {
	dout(7) << *mds << "fetch_dir_2 on ino " << ino << " but no longer in our cache!" << endl;
	return false;
  } 

  if (idir->dir_authority(mds->get_cluster()) != mds->get_nodeid()) {

	// oh well
	dout(7) << *mds << "fetch_dir_2 on " << *idir << ", but i'm not the authority." << endl;
	
  } else {

	// do it

	dout(7) << *mds << "fetch_dir_2 on " << *idir << " has " << idir->dir->get_size() << endl;
	
	// make sure we have a CDir
	if (idir->dir == NULL) idir->dir = new CDir(idir, true);
	
	// parse buffer contents into cache
	const char *buf = buffer.c_str();
	long buflen = buffer.length();
	
	__uint32_t num = *((__uint32_t*)buf);
	dout(7) << "  " << num << " items" << endl;
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
		
		if (mds->mdcache->have_inode(inode->ino)) {
		  CInode *in = mds->mdcache->get_inode(inode->ino);
		  dout(7) << "readdir got (but i already had) " << *in << " isdir " << in->inode.isdir << " touched " << in->inode.touched<< endl;
		  continue;
		}
		
		// inode
		CInode *in = new CInode();
		memcpy(&in->inode, inode, sizeof(inode_t));
		
		// add and link
		mds->mdcache->add_inode( in );
		mds->mdcache->link_inode( idir, dname, in );
		
		dout(7) << "readdir got " << *in << " isdir " << in->inode.isdir << " touched " << in->inode.touched<< endl;
		
		// HACK
		/*
		  if (idir->inode.ino == 1 && mds->get_nodeid() == 0 && in->is_dir()) {
		  int d = rand() % mds->mdcluster->get_size();
		  if (d > 0) {
		  dout(7) << "hack: exporting dir" << endl;
		  mds->mdcache->export_dir( in, d);
		  }
		  }
		*/
		
	  }
	}

	// dir is now complete
	idir->dir->state_set(CDIR_STATE_COMPLETE);
  }

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
  Context *c;
  __uint64_t version;

 public:
  crope buffer;

  MDCommitDirContext(MDStore *ms, CInode *in, Context *c) : Context() {
	this->ms = ms;
	this->in = in;
	this->c = c;
	version = in->dir->get_version();
  }
  
  void finish(int result) {
	ms->commit_dir_2( result, in, c, version );
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
  assert(in->dir->is_auth());

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

  dout(7) << "commit_dir " << *in << endl;

  // get continuation ready
  MDCommitDirContext *fin = new MDCommitDirContext(this, in, c);
  
  // fill buffer
  __uint32_t num = in->dir->get_size();
  fin->buffer.append((char*)&num, sizeof(__uint32_t));

  for (CDir_map_t::iterator it = in->dir->begin();
	   it != in->dir->end();
	   it++) {
	// name
	fin->buffer.append( it->first.c_str(), it->first.length() + 1);
	
	// marker
	fin->buffer.append( 'I' ); // inode

	// inode
	fin->buffer.append( (char*) &it->second->get_inode()->inode, sizeof(inode_t));

	// put inode in this dir version
	if (it->second->get_inode()->is_dirty())
	  it->second->get_inode()->float_parent_dir_version(in->dir->get_version());
	
	num++;
  }
  
  // pin inode
  in->dir->auth_pin();
  in->dir->state_set(CDIR_STATE_COMMITTING);

  // submit to osd
  int osd = mds->mdcluster->get_meta_osd(in->inode.ino);
  object_t oid = mds->mdcluster->get_meta_oid(in->inode.ino);
  
  mds->osd_write( osd, oid, 
				  fin->buffer.length(), 0,
				  fin->buffer, 
				  0, // flags
				  fin );
  return true;
}


bool MDStore::commit_dir_2( int result,
							CInode *in,
							Context *c,
							__uint64_t committed_version)
{
  dout(5) << "commit_dir_2 " << *in << endl;

  // is the dir now clean?
  if (committed_version == in->dir->get_version())
	in->dir->mark_clean();
 
  in->dir->state_clear(CDIR_STATE_COMMITTING);

  // mark inodes clean too (if we committed them!)
  for (CDir_map_t::iterator it = in->dir->begin();
	   it != in->dir->end();
	   it++) {
	CInode *in = it->second->get_inode();
	if (in->get_parent_dir_version() == committed_version) {
	  in->mark_clean();     // might not but could be dirty
	  dout(5) << " now clean " << *(in) << endl;
	} else {
	  dout(5) << " still dirty " << *(in) << endl;
	}
  }

  // unpin
  in->dir->auth_unpin();

  // finish
  list<Context*> finished;
  finished.push_back(c);
  in->dir->take_waiting(CDIR_WAIT_COMMITTED,
						finished);

  if (result >= 0) result = 0;

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
