#ifndef __EINODEUNLINK_H
#define __EINODEUNLINK_H

#include <assert.h>
#include "include/config.h"
#include "include/types.h"
#include "../LogEvent.h"
#include "../CInode.h"
#include "../MDCache.h"
#include "../MDStore.h"

/* so we can verify the inode is in fact flushed to disk
   after a commit_dir finishes (the commit could have started before 
   and been in progress when we asked. */
class C_EIU_VerifyDirCommit : public Context {
  MDS *mds;
  CDir *dir;
  Context *fin;
  __uint64_t version;

 public:
  C_EIU_VerifyDirCommit(MDS *mds, CDir *dir, __uint64_t version, Context *fin) {
	this->mds = mds;
	this->dir = dir;
	this->version = version;
	this->fin = fin;
  }
  virtual void finish(int r) {

	if (dir->get_version() <= version) {
	  // still dirty
	  mds->mdstore->commit_dir(dir,
							   new C_EIU_VerifyDirCommit(mds,
														 dir,
														 version,
														 fin));
	  return;
	}
	// we're fine.
	if (fin) {
	  fin->finish(0);
	  delete fin;
	}
  }
};

class EInodeUnlink : public LogEvent {
 protected:
  inodeno_t ino;
  inodeno_t dir_ino;

 public:
  EInodeUnlink(CInode *in, CDir *dir) :
	LogEvent(EVENT_INODEUNLINK) {
	this->ino = in->ino();
	this->dir_ino = dir->ino();
  }
  EInodeUnlink(crope s) :
	LogEvent(EVENT_INODEUPDATE) {
	s.copy(0, sizeof(ino), (char*)&ino);
	s.copy(sizeof(ino), sizeof(dir_ino), (char*)&dir_ino);
  }
  
  virtual crope get_payload() {
	crope r;
	r.append((char*)&ino, sizeof(ino));
	r.append((char*)&dir_ino, sizeof(dir_ino));
	return r;
  }
  
  virtual bool obsolete(MDS *mds) {
	// am i obsolete?
	CInode *idir = mds->mdcache->get_inode(dir_ino);
	if (!idir->dir->is_auth() ||
		!idir->dir->is_hashed()) 
	  return true;
	if (idir->dir->is_clean())
	  return true;

	CInode *in = mds->mdcache->get_inode(ino);
	assert(in);

	if (idir->dir->get_version() > in->get_parent_dir_version())
	  return true;

	return false;
  }

  virtual void retire(MDS *mds, Context *c) {
	// commit my containing directory
	CDir *dir = mds->mdcache->get_inode(dir_ino)->dir;
	assert(dir);
	CInode *in = mds->mdcache->get_inode(ino);
	assert(in);
	
	// okay!
	dout(7) << "commiting containing dir " << *dir << " which has unlinked inode " << *in << endl;
	mds->mdstore->commit_dir(dir,
							 new C_EIU_VerifyDirCommit(mds,
													   dir,
													   in->get_parent_dir_version(),
													   c));
  }
};

#endif
