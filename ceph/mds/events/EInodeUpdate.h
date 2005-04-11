#ifndef __EINODEUPDATE_H
#define __EINODEUPDATE_H

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
class C_EIU_VerifyInodeUpdate : public Context {
  MDS *mds;
  inodeno_t ino;
  __uint64_t version;
  Context *fin;

 public:
  C_EIU_VerifyInodeUpdate(MDS *mds, inodeno_t ino, __uint64_t version, Context *fin) {
	this->mds = mds;
	this->ino = ino;
	this->version = version;
	this->fin = fin;
  }
  virtual void finish(int r) {
	CInode *in = mds->mdcache->get_inode(ino);
	if (in) {
	  // if it's mine, dirty, and the same version, commit
	  if (in->authority() == mds->get_nodeid() &&  // mine
		  in->is_dirty() &&                         // dirty
		  in->get_version() == version) {           // same version that i have to deal with
		dout(7) << "ARGH, did EInodeUpdate commit but inode " << *in << " is still dirty" << endl;
		// damnit
		mds->mdstore->commit_dir(in->get_parent_dir(),
								 new C_EIU_VerifyInodeUpdate(mds,
															 in->ino(),
															 in->get_version(),
															 fin));
		return;
	  }
	}
	// we're fine.
	if (fin) {
	  fin->finish(0);
	  delete fin;
	}
  }
};

class EInodeUpdate : public LogEvent {
 protected:
  inode_t inode;
  __uint32_t version;

 public:
  EInodeUpdate(CInode *in) :
	LogEvent(EVENT_INODEUPDATE) {
	this->inode = in->inode;
	version = in->get_version();
  }
  EInodeUpdate(crope s) :
	LogEvent(EVENT_INODEUPDATE) {
	s.copy(0, sizeof(version), (char*)&version);
	s.copy(sizeof(version), sizeof(inode), (char*)&inode);
  }
  
  virtual crope get_payload() {
	crope r;
	r.append((char*)&version, sizeof(version));
	r.append((char*)&inode, sizeof(inode));
	return r;
  }
  
  virtual bool obsolete(MDS *mds) {
	// am i obsolete?
	CInode *in = mds->mdcache->get_inode(inode.ino);
	//assert(in);
	if (!in) {
	  dout(7) << "inode " << inode.ino << " not in cache, must have exported" << endl;
	  return true;
	}
	dout(7) << "EInodeUpdate obsolete? on " << *in << endl;
	if (!in->is_auth())
	  return true;  // not my inode anymore!
	if (in->get_version() != version)
	  return true;  // i'm obsolete!  (another log entry follows)

	// frozen -> exporting -> obsolete    (FOR NOW?)
	if (in->is_frozen())
	  return true; 

	return false;  
  }

  virtual void retire(MDS *mds, Context *c) {
	// commit my containing directory
	CInode *in = mds->mdcache->get_inode(inode.ino);
	assert(in);
	CDir *parent = in->get_parent_dir();

	if (parent) {
	  // okay!
	  dout(7) << "commiting containing dir for " << *in << ", which is " << *parent << endl;
	  mds->mdstore->commit_dir(parent,
							   new C_EIU_VerifyInodeUpdate(mds,
														   in->ino(),
														   in->get_version(),
														   c));
	} else {
	  // oh, i'm the root inode
	  dout(7) << "don't know how to commit the root inode" << endl;
	  if (c) {
		c->finish(0);
		delete c;
	  }
	}

  }
  
};

#endif
