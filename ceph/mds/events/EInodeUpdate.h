#ifndef __EINODEUPDATE_H
#define __EINODEUPDATE_H

#include <assert.h>
#include "include/types.h"
#include "../LogEvent.h"
#include "../CInode.h"
#include "../MDCache.h"
#include "../MDStore.h"

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
	  cout << "inode " << inode.ino << " not in cache, must have exported" << endl;
	  return true;
	}
	if (in->get_version() != version)
	  return true;  // i'm obsolete!
	return false;  
  }

  virtual void retire(MDS *mds, Context *c) {
	// commit my containing directory
	CInode *in = mds->mdcache->get_inode(inode.ino);
	assert(in);
	CInode *parent = in->get_parent_inode();

	if (parent) {
	  // okay!
	  cout << "commiting containing dir for " << inode.ino << endl;
	  mds->mdstore->commit_dir(parent,
							   c);
	} else {
	  // oh, i'm the root inode
	  cout << "don't know how to commit the root inode" << endl;
	  if (c) {
		c->finish(0);
		delete c;
	  }
	}

  }
  
};

#endif
