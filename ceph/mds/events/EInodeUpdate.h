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
  EInodeUpdate(char *buf) :
	LogEvent(EVENT_INODEUPDATE) {
	version = *(__uint32_t*)buf;
	inode = *(inode_t*)(buf+sizeof(__uint32_t));
  }
  
  virtual int serialize() {
	int len = sizeof(inode_t) + sizeof(version);
	char *buf = alloc_serial_buf(len);
	memcpy(buf, &version, sizeof(version));
	memcpy(buf + sizeof(version), &inode, sizeof(inode_t));
	return 0;
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
