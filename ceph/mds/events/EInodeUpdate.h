#ifndef __EINODEUPDATE_H
#define __EINODEUPDATE_H

#include <assert.h>
#include "config.h"
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
  EInodeUpdate() :
	LogEvent(EVENT_INODEUPDATE) {
  }
  
  virtual void encode_payload(bufferlist& bl) {
	bl.append((char*)&version, sizeof(version));
	bl.append((char*)&inode, sizeof(inode));
  }
  void decode_payload(bufferlist& bl, int& off) {
	bl.copy(off, sizeof(version), (char*)&version);
	off += sizeof(version);
	bl.copy(off, sizeof(inode), (char*)&inode);
	off += sizeof(inode);
  }

  
  bool can_expire(MDS *mds) {
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

	CDir *parent = in->get_parent_dir();
	if (!parent) return true;  // root?
	if (!parent->is_dirty()) return true; // dir is clean!

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
	  mds->mdstore->commit_dir(parent, c);
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
