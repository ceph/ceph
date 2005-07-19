#ifndef __EUNLINK_H
#define __EUNLINK_H

#include <assert.h>
#include "config.h"
#include "include/types.h"
#include "../LogEvent.h"
#include "../CInode.h"
#include "../MDCache.h"
#include "../MDStore.h"



class EUnlink : public LogEvent {
 protected:
  inodeno_t dir_ino;
  __uint64_t version;
  string dname;

 public:
  EUnlink(CDir *dir, CDentry* dn) :
	LogEvent(EVENT_UNLINK) {
	this->dir_ino = dir->ino();
	this->dname = dn->get_name();
	this->version = dir->get_version();
  }
  EUnlink() :
	LogEvent(EVENT_UNLINK) {
  }
  
  virtual void encode_payload(bufferlist& bl) {
	bl.append((char*)&dir_ino, sizeof(dir_ino));
	bl.append((char*)&version, sizeof(version));
	bl.append((char*)dname.c_str(), dname.length() + 1);
  }
  void decode_payload(bufferlist& bl, int& off) {
	bl.copy(off, sizeof(dir_ino), (char*)&dir_ino);
	off += sizeof(dir_ino);
	bl.copy(off, sizeof(version), (char*)&version);
	off += sizeof(version);
	dname = bl.c_str() + off;
	off += dname.length() + 1;
  }
  
  virtual bool obsolete(MDS *mds) {
	// am i obsolete?
	CInode *idir = mds->mdcache->get_inode(dir_ino);
	if (!idir) return true;

	CDir *dir = idir->dir;

	if (!dir) return true;

	if (!idir->dir->is_auth()) return true;
	if (idir->dir->is_clean()) return true;

	if (idir->dir->get_last_committed_version() >= version) return true;
	return false;
  }

  virtual void retire(MDS *mds, Context *c) {
	// commit my containing directory
	CDir *dir = mds->mdcache->get_inode(dir_ino)->dir;
	assert(dir);
	
	// okay!
	dout(7) << "commiting dirty (from unlink) dir " << *dir << endl;
	mds->mdstore->commit_dir(dir, version, c);
  }
};

#endif
