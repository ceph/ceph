#ifndef __EUNLINK_H
#define __EUNLINK_H

#include <assert.h>
#include "include/config.h"
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
  EUnlink(crope s) :
	LogEvent(EVENT_UNLINK) {
	s.copy(0, sizeof(dir_ino), (char*)&dir_ino);
	s.copy(sizeof(dir_ino), sizeof(version), (char*)&version);
	dname = s.c_str() + sizeof(dir_ino) + sizeof(version);
  }
  
  virtual crope get_payload() {
	crope r;
	r.append((char*)&dir_ino, sizeof(dir_ino));
	r.append((char*)&version, sizeof(version));
	r.append((char*)dname.c_str(), dname.length() + 1);
	return r;
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
