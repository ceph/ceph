#ifndef __EDIRUPDATE_H
#define __EDIRUPDATE_H

#include <assert.h>
#include "config.h"
#include "include/types.h"
#include "../LogEvent.h"
#include "../CDir.h"
#include "../MDCache.h"
#include "../MDStore.h"



class EDirUpdate : public LogEvent {
 protected:
  inodeno_t dirino;
  version_t version;

 public:
  EDirUpdate(CDir *dir) :
	LogEvent(EVENT_DIRUPDATE) {
	this->dirino = dir->ino();
	version = dir->get_version();
  }
  EDirUpdate() :
	LogEvent(EVENT_DIRUPDATE) {
  }
  
  virtual void encode_payload(bufferlist& bl) {
	bl.append((char*)&version, sizeof(version));
	bl.append((char*)&dirino, sizeof(dirino));
  }
  void decode_payload(bufferlist& bl, int& off) {
	bl.copy(off, sizeof(version), (char*)&version);
	off += sizeof(version);
	bl.copy(off, sizeof(dirino), (char*)&dirino);
	off += sizeof(dirino);
  }

  
  virtual bool obsolete(MDS *mds) {
	// am i obsolete?
	CInode *in = mds->mdcache->get_inode(dirino);
	if (!in) return true;
	CDir *dir = in->dir;
	if (!dir) return true;

	dout(0) << "EDirUpdate v " << version << " on dir " << *dir << endl;

	if (!dir->is_auth()) return true;     // not mine!
	if (dir->is_frozen()) return true;    // frozen -> exporting -> obsolete? FIXME
	
	if (!dir->is_dirty()) return true;

	if (dir->get_committing_version() > version)
	  return true;

	return false;
  }

  virtual void retire(MDS *mds, Context *c) {
	// commit directory
	CInode *in = mds->mdcache->get_inode(dirino);
	assert(in);
	CDir *dir = in->dir;
	assert(dir);

	dout(0) << "EDirUpdate committing dir " << *dir << endl;
	mds->mdstore->commit_dir(dir, c);
  }
  
};

#endif
