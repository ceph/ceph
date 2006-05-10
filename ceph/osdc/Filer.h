// -*- mode:C++; tab-width:4; c-basic-offset:2; indent-tabs-mode:t -*- 
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software 
 * Foundation.  See file COPYING.
 * 
 */


#ifndef __FILER_H
#define __FILER_H

/*** Filer
 *
 * client/mds interface to access "files" in OSD cluster.
 *
 * generic non-blocking interface for reading/writing to osds, using
 * the file-to-object mappings defined by OSDMap.
 *
 * Filer also handles details of replication on OSDs (to the extent that 
 * it affects OSD clients)
 *
 * "files" are identified by ino. 
 */

#include <set>
#include <map>
using namespace std;

#include <ext/hash_map>
#include <ext/rope>
using namespace __gnu_cxx;

#include "include/types.h"

#include "OSDMap.h"
#include "Objecter.h"
#include "ObjectCacher.h"

class Context;
class Messenger;
class OSDMap;
class ObjectCacher;


/**** Filer interface ***/

class Filer {
  Objecter   *objecter;
  ObjectCacher *oc;
  
 public:
  Filer(Objecter *o, ObjectCacher *c=0) : objecter(o), oc(c) {}
  ~Filer() {}

  bool is_active() {
	return objecter->is_active(); // || (oc && oc->is_active());
  }

  /*** async file interface ***/
  int read(inode_t& inode,
		   size_t len, 
		   off_t offset, 
		   bufferlist *bl,   // ptr to data
		   Context *onfinish) {
	Objecter::OSDRead *rd = new Objecter::OSDRead(bl);
	file_to_extents(inode, len, offset, rd->extents);

	return objecter->readx(rd, onfinish);
  }

  int write(inode_t& inode,
			size_t len, 
			off_t offset, 
			bufferlist& bl,
			int flags, 
			Context *onack,
			Context *oncommit) {
	Objecter::OSDWrite *wr = new Objecter::OSDWrite(bl);
	file_to_extents(inode, len, offset, wr->extents);

	return objecter->writex(wr, onack, oncommit);
  }

  int zero(inode_t& inode,
		   size_t len,
		   off_t offset,
		   Context *onack,
		   Context *oncommit) {
	Objecter::OSDZero *z = new Objecter::OSDZero;
	file_to_extents(inode, len, offset, z->extents);

	return objecter->zerox(z, onack, oncommit);
  }


  /*** async+caching (non-blocking) file interface ***/
  int caching_read(inode_t& inode,
				   size_t len, 
				   off_t offset, 
				   bufferlist *bl,
				   Context *onfinish) {
	Objecter::OSDRead *rd = new Objecter::OSDRead(bl);
	file_to_extents(inode, len, offset, rd->extents);
	return oc->readx(rd, inode.ino, onfinish);
  }

  int caching_write(inode_t& inode,
					size_t len, 
					off_t offset, 
					bufferlist& bl) {
	Objecter::OSDWrite *wr = new Objecter::OSDWrite(bl);
	file_to_extents(inode, len, offset, wr->extents);
	return oc->writex(wr, inode.ino);
  }



  /*** sync+blocking file interface ***/
  
  int atomic_sync_read(inode_t& inode,
					   size_t len, off_t offset,
					   bufferlist *bl,
					   Mutex &lock) {
	Objecter::OSDRead *rd = new Objecter::OSDRead(bl);
	file_to_extents(inode, len, offset, rd->extents);

	assert(oc);
	int r = oc->atomic_sync_readx(rd, inode.ino, lock);
	return r;
  }

  int atomic_sync_write(inode_t& inode,
						size_t len, off_t offset,
						bufferlist& bl,
						Mutex &lock) {
	Objecter::OSDWrite *wr = new Objecter::OSDWrite(bl);
	file_to_extents(inode, len, offset, wr->extents);

	assert(oc);
	int r = oc->atomic_sync_writex(wr, inode.ino, lock);
	return r;
  }






  /***** mapping *****/

  /* map (ino, ono) to an object name
	 (to be used on any osd in the proper replica group) */
  object_t file_to_object(inodeno_t ino,
						  size_t    _ono) {  
	__uint64_t ono = _ono;
	assert(ino < (1ULL<<OID_INO_BITS));       // legal ino can't be too big
	assert(ono < (1ULL<<OID_ONO_BITS));
	return ono + (ino << OID_ONO_BITS);
  }


  /* map (ino, offset, len) to a (list of) OSDExtents 
	 (byte ranges in objects on (primary) osds) */
  void file_to_extents(inode_t inode,
					   size_t len,
					   off_t offset,
					   list<ObjectExtent>& extents);
  
};



#endif
