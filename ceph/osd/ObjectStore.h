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


#ifndef __OBJECTSTORE_H
#define __OBJECTSTORE_H

#include "include/types.h"
#include "include/Context.h"
#include "include/bufferlist.h"

#include "include/Distribution.h"

#include <sys/stat.h>
#include <sys/vfs.h>    /* or <sys/statfs.h> */

#include <list>
using namespace std;

#ifndef MIN
# define MIN(a,b) ((a) < (b) ? (a):(b))
#endif

/*
 * low-level interface to the local OSD file system
 */

class ObjectStore {
 public:
  ObjectStore() {}
  virtual ~ObjectStore() {}

  // mgmt
  virtual int mount() = 0;
  virtual int umount() = 0;
  virtual int mkfs() = 0;  // wipe

  virtual int statfs(struct statfs *buf) = 0;

  // objects
  virtual bool exists(object_t oid) = 0;                   // useful?
  virtual int stat(object_t oid, struct stat *st) = 0;     // struct stat?

  virtual int remove(object_t oid,
					 Context *onsafe=0) = 0;
  virtual int remove_transaction(object_t oid,
								 map<coll_t, map<const char*, pair<void*,int> > >& cmods,					 
								 Context *onsafe=0) {
	int r = remove(oid, onsafe);
	for (map<coll_t, map<const char*, pair<void*,int> > >::iterator cit = cmods.begin();
		 cit != cmods.end();
		 cit++) {
	  collection_add(cit->first, oid);
	  for (map<const char*, pair<void*,int> >::iterator ait = cit->second.begin();
		   ait != cit->second.end();
		   ait++) 
		collection_setattr(cit->first, ait->first, ait->second.first, ait->second.second);
	}
	return r;
  }

  virtual int truncate(object_t oid, off_t size,
					   Context *onsafe=0) = 0;
  virtual int truncate_transaction(object_t oid, off_t size, 
								   map<const char*, pair<void*,int> >& setattrs,
								   map<coll_t, map<const char*, pair<void*,int> > >& cmods,
								   Context *onsafe) { 
	int r = truncate(oid, size, onsafe);
	for (map<const char*, pair<void*,int> >::iterator it = setattrs.begin();
		 it != setattrs.end();
		 it++) 
	  setattr(oid, it->first, it->second.first, it->second.second);
	for (map<coll_t, map<const char*, pair<void*,int> > >::iterator cit = cmods.begin();
		 cit != cmods.end();
		 cit++) {
	  collection_add(cit->first, oid);
	  for (map<const char*, pair<void*,int> >::iterator ait = cit->second.begin();
		   ait != cit->second.end();
		   ait++) 
		collection_setattr(cit->first, ait->first, ait->second.first, ait->second.second);
	}  
	return r;
  }
  
  virtual int read(object_t oid, 
				   size_t len, off_t offset,
				   bufferlist& bl) = 0;

  virtual int write(object_t oid,
					size_t len, off_t offset,
					bufferlist& bl,
					bool fsync=true) = 0;     
  virtual int write(object_t oid, 
					size_t len, off_t offset, 
					bufferlist& bl, 
					Context *onsafe) = 0;//{ return -1; }
  virtual int write_transaction(object_t oid, 
								size_t len, off_t offset, 
								bufferlist& bl, 
								map<const char*, pair<void*,int> >& setattrs,
								map<coll_t, map<const char*, pair<void*,int> > >& cmods,
								Context *onsafe) { 
	int r = write(oid, len, offset, bl, onsafe);
	for (map<const char*, pair<void*,int> >::iterator it = setattrs.begin();
		 it != setattrs.end();
		 it++) 
	  setattr(oid, it->first, it->second.first, it->second.second);
	
	for (map<coll_t, map<const char*, pair<void*,int> > >::iterator cit = cmods.begin();
		 cit != cmods.end();
		 cit++) {
	  collection_add(cit->first, oid);
	  for (map<const char*, pair<void*,int> >::iterator ait = cit->second.begin();
		   ait != cit->second.end();
		   ait++) 
		collection_setattr(cit->first, ait->first, ait->second.first, ait->second.second);
	}
	return r;
  }

  virtual int setattr(object_t oid, const char *name,
					  void *value, size_t size,
					  Context *onsafe=0) {return 0;} //= 0;
  virtual int getattr(object_t oid, const char *name,
					  void *value, size_t size) {return 0;} //= 0;

  virtual int rmattr(object_t oid, const char *name,
					 Context *onsafe=0) {return 0;}

  virtual int listattr(object_t oid, char *attrs, size_t size) {return 0;} //= 0;
  
  // collections
  virtual int list_collections(list<coll_t>& ls) {return 0;}//= 0;
  virtual int create_collection(coll_t c) {return 0;}//= 0;
  virtual int destroy_collection(coll_t c) {return 0;}//= 0;
  virtual bool collection_exists(coll_t c) {return 0;}
  virtual int collection_stat(coll_t c, struct stat *st) {return 0;}//= 0;
  virtual int collection_add(coll_t c, object_t o) {return 0;}//= 0;
  virtual int collection_remove(coll_t c, object_t o) {return 0;}// = 0;
  virtual int collection_list(coll_t c, list<object_t>& o) {return 0;}//= 0;

  virtual int collection_setattr(coll_t cid, const char *name,
								 void *value, size_t size) {return 0;} //= 0;
  virtual int collection_getattr(coll_t cid, const char *name,
								 void *value, size_t size) {return 0;} //= 0;
  virtual int collection_listattr(coll_t cid, char *attrs, size_t size) {return 0;} //= 0;
  
  virtual void sync() {};
  
  
};

#endif
