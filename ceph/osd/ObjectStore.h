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
private:
  list<object_t>           age_free_oids;
  object_t                 age_cur_oid;
  vector< list<object_t> > age_objects;
  Distribution file_size_distn; //kb
  bool         did_distn;

 public:
  ObjectStore() : did_distn(false) {}
  virtual ~ObjectStore() {}

  // mgmt
  virtual int mount() = 0;
  virtual int umount() = 0;
  virtual int mkfs() = 0;  // wipe

  virtual int statfs(struct statfs *buf) = 0;

  // objects
  virtual bool exists(object_t oid) = 0;                   // useful?
  virtual int stat(object_t oid, struct stat *st) = 0;     // struct stat?

  virtual int remove(object_t oid) = 0;
  virtual int truncate(object_t oid, off_t size) = 0;

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

  virtual int setattr(object_t oid, const char *name,
					  void *value, size_t size) {return 0;} //= 0;
  virtual int getattr(object_t oid, const char *name,
					  void *value, size_t size) {return 0;} //= 0;
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

  virtual int collection_setattr(object_t oid, const char *name,
								 void *value, size_t size) {return 0;} //= 0;
  virtual int collection_getattr(object_t oid, const char *name,
								 void *value, size_t size) {return 0;} //= 0;
  virtual int collection_listattr(object_t oid, char *attrs, size_t size) {return 0;} //= 0;
  
  
  
  // age store
private:
  void age_empty(float pc);
  void age_fill(float pc);
  ssize_t age_pick_size();
  object_t age_get_oid();

public:
  void age(float high_water,    // fill to this %
		  float low_water,     // then empty to this %
		  int count,         // this many times
		  float final_water,   // and end here ( <= low_water)
		  int fake_size_mb=0);

};

#endif
