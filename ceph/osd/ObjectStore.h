#ifndef __OBJECTSTORE_H
#define __OBJECTSTORE_H

#include "include/types.h"

/*
 * low-level interface to the local OSD file system
 */

class ObjectStore {
 public:
  virtual int init() = 0;
  virtual int finalize() = 0;

  virtual int mkfs() = 0;  // wipe

  // objects
  virtual bool exists(object_t oid) = 0;                   // useful?
  virtual int stat(object_t oid, struct stat *st) = 0;     // struct stat?

  virtual int remove(object_t oid) = 0;
  virtual int truncate(object_t oid, off_t size) = 0;

  virtual int read(object_t oid, 
				   size_t len, off_t offset,
				   char *buffer) = 0;
  virtual int write(object_t oid,
					size_t len, off_t offset,
					char *buffer,
					bool fsync=true) = 0;

  /*
  // attributes
  virtual int setattr(...) = 0;
  virtual int getattr(...) = 0;

  // collections
  virtual int collection_create(coll_t c) = 0;
  virtual int collection_destroy(coll_t c) = 0;
  virtual int collection_add(coll_t c, object_t o) = 0;
  virtual int collection_remove(coll_t c, object_t o) = 0;
  */

};

#endif
