#ifndef __OBJECTSTORE_H
#define __OBJECTSTORE_H

#include "include/types.h"

class ObjectStore {
 public:
  virtual bool exists(object_t oid) = 0;
  //bool create(object_t oid);

  virtual int stat(object_t oid, struct stat *st) = 0;  // ?????

  virtual int read(object_t oid, 
				   size_t len, off_t offset,
				   char *buffer) = 0;
  virtual int write(object_t oid,
					size_t len, off_t offset,
					char *buffer) = 0;
  virtual int unlink(object_t oid);
  
};

#endif
