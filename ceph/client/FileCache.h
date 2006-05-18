#ifndef __FILECACHE_H
#define __FILECACHE_H

#include "Filer.h"

class FileCache {
  ObjectCacher *oc;
  Filer filer;
  inodeno_t ino;
  
 public:
  FileCache(ObjectCacher *_oc, inodeno_t _ino) : 
	oc(_oc), 
	filer(_oc), ino(_ino) {}
  
  void flush_dirty(Context *onflush=0);     // blocks if !onfinish
  void release_clean(Context *onfinish=0);  // blocks if !onfinish
};


#endif
