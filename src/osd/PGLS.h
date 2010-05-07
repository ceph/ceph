#ifndef __PGLS_H
#define __PGLS_H


#include "include/types.h"
#include "os/ObjectStore.h"

struct PGLSResponse {
  collection_list_handle_t handle; 
  list<object_t> entries;

  void encode(bufferlist& bl) const {
    ::encode((uint64_t)handle, bl);
    ::encode(entries, bl);
  }
  void decode(bufferlist::iterator& bl) {
    uint64_t tmp;
    ::decode(tmp, bl);
    handle = (collection_list_handle_t)tmp;
    ::decode(entries, bl);
  }
};

WRITE_CLASS_ENCODER(PGLSResponse)


#endif
