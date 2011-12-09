#ifndef CEPH_PGLS_H
#define CEPH_PGLS_H


#include "include/types.h"
#include "os/ObjectStore.h"

struct PGLSResponse {
  collection_list_handle_t handle; 
  list<pair<object_t, string> > entries;

  void encode(bufferlist& bl) const {
    __u8 v = 1;
    ::encode(v, bl);
    ::encode(handle, bl);
    ::encode(entries, bl);
  }
  void decode(bufferlist::iterator& bl) {
    __u8 v;
    ::decode(v, bl);
    assert(v == 1);
    ::decode(handle, bl);
    ::decode(entries, bl);
  }
};

WRITE_CLASS_ENCODER(PGLSResponse)


#endif
