

#ifndef __CEPH_CAS_H
#define __CEPH_CAS_H

#include "include/object.h"

object_t calc_cas_name(bufferlist &bl);
void chunk_buffer(bufferlist &bl, list<bufferlist> &chunks);


#endif
