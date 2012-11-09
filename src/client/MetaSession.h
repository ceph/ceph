#ifndef CEPH_CLIENT_METASESSION_H
#define CEPH_CLIENT_METASESSION_H

#include "include/types.h"
#include "include/utime.h"
#include "msg/msg_types.h"
#include "include/xlist.h"

#include "messages/MClientCapRelease.h"

class Cap;
class Inode;
class CapSnap;
class MetaRequest;
class MClientCapRelease;

struct MetaSession {
  int mds_num;
  version_t seq;
  uint64_t cap_gen;
  utime_t cap_ttl, last_cap_renew_request;
  uint64_t cap_renew_seq;
  int num_caps;
  entity_inst_t inst;
  bool closing;
  bool was_stale;

  xlist<Cap*> caps;
  xlist<Inode*> flushing_caps;
  xlist<CapSnap*> flushing_capsnaps;
  xlist<MetaRequest*> requests;
  xlist<MetaRequest*> unsafe_requests;

  MClientCapRelease *release;
  
  MetaSession() : mds_num(-1), seq(0), cap_gen(0), cap_renew_seq(0), num_caps(0),
		 closing(false), was_stale(false), release(NULL) {}
  ~MetaSession() {
    release->put();
  }
};

#endif
