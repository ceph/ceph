#ifndef CEPH_MCLIENTQUOTA_H
#define CEPH_MCLIENTQUOTA_H

#include "msg/Message.h"

struct MClientQuota : public Message {
  inodeno_t ino;
  nest_info_t rstat;
  quota_info_t quota;

  MClientQuota() :
    Message(CEPH_MSG_CLIENT_QUOTA),
    ino(0)
  {
    memset(&rstat, 0, sizeof(rstat));
    memset(&quota, 0, sizeof(quota));
  }
private:
  ~MClientQuota() {}

public:
  const char *get_type_name() const { return "client_quota"; }
  void print(ostream& out) const {
    out << "client_quota(";
    out << " [" << ino << "] ";
    out << rstat;
    out << ")";
  }

  void encode_payload(uint64_t features) {
    ::encode(ino, payload);
    ::encode(rstat.rctime, payload);
    ::encode(rstat.rbytes, payload);
    ::encode(rstat.rfiles, payload);
    ::encode(rstat.rsubdirs, payload);
    ::encode(quota, payload);
  }
  void decode_payload() {
    bufferlist::iterator p = payload.begin();
    ::decode(ino, p);
    ::decode(rstat.rctime, p);
    ::decode(rstat.rbytes, p);
    ::decode(rstat.rfiles, p);
    ::decode(rstat.rsubdirs, p);
    ::decode(quota, p);
    assert(p.end());
  }
};

#endif
