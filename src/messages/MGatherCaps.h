#ifndef CEPH_MGATHERCAPS_H
#define CEPH_MGATHERCAPS_H

#include "msg/Message.h"


class MGatherCaps : public Message {
 public:
  inodeno_t ino;

  MGatherCaps() :
    Message(MSG_MDS_GATHERCAPS) {}
private:
  ~MGatherCaps() {}

public:
  const char *get_type_name() const { return "gather_caps"; }
  void print(ostream& o) const {
    o << "gather_caps(" << ino << ")";
  }

  void encode_payload(uint64_t features) {
    ::encode(ino, payload);
  }
  void decode_payload() {
    bufferlist::iterator p = payload.begin();
    ::decode(ino, p);
  }

};
REGISTER_MESSAGE(MGatherCaps, MSG_MDS_GATHERCAPS);
#endif
