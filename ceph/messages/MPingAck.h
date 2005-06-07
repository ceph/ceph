#ifndef __MPINGACK_H
#define __MPINGACK_H

#include "MPing.h"


class MPingAck : public Message {
 public:
  int seq;
  MPingAck() {}
  MPingAck(MPing *p) : Message(MSG_PING_ACK) {
	this->seq = p->seq;
  }

  virtual void decode_payload(crope& s, int& off) {
	s.copy(0, sizeof(seq), (char*)&seq);
	off += sizeof(seq);
  }
  virtual void encode_payload(crope& s) {
	s.append((char*)&seq, sizeof(seq));
  }

  virtual char *get_type_name() { return "pinga"; }
};

#endif
