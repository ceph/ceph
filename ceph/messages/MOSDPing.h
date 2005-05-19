
#ifndef __MOSDPING_H
#define __MOSDPING_H

#include "msg/Message.h"


class MOSDPing : public Message {
 public:
  int ttl;
  int osd_status;  

  int get_status() { return osd_status; }

  MOSDPing(int status) : Message(MSG_OSD_PING) {
	//ttl = n;
	this->osd_status = status;
  }
  MOSDPing() {}

  virtual void decode_payload(crope& s, int& off) {
	s.copy(off, sizeof(ttl), (char*)&ttl);
	off += sizeof(ttl);
	s.copy(off, sizeof(osd_status), (char*)&osd_status);
	off += sizeof(osd_status);
  }
  virtual void encode_payload(crope& s) {
	s.append((char*)&ttl, sizeof(ttl));
	s.append((char*)&osd_status, sizeof(osd_status));
  }

  virtual char *get_type_name() { return "oping"; }
};

#endif
