#ifndef __MOSDGETMAPACK_H
#define __MOSDGETMAPACK_H

#include "msg/Message.h"
#include "osd/OSDMap.h"


class MOSDGetMapAck : public Message {
  bufferlist osdmap;

 public:
  // osdmap
  bufferlist& get_osdmap() { 
	return osdmap;
  }

  MOSDGetMapAck(OSDMap *oc) :
	Message(MSG_OSD_GETMAPACK) {
	oc->encode(osdmap);
  }
  MOSDGetMapAck() {}


  // marshalling
  virtual void decode_payload() {
	osdmap.claim(payload);
  }
  virtual void encode_payload() {
	payload.claim(osdmap);
  }

  virtual char *get_type_name() { return "ogma"; }
};

#endif
