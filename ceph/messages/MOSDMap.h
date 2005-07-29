#ifndef __MOSDGETCLUSTERACK_H
#define __MOSDGETCLUSTERACK_H

#include "msg/Message.h"
#include "osd/OSDMap.h"


class MOSDGetClusterAck : public Message {
  bufferlist osdmap;

 public:
  // osdmap
  bufferlist& get_osdmap() { 
	return osdmap;
  }

  MOSDGetClusterAck(OSDMap *oc) :
	Message(MSG_OSD_GETCLUSTERACK) {
	oc->encode(osdmap);
  }
  MOSDGetClusterAck() {}


  // marshalling
  virtual void decode_payload() {
	osdmap.claim(payload);
  }
  virtual void encode_payload() {
	payload.claim(osdmap);
  }

  virtual char *get_type_name() { return "ogca"; }
};

#endif
