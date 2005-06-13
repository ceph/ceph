#ifndef __MOSDGETCLUSTERACK_H
#define __MOSDGETCLUSTERACK_H

#include "msg/Message.h"
#include "osd/OSDCluster.h"


class MOSDGetClusterAck : public Message {
  bufferlist osdcluster;

 public:
  // osdcluster
  bufferlist& get_osdcluster() { 
	return osdcluster;
  }

  MOSDGetClusterAck(OSDCluster *oc) :
	Message(MSG_OSD_GETCLUSTERACK) {
	oc->encode(osdcluster);
  }
  MOSDGetClusterAck() {}


  // marshalling
  virtual void decode_payload() {
	osdcluster.claim(payload);
  }
  virtual void encode_payload() {
	payload.claim(osdcluster);
  }

  virtual char *get_type_name() { return "ogca"; }
};

#endif
