#ifndef __MOSDGETMAPACK_H
#define __MOSDGETMAPACK_H

#include "msg/Message.h"
#include "osd/OSDMap.h"


class MOSDMap : public Message {
  bufferlist osdmap;
  __uint64_t version;

 public:
  // osdmap
  bufferlist& get_osdmap() { 
	return osdmap;
  }

  __uint64_t get_version() { return version; }

  MOSDMap(OSDMap *oc) :
	Message(MSG_OSD_MAP) {
	oc->encode(osdmap);
	version = oc->get_version();
  }
  MOSDMap() {}


  // marshalling
  virtual void decode_payload() {
	payload.copy(0, sizeof(version), (char*)&version);
	payload.splice(0, sizeof(version));
	osdmap.claim(payload);
  }
  virtual void encode_payload() {
	payload.append((char*)&version, sizeof(version));
	payload.claim_append(osdmap);
  }

  virtual char *get_type_name() { return "ogma"; }
};

#endif
