#ifndef __MOSDGETMAPACK_H
#define __MOSDGETMAPACK_H

#include "msg/Message.h"
#include "osd/OSDMap.h"


class MOSDMap : public Message {
  bufferlist osdmap;
  __uint64_t version;
  bool mkfs;

 public:
  // osdmap
  bufferlist& get_osdmap() { 
	return osdmap;
  }

  __uint64_t get_version() { return version; }
  bool is_mkfs() { return mkfs; }

  MOSDMap(OSDMap *oc, bool mkfs=false) :
	Message(MSG_OSD_MAP) {
	oc->encode(osdmap);
	version = oc->get_version();
	this->mkfs = mkfs;
  }
  MOSDMap() {}


  // marshalling
  virtual void decode_payload() {
	int off = 0;
	payload.copy(off, sizeof(version), (char*)&version);
	off += sizeof(version);
	payload.copy(off, sizeof(mkfs), (char*)&mkfs);
	off += sizeof(mkfs);
	payload.splice(0, off);
	osdmap.claim(payload);
  }
  virtual void encode_payload() {
	payload.append((char*)&version, sizeof(version));
	payload.append((char*)&mkfs, sizeof(mkfs));
	payload.claim_append(osdmap);
  }

  virtual char *get_type_name() { return "omap"; }
};

#endif
