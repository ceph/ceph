#ifndef __MOSDPEERREQUEST_H
#define __MOSDPEERREQUEST_H

#include "msg/Message.h"


class MOSDPGPeerRequest : public Message {
  __uint64_t       map_version;
  list<repgroup_t> pg_list;

 public:
  __uint64_t get_version() { return map_version; }
  list<repgroup_t>& get_pg_list() { return pg_list; }

  MOSDPGPeerRequest() {}
  MOSDPGPeerRequest(__uint64_t v, list<repgroup_t>& l) :
	Message(MSG_OSD_PG_PEERREQUEST) {
	this->map_version = v;
	pg_list.splice(pg_list.begin(), l);
  }
  
  char *get_type_name() { return "PGPR"; }

  void encode_payload() {
	payload.append((char*)&map_version, sizeof(map_version));
	_encode(pg_list, payload);
  }
  void decode_payload() {
	int off = 0;
	payload.copy(off, sizeof(map_version), (char*)&map_version);
	off += sizeof(map_version);
	_decode(pg_list, payload, off);
  }
};

#endif
