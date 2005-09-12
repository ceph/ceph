#ifndef __MOSDPGPEER_H
#define __MOSDPGPEER_H

#include "msg/Message.h"


class MOSDPGPeer : public Message {
  __uint64_t       map_version;
  list<pg_t> pg_list;

 public:
  __uint64_t get_version() { return map_version; }
  list<pg_t>& get_pg_list() { return pg_list; }

  MOSDPGPeer() {}
  MOSDPGPeer(__uint64_t v, list<pg_t>& l) :
	Message(MSG_OSD_PG_PEER) {
	this->map_version = v;
	pg_list.splice(pg_list.begin(), l);
  }
  
  char *get_type_name() { return "PGPeer"; }

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
