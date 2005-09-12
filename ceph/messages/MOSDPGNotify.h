#ifndef __MOSDPGPEERNOTIFY_H
#define __MOSDPGPEERNOTIFY_H

#include "msg/Message.h"


class MOSDPGNotify : public Message {
  __uint64_t           map_version;
  map<pg_t, version_t> pg_list;   // pgid -> last_complete

 public:
  __uint64_t get_version() { return map_version; }
  map<pg_t, version_t>& get_pg_list() { return pg_list; }

  MOSDPGNotify() {}
  MOSDPGNotify(__uint64_t v, map<pg_t,version_t>& l) :
	Message(MSG_OSD_PG_NOTIFY) {
	this->map_version = v;
	pg_list = l;                          // FIXME
  }
  
  char *get_type_name() { return "PGnot"; }

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
