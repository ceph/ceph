#ifndef __MOSDPGUPDATE_H
#define __MOSDPGUPDATE_H

#include "msg/Message.h"

class MOSDPGUpdate : public Message {
  version_t   map_version;
  pg_t        pgid;
  pginfo_t    info;

 public:
  version_t get_version() { return map_version; }
  pg_t get_pgid() { return pgid; }
  pginfo_t& get_pginfo() { return info; }

  MOSDPGUpdate() {}
  MOSDPGUpdate(version_t mv, pg_t pgid, pginfo_t info) :
	Message(MSG_OSD_PG_UPDATE) {
	this->map_version = mv;
	this->pgid = pgid;
	this->info = info;
  }
  
  char *get_type_name() { return "PGUp"; }

  void encode_payload() {
	payload.append((char*)&map_version, sizeof(map_version));
	payload.append((char*)&pgid, sizeof(pgid));
	payload.append((char*)&info, sizeof(info));
  }
  void decode_payload() {
	int off = 0;
	payload.copy(off, sizeof(map_version), (char*)&map_version);
	off += sizeof(map_version);
	payload.copy(off, sizeof(pgid), (char*)&pgid);
	off += sizeof(pgid);
	payload.copy(off, sizeof(info), (char*)&info);
	off += sizeof(info);
  }
};

#endif
