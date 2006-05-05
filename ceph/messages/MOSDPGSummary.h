// -*- mode:C++; tab-width:4; c-basic-offset:2; indent-tabs-mode:t -*- 
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software 
 * Foundation.  See file COPYING.
 * 
 */


#ifndef __MOSDPGQUERYREPLY_H
#define __MOSDPGQUERYREPLY_H

#include "msg/Message.h"

class MOSDPGSummary : public Message {
  epoch_t epoch;
  pg_t pgid;
  bufferlist    sumbl;

public:
  epoch_t get_epoch() { return epoch; }
  
  MOSDPGSummary() {}
  MOSDPGSummary(version_t mv, pg_t pgid, PG::PGContentSummary *sum) :
	Message(MSG_OSD_PG_SUMMARY) {
	this->epoch = mv;
	this->pgid = pgid;
	sum->_encode(sumbl);
  }

  pg_t get_pgid() { return pgid; }
  PG::PGContentSummary *get_summary() {
	PG::PGContentSummary *sum = new PG::PGContentSummary;
	int off = 0;
	sum->_decode(sumbl,off);
	return sum;
  }
  
  char *get_type_name() { return "PGsum"; }

  void encode_payload() {
	payload.append((char*)&epoch, sizeof(epoch));
	payload.append((char*)&pgid, sizeof(pgid));
	payload.claim_append(sumbl);
  }
  void decode_payload() {
	int off = 0;
	payload.copy(off, sizeof(epoch), (char*)&epoch);
	off += sizeof(epoch);
	payload.copy(off, sizeof(pgid), (char*)&pgid);
	off += sizeof(pgid);

	payload.splice(0, off);
	sumbl.claim(payload);
  }
};

#endif
