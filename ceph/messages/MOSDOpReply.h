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


#ifndef __MOSDOPREPLY_H
#define __MOSDOPREPLY_H

#include "msg/Message.h"
#include "osd/OSDMap.h"

#include "MOSDOp.h"

/*
 * OSD op reply
 *
 * oid - object id
 * op  - OSD_OP_DELETE, etc.
 *
 */


typedef struct {
  // req
  long tid;
  long pcid;

  object_t oid;
  pg_t pg;

  int op;
  
  // reply
  int    result;
  bool   commit;
  size_t length, offset;
  size_t object_size;
  version_t version;

  epoch_t _new_map_epoch;
  size_t _data_len, _oc_len;
} MOSDOpReply_st;


class MOSDOpReply : public Message {
  MOSDOpReply_st st;
  bufferlist data;
  bufferlist osdmap;

 public:
  long     get_tid() { return st.tid; }
  object_t get_oid() { return st.oid; }
  pg_t     get_pg() { return st.pg; }
  int      get_op()  { return st.op; }
  bool     get_commit() { return st.commit; }
  
  int    get_result() { return st.result; }
  size_t get_length() { return st.length; }
  size_t get_offset() { return st.offset; }
  size_t get_object_size() { return st.object_size; }
  version_t get_version() { return st.version; }

  void set_result(int r) { st.result = r; }
  void set_length(size_t s) { st.length = s; }
  void set_offset(size_t o) { st.offset = o; }
  void set_object_size(size_t s) { st.object_size = s; }
  void set_version(version_t v) { st.version = v; }

  // data payload
  void set_data(bufferlist &d) {
	data.claim(d);
	st._data_len = data.length();
  }
  bufferlist& get_data() {
	return data;
  }

  // osdmap
  epoch_t get_map_epoch() { return st._new_map_epoch; }
  bufferlist& get_osdmap() { 
	return osdmap;
  }

  // keep a pcid (procedure call id) to match up request+reply
  void set_pcid(long pcid) { this->st.pcid = pcid; }
  long get_pcid()          { return st.pcid; }

  MOSDOpReply(MOSDOp *req, int result, OSDMap *oc, bool commit) :
	Message(MSG_OSD_OPREPLY) {
	memset(&st, 0, sizeof(st));
	this->st.pcid = req->st.pcid;
	this->st.tid = req->st.tid;

	this->st.oid = req->st.oid;
	this->st.pg = req->st.pg;
	this->st.op = req->st.op;
	this->st.result = result;
	this->st.commit = commit;

	this->st.length = req->st.length;   // speculative... OSD should ensure these are correct
	this->st.offset = req->st.offset;
	this->st.version = req->st.version;

	// attach updated cluster spec?
	if (oc &&
		req->get_map_epoch() < oc->get_epoch()) {
	  oc->encode(osdmap);
	  st._new_map_epoch = oc->get_epoch();
	  st._oc_len = osdmap.length();
	}
  }
  MOSDOpReply() {}


  // marshalling
  virtual void decode_payload() {
	payload.copy(0, sizeof(st), (char*)&st);
	payload.splice(0, sizeof(st));
	if (st._data_len) payload.splice(0, st._data_len, &data);
	if (st._oc_len) payload.splice(0, st._oc_len, &osdmap);
  }
  virtual void encode_payload() {
	payload.push_back( new buffer((char*)&st, sizeof(st)) );
	payload.claim_append( data );
	payload.claim_append( osdmap );
  }

  virtual char *get_type_name() { return "oopr"; }
};

#endif
