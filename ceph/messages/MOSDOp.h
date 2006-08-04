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


#ifndef __MOSDOP_H
#define __MOSDOP_H

#include "msg/Message.h"

#include "osd/OSDMap.h"

/*
 * OSD op
 *
 * oid - object id
 * op  - OSD_OP_DELETE, etc.
 *
 */

//#define OSD_OP_MKFS       20

// client ops
#define OSD_OP_READ       1
#define OSD_OP_STAT       2

#define OSD_OP_WRNOOP     10
#define OSD_OP_WRITE      11
#define OSD_OP_DELETE     12
#define OSD_OP_TRUNCATE   13
#define OSD_OP_ZERO       14

#define OSD_OP_WRLOCK     20
#define OSD_OP_WRUNLOCK   21
#define OSD_OP_RDLOCK     22
#define OSD_OP_RDUNLOCK   23
#define OSD_OP_UPLOCK     24
#define OSD_OP_DNLOCK     25


#define OSD_OP_IS_REP(x)  ((x) >= 30)

// replication/recovery -- these ops are relative to a specific object version #
#define OSD_OP_REP_WRITE    (100+OSD_OP_WRITE)     // replicated (partial object) write
#define OSD_OP_REP_WRNOOP   (100+OSD_OP_WRNOOP)     // replicated (partial object) write
#define OSD_OP_REP_TRUNCATE (100+OSD_OP_TRUNCATE)  // replicated truncate
#define OSD_OP_REP_DELETE   (100+OSD_OP_DELETE)
#define OSD_OP_REP_WRLOCK   (100+OSD_OP_WRLOCK)
#define OSD_OP_REP_WRUNLOCK (100+OSD_OP_WRUNLOCK)
#define OSD_OP_REP_RDLOCK   (100+OSD_OP_RDLOCK)
#define OSD_OP_REP_RDUNLOCK (100+OSD_OP_RDUNLOCK)
#define OSD_OP_REP_UPLOCK   (100+OSD_OP_UPLOCK)
#define OSD_OP_REP_DNLOCK   (100+OSD_OP_DNLOCK)

#define OSD_OP_REP_PULL     30   // whole object read
//#define OSD_OP_REP_PUSH     31   // whole object write
//#define OSD_OP_REP_REMOVE   32   // delete replica

//#define OSD_OP_FLAG_TRUNCATE  1   // truncate object after end of write

typedef struct {
  tid_t tid;
  long pcid;
  msg_addr_t asker;

  tid_t      orig_tid;
  msg_addr_t orig_asker;

  object_t oid;
  pg_t pg;
  int        pg_role;//, rg_nrep;
  epoch_t map_epoch;

  eversion_t pg_trim_to;   // primary->replica: trim to here

  int op;
  size_t length, offset;
  eversion_t version;
  eversion_t old_version;

  bool   want_ack;
  bool   want_commit;

  //epoch_t _included_map_epoch;
  size_t _data_len;//, _osdmap_len;

} MOSDOp_st;

class MOSDOp : public Message {
public:
  static const char* get_opname(int op) {
	switch (op) {
	case OSD_OP_WRITE: return "write"; 
	case OSD_OP_ZERO: return "zero"; 
	case OSD_OP_DELETE: return "delete"; 
	case OSD_OP_TRUNCATE: return "truncate"; 
	case OSD_OP_WRLOCK: return "wrlock"; 
	case OSD_OP_WRUNLOCK: return "wrunlock"; 
	case OSD_OP_RDLOCK: return "rdlock"; 
	case OSD_OP_RDUNLOCK: return "rdunlock"; 
	case OSD_OP_UPLOCK: return "uplock"; 
	case OSD_OP_DNLOCK: return "dnlock"; 
	case OSD_OP_WRNOOP: return "wrnoop"; 
	default: assert(0);
	}
	return 0;
  }

private:
  MOSDOp_st st;
  bufferlist data;
  //bufferlist osdmap;

  friend class MOSDOpReply;

 public:

  const tid_t       get_tid() { return st.tid; }
  const msg_addr_t& get_asker() { return st.asker; }

  const tid_t       get_orig_tid() { return st.orig_tid; }
  const msg_addr_t& get_orig_asker() { return st.orig_asker; }
  void set_orig_tid(tid_t t) { st.orig_tid = t; }
  void set_orig_asker(const msg_addr_t& a) { st.orig_asker = a; }

  const object_t   get_oid() { return st.oid; }
  const pg_t get_pg() { return st.pg; }
  const epoch_t  get_map_epoch() { return st.map_epoch; }

  const int        get_pg_role() { return st.pg_role; }  // who am i asking for?
  const eversion_t  get_version() { return st.version; }
  const eversion_t  get_old_version() { return st.old_version; }

  const eversion_t get_pg_trim_to() { return st.pg_trim_to; }
  void set_pg_trim_to(eversion_t v) { st.pg_trim_to = v; }
  
  const int    get_op() { return st.op; }
  void set_op(int o) { st.op = o; }

  const size_t get_length() { return st.length; }
  const size_t get_offset() { return st.offset; }

  const bool wants_ack() { return st.want_ack; }
  const bool wants_commit() { return st.want_commit; }

  
  void set_data(bufferlist &d) {
	data.claim(d);
  }
  bufferlist& get_data() {
	return data;
  }
  size_t get_data_len() { return st._data_len; }

  /*void attach_map(OSDMap *o) {
	o->encode(osdmap);
	st._included_map_epoch = o->get_epoch();
  }
  epoch_t get_osdmap_epoch() {
	return st._included_map_epoch;
  }
  bufferlist& get_osdmap_bl() { 
	return osdmap;
	}*/


  // keep a pcid (procedure call id) to match up request+reply
  void set_pcid(long pcid) { this->st.pcid = pcid; }
  long get_pcid() { return st.pcid; }

  MOSDOp(long tid, msg_addr_t asker, 
		 object_t oid, pg_t pg, epoch_t mapepoch, int op) :
	Message(MSG_OSD_OP) {
	memset(&st, 0, sizeof(st));
	this->st.orig_tid = this->st.tid = tid;
	this->st.orig_asker = this->st.asker = asker;

	this->st.oid = oid;
	this->st.pg = pg;
	this->st.pg_role = 0;
	this->st.map_epoch = mapepoch;
	this->st.op = op;

	this->st.want_ack = true;
	this->st.want_commit = true;
  }
  MOSDOp() {}

  void set_pg_role(int r) { st.pg_role = r; }
  //void set_rg_nrep(int n) { st.rg_nrep = n; }

  void set_length(size_t l) { st.length = l; }
  void set_offset(size_t o) { st.offset = o; }
  void set_version(eversion_t v) { st.version = v; }
  void set_old_version(eversion_t ov) { st.old_version = ov; }
  
  void set_want_ack(bool b) { st.want_ack = b; }
  void set_want_commit(bool b) { st.want_commit = b; }

  // marshalling
  virtual void decode_payload() {
	payload.copy(0, sizeof(st), (char*)&st);
	payload.splice(0, sizeof(st));
	if (st._data_len) payload.splice(0, st._data_len, &data);
	//if (st._osdmap_len) payload.splice(0, st._osdmap_len, &osdmap);
  }
  virtual void encode_payload() {
	st._data_len = data.length();
	//st._osdmap_len = osdmap.length();
	payload.push_back( new buffer((char*)&st, sizeof(st)) );
	payload.claim_append( data );
	//payload.claim_append( osdmap );
  }

  virtual char *get_type_name() { return "oop"; }
};

inline ostream& operator<<(ostream& out, MOSDOp& op)
{
  return out << "MOSDOp(" << MSG_ADDR_NICE(op.get_asker()) << "." << op.get_tid() 
			 << " op " << op.get_op()
			 << " oid " << hex << op.get_oid() << dec << " " << &op << ")";
}

#endif
