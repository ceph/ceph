#ifndef __MOSDOPREPLY_H
#define __MOSDOPREPLY_H

#include "msg/Message.h"

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
  int op;

  // reply
  int    result;
  size_t size;
} MOSDOpReply_st;

class MOSDOpReply : public Message {
  MOSDOpReply_st st;

 public:
  long     get_tid() { return st.tid; }
  object_t get_oid() { return st.oid; }
  int      get_op()  { return st.op; }
  
  int    get_result() { return st.result; }
  size_t get_size()   { return st.size; }

  void set_size(size_t s) { st.size = s; }

  // keep a pcid (procedure call id) to match up request+reply
  void set_pcid(long pcid) { this->st.pcid = pcid; }
  long get_pcid()          { return st.pcid; }

  MOSDOpReply(MOSDOp *req, int result) :
	Message(MSG_OSD_OPREPLY) {
	this->st.pcid = req->st.pcid;
	this->st.tid = req->st.tid;
	this->st.oid = req->st.oid;
	this->st.op = req->st.op;

	this->st.result = result;
  }
  MOSDOpReply() {}

  virtual void decode_payload(crope& s, int& off) {
	s.copy(off, sizeof(st), (char*)&st);
	off += sizeof(st);
  }
  virtual void encode_payload(crope& s) {
	s.append((char*)&st, sizeof(st));
  }

  virtual char *get_type_name() { return "oop"; }
};

#endif
