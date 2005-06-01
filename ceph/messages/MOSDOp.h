#ifndef __MOSDOP_H
#define __MOSDOP_H

#include "msg/Message.h"

/*
 * OSD op
 *
 * oid - object id
 * op  - OSD_OP_DELETE, etc.
 *
 */

#define OSD_OP_STAT       1
#define OSD_OP_DELETE     2
#define OSD_OP_ZERORANGE  3
#define OSD_OP_MKFS       10

typedef struct {
  long tid;
  long pcid;
  object_t oid;
  int op;

  size_t length, offset;
} MOSDOp_st;

class MOSDOp : public Message {
  MOSDOp_st st;

  friend class MOSDOpReply;

 public:
  long get_tid() { return st.tid; }
  object_t get_oid() { return st.oid; }
  int get_op() { return st.op; }

  // keep a pcid (procedure call id) to match up request+reply
  void set_pcid(long pcid) { this->st.pcid = pcid; }
  long get_pcid() { return st.pcid; }

  MOSDOp(long tid, object_t oid, int op) :
	Message(MSG_OSD_OP) {
	this->st.tid = tid;
	this->st.oid = oid;
	this->st.op = op;
  }
  MOSDOp() {}

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
