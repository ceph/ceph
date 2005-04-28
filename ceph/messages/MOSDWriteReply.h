#ifndef __MOSDWRITEREPLY_H
#define __MOSDWRITEREPLY_H

#include "MOSDWrite.h"

/*
 * OSD WRite Reply
 *
 * tid - caller's transaction #
 * oid - object id
 * offset, len - ...
 * result - result code, matchines write() system call: # of bytes written, or error code.
 */

typedef struct {
  long tid;
  long result;
  off_t offset;
  object_t oid;
} MOSDWriteReply_st;

class MOSDWriteReply : public Message {
  MOSDWriteReply_st st;
  
 public:
  long get_tid() { return st.tid; }
  long get_result() { return st.result; }
  off_t get_offset() { return st.offset; }
  object_t get_oid() { return st.oid; }

  MOSDWriteReply() {}
  MOSDWriteReply(MOSDWrite *r, long wrote) :
	Message(MSG_OSD_WRITEREPLY) {
	this->st.tid = r->st.tid;
	this->st.oid = r->st.oid;
	this->st.offset = r->st.offset;
	this->st.result = wrote;
  }

  virtual void decode_payload(crope& s) {
	s.copy(0, sizeof(st), (char*)&st);
  }
  virtual void encode_payload(crope& s) {
	s.append((char*)&st, sizeof(st));
  }

  virtual char *get_type_name() { return "owrr"; }
};

#endif
