#ifndef __MOSDWRITEREPLY_H
#define __MOSDWRITEREPLY_H

#include "MOSDWrite.h"

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

  virtual int decode_payload(crope s) {
	st = *((MOSDWriteReply_st*)s.c_str());
  }
  virtual crope get_payload() {
	return crope( (char*)&st, sizeof(st) );
  }

  virtual char *get_type_name() { return "owrr"; }
};

#endif
