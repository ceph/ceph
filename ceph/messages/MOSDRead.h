#ifndef __MOSDREAD_H
#define __MOSDREAD_H

#include "include/Message.h"

typedef struct {
  long tid;
  size_t len;
  off_t offset;
  object_t oid;
} MOSDRead_st;

class MOSDRead : public Message {
  MOSDRead_st st;

  friend class MOSDReadReply;

 public:
  long get_tid() { return st.tid; }
  size_t get_len() { return st.len; }
  off_t get_offset() { return st.offset; }
  object_t get_oid() { return st.oid; }
  
  MOSDRead(long tid, object_t oid, size_t len, off_t offset) :
	Message(MSG_OSD_READ) {
	this->st.tid = tid;
	this->st.oid = oid;
	this->st.len = len;
	this->st.offset = offset;
  }
  MOSDRead() {}

  virtual int decode_payload(crope s) {
	st = *((MOSDRead_st*)s.c_str());
  }
  virtual crope get_payload() {
	return crope( (char*)&st, sizeof(st) );
  }
  virtual char *get_type_name() { return "oread"; }
};

#endif
