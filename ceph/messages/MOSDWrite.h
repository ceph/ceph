#ifndef __MOSDWRITE_H
#define __MOSDWRITE_H

#include "include/Message.h"

typedef struct {
  long tid;
  off_t offset;
  object_t oid;
  int flags;
  size_t len;
} MOSDWrite_st;

class MOSDWrite : public Message {
  MOSDWrite_st st;
  crope buffer;

  friend class MOSDWriteReply;

 public:
  long get_tid() { return st.tid; }
  off_t get_offset() { return st.offset; }
  object_t get_oid() { return st.oid; }
  int get_flags() { return st.flags; }
  long get_len() { return st.len; }

  const char *get_buf() {
	return buffer.c_str();
  }

  MOSDWrite(long tid, object_t oid, size_t len, off_t offset, crope& buffer, int flags=0) :
	Message(MSG_OSD_WRITE) {
	this->st.tid = tid;
	this->st.oid = oid;
	this->st.offset = offset;
	this->st.flags = flags;
	this->st.len = len;
	this->buffer = buffer;
  }
  MOSDWrite() {}

  virtual int decode_payload(crope s) {
	s.copy(0, sizeof(st), (char*)&st);
	buffer = s.substr(sizeof(st), s.length() - sizeof(st));
  }
  virtual crope get_payload() {
	crope payload;
	payload.append((char*)&st,sizeof(st));
	payload.append(buffer);
	return payload;
  }

  virtual char *get_type_name() { return "owr"; }
};

#endif
