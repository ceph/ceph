#ifndef __MOSDWRITE_H
#define __MOSDWRITE_H

#include "msg/Message.h"

/*
 * OSD Write
 *
 * tid - caller's transaction id
 * 
 * oid - object id
 * offset, len - 
 *
 * flags - passed to open().  not used at all.. this should be removed?
 * 
 */


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

  virtual void decode_payload(crope& s) {
	int off = 0;
	s.copy(off, sizeof(st), (char*)&st);
	off += sizeof(st);
	buffer = s.substr(off, st.len);
	off += st.len;
  }
  virtual void encode_payload(crope& s) {
	assert(buffer.length() == st.len);
	s.append((char*)&st,sizeof(st));
	s.append(buffer);
  }

  virtual char *get_type_name() { return "owr"; }
};

#endif
