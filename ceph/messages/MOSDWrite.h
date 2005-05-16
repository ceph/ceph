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
  long pcid;
  off_t offset;
  object_t oid;
  int flags;
  size_t len;
} MOSDWrite_st;

class MOSDWrite : public Message {
  MOSDWrite_st st;

  friend class MOSDWriteReply;

 public:
  long get_tid() { return st.tid; }
  off_t get_offset() { return st.offset; }
  object_t get_oid() { return st.oid; }
  int get_flags() { return st.flags; }
  long get_len() { return st.len; }

  char *get_buffer() {
	if (!raw_message) {
	  raw_message_len = MSG_ENVELOPE_LEN + sizeof(MOSDWrite_st) + st.len;
	  raw_message = new char[raw_message_len];
	}
	return raw_message + MSG_ENVELOPE_LEN + sizeof(st);
  }

  // keep a pcid (procedure call id) to match up request+reply
  void set_pcid(long pcid) { this->st.pcid = pcid; }
  long get_pcid() { return st.pcid; }

  MOSDWrite() {}
  MOSDWrite(long tid, object_t oid, size_t len, off_t offset, const char *buffer=0, int flags=0) :
	Message(MSG_OSD_WRITE) {
	this->st.tid = tid;
	this->st.oid = oid;
	this->st.offset = offset;
	this->st.flags = flags;
	this->st.len = len;

	if (buffer) 
	  memcpy(get_buffer(), buffer, len);
  }
  
  virtual void decode_payload() {
	st = *(MOSDWrite_st*)(raw_message + MSG_ENVELOPE_LEN);
  }

  virtual void encode() {
	raw_message_len = MSG_ENVELOPE_LEN + sizeof(st) + st.len;
	if (!raw_message) {
	  assert(st.len == 0);
	  raw_message = new char[raw_message_len];
	}

	encode_envelope();

	*(MOSDWrite_st*)(raw_message+MSG_ENVELOPE_LEN) = st;
  }

  virtual char *get_type_name() { return "owr"; }
};

#endif
