#ifndef __MOSDREADREPLY_H
#define __MOSDREADREPLY_H

#include "MOSDRead.h"

/*
 * OSD Read Reply
 *
 * oid - object id
 * offset, len - data returned
 *
 * len may not match the read request, if the end of object is hit.
 */

typedef struct {
  long tid;
  long pcid;
  off_t offset;
  object_t oid;  
  size_t len;
  long result;
} MOSDReadReply_st;

class MOSDReadReply : public Message {
  MOSDReadReply_st st;

 public:
  size_t get_len() { return st.len; }
  int get_result() { return st.result; }
  object_t get_oid() { return st.oid; }
  off_t get_offset() { return st.offset; }
  long get_tid() { return st.tid; }

  // keep a pcid (procedure call id) to match up request+reply
  void set_pcid(long pcid) { this->st.pcid = pcid; }
  long get_pcid() { return st.pcid; }

  MOSDReadReply() { 
  }
  MOSDReadReply(MOSDRead *r, long result) :
	Message(MSG_OSD_READREPLY) {
	this->st.tid = r->st.tid;
	this->st.pcid = r->st.pcid;
	this->st.oid = r->st.oid;
	this->st.offset = r->st.offset;
	this->st.result = result;
	if (result >= 0) 
	  this->st.len = result;
	else
	  this->st.len = 0;	  
  }
  ~MOSDReadReply() {
  }

  // called by sender and receiver.  on sender, allocates raw_message buffer.
  char *get_buffer() {
	if (!raw_message) {
	  raw_message_len = MSG_ENVELOPE_LEN + sizeof(MOSDReadReply_st) + st.len;
	  raw_message = new char[raw_message_len];
	}
	return raw_message + MSG_ENVELOPE_LEN + sizeof(MOSDReadReply_st);
  }
  void set_len(int len) {
	this->st.len = len;
  }
  void set_result(int result) {
	this->st.result = result;
  }
  
  virtual void decode_payload() {
	st = *(MOSDReadReply_st*)(raw_message + MSG_ENVELOPE_LEN);
  }
  virtual void encode() {
	raw_message_len = MSG_ENVELOPE_LEN + sizeof(st) + st.len;
	if (!raw_message) {
	  assert(st.len == 0);
	  raw_message = new char[raw_message_len];
	}

	encode_envelope();

	*(MOSDReadReply_st*)(raw_message+MSG_ENVELOPE_LEN) = st;
  }

  virtual char *get_type_name() { return "oreadr"; }
};

#endif
