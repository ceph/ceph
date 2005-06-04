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
  //int flags;
  size_t len;
} MOSDWrite_st;

class MOSDWrite : public Message {
  MOSDWrite_st st;
  bufferlist   data;

  friend class MOSDWriteReply;

 public:
  long get_tid() { return st.tid; }
  off_t get_offset() { return st.offset; }
  object_t get_oid() { return st.oid; }
  //int get_flags() { return st.flags; }
  long get_len() { return st.len; }

  // keep a pcid (procedure call id) to match up request+reply
  void set_pcid(long pcid) { this->st.pcid = pcid; }
  long get_pcid() { return st.pcid; }

  MOSDWrite() {}
  MOSDWrite(long tid, object_t oid, size_t len, off_t offset) :
	Message(MSG_OSD_WRITE) {
	this->st.tid = tid;
	this->st.oid = oid;
	this->st.offset = offset;
	//this->st.flags = flags;
	this->st.len = len;
	this->st.pcid = 0;
  }

  void set_data(bufferlist &d) {
	data.claim(d);
	assert(data.length() == st.len);
  }
  bufferlist& get_data() {
	return data;
  }

  
  virtual void decode_payload() {
	payload.copy(0, sizeof(st), (char*)&st);
	payload.splice(0, sizeof(st));
	data.claim(payload);
  }

  virtual void encode_payload() {
	payload.push_back( new buffer((char*)&st, sizeof(st)) );
	payload.claim_append( data );
  }

  virtual char *get_type_name() { return "owr"; }
};

#endif
