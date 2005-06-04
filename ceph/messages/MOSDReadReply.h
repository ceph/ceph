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
  bufferlist data;

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
	this->st.len = 0;
  }

  bufferlist& get_data() {
	return data;
  }
  void set_data(bufferlist &bl) {
	data.claim(bl);
	this->st.len = data.length();
  }
  void set_result(int result) {
	this->st.result = result;
  }
  
  virtual void decode_payload() {
	// warning: only call this once, we modify the payload!
	payload.copy(0, sizeof(st), (char*)&st);
	payload.splice(0, sizeof(st));
	data.claim(payload);
  }
  virtual void encode_payload() {
	payload.push_back( new buffer((char*)&st, sizeof(st)) );
	payload.claim_append(data);
  }
  
  virtual char *get_type_name() { return "oreadr"; }
};

#endif
