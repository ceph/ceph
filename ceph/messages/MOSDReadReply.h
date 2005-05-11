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
  crope buffer;

 public:
  size_t get_len() { return st.len; }
  int get_result() { return st.result; }
  object_t get_oid() { return st.oid; }
  off_t get_offset() { return st.offset; }
  long get_tid() { return st.tid; }

  // keep a pcid (procedure call id) to match up request+reply
  void set_pcid(long pcid) { this->st.pcid = pcid; }
  long get_pcid() { return st.pcid; }

  MOSDReadReply() { }
  MOSDReadReply(MOSDRead *r, char *buf, long result) :
	Message(MSG_OSD_READREPLY) {
	this->st.tid = r->st.tid;
	this->st.pcid = r->st.pcid;
	this->st.oid = r->st.oid;
	this->st.offset = r->st.offset;
	this->st.result = result;
	if (buf && result > 0) buffer.append( buf, result );
  }

  crope& get_buffer() {
	return buffer;
  }
  
  virtual void decode_payload(crope& s) {
	int off = 0;
	s.copy(off, sizeof(st), (char*)&st);
	off += sizeof(st);
	buffer = s.substr(off, st.len);                
  }
  virtual void encode_payload(crope& payload) {
	st.len = buffer.length();
	payload.append((char*)&st, sizeof(st));
	payload.append(buffer);
  }

  virtual char *get_type_name() { return "oreadr"; }
};

#endif
