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
  off_t offset;
  object_t oid;  
  size_t len;
} MOSDReadReply_st;

class MOSDReadReply : public Message {
  MOSDReadReply_st st;
  crope buffer;

 public:
  size_t get_len() { return st.len; }
  object_t get_oid() { return st.oid; }
  off_t get_offset() { return st.offset; }
  long get_tid() { return st.tid; }

  MOSDReadReply() { }
  MOSDReadReply(MOSDRead *r, char *buf, long len) :
	Message(MSG_OSD_READREPLY) {
	this->st.tid = r->st.tid;
	this->st.oid = r->st.oid;
	this->st.offset = r->st.offset;
	if (buf) 
	  buffer.append( buf, len );
	this->st.len = len;
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
	assert(buffer.length() == st.len);
	payload.append((char*)&st, sizeof(st));
	payload.append(buffer);
  }

  virtual char *get_type_name() { return "oreadr"; }
};

#endif
