#ifndef __MOSDREADREPLY_H
#define __MOSDREADREPLY_H

#include "MOSDRead.h"

typedef struct {
  long tid;
  off_t offset;
  object_t oid;  
  size_t len;
} MOSDReadReply_st;

class MOSDReadReply : public Message {
  MOSDReadReply_st st;
  crope buffer;
  crope buffertest;

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
	if (buf) {
	  buffer.append( buf, len );
	  //	  buffertest.append( buffer );
	}
	this->st.len = len;
  }

  crope get_buffer() {
	return buffer;
  }
  
  virtual int decode_payload(crope s) {
	s.copy(0, sizeof(st), (char*)&st);
	buffer = s.substr(sizeof(st), s.length()-sizeof(st));
  }
  virtual crope get_payload() {
	crope payload;
	payload.append((char*)&st,sizeof(st));
	payload.append(buffer);
	return payload;
  }

  virtual char *get_type_name() { return "oreadr"; }
};

#endif
