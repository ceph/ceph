#ifndef __MOSDREAD_H
#define __MOSDREAD_H

#include "msg/Message.h"

/*
 * OSD read request
 *
 * oid - object id
 * offset, len -- guess
 *
 * caveat: if len=0, then the _entire_ object is read.  this is currently
 *   used by the MDS, and pretty much a dumb idea in general.
 */

typedef struct {
  long tid;
  long pcid;
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
  
  // keep a pcid (procedure call id) to match up request+reply
  void set_pcid(long pcid) { this->st.pcid = pcid; }
  long get_pcid() { return st.pcid; }

  MOSDRead(long tid, object_t oid, size_t len, off_t offset) :
	Message(MSG_OSD_READ) {
	this->st.tid = tid;
	this->st.oid = oid;
	this->st.len = len;
	this->st.offset = offset;
  }
  MOSDRead() {}

  virtual void decode_payload(crope& s) {
	s.copy(0, sizeof(st), (char*)&st);
  }
  virtual void encode_payload(crope& s) {
	s.append((char*)&st, sizeof(st));
  }

  virtual char *get_type_name() { return "oread"; }
};

#endif
