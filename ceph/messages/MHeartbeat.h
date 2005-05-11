#ifndef __MHEARTBEAT_H
#define __MHEARTBEAT_H

#include "include/types.h"
#include "msg/Message.h"

class MHeartbeat : public Message {
  mds_load_t load;
  int        beat;

 public:
  mds_load_t& get_load() { return load; }
  int get_beat() { return beat; }

  MHeartbeat() {}
  MHeartbeat(mds_load_t& load, int beat) :
	Message(MSG_MDS_HEARTBEAT) {
	this->load = load;
	this->beat = beat;
  }

  virtual char *get_type_name() { return "HB"; }

  virtual void decode_payload(crope& s, int& off) {
	s.copy(off,sizeof(load), (char*)&load);
	off += sizeof(load);
	s.copy(off, sizeof(beat), (char*)&beat);
	off += sizeof(beat);
  }
  virtual void encode_payload(crope& s) {
	s.append((char*)&load, sizeof(load));
	s.append((char*)&beat, sizeof(beat));
  }

};

#endif
