#ifndef __MHEARTBEAT_H
#define __MHEARTBEAT_H

#include "include/Message.h"
#include "include/types.h"

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

  virtual int decode_payload(crope s) {
	s.copy(0,sizeof(load), (char*)&load);
	s.copy(sizeof(load), sizeof(beat), (char*)&beat);
  }
  virtual crope get_payload() {
	crope s;
	s.append((char*)&load, sizeof(load));
	s.append((char*)&beat, sizeof(beat));
	return s;
  }

};

#endif
