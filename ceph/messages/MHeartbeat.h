#ifndef __MHEARTBEAT_H
#define __MHEARTBEAT_H

#include "include/Message.h"
#include "include/types.h"

class MHeartbeat : public Message {
 public:
  mds_load_t load;
  int        beat;

  MHeartbeat(mds_load_t& load, int beat) :
	Message(MSG_MDS_HEARTBEAT) {
	this->load = load;
	this->beat = beat;
  }

};

#endif
