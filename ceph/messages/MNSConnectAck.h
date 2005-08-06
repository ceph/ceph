#ifndef __MNSCONNECTACK_H
#define __MNSCONNECTACK_H

#include "msg/Message.h"
#include "msg/TCPMessenger.h"

class MNSConnectAck : public Message {
  int rank;

 public:
  MNSConnectAck() {}
  MNSConnectAck(int r) : 
	Message(MSG_NS_CONNECTACK) { 
	rank = r;
  }
  
  char *get_type_name() { return "NSConA"; }

  int get_rank() { return rank; }

  void encode_payload() {
	payload.append((char*)&rank, sizeof(rank));
  }
  void decode_payload() {
	payload.copy(0, sizeof(rank), (char*)&rank);
  }
};


#endif

