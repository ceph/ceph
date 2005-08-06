#ifndef __MNSCONNECT_H
#define __MNSCONNECT_H

#include "msg/Message.h"
#include "msg/TCPMessenger.h"

class MNSConnect : public Message {
  tcpaddr_t tcpaddr;

 public:
  MNSConnect() {}
  MNSConnect(tcpaddr_t t) :
	Message(MSG_NS_CONNECT) { 
	tcpaddr = t;
  }
  
  char *get_type_name() { return "NSCon"; }

  tcpaddr_t& get_addr() { return tcpaddr; }

  void encode_payload() {
	payload.append((char*)&tcpaddr, sizeof(tcpaddr));
  }
  void decode_payload() {
	payload.copy(0, sizeof(tcpaddr), (char*)&tcpaddr);
  }
};


#endif

