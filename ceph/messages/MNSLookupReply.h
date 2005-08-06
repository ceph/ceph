#ifndef __MNSLOOKUPREPLY_H
#define __MNSLOOKUPREPLY_H

#include "msg/Message.h"
#include "msg/TCPMessenger.h"

class MNSLookupReply : public Message {
 public:
  map<msg_addr_t, int> entity_map;  // e -> rank
  map<int, tcpaddr_t>  rank_addr;   // rank -> addr

 public:
  MNSLookupReply() {}
  MNSLookupReply(MNSLookup *m) : 
	Message(MSG_NS_LOOKUPREPLY) { 
  }
  
  char *get_type_name() { return "NSLookR"; }

  /*
  void map_rank(int e, int r) {
	entity_map[e] = r;
  }
  void map_addr(int r, tcpaddr_t& a) {
	rank_addr[r] = a;
  }
  */

  void encode_payload() {
	int n = entity_map.size();
	payload.append((char*)&n, sizeof(n));
	for (map<msg_addr_t,int>::iterator it = entity_map.begin();
		 it != entity_map.end();
		 it++) {
	  payload.append((char*)&it->first, sizeof(it->first));
	  payload.append((char*)&it->second, sizeof(it->second));
	}

	n = rank_addr.size();
	payload.append((char*)&n, sizeof(n));
	for (map<int, tcpaddr_t>::iterator it = rank_addr.begin();
		 it != rank_addr.end();
		 it++) {
	  payload.append((char*)&it->first, sizeof(it->first));
	  payload.append((char*)&it->second, sizeof(it->second));
	}
  }
  void decode_payload() {
	int off = 0;
	int n;

	payload.copy(off, sizeof(n), (char*)&n);
	off += sizeof(n);
	for (int i=0; i<n; i++) {
	  msg_addr_t e;
	  int r;
	  payload.copy(off, sizeof(e), (char*)&e);
	  off += sizeof(e);
	  payload.copy(off, sizeof(r), (char*)&r);
	  off += sizeof(r);
	  entity_map[e] = r;
	}

	payload.copy(off, sizeof(n), (char*)&n);
	off += sizeof(n);
	for (int i=0; i<n; i++) {
	  int r;
	  tcpaddr_t a;
	  payload.copy(off, sizeof(r), (char*)&r);
	  off += sizeof(r);
	  payload.copy(off, sizeof(a), (char*)&a);
	  off += sizeof(a);
	  rank_addr[r] = a;
	}
  }
};


#endif

