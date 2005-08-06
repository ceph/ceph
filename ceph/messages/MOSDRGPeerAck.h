#ifndef __MOSDRGPEERACK_H
#define __MOSDRGPEERACK_H

#include "msg/Message.h"
#include "osd/OSD.h"

class MOSDRGPeerAck : public Message {
  __uint64_t       map_version;

 public:
  list<repgroup_t>                rg_dne;   // rg dne
  map<repgroup_t, RGReplicaInfo > rg_state; // state, lists, etc.

  __uint64_t get_version() { return map_version; }

  MOSDRGPeerAck() {}
  MOSDRGPeerAck(__uint64_t v) :
	Message(MSG_OSD_RG_PEERACK) {
	this->map_version = v;
  }
  
  char *get_type_name() { return "RGPeer"; }

  void encode_payload() {
	payload.append((char*)&map_version, sizeof(map_version));
	_encode(rg_dne, payload);
	
	int n = rg_state.size();
	payload.append((char*)&n, sizeof(n));
	for (map<repgroup_t, RGReplicaInfo >::iterator it = rg_state.begin();
		 it != rg_state.end();
		 it++) {
	  payload.append((char*)&it->first, sizeof(it->first));
	  it->second._encode(payload);
	}
  }
  void decode_payload() {
	int off = 0;
	payload.copy(off, sizeof(map_version), (char*)&map_version);
	off += sizeof(map_version);
	_decode(rg_dne, payload, off);

	int n;
	payload.copy(off, sizeof(n), (char*)&n);
	off += sizeof(n);
	for (int i=0; i<n; i++) {
	  repgroup_t rgid;
	  payload.copy(off, sizeof(rgid), (char*)&rgid);
	  off += sizeof(rgid);
	  rg_state[rgid]._decode(payload, off);
	}
  }
};

#endif
