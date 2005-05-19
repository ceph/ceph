#ifndef __MCLIENTMOUNTACK_H
#define __MCLIENTMOUNTACK_H

#include "msg/Message.h"
#include "MClientMount.h"
#include "osd/OSDCluster.h"


class MClientMountAck : public Message {
  long pcid;
  crope osd_cluster_state;

 public:
  MClientMountAck() {}
  MClientMountAck(MClientMount *mnt, OSDCluster *osdcluster) : Message(MSG_CLIENT_MOUNTACK) { 
	this->pcid = mnt->get_pcid();
	osdcluster->_rope( osd_cluster_state );
  }
  
  crope& get_osd_cluster_state() { return osd_cluster_state; }

  void set_pcid(long pcid) { this->pcid = pcid; }
  long get_pcid() { return pcid; }

  char *get_type_name() { return "CmntA"; }

  virtual void decode_payload(crope& s, int& off) {  
	s.copy(off, sizeof(pcid), (char*)&pcid);
	off += sizeof(pcid);
	osd_cluster_state = s.substr(off, s.length()-off);
	off += osd_cluster_state.length();
  }
  virtual void encode_payload(crope& s) {  
	s.append((char*)&pcid, sizeof(pcid));
	s.append(osd_cluster_state);
  }
};

#endif
