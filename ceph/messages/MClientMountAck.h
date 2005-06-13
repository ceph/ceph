#ifndef __MCLIENTMOUNTACK_H
#define __MCLIENTMOUNTACK_H

#include "msg/Message.h"
#include "MClientMount.h"
#include "osd/OSDCluster.h"


class MClientMountAck : public Message {
  long pcid;
  bufferlist osd_cluster_state;

 public:
  MClientMountAck() {}
  MClientMountAck(MClientMount *mnt, OSDCluster *osdcluster) : Message(MSG_CLIENT_MOUNTACK) { 
	this->pcid = mnt->get_pcid();
	osdcluster->encode( osd_cluster_state );
  }
  
  bufferlist& get_osd_cluster_state() { return osd_cluster_state; }

  void set_pcid(long pcid) { this->pcid = pcid; }
  long get_pcid() { return pcid; }

  char *get_type_name() { return "CmntA"; }

  virtual void decode_payload() {  
	int off;
	payload.copy(off, sizeof(pcid), (char*)&pcid);
	off += sizeof(pcid);
	if (off < payload.length())
	  payload.splice( off, payload.length()-off, &osd_cluster_state);
  }
  virtual void encode_payload() {  
	payload.append((char*)&pcid, sizeof(pcid));
	payload.claim_append(osd_cluster_state);
  }
};

#endif
