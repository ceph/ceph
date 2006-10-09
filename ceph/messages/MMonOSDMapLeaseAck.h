#ifndef __MMONOSDMAPLEASEACK_H
#define __MMONOSDMAPLEASEACK_H

#include "msg/Message.h"

#include "include/types.h"

class MMonOSDMapLeaseAck : public Message {
  epoch_t epoch;

 public:
  epoch_t get_epoch() { return epoch; }

  MMonOSDMapLeaseAck(epoch_t e) :
	Message(MSG_MON_OSDMAP_LEASE_ACK),
	epoch(e) {
  }

  char *get_type_name() { return "omap_lease_ack"; }
  
  void encode_payload() {
	payload.append((char*)&epoch, sizeof(epoch));
  }
  void decode_payload() {
	int off = 0;
	payload.copy(off, sizeof(epoch), (char*)&epoch);
	off += sizeof(epoch);
  }
};

#endif
