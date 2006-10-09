#ifndef __MMONOSDMAPUPDATECOMMIT_H
#define __MMONOSDMAPUPDATECOMMIT_H

#include "msg/Message.h"

#include "include/types.h"

class MMonOSDMapUpdateCommit : public Message {
 public:
  epoch_t epoch;

  MMonOSDMapUpdateCommit(epoch_t e) :
	Message(MSG_MON_OSDMAP_UPDATE_COMMIT),
	epoch(e) {
  }

  char *get_type_name() { return "omap_update_commit"; }
  
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
