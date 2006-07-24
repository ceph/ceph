#ifndef __MOSDBOOT_H
#define __MOSDBOOT_H

#include "msg/Message.h"

#include "include/types.h"

class MOSDBoot : public Message {
 public:
  OSDSuperblock sb;

  MOSDBoot() {}
  MOSDBoot(OSDSuperblock& s) : 
	Message(MSG_OSD_BOOT),
	sb(s) {
  }

  char *get_type_name() { return "oboot"; }
  
  void encode_payload() {
	payload.append((char*)&sb, sizeof(sb));
  }
  void decode_payload() {
	int off = 0;
	payload.copy(off, sizeof(sb), (char*)&sb);
	off += sizeof(sb);
  }
};

#endif
