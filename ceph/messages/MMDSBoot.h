#ifndef __MMDSBOOT_H
#define __MMDSBOOT_H

#include "msg/Message.h"

#include "include/types.h"

class MMDSBoot : public Message {
 public:
  MMDSBoot() : Message(MSG_MDS_BOOT) {
  }

  char *get_type_name() { return "mdsboot"; }
  
  void encode_payload() {
    //payload.append((char*)&sb, sizeof(sb));
  }
  void decode_payload() {
    //int off = 0;
    //payload.copy(off, sizeof(sb), (char*)&sb);
    //off += sizeof(sb);
  }
};

#endif
