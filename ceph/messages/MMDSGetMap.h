#ifndef __MMDSGETMAP_H
#define __MMDSGETMAP_H

#include "msg/Message.h"

#include "include/types.h"

class MMDSGetMap : public Message {
 public:
  MMDSGetMap() : Message(MSG_MDS_GETMAP) {
  }

  char *get_type_name() { return "mdsgetmap"; }
  
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
