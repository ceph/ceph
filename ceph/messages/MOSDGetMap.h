#ifndef __MOSDGETMAP_H
#define __MOSDGETMAP_H

#include "msg/Message.h"

#include "include/types.h"

class MOSDGetMap : public Message {
 public:
  epoch_t since;

  //MOSDGetMap() : since(0) {}
  MOSDGetMap(epoch_t s=0) : 
	Message(MSG_OSD_GETMAP),
	since(s) {
  }

  epoch_t get_since() { return since; }

  char *get_type_name() { return "getomap"; }
  
  void encode_payload() {
	payload.append((char*)&since, sizeof(since));
  }
  void decode_payload() {
	int off = 0;
	payload.copy(off, sizeof(since), (char*)&since);
	off += sizeof(since);
  }
};

#endif
