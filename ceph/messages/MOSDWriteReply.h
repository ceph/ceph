#ifndef __MOSDWRITEREPLY_H
#define __MOSDWRITEREPLY_H

#include "MOSDWrite.h"

class MOSDWriteReply : public Message {
 public:
  long tid;
  long len;
  off_t offset;
  object_t oid;
  MOSDWriteReply(MOSDWrite *r, long wrote) :
	Message(MSG_OSD_WRITEREPLY) {
	this->tid = r->tid;
	this->len = wrote;
	this->oid = r->oid;
	this->offset = r->offset;
  }
};

#endif
