#ifndef __MOSDREADREPLY_H
#define __MOSDREADREPLY_H

#include "MBuffer.h"
#include "MOSDRead.h"

class MOSDReadReply : public MBuffer {
 public:
  long tid;
  long len;
  char *buf;
  off_t offset;
  object_t oid;
  MOSDReadReply(MOSDRead *r, char *buf, long len) :
	MBuffer(MSG_OSD_READREPLY, buf, len) {
	this->tid = r->tid;
	this->oid = r->oid;
	this->offset = r->offset;
  }
};

#endif
