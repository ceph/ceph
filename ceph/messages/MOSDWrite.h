#ifndef __MOSDWRITE_H
#define __MOSDWRITE_H

#include "MBuffer.h"

class MOSDWrite : public MBuffer {
 public:
  long tid;
  off_t offset;
  object_t oid;
  int flags;
  MOSDWrite(long tid, object_t oid, size_t len, off_t offset, char *buf, int flags=0) :
	MBuffer(MSG_OSD_WRITE, buf, len) {
	this->tid = tid;
	this->oid = oid;
	this->offset = offset;
	this->flags = flags;
	//cout << "message(t) serialized " << serialized << endl;
  }
  virtual char *get_type_name() { return "owr"; }
};

#endif
