#ifndef __LOGSTREAM_H
#define __LOGSTREAM_H

#include "../include/types.h"
#include "../include/Context.h"

class LogEvent;
class MDS;

class LogStream {
 protected:
  MDS *mds;
  off_t cur_pos, append_pos;
  int osd;
  object_t oid;

  char *buf;
  off_t buf_start;
  size_t buf_valid;
 public:
  LogStream(MDS *mds, int osd, object_t oid) {
	this->mds = mds;
	this->osd = osd;
	this->oid = oid;
	cur_pos = 0;
	append_pos = 0; // fixme
	buf = 0;
  }
  ~LogStream() {
	if (buf) { delete[] buf; buf = 0; }
  }

  off_t seek(off_t offset) {
	cur_pos = offset;
  }

  off_t get_pos() {
	return cur_pos;
  }

  // WARNING: non-reentrant; single reader only
  int read_next(LogEvent **le, Context *c, int step=1);

  int append(LogEvent *e, Context *c);  // append at cur_pos, mind you!
};

#endif
