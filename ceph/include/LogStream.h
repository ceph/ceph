#ifndef __LOGSTREAM_H
#define __LOGSTREAM_H

#include <sys/types.h>
#include "osd.h"
#include "Context.h"

class LogEvent;

class LogStream {
 protected:
  off_t cur_pos, append_pos;
  int osd;
  object_t oid;

  char *buf;
  size_t buflen;
  off_t buf_start;

 public:
  LogStream(int osd, object_t oid) {
	this->osd = osd;
	this->oid = oid;
	cur_pos = 0;
	append_pos = 0; // fixme
	buf = 0;
  }
  ~LogStream() {
	if (buf) { delete buf; buf = 0; }
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
