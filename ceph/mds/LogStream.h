#ifndef __LOGSTREAM_H
#define __LOGSTREAM_H

#include "include/types.h"
#include "../include/Context.h"
#include <ext/rope>
using namespace std;

class LogEvent;
class MDS;

class LogStream {
 protected:
  MDS *mds;
  off_t cur_pos, append_pos;
  int osd;
  object_t oid;

  bool reading_block;
  //list<Context*> waiting_for_read_block;

  crope buffer;
  off_t buf_start;
 public:
  LogStream(MDS *mds, int osd, object_t oid) {
	this->mds = mds;
	this->osd = osd;
	this->oid = oid;
	cur_pos = 0;
	append_pos = 0; // fixme
	buf_start = 0;
	reading_block = false;
  }

  off_t seek(off_t offset) {
	cur_pos = offset;
  }

  off_t get_pos() {
	return cur_pos;
  }

  // WARNING: non-reentrant; single reader only
  int read_next(LogEvent **le, Context *c, int step=1);
  void did_read_bit(crope& next_bit, LogEvent **le, Context *c) ;

  int append(LogEvent *e, Context *c);  // append at cur_pos, mind you!
};

#endif
