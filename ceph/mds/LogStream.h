#ifndef __LOGSTREAM_H
#define __LOGSTREAM_H

#include "include/types.h"
#include "include/Context.h"

#include "include/buffer.h"
#include "include/bufferlist.h"

#include <map>
#include <list>

class LogEvent;
class Filer;
class MDS;

class LogStream {
 protected:
  MDS *mds;
  Filer *filer;

  inodeno_t log_ino;

  // writing
  off_t sync_pos;        // first non-written byte
  off_t flush_pos;       // first non-writing byte, beginning of write_buf
  off_t append_pos;      // where next event will be written
  bufferlist write_buf;  // unwritten (between flush_pos and append_pos)

  // reading
  off_t read_pos;        // abs position in file
  //off_t read_buf_start;  // where read buf begins
  bufferlist read_buf;
  bool reading;

  std::map< off_t, std::list<Context*> > waiting_for_sync;
  std::list<Context*>                    waiting_for_read;


  bool autoflush;
  
 public:
  LogStream(MDS *mds, Filer *filer, inodeno_t log_ino) {
	this->mds = mds;
	this->filer = filer;
	this->log_ino = log_ino;

	// wr
	sync_pos = flush_pos = append_pos = 0;
	autoflush = true;

	// rd
	read_pos = 0;
	reading = false;
  }

  off_t get_read_pos() { return read_pos; }
  off_t get_append_pos() { return append_pos; }

  // write (append to end)
  off_t append(LogEvent *e);          // returns offset it will be written to
  void  _append_2(off_t off);     
  void  wait_for_sync(Context *c, off_t off=0);  // wait for flush
  void  flush();                                 // initiate flush
  
  // read (from front)
  //bool      has_next_event();
  LogEvent *get_next_event();
  void      wait_for_next_event(Context *c);
  void      _did_read(bufferlist& blist);


  // old interface
  /*
  // WARNING: non-reentrant; single reader only
  int read_next(LogEvent **le, Context *c, int step=1);
  void did_read_bit(crope& next_bit, LogEvent **le, Context *c) ;

  int append(LogEvent *e, Context *c);  // append at cur_pos, mind you!
  */
};

#endif
