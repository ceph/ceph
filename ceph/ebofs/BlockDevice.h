#ifndef __EBOFS_BLOCKDEVICE_H
#define __EBOFS_BLOCKDEVICE_H

#include "include/bufferlist.h"
#include "include/interval_set.h"
#include "include/Context.h"
#include "common/Mutex.h"
#include "common/Cond.h"
#include "common/Thread.h"

#include "types.h"


typedef void *ioh_t;    // opaque handle to an io request.  (in actuality, a biovec*)


class BlockDevice {
 public:
  // callback type for io completion notification
  class callback {
  public:
	virtual void finish(ioh_t ioh, int rval) = 0;
  };

 private:
  char   *dev;
  int     fd;
  block_t num_blocks;

  Mutex lock;
  
  // io queue
  class biovec {
  public:
	static const char IO_WRITE = 1;
	static const char IO_READ = 2;

	char type;
	block_t start, length;
	bufferlist bl;
	callback *cb;
	Cond *cond;
	int rval;
	char *note;

	biovec(char t, block_t s, block_t l, bufferlist& b, callback *c, char *n=0) :
	  type(t), start(s), length(l), bl(b), cb(c), cond(0), rval(0), note(n) {}
	biovec(char t, block_t s, block_t l, bufferlist& b, Cond *c, char *n=0) :
	  type(t), start(s), length(l), bl(b), cb(0), cond(c), rval(0), note(n) {}
  };
  friend ostream& operator<<(ostream& out, biovec &bio);


  interval_set<block_t>       io_block_lock;    // blocks currently dispatched to kernel
  multimap<block_t, biovec*> io_queue;
  map<biovec*, block_t>      io_queue_map;
  Cond                       io_wakeup;
  bool                       io_stop;
  int                        io_threads_started, io_threads_running;
  
  void _submit_io(biovec *b);
  int _cancel_io(biovec *bio);
  void do_io(int fd, list<biovec*>& biols);

  // elevator scheduler
  bool    el_dir_forward;
  block_t el_pos;
  utime_t el_stop;


  // io thread
  int io_thread_entry();
  class IOThread : public Thread {
	BlockDevice *dev;
  public:
	IOThread(BlockDevice *d) : dev(d) {}
	void *entry() { return (void*)dev->io_thread_entry(); }
  } ;
  vector<IOThread*> io_threads;

  // low level io
  int _read(int fd, block_t bno, unsigned num, bufferlist& bl);
  int _write(int fd, unsigned bno, unsigned num, bufferlist& bl);


  // complete queue
  Mutex          complete_lock;
  Cond           complete_wakeup;
  list<biovec*>  complete_queue;
  
  void finish_io(biovec *bio);

  // complete thread
  int complete_thread_entry();
  class CompleteThread : public Thread {
	BlockDevice *dev;
  public:
	CompleteThread(BlockDevice *d) : dev(d) {}
	void *entry() { return (void*)dev->complete_thread_entry(); }
  } complete_thread;


  int open_fd();  // get an fd

 public:
  BlockDevice(char *d) : 
	dev(d), fd(0), num_blocks(0),
	io_stop(false), io_threads_started(0), io_threads_running(0),
	el_dir_forward(true), el_pos(0),
	complete_thread(this) 
	{ };
  ~BlockDevice() {
	if (fd > 0) close();
  }

  // get size in blocks
  block_t get_num_blocks();

  char *get_device_name() const { return dev; }

  int open();
  int close();

  // 
  int count_io(block_t start, block_t len);


  // ** blocking interface **

  // read
  int read(block_t bno, unsigned num, bufferptr& bptr, char *n=0) {
	bufferlist bl;
	bl.push_back(bptr);
	return read(bno, num, bl, n);
  }
  int read(block_t bno, unsigned num, bufferlist& bl, char *n=0) {
	Cond c;
	biovec bio(biovec::IO_READ, bno, num, bl, &c, n);
	
	lock.Lock();
	_submit_io(&bio);
	c.Wait(lock);
	lock.Unlock();
	return bio.rval;
  }

  // write
  int write(unsigned bno, unsigned num, bufferptr& bptr, char *n=0) {
	bufferlist bl;
	bl.push_back(bptr);
	return write(bno, num, bl, n);
  }
  int write(unsigned bno, unsigned num, bufferlist& bl, char *n=0) {
	Cond c;
	biovec bio(biovec::IO_WRITE, bno, num, bl, &c, n);

	lock.Lock();
	_submit_io(&bio);
	c.Wait(lock);
	lock.Unlock();
	return bio.rval;
  }

  // ** non-blocking interface **
  ioh_t read(block_t bno, unsigned num, bufferlist& bl, callback *fin, char *n=0) {
	biovec *pbio = new biovec(biovec::IO_READ, bno, num, bl, fin, n);
	lock.Lock();
	_submit_io(pbio);
	lock.Unlock();
	return (ioh_t)pbio;
  }
  ioh_t write(block_t bno, unsigned num, bufferlist& bl, callback *fin, char *n=0) {
	biovec *pbio = new biovec(biovec::IO_WRITE, bno, num, bl, fin, n);
	lock.Lock();
	_submit_io(pbio);
	lock.Unlock();
	return (ioh_t)pbio;
  }
  int cancel_io(ioh_t ioh);



};




#endif
