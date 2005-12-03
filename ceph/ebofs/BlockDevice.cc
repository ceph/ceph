
#include "BlockDevice.h"

#include "config.h"

#undef dout
#define dout(x) if (x <= g_conf.debug) cout << "blockdevice."


int BlockDevice::io_thread_entry()
{
  dout(1) << "io_thread start" << endl;
  
  // elevator nonsense!
  bool dir_forward = true;
  block_t pos = 0;
  
  lock.Lock();
  while (!io_stop) {
	// queue?
	if (!io_queue.empty()) {
	  
	  if (dir_forward) {
		// forward sweep
		dout(20) << "io_thread forward sweep" << endl;
		pos = 0;

		while (1) {
		  // find i >= pos
		  multimap<block_t,biovec*>::iterator i = io_queue.lower_bound(pos);
		  if (i == io_queue.end()) break;
		  
		  pos = i->first;
		  biovec *bio = i->second;

		  dout(20) << "io_thread dequeue io at " << pos << endl;
		  io_queue_map.erase(i->second);
		  io_queue.erase(i);
		  
		  lock.Unlock();
		  do_io(bio);
		  lock.Lock();
		}
	  } else {
		// reverse sweep
		dout(20) << "io_thread reverse sweep" << endl;
		pos = get_num_blocks();
		while (1) {
		  // find i > pos
		  multimap<block_t,biovec*>::iterator i = io_queue.upper_bound(pos);
		  if (i == io_queue.begin()) break;
		  i--;  // and back down one (to get i <= pos)

		  pos = i->first;
		  biovec *bio = i->second;

		  dout(20) << "io_thread dequeue io at " << pos << endl;
		  io_queue_map.erase(i->second);
		  io_queue.erase(i);
		  
		  lock.Unlock();
		  do_io(bio);
		  lock.Lock();
		}
	  }
	  dir_forward = !dir_forward;

	} else {
	  // sleep
	  dout(20) << "io_thread sleeping" << endl;
	  io_wakeup.Wait(lock);
	  dout(20) << "io_thread woke up" << endl;
	}
  }
  lock.Unlock();

  dout(1) << "io_thread finish" << endl;
  return 0;
}

void BlockDevice::do_io(biovec *bio)
{
  int r;

  if (bio->type == biovec::IO_WRITE) {
	r = _write(bio->start, bio->length, bio->bl);
  } else if (bio->type == biovec::IO_READ) {
	r = _read(bio->start, bio->length, bio->bl);
  } else assert(0);

  dout(20) << "do_io finish " << (void*)bio << endl;
  if (bio->context) {
	bio->context->finish(r);
	delete bio->context;
	delete bio;
  }
  if (bio->cond) {
	bio->cond->Signal();
	bio->rval = r;
  }
}


// io queue

void BlockDevice::_submit_io(biovec *b) 
{
  // NOTE: lock must be held
  dout(1) << "_submit_io " << (void*)b << endl;
  
  // wake up thread?
  if (io_queue.empty()) io_wakeup.Signal();
  
  // queue
  io_queue.insert(pair<block_t,biovec*>(b->start, b));
  io_queue_map[b] = b->start;
}

int BlockDevice::_cancel_io(biovec *bio) 
{
  // NOTE: lock must be held
  if (io_queue_map.count(bio) == 0) {
	dout(1) << "_cancel_io " << (void*)bio << " FAILED" << endl;
	return -1;
  }
  
  dout(1) << "_cancel_io " << (void*)bio << endl;

  block_t b = io_queue_map[bio];
  multimap<block_t,biovec*>::iterator p = io_queue.lower_bound(b);
  while (p->second != bio) p++;
  assert(p->second == bio);
  io_queue_map.erase(bio);
  io_queue.erase(p);
  return 0;
}



// low level io

int BlockDevice::_read(block_t bno, unsigned num, bufferlist& bl) 
{
  dout(10) << "_read " << bno << "+" << num << endl;

  assert(fd > 0);
  
  off_t offset = bno * EBOFS_BLOCK_SIZE;
  off_t actual = lseek(fd, offset, SEEK_SET);
  assert(actual == offset);
  
  off_t len = num*EBOFS_BLOCK_SIZE;
  assert((int)bl.length() >= len);
  
  for (list<bufferptr>::iterator i = bl.buffers().begin();
	   i != bl.buffers().end();
	   i++) {
	assert(i->length() % EBOFS_BLOCK_SIZE == 0);
	
	int blen = i->length();
	if (blen > len) blen = len;
	
	int got = ::read(fd, i->c_str(), blen);
	assert(got <= blen);
	
	len -= blen;
	if (len == 0) break;
  }
  
  return 0;
}

int BlockDevice::_write(unsigned bno, unsigned num, bufferlist& bl) 
{
  dout(10) << "_write " << bno << "+" << num << endl;

  assert(fd > 0);
  
  off_t offset = bno * EBOFS_BLOCK_SIZE;
  off_t actual = lseek(fd, offset, SEEK_SET);
  assert(actual == offset);
  
  // write buffers
  off_t len = num*EBOFS_BLOCK_SIZE;
  
  for (list<bufferptr>::iterator i = bl.buffers().begin();
	   i != bl.buffers().end();
	   i++) {
	assert(i->length() % EBOFS_BLOCK_SIZE == 0);
	
	off_t left = i->length();
	if (left > len) left = len;
	int r = ::write(fd, i->c_str(), left);
	dout(1) << "write " << fd << " " << (void*)i->c_str() << " " << left << endl;
	if (r < 0) {
	  dout(1) << "couldn't write bno " << bno << " num " << num << " (" << left << " bytes) p=" << (void*)i->c_str() << " r=" << r << " errno " << errno << " " << strerror(errno) << endl;
	} else {
	  assert(r == left);
	}
	
	len -= left;
	if (len == 0) break;
  }
  
  return 0;
}



// open/close

int BlockDevice::open() 
{
  dout(1) << "open " << dev << endl;
  assert(fd == 0);

  fd = ::open(dev, O_CREAT|O_RDWR|O_SYNC|O_DIRECT);
  if (fd < 0) {
	dout(1) << "open failed, r = " << fd << " " << strerror(errno) << endl;
	fd = 0;
	return -1;
  }
  
  // figure size
  __uint64_t bsize = 0;
  ioctl(fd, BLKGETSIZE64, &bsize);
  if (bsize == 0) {
	// hmm, try stat!
	struct stat st;
	fstat(fd, &st);
	bsize = st.st_size;
  }	
  num_blocks = bsize / (__uint64_t)EBOFS_BLOCK_SIZE;
  
  dout(1) << "open " << dev << " is " << bsize << " bytes, " << num_blocks << " blocks" << endl;
  
  // start thread
  io_thread.create();
 
  return fd;
}


int BlockDevice::close() 
{
  assert(fd>0);

  // shut down io thread
  dout(1) << "close stopping io thread" << endl;
  lock.Lock();
  io_stop = true;
  io_wakeup.Signal();
  lock.Unlock();	
  io_thread.join();

  dout(1) << "close closing" << endl;
  return ::close(fd);
}

int BlockDevice::cancel_io(ioh_t ioh) 
{
  biovec *pbio = (biovec*)ioh;
  
  lock.Lock();
  int r = _cancel_io(pbio);
  lock.Unlock();
  
  // FIXME?
  if (r == 0 && pbio->context) {
	pbio->context->finish(-1);
	delete pbio->context;
	delete pbio;
  }
  
  return r;
}

