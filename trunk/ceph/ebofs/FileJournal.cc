// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab

#include "FileJournal.h"
#include "Ebofs.h"

#include "config.h"
#define dout(x) if (x <= g_conf.debug_ebofs) cout << "ebofs(" << dev.get_device_name() << ").journal "
#define derr(x) if (x <= g_conf.debug_ebofs) cerr << "ebofs(" << dev.get_device_name() << ").journal "



void FileJournal::create()
{
  dout(1) << "create " << fn << endl;

  // open/create
  fd = ::open(fn.c_str(), O_CREAT|O_WRONLY);
  assert(fd > 0);

  ::ftruncate(fd);
  ::fchmod(fd, 0644);

  ::close(fd);
}


void FileJournal::open()
{
  dout(1) << "open " << fn << endl;

  // open and file
  assert(fd == 0);
  fd = ::open(fn.c_str(), O_RDWR);
  assert(fd > 0);

  // read header?
  // ***


  start_writer();
}

void FileJournal::close()
{
  dout(1) << "close " << fn << endl;

  // stop writer thread
  stop_writer();

  // close
  assert(q.empty());
  assert(fd > 0);
  ::close(fd);
  fd = 0;
}

void FileJournal::start_writer()
{
  write_stop = false;
  write_thread.create();
}

void FileJournal::stop_writer()
{
  write_lock.Lock();
  {
    write_stop = true;
    write_cond.Signal();
  } 
  write_lock.Unlock();
  write_thread.join();
}


void FileJournal::write_header()
{
  dout(10) << "write_header" << endl;
  
  ::lseek(fd, 0, SEEK_SET);
  ::write(fd, &header, sizeof(header));
}


void FileJournal::write_thread_entry()
{
  dout(10) << "write_thread_entry start" << endl;
  write_lock.Lock();
  
  while (!write_stop) {
    if (writeq.empty()) {
      // sleep
      dout(20) << "write_thread_entry going to sleep" << endl;
      write_cond.Wait(write_lock);
      dout(20) << "write_thread_entry woke up" << endl;
      continue;
    }
    
    // do queued writes
    while (!writeq.empty()) {
      // grab next item
      epoch_t e = writeq.front().first;
      bufferlist bl;
      bl.claim(writeq.front().second);
      writeq.pop_front();
      Context *oncommit = commitq.front();
      commitq.pop_front();
      
      dout(15) << "write_thread_entry writing " << bottom << " : " 
	       << bl.length() 
	       << " epoch " << e
	       << endl;
      
      // write epoch, len, data.
      ::fseek(fd, bottom, SEEK_SET);
      ::write(fd, &e, sizeof(e));
      
      uint32_t len = bl.length();
      ::write(fd, &len, sizeof(len));
      
      for (list<bufferptr>::const_iterator it = bl.buffers().begin();
	   it != bl.buffers().end();
	   it++) {
	if ((*it).length() == 0) continue;  // blank buffer.
	::write(fd, (char*)(*it).c_str(), (*it).length() );
      }
      
      // move position pointer
      bottom += sizeof(epoch_t) + sizeof(uint32_t) + e.length();
      
      // do commit callback
      if (oncommit) {
	oncommit->finish(0);
	delete oncommit;
      }
    }
  }
  
  write_lock.Unlock();
  dout(10) << "write_thread_entry finish" << endl;
}

void FileJournal::submit_entry(bufferlist& e, Context *oncommit)
{
  dout(10) << "submit_entry " << bottom << " : " << e.length()
	   << " epoch " << ebofs->super_epoch
	   << " " << oncommit << endl;
  
  // dump on queue
  writeq.push_back(pair<epoch_t,bufferlist>(ebofs->super_epoch, e));
  commitq.push_back(oncommit);

  // kick writer thread
  write_cond.Signal();
}


void FileJournal::commit_epoch_start()
{
  dout(10) << "commit_epoch_start" << endl;

  write_lock.Lock();
  {
    header.epoch2 = ebofs->super_epoch;
    header.top2 = bottom;
    write_header();
  }
  write_lock.Unlock();
}

void FileJournal::commit_epoch_finish()
{
  dout(10) << "commit_epoch_finish" << endl;

  write_lock.Lock();
  {
    // update header
    header.epoch1 = ebofs->super_epoch;
    header.top1 = header.top2;
    header.epoch2 = 0;
    header.top2 = 0;
    write_header();

    // flush any unwritten items in previous epoch
    while (!writeq.empty() &&
	   writeq.front().first < ebofs->super_epoch) {
      dout(15) << " dropping uncommitted journal item from prior epoch" << endl;
      writeq.pop_front();
      Context *oncommit = commitq.front();
      commitq.pop_front();
	  
      if (oncommit) {
	oncommit->finish(0);
	delete oncommit;
      }
    }
  }
  write_lock.Unlock();
  
}
