

#include "include/types.h"
#include "include/Messenger.h"
#include "include/Message.h"

#include "OSD.h"

#include "messages/MOSDRead.h"
#include "messages/MOSDReadReply.h"
#include "messages/MOSDWrite.h"
#include "messages/MOSDWriteReply.h"

#include <unistd.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <sys/file.h>
#include <iostream>
#include <cassert>

#include "include/config.h"
#define  dout(l)    if (l<=DEBUG_LEVEL) cout << "osd" << whoami << " "
#define  dout2(l)   if (1<=DEBUG_LEVEL) cout

char *osd_base_path = "./osddata";

// cons/des

OSD::OSD(int id, Messenger *m) 
{
  whoami = id;
  messenger = m;
}

OSD::~OSD()
{
  if (messenger) { delete messenger; messenger = 0; }
}

int OSD::init()
{
  messenger->set_dispatcher(this);
  return 0;
}

int OSD::shutdown()
{
  messenger->shutdown();
  return 0;
}



// dispatch

void OSD::dispatch(Message *m) 
{
  switch (m->get_type()) {
  case MSG_OSD_READ:
	read((MOSDRead*)m);
	break;

  case MSG_OSD_WRITE:
	write((MOSDWrite*)m);
	break;

  default:
	dout(1) << " got unknown message " << m->get_type() << endl;
  }

  delete m;
}


// -- osd_read



char fn[100];
char *get_filename(int osd, object_t oid) 
{
  sprintf(fn, "%s/%d/%d", osd_base_path, osd, oid);
  return fn;
}

char dir[100];
char *get_dir(int osd)
{
  sprintf(dir, "%s/%d", osd_base_path, osd);
  return dir;
}



void OSD::read(MOSDRead *r)
{
  MOSDReadReply *reply;

  char *f = get_filename(whoami, r->get_oid());
  int fd = open(f, O_RDONLY);
  if (fd < 0) {

	// send reply (failure)
	dout(1) << "read open FAILED on " << get_filename(whoami, r->get_oid()) << " errno " << errno << endl;
	reply = new MOSDReadReply(r, NULL, -1);
	assert(0);

  } else {

	// lock
	flock(fd, LOCK_EX);

	long len = r->get_len();
	if (len == 0) { 	              // read whole thing
	  len = lseek(fd, 0, SEEK_END);  // get size
	  lseek(fd, 0, SEEK_SET);           // back to beginning
	} else
	  lseek(fd, r->get_offset(), SEEK_SET);   // seek	

	// read into a buffer
	char *buf = new char[len];
	long got = ::read(fd, buf, len);

	dout(10) << "osd_read " << got << " / " << len << " bytes to " << f << endl;

	// close
	flock(fd, LOCK_UN);
	close(fd);
	
	// send reply
	reply = new MOSDReadReply(r, buf, got);

	// free buffer
	delete buf;
  }

  // send it
  messenger->send_message(reply, r->get_source(), r->get_source_port());
}


// -- osd_write

void OSD::write(MOSDWrite *m)
{
  MOSDWriteReply *reply;
  
  char *f = get_filename(whoami, m->get_oid());
  int fd = open(f, O_RDWR|O_CREAT|m->get_flags());
  if (fd < 0 && errno == 2) {  // create dir and retry
	mkdir(get_dir(whoami), 0755);
	dout(11) << "mkdir errno " << errno << " on " << get_dir(whoami) << endl;
	fd = open(f, O_RDWR|O_CREAT|m->get_flags());
  }
  if (fd < 0) {
	dout(1) << "err opening " << f << " " << errno << endl;
	
	reply = new MOSDWriteReply(m, -1);
	assert(0);

  } else {
	// lock
	flock(fd, LOCK_EX);

    fchmod(fd, 0664);
	
	dout(10) << "osd_write " << m->get_len() << " bytes to " << f << endl;
	
	if (m->get_offset())
	  lseek(fd, m->get_offset(), SEEK_SET);
	long wrote = ::write(fd, m->get_buf(), m->get_len());
	flock(fd, LOCK_UN);
	close(fd);

	// reply
	reply = new MOSDWriteReply(m, wrote);
  }

  // clean up
  messenger->send_message(reply, m->get_source(), m->get_source_port());
}

