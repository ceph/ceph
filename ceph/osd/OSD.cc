

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

  char *f = get_filename(whoami, r->oid);
  int fd = open(f, O_RDONLY);
  if (fd < 0) {

	// send reply (failure)
	dout(1) << "read open FAILED on " << get_filename(whoami, r->oid) << " errno " << errno << endl;
	reply = new MOSDReadReply(r, NULL, -1);
	assert(0);

  } else {

	// lock
	flock(fd, LOCK_EX);

	if (r->len == 0) { 	              // read whole thing
	  r->len = lseek(fd, 0, SEEK_END);  // get size
	  lseek(fd, 0, SEEK_SET);           // back to beginning
	} else
	  lseek(fd, r->offset, SEEK_SET);   // seek	

	char *buf = new char[r->len];
	
	long got = ::read(fd, buf, r->len);
	flock(fd, LOCK_UN);
	close(fd);
	
	dout(10) << "osd_read " << r->len << " bytes to " << f << endl;

	// send reply
	reply = new MOSDReadReply(r, buf, got);

  }

  // send it
  messenger->send_message(reply, r->get_source(), r->get_source_port());
}


// -- osd_write

void OSD::write(MOSDWrite *m)
{
  MOSDWriteReply *reply;
  
  char *f = get_filename(whoami, m->oid);
  int fd = open(f, O_RDWR|O_CREAT|m->flags);
  if (fd < 0 && errno == 2) {  // create dir and retry
	mkdir(get_dir(whoami), 0755);
	dout(11) << "mkdir errno " << errno << " on " << get_dir(whoami) << endl;
	fd = open(f, O_RDWR|O_CREAT|m->flags);
  }
  if (fd < 0) {
	dout(1) << "err opening " << f << " " << errno << endl;
	
	reply = new MOSDWriteReply(m, -1);
	assert(0);

  } else {
	// lock
	flock(fd, LOCK_EX);

    fchmod(fd, 0664);
	
	dout(10) << "osd_write " << m->len << " bytes to " << f << endl;
	
	if (m->offset)
	  lseek(fd, m->offset, SEEK_SET);
	long wrote = ::write(fd, m->buf, m->len);
	flock(fd, LOCK_UN);
	close(fd);

	// reply
	reply = new MOSDWriteReply(m, wrote);
  }

  // clean up
  messenger->send_message(reply, m->get_source(), m->get_source_port());

  // free buffer
  delete[] m->buf;
  m->buf = 0;
}

