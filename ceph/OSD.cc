

#include "include/types.h"
#include "include/OSD.h"
#include "include/Messenger.h"
#include "include/Message.h"

#include "messages/MOSDRead.h"
#include "messages/MOSDReadReply.h"
#include "messages/MOSDWrite.h"
#include "messages/MOSDWriteReply.h"

#include <unistd.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <iostream>

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
	cout << "osd " << whoami << " got unknown message " << m->get_type() << endl;
  }
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

  int fd = open(get_filename(whoami, r->oid), O_RDONLY);
  if (fd < 0) {

	// send reply (failure)
	reply = new MOSDReadReply(r, NULL, -1);

  } else {

	if (r->len == 0) { 	              // read whole thing
	  r->len = lseek(fd, 0, SEEK_END);  // get size
	  lseek(fd, 0, SEEK_SET);           // back to beginning
	} else
	  lseek(fd, r->offset, SEEK_SET);   // seek	

	char *buf = new char[r->len];
	
	long got = ::read(fd, buf, r->len);
	close(fd);
	
	// send reply
	reply = new MOSDReadReply(r, buf, got);

  }

  // send it
  messenger->send_message(reply, r->get_source(), r->get_source_port());
  delete r;
}


// -- osd_write

void OSD::write(MOSDWrite *m)
{
  MOSDWriteReply *reply;
  
  char *f = get_filename(whoami, m->oid);
  int fd = open(f, O_RDWR|O_CREAT|m->flags);
  if (fd < 0 && errno == 2) {  // create dir and retry
	mkdir(get_dir(whoami), 0755);
	cout << "mkdir errno " << errno << " on " << get_dir(whoami) << endl;
	fd = open(f, O_RDWR|O_CREAT|m->flags);
  }
  if (fd < 0) {
	cout << "err opening " << f << " " << errno << endl;
	
	reply = new MOSDWriteReply(m, -1);

  } else {
    fchmod(fd, 0664);
	
	cout << "osd_write " << m->len << " bytes to " << f << endl;
	
	if (m->offset)
	  lseek(fd, m->offset, SEEK_SET);
	long wrote = ::write(fd, m->buf, m->len);
	close(fd);

	// reply
	reply = new MOSDWriteReply(m, wrote);
  }

  // clean up
  cout << "sending reply" << endl;
  messenger->send_message(reply, m->get_source(), m->get_source_port());

  delete m->buf;
  delete m;
}

