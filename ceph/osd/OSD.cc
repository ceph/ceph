

#include "include/types.h"

#include "OSD.h"

#include "msg/Messenger.h"
#include "msg/Message.h"

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
#undef dout
#define  dout(l)    if (l<=g_conf.debug) cout << "osd" << whoami << " "

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
char fn2[100];
char *get_filename_nopid(int osd, object_t oid) 
{
  sprintf(fn, "%s/%d/%d", osd_base_path, osd, oid);
  return fn;
}
char *get_filename(int osd, object_t oid) 
{
  if (!g_conf.osd_cow)
	return get_filename_nopid(osd,oid);
  
  sprintf(fn, "%s/%d/%d.%d", osd_base_path, osd, oid, getpid());
  return fn;
}
char *get_filename2(int osd, object_t oid) 
{
  sprintf(fn2, "%s/%d/%d.tmp", osd_base_path, osd, oid);
  return fn2;
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

	// try with no pid, in case we're cow
	char *f = get_filename_nopid(whoami, r->get_oid());
	int fd = open(f, O_RDONLY);
	
	if (fd < 0) {
	  // send reply (failure)
	  dout(1) << "read open FAILED on " << get_filename(whoami, r->get_oid()) << " errno " << errno << endl;
	  reply = new MOSDReadReply(r, NULL, -1);
	  assert(0);
	}
  }
  
  if (fd >= 0) {
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

	dout(10) << "osd_read " << got << " / " << len << " bytes from " << f << endl;

	// close
	flock(fd, LOCK_UN);
	close(fd);
	
	// send reply
	reply = new MOSDReadReply(r, buf, got);

	// free buffer
	delete[] buf;
  }

  // send it
  messenger->send_message(reply, r->get_source(), r->get_source_port());
}


// -- osd_write

void OSD::write(MOSDWrite *m)
{
  MOSDWriteReply *reply;
  
  char *f;
  if (m->get_offset() == 0)
	f = get_filename2(whoami, m->get_oid());   // HACK HACK
  else 
	f = get_filename(whoami, m->get_oid());
  int fd = open(f, O_RDWR|O_CREAT|m->get_flags());
  if (fd < 0 && errno == 2) {  // create dir and retry
	mkdir(get_dir(whoami), 0755);
	dout(11) << "mkdir errno " << errno << " on " << get_dir(whoami) << endl;
	fd = open(f, O_RDWR|O_CREAT|m->get_flags());
  }
  if (fd < 0) {
	dout(1) << "err opening " << f << " " << errno << endl;
	
	reply = new MOSDWriteReply(m, -1);
	assert(2+2==5);

  } else {
	// lock
	flock(fd, LOCK_EX);

    fchmod(fd, 0664);
	
	dout(10) << "osd_write " << m->get_len() << " bytes at offset " << m->get_offset() << " to " << f << endl;
	
	if (m->get_offset())
	  lseek(fd, m->get_offset(), SEEK_SET);
	long wrote = ::write(fd, m->get_buf(), m->get_len());
	flock(fd, LOCK_UN);
	close(fd);

	// reply
	reply = new MOSDWriteReply(m, wrote);
  }

  if (m->get_offset() == 0) {
	char *n = get_filename(whoami, m->get_oid());
	int r = rename(f,n);
	dout(11) << f << " to " << n << " rename sez " << r << endl;	
  }

  // clean up
  messenger->send_message(reply, m->get_source(), m->get_source_port());
}

