

#include "include/mds.h"
#include "include/osd.h"
#include "include/Context.h"

#include <unistd.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <iostream>

char *osd_base_path = "/home/weil/tmp/osd";

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



int osd_read(int osd, object_t oid, size_t len, size_t offset, void *buf, Context *c)
{
  // fake it.
  int fd = open(get_filename(osd,oid), O_RDONLY);
  if (fd < 0) {
	c->finish(-ENOENT);
	return 0;
  }
  
  // read part of the object
  lseek(fd, offset, SEEK_SET);
  read(fd, buf, len);
  close(fd);

  c->finish(0);
  delete c;
}



int osd_read_all(int osd, object_t oid, void **bufptr, size_t *buflen, Context *c)
{
  // fake it.
  int fd = open(get_filename(osd,oid), O_RDONLY);
  if (fd < 0) {
	c->finish(-ENOENT);
	return 0;
  }
  
  // read entire object
  *buflen = lseek(fd, 0, SEEK_END);
  *bufptr = new char[*buflen];
  
  lseek(fd, 0, SEEK_SET);
  read(fd, *bufptr, *buflen);
  close(fd);

  c->finish(0);
  delete c;
}



// -- osd_write

int osd_write(int osd, object_t oid, size_t len, size_t offset, void *buf, Context *c)
{
  // fake it
  char *f = get_filename(osd,oid);
  int fd = open(f, O_WRONLY|O_CREAT);
  if (fd < 0 && errno == 2) {  // create dir and retry
	mkdir(get_dir(osd), 0755);
	cout << "mkdir errno " << errno << " on " << get_dir(osd) << endl;
	fd = open(f, O_WRONLY|O_CREAT);
  }
  if (fd < 0) {
	cout << "err opening " << f << " " << errno << endl;
	c->finish(-ENOENT);
	return 0;
  }
  
  fchmod(fd, 0664);
  
  cout << "osd_write " << len << " bytes to " << f << endl;

  if (offset)
	lseek(fd, offset, SEEK_SET);
  write(fd, (char*)buf+offset, len);
  close(fd);
 

  c->finish(0);
  delete c;
}

