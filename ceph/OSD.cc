

#include "include/mds.h"
#include "include/osd.h"
#include "include/Context.h"

#include <unistd.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

char *osd_base_path = "~/osd";

// -- osd_read

int osd_read(int osd, object_t oid, size_t offset, size_t len, void *buf, Context *c)
{
  c->finish(0);
}


int osd_read(int osd, object_t oid, size_t offset, size_t len, void **bufptr, size_t *buflen, Context *c)
{
  // fake it.
  char fn[100];
  sprintf(fn, "%s/%d/%ll", osd_base_path, osd, oid);

  int fd = open(fn, O_RDONLY);
  if (fd < 0) {
	c->finish(-ENOENT);
	return 0;
  }
  
  *buflen = lseek(fd, 0, SEEK_END);
  *bufptr = new char[*buflen];

  lseek(fd, 0, SEEK_SET);
  read(fd, *bufptr, *buflen);

  close(fd);

  c->finish(0);
}



