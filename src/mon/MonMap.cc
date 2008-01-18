
#include "MonMap.h"

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

// read from/write to a file
int MonMap::write(const char *fn) 
{
  // encode
  bufferlist bl;
  encode(bl);
  
  // write
  int fd = ::open(fn, O_RDWR|O_CREAT);
  if (fd < 0) return fd;
  ::fchmod(fd, 0644);
  ::write(fd, (void*)bl.c_str(), bl.length());
  ::close(fd);
  return 0;
}

int MonMap::read(const char *fn) 
{
  // read
  bufferlist bl;
  int fd = ::open(fn, O_RDONLY);
  if (fd < 0) return fd;
  struct stat st;
  ::fstat(fd, &st);
  bufferptr bp(st.st_size);
  bl.append(bp);
  ::read(fd, (void*)bl.c_str(), bl.length());
  ::close(fd);
  
  // decode
  decode(bl);
  return 0;
}
