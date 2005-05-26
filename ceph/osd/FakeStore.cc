
#include "FakeStore.h"
#include "include/types.h"

#include <unistd.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <sys/file.h>
#include <iostream>
#include <cassert>
#include <errno.h>

int FakeStore::stat(object_t oid,
					struct stat *st)
{
  string fn;
  make_oname(oid,fn);
  return ::stat(fn.c_str(), st);
}

int FakeStore::unlink(object_t oid) 
{
  string fn;
  make_oname(oid,fn);
  return ::unlink(fn.c_str());
}

int FakeStore::read(object_t oid, 
					size_t len, off_t offset,
					char *buffer) {
  string fn;
  make_oname(oid,fn);
  
  int fd = open(fn.c_str(), O_RDONLY);
  if (fd < 0) return fd;
  flock(fd, LOCK_EX);    // lock for safety
  
  off_t actual = lseek(fd, offset, SEEK_SET);
  size_t got = 0;
  if (actual == offset) {
	got = ::read(fd, buffer, len);
  }
  flock(fd, LOCK_UN);
  close(fd);
  return got;
}

int FakeStore::write(object_t oid,
					 size_t len, off_t offset,
					 char *buffer) {
  string fn;
  make_oname(oid,fn);
  
  int fd = open(fn.c_str(), O_WRONLY|O_CREAT);
  if (fd < 0) return fd;
  flock(fd, LOCK_EX);    // lock for safety
  fchmod(fd, 0664);
  
  off_t actual = lseek(fd, offset, SEEK_SET);
  size_t did = 0;
  if (actual == offset) {
	did = ::write(fd, buffer, len);
  }
  flock(fd, LOCK_UN);
  close(fd);
  
  return did;
}

