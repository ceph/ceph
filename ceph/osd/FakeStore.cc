
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
#include <dirent.h>


#include "include/config.h"
#undef dout
#define  dout(l)    if (l<=g_conf.debug) cout << "osd" << whoami << ".fakestore "


// crap-a-crap hash
#define HASH_DIRS       128LL
#define HASH_FUNC(x)    (((x) ^ ((x)>>30) ^ ((x)>>18) ^ ((x)>>45) ^ 0xdead1234) * 884811 % HASH_DIRS)
// end crap hash



FakeStore::FakeStore(char *base, int whoami, char *shadow) 
{
  this->basedir = base;
  this->whoami = whoami;

  if (shadow) {
	is_shadow = true;
	shadowdir = shadow;
  } else
	is_shadow = false;
}


int FakeStore::init() 
{
  string mydir;
  get_dir(mydir);

  dout(5) << "init with basedir " << mydir << endl;
  if (is_shadow) {
	dout(5) << " SHADOW dir is " << shadowdir << endl;
  }

  // make sure global base dir exists
  struct stat st;
  int r = ::stat(basedir.c_str(), &st);
  if (r != 0) {
	dout(1) << "unable to stat basedir " << basedir << ", r = " << r << endl;
	return r;
  }

  // all okay.
  return 0;
}

int FakeStore::finalize() 
{
  dout(5) << "finalize" << endl;
  // nothing
}




////

void FakeStore::get_dir(string& dir) {
  char s[30];
  sprintf(s, "%d", whoami);
  dir = basedir + "/" + s;
}
void FakeStore::get_oname(object_t oid, string& fn, bool shadow) {
  char s[100];
  sprintf(s, "%d/%02llx/%016llx", whoami, HASH_FUNC(oid), oid);
  if (shadow)
	fn = shadowdir + "/" + s;
  else 
	fn = basedir + "/" + s;
  //  dout(1) << "oname is " << fn << endl;
}


void FakeStore::wipe_dir(string mydir)
{
  DIR *dir = ::opendir(mydir.c_str());
  if (dir) {
	dout(10) << "wiping " << mydir << endl;
	struct dirent *ent = 0;
	
	while (ent = ::readdir(dir)) {
	  if (ent->d_name[0] == '.') continue;
	  dout(25) << "mkfs unlinking " << ent->d_name << endl;
	  string fn = mydir + "/" + ent->d_name;
	  ::unlink(fn.c_str());
	}	
	
	::closedir(dir);
  } else {
	dout(1) << "mkfs couldn't read dir " << mydir << endl;
  }
}

int FakeStore::mkfs()
{
  int r = 0;
  struct stat st;
  string mydir;
  get_dir(mydir);

  dout(1) << "mkfs in " << mydir << endl;

  if (is_shadow) {
	dout(1) << "WARNING mkfs reverting to shadow fs, which pbly isn't what MDS expects!" << endl;
  }

  // make sure my dir exists
  r = ::stat(mydir.c_str(), &st);
  if (r != 0) {
	dout(10) << "creating " << mydir << endl;
	mkdir(mydir.c_str(), 0755);
	r = ::stat(mydir.c_str(), &st);
	if (r != 0) {
	  dout(1) << "couldnt create dir, r = " << r << endl;
	  return r;
	}
  }
  else wipe_dir(mydir);

  // hashed bits too
  for (int i=0; i<HASH_DIRS; i++) {
	char s[4];
	sprintf(s, "%02x", i);
	string subdir = mydir + "/" + s;
	r = ::stat(subdir.c_str(), &st);
	if (r != 0) {
	  dout(2) << " creating " << subdir << endl;
	  ::mkdir(subdir.c_str(), 0755);
	  r = ::stat(subdir.c_str(), &st);
	  if (r != 0) {
		dout(1) << "couldnt create subdir, r = " << r << endl;
		return r;
	  }
	}
	else
	  wipe_dir( subdir );
  }

  return r;
}



bool FakeStore::exists(object_t oid)
{
  struct stat st;
  if (stat(oid, &st) == 0)
	return true;
  else 
	return false;
}

  
int FakeStore::stat(object_t oid,
					struct stat *st)
{
  dout(20) << "stat " << oid << endl;
  string fn;
  get_oname(oid,fn);
  int r = ::stat(fn.c_str(), st);
  
  if (is_shadow && 
	  r != 0 &&                          // primary didn't exist
	  ::lstat(fn.c_str(), st) != 0)  {   // and wasn't an intentionally bad symlink
	get_oname(oid,fn,true);
	return ::stat(fn.c_str(), st);
  } else
	return r;
}
 
 
void FakeStore::shadow_copy_maybe(object_t oid) {
  struct stat st;
  string fn;
  get_oname(oid, fn);
  if (::lstat(fn.c_str(), &st) == 0) 
	return;  // live copy exists, we're fine, do nothing.

  // is there a shadow object?
  string sfn;
  get_oname(oid, sfn, true);
  if (::stat(sfn.c_str(), &st) == 0) {
	// shadow exists.  copy!
	dout(10) << "copying shadow for " << oid << " " << st.st_size << " bytes" << endl;
	char *buf = new char[1024*1024];
	int left = st.st_size;

	int sfd = ::open(sfn.c_str(), O_RDONLY);
	int fd = ::open(fn.c_str(), O_WRONLY);
	assert(sfd && fd);
	while (left) {
	  int howmuch = left;
	  if (howmuch > 1024*1024) howmuch = 1024*1024;
	  int got = ::read(sfd, buf, howmuch);
	  int wrote = ::write(fd, buf, got);
	  assert(wrote == got);
	  left -= got;
	}
	::close(fd);
	::close(sfd);
  }
}


int FakeStore::remove(object_t oid) 
{
  dout(20) << "remove " << oid << endl;
  string fn;
  get_oname(oid,fn);
  int r = ::unlink(fn.c_str());

  if (r == 0 && is_shadow) {
	string sfn;
	struct stat st;
	get_oname(oid, sfn, true);
	int s = ::stat(sfn.c_str(), &st);
	if (s == 0) {
	  // shadow exists.  make a bad symlink to mask it.
	  ::symlink(sfn.c_str(), "doesnotexist");
	  r = 0;
	}
  }
  return r;
}

int FakeStore::truncate(object_t oid, off_t size)
{
  dout(20) << "truncate " << oid << " size " << size << endl;

  if (is_shadow) shadow_copy_maybe(oid);
  
  string fn;
  get_oname(oid,fn);
  ::truncate(fn.c_str(), size);
}

int FakeStore::read(object_t oid, 
					size_t len, off_t offset,
					char *buffer) {
  dout(20) << "read " << oid << " len " << len << " off " << offset << endl;

  string fn;
  get_oname(oid,fn);
  
  int fd = ::open(fn.c_str(), O_RDONLY);
  if (fd < 0) {
	if (is_shadow) {
	  struct stat st;
	  if (::lstat(fn.c_str(), &st) == 0) return fd;  // neg symlink
	  get_oname(oid,fn);
	  fd = ::open(fn.c_str(), O_RDONLY);
	  if (fd < 0) 
		return fd;    // no shadow either.
	} else {
	  dout(1) << "read couldn't open " << fn.c_str() << " errno " << errno << " " << strerror(errno) << endl;
	  return fd;
	}
  }
  ::flock(fd, LOCK_EX);    // lock for safety
  
  off_t actual = lseek(fd, offset, SEEK_SET);
  size_t got = 0;
  if (actual == offset) {
	got = ::read(fd, buffer, len);
  }
  ::flock(fd, LOCK_UN);
  ::close(fd);
  return got;
}

int FakeStore::write(object_t oid,
					 size_t len, off_t offset,
					 char *buffer,
					 bool do_fsync) {
  dout(20) << "write " << oid << " len " << len << " off " << offset << endl;

  if (is_shadow) shadow_copy_maybe(oid);

  string fn;
  get_oname(oid,fn);
  
  ::mknod(fn.c_str(), 0644, 0);  // in case it doesn't exist yet.

  int flags = O_WRONLY;//|O_CREAT;
  if (do_fsync && g_conf.osd_writesync) flags |= O_SYNC;
  int fd = ::open(fn.c_str(), flags);
  if (fd < 0) {
	dout(1) << "write couldn't open " << fn.c_str() << " flags " << flags << " errno " << errno << " " << strerror(errno) << endl;
	return fd;
  }
  ::flock(fd, LOCK_EX);    // lock for safety
  //::fchmod(fd, 0664);
  
  off_t actual = lseek(fd, offset, SEEK_SET);
  int did = 0;
  assert(actual == offset);
  did = ::write(fd, buffer, len);
  if (did < 0) {
	dout(1) << "couldn't write to " << fn.c_str() << " len " << len << " off " << offset << " errno " << errno << " " << strerror(errno) << endl;
  }

  // sync to to disk?
  if (do_fsync && g_conf.osd_fsync) ::fsync(fd); // fsync or fdatasync?

  ::flock(fd, LOCK_UN);
  ::close(fd);
  
  return did;
}

