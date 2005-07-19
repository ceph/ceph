
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
#include <sys/xattr.h>

#include "config.h"
#undef dout
#define  dout(l)    if (l<=g_conf.debug) cout << "osd" << whoami << ".fakestore "


// crap-a-crap hash
#define HASH_DIRS       128LL
#define HASH_FUNC(x)    (((x) ^ ((x)>>30) ^ ((x)>>18) ^ ((x)>>45) ^ 0xdead1234) * 884811 % HASH_DIRS)
// end crap hash



FakeStore::FakeStore(char *base, int whoami) 
{
  this->basedir = base;
  this->whoami = whoami;
}


int FakeStore::init() 
{
  string mydir;
  get_dir(mydir);

  dout(5) << "init with basedir " << mydir << endl;

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
  return 0;
}




////

void FakeStore::get_dir(string& dir) {
  char s[30];
  sprintf(s, "%d", whoami);
  dir = basedir + "/" + s;
}
void FakeStore::get_oname(object_t oid, string& fn) {
  char s[100];
  sprintf(s, "%d/%02llx/%016llx", whoami, HASH_FUNC(oid), oid);
  fn = basedir + "/" + s;
  //  dout(1) << "oname is " << fn << endl;
}


void FakeStore::wipe_dir(string mydir)
{
  DIR *dir = ::opendir(mydir.c_str());
  if (dir) {
	dout(10) << "wiping " << mydir << endl;
	struct dirent *ent = 0;
	
	while ((ent = ::readdir(dir)) != 0) {
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
  return r;
}
 
 

int FakeStore::remove(object_t oid) 
{
  dout(20) << "remove " << oid << endl;
  string fn;
  get_oname(oid,fn);
  int r = ::unlink(fn.c_str());
  return r;
}

int FakeStore::truncate(object_t oid, off_t size)
{
  dout(20) << "truncate " << oid << " size " << size << endl;

  string fn;
  get_oname(oid,fn);
  return ::truncate(fn.c_str(), size);
}

int FakeStore::read(object_t oid, 
					size_t len, off_t offset,
					char *buffer) {
  dout(20) << "read " << oid << " len " << len << " off " << offset << endl;

  string fn;
  get_oname(oid,fn);
  
  int fd = ::open(fn.c_str(), O_RDONLY);
  if (fd < 0) {
	dout(1) << "read couldn't open " << fn.c_str() << " errno " << errno << " " << strerror(errno) << endl;
	return fd;
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







// ------------------
// collections

void FakeStore::get_collfn(coll_t c, string &fn) {
  char s[100];
  sprintf(s, "collection.%02llx", c);
  fn = basedir;
  fn += "/";
  fn += s;
}
void FakeStore::open_collection(coll_t c) {
  if (collection_map.count(c) == 0) {
	string fn;
	get_collfn(c,fn);
	collection_map[c] = new BDBMap<coll_t,int>;
	collection_map[c]->open(fn.c_str());
  }
}
int FakeStore::collection_create(coll_t c) {
  collections.put(c, 1);
  open_collection(c);
  return 0;
}
int FakeStore::collection_destroy(coll_t c) {
  collections.del(c);
  
  open_collection(c);
  collection_map[c]->close();
  
  string fn;
  get_collfn(c,fn);
  collection_map[c]->remove(fn.c_str());
  delete collection_map[c];
  collection_map.erase(c);
  return 0;
}
int FakeStore::collection_add(coll_t c, object_t o) {
  open_collection(c);
  collection_map[c]->put(o,1);
  return 0;
}
int FakeStore::collection_remove(coll_t c, object_t o) {
  open_collection(c);
  collection_map[c]->del(o);
  return 0;
}
int FakeStore::collection_list(coll_t c, list<object_t>& o) {
  open_collection(c);
  collection_map[c]->list_keys(o);
  return 0;
}



// ------------------
// attributes

int FakeStore::setattr(object_t oid, const char *name,
					   void *value, size_t size)
{
  string fn;
  get_oname(oid, fn);
  return setxattr(fn.c_str(), name, value, size, 0);
}


int FakeStore::getattr(object_t oid, const char *name,
					   void *value, size_t size)
{
  string fn;
  get_oname(oid, fn);
  return getxattr(fn.c_str(), name, value, size);
}

int FakeStore::listattr(object_t oid, char *attrs, size_t size)
{
  string fn;
  get_oname(oid, fn);
  return listxattr(fn.c_str(), attrs, size);
}

