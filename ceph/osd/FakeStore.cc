
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

#include "include/bufferlist.h"

#include <map>
#include <ext/hash_map>
using namespace __gnu_cxx;

// crap-a-crap hash
#define HASH_DIRS       128LL
#define HASH_FUNC(x)    (((x) ^ ((x)>>30) ^ ((x)>>18) ^ ((x)>>45) ^ 0xdead1234) * 884811 % HASH_DIRS)
// end crap hash


map<int, hash_map<object_t, map<const char*, bufferptr> > > fakeattrs;


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

  {
	char name[80];
	sprintf(name,"osd%d.fakestore.threadpool", whoami);
	fsync_threadpool = new ThreadPool<FakeStore, pair<int,Context*> >(name, g_conf.osd_fakestore_syncthreads, 
																	  (void (*)(FakeStore*, pair<int,Context*>*))dofsync, 
																	  this);
  }

  // all okay.
  return 0;
}

int FakeStore::finalize() 
{
  dout(5) << "finalize" << endl;

  // close collections db files
  close_collections();

  delete fsync_threadpool;

  // nothing
  return 0;
}



///////////

void FakeStore::do_fsync(int fd, Context *c)
{
  ::fsync(fd);
  ::close(fd);
  dout(10) << "do_fsync finished on " << fd << " context " << c << endl;
  c->finish(0);
  delete c;
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
void FakeStore::get_collfn(coll_t c, string &fn) {
  char s[100];
  sprintf(s, "%d/%02llx/%016llx.co", whoami, HASH_FUNC(c), c);
  fn = basedir + "/" + s;
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

  close_collections();

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
					bufferlist& bl) {
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
	bufferptr bptr = new buffer(len);  // prealloc space for entire read
	got = ::read(fd, bptr.c_str(), len);
	bptr.set_length(got);   // properly size the buffer
	bl.push_back( bptr );   // put it in the target bufferlist
  }
  ::flock(fd, LOCK_UN);
  ::close(fd);
  return got;
}

int FakeStore::write(object_t oid,
					 size_t len, off_t offset,
					 bufferlist& bl,
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
  
  // seek
  off_t actual = lseek(fd, offset, SEEK_SET);
  int did = 0;
  assert(actual == offset);

  // write buffers
  for (list<bufferptr>::iterator it = bl.buffers().begin();
	   it != bl.buffers().end();
	   it++) {
	int r = ::write(fd, (*it).c_str(), (*it).length());
	if (r > 0)
	  did += r;
	else {
	  dout(1) << "couldn't write to " << fn.c_str() << " len " << len << " off " << offset << " errno " << errno << " " << strerror(errno) << endl;
	}
  }
  
  if (did < 0) {
	dout(1) << "couldn't write to " << fn.c_str() << " len " << len << " off " << offset << " errno " << errno << " " << strerror(errno) << endl;
  }

  // sync to to disk?
  if (do_fsync && g_conf.osd_fsync) ::fsync(fd); // fsync or fdatasync?

  ::flock(fd, LOCK_UN);
  ::close(fd);
  
  return did;
}

int FakeStore::write(object_t oid, 
					 size_t len, off_t offset, 
					 bufferlist& bl, 
					 Context *onsafe)
{
  dout(20) << "write " << oid << " len " << len << " off " << offset << endl;

  string fn;
  get_oname(oid,fn);
  
  ::mknod(fn.c_str(), 0644, 0);  // in case it doesn't exist yet.

  int flags = O_WRONLY;//|O_CREAT;
  int fd = ::open(fn.c_str(), flags);
  if (fd < 0) {
	dout(1) << "write couldn't open " << fn.c_str() << " flags " << flags << " errno " << errno << " " << strerror(errno) << endl;
	return fd;
  }
  ::flock(fd, LOCK_EX);    // lock for safety
  //::fchmod(fd, 0664);
  
  // seek
  off_t actual = lseek(fd, offset, SEEK_SET);
  int did = 0;
  assert(actual == offset);

  // write buffers
  for (list<bufferptr>::iterator it = bl.buffers().begin();
	   it != bl.buffers().end();
	   it++) {
	int r = ::write(fd, (*it).c_str(), (*it).length());
	if (r > 0)
	  did += r;
	else {
	  dout(1) << "couldn't write to " << fn.c_str() << " len " << len << " off " << offset << " errno " << errno << " " << strerror(errno) << endl;
	}
  }
  
  if (did < 0) {
	dout(1) << "couldn't write to " << fn.c_str() << " len " << len << " off " << offset << " errno " << errno << " " << strerror(errno) << endl;
  }

  ::flock(fd, LOCK_UN);

  // schedule sync
  queue_fsync(fd, onsafe);
  
  return did;
}



// ------------------
// attributes

int FakeStore::setattr(object_t oid, const char *name,
					   void *value, size_t size)
{
  if (1) {
	bufferptr bp(new buffer((char*)value,size));
	fakeattrs[whoami][oid][name] = bp;
	return 0;
  } else {
	string fn;
	get_oname(oid, fn);
	int r = setxattr(fn.c_str(), name, value, size, 0);
	if (r == -1) 
	  cerr << " errno is " << errno << " " << strerror(errno) << endl;
	assert(r == 0);
	return r;
  }
}


int FakeStore::getattr(object_t oid, const char *name,
					   void *value, size_t size)
{
  if (1) {
	if (fakeattrs[whoami][oid].count(name)) {
	  size_t l = fakeattrs[whoami][oid][name].length();
	  if (l > size) l = size;
	  bufferlist bl;
	  bl.append(fakeattrs[whoami][oid][name]);
	  bl.copy(0, l, (char*)value);
	  return l;
	} else {
	  return -1;
	}
  } else {
	string fn;
	get_oname(oid, fn);
	int r = getxattr(fn.c_str(), name, value, size);
	//	assert(r == 0);
	return r;
  }
}

int FakeStore::listattr(object_t oid, char *attrs, size_t size)
{
  string fn;
  get_oname(oid, fn);
  return listxattr(fn.c_str(), attrs, size);
}




// ------------------
// collections

// helpers

void FakeStore::open_collections() 
{
  string cfn;
  get_dir(cfn);
  cfn += "/collections";
  collections.open(cfn.c_str());  
  list<coll_t> ls;
  collections.list_keys(ls);
}

void FakeStore::close_collections()
{
  if (collections.is_open())
	collections.close();

  for (map<coll_t, BDBMap<object_t, int>*>::iterator it = collection_map.begin();
	   it != collection_map.end();
	   it++) {
	it->second->close();
  }
  collection_map.clear();
}


int FakeStore::open_collection(coll_t c) {
  if (collection_map.count(c))
	return 0;  // already open.

  string fn;
  get_collfn(c,fn);
  collection_map[c] = new BDBMap<coll_t,int>;
  int r = collection_map[c]->open(fn.c_str());
  if (r != 0)
	collection_map.erase(c);  // failed
  return r;
}

// public
int FakeStore::list_collections(list<coll_t>& ls)
{
  if (!collections.is_open()) open_collections();

  ls.clear();
  collections.list_keys(ls);
  return 0;
}

int FakeStore::collection_stat(coll_t c, struct stat *st) {
  if (!collections.is_open()) open_collections();

  string fn;
  get_collfn(c,fn);
  return ::stat(fn.c_str(), st);
}

int FakeStore::collection_create(coll_t c) {
  if (!collections.is_open()) open_collections();

  collections.put(c, 1);
  open_collection(c);
  return 0;
}

int FakeStore::collection_destroy(coll_t c) {
  if (!collections.is_open()) open_collections();

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
  if (!collections.is_open()) open_collections();

  open_collection(c);
  collection_map[c]->put(o,1);
  return 0;
}
int FakeStore::collection_remove(coll_t c, object_t o) {
  if (!collections.is_open()) open_collections();

  open_collection(c);
  collection_map[c]->del(o);
  return 0;
}
int FakeStore::collection_list(coll_t c, list<object_t>& o) {
  if (!collections.is_open()) open_collections();

  open_collection(c);
  collection_map[c]->list_keys(o);
  return 0;
}

int FakeStore::collection_setattr(coll_t cid, const char *name,
								  void *value, size_t size)
{
  if (!collections.is_open()) open_collections();

  string fn;
  get_collfn(cid,fn);
  return setxattr(fn.c_str(), name, value, size, 0);
}


int FakeStore::collection_getattr(coll_t cid, const char *name,
					   void *value, size_t size)
{
  if (!collections.is_open()) open_collections();

  string fn;
  get_collfn(cid,fn);
  return getxattr(fn.c_str(), name, value, size);
}

int FakeStore::collection_listattr(coll_t cid, char *attrs, size_t size)
{
  if (!collections.is_open()) open_collections();

  string fn;
  get_collfn(cid, fn);
  return listxattr(fn.c_str(), attrs, size);
}


