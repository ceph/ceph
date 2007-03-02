// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software 
 * Foundation.  See file COPYING.
 * 
 */



#include "FakeStore.h"
#include "include/types.h"

#include "common/Timer.h"

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
//#include <sys/vfs.h>

#ifdef DARWIN
#include <sys/param.h>
#include <sys/mount.h>
#endif // DARWIN

#include "config.h"
#undef dout
#define  dout(l)    if (l<=g_conf.debug) cout << g_clock.now() << " osd" << whoami << ".fakestore "
#define  derr(l)    if (l<=g_conf.debug) cerr << g_clock.now() << " osd" << whoami << ".fakestore "

#include "include/buffer.h"

#include <map>
#include <ext/hash_map>
using namespace __gnu_cxx;

// crap-a-crap hash
#define HASH_DIRS       0x80
#define HASH_MASK       0x7f
// end crap hash




int FakeStore::statfs(struct statfs *buf)
{
  return ::statfs(basedir.c_str(), buf);
}


/* 
 * sorry, these are sentitive to the object_t and coll_t typing.
 */ 
void FakeStore::get_oname(object_t oid, char *s) 
{
  static hash<object_t> H;
  assert(sizeof(oid) == 16);
#ifdef __LP64__
  sprintf(s, "%s/objects/%02lx/%016lx.%016lx", basedir.c_str(), H(oid) & HASH_MASK, 
	  *((__uint64_t*)&oid),
	  *(((__uint64_t*)&oid) + 1));
#else
  sprintf(s, "%s/objects/%02x/%016llx.%016llx", basedir.c_str(), H(oid) & HASH_MASK, 
	  *((__uint64_t*)&oid),
	  *(((__uint64_t*)&oid) + 1));
#endif
}

void FakeStore::get_cdir(coll_t cid, char *s) 
{
  assert(sizeof(cid) == 8);
#ifdef __LP64__
  sprintf(s, "%s/collections/%016lx", basedir.c_str(), 
	  cid);
#else
  sprintf(s, "%s/collections/%016llx", basedir.c_str(), 
	  cid);
#endif
}

void FakeStore::get_coname(coll_t cid, object_t oid, char *s) 
{
  assert(sizeof(oid) == 16);
#ifdef __LP64__
  sprintf(s, "%s/collections/%016lx/%016lx.%016lx", basedir.c_str(), cid, 
	  *((__uint64_t*)&oid),
	  *(((__uint64_t*)&oid) + 1));
#else
  sprintf(s, "%s/collections/%016llx/%016llx.%016llx", basedir.c_str(), cid, 
	  *((__uint64_t*)&oid),
	  *(((__uint64_t*)&oid) + 1));
#endif
}




int FakeStore::mkfs()
{
  char cmd[200];
  if (g_conf.fakestore_dev) {
    dout(0) << "mounting" << endl;
    sprintf(cmd,"mount %s", g_conf.fakestore_dev);
    system(cmd);
  }

  dout(1) << "mkfs in " << basedir << endl;

  // wipe
  sprintf(cmd, "test -d %s && rm -r %s ; mkdir -p %s/collections && mkdir -p %s/objects",
	  basedir.c_str(), basedir.c_str(), basedir.c_str(), basedir.c_str());
  
  dout(5) << "wipe: " << cmd << endl;
  system(cmd);

  // hashed bits too
  for (int i=0; i<HASH_DIRS; i++) {
    char s[4];
    sprintf(s, "%02x", i);
    string subdir = basedir + "/objects/" + s;

    dout(15) << " creating " << subdir << endl;
    int r = ::mkdir(subdir.c_str(), 0755);
    if (r != 0) {
      derr(0) << "couldnt create subdir, r = " << r << endl;
      return r;
    }
  }
  
  if (g_conf.fakestore_dev) {
    char cmd[100];
    dout(0) << "umounting" << endl;
    sprintf(cmd,"umount %s", g_conf.fakestore_dev);
    //system(cmd);
  }

  dout(1) << "mkfs done in " << basedir << endl;

  return 0;
}

int FakeStore::mount() 
{
  if (g_conf.fakestore_dev) {
    dout(0) << "mounting" << endl;
    char cmd[100];
    sprintf(cmd,"mount %s", g_conf.fakestore_dev);
    //system(cmd);
  }

  dout(5) << "basedir " << basedir << endl;
  
  // make sure global base dir exists
  struct stat st;
  int r = ::stat(basedir.c_str(), &st);
  if (r != 0) {
    derr(0) << "unable to stat basedir " << basedir << ", r = " << r << endl;
    return r;
  }
  
  if (g_conf.fakestore_fake_collections) {
    dout(0) << "faking collections (in memory)" << endl;
    fake_collections = true;
  }

  // fake attrs? 
  // let's test to see if they work.
  if (g_conf.fakestore_fake_attrs) {
    dout(0) << "faking attrs (in memory)" << endl;
    fake_attrs = true;
  } else {
    char names[1000];
    r = ::listxattr(basedir.c_str(), names, 1000);
    if (r < 0) {
      derr(0) << "xattrs don't appear to work (" << strerror(errno) << "), specify --fakestore_fake_attrs to fake them (in memory)." << endl;
      assert(0);
    }
  }

  // all okay.
  return 0;
}

int FakeStore::umount() 
{
  dout(5) << "umount " << basedir << endl;
  
  sync();

  if (g_conf.fakestore_dev) {
    char cmd[100];
    dout(0) << "umounting" << endl;
    sprintf(cmd,"umount %s", g_conf.fakestore_dev);
    //system(cmd);
  }

  // nothing
  return 0;
}


// --------------------
// objects


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
  char fn[200];
  get_oname(oid,fn);
  int r = ::stat(fn, st);
  return r;
}
 
 

int FakeStore::remove(object_t oid, Context *onsafe) 
{
  dout(20) << "remove " << oid << endl;
  char fn[200];
  get_oname(oid,fn);
  int r = ::unlink(fn);
  if (onsafe) sync(onsafe);
  return r;
}

int FakeStore::truncate(object_t oid, off_t size, Context *onsafe)
{
  dout(20) << "truncate " << oid << " size " << size << endl;

  char fn[200];
  get_oname(oid,fn);
  int r = ::truncate(fn, size);
  if (onsafe) sync(onsafe);
  return r;
}

int FakeStore::read(object_t oid, 
                    off_t offset, size_t len,
                    bufferlist& bl) {
  dout(20) << "read " << oid << " len " << len << " off " << offset << endl;

  char fn[200];
  get_oname(oid,fn);
  
  int fd = ::open(fn, O_RDONLY);
  if (fd < 0) {
    dout(10) << "read couldn't open " << fn << " errno " << errno << " " << strerror(errno) << endl;
    return fd;
  }
  ::flock(fd, LOCK_EX);    // lock for safety
  
  off_t actual = lseek(fd, offset, SEEK_SET);
  size_t got = 0;

  if (len == 0) {
    struct stat st;
    ::fstat(fd, &st);
    len = st.st_size;
  }

  if (actual == offset) {
    bufferptr bptr(len);  // prealloc space for entire read
    got = ::read(fd, bptr.c_str(), len);
    bptr.set_length(got);   // properly size the buffer
    if (got > 0) bl.push_back( bptr );   // put it in the target bufferlist
  }
  ::flock(fd, LOCK_UN);
  ::close(fd);
  return got;
}


int FakeStore::write(object_t oid, 
                     off_t offset, size_t len,
                     bufferlist& bl, 
                     Context *onsafe)
{
  dout(20) << "write " << oid << " len " << len << " off " << offset << endl;

  char fn[200];
  get_oname(oid,fn);
  
  ::mknod(fn, 0644, 0);  // in case it doesn't exist yet.

  int flags = O_WRONLY;//|O_CREAT;
  int fd = ::open(fn, flags);
  if (fd < 0) {
    derr(0) << "write couldn't open " << fn << " flags " << flags << " errno " << errno << " " << strerror(errno) << endl;
    return fd;
  }
  ::fchmod(fd, 0664);
  ::flock(fd, LOCK_EX);    // lock for safety
  
  // seek
  off_t actual = ::lseek(fd, offset, SEEK_SET);
  int did = 0;
  assert(actual == offset);

  // write buffers
  for (list<bufferptr>::const_iterator it = bl.buffers().begin();
       it != bl.buffers().end();
       it++) {
    int r = ::write(fd, (char*)(*it).c_str(), (*it).length());
    if (r > 0)
      did += r;
    else {
      derr(0) << "couldn't write to " << fn << " len " << len << " off " << offset << " errno " << errno << " " << strerror(errno) << endl;
    }
  }
  
  if (did < 0) {
    derr(0) << "couldn't write to " << fn << " len " << len << " off " << offset << " errno " << errno << " " << strerror(errno) << endl;
  }

  ::flock(fd, LOCK_UN);

  // schedule sync
  if (onsafe) sync(onsafe);

  ::close(fd);
  
  return did;
}


class C_FakeSync : public Context {
  Context *c;
  int *n;
  Mutex *lock;
  Cond *cond;

public:
  C_FakeSync(Context *c_, int *n_, Mutex *lo, Cond *co) : 
    c(c_), n(n_),
    lock(lo), cond(co) {
    lock->Lock();
    ++*n;
    lock->Unlock();
  }
  void finish(int r) {
    c->finish(r);
    
    lock->Lock();
    --(*n);
    if (*n == 0) cond->Signal();
    lock->Unlock();
  }
};

void FakeStore::sync()
{
  synclock.Lock();
  while (unsync > 0) {
    dout(0) << "sync waiting for " << unsync << " items to (fake) sync" << endl;
    synccond.Wait(synclock);
  }
  synclock.Unlock();
}

void FakeStore::sync(Context *onsafe)
{
  if (g_conf.fakestore_fake_sync) {
    g_timer.add_event_after((float)g_conf.fakestore_fake_sync,
                            new C_FakeSync(onsafe, &unsync, &synclock, &synccond));
    
  } else {
    assert(0); // der..no implemented anymore
  }
}


// -------------------------------
// attributes

// objects

int FakeStore::setattr(object_t oid, const char *name,
		       const void *value, size_t size,
		       Context *onsafe) 
{
  if (fake_attrs) return attrs.setattr(oid, name, value, size, onsafe);

  char fn[100];
  get_oname(oid, fn);
  int r = ::setxattr(fn, name, value, size, 0);
  return r;
}

int FakeStore::setattrs(object_t oid, map<string,bufferptr>& aset) 
{
  if (fake_attrs) return attrs.setattrs(oid, aset);

  char fn[100];
  get_oname(oid, fn);
  int r = 0;
  for (map<string,bufferptr>::iterator p = aset.begin();
       p != aset.end();
       ++p) {
    r = ::setxattr(fn, p->first.c_str(), p->second.c_str(), p->second.length(), 0);
    if (r < 0) break;
  }
  return r;
}

int FakeStore::getattr(object_t oid, const char *name,
		       void *value, size_t size) 
{
  if (fake_attrs) return attrs.getattr(oid, name, value, size);
  char fn[100];
  get_oname(oid, fn);
  int r = ::getxattr(fn, name, value, size);
  return r;
}

int FakeStore::getattrs(object_t oid, map<string,bufferptr>& aset) 
{
  if (fake_attrs) return attrs.getattrs(oid, aset);

  char fn[100];
  get_oname(oid, fn);

  char val[1000];
  char names[1000];
  int num = ::listxattr(fn, names, 1000);
  
  char *name = names;
  for (int i=0; i<num; i++) {
    dout(0) << "getattrs " << oid << " getting " << (i+1) << "/" << num << " '" << names << "'" << endl;
    int l = ::getxattr(fn, name, val, 1000);
    dout(0) << "getattrs " << oid << " getting " << (i+1) << "/" << num << " '" << names << "' = " << l << " bytes" << endl;
    aset[names].append(val, l);
    name += strlen(name) + 1;
  }
  
  return 0;
}

int FakeStore::rmattr(object_t oid, const char *name, Context *onsafe) 
{
  if (fake_attrs) return attrs.rmattr(oid, name, onsafe);
  char fn[100];
  get_oname(oid, fn);
  int r = ::removexattr(fn, name);
  return r;
}

/*
int FakeStore::listattr(object_t oid, char *attrls, size_t size) 
{
  if (fake_attrs) return attrs.listattr(oid, attrls, size);
  char fn[100];
  get_oname(oid, fn);
  return ::listxattr(fn, attrls, size);
}
*/


// collections

int FakeStore::collection_setattr(coll_t c, const char *name,
				  void *value, size_t size,
				  Context *onsafe) 
{
  if (fake_attrs) return attrs.collection_setattr(c, name, value, size, onsafe);
  return 0;
}

int FakeStore::collection_rmattr(coll_t c, const char *name,
				 Context *onsafe) 
{
  if (fake_attrs) return attrs.collection_rmattr(c, name, onsafe);
  return 0;
}

int FakeStore::collection_getattr(coll_t c, const char *name,
				  void *value, size_t size) 
{
  if (fake_attrs) return attrs.collection_getattr(c, name, value, size);
  return 0;
}

/*
int FakeStore::collection_listattr(coll_t c, char *attrs, size_t size) 
{
  if (fake_attrs) return collection_listattr(c, attrs, size);
  return 0;
}
*/

// --------------------------
// collections

int FakeStore::list_collections(list<coll_t>& ls) 
{
  if (fake_collections) return collections.list_collections(ls);

  char fn[200];
  sprintf(fn, "%s/collections", basedir.c_str());

  DIR *dir = ::opendir(fn);
  assert(dir);

  struct dirent *de;
  while ((de = ::readdir(dir)) != 0) {
    // parse
    coll_t c = strtoll(de->d_name, 0, 16);
    dout(0) << " got " << c << " errno " << errno << " on " << de->d_name << endl;
    if (errno) continue;
    ls.push_back(c);
  }
  
  ::closedir(dir);
  return 0;
}

int FakeStore::create_collection(coll_t c,
				 Context *onsafe) 
{
  if (fake_collections) return collections.create_collection(c, onsafe);
  
  char fn[200];
  get_cdir(c, fn);

  int r = ::mkdir(fn, 0755);

  if (onsafe) sync(onsafe);
  return r;
}

int FakeStore::destroy_collection(coll_t c,
				  Context *onsafe) 
{
  if (fake_collections) return collections.destroy_collection(c, onsafe);

  char fn[200];
  get_cdir(c, fn);
  char cmd[200];
  sprintf(cmd, "test -d %s && rm -r %s", fn, fn);
  system(cmd);

  if (onsafe) sync(onsafe);
  return 0;
}

int FakeStore::collection_stat(coll_t c, struct stat *st) 
{
  if (fake_collections) return collections.collection_stat(c, st);

  char fn[200];
  get_cdir(c, fn);
  return ::lstat(fn, st);
}

bool FakeStore::collection_exists(coll_t c) 
{
  if (fake_collections) return collections.collection_exists(c);

  struct stat st;
  return collection_stat(c, &st) == 0;
}


int FakeStore::collection_add(coll_t c, object_t o,
			      Context *onsafe) 
{
  if (fake_collections) return collections.collection_add(c, o, onsafe);

  char cof[200];
  get_coname(c, o, cof);
  char of[200];
  get_oname(o, of);
  
  int r = ::link(of, cof);
  if (onsafe) sync(onsafe);
  return r;
}

int FakeStore::collection_remove(coll_t c, object_t o,
				 Context *onsafe) 
{
  if (fake_collections) return collections.collection_remove(c, o, onsafe);

  char cof[200];
  get_coname(c, o, cof);

  int r = ::unlink(cof);
  if (onsafe) sync(onsafe);
  return r;
}

int FakeStore::collection_list(coll_t c, list<object_t>& ls) 
{  
  if (fake_collections) return collections.collection_list(c, ls);

  char fn[200];
  get_cdir(c, fn);

  DIR *dir = ::opendir(fn);
  assert(dir);
  
  struct dirent *de;
  while ((de = ::readdir(dir)) != 0) {
    // parse
    object_t o;
    assert(sizeof(o) == 16);
    *(((__uint64_t*)&o) + 0) = strtoll(de->d_name, 0, 16);
    assert(de->d_name[16] == '.');
    *(((__uint64_t*)&o) + 1) = strtoll(de->d_name+17, 0, 16);
    dout(0) << " got " << o << " errno " << errno << " on " << de->d_name << endl;
    if (errno) continue;
    ls.push_back(o);
  }
  
  ::closedir(dir);
  return 0;
}

// eof.
