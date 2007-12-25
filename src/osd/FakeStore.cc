// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
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
#ifndef __CYGWIN__
# include <sys/xattr.h>
#endif
//#include <sys/vfs.h>

#ifdef DARWIN
#include <sys/param.h>
#include <sys/mount.h>
#endif // DARWIN

#include "config.h"

#define  dout(l)    if (l<=g_conf.debug) *_dout << dbeginl << g_clock.now() << " fakestore(" << basedir << ") "
#define  derr(l)    if (l<=g_conf.debug) *_derr << dbeginl << g_clock.now() << " fakestore(" << basedir << ") "

#include "include/buffer.h"

#include <map>


// crap-a-crap hash
//#define HASH_DIRS       0x80
//#define HASH_MASK       0x7f
// end crap hash




int FakeStore::statfs(struct statfs *buf)
{
  return ::statfs(basedir.c_str(), buf);
}


/* 
 * sorry, these are sentitive to the pobject_t and coll_t typing.
 */ 
void FakeStore::get_oname(pobject_t oid, char *s) 
{
  //static hash<pobject_t> H;
  assert(sizeof(oid) == 24);
#ifdef __LP64__
  //sprintf(s, "%s/objects/%02lx/%016lx.%016lx", basedir.c_str(), H(oid) & HASH_MASK, 
  sprintf(s, "%s/objects/%04x.%04x.%016lx.%016lx", basedir.c_str(), 
	  oid.volume, oid.rank,
	  *((uint64_t*)&oid.oid),
	  *(((uint64_t*)&oid.oid) + 1));
#else
  //sprintf(s, "%s/objects/%02x/%016llx.%016llx", basedir.c_str(), H(oid) & HASH_MASK, 
  sprintf(s, "%s/objects/%04x.%04x.%016llx.%016llx", basedir.c_str(), 
	  oid.volume, oid.rank,
	  *((uint64_t*)&oid),
	  *(((uint64_t*)&oid) + 1));
#endif
}

pobject_t FakeStore::parse_object(char *s)
{
  pobject_t o;
  assert(sizeof(o) == 24);
  //cout << "  got object " << de->d_name << std::endl;
  o.volume = strtoll(s, 0, 16);
  assert(s[4] == '.');
  o.rank = strtoll(s+5, 0, 16);
  assert(s[9] == '.');
  *(((uint64_t*)&o.oid) + 0) = strtoll(s+10, 0, 16);
  assert(s[26] == '.');
  *(((uint64_t*)&o.oid) + 1) = strtoll(s+27, 0, 16);
  dout(0) << " got " << o << " errno " << errno << " on " << s << dendl;
  return o;
}

coll_t FakeStore::parse_coll(char *s)
{
  return strtoll(s, 0, 16);
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

void FakeStore::get_coname(coll_t cid, pobject_t oid, char *s) 
{
  assert(sizeof(oid) == 24);
#ifdef __LP64__
  sprintf(s, "%s/collections/%016lx/%04x.%04x.%016lx.%016lx", basedir.c_str(), cid, 
	  oid.volume, oid.rank,
	  *((uint64_t*)&oid.oid),
	  *(((uint64_t*)&oid.oid) + 1));
#else
  sprintf(s, "%s/collections/%016llx/%04x.%04x.%016llx.%016llx", basedir.c_str(), cid, 
	  oid.volume, oid.rank,
	  *((uint64_t*)&oid),
	  *(((uint64_t*)&oid) + 1));
#endif
}




int FakeStore::mkfs()
{
  char cmd[200];
  if (g_conf.fakestore_dev) {
    dout(0) << "mounting" << dendl;
    sprintf(cmd,"mount %s", g_conf.fakestore_dev);
    system(cmd);
  }

  dout(1) << "mkfs in " << basedir << dendl;

  // wipe
  sprintf(cmd, "test -d %s && rm -r %s ; mkdir -p %s/collections && mkdir -p %s/objects",
	  basedir.c_str(), basedir.c_str(), basedir.c_str(), basedir.c_str());
  
  dout(5) << "wipe: " << cmd << dendl;
  system(cmd);

  // hashed bits too
  /*
  for (int i=0; i<HASH_DIRS; i++) {
    char s[4];
    sprintf(s, "%02x", i);
    string subdir = basedir + "/objects/" + s;

    dout(15) << " creating " << subdir << dendl;
    int r = ::mkdir(subdir.c_str(), 0755);
    if (r != 0) {
      derr(0) << "couldnt create subdir, r = " << r << dendl;
      return r;
    }
  }
  */

  if (g_conf.fakestore_dev) {
    char cmd[100];
    dout(0) << "umounting" << dendl;
    sprintf(cmd,"umount %s", g_conf.fakestore_dev);
    //system(cmd);
  }

  dout(1) << "mkfs done in " << basedir << dendl;

  return 0;
}

int FakeStore::mount() 
{
  if (g_conf.fakestore_dev) {
    dout(0) << "mounting" << dendl;
    char cmd[100];
    sprintf(cmd,"mount %s", g_conf.fakestore_dev);
    //system(cmd);
  }

  dout(5) << "basedir " << basedir << dendl;
  
  // make sure global base dir exists
  struct stat st;
  int r = ::stat(basedir.c_str(), &st);
  if (r != 0) {
    derr(0) << "unable to stat basedir " << basedir << ", " << strerror(errno) << dendl;
    return r;
  }
  
  if (g_conf.fakestore_fake_collections) {
    dout(0) << "faking collections (in memory)" << dendl;
    fake_collections = true;
  }

  // fake attrs? 
  // let's test to see if they work.
#ifndef __CYGWIN__
  if (g_conf.fakestore_fake_attrs) {
#endif
    dout(0) << "faking attrs (in memory)" << dendl;
    fake_attrs = true;
#ifndef __CYGWIN__
  } else {
    char names[1000];
    r = ::listxattr(basedir.c_str(), names, 1000);
    if (r < 0) {
      derr(0) << "xattrs don't appear to work (" << strerror(errno) << "), specify --fakestore_fake_attrs to fake them (in memory)." << dendl;
      assert(0);
    }
  }
#endif

  // all okay.
  return 0;
}

int FakeStore::umount() 
{
  dout(5) << "umount " << basedir << dendl;
  
  sync();

  if (g_conf.fakestore_dev) {
    char cmd[100];
    dout(0) << "umounting" << dendl;
    sprintf(cmd,"umount %s", g_conf.fakestore_dev);
    //system(cmd);
  }

  // nothing
  return 0;
}


// --------------------
// objects


bool FakeStore::exists(pobject_t oid)
{
  struct stat st;
  if (stat(oid, &st) == 0)
    return true;
  else 
    return false;
}

  
int FakeStore::stat(pobject_t oid,
                    struct stat *st)
{
  dout(20) << "stat " << oid << dendl;
  char fn[200];
  get_oname(oid,fn);
  int r = ::stat(fn, st);
  return r;
}
 
 

int FakeStore::remove(pobject_t oid, Context *onsafe) 
{
  dout(20) << "remove " << oid << dendl;
  char fn[200];
  get_oname(oid,fn);
  int r = ::unlink(fn);
  if (onsafe) sync(onsafe);
  return r;
}

int FakeStore::truncate(pobject_t oid, off_t size, Context *onsafe)
{
  dout(20) << "truncate " << oid << " size " << size << dendl;

  char fn[200];
  get_oname(oid,fn);
  int r = ::truncate(fn, size);
  if (onsafe) sync(onsafe);
  return r;
}

int FakeStore::read(pobject_t oid, 
                    off_t offset, size_t len,
                    bufferlist& bl) {
  dout(20) << "read " << oid << " len " << len << " off " << offset << dendl;

  char fn[200];
  get_oname(oid,fn);
  
  int fd = ::open(fn, O_RDONLY);
  if (fd < 0) {
    dout(10) << "read couldn't open " << fn << " errno " << errno << " " << strerror(errno) << dendl;
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


int FakeStore::write(pobject_t oid, 
                     off_t offset, size_t len,
                     const bufferlist& bl, 
                     Context *onsafe)
{
  char fn[200];
  get_oname(oid,fn);

  dout(20) << "write " << fn << " len " << len << " off " << offset << dendl;

  
  ::mknod(fn, 0644, 0);  // in case it doesn't exist yet.

  int flags = O_WRONLY;//|O_CREAT;
  int fd = ::open(fn, flags);
  if (fd < 0) {
    derr(0) << "write couldn't open " << fn << " flags " << flags << " errno " << errno << " " << strerror(errno) << dendl;
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
      derr(0) << "couldn't write to " << fn << " len " << len << " off " << offset << " errno " << errno << " " << strerror(errno) << dendl;
    }
  }
  
  if (did < 0) {
    derr(0) << "couldn't write to " << fn << " len " << len << " off " << offset << " errno " << errno << " " << strerror(errno) << dendl;
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
    dout(0) << "sync waiting for " << unsync << " items to (fake) sync" << dendl;
    synccond.Wait(synclock);
  }
  synclock.Unlock();
}

void FakeStore::sync(Context *onsafe)
{
  if (g_conf.fakestore_fake_sync > 0.0) {
    g_timer.add_event_after((float)g_conf.fakestore_fake_sync,
                            new C_FakeSync(onsafe, &unsync, &synclock, &synccond));
    
  } else {
    assert(0); // der..no implemented anymore
  }
}


// -------------------------------
// attributes

// objects

int FakeStore::setattr(pobject_t oid, const char *name,
		       const void *value, size_t size,
		       Context *onsafe) 
{
  if (fake_attrs) return attrs.setattr(oid, name, value, size, onsafe);

  int r = 0;
#ifndef __CYGWIN__
  char fn[100];
  get_oname(oid, fn);
  r = ::setxattr(fn, name, value, size, 0);
#endif
  return r;
}

int FakeStore::setattrs(pobject_t oid, map<string,bufferptr>& aset) 
{
  if (fake_attrs) return attrs.setattrs(oid, aset);

  char fn[100];
  get_oname(oid, fn);
  int r = 0;
#ifndef __CYGWIN__
  for (map<string,bufferptr>::iterator p = aset.begin();
       p != aset.end();
       ++p) {
    r = ::setxattr(fn, p->first.c_str(), p->second.c_str(), p->second.length(), 0);
    if (r < 0) {
      cerr << "error setxattr " << strerror(errno) << std::endl;
      break;
    }
  }
#endif
  return r;
}

int FakeStore::getattr(pobject_t oid, const char *name,
		       void *value, size_t size) 
{
  if (fake_attrs) return attrs.getattr(oid, name, value, size);
  int r = 0;
#ifndef __CYGWIN__
  char fn[100];
  get_oname(oid, fn);
  r = ::getxattr(fn, name, value, size);
#endif
  return r;
}

int FakeStore::getattrs(pobject_t oid, map<string,bufferptr>& aset) 
{
  if (fake_attrs) return attrs.getattrs(oid, aset);

#ifndef __CYGWIN__
  char fn[100];
  get_oname(oid, fn);

  char val[1000];
  char names[1000];
  int num = ::listxattr(fn, names, 1000);
  
  char *name = names;
  for (int i=0; i<num; i++) {
    dout(0) << "getattrs " << oid << " getting " << (i+1) << "/" << num << " '" << names << "'" << dendl;
    int l = ::getxattr(fn, name, val, 1000);
    dout(0) << "getattrs " << oid << " getting " << (i+1) << "/" << num << " '" << names << "' = " << l << " bytes" << dendl;
    aset[names].append(val, l);
    name += strlen(name) + 1;
  }
#endif
  return 0;
}

int FakeStore::rmattr(pobject_t oid, const char *name, Context *onsafe) 
{
  if (fake_attrs) return attrs.rmattr(oid, name, onsafe);
  int r = 0;
#ifndef __CYGWIN__
  char fn[100];
  get_oname(oid, fn);
  r = ::removexattr(fn, name);
#endif
  return r;
}

/*
int FakeStore::listattr(pobject_t oid, char *attrls, size_t size) 
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

int FakeStore::collection_setattrs(coll_t cid, map<string,bufferptr>& aset) 
{
  if (fake_attrs) return attrs.collection_setattrs(cid, aset);

  char fn[100];
  get_cdir(cid, fn);
  int r = 0;
#ifndef __CYGWIN__
  for (map<string,bufferptr>::iterator p = aset.begin();
       p != aset.end();
       ++p) {
    r = ::setxattr(fn, p->first.c_str(), p->second.c_str(), p->second.length(), 0);
    if (r < 0) break;
  }
#endif
  return r;
}

int FakeStore::collection_getattrs(coll_t cid, map<string,bufferptr>& aset) 
{
  if (fake_attrs) return attrs.collection_getattrs(cid, aset);

#ifndef __CYGWIN__
  char fn[100];
  get_cdir(cid, fn);

  char val[1000];
  char names[1000];
  int num = ::listxattr(fn, names, 1000);
  
  char *name = names;
  for (int i=0; i<num; i++) {
    dout(0) << "getattrs " << cid << " getting " << (i+1) << "/" << num << " '" << names << "'" << dendl;
    int l = ::getxattr(fn, name, val, 1000);
    dout(0) << "getattrs " << cid << " getting " << (i+1) << "/" << num << " '" << names << "' = " << l << " bytes" << dendl;
    aset[names].append(val, l);
    name += strlen(name) + 1;
  }
#endif
  return 0;
}


/*
int FakeStore::collection_listattr(coll_t c, char *attrs, size_t size) 
{
  if (fake_attrs) return collection_listattr(c, attrs, size);
  return 0;
}
*/


int FakeStore::list_objects(list<pobject_t>& ls) 
{
  char fn[200];
  sprintf(fn, "%s/objects", basedir.c_str());

  DIR *dir = ::opendir(fn);
  assert(dir);

  struct dirent *de;
  while ((de = ::readdir(dir)) != 0) {
    if (de->d_name[0] == '.') continue;
    // parse
    pobject_t o = parse_object(de->d_name);
    if (errno) continue;
    ls.push_back(o);
  }
  
  ::closedir(dir);
  return 0;
}


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
    errno = 0;
    coll_t c = parse_coll(de->d_name);
    if (c) ls.push_back(c);
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


int FakeStore::collection_add(coll_t c, pobject_t o,
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

int FakeStore::collection_remove(coll_t c, pobject_t o,
				 Context *onsafe) 
{
  if (fake_collections) return collections.collection_remove(c, o, onsafe);

  char cof[200];
  get_coname(c, o, cof);

  int r = ::unlink(cof);
  if (onsafe) sync(onsafe);
  return r;
}

int FakeStore::collection_list(coll_t c, list<pobject_t>& ls) 
{  
  if (fake_collections) return collections.collection_list(c, ls);

  char fn[200];
  get_cdir(c, fn);

  DIR *dir = ::opendir(fn);
  assert(dir);
  
  struct dirent *de;
  while ((de = ::readdir(dir)) != 0) {
    // parse
    if (de->d_name[0] == '.') continue;
    //cout << "  got object " << de->d_name << std::endl;
    pobject_t o = parse_object(de->d_name);
    if (errno) continue;
    ls.push_back(o);
  }
  
  ::closedir(dir);
  return 0;
}

// eof.
