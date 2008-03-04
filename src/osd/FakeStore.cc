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

#include "FileJournal.h"

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

#define  dout(l)    if (l<=g_conf.debug_fakestore) *_dout << dbeginl << g_clock.now() << " fakestore(" << basedir << ") "
#define  derr(l)    if (l<=g_conf.debug_fakestore) *_derr << dbeginl << g_clock.now() << " fakestore(" << basedir << ") "

#include "include/buffer.h"

#include <map>

int FakeStore::statfs(struct statfs *buf)
{
  return ::statfs(basedir.c_str(), buf);
}


/* 
 * sorry, these are sentitive to the pobject_t and coll_t typing.
 */ 
void FakeStore::get_oname(pobject_t oid, char *s) 
{
  assert(sizeof(oid) == 24);
#ifdef __LP64__
  sprintf(s, "%s/objects/%04x.%04x.%016lx.%016lx", basedir.c_str(), 
	  oid.volume, oid.rank,
	  *((uint64_t*)&oid.oid),
	  *(((uint64_t*)&oid.oid) + 1));
#else
  sprintf(s, "%s/objects/%04x.%04x.%016llx.%016llx", basedir.c_str(), 
	  oid.volume, oid.rank,
	  *((uint64_t*)&oid.oid),
	  *(((uint64_t*)&oid.oid) + 1));
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

  // fsid
  fsid = rand();
  char fn[100];
  sprintf(fn, "%s/fsid", basedir.c_str());
  int fd = ::open(fn, O_CREAT|O_TRUNC|O_WRONLY);
  ::write(fd, &fsid, sizeof(fsid));
  ::fchmod(fd, 0644);
  ::close(fd);
  dout(10) << "mkfs fsid is " << fsid << dendl;

  // journal?
  struct stat st;
  sprintf(fn, "%s.journal", basedir.c_str());
  if (::stat(fn, &st) == 0) {
    journal = new FileJournal(fsid, &finisher, fn, g_conf.journal_dio);
    if (journal->create() < 0) {
      dout(0) << "mkfs error creating journal on " << fn << dendl;
    } else {
      dout(0) << "mkfs created journal on " << fn << dendl;
    }
    delete journal;
    journal = 0;
  } else {
    dout(10) << "mkfs no journal at " << fn << dendl;
  }

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
  if (g_conf.fakestore_fake_attrs) {
    dout(0) << "faking attrs (in memory)" << dendl;
    fake_attrs = true;
  } else {
    char names[1000];
    r = ::listxattr(basedir.c_str(), names, 1000);
    if (r < 0) {
      derr(0) << "xattrs don't appear to work (" << strerror(errno) << "), specify --fakestore_fake_attrs to fake them (in memory)." << dendl;
      assert(0);
    }
  }

  char fn[100];
  int fd;

  // get fsid
  sprintf(fn, "%s/fsid", basedir.c_str());
  fd = ::open(fn, O_RDONLY);
  ::read(fd, &fsid, sizeof(fsid));
  ::close(fd);
  dout(10) << "mount fsid is " << fsid << dendl;
  
  // get epoch
  sprintf(fn, "%s/commit_epoch", basedir.c_str());
  fd = ::open(fn, O_RDONLY);
  ::read(fd, &super_epoch, sizeof(super_epoch));
  ::close(fd);
  dout(5) << "mount epoch is " << super_epoch << dendl;

  // journal
  sprintf(fn, "%s.journal", basedir.c_str());
  if (::stat(fn, &st) == 0) {
    dout(10) << "mount opening journal at " << fn << dendl;
    journal = new FileJournal(fsid, &finisher, fn, g_conf.journal_dio);
  } else {
    dout(10) << "mount no journal at " << fn << dendl;
  }
  r = journal_replay();
  if (r == -EINVAL) {
    dout(0) << "mount got EINVAL on journal open, not mounting" << dendl;
    return r;
  }
  journal_start();
  sync_thread.create();

  // all okay.
  return 0;
}

int FakeStore::umount() 
{
  dout(5) << "umount " << basedir << dendl;
  
  sync();
  journal_stop();

  lock.Lock();
  stop = true;
  sync_cond.Signal();
  lock.Unlock();
  sync_thread.join();

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

  
int FakeStore::stat(pobject_t oid, struct stat *st)
{
  dout(20) << "stat " << oid << dendl;
  char fn[200];
  get_oname(oid,fn);
  int r = ::stat(fn, st);
  dout(20) << "stat " << oid << " at " << fn << " = " << r << dendl;
  return r;
}
 
 
int FakeStore::remove(pobject_t oid, Context *onsafe) 
{
  dout(20) << "remove " << oid << dendl;
  char fn[200];
  get_oname(oid,fn);
  int r = ::unlink(fn);
  if (r == 0) 
    journal_remove(oid, onsafe);
  else
    delete onsafe;
  return r;
}

int FakeStore::truncate(pobject_t oid, off_t size, Context *onsafe)
{
  dout(20) << "truncate " << oid << " size " << size << dendl;

  char fn[200];
  get_oname(oid,fn);
  int r = ::truncate(fn, size);
  if (r >= 0) journal_truncate(oid, size, onsafe);
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
  ::fchmod(fd, 0644);
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
  if (did >= 0)
    journal_write(oid, offset, len, bl, onsafe);
  else
    delete onsafe;

  ::close(fd);
  
  return did;
}


void FakeStore::sync_entry()
{
  lock.Lock();
  utime_t interval;
  interval.set_from_double(g_conf.fakestore_sync_interval);
  while (!stop) {
    dout(10) << "sync_entry waiting for " << interval << dendl;
    sync_cond.WaitInterval(lock, interval);
    dout(10) << "sync_entry committing " << super_epoch << dendl;
    commit_start();

    // induce an fs sync.
    // we assume data=ordered or similar semantics
    char fn[100];
    sprintf(fn, "%s/commit_epoch", basedir.c_str());
    int fd = ::open(fn, O_CREAT|O_WRONLY);
    ::write(fd, &super_epoch, sizeof(super_epoch));
    ::fchmod(fd, 0644);
    ::fsync(fd);  // this should cause the fs's journal to commit.
    ::close(fd);

    commit_finish();
    dout(10) << "sync_entry committed " << super_epoch << dendl;
  }
  lock.Unlock();
}

void FakeStore::sync()
{
  Mutex::Locker l(lock);
  sync_cond.Signal();
}

void FakeStore::sync(Context *onsafe)
{
  journal_sync(onsafe);
  sync();
}


// -------------------------------
// attributes

// objects

int FakeStore::setattr(pobject_t oid, const char *name,
		       const void *value, size_t size,
		       Context *onsafe) 
{
  int r;
  if (fake_attrs) 
    r = attrs.setattr(oid, name, value, size, onsafe);
  else {
    char fn[100];
    get_oname(oid, fn);
    r = ::setxattr(fn, name, value, size, 0);
  }
  if (r >= 0)
    journal_setattr(oid, name, value, size, onsafe);
  else
    delete onsafe;
  return r;
}

int FakeStore::setattrs(pobject_t oid, map<string,bufferptr>& aset) 
{
  int r;
  if (fake_attrs) 
    r = attrs.setattrs(oid, aset);
  else {
    char fn[100];
    get_oname(oid, fn);
    r = 0;
    for (map<string,bufferptr>::iterator p = aset.begin();
	 p != aset.end();
	 ++p) {
      r = ::setxattr(fn, p->first.c_str(), p->second.c_str(), p->second.length(), 0);
      if (r < 0) {
	cerr << "error setxattr " << strerror(errno) << std::endl;
	break;
      }
    }
  }
  if (r >= 0)
    journal_setattrs(oid, aset, 0);
  return r;
}

int FakeStore::getattr(pobject_t oid, const char *name,
		       void *value, size_t size) 
{
  int r;
  if (fake_attrs) 
    r = attrs.getattr(oid, name, value, size);
  else {
    char fn[100];
    get_oname(oid, fn);
    r = ::getxattr(fn, name, value, size);
  }
  return r;
}

int FakeStore::getattrs(pobject_t oid, map<string,bufferptr>& aset) 
{
  int r;
  if (fake_attrs) 
    r = attrs.getattrs(oid, aset);
  else {
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
  }
  return r;
}

int FakeStore::rmattr(pobject_t oid, const char *name, Context *onsafe) 
{
  int r;
  if (fake_attrs) 
    r = attrs.rmattr(oid, name, onsafe);
  else {
    char fn[100];
    get_oname(oid, fn);
    r = ::removexattr(fn, name);
  }
  if (r >= 0)
    journal_rmattr(oid, name, onsafe);
  else
    delete onsafe;
  return r;
}



// collections

int FakeStore::collection_setattr(coll_t c, const char *name,
				  void *value, size_t size,
				  Context *onsafe) 
{
  int r;
  if (fake_attrs) 
    r = attrs.collection_setattr(c, name, value, size, onsafe);
  else {
    char fn[200];
    get_cdir(c, fn);
    r = ::setxattr(fn, name, value, size, 0);
  }
  if (r >= 0)
    journal_collection_setattr(c, name, value, size, onsafe);
  else 
    delete onsafe;
  return r;
}

int FakeStore::collection_rmattr(coll_t c, const char *name,
				 Context *onsafe) 
{
  int r;
  if (fake_attrs) 
    r = attrs.collection_rmattr(c, name, onsafe);
  else {
    char fn[200];
    get_cdir(c, fn);
    r = ::removexattr(fn, name);
  }
  return 0;
}

int FakeStore::collection_getattr(coll_t c, const char *name,
				  void *value, size_t size) 
{
  int r;
  if (fake_attrs) 
    r = attrs.collection_getattr(c, name, value, size);
  else {
    char fn[200];
    get_cdir(c, fn);
    r = ::getxattr(fn, name, value, size);   
  }
  return r;
}

int FakeStore::collection_setattrs(coll_t cid, map<string,bufferptr>& aset) 
{
  int r;
  if (fake_attrs) 
    r = attrs.collection_setattrs(cid, aset);
  else {
    char fn[100];
    get_cdir(cid, fn);
    int r = 0;
    for (map<string,bufferptr>::iterator p = aset.begin();
	 p != aset.end();
       ++p) {
      r = ::setxattr(fn, p->first.c_str(), p->second.c_str(), p->second.length(), 0);
      if (r < 0) break;
    }
  }
  if (r >= 0)
    journal_collection_setattrs(cid, aset, 0);
  return r;
}

int FakeStore::collection_getattrs(coll_t cid, map<string,bufferptr>& aset) 
{
  int r;
  if (fake_attrs) 
    r = attrs.collection_getattrs(cid, aset);
  else {
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
    r = 0;
  }
  return r;
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

  if (r >= 0)
    journal_create_collection(c, onsafe);
  else 
    delete onsafe;
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
  int r = 0; // fixme

  if (r >= 0)
    journal_destroy_collection(c, onsafe);
  else 
    delete onsafe;
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
  int r;
  if (fake_collections) 
    r = collections.collection_add(c, o, onsafe);
  else {
    char cof[200];
    get_coname(c, o, cof);
    char of[200];
    get_oname(o, of);
    r = ::link(of, cof);
  }
  if (r >= 0)
    journal_collection_add(c, o, onsafe);
  else 
    delete onsafe;
  return r;
}

int FakeStore::collection_remove(coll_t c, pobject_t o,
				 Context *onsafe) 
{
  int r;
  if (fake_collections) 
    r = collections.collection_remove(c, o, onsafe);
  else {
    char cof[200];
    get_coname(c, o, cof);
    r = ::unlink(cof);
  }
  if (r >= 0)
    journal_collection_remove(c, o, onsafe);
  else
    delete onsafe;
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
