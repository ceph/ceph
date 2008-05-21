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



#include "FileStore.h"
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
#include <sys/ioctl.h>
#ifndef __CYGWIN__
# include <sys/xattr.h>
#endif

#ifdef DARWIN
#include <sys/param.h>
#include <sys/mount.h>
#endif // DARWIN


#ifndef __CYGWIN__
#ifndef DARWIN
# include <linux/ioctl.h>
# define BTRFS_IOCTL_MAGIC 0x94
# define BTRFS_IOC_TRANS_START  _IO(BTRFS_IOCTL_MAGIC, 6)
# define BTRFS_IOC_TRANS_END    _IO(BTRFS_IOCTL_MAGIC, 7)
# define BTRFS_IOC_SYNC         _IO(BTRFS_IOCTL_MAGIC, 8)
# define BTRFS_IOC_CLONE        _IOW(BTRFS_IOCTL_MAGIC, 9, int)
#endif
#endif


#include "config.h"

#define  dout(l)    if (l<=g_conf.debug_filestore) *_dout << dbeginl << g_clock.now() << " filestore(" << basedir << ") "
#define  derr(l)    if (l<=g_conf.debug_filestore) *_derr << dbeginl << g_clock.now() << " filestore(" << basedir << ") "

#include "include/buffer.h"

#include <map>


/*
 * xattr portability stupidity
 */ 

#ifdef DARWIN
int do_getxattr(const char *fn, const char *name, void *val, size_t size) {
  return ::getxattr(fn, name, val, size, 0, 0);
}
int do_setxattr(const char *fn, const char *name, const void *val, size_t size) {
  return ::setxattr(fn, name, val, size, 0, 0);
}
int do_removexattr(const char *fn, const char *name) {
  return ::removexattr(fn, name, 0);
}
int do_listxattr(const char *fn, char *names, size_t len) {
  return ::listxattr(fn, names, len, 0);
}
#else
int do_getxattr(const char *fn, const char *name, void *val, size_t size) {
  return ::getxattr(fn, name, val, size);
}
int do_setxattr(const char *fn, const char *name, const void *val, size_t size) {
  return ::setxattr(fn, name, val, size, 0);
}
int do_removexattr(const char *fn, const char *name) {
  return ::removexattr(fn, name);
}
int do_listxattr(const char *fn, char *names, size_t len) {
  return ::listxattr(fn, names, len);
}

#endif




int FileStore::statfs(struct statfs *buf)
{
  if (::statfs(basedir.c_str(), buf) < 0)
    return -errno;
  return 0;
}


/* 
 * sorry, these are sentitive to the pobject_t and coll_t typing.
 */ 
void FileStore::append_oname(const pobject_t &oid, char *s)
{
  //assert(sizeof(oid) == 28);
  char *t = s + strlen(s);
#ifdef __LP64__
  sprintf(t, "/%04x.%04x.%016lx.%08x.%lx", 
	  oid.volume, oid.rank, oid.oid.ino, oid.oid.bno, oid.oid.rev);
#else
  sprintf(t, "/%04x.%04x.%016llx.%08x.%llx", 
	  oid.volume, oid.rank, oid.oid.ino, oid.oid.bno, oid.oid.rev);
#endif
  //parse_object(t+1);
}

pobject_t FileStore::parse_object(char *s)
{
  pobject_t o;
  //assert(sizeof(o) == 28);
  dout(0) << "  got object " << s << dendl;
  o.volume = strtoll(s, 0, 16);
  assert(s[4] == '.');
  o.rank = strtoll(s+5, 0, 16);
  assert(s[9] == '.');
  o.oid.ino = strtoll(s+10, 0, 16);
  assert(s[26] == '.');
  o.oid.bno = strtoll(s+27, 0, 16);
  assert(s[35] == '.');
  o.oid.rev = strtoll(s+36, 0, 16);
  dout(0) << " got " << o << " errno " << errno << " on " << s << dendl;
  return o;
}

coll_t FileStore::parse_coll(char *s)
{
  return strtoll(s, 0, 16);
}

void FileStore::get_cdir(coll_t cid, char *s) 
{
  assert(sizeof(cid) == 8);
#ifdef __LP64__
  sprintf(s, "%s/%016lx", basedir.c_str(), cid);
#else
  sprintf(s, "%s/%016llx", basedir.c_str(), cid);
#endif
}

void FileStore::get_coname(coll_t cid, pobject_t oid, char *s) 
{
  get_cdir(cid, s);
  append_oname(oid, s);
}




int FileStore::mkfs()
{
  char cmd[200];
  if (g_conf.filestore_dev) {
    dout(0) << "mounting" << dendl;
    sprintf(cmd,"mount %s", g_conf.filestore_dev);
    system(cmd);
  }

  dout(1) << "mkfs in " << basedir << dendl;

  // wipe
  sprintf(cmd, "test -d %s && rm -r %s ; mkdir -p %s",
	  basedir.c_str(), basedir.c_str(), basedir.c_str());
  
  dout(5) << "wipe: " << cmd << dendl;
  system(cmd);

  // fsid
  fsid = rand();
  char fn[100];
  sprintf(fn, "%s/fsid", basedir.c_str());
  int fd = ::open(fn, O_CREAT|O_TRUNC|O_WRONLY, 0644);
  ::write(fd, &fsid, sizeof(fsid));
  ::close(fd);
  dout(10) << "mkfs fsid is " << fsid << dendl;

  // journal?
  struct stat st;
  sprintf(fn, "%s.journal", basedir.c_str());
  if (::lstat(fn, &st) == 0) {
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

  if (g_conf.filestore_dev) {
    char cmd[100];
    dout(0) << "umounting" << dendl;
    sprintf(cmd,"umount %s", g_conf.filestore_dev);
    //system(cmd);
  }

  dout(1) << "mkfs done in " << basedir << dendl;

  return 0;
}

int FileStore::mount() 
{
  if (g_conf.filestore_dev) {
    dout(0) << "mounting" << dendl;
    char cmd[100];
    sprintf(cmd,"mount %s", g_conf.filestore_dev);
    //system(cmd);
  }

  dout(5) << "basedir " << basedir << dendl;
  
  // make sure global base dir exists
  struct stat st;
  int r = ::stat(basedir.c_str(), &st);
  if (r != 0) {
    derr(0) << "unable to stat basedir " << basedir << ", " << strerror(errno) << dendl;
    return -errno;
  }
  
  if (g_conf.filestore_fake_collections) {
    dout(0) << "faking collections (in memory)" << dendl;
    fake_collections = true;
  }

  // fake attrs? 
  // let's test to see if they work.
  if (g_conf.filestore_fake_attrs) {
    dout(0) << "faking attrs (in memory)" << dendl;
    fake_attrs = true;
  } else {
    int x = rand();
    int y = x+1;
    do_setxattr(basedir.c_str(), "test", &x, sizeof(x));
    do_getxattr(basedir.c_str(), "test", &y, sizeof(y));
    if (x != y) {
      derr(0) << "xattrs don't appear to work (" << strerror(errno) << "), specify --filestore_fake_attrs to fake them (in memory)." << dendl;
      return -errno;
    }
  }

  char fn[100];
  int fd;

#ifdef BTRFS_IOC_SYNC
  // is this btrfs?
  btrfs_fd = ::open(basedir.c_str(), O_DIRECTORY);
  r = ::ioctl(btrfs_fd, BTRFS_IOC_SYNC);
  if (r == 0) {
    dout(0) << "mount detected btrfs" << dendl;
  } else {
    dout(0) << "mount did NOT detect btrfs: " << strerror(-r) << dendl;
    ::close(btrfs_fd);
    btrfs_fd = -1;
  }
#endif

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

int FileStore::umount() 
{
  dout(5) << "umount " << basedir << dendl;
  
  sync();
  journal_stop();

  lock.Lock();
  stop = true;
  sync_cond.Signal();
  lock.Unlock();
  sync_thread.join();

  if (btrfs_fd >= 0) {
    ::close(btrfs_fd);
    btrfs_fd = -1;
  } 

  if (g_conf.filestore_dev) {
    char cmd[100];
    dout(0) << "umounting" << dendl;
    sprintf(cmd,"umount %s", g_conf.filestore_dev);
    //system(cmd);
  }

  // nothing
  return 0;
}


int FileStore::transaction_start()
{
  if (btrfs_fd < 0)
    return 0;

  int fd = ::open(basedir.c_str(), O_RDONLY);
  if (fd < 0) 
    derr(0) << "transaction_start got " << strerror(errno)
	    << " from btrfs open" << dendl;
  if (::ioctl(fd, BTRFS_IOC_TRANS_START) < 0) {
    derr(0) << "transaction_start got " << strerror(errno)
	    << " from btrfs ioctl" << dendl;    
    ::close(fd);
    return -errno;
  }
  dout(10) << "transaction_start " << fd << dendl;
  return fd;
}

void FileStore::transaction_end(int fd)
{
  if (btrfs_fd < 0)
    return;
  dout(10) << "transaction_end " << fd << dendl;
  ::close(fd);
}


// --------------------
// objects

bool FileStore::exists(coll_t cid, pobject_t oid)
{
  struct stat st;
  if (stat(cid, oid, &st) == 0)
    return true;
  else 
    return false;
}
  
int FileStore::stat(coll_t cid, pobject_t oid, struct stat *st)
{
  dout(20) << "stat " << cid << " " << oid << dendl;
  char fn[200];
  get_coname(cid, oid, fn);
  int r = ::stat(fn, st);
  dout(20) << "stat " << cid << " " << oid << " at " << fn << " = " << r << dendl;
  return r < 0 ? -errno:r;
}

 
int FileStore::remove(coll_t cid, pobject_t oid, Context *onsafe) 
{
  dout(20) << "remove " << cid << " " << oid << dendl;
  char fn[200];
  get_coname(cid, oid, fn);
  int r = ::unlink(fn);
  if (r == 0) 
    journal_remove(cid, oid, onsafe);
  else
    delete onsafe;
  return r < 0 ? -errno:r;
}

int FileStore::truncate(coll_t cid, pobject_t oid, off_t size, Context *onsafe)
{
  dout(20) << "truncate " << cid << " " << oid << " size " << size << dendl;

  char fn[200];
  get_coname(cid, oid, fn);
  int r = ::truncate(fn, size);
  if (r >= 0) journal_truncate(cid, oid, size, onsafe);
  return r < 0 ? -errno:r;
}

int FileStore::read(coll_t cid, pobject_t oid, 
                    off_t offset, size_t len,
                    bufferlist& bl) {
  dout(20) << "read " << cid << " " << oid << " len " << len << " off " << offset << dendl;

  char fn[200];
  get_coname(cid, oid, fn);
  
  int fd = ::open(fn, O_RDONLY);
  if (fd < 0) {
    dout(10) << "read couldn't open " << fn << " errno " << errno << " " << strerror(errno) << dendl;
    return -errno;
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


int FileStore::write(coll_t cid, pobject_t oid, 
                     off_t offset, size_t len,
                     const bufferlist& bl, 
                     Context *onsafe)
{
  char fn[200];
  get_coname(cid, oid, fn);

  dout(20) << "write " << fn << " len " << len << " off " << offset << dendl;

  int flags = O_WRONLY|O_CREAT;
  int fd = ::open(fn, flags, 0644);
  if (fd < 0) {
    derr(0) << "write couldn't open " << fn << " flags " << flags << " errno " << errno << " " << strerror(errno) << dendl;
    return -errno;
  }
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
    journal_write(cid, oid, offset, len, bl, onsafe);
  else
    delete onsafe;

  ::close(fd);
  
  return did;
}

int FileStore::clone(coll_t cid, pobject_t oldoid, pobject_t newoid)
{
  char ofn[200], nfn[200];
  get_coname(cid, oldoid, ofn);
  get_coname(cid, newoid, nfn);

  dout(20) << "clone " << ofn << " -> " << nfn << dendl;

  int o = ::open(ofn, O_RDONLY);
  if (o < 0)
      return -errno;
  int n = ::open(nfn, O_CREAT|O_TRUNC|O_WRONLY, 0644);
  if (n < 0)
      return -errno;
  int r = 0;
  if (btrfs_fd >= 0)
    r = ::ioctl(n, BTRFS_IOC_CLONE, o);
  else {
    struct stat st;
    ::fstat(o, &st);

#ifdef SPLICE_F_MOVE
    loff_t op = 0, np = 0;
    while (op < st.st_size && r >= 0)
      r = ::splice(o, &op, n, &np, st.st_size-op, 0);
#else
    loff_t pos = 0;
    int buflen = 4096*10;
    char buf[buflen];
    while (pos < st.st_size) {
      int l = MIN(st.st_size-pos, buflen);
      r = ::read(o, buf, l);
      if (r < 0)
	break;
      int op = 0;
      while (op < l) {
	int r2 = ::write(n, buf+op, l-op);
	
	if (r2 < 0) { r = r2; break; }
	op += r2;	  
      }
      if (r < 0) break;
      pos += r;
    }
#endif
  }
  if (r < 0)
    return -errno;

  ::close(n);
  ::close(o);
  return 0;
}


void FileStore::sync_entry()
{
  lock.Lock();
  utime_t interval;
  interval.set_from_double(g_conf.filestore_sync_interval);
  while (!stop) {
    dout(20) << "sync_entry waiting for " << interval << dendl;
    sync_cond.WaitInterval(lock, interval);
    lock.Unlock();

    dout(20) << "sync_entry committing " << super_epoch << " " << interval << dendl;
    commit_start();

    // induce an fs sync.
    // we assume data=ordered or similar semantics
    char fn[100];
    sprintf(fn, "%s/commit_epoch", basedir.c_str());
    int fd = ::open(fn, O_CREAT|O_WRONLY, 0644);
    ::write(fd, &super_epoch, sizeof(super_epoch));
    ::fsync(fd);  // this should cause the fs's journal to commit.  (on btrfs too.)
    ::close(fd);

    commit_finish();

    lock.Lock();
    dout(20) << "sync_entry committed " << super_epoch << dendl;
  }
  lock.Unlock();
}

void FileStore::sync()
{
  Mutex::Locker l(lock);
  sync_cond.Signal();
}

void FileStore::sync(Context *onsafe)
{
  journal_sync(onsafe);
  sync();
}


// -------------------------------
// attributes

// objects

int FileStore::setattr(coll_t cid, pobject_t oid, const char *name,
		       const void *value, size_t size,
		       Context *onsafe) 
{
  int r;
  if (fake_attrs) 
    r = attrs.setattr(cid, oid, name, value, size, onsafe);
  else {
    char fn[100];
    get_coname(cid, oid, fn);
    r = do_setxattr(fn, name, value, size);
    dout(10) << "setattr " << cid << " " << oid << " '" << name << "' size " << size << " = " << r << dendl;
 }
  if (r >= 0)
    journal_setattr(cid, oid, name, value, size, onsafe);
  else
    delete onsafe;
  return r < 0 ? -errno:r;
}

int FileStore::setattrs(coll_t cid, pobject_t oid, map<string,bufferptr>& aset) 
{
  int r;
  if (fake_attrs) 
    r = attrs.setattrs(cid, oid, aset);
  else {
    char fn[100];
    get_coname(cid, oid, fn);
    r = 0;
    for (map<string,bufferptr>::iterator p = aset.begin();
	 p != aset.end();
	 ++p) {
      r = do_setxattr(fn, p->first.c_str(), p->second.c_str(), p->second.length());
      if (r < 0) {
	cerr << "error setxattr " << strerror(errno) << std::endl;
	break;
      }
    }
  }
  if (r >= 0)
    journal_setattrs(cid, oid, aset, 0);
  return r < 0 ? -errno:r;
}

int FileStore::getattr(coll_t cid, pobject_t oid, const char *name,
		       void *value, size_t size) 
{
  int r;
  if (fake_attrs) 
    r = attrs.getattr(cid, oid, name, value, size);
  else {
    char fn[100];
    get_coname(cid, oid, fn);
    r = do_getxattr(fn, name, value, size);
    dout(10) << "getattr " << cid << " " << oid << " '" << name << "' size " << size << " = " << r << dendl;
  }
  return r < 0 ? -errno:r;
}

int FileStore::getattrs(coll_t cid, pobject_t oid, map<string,bufferptr>& aset) 
{
  int r;
  if (fake_attrs) 
    r = attrs.getattrs(cid, oid, aset);
  else {
    char fn[100];
    get_coname(cid, oid, fn);
    
    char val[1000];
    char names[1000];
    int num = do_listxattr(fn, names, 1000);
    
    char *name = names;
    for (int i=0; i<num; i++) {
      dout(0) << "getattrs " << oid << " getting " << (i+1) << "/" << num << " '" << names << "'" << dendl;
      int l = do_getxattr(fn, name, val, 1000);
      dout(0) << "getattrs " << oid << " getting " << (i+1) << "/" << num << " '" << names << "' = " << l << " bytes" << dendl;
      aset[names].append(val, l);
      name += strlen(name) + 1;
    }
  }
  return r < 0 ? -errno:r;
}

int FileStore::rmattr(coll_t cid, pobject_t oid, const char *name, Context *onsafe) 
{
  int r;
  if (fake_attrs) 
    r = attrs.rmattr(cid, oid, name, onsafe);
  else {
    char fn[100];
    get_coname(cid, oid, fn);
    r = do_removexattr(fn, name);
  }
  if (r >= 0)
    journal_rmattr(cid, oid, name, onsafe);
  else
    delete onsafe;
  return r < 0 ? -errno:r;
}



// collections
int FileStore::collection_setattr(coll_t c, const char *name,
				  const void *value, size_t size,
				  Context *onsafe) 
{
  int r;
  if (fake_attrs) 
    r = attrs.collection_setattr(c, name, value, size, onsafe);
  else {
    char fn[200];
    get_cdir(c, fn);
    r = do_setxattr(fn, name, value, size);
  }
  if (r >= 0)
    journal_collection_setattr(c, name, value, size, onsafe);
  else 
    delete onsafe;
  return r < 0 ? -errno:r;
}

int FileStore::collection_rmattr(coll_t c, const char *name,
				 Context *onsafe) 
{
  int r;
  if (fake_attrs) 
    r = attrs.collection_rmattr(c, name, onsafe);
  else {
    char fn[200];
    get_cdir(c, fn);
    r = do_removexattr(fn, name);
  }
  return r < 0 ? -errno:r;
}

int FileStore::collection_getattr(coll_t c, const char *name,
				  void *value, size_t size) 
{
  int r;
  if (fake_attrs) 
    r = attrs.collection_getattr(c, name, value, size);
  else {
    char fn[200];
    get_cdir(c, fn);
    r = do_getxattr(fn, name, value, size);   
  }
  return r < 0 ? -errno:r;
}

int FileStore::collection_setattrs(coll_t cid, map<string,bufferptr>& aset) 
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
      r = do_setxattr(fn, p->first.c_str(), p->second.c_str(), p->second.length());
      if (r < 0) break;
    }
  }
  if (r >= 0)
    journal_collection_setattrs(cid, aset, 0);
  return r < 0 ? -errno:r;
}

int FileStore::collection_getattrs(coll_t cid, map<string,bufferptr>& aset) 
{
  int r;
  if (fake_attrs) 
    r = attrs.collection_getattrs(cid, aset);
  else {
    char fn[100];
    get_cdir(cid, fn);
    
    char val[1000];
    char names[1000];
    int num = do_listxattr(fn, names, 1000);
    
    char *name = names;
    for (int i=0; i<num; i++) {
      dout(0) << "getattrs " << cid << " getting " << (i+1) << "/" << num << " '" << names << "'" << dendl;
      int l = do_getxattr(fn, name, val, 1000);
      dout(0) << "getattrs " << cid << " getting " << (i+1) << "/" << num << " '" << names << "' = " << l << " bytes" << dendl;
      aset[names].append(val, l);
      name += strlen(name) + 1;
    }
    r = 0;
  }
  return r < 0 ? -errno:r;
}


/*
int FileStore::collection_listattr(coll_t c, char *attrs, size_t size) 
{
  if (fake_attrs) return collection_listattr(c, attrs, size);
  return 0;
}
*/


// --------------------------
// collections

int FileStore::list_collections(list<coll_t>& ls) 
{
  if (fake_collections) return collections.list_collections(ls);

  char fn[200];
  sprintf(fn, "%s", basedir.c_str());

  DIR *dir = ::opendir(fn);
  assert(dir);

  struct dirent *de;
  while ((de = ::readdir(dir)) != 0) {
    // parse
    if (strlen(de->d_name) != 16)
      continue;
    errno = 0;
    coll_t c = parse_coll(de->d_name);
    if (c) ls.push_back(c);
  }
  
  ::closedir(dir);
  return 0;
}

int FileStore::create_collection(coll_t c,
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
  return r < 0 ? -errno:r;
}

int FileStore::destroy_collection(coll_t c,
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

int FileStore::collection_stat(coll_t c, struct stat *st) 
{
  if (fake_collections) return collections.collection_stat(c, st);

  char fn[200];
  get_cdir(c, fn);
  int r = ::lstat(fn, st);
  return r < 0 ? -errno:r;
}

bool FileStore::collection_exists(coll_t c) 
{
  if (fake_collections) return collections.collection_exists(c);

  struct stat st;
  return collection_stat(c, &st) == 0;
}


int FileStore::collection_add(coll_t c, coll_t cid, pobject_t o,
			      Context *onsafe) 
{
  int r;
  if (fake_collections) 
    r = collections.collection_add(c, o, onsafe);
  else {
    char cof[200];
    get_coname(c, o, cof);
    char of[200];
    get_coname(cid, o, of);
    r = ::link(of, cof);
  }
  if (r >= 0)
    journal_collection_add(c, cid, o, onsafe);
  else 
    delete onsafe;
  return r < 0 ? -errno:r;
}

int FileStore::collection_remove(coll_t c, pobject_t o,
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
  return r < 0 ? -errno:r;
}

int FileStore::collection_list(coll_t c, list<pobject_t>& ls) 
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
