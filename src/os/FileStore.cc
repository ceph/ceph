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

// alternate usertrans interface...
#define BTRFS_IOC_USERTRANS_OPEN   1
#define BTRFS_IOC_USERTRANS_CLOSE  2
#define BTRFS_IOC_USERTRANS_SEEK   3
#define BTRFS_IOC_USERTRANS_WRITE  5
#define BTRFS_IOC_USERTRANS_UNLINK 6
#define BTRFS_IOC_USERTRANS_MKDIR  7
#define BTRFS_IOC_USERTRANS_RMDIR  8
#define BTRFS_IOC_USERTRANS_TRUNCATE  9
#define BTRFS_IOC_USERTRANS_SETXATTR 10
#define BTRFS_IOC_USERTRANS_REMOVEXATTR  11
#define BTRFS_IOC_USERTRANS_CLONE 12

struct btrfs_ioctl_usertrans_op {
	__u64 op;
	__s64 args[5];
	__s64 rval;
};

struct btrfs_ioctl_usertrans {
	__u64 len;
	struct btrfs_ioctl_usertrans_op ops[0];
};

#define BTRFS_IOC_USERTRANS  _IOW(BTRFS_IOCTL_MAGIC, 13,	\
				  struct btrfs_ioctl_usertrans)

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



void get_attrname(const char *name, char *buf)
{
  sprintf(buf, "user.ceph.%s", name);
}
bool parse_attrname(char **name)
{
  if (strncmp(*name, "user.ceph.", 10) == 0) {
    *name += 10;
    return true;
  }
  return false;
}


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
	  oid.volume, oid.rank, oid.oid.ino, oid.oid.bno, oid.oid.snap);
#else
  sprintf(t, "/%04x.%04x.%016llx.%08x.%llx", 
	  oid.volume, oid.rank, oid.oid.ino, oid.oid.bno, oid.oid.snap);
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
  o.oid.snap = strtoll(s+36, 0, 16);
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

Mutex sig_lock;
Cond sig_cond;
bool sig_installed = false;
int sig_pending = 0;
int trans_running = 0;
struct sigaction safe_sigint;
struct sigaction old_sigint, old_sigterm;

void _handle_signal(int signal)
{
  cerr << "got signal " << signal << ", stopping" << std::endl;
  _exit(0);
}

void handle_signal(int signal, siginfo_t *info, void *p)
{
  //cout << "sigint signal " << signal << std::endl;
  int running;
  sig_lock.Lock();
  running = trans_running;
  sig_pending = signal;
  sig_lock.Unlock();
  if (!running)
    _handle_signal(signal);
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
    do_setxattr(basedir.c_str(), "user.test", &x, sizeof(x));
    do_getxattr(basedir.c_str(), "user.test", &y, sizeof(y));
    /*dout(10) << "x = " << x << "   y = " << y 
	     << "  r1 = " << r1 << "  r2 = " << r2
	     << " " << strerror(errno)
	     << dendl;*/
    if (x != y) {
      derr(0) << "xattrs don't appear to work (" << strerror(errno) << "), specify --filestore_fake_attrs to fake them (in memory)." << dendl;
      return -errno;
    }
  }

  char fn[100];
  int fd;

  // get fsid
  sprintf(fn, "%s/fsid", basedir.c_str());
  lock_fd = ::open(fn, O_RDONLY);
  ::read(lock_fd, &fsid, sizeof(fsid));

  // and lock it..
  /*
  struct flock l;
  memset(&l, 0, sizeof(l));
  l.l_type = F_WRLCK;
  l.l_whence = SEEK_SET;
  l.l_start = 0;
  l.l_len = 0;
  r = ::fcntl(lock_fd, F_SETLK, &l);
  if (r < 0) {
    derr(0) << "mount failed to lock " << fn << ", is another cosd still running? " << strerror(errno) << dendl;
    return -errno;
    }*/

  dout(10) << "mount fsid is " << fsid << dendl;

  // is this btrfs?
  Transaction empty;
  btrfs = true;
  btrfs_trans_start_end = true;  // trans start/end interface
  r = apply_transaction(empty, 0);
  if (r == 0) {
    dout(0) << "mount detected btrfs" << dendl;
  } else {
    dout(0) << "mount did NOT detect btrfs: " << strerror(-r) << dendl;
    btrfs = false;
  }

  // install signal handler for SIGINT, SIGTERM
  sig_lock.Lock();
  if (!sig_installed) {
    dout(10) << "mount installing signal handler to (somewhat) protect transactions" << dendl;
    sigset_t trans_sigmask;
    sigemptyset(&trans_sigmask);
    sigaddset(&trans_sigmask, SIGINT);
    sigaddset(&trans_sigmask, SIGTERM);

    memset(&safe_sigint, 0, sizeof(safe_sigint));
    safe_sigint.sa_sigaction = handle_signal;
    safe_sigint.sa_mask = trans_sigmask;
    sigaction(SIGTERM, &safe_sigint, &old_sigterm);
    sigaction(SIGINT, &safe_sigint, &old_sigint);
    sig_installed = true;
  }
  sig_lock.Unlock();

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

  ::close(lock_fd);

  if (g_conf.filestore_dev) {
    char cmd[100];
    dout(0) << "umounting" << dendl;
    sprintf(cmd,"umount %s", g_conf.filestore_dev);
    //system(cmd);
  }

  // nothing
  return 0;
}

// btrfs transaction start/end interface

int FileStore::transaction_start(int len)
{
  if (!btrfs || !btrfs_trans_start_end)
    return 0;

  int fd = ::open(basedir.c_str(), O_RDONLY);
  if (fd < 0) {
    derr(0) << "transaction_start got " << strerror(errno)
 	    << " from btrfs open" << dendl;
    assert(0);
  }
  if (::ioctl(fd, BTRFS_IOC_TRANS_START) < 0) {
    derr(0) << "transaction_start got " << strerror(errno)
 	    << " from btrfs ioctl" << dendl;    
    ::close(fd);
    return -errno;
  }
  dout(10) << "transaction_start " << fd << dendl;

  sig_lock.Lock();
 retry:
  if (trans_running && sig_pending) {
    dout(-10) << "transaction_start signal " << sig_pending << " pending" << dendl;
    sig_cond.Wait(sig_lock);
    goto retry;
  }
  trans_running++;
  sig_lock.Unlock();

  char fn[80];
  sprintf(fn, "%s/trans.%d", basedir.c_str(), fd);
  ::mknod(fn, 0644, 0);

  return fd;
}

void FileStore::transaction_end(int fd)
{
  if (!btrfs || !btrfs_trans_start_end)
    return;

  char fn[80];
  sprintf(fn, "%s/trans.%d", basedir.c_str(), fd);
  ::unlink(fn);
  
  dout(10) << "transaction_end " << fd << dendl;
  ::close(fd);

  sig_lock.Lock();
  trans_running--;
  if (trans_running == 0 && sig_pending) {
    dout(-10) << "transaction_end signal " << sig_pending << " pending" << dendl;
    _handle_signal(sig_pending);
  }
  sig_lock.Unlock();
}

unsigned FileStore::apply_transaction(Transaction &t, Context *onsafe)
{
  // no btrfs transaction support?
  // or, use trans start/end ioctls?
  if (!btrfs || btrfs_trans_start_end)
    return ObjectStore::apply_transaction(t, onsafe);

  // create transaction
  int len = t.get_btrfs_len();
  dout(20) << "apply_transaction allocation btrfs usertrans len " << len << dendl;
  btrfs_ioctl_usertrans *trans =
    (btrfs_ioctl_usertrans *)new char[sizeof(*trans) + len * sizeof(trans->ops[0])];

  trans->len = 0;

  list<char *> str;

  while (t.have_op()) {
    int op = t.get_op();

    switch (op) {

    case Transaction::OP_WRITE:
    case Transaction::OP_ZERO:   // write actual zeros.
      {
	coll_t cid;
	t.get_cid(cid);
	pobject_t oid;
	t.get_oid(oid);
	__u64 offset, len;
	t.get_length(offset);
	t.get_length(len);
	bufferlist bl;
	if (op == Transaction::OP_WRITE)
	  t.get_bl(bl);
	else {
	  bufferptr bp(len);
	  bp.zero();
	  bl.push_back(bp);
	}
	
	dout(10) << "write" << dendl;
	//write(cid, oid, offset, len, bl, 0);
	char *fn = new char[80];
	str.push_back(fn);
	get_coname(cid, oid, fn);
	trans->ops[trans->len].op = BTRFS_IOC_USERTRANS_OPEN;
	trans->ops[trans->len].args[0] = (__s64)fn;
	trans->ops[trans->len].args[1] = O_WRONLY|O_CREAT;
	trans->len++;
	trans->ops[trans->len].op = BTRFS_IOC_USERTRANS_SEEK;
	trans->ops[trans->len].args[0] = -1;
	trans->ops[trans->len].args[1] = offset;
	trans->ops[trans->len].args[2] = (__s64)&trans->ops[trans->len].args[4];  // whatever.
	trans->ops[trans->len].args[3] = SEEK_SET;
	trans->len++;
	for (list<bufferptr>::const_iterator it = bl.buffers().begin();
	     it != bl.buffers().end();
	     it++) {
	  trans->ops[trans->len].op = BTRFS_IOC_USERTRANS_WRITE;
	  trans->ops[trans->len].args[0] = -1;
	  trans->ops[trans->len].args[1] = (__s64)(*it).c_str();
	  trans->ops[trans->len].args[2] = (__s64)(*it).length();
	  trans->len++;
	}
	trans->ops[trans->len].op = BTRFS_IOC_USERTRANS_CLOSE;
	trans->ops[trans->len].args[0] = -1;
	trans->len++;
      }
      break;
      
    case Transaction::OP_TRIMCACHE:
      {
	coll_t cid;
	t.get_cid(cid);
	pobject_t oid;
	t.get_oid(oid);
	__u64 offset, len;
	t.get_length(offset);
	t.get_length(len);
	trim_from_cache(cid, oid, offset, len);
      }
      break;
      
    case Transaction::OP_TRUNCATE:
      {
	coll_t cid;
	t.get_cid(cid);
	pobject_t oid;
	t.get_oid(oid);
	__u64 len;
	t.get_length(len);
	//truncate(cid, oid, len, 0);
	
	dout(10) << "truncate" << dendl;
	char *fn = new char[80];
	str.push_back(fn);
	get_coname(cid, oid, fn);
	trans->ops[trans->len].op = BTRFS_IOC_USERTRANS_TRUNCATE;
	trans->ops[trans->len].args[0] = (__s64)fn;
	trans->ops[trans->len].args[1] = len;
	trans->len++;
      }
      break;
      
    case Transaction::OP_REMOVE:
      {
	coll_t cid;
	t.get_cid(cid);
	pobject_t oid;
	t.get_oid(oid);
	//remove(cid, oid, 0);
	
	dout(10) << "remove " << cid << " " << oid << dendl;
	char *fn = new char[80];
	str.push_back(fn);
	get_coname(cid, oid, fn);
	trans->ops[trans->len].op = BTRFS_IOC_USERTRANS_UNLINK;
	trans->ops[trans->len].args[0] = (__u64)fn;
	trans->len++;
      }
      break;
      
    case Transaction::OP_SETATTR:
      {
	coll_t cid;
	t.get_cid(cid);
	pobject_t oid;
	t.get_oid(oid);
	const char *attrname;
	t.get_attrname(attrname);
	bufferlist bl;
	t.get_bl(bl);
	//setattr(cid, oid, attrname, bl.c_str(), bl.length(), 0);
	dout(10) << "setattr " << cid << " " << oid << dendl;
	char *fn = new char[80];
	str.push_back(fn);
	get_coname(cid, oid, fn);
	trans->ops[trans->len].op = BTRFS_IOC_USERTRANS_SETXATTR;
	trans->ops[trans->len].args[0] = (__u64)fn;
	char aname[40];
	sprintf(aname, "user.ceph.%s", attrname);
	trans->ops[trans->len].args[1] = (__u64)aname;
	trans->ops[trans->len].args[2] = (__u64)bl.c_str();
	trans->ops[trans->len].args[3] = bl.length();
	trans->ops[trans->len].args[4] = 0;	  
	trans->len++;
      }
      break;


    case Transaction::OP_SETATTRS:
    case Transaction::OP_COLL_SETATTRS:
      {
	// make note of old attrs
	map<string,bufferptr> oldattrs;
	char *fn = new char[80];
	str.push_back(fn);

	if (op == Transaction::OP_SETATTRS) {
	  coll_t cid;
	  t.get_cid(cid);
	  pobject_t oid;
	  t.get_oid(oid);
	  getattrs(cid, oid, oldattrs);
	  get_coname(cid, oid, fn);
	} else {
	  coll_t cid;
	  t.get_cid(cid);
	  collection_getattrs(cid, oldattrs);
	  get_cdir(cid, fn);
	}
	map<string,bufferptr> *pattrset;
	t.get_pattrset(pattrset);
	
	for (map<string,bufferptr>::iterator p = pattrset->begin();
	     p != pattrset->end();
	     p++) {
	  trans->ops[trans->len].op = BTRFS_IOC_USERTRANS_SETXATTR;
	  trans->ops[trans->len].args[0] = (__u64)fn;
	  char *aname = new char[40];
	  str.push_back(aname);
	  sprintf(aname, "user.ceph.%s", p->first.c_str());
	  trans->ops[trans->len].args[1] = (__u64)aname;
	  trans->ops[trans->len].args[2] = (__u64)p->second.c_str();
	  trans->ops[trans->len].args[3] = p->second.length();
	  trans->ops[trans->len].args[4] = 0;	  
	  trans->len++;
	  oldattrs.erase(p->first);
	}
	
	// and remove any leftovers
	for (map<string,bufferptr>::iterator p = oldattrs.begin();
	     p != oldattrs.end();
	     p++) {
	  trans->ops[trans->len].op = BTRFS_IOC_USERTRANS_REMOVEXATTR;
	  trans->ops[trans->len].args[0] = (__u64)fn;
	  trans->ops[trans->len].args[1] = (__u64)p->first.c_str();
	  trans->len++;
	}
      }
      break;
      
    case Transaction::OP_RMATTR:
      {
	coll_t cid;
	t.get_cid(cid);
	pobject_t oid;
	t.get_oid(oid);
	const char *attrname;
	t.get_attrname(attrname);
	//rmattr(cid, oid, attrname, 0);
	
	dout(10) << "rmattr " << cid << " " << oid << dendl;
	char *fn = new char[80];
	str.push_back(fn);
	get_coname(cid, oid, fn);
	trans->ops[trans->len].op = BTRFS_IOC_USERTRANS_REMOVEXATTR;
	trans->ops[trans->len].args[0] = (__u64)fn;
	trans->ops[trans->len].args[1] = (__u64)attrname;
	trans->len++;
      }
      break;
      
    case Transaction::OP_CLONE:
      {
	coll_t cid;
	t.get_cid(cid);
	pobject_t oid;
	t.get_oid(oid);
	pobject_t noid;
	t.get_oid(noid);
	//clone(cid, oid, noid);
	
	dout(10) << "clone " << cid << " " << oid << dendl;
	char *ofn = new char[80];
	str.push_back(ofn);
	char *nfn = new char[80];
	str.push_back(nfn);
	get_coname(cid, oid, ofn);
	get_coname(cid, noid, nfn);
	
	trans->ops[trans->len].op = BTRFS_IOC_USERTRANS_OPEN;
	trans->ops[trans->len].args[0] = (__u64)nfn;
	trans->ops[trans->len].args[1] = O_WRONLY;
	trans->len++;
	trans->ops[trans->len].op = BTRFS_IOC_USERTRANS_OPEN;
	trans->ops[trans->len].args[0] = (__u64)ofn;
	trans->ops[trans->len].args[1] = O_RDONLY;
	trans->len++;
	trans->ops[trans->len].op = BTRFS_IOC_USERTRANS_CLONE;
	trans->ops[trans->len].args[0] = -2;
	trans->ops[trans->len].args[1] = -1;
	trans->len++;
	trans->ops[trans->len].op = BTRFS_IOC_USERTRANS_CLOSE;
	trans->ops[trans->len].args[0] = -1;
	trans->len++;
	trans->ops[trans->len].op = BTRFS_IOC_USERTRANS_CLOSE;
	trans->ops[trans->len].args[0] = -2;
	trans->len++;
      }
      break;
      
    case Transaction::OP_MKCOLL:
      {
	coll_t cid;
	t.get_cid(cid);
	//create_collection(cid, 0);
	dout(10) << "mkcoll " << cid << dendl;
	char *fn = new char[80];
	str.push_back(fn);
	get_cdir(cid, fn);
	trans->ops[trans->len].op = BTRFS_IOC_USERTRANS_MKDIR;
	trans->ops[trans->len].args[0] = (__u64)fn;
	trans->ops[trans->len].args[1] = 0644;
 	trans->len++;
     }
      break;
      
    case Transaction::OP_RMCOLL:
      {
	coll_t cid;
	t.get_cid(cid);
	//destroy_collection(cid, 0);
	dout(10) << "rmcoll " << cid << dendl;
	char *fn = new char[80];
	str.push_back(fn);
	get_cdir(cid, fn);
	trans->ops[trans->len].op = BTRFS_IOC_USERTRANS_RMDIR;
	trans->ops[trans->len].args[0] = (__u64)fn;
	trans->ops[trans->len].args[1] = 0644;
	trans->len++;
      }
      break;
      
    case Transaction::OP_COLL_ADD:
      {
	coll_t cid, ocid;
	t.get_cid(cid);
	t.get_cid(ocid);
	pobject_t oid;
	t.get_oid(oid);
	collection_add(cid, ocid, oid, 0);
	assert(0);
      }
      break;
      
    case Transaction::OP_COLL_REMOVE:
      {
	coll_t cid;
	t.get_cid(cid);
	pobject_t oid;
	t.get_oid(oid);
	collection_remove(cid, oid, 0);
	assert(0);
      }
      break;
      
    case Transaction::OP_COLL_SETATTR:
      {
	coll_t cid;
	t.get_cid(cid);
	const char *attrname;
	t.get_attrname(attrname);
	bufferlist bl;
	t.get_bl(bl);
	dout(10) << "coll_setattr " << cid << dendl;
	//collection_setattr(cid, attrname, bl.c_str(), bl.length(), 0);
	char *fn = new char[80];
	str.push_back(fn);
	get_cdir(cid, fn);
	trans->ops[trans->len].op = BTRFS_IOC_USERTRANS_SETXATTR;
	trans->ops[trans->len].args[0] = (__u64)fn;
	char aname[40];
	sprintf(aname, "user.ceph.%s", attrname);
	trans->ops[trans->len].args[1] = (__u64)aname;
	trans->ops[trans->len].args[2] = (__u64)bl.c_str();
	trans->ops[trans->len].args[3] = bl.length();
	trans->ops[trans->len].args[4] = 0;	  
 	trans->len++;
     }
      break;
      
    case Transaction::OP_COLL_RMATTR:
      {
	coll_t cid;
	t.get_cid(cid);
	const char *attrname;
	t.get_attrname(attrname);
	dout(10) << "coll_rmattr " << cid << dendl;
	//collection_rmattr(cid, attrname, 0);
	char *fn = new char[80];
	str.push_back(fn);
	get_cdir(cid, fn);
	trans->ops[trans->len].op = BTRFS_IOC_USERTRANS_REMOVEXATTR;
	trans->ops[trans->len].args[0] = (__u64)fn;
	trans->ops[trans->len].args[1] = (__u64)attrname;
 	trans->len++;
      }
      break;

      
    default:
      cerr << "bad op " << op << std::endl;
      assert(0);
    }
  }  

  dout(20) << "apply_transaction final btrfs usertrans len is " << trans->len << dendl;
  assert((int)trans->len <= (int)len);

  // apply
  int r = 0;
  if (trans->len) {
    r = ::ioctl(lock_fd, BTRFS_IOC_USERTRANS, (unsigned long)trans);
    if (r < 0) {
      derr(0) << "apply_transaction_end got " << strerror(errno)
	      << " from btrfs usertrans ioctl" << dendl;    
      r = -errno;
    } 
  }
  delete[] (char *)trans;

  while (!str.empty()) {
    delete[] str.front();
    str.pop_front();
  }

  if (r >= 0)
    journal_transaction(t, onsafe);
  else
    delete onsafe;

  return r;
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

int FileStore::truncate(coll_t cid, pobject_t oid, __u64 size, Context *onsafe)
{
  dout(20) << "truncate " << cid << " " << oid << " size " << size << dendl;

  char fn[200];
  get_coname(cid, oid, fn);
  int r = ::truncate(fn, size);
  if (r >= 0) journal_truncate(cid, oid, size, onsafe);
  return r < 0 ? -errno:r;
}

int FileStore::read(coll_t cid, pobject_t oid, 
                    __u64 offset, size_t len,
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
  
  __u64 actual = lseek(fd, offset, SEEK_SET);
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
                     __u64 offset, size_t len,
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
  __u64 actual = ::lseek(fd, offset, SEEK_SET);
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
  if (btrfs)
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
    char n[40];
    get_attrname(name, n);
    r = do_setxattr(fn, n, value, size);
    dout(10) << "setattr " << cid << " " << oid << " '" << name << "' size " << size << " = " << r << dendl;
 }
  if (r >= 0)
    journal_setattr(cid, oid, name, value, size, onsafe);
  else
    delete onsafe;
  return r < 0 ? -errno:r;
}

int FileStore::setattrs(coll_t cid, pobject_t oid, map<string,bufferptr>& aset, Context *onsafe) 
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
      char n[40];
      get_attrname(p->first.c_str(), n);
      r = do_setxattr(fn, n, p->second.c_str(), p->second.length());
      if (r < 0) {
	cerr << "error setxattr " << strerror(errno) << std::endl;
	break;
      }
    }
  }
  if (r >= 0)
    journal_setattrs(cid, oid, aset, onsafe);
  else
    delete onsafe;
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
    char n[40];
    get_attrname(name, n);
    r = do_getxattr(fn, n, value, size);
    dout(10) << "getattr " << cid << " " << oid << " '" << name << "' size " << size << " = " << r << dendl;
  }
  return r < 0 ? -errno:r;
}

int FileStore::getattr(coll_t cid, pobject_t oid, const char *name, bufferptr &bp)
{
  int r;
  if (fake_attrs) 
    r = attrs.getattr(cid, oid, name, bp);
  else {
    char fn[100];
    get_coname(cid, oid, fn);
    char n[40];
    get_attrname(name, n);
    r = do_getxattr(fn, n, 0, 0);
    if (r > 0) {
      bp = buffer::create(r);
      r = do_getxattr(fn, n, bp.c_str(), r);
      if (r > 0) bp.set_length(r);
    }
    dout(10) << "getattr " << cid << " " << oid << " '" << name << "' size " << bp.length() << " = " << r << dendl;
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
      if (parse_attrname(&name)) {
	aset[name] = buffer::create(l);
	memcpy(aset[name].c_str(), val, l);
      }
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
    char n[40];
    get_attrname(name, n);
    r = do_removexattr(fn, n);
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
      if (parse_attrname(&name)) {
	aset[name] = buffer::create(l);
	memcpy(aset[name].c_str(), val, l);
      }
      name += strlen(name) + 1;
    }
    r = 0;
  }
  return r < 0 ? -errno:r;
}



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
