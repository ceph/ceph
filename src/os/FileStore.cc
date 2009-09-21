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


#define ATTR_MAX 80

#ifndef __CYGWIN__
#ifndef DARWIN
# include <linux/ioctl.h>
# define BTRFS_IOCTL_MAGIC 0x94
# define BTRFS_IOC_TRANS_START  _IO(BTRFS_IOCTL_MAGIC, 6)
# define BTRFS_IOC_TRANS_END    _IO(BTRFS_IOCTL_MAGIC, 7)
# define BTRFS_IOC_SYNC         _IO(BTRFS_IOCTL_MAGIC, 8)
# define BTRFS_IOC_CLONE        _IOW(BTRFS_IOCTL_MAGIC, 9, int)
#define BTRFS_IOC_WAIT_FOR_SYNC _IO(BTRFS_IOCTL_MAGIC, 5)
struct btrfs_ioctl_clone_range_args {
  __s64 src_fd;
  __u64 src_offset, src_length;
  __u64 dest_offset;
};

#define BTRFS_IOC_CLONE_RANGE _IOW(BTRFS_IOCTL_MAGIC, 13, \
				  struct btrfs_ioctl_clone_range_args)

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

#define DOUT_SUBSYS filestore
#undef dout_prefix
#define dout_prefix *_dout << dbeginl << pthread_self() << " filestore(" << basedir << ") "

#include "include/buffer.h"

#include <map>


/*
 * xattr portability stupidity.  hide errno, while we're at it.
 */ 

int do_getxattr(const char *fn, const char *name, void *val, size_t size) {
#ifdef DARWIN
  int r = ::getxattr(fn, name, val, size, 0, 0);
#else
  int r = ::getxattr(fn, name, val, size);
#endif
  return r < 0 ? -errno:r;
}
int do_setxattr(const char *fn, const char *name, const void *val, size_t size) {
#ifdef DARWIN
  int r = ::setxattr(fn, name, val, size, 0, 0);
#else
  int r = ::setxattr(fn, name, val, size, 0);
#endif
  return r < 0 ? -errno:r;
}
int do_removexattr(const char *fn, const char *name) {
#ifdef DARWIN
  int r = ::removexattr(fn, name, 0);
#else
  int r = ::removexattr(fn, name);
#endif
  return r < 0 ? -errno:r;
}
int do_listxattr(const char *fn, char *names, size_t len) {
#ifdef DARWIN
  int r = ::listxattr(fn, names, len, 0);
#else
  int r = ::listxattr(fn, names, len);
#endif
  return r < 0 ? -errno:r;
}


// .............


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
 * sorry, these are sentitive to the sobject_t and coll_t typing.
 */ 

  //           11111111112222222222333333333344444444445555555555
  // 012345678901234567890123456789012345678901234567890123456789
  // yyyyyyyyyyyyyyyy.zzzzzzzz.a_s

void FileStore::append_oname(const sobject_t &oid, char *s)
{
  //assert(sizeof(oid) == 28);
  char *t = s + strlen(s);

  *t++ = '/';
  char *i = oid.oid.name.c_str();
  while (*i) {
    if (*i == '\\') {
      *t++ = '\\';
      *t++ = '\\';      
    } else if (*i == '.' && i == oid.oid.name.c_str()) {  // only escape leading .
      *t++ = '\\';
      *t++ = '.';
    } else if (*i == '/') {
      *t++ = '\\';
      *t++ = 's';
    } else
      *t++ = *i;
    i++;
  }

  if (oid.snap == CEPH_NOSNAP)
    sprintf(t, "_head");
  else if (oid.snap == CEPH_SNAPDIR)
    sprintf(t, "_snapdir");
  else
    sprintf(t, "_%llx", (long long unsigned)oid.snap);
  //parse_object(t+1);
}

bool FileStore::parse_object(char *s, sobject_t& o)
{
  char *bar = s + strlen(s) - 1;
  while (*bar != '_' &&
	 bar > s)
    bar--;
  if (*bar == '_') {
    char buf[bar-s + 1];
    char *t = buf;
    char *i = s;
    while (i < bar) {
      if (*i == '\\') {
	i++;
	switch (*i) {
	case '\\': *t++ = '\\'; break;
	case '.': *t++ = '.'; break;
	case 's': *t++ = '/'; break;
	default: assert(0);
	}
      } else {
	*t++ = *i;
      }
      i++;
    }
    *t = 0;
    o.oid.name = nstring(t-buf, buf);
    if (strcmp(bar+1, "head") == 0)
      o.snap = CEPH_NOSNAP;
    else if (strcmp(bar+1, "snapdir") == 0)
      o.snap = CEPH_SNAPDIR;
    else
      o.snap = strtoull(bar+1, &s, 16);
    return true;
  }
  return false;
}

  //           11111111112222222222333
  // 012345678901234567890123456789012
  // pppppppppppppppp.ssssssssssssssss

bool FileStore::parse_coll(char *s, coll_t& c)
{
  if (strlen(s) == 33 && s[16] == '.') {
    s[16] = 0;
    c.pgid = strtoull(s, 0, 16);
    c.snap = strtoull(s+17, 0, 16);
    return true;
  } else 
    return false;
}

void FileStore::get_cdir(coll_t cid, char *s) 
{
  assert(sizeof(cid) == 16);
#ifdef __LP64__
  sprintf(s, "%s/%016lx.%016lx", basedir.c_str(), cid.pgid, (__u64)cid.snap);
#else
  sprintf(s, "%s/%016llx.%016llx", basedir.c_str(), cid.pgid, (__u64)cid.snap);
#endif
}

void FileStore::get_coname(coll_t cid, const sobject_t& oid, char *s) 
{
  get_cdir(cid, s);
  append_oname(oid, s);
}

int FileStore::open_journal()
{
  struct stat st;
  char fn[PATH_MAX];

  if (journalpath.length() == 0) {
    sprintf(fn, "%s.journal", basedir.c_str());
    if (::stat(fn, &st) == 0)
      journalpath = fn;
  }
  if (journalpath.length()) {
    dout(10) << "open_journal at " << journalpath << dendl;
    journal = new FileJournal(fsid, &finisher, &sync_cond, journalpath.c_str(), g_conf.journal_dio);
  }
  return 0;
}

int FileStore::mkfs()
{
  char cmd[PATH_MAX];
  if (g_conf.filestore_dev) {
    dout(0) << "mounting" << dendl;
    sprintf(cmd,"mount %s", g_conf.filestore_dev);
    system(cmd);
  }

  dout(1) << "mkfs in " << basedir << dendl;

  char fn[PATH_MAX];
  sprintf(fn, "%s/fsid", basedir.c_str());
  fsid_fd = ::open(fn, O_CREAT|O_RDWR, 0644);
  if (lock_fsid() < 0)
    return -EBUSY;

  // wipe
  sprintf(cmd, "test -d %s && rm -r %s/* ; mkdir -p %s",
	  basedir.c_str(), basedir.c_str(), basedir.c_str());
  
  dout(5) << "wipe: " << cmd << dendl;
  system(cmd);

  // fsid
  srand(time(0) + getpid());
  fsid = rand();

  ::close(fsid_fd);
  fsid_fd = ::open(fn, O_CREAT|O_RDWR, 0644);
  if (lock_fsid() < 0)
    return -EBUSY;
  ::write(fsid_fd, &fsid, sizeof(fsid));
  ::close(fsid_fd);
  dout(10) << "mkfs fsid is " << fsid << dendl;

  // journal?
  open_journal();
  if (journal) {
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
    char cmd[PATH_MAX];
    dout(0) << "umounting" << dendl;
    sprintf(cmd,"umount %s", g_conf.filestore_dev);
    //system(cmd);
  }

  dout(1) << "mkfs done in " << basedir << dendl;

  return 0;
}

Mutex sig_lock("FileStore.cc sig_lock");
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

int FileStore::lock_fsid()
{
  struct flock l;
  memset(&l, 0, sizeof(l));
  l.l_type = F_WRLCK;
  l.l_whence = SEEK_SET;
  l.l_start = 0;
  l.l_len = 0;
  int r = ::fcntl(fsid_fd, F_SETLK, &l);
  if (r < 0) {
    char buf[80];
    derr(0) << "mount failed to lock " << basedir << "/fsid, is another cosd still running? " << strerror_r(errno, buf, sizeof(buf)) << dendl;
    return -errno;
  }
  return 0;
}

int FileStore::mount() 
{
  char buf[80];

  if (g_conf.filestore_dev) {
    dout(0) << "mounting" << dendl;
    char cmd[100];
    sprintf(cmd,"mount %s", g_conf.filestore_dev);
    //system(cmd);
  }

  dout(5) << "basedir " << basedir << " journal " << journalpath << dendl;
  
  // make sure global base dir exists
  struct stat st;
  int r = ::stat(basedir.c_str(), &st);
  if (r != 0) {
    derr(0) << "unable to stat basedir " << basedir << ", " << strerror_r(errno, buf, sizeof(buf)) << dendl;
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
      derr(0) << "xattrs don't appear to work (" << strerror_r(errno, buf, sizeof(buf)) << ") on " << basedir << ", be sure to mount underlying file system with 'user_xattr' option" << dendl;
      return -errno;
    }
  }

  char fn[PATH_MAX];

  // get fsid
  sprintf(fn, "%s/fsid", basedir.c_str());
  fsid_fd = ::open(fn, O_RDWR|O_CREAT, 0644);
  ::read(fsid_fd, &fsid, sizeof(fsid));

  if (lock_fsid() < 0)
    return -EBUSY;

  dout(10) << "mount fsid is " << fsid << dendl;

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
  sprintf(fn, "%s/commit_op_seq", basedir.c_str());
  op_fd = ::open(fn, O_CREAT|O_RDWR, 0644);
  assert(op_fd >= 0);
  op_seq = 0;
  ::read(op_fd, &op_seq, sizeof(op_seq));

  dout(5) << "mount op_seq is " << op_seq << dendl;

  // journal
  open_journal();
  r = journal_replay();
  if (r == -EINVAL) {
    dout(0) << "mount got EINVAL on journal open, not mounting" << dendl;
    return r;
  }
  journal_start();
  sync_thread.create();


  // is this btrfs?
  Transaction empty;
  btrfs = 1;
  btrfs_trans_start_end = true;  // trans start/end interface
  r = apply_transaction(empty, 0);
  if (r == 0) {
    // do we have the shiny new CLONE_RANGE ioctl?
    btrfs = 2;
    int r = _do_clone_range(fsid_fd, -1, 0, 1);
    if (r == -EBADF) {
      dout(0) << "mount detected shiny new btrfs" << dendl;      
    } else {
      dout(0) << "mount detected dingey old btrfs (r=" << r << " " << strerror_r(-r, buf, sizeof(buf)) << ")" << dendl;
      btrfs = 1;
    }
  } else {
    dout(0) << "mount did NOT detect btrfs: " << strerror_r(-r, buf, sizeof(buf)) << dendl;
    btrfs = 0;
  }

  // all okay.
  return 0;
}

int FileStore::umount() 
{
  dout(5) << "umount " << basedir << dendl;
  
  sync();

  lock.Lock();
  stop = true;
  sync_cond.Signal();
  lock.Unlock();
  sync_thread.join();

  journal_stop();

  ::close(fsid_fd);
  ::close(op_fd);

  if (g_conf.filestore_dev) {
    char cmd[PATH_MAX];
    dout(0) << "umounting" << dendl;
    sprintf(cmd,"umount %s", g_conf.filestore_dev);
    //system(cmd);
  }

  // nothing
  return 0;
}




unsigned FileStore::apply_transaction(Transaction &t,
				      Context *onjournal,
				      Context *ondisk)
{
  op_start();

  // non-atomic implementation
  int id = _transaction_start(0);// t.get_trans_len());
  if (id < 0) {
    op_journal_start();
    op_finish();
    return id;
  }

  int r = _apply_transaction(t);

  _transaction_finish(id);

  op_journal_start();
  dout(10) << "op_seq is " << op_seq << dendl;
  if (r >= 0) {
    journal_transaction(t, onjournal, ondisk);

    ::pwrite(op_fd, &op_seq, sizeof(op_seq), 0);

  } else {
    delete onjournal;
    delete ondisk;
  }

  op_finish();
  return r;
}

unsigned FileStore::apply_transactions(list<Transaction*> &tls,
				       Context *onjournal,
				       Context *ondisk)
{
  op_start();

  int id = _transaction_start(0);// t.get_trans_len());
  if (id < 0) {
    op_journal_start();
    op_finish();
    return id;
  }

  int r = 0;
  for (list<Transaction*>::iterator p = tls.begin();
       p != tls.end();
       p++) {
    r = _apply_transaction(**p);
    if (r < 0)
      break;
  }

  _transaction_finish(id);

  op_journal_start();
  dout(10) << "op_seq is " << op_seq << dendl;
  if (r >= 0) {
    journal_transactions(tls, onjournal, ondisk);

    ::pwrite(op_fd, &op_seq, sizeof(op_seq), 0);

  } else {
    delete onjournal;
    delete ondisk;
  }

  op_finish();
  return r;
}


// btrfs transaction start/end interface

int FileStore::_transaction_start(int len)
{
#ifdef DARWIN
  return 0;
#else
  if (!btrfs || !btrfs_trans_start_end ||
      !g_conf.filestore_btrfs_trans)
    return 0;

  char buf[80];
  int fd = ::open(basedir.c_str(), O_RDONLY);
  if (fd < 0) {
    derr(0) << "transaction_start got " << strerror_r(errno, buf, sizeof(buf))
 	    << " from btrfs open" << dendl;
    assert(0);
  }
  if (::ioctl(fd, BTRFS_IOC_TRANS_START) < 0) {
    derr(0) << "transaction_start got " << strerror_r(errno, buf, sizeof(buf))
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

  char fn[PATH_MAX];
  sprintf(fn, "%s/trans.%d", basedir.c_str(), fd);
  ::mknod(fn, 0644, 0);

  return fd;
#endif /* DARWIN */
}

void FileStore::_transaction_finish(int fd)
{
#ifdef DARWIN
  return;
#else
  if (!btrfs || !btrfs_trans_start_end ||
      !g_conf.filestore_btrfs_trans)
    return;

  char fn[PATH_MAX];
  sprintf(fn, "%s/trans.%d", basedir.c_str(), fd);
  ::unlink(fn);
  
  dout(10) << "transaction_finish " << fd << dendl;
  ::close(fd);

  sig_lock.Lock();
  trans_running--;
  if (trans_running == 0 && sig_pending) {
    dout(-10) << "transaction_finish signal " << sig_pending << " pending" << dendl;
    _handle_signal(sig_pending);
  }
  sig_lock.Unlock();
#endif /* DARWIN */
}

unsigned FileStore::_apply_transaction(Transaction& t)
{
  dout(10) << "_apply_transaction on " << &t << dendl;

  while (t.have_op()) {
    int op = t.get_op();
    switch (op) {
    case Transaction::OP_TOUCH:
      _touch(t.get_cid(), t.get_oid());
      break;
      
    case Transaction::OP_WRITE:
      {
	__u64 off = t.get_length();
	__u64 len = t.get_length();
	_write(t.get_cid(), t.get_oid(), off, len, t.get_bl());
      }
      break;
      
    case Transaction::OP_ZERO:
      {
	__u64 off = t.get_length();
	__u64 len = t.get_length();
	_zero(t.get_cid(), t.get_oid(), off, len);
      }
      break;
      
    case Transaction::OP_TRIMCACHE:
      {
	__u64 off = t.get_length();
	__u64 len = t.get_length();
	trim_from_cache(t.get_cid(), t.get_oid(), off, len);
      }
      break;
      
    case Transaction::OP_TRUNCATE:
      _truncate(t.get_cid(), t.get_oid(), t.get_length());
      break;
      
    case Transaction::OP_REMOVE:
      _remove(t.get_cid(), t.get_oid());
      break;
      
    case Transaction::OP_SETATTR:
      {
	bufferlist& bl = t.get_bl();
	_setattr(t.get_cid(), t.get_oid(), t.get_attrname(), bl.c_str(), bl.length());
      }
      break;
      
    case Transaction::OP_SETATTRS:
      _setattrs(t.get_cid(), t.get_oid(), t.get_attrset());
      break;

    case Transaction::OP_RMATTR:
      _rmattr(t.get_cid(), t.get_oid(), t.get_attrname());
      break;

    case Transaction::OP_RMATTRS:
      _rmattrs(t.get_cid(), t.get_oid());
      break;
      
    case Transaction::OP_CLONE:
      {
	const sobject_t& oid = t.get_oid();
	const sobject_t& noid = t.get_oid();
	_clone(t.get_cid(), oid, noid);
      }
      break;

    case Transaction::OP_CLONERANGE:
      {
	const sobject_t& oid = t.get_oid();
	const sobject_t& noid = t.get_oid();
 	__u64 off = t.get_length();
	__u64 len = t.get_length();
	_clone_range(t.get_cid(), oid, noid, off, len);
      }
      break;

    case Transaction::OP_MKCOLL:
      _create_collection(t.get_cid());
      break;

    case Transaction::OP_RMCOLL:
      _destroy_collection(t.get_cid());
      break;

    case Transaction::OP_COLL_ADD:
      {
	coll_t ocid = t.get_cid();
	coll_t ncid = t.get_cid();
	_collection_add(ocid, ncid, t.get_oid());
      }
      break;

    case Transaction::OP_COLL_REMOVE:
      _collection_remove(t.get_cid(), t.get_oid());
      break;

    case Transaction::OP_COLL_SETATTR:
      {
	bufferlist& bl = t.get_bl();
	_collection_setattr(t.get_cid(), t.get_attrname(), bl.c_str(), bl.length());
      }
      break;

    case Transaction::OP_COLL_RMATTR:
      _collection_rmattr(t.get_cid(), t.get_attrname());
      break;

    case Transaction::OP_STARTSYNC:
      _start_sync();
      break;

    default:
      cerr << "bad op " << op << std::endl;
      assert(0);
    }
  }
  
  return 0;  // FIXME count errors
}

  /*********************************************/


#if 0
/*
 * compound btrfs usertrans thinger version
 */
unsigned FileStore::apply_transaction(Transaction &t, Context *onsafe)
{
#ifdef DARWIN
  return ObjectStore::apply_transaction(t, onsafe);
#else

  // no btrfs transaction support?
  // or, use trans start/end ioctls?
  if (!btrfs || btrfs_trans_start_end) {
    bufferlist tbl;
    t.encode(tbl);  // apply_transaction modifies t; encode first
    op_start();
    int r = ObjectStore::apply_transaction(t);
    dout(10) << "op_seq is " << op_seq << dendl;
    if (r >= 0)
      journal_transaction(tbl, onsafe);
    else
      delete onsafe;
    op_finish();
    return r;
  }

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
	sobject_t oid;
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
	char *fn = new char[PATH_MAX];
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
	sobject_t oid;
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
	sobject_t oid;
	t.get_oid(oid);
	__u64 len;
	t.get_length(len);
	//truncate(cid, oid, len, 0);
	
	dout(10) << "truncate" << dendl;
	char *fn = new char[PATH_MAX];
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
	sobject_t oid;
	t.get_oid(oid);
	//remove(cid, oid, 0);
	
	dout(10) << "remove " << cid << " " << oid << dendl;
	char *fn = new char[PATH_MAX];
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
	sobject_t oid;
	t.get_oid(oid);
	const char *attrname;
	t.get_attrname(attrname);
	bufferlist bl;
	t.get_bl(bl);
	//setattr(cid, oid, attrname, bl.c_str(), bl.length(), 0);
	dout(10) << "setattr " << cid << " " << oid << dendl;
	char *fn = new char[PATH_MAX];
	str.push_back(fn);
	get_coname(cid, oid, fn);
	trans->ops[trans->len].op = BTRFS_IOC_USERTRANS_SETXATTR;
	trans->ops[trans->len].args[0] = (__u64)fn;
	char aname[ATTR_MAX];
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
	map<nstring,bufferptr> oldattrs;
	char *fn = new char[PATH_MAX];
	str.push_back(fn);

	if (op == Transaction::OP_SETATTRS) {
	  coll_t cid;
	  t.get_cid(cid);
	  sobject_t oid;
	  t.get_oid(oid);
	  getattrs(cid, oid, oldattrs);
	  get_coname(cid, oid, fn);
	} else {
	  coll_t cid;
	  t.get_cid(cid);
	  collection_getattrs(cid, oldattrs);
	  get_cdir(cid, fn);
	}
	map<nstring,bufferptr> *pattrset;
	t.get_pattrset(pattrset);
	
	for (map<nstring,bufferptr>::iterator p = pattrset->begin();
	     p != pattrset->end();
	     p++) {
	  trans->ops[trans->len].op = BTRFS_IOC_USERTRANS_SETXATTR;
	  trans->ops[trans->len].args[0] = (__u64)fn;
	  char *aname = new char[ATTR_MAX];
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
	for (map<nstring,bufferptr>::iterator p = oldattrs.begin();
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
	sobject_t oid;
	t.get_oid(oid);
	const char *attrname;
	t.get_attrname(attrname);
	//rmattr(cid, oid, attrname, 0);
	
	dout(10) << "rmattr " << cid << " " << oid << dendl;
	char *fn = new char[PATH_MAX];
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
	sobject_t oid;
	t.get_oid(oid);
	sobject_t noid;
	t.get_oid(noid);
	clone(cid, oid, noid);
	
	dout(10) << "clone " << cid << " " << oid << dendl;
	char *ofn = new char[PATH_MAX];
	str.push_back(ofn);
	char *nfn = new char[PATH_MAX];
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
	char *fn = new char[PATH_MAX];
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
	char *fn = new char[PATH_MAX];
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
	sobject_t oid;
	t.get_oid(oid);
	collection_add(cid, ocid, oid, 0);
	assert(0);
      }
      break;
      
    case Transaction::OP_COLL_REMOVE:
      {
	coll_t cid;
	t.get_cid(cid);
	sobject_t oid;
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
	char *fn = new char[PATH_MAX];
	str.push_back(fn);
	get_cdir(cid, fn);
	trans->ops[trans->len].op = BTRFS_IOC_USERTRANS_SETXATTR;
	trans->ops[trans->len].args[0] = (__u64)fn;
	char aname[ATTR_MAX];
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
	char *fn = new char[PATH_MAX];
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
    r = ::ioctl(fsid_fd, BTRFS_IOC_USERTRANS, (unsigned long)trans);
    if (r < 0) {
      derr(0) << "apply_transaction_end got " << strerror_r(errno, buf, sizeof(buf))
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
#endif /* DARWIN */
}

#endif 



// --------------------
// objects

bool FileStore::exists(coll_t cid, const sobject_t& oid)
{
  struct stat st;
  if (stat(cid, oid, &st) == 0)
    return true;
  else 
    return false;
}
  
int FileStore::stat(coll_t cid, const sobject_t& oid, struct stat *st)
{
  char fn[PATH_MAX];
  get_coname(cid, oid, fn);
  int r = ::stat(fn, st);
  dout(10) << "stat " << fn << " = " << r << dendl;
  return r < 0 ? -errno:r;
}

int FileStore::read(coll_t cid, const sobject_t& oid, 
                    __u64 offset, size_t len,
                    bufferlist& bl) {
  char fn[PATH_MAX];
  get_coname(cid, oid, fn);

  dout(15) << "read " << fn << " " << offset << "~" << len << dendl;

  int r;
  int fd = ::open(fn, O_RDONLY);
  if (fd < 0) {
    char buf[80];
    dout(10) << "read couldn't open " << fn << " errno " << errno << " " << strerror_r(errno, buf, sizeof(buf)) << dendl;
    r = -errno;
  } else {
    __u64 actual = ::lseek64(fd, offset, SEEK_SET);
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
    ::close(fd);
    r = got;
  }
  dout(10) << "read " << fn << " " << offset << "~" << len << " = " << r << dendl;
  return r;
}



int FileStore::_remove(coll_t cid, const sobject_t& oid) 
{
  char fn[PATH_MAX];
  get_coname(cid, oid, fn);
  dout(15) << "remove " << fn << dendl;
  int r = ::unlink(fn);
  if (r < 0) r = -errno;
  dout(10) << "remove " << fn << " = " << r << dendl;
  return r;
}

int FileStore::_truncate(coll_t cid, const sobject_t& oid, __u64 size)
{
  char fn[PATH_MAX];
  get_coname(cid, oid, fn);
  dout(15) << "truncate " << fn << " size " << size << dendl;
  int r = ::truncate(fn, size);
  if (r < 0) r = -errno;
  dout(10) << "truncate " << fn << " size " << size << " = " << r << dendl;
  return r;
}


int FileStore::_touch(coll_t cid, const sobject_t& oid)
{
  char fn[PATH_MAX];
  get_coname(cid, oid, fn);

  dout(15) << "touch " << fn << dendl;

  int flags = O_WRONLY|O_CREAT;
  int fd = ::open(fn, flags, 0644);
  int r;
  if (fd >= 0) {
    ::close(fd);
    r = 0;
  } else
    r = -errno;
  dout(10) << "touch " << fn << " = " << r << dendl;
  return r;
}

int FileStore::_write(coll_t cid, const sobject_t& oid, 
                     __u64 offset, size_t len,
                     const bufferlist& bl)
{
  char fn[PATH_MAX];
  get_coname(cid, oid, fn);

  dout(15) << "write " << fn << " " << offset << "~" << len << dendl;
  int r;

  char buf[80];
  int flags = O_WRONLY|O_CREAT;
  int fd = ::open(fn, flags, 0644);
  if (fd < 0) {
    derr(0) << "write couldn't open " << fn << " flags " << flags << " errno " << errno << " " << strerror_r(errno, buf, sizeof(buf)) << dendl;
    r = -errno;
  } else {
    
    // seek
    __u64 actual = ::lseek64(fd, offset, SEEK_SET);
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
	derr(0) << "couldn't write to " << fn << " len " << len << " off " << offset << " errno " << errno << " " << strerror_r(errno, buf, sizeof(buf)) << dendl;
      }
    }
    
    if (did < 0) {
      derr(0) << "couldn't write to " << fn << " len " << len << " off " << offset << " errno " << errno << " " << strerror_r(errno, buf, sizeof(buf)) << dendl;
    }
    
    ::close(fd);
    r = did;
  }

  dout(10) << "write " << fn << " " << offset << "~" << len << " = " << r << dendl;
  return r;
}

int FileStore::_zero(coll_t cid, const sobject_t& oid, __u64 offset, size_t len)
{
  // write zeros.. yuck!
  bufferptr bp(len);
  bufferlist bl;
  bl.push_back(bp);
  return _write(cid, oid, offset, len, bl);
}

int FileStore::_clone(coll_t cid, const sobject_t& oldoid, const sobject_t& newoid)
{
  char ofn[PATH_MAX], nfn[PATH_MAX];
  get_coname(cid, oldoid, ofn);
  get_coname(cid, newoid, nfn);

  dout(15) << "clone " << ofn << " -> " << nfn << dendl;

  int o, n, r;
  o = ::open(ofn, O_RDONLY);
  if (o < 0) {
    r = -errno;
    goto out2;
  }
  n = ::open(nfn, O_CREAT|O_TRUNC|O_WRONLY, 0644);
  if (n < 0) {
    r = -errno;
    goto out;
  }
  if (btrfs)
#ifndef DARWIN
    r = ::ioctl(n, BTRFS_IOC_CLONE, o);
#else 
  ;
#endif /* DARWIN */
  else {
    struct stat st;
    ::fstat(o, &st);
    dout(10) << "clone " << ofn << " -> " << nfn << " READ+WRITE" << dendl;
    r = _do_clone_range(o, n, 0, st.st_size);
  }
  if (r < 0) r = -errno;

 out:
  ::close(n);
 out2:
  ::close(o);
  
  dout(10) << "clone " << ofn << " -> " << nfn << " = " << r << dendl;
  return 0;
}

int FileStore::_do_clone_range(int from, int to, __u64 off, __u64 len)
{
  dout(20) << "_do_clone_range " << off << "~" << len << dendl;
  int r = 0;
  
  if (btrfs >= 2) {
    btrfs_ioctl_clone_range_args a;
    a.src_fd = from;
    a.src_offset = off;
    a.src_length = len;
    a.dest_offset = off;
    r = ::ioctl(to, BTRFS_IOC_CLONE_RANGE, &a);
    if (r >= 0)
      return r;
    return -errno;
  }

  loff_t pos = off;
  loff_t end = off + len;
  int buflen = 4096*32;
  char buf[buflen];
  while (pos < end) {
    int l = MIN(end-pos, buflen);
    r = ::read(from, buf, l);
    if (r < 0)
      break;
    int op = 0;
    while (op < l) {
      int r2 = ::write(to, buf+op, l-op);
      
      if (r2 < 0) { r = r2; break; }
      op += r2;	  
    }
    if (r < 0) break;
    pos += r;
  }

  return r;
}

int FileStore::_clone_range(coll_t cid, const sobject_t& oldoid, const sobject_t& newoid, __u64 off, __u64 len)
{
  char ofn[PATH_MAX], nfn[PATH_MAX];
  get_coname(cid, oldoid, ofn);
  get_coname(cid, newoid, nfn);

  dout(15) << "clone_range " << ofn << " -> " << nfn << " " << off << "~" << len << dendl;

  int r;
  int o, n;
  o = ::open(ofn, O_RDONLY);
  if (o < 0) {
    r = -errno;
    goto out2;
  }
  n = ::open(nfn, O_CREAT|O_WRONLY, 0644);
  if (n < 0) {
    r = -errno;
    goto out;
  }
  r = _do_clone_range(o, n, off, len);
  ::close(n);
 out:
  ::close(o);
 out2:
  dout(10) << "clone_range " << ofn << " -> " << nfn << " " << off << "~" << len << " = " << r << dendl;
  return r;
}


void FileStore::sync_entry()
{
  Cond othercond;

  lock.Lock();
  while (!stop) {
    utime_t max_interval;
    max_interval.set_from_double(g_conf.filestore_max_sync_interval);
    utime_t min_interval;
    min_interval.set_from_double(g_conf.filestore_min_sync_interval);

    dout(20) << "sync_entry waiting for max_interval " << max_interval << dendl;
    utime_t startwait = g_clock.now();

    sync_cond.WaitInterval(lock, max_interval);

    // wait for at least the min interval
    utime_t woke = g_clock.now();
    woke -= startwait;
    dout(20) << "sync_entry woke after " << woke << dendl;
    if (woke < min_interval) {
      utime_t t = min_interval;
      t -= woke;
      dout(20) << "sync_entry waiting for another " << t << " to reach min interval " << min_interval << dendl;
      othercond.WaitInterval(lock, t);
    }

    lock.Unlock();

    if (commit_start()) {
      dout(15) << "sync_entry committing " << op_seq << dendl;

      __u64 cp = op_seq;
      
      commit_started();
      
      if (btrfs) {
	// do a full btrfs commit
	::ioctl(op_fd, BTRFS_IOC_SYNC);
      } else {
	// make the file system's journal commit.
	//  this works with ext3, but NOT ext4
	::fsync(op_fd);  
      }
      
      commit_finish();
      dout(15) << "sync_entry committed to op_seq " << cp << dendl;
    }

    lock.Lock();

  }
  lock.Unlock();
}

void FileStore::_start_sync()
{
  if (!journal) {  // don't do a big sync if the journal is on
    dout(10) << "start_sync" << dendl;
    sync_cond.Signal();
  } else {
    dout(10) << "start_sync - NOOP (journal is on)" << dendl;
  }
}

void FileStore::sync()
{
  Mutex::Locker l(lock);
  sync_cond.Signal();
}

void FileStore::sync(Context *onsafe)
{
  ObjectStore::Transaction t;
  apply_transaction(t);
  sync();
}


// -------------------------------
// attributes

// low-level attr helpers
int FileStore::_getattr(const char *fn, const char *name, bufferptr& bp)
{
  char val[100];
  int l = do_getxattr(fn, name, val, sizeof(val));
  if (l >= 0) {
    bp = buffer::create(l);
    memcpy(bp.c_str(), val, l);
  } else if (l == -ERANGE) {
    l = do_getxattr(fn, name, 0, 0);
    if (l) {
      bp = buffer::create(l);
      l = do_getxattr(fn, name, bp.c_str(), l);
    }
  }
  return l;
}

int FileStore::_getattrs(const char *fn, map<nstring,bufferptr>& aset, bool user_only) 
{
  // get attr list
  char names1[100];
  int len = do_listxattr(fn, names1, sizeof(names1)-1);
  char *names2 = 0;
  char *name = 0;
  if (len == -ERANGE) {
    len = do_listxattr(fn, 0, 0);
    if (len < 0)
      return len;
    dout(10) << " -ERANGE, len is " << len << dendl;
    names2 = new char[len+1];
    len = do_listxattr(fn, names2, len);
    dout(10) << " -ERANGE, got " << len << dendl;
    if (len < 0)
      return len;
    name = names2;
  } else if (len < 0)
    return len;
  else
    name = names1;
  name[len] = 0;

  char *end = name + len;
  while (name < end) {
    char *attrname = name;
    if (parse_attrname(&name)) {
      char *set_name = name;
      bool can_get = true;
      if (user_only) {
          if (*set_name =='_')
            set_name++;
          else
            can_get = false;
      }
      if (*set_name && can_get) {
        dout(20) << "getattrs " << fn << " getting '" << name << "'" << dendl;
        //dout(0) << "getattrs " << fn << " set_name '" << set_name << "' user_only=" << user_only << dendl;
      
        int r = _getattr(fn, attrname, aset[set_name]);
        if (r < 0) return r;
      }
    }
    name += strlen(name) + 1;
  }

  delete[] names2;
  return 0;
}

// objects

int FileStore::getattr(coll_t cid, const sobject_t& oid, const char *name,
		       void *value, size_t size) 
{
  if (fake_attrs) return attrs.getattr(cid, oid, name, value, size);

  char fn[PATH_MAX];
  get_coname(cid, oid, fn);
  dout(15) << "getattr " << fn << " '" << name << "' len " << size << dendl;
  char n[ATTR_MAX];
  get_attrname(name, n);
  int r = do_getxattr(fn, n, value, size);
  dout(10) << "getattr " << fn << " '" << name << "' len " << size << " = " << r << dendl;
  return r;
}

int FileStore::getattr(coll_t cid, const sobject_t& oid, const char *name, bufferptr &bp)
{
  if (fake_attrs) return attrs.getattr(cid, oid, name, bp);

  char fn[PATH_MAX];
  get_coname(cid, oid, fn);
  dout(15) << "getattr " << fn << " '" << name << "'" << dendl;
  char n[ATTR_MAX];
  get_attrname(name, n);
  int r = _getattr(fn, n, bp);
  dout(10) << "getattr " << fn << " '" << name << "' = " << r << dendl;
  return r;
}

int FileStore::getattrs(coll_t cid, const sobject_t& oid, map<nstring,bufferptr>& aset, bool user_only) 
{
  if (fake_attrs) return attrs.getattrs(cid, oid, aset);

  char fn[PATH_MAX];
  get_coname(cid, oid, fn);
  dout(15) << "getattrs " << fn << dendl;
  int r = _getattrs(fn, aset, user_only);
  dout(10) << "getattrs " << fn << " = " << r << dendl;
  return r;
}





int FileStore::_setattr(coll_t cid, const sobject_t& oid, const char *name,
			const void *value, size_t size) 
{
  if (fake_attrs) return attrs.setattr(cid, oid, name, value, size);

  char fn[PATH_MAX];
  get_coname(cid, oid, fn);
  dout(15) << "setattr " << fn << " '" << name << "' len " << size << dendl;
  char n[ATTR_MAX];
  get_attrname(name, n);
  int r = do_setxattr(fn, n, value, size);
  dout(10) << "setattr " << fn << " '" << name << "' len " << size << " = " << r << dendl;
  return r;
}

int FileStore::_setattrs(coll_t cid, const sobject_t& oid, map<nstring,bufferptr>& aset) 
{
  if (fake_attrs) return attrs.setattrs(cid, oid, aset);

  char fn[PATH_MAX];
  get_coname(cid, oid, fn);
  dout(15) << "setattrs " << fn << dendl;
  int r = 0;
  for (map<nstring,bufferptr>::iterator p = aset.begin();
       p != aset.end();
       ++p) {
    char n[ATTR_MAX];
    get_attrname(p->first.c_str(), n);
    const char *val;
    if (p->second.length())
      val = p->second.c_str();
    else
      val = "";
    r = do_setxattr(fn, n, val, p->second.length());
    if (r < 0) {
      char buf[80];
      cerr << "error setxattr " << strerror_r(errno, buf, sizeof(buf)) << std::endl;
      break;
    }
  }
  dout(10) << "setattrs " << fn << " = " << r << dendl;
  return r;
}


int FileStore::_rmattr(coll_t cid, const sobject_t& oid, const char *name) 
{
  if (fake_attrs) return attrs.rmattr(cid, oid, name);

  char fn[PATH_MAX];
  get_coname(cid, oid, fn);
  dout(15) << "rmattr " << fn << " '" << name << "'" << dendl;
  char n[ATTR_MAX];
  get_attrname(name, n);
  int r = do_removexattr(fn, n);
  dout(10) << "rmattr " << fn << " '" << name << "' = " << r << dendl;
  return r;
}

int FileStore::_rmattrs(coll_t cid, const sobject_t& oid) 
{
  //if (fake_attrs) return attrs.rmattrs(cid, oid);

  char fn[PATH_MAX];
  get_coname(cid, oid, fn);

  dout(15) << "rmattrs " << fn << dendl;

  map<nstring,bufferptr> aset;
  int r = _getattrs(fn, aset);
  if (r >= 0) {
    for (map<nstring,bufferptr>::iterator p = aset.begin(); p != aset.end(); p++) {
      char n[ATTR_MAX];
      get_attrname(p->first.c_str(), n);
      r = do_removexattr(fn, n);
      if (r < 0)
	break;
    }
  }
  dout(10) << "rmattrs " << fn << " = " << r << dendl;
  return r;
}



// collections

int FileStore::collection_getattr(coll_t c, const char *name,
				  void *value, size_t size) 
{
  if (fake_attrs) return attrs.collection_getattr(c, name, value, size);

  char fn[PATH_MAX];
  get_cdir(c, fn);
  dout(15) << "collection_getattr " << fn << " '" << name << "' len " << size << dendl;
  char n[PATH_MAX];
  get_attrname(name, n);
  int r = do_getxattr(fn, n, value, size);   
  dout(10) << "collection_getattr " << fn << " '" << name << "' len " << size << " = " << r << dendl;
  return r;
}

int FileStore::collection_getattr(coll_t c, const char *name, bufferlist& bl)
{
  if (fake_attrs) return attrs.collection_getattr(c, name, bl);

  char fn[PATH_MAX];
  get_cdir(c, fn);
  dout(15) << "collection_getattr " << fn << " '" << name << "'" << dendl;
  char n[PATH_MAX];
  get_attrname(name, n);
  
  buffer::ptr bp;
  int r = _getattr(fn, n, bp);
  bl.push_back(bp);
  dout(10) << "collection_getattr " << fn << " '" << name << "' = " << r << dendl;
  return r;
}

int FileStore::collection_getattrs(coll_t cid, map<nstring,bufferptr>& aset) 
{
  if (fake_attrs) return attrs.collection_getattrs(cid, aset);

  char fn[PATH_MAX];
  get_cdir(cid, fn);
  dout(10) << "collection_getattrs " << fn << dendl;
  int r = _getattrs(fn, aset);
  dout(10) << "collection_getattrs " << fn << " = " << r << dendl;
  return r;
}


int FileStore::_collection_setattr(coll_t c, const char *name,
				  const void *value, size_t size) 
{
  if (fake_attrs) return attrs.collection_setattr(c, name, value, size);

  char fn[PATH_MAX];
  get_cdir(c, fn);
  dout(10) << "collection_setattr " << fn << " '" << name << "' len " << size << dendl;
  char n[PATH_MAX];
  get_attrname(name, n);
  int r = do_setxattr(fn, n, value, size);
  dout(10) << "collection_setattr " << fn << " '" << name << "' len " << size << " = " << r << dendl;
  return r;
}

int FileStore::_collection_rmattr(coll_t c, const char *name) 
{
  if (fake_attrs) return attrs.collection_rmattr(c, name);

  char fn[PATH_MAX];
  get_cdir(c, fn);
  dout(15) << "collection_rmattr " << fn << dendl;
  char n[PATH_MAX];
  get_attrname(name, n);
  int r = do_removexattr(fn, n);
  dout(10) << "collection_rmattr " << fn << " = " << r << dendl;
  return r;
}


int FileStore::_collection_setattrs(coll_t cid, map<nstring,bufferptr>& aset) 
{
  if (fake_attrs) return attrs.collection_setattrs(cid, aset);

  char fn[PATH_MAX];
  get_cdir(cid, fn);
  dout(15) << "collection_setattrs " << fn << dendl;
  int r = 0;
  for (map<nstring,bufferptr>::iterator p = aset.begin();
       p != aset.end();
       ++p) {
    char n[PATH_MAX];
    get_attrname(p->first.c_str(), n);
    r = do_setxattr(fn, n, p->second.c_str(), p->second.length());
    if (r < 0) break;
  }
  dout(10) << "collection_setattrs " << fn << " = " << r << dendl;
  return r;
}



// --------------------------
// collections

int FileStore::list_collections(vector<coll_t>& ls) 
{
  if (fake_collections) return collections.list_collections(ls);

  dout(10) << "list_collections" << dendl;

  char fn[PATH_MAX];
  sprintf(fn, "%s", basedir.c_str());

  DIR *dir = ::opendir(fn);
  if (!dir)
    return -errno;

  struct dirent sde, *de;
  while (::readdir_r(dir, &sde, &de) == 0) {
    if (!de)
      break;
    coll_t c;
    if (parse_coll(de->d_name, c))
      ls.push_back(c);
  }
  
  ::closedir(dir);
  return 0;
}

int FileStore::collection_stat(coll_t c, struct stat *st) 
{
  if (fake_collections) return collections.collection_stat(c, st);

  char fn[PATH_MAX];
  get_cdir(c, fn);
  dout(15) << "collection_stat " << fn << dendl;
  int r = ::stat(fn, st);
  if (r < 0) r = -errno;
  dout(10) << "collection_stat " << fn << " = " << r << dendl;
  return r;
}

bool FileStore::collection_exists(coll_t c) 
{
  if (fake_collections) return collections.collection_exists(c);

  struct stat st;
  return collection_stat(c, &st) == 0;
}

bool FileStore::collection_empty(coll_t c) 
{  
  if (fake_collections) return collections.collection_empty(c);

  char fn[PATH_MAX];
  get_cdir(c, fn);
  dout(15) << "collection_empty " << fn << dendl;

  DIR *dir = ::opendir(fn);
  if (!dir)
    return -errno;

  bool empty = true;
  struct dirent sde, *de;
  while (::readdir_r(dir, &sde, &de) == 0) {
    if (!de)
      break;
    // parse
    if (de->d_name[0] == '.') continue;
    //cout << "  got object " << de->d_name << std::endl;
    sobject_t o;
    if (parse_object(de->d_name, o)) {
      empty = false;
      break;
    }
  }
  
  ::closedir(dir);
  dout(10) << "collection_empty " << fn << " = " << empty << dendl;
  return empty;
}

int FileStore::collection_list_partial(coll_t c, snapid_t seq, vector<sobject_t>& ls, int max_count,
				       collection_list_handle_t *handle)
{  
  if (fake_collections) return collections.collection_list(c, ls);

  char fn[PATH_MAX];
  get_cdir(c, fn);

  DIR *dir = NULL;
  struct dirent sde, *de;
  bool end;
  
  dir = ::opendir(fn);

  if (!dir) {
    dout(0) << "error opening directory " << fn << dendl;
    return -errno;
  }

  if (handle && *handle) {
    seekdir(dir, *(off_t *)handle);
    *handle = 0;
  }

  int i=0;
  while (i < max_count) {
    errno = 0;
    end = false;
    ::readdir_r(dir, &sde, &de);
    if (!de && errno) {
      dout(0) << "error reading directory " << fn << dendl;
      return -errno;
    }
    if (!de) {
      end = true;
      break;
    }

    // parse
    if (de->d_name[0] == '.') {
      continue;
    }
    //cout << "  got object " << de->d_name << std::endl;
    sobject_t o;
    if (parse_object(de->d_name, o)) {
      if (o.snap >= seq) {
	ls.push_back(o);
	i++;
      }
    }
  }

  if (handle && !end)
    *handle = (collection_list_handle_t)telldir(dir);

  ::closedir(dir);

  dout(10) << "collection_list " << fn << " = 0 (" << ls.size() << " objects)" << dendl;
  return 0;
}


int FileStore::collection_list(coll_t c, vector<sobject_t>& ls) 
{  
  if (fake_collections) return collections.collection_list(c, ls);

  char fn[PATH_MAX];
  get_cdir(c, fn);
  dout(10) << "collection_list " << fn << dendl;

  DIR *dir = ::opendir(fn);
  if (!dir)
    return -errno;
  
  // first, build (ino, object) list
  vector< pair<ino_t,sobject_t> > inolist;

  struct dirent sde, *de;
  while (::readdir_r(dir, &sde, &de) == 0) {
    if (!de)
      break;
    // parse
    if (de->d_name[0] == '.')
      continue;
    //cout << "  got object " << de->d_name << std::endl;
    sobject_t o;
    if (parse_object(de->d_name, o)) {
      inolist.push_back(pair<ino_t,sobject_t>(de->d_ino, o));
      ls.push_back(o);
    }
  }

  // sort
  dout(10) << "collection_list " << fn << " sorting " << inolist.size() << " objects" << dendl;
  sort(inolist.begin(), inolist.end());

  // build final list
  ls.resize(inolist.size());
  int i = 0;
  for (vector< pair<ino_t,sobject_t> >::iterator p = inolist.begin(); p != inolist.end(); p++)
    ls[i++] = p->second;
  
  dout(10) << "collection_list " << fn << " = 0 (" << ls.size() << " objects)" << dendl;
  ::closedir(dir);
  return 0;
}


int FileStore::_create_collection(coll_t c) 
{
  if (fake_collections) return collections.create_collection(c);
  
  char fn[PATH_MAX];
  get_cdir(c, fn);
  dout(15) << "create_collection " << fn << dendl;
  int r = ::mkdir(fn, 0755);
  if (r < 0) r = -errno;
  dout(10) << "create_collection " << fn << " = " << r << dendl;
  return r;
}

int FileStore::_destroy_collection(coll_t c) 
{
  if (fake_collections) return collections.destroy_collection(c);

  char fn[PATH_MAX];
  get_cdir(c, fn);
  dout(15) << "_destroy_collection " << fn << dendl;
  int r = ::rmdir(fn);
  //char cmd[PATH_MAX];
  //sprintf(cmd, "test -d %s && rm -r %s", fn, fn);
  //system(cmd);
  if (r < 0) r = -errno;
  dout(10) << "_destroy_collection " << fn << " = " << r << dendl;
  return r;
}


int FileStore::_collection_add(coll_t c, coll_t cid, const sobject_t& o) 
{
  if (fake_collections) return collections.collection_add(c, o);

  char cof[PATH_MAX];
  get_coname(c, o, cof);
  char of[PATH_MAX];
  get_coname(cid, o, of);
  dout(15) << "collection_add " << cof << " " << of << dendl;
  int r = ::link(of, cof);
  if (r < 0) r = -errno;
  dout(10) << "collection_add " << cof << " " << of << " = " << r << dendl;
  return r;
}

int FileStore::_collection_remove(coll_t c, const sobject_t& o) 
{
  if (fake_collections) return collections.collection_remove(c, o);

  char cof[PATH_MAX];
  get_coname(c, o, cof);
  dout(15) << "collection_remove " << cof << dendl;
  int r = ::unlink(cof);
  if (r < 0) r = -errno;
  dout(10) << "collection_remove " << cof << " = " << r << dendl;
  return r;
}



// eof.
