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

#include "osd/osd_types.h"

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

#include <sstream>


#define ATTR_MAX 80

#define COMMIT_SNAP_ITEM "%lld"

#ifndef __CYGWIN__
# ifndef DARWIN
#  include "btrfs_ioctl.h"



# endif
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


static void get_attrname(const char *name, char *buf, int len)
{
  snprintf(buf, len, "user.ceph.%s", name);
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

void FileStore::append_oname(const sobject_t &oid, char *s, int len)
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
    snprintf(t, len, "_head");
  else if (oid.snap == CEPH_SNAPDIR)
    snprintf(t, len, "_snapdir");
  else
    snprintf(t, len, "_%llx", (long long unsigned)oid.snap);
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
  bool r = c.parse(s);
  dout(0) << "parse " << s << " -> " << c << " = " << r << dendl;
  return r;
}

void FileStore::get_cdir(coll_t cid, char *s, int len) 
{
  int ret = snprintf(s, len, "%s/current/", basedir.c_str());
  s += ret;
  len -= ret;
  s += cid.print(s, len);
}

void FileStore::get_coname(coll_t cid, const sobject_t& oid, char *s, int len) 
{
  get_cdir(cid, s, len);
  append_oname(oid, s, len);
}

int FileStore::open_journal()
{
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
    snprintf(cmd, sizeof(cmd), "mount %s", g_conf.filestore_dev);
    system(cmd);
  }

  dout(1) << "mkfs in " << basedir << dendl;

  char fn[PATH_MAX];
  snprintf(fn, sizeof(fn), "%s/fsid", basedir.c_str());
  fsid_fd = ::open(fn, O_CREAT|O_RDWR, 0644);
  if (lock_fsid() < 0)
    return -EBUSY;

  // wipe
  snprintf(cmd, sizeof(cmd), "test -d %s && ( test -d %s/current && rm -r %s/current/* %s/fsid || rm -r %s/* ) ; mkdir -p %s",
	  basedir.c_str(), basedir.c_str(), basedir.c_str(), basedir.c_str(), basedir.c_str(), basedir.c_str());
  
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

  // current
  struct btrfs_ioctl_vol_args volargs;
  memset(&volargs, 0, sizeof(volargs));
  int fd = ::open(basedir.c_str(), O_RDONLY);
  volargs.fd = 0;
  strcpy(volargs.name, "current");
  int r = ::ioctl(fd, BTRFS_IOC_SUBVOL_CREATE, (unsigned long int)&volargs);
  char current_fn[PATH_MAX];
  snprintf(current_fn, sizeof(current_fn), "%s/current", basedir.c_str());
  if (r == 0) {
    // yay
    dout(2) << " created btrfs subvol " << current_fn << dendl;
    ::chmod(current_fn, 0755);
  } else if (errno == EEXIST) {
    dout(2) << " current already exists" << dendl;
    r = 0;
  } else if (errno == EOPNOTSUPP || errno == ENOTTY) {
    dout(2) << " BTRFS_IOC_SUBVOL_CREATE ioctl failed, trying mkdir " << current_fn << dendl;
    r = ::mkdir(current_fn, 0755);
    if (errno == EEXIST)
      r = 0;
  }
  ::close(fd);
  if (r < 0) {
    char err[80];
    dout(0) << "failed to create current: " << strerror_r(errno, err, sizeof(err)) << dendl;
    return -errno;
  }

  // journal?
  int err = mkjournal();
  if (err)
    return err;

  if (g_conf.filestore_dev) {
    char cmd[PATH_MAX];
    dout(0) << "umounting" << dendl;
    snprintf(cmd, sizeof(cmd), "umount %s", g_conf.filestore_dev);
    //system(cmd);
  }

  dout(1) << "mkfs done in " << basedir << dendl;
  return 0;
}

int FileStore::mkjournal()
{
  open_journal();
  if (journal) {
    int err = journal->create();
    if (err < 0) {
      dout(0) << "mkjournal error creating journal on " << journalpath << dendl;
    } else {
      dout(0) << "mkjournal created journal on " << journalpath << dendl;
    }
    delete journal;
    journal = 0;
  }
  return 0;
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
    snprintf(cmd, sizeof(cmd), "mount %s", g_conf.filestore_dev);
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

  // get fsid
  char fn[PATH_MAX];
  snprintf(fn, sizeof(fn), "%s/fsid", basedir.c_str());

  // fake attrs? 
  // let's test to see if they work.
  if (g_conf.filestore_fake_attrs) {
    dout(0) << "faking attrs (in memory)" << dendl;
    fake_attrs = true;
  } else {
    int x = rand();
    int y = x+1;
    do_setxattr(fn, "user.test", &x, sizeof(x));
    do_getxattr(fn, "user.test", &y, sizeof(y));
    /*dout(10) << "x = " << x << "   y = " << y 
	     << "  r1 = " << r1 << "  r2 = " << r2
	     << " " << strerror(errno)
	     << dendl;*/
    if (x != y) {
      derr(0) << "xattrs don't appear to work (" << strerror_r(errno, buf, sizeof(buf)) << ") on " << basedir << ", be sure to mount underlying file system with 'user_xattr' option" << dendl;
      return -errno;
    }
  }

  fsid_fd = ::open(fn, O_RDWR|O_CREAT, 0644);
  ::read(fsid_fd, &fsid, sizeof(fsid));

  if (lock_fsid() < 0)
    return -EBUSY;

  dout(10) << "mount fsid is " << fsid << dendl;

  // get epoch
  snprintf(fn, sizeof(fn), "%s/current/commit_op_seq", basedir.c_str());
  op_fd = ::open(fn, O_CREAT|O_RDWR, 0644);
  assert(op_fd >= 0);
  __u64 initial_op_seq = 0;
  ::read(op_fd, &initial_op_seq, sizeof(initial_op_seq));

  dout(5) << "mount op_seq is " << initial_op_seq << dendl;

  // journal
  open_journal();
  r = journal_replay(initial_op_seq);
  if (r == -EINVAL) {
    dout(0) << "mount got EINVAL on journal open, not mounting" << dendl;
    return r;
  }
  journal_start();
  sync_thread.create();
  op_thread.create();
  flusher_thread.create();
  op_finisher.start();

  if (journal && g_conf.filestore_journal_writeahead)
    journal->set_wait_on_full(true);

  // is this btrfs?
  Transaction empty;
  btrfs = 1;

  btrfs_snap = false;
  if (btrfs_snap) {
    snapdir_fd = ::open(basedir.c_str(), O_RDONLY);

    // get snap list
    DIR *dir = ::opendir(basedir.c_str());
    if (!dir)
      return -errno;

    struct dirent sde, *de;
    while (::readdir_r(dir, &sde, &de) == 0) {
      if (!de)
	break;
      long long unsigned c;
      if (sscanf(de->d_name, COMMIT_SNAP_ITEM, &c) == 1)
	snaps.push_back(c);
    }
    
    ::closedir(dir);

    dout(0) << " found snaps " << snaps << dendl;
  }

  btrfs_trans_start_end = true;  // trans start/end interface
  r = apply_transaction(empty, 0);
  if (r == 0) {
    dout(0) << "mount btrfs TRANS_START ioctl is supported" << dendl;
  } else {
    dout(0) << "mount btrfs TRANS_START ioctl is NOT supported: " << strerror_r(-r, buf, sizeof(buf)) << dendl;
  }
  if (r == 0) {
    // do we have the shiny new CLONE_RANGE ioctl?
    btrfs = 2;
    int r = _do_clone_range(fsid_fd, -1, 0, 1);
    if (r == -EBADF) {
      dout(0) << "mount btrfs CLONE_RANGE ioctl is supported" << dendl;
    } else {
      dout(0) << "mount btrfs CLONE_RANGE ioctl is NOT supported: " << strerror_r(-r, buf, sizeof(buf)) << dendl;
      btrfs = 1;
    }
    dout(0) << "mount detected btrfs" << dendl;      
  } else {
    dout(0) << "mount did NOT detect btrfs" << dendl;
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
  flusher_cond.Signal();
  op_cond.Signal();
  lock.Unlock();
  sync_thread.join();
  op_thread.join();
  flusher_thread.join();

  journal_stop();

  op_finisher.stop();

  ::close(fsid_fd);
  ::close(op_fd);

  if (g_conf.filestore_dev) {
    char cmd[PATH_MAX];
    dout(0) << "umounting" << dendl;
    snprintf(cmd, sizeof(cmd), "umount %s", g_conf.filestore_dev);
    //system(cmd);
  }

  // nothing
  return 0;
}


/// -----------------------------

void FileStore::queue_op(__u64 op_seq, list<Transaction*>& tls, Context *onreadable,
			 Context *oncommit)
{
  op_lock.Lock();
  Op *o = new Op;
  dout(10) << "queue_op " << o << " " << op_seq << dendl;
  o->op = op_seq;
  o->tls.swap(tls);
  o->onreadable = onreadable;
  o->oncommit = oncommit;
  op_queue.push_back(o);
  op_cond.Signal();
  op_lock.Unlock();
}

void FileStore::op_entry()
{
  op_lock.Lock();
  while (1) {
    while (!op_queue.empty()) {
      Op *o = op_queue.front();
      op_queue.pop_front();
      op_lock.Unlock();

      dout(10) << "op_entry " << o << " " << o->op << " start" << dendl;
      op_apply_start(o->op, o->oncommit);
      int r = do_transactions(o->tls, o->op);
      op_apply_finish();
      dout(10) << "op_entry " << o << " " << o->op << " r = " << r
	       << ", finisher " << o->onreadable << dendl;

      op_finisher.queue(o->onreadable, r);

      delete o;

      op_lock.Lock();
      op_empty_cond.Signal();
    }
    if (stop)
      break;
    op_cond.Wait(op_lock);
  }
  op_lock.Unlock();
}

struct C_JournaledAhead : public Context {
  FileStore *fs;
  __u64 op;
  list<ObjectStore::Transaction*> tls;
  Context *onreadable;
  Context *onjournal;
  Context *ondisk;

  C_JournaledAhead(FileStore *f, __u64 o, list<ObjectStore::Transaction*>& t,
	      Context *onr, Context *onj, Context *ond) :
    fs(f), op(o), tls(t), onreadable(onr), onjournal(onj), ondisk(ond) { }
  void finish(int r) {
    fs->_journaled_ahead(op, tls, onreadable, onjournal, ondisk);
  }
};

int FileStore::queue_transaction(Transaction *t)
{
  list<Transaction*> tls;
  tls.push_back(t);
  return queue_transactions(tls, new C_DeleteTransaction(t));
}

int FileStore::queue_transactions(list<Transaction*> &tls,
				  Context *onreadable,
				  Context *onjournal,
				  Context *ondisk)
{
  if (journal && journal->is_writeable()) {
    if (g_conf.filestore_journal_parallel) {
      __u64 op = op_journal_start(0);
      dout(10) << "queue_transactions (parallel) " << op << " " << tls << dendl;
      
      journal_transactions(tls, op, onjournal);
      
      // queue inside journal lock, to preserve ordering
      queue_op(op, tls, onreadable, ondisk);
      
      op_journal_finish();
      return 0;
    }
    else if (g_conf.filestore_journal_writeahead) {
      __u64 op = op_journal_start(0);
      dout(10) << "queue_transactions (writeahead) " << op << " " << tls << dendl;
      journal_transactions(tls, op, new C_JournaledAhead(this, op, tls, onreadable, onjournal, ondisk));
      op_journal_finish();
      return 0;
    }
  }

  __u64 op_seq = op_apply_start(0, ondisk);
  dout(10) << "queue_transactions (trailing journal) " << op_seq << " " << tls << dendl;
  int r = do_transactions(tls, op_seq);
  op_apply_finish();
  op_finisher.queue(onreadable, r);
    
  if (r >= 0) {
    op_journal_start(op_seq);
    journal_transactions(tls, op_seq, onjournal);
    op_journal_finish();
  } else {
    delete onjournal;
    delete ondisk;
  }
  return r;
}

void FileStore::_journaled_ahead(__u64 op,
				 list<Transaction*> &tls,
				 Context *onreadable,
				 Context *onjournal,
				 Context *ondisk)
{
  dout(10) << "_journaled_ahead " << op << " " << tls << dendl;
  // this should queue in order because the journal does it's completions in order.
  queue_op(op, tls, onreadable, ondisk);
  if (onjournal) {
    onjournal->finish(0);
    delete onjournal;
  }
}

int FileStore::do_transactions(list<Transaction*> &tls, __u64 op_seq)
{
  int r;

  __u64 bytes = 0, ops = 0;
  for (list<Transaction*>::iterator p = tls.begin();
       p != tls.end();
       p++) {
    bytes += (*p)->get_num_bytes();
    ops += (*p)->get_num_ops();
  }

  int id = _transaction_start(bytes, ops);
  if (id < 0) {
    return id;
  }
    
  for (list<Transaction*>::iterator p = tls.begin();
       p != tls.end();
       p++) {
    r = _do_transaction(**p);
    if (r < 0)
      break;
  }
  
  ::pwrite(op_fd, &op_seq, sizeof(op_seq), 0);
  
  _transaction_finish(id);
  return r;
}

unsigned FileStore::apply_transaction(Transaction &t,
				      Context *onjournal,
				      Context *ondisk)
{
  list<Transaction*> tls;
  tls.push_back(&t);
  return apply_transactions(tls, onjournal, ondisk);
}

unsigned FileStore::apply_transactions(list<Transaction*> &tls,
				       Context *onjournal,
				       Context *ondisk)
{
  int r = 0;

  if (journal && journal->is_writeable() &&
      (g_conf.filestore_journal_parallel || g_conf.filestore_journal_writeahead)) {
    // use op pool
    Cond my_cond;
    Mutex my_lock("FileStore::apply_transaction::my_lock");
    bool done;
    C_SafeCond *onreadable = new C_SafeCond(&my_lock, &my_cond, &done, &r);

    dout(10) << "apply queued" << dendl;
    queue_transactions(tls, onreadable, onjournal, ondisk);
    
    my_lock.Lock();
    while (!done)
      my_cond.Wait(my_lock);
    my_lock.Unlock();
    dout(10) << "apply done r = " << r << dendl;
  } else {
    __u64 op_seq = op_apply_start(0, ondisk);
    r = do_transactions(tls, op_seq);
    op_apply_finish();

    if (r >= 0) {
      op_journal_start(op_seq);
      journal_transactions(tls, op_seq, onjournal);
      op_journal_finish();
    } else {
      delete onjournal;
      delete ondisk;
    }
  }
  return r;
}


// btrfs transaction start/end interface

int FileStore::_transaction_start(__u64 bytes, __u64 ops)
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

  int r = ::ioctl(fd, BTRFS_IOC_TRANS_START);
  if (r < 0) {
    derr(0) << "transaction_start got " << strerror_r(errno, buf, sizeof(buf))
 	    << " from btrfs ioctl" << dendl;    
    ::close(fd);
    return -errno;
  }
  dout(10) << "transaction_start " << fd << dendl;

  char fn[PATH_MAX];
  snprintf(fn, sizeof(fn), "%s/current/trans.%d", basedir.c_str(), fd);
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
  snprintf(fn, sizeof(fn), "%s/current/trans.%d", basedir.c_str(), fd);
  ::unlink(fn);
  
  dout(10) << "transaction_finish " << fd << dendl;
  ::ioctl(fd, BTRFS_IOC_TRANS_END);
  ::close(fd);
#endif /* DARWIN */
}

unsigned FileStore::_do_transaction(Transaction& t)
{
  dout(10) << "_do_transaction on " << &t << dendl;

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
  get_coname(cid, oid, fn, sizeof(fn));
  int r = ::stat(fn, st);
  dout(10) << "stat " << fn << " = " << r << dendl;
  return r < 0 ? -errno:r;
}

int FileStore::read(coll_t cid, const sobject_t& oid, 
                    __u64 offset, size_t len,
                    bufferlist& bl) {
  char fn[PATH_MAX];
  get_coname(cid, oid, fn, sizeof(fn));

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
  get_coname(cid, oid, fn, sizeof(fn));
  dout(15) << "remove " << fn << dendl;
  int r = ::unlink(fn);
  if (r < 0) r = -errno;
  dout(10) << "remove " << fn << " = " << r << dendl;
  return r;
}

int FileStore::_truncate(coll_t cid, const sobject_t& oid, __u64 size)
{
  char fn[PATH_MAX];
  get_coname(cid, oid, fn, sizeof(fn));
  dout(15) << "truncate " << fn << " size " << size << dendl;
  int r = ::truncate(fn, size);
  if (r < 0) r = -errno;
  dout(10) << "truncate " << fn << " size " << size << " = " << r << dendl;
  return r;
}


int FileStore::_touch(coll_t cid, const sobject_t& oid)
{
  char fn[PATH_MAX];
  get_coname(cid, oid, fn, sizeof(fn));

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
  get_coname(cid, oid, fn, sizeof(fn));

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

    if (!g_conf.filestore_flusher ||
	!queue_flusher(fd, offset, len)) {
      if (g_conf.filestore_sync_flush)
	::sync_file_range(fd, offset, len, SYNC_FILE_RANGE_WRITE);
      ::close(fd);
    }
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
  get_coname(cid, oldoid, ofn, sizeof(ofn));
  get_coname(cid, newoid, nfn, sizeof(nfn));

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
  get_coname(cid, oldoid, ofn, sizeof(ofn));
  get_coname(cid, newoid, nfn, sizeof(ofn));

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


bool FileStore::queue_flusher(int fd, __u64 off, __u64 len)
{
  bool queued;
  lock.Lock();
  if (flusher_queue_len < g_conf.filestore_flusher_max_fds) {
    flusher_queue.push_back(sync_epoch);
    flusher_queue.push_back(fd);
    flusher_queue.push_back(off);
    flusher_queue.push_back(len);
    flusher_queue_len++;
    flusher_cond.Signal();
    dout(10) << "queue_flusher ep " << sync_epoch << " fd " << fd << " " << off << "~" << len
	     << " qlen " << flusher_queue_len
	     << dendl;
    queued = true;
  } else {
    dout(10) << "queue_flusher ep " << sync_epoch << " fd " << fd << " " << off << "~" << len
	     << " qlen " << flusher_queue_len 
	     << " hit flusher_max_fds " << g_conf.filestore_flusher_max_fds
	     << ", skipping async flush" << dendl;
    queued = false;
  }
  lock.Unlock();
  return queued;
}

void FileStore::flusher_entry()
{
  lock.Lock();
  dout(20) << "flusher_entry start" << dendl;
  while (true) {
    if (!flusher_queue.empty()) {
      list<__u64> q;
      q.swap(flusher_queue);

      int num = flusher_queue_len;  // see how many we're taking, here

      lock.Unlock();
      while (!q.empty()) {
	__u64 ep = q.front();
	q.pop_front();
	int fd = q.front();
	q.pop_front();
	__u64 off = q.front();
	q.pop_front();
	__u64 len = q.front();
	q.pop_front();
	if (!stop && ep == sync_epoch) {
	  dout(10) << "flusher_entry flushing+closing " << fd << " ep " << ep << dendl;
	  ::sync_file_range(fd, off, len, SYNC_FILE_RANGE_WRITE);
	} else 
	  dout(10) << "flusher_entry JUST closing " << fd << " (stop=" << stop << ", ep=" << ep
		   << ", sync_epoch=" << sync_epoch << ")" << dendl;
	::close(fd);
      }
      lock.Lock();
      flusher_queue_len -= num;   // they're definitely closed, forget
    } else {
      if (stop)
	break;
      dout(20) << "flusher_entry sleeping" << dendl;
      flusher_cond.Wait(lock);
      dout(20) << "flusher_entry awoke" << dendl;
    }
  }
  dout(20) << "flusher_entry finish" << dendl;
  lock.Unlock();
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
      utime_t start = g_clock.now();
      __u64 cp = op_seq;

      // make flusher stop flushing previously queued stuff
      sync_epoch++;

      dout(15) << "sync_entry committing " << cp << " sync_epoch " << sync_epoch << dendl;


      if (btrfs_snap) {
	btrfs_ioctl_vol_args snapargs;
	snapargs.fd = snapdir_fd;
	snprintf(snapargs.name, sizeof(snapargs.name), COMMIT_SNAP_ITEM, (long long unsigned)cp);
	dout(0) << "taking snap '" << snapargs.name << "'" << dendl;
	int r = ::ioctl(snapargs.fd, BTRFS_IOC_SNAP_CREATE, &snapargs);
	char buf[100];
	dout(0) << "snap create '" << snapargs.name << "' got " << r
		<< " " << strerror_r(r < 0 ? errno : 0, buf, sizeof(buf)) << dendl;
	snaps.push_back(cp);
      }

      commit_started();

      if (!btrfs_snap) {
	if (btrfs) {
	  dout(15) << "sync_entry doing btrfs sync" << dendl;
	  // do a full btrfs commit
	  ::ioctl(op_fd, BTRFS_IOC_SYNC);
	} else {
	  // make the file system's journal commit.
	  //  this works with ext3, but NOT ext4
	  ::fsync(op_fd);  
	}
      }
      
      utime_t done = g_clock.now();
      done -= start;
      dout(10) << "sync_entry commit took " << done << dendl;
      commit_finish();

      // remove old snaps?
      if (false && btrfs_snap) {
	while (snaps.size() > 2) {
	  btrfs_ioctl_vol_args snapargs;
	  snapargs.fd = snapdir_fd;
	  snprintf(snapargs.name, sizeof(snapargs.name), COMMIT_SNAP_ITEM, (long long unsigned)snaps.front());
	  snaps.pop_front();
	  dout(0) << "removing snap '" << snapargs.name << "'" << dendl;
	  int r = ::ioctl(snapargs.fd, BTRFS_IOC_SNAP_DESTROY, &snapargs);
	  char buf[100];
	  dout(0) << "snap destroyed '" << snapargs.name << "' got " << r
		  << " " << strerror_r(r < 0 ? errno : 0, buf, sizeof(buf)) << dendl;
	}
      }

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

void FileStore::sync_and_flush()
{
  dout(10) << "sync_and_flush" << dendl;
  sync();
  
  if (g_conf.filestore_journal_writeahead) {
    dout(10) << "sync_and_flush waiting for journal finisher" << dendl;
    finisher.wait_for_empty();
  }
  
  op_lock.Lock();
  while (!op_queue.empty()) {
    dout(10) << "sync_and_flush waiting for op queue to flush" << dendl;
    op_empty_cond.Wait(op_lock);
  }
  op_lock.Unlock();

  dout(10) << "sync_and_flush waiting for apply finisher" << dendl;
  op_finisher.wait_for_empty();

  if (!g_conf.filestore_journal_writeahead) {
    dout(10) << "sync_and_flush waiting for journal finisher" << dendl;
    finisher.wait_for_empty();
  }

  dout(10) << "sync_and_flush done" << dendl;
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
  get_coname(cid, oid, fn, sizeof(fn));
  dout(15) << "getattr " << fn << " '" << name << "' len " << size << dendl;
  char n[ATTR_MAX];
  get_attrname(name, n, ATTR_MAX);
  int r = do_getxattr(fn, n, value, size);
  dout(10) << "getattr " << fn << " '" << name << "' len " << size << " = " << r << dendl;
  return r;
}

int FileStore::getattr(coll_t cid, const sobject_t& oid, const char *name, bufferptr &bp)
{
  if (fake_attrs) return attrs.getattr(cid, oid, name, bp);

  char fn[PATH_MAX];
  get_coname(cid, oid, fn, sizeof(fn));
  dout(15) << "getattr " << fn << " '" << name << "'" << dendl;
  char n[ATTR_MAX];
  get_attrname(name, n, ATTR_MAX);
  int r = _getattr(fn, n, bp);
  dout(10) << "getattr " << fn << " '" << name << "' = " << r << dendl;
  return r;
}

int FileStore::getattrs(coll_t cid, const sobject_t& oid, map<nstring,bufferptr>& aset, bool user_only) 
{
  if (fake_attrs) return attrs.getattrs(cid, oid, aset);

  char fn[PATH_MAX];
  get_coname(cid, oid, fn, sizeof(fn));
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
  get_coname(cid, oid, fn, sizeof(fn));
  dout(15) << "setattr " << fn << " '" << name << "' len " << size << dendl;
  char n[ATTR_MAX];
  get_attrname(name, n, ATTR_MAX);
  int r = do_setxattr(fn, n, value, size);
  dout(10) << "setattr " << fn << " '" << name << "' len " << size << " = " << r << dendl;
  return r;
}

int FileStore::_setattrs(coll_t cid, const sobject_t& oid, map<nstring,bufferptr>& aset) 
{
  if (fake_attrs) return attrs.setattrs(cid, oid, aset);

  char fn[PATH_MAX];
  get_coname(cid, oid, fn, sizeof(fn));
  dout(15) << "setattrs " << fn << dendl;
  int r = 0;
  for (map<nstring,bufferptr>::iterator p = aset.begin();
       p != aset.end();
       ++p) {
    char n[ATTR_MAX];
    get_attrname(p->first.c_str(), n, ATTR_MAX);
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
  get_coname(cid, oid, fn, sizeof(fn));
  dout(15) << "rmattr " << fn << " '" << name << "'" << dendl;
  char n[ATTR_MAX];
  get_attrname(name, n, ATTR_MAX);
  int r = do_removexattr(fn, n);
  dout(10) << "rmattr " << fn << " '" << name << "' = " << r << dendl;
  return r;
}

int FileStore::_rmattrs(coll_t cid, const sobject_t& oid) 
{
  //if (fake_attrs) return attrs.rmattrs(cid, oid);

  char fn[PATH_MAX];
  get_coname(cid, oid, fn, sizeof(fn));

  dout(15) << "rmattrs " << fn << dendl;

  map<nstring,bufferptr> aset;
  int r = _getattrs(fn, aset);
  if (r >= 0) {
    for (map<nstring,bufferptr>::iterator p = aset.begin(); p != aset.end(); p++) {
      char n[ATTR_MAX];
      get_attrname(p->first.c_str(), n, ATTR_MAX);
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
  get_cdir(c, fn, sizeof(fn));
  dout(15) << "collection_getattr " << fn << " '" << name << "' len " << size << dendl;
  char n[PATH_MAX];
  get_attrname(name, n, PATH_MAX);
  int r = do_getxattr(fn, n, value, size);   
  dout(10) << "collection_getattr " << fn << " '" << name << "' len " << size << " = " << r << dendl;
  return r;
}

int FileStore::collection_getattr(coll_t c, const char *name, bufferlist& bl)
{
  if (fake_attrs) return attrs.collection_getattr(c, name, bl);

  char fn[PATH_MAX];
  get_cdir(c, fn, sizeof(fn));
  dout(15) << "collection_getattr " << fn << " '" << name << "'" << dendl;
  char n[PATH_MAX];
  get_attrname(name, n, PATH_MAX);
  
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
  get_cdir(cid, fn, sizeof(fn));
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
  get_cdir(c, fn, sizeof(fn));
  dout(10) << "collection_setattr " << fn << " '" << name << "' len " << size << dendl;
  char n[PATH_MAX];
  get_attrname(name, n, PATH_MAX);
  int r = do_setxattr(fn, n, value, size);
  dout(10) << "collection_setattr " << fn << " '" << name << "' len " << size << " = " << r << dendl;
  return r;
}

int FileStore::_collection_rmattr(coll_t c, const char *name) 
{
  if (fake_attrs) return attrs.collection_rmattr(c, name);

  char fn[PATH_MAX];
  get_cdir(c, fn, sizeof(fn));
  dout(15) << "collection_rmattr " << fn << dendl;
  char n[PATH_MAX];
  get_attrname(name, n, PATH_MAX);
  int r = do_removexattr(fn, n);
  dout(10) << "collection_rmattr " << fn << " = " << r << dendl;
  return r;
}


int FileStore::_collection_setattrs(coll_t cid, map<nstring,bufferptr>& aset) 
{
  if (fake_attrs) return attrs.collection_setattrs(cid, aset);

  char fn[PATH_MAX];
  get_cdir(cid, fn, sizeof(fn));
  dout(15) << "collection_setattrs " << fn << dendl;
  int r = 0;
  for (map<nstring,bufferptr>::iterator p = aset.begin();
       p != aset.end();
       ++p) {
    char n[PATH_MAX];
    get_attrname(p->first.c_str(), n, PATH_MAX);
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
  snprintf(fn, sizeof(fn), "%s/current", basedir.c_str());

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
  get_cdir(c, fn, sizeof(fn));
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
  get_cdir(c, fn, sizeof(fn));
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
  get_cdir(c, fn, sizeof(fn));

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
  get_cdir(c, fn, sizeof(fn));
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
    ls[i++].swap(p->second);
  
  dout(10) << "collection_list " << fn << " = 0 (" << ls.size() << " objects)" << dendl;
  ::closedir(dir);
  return 0;
}


int FileStore::_create_collection(coll_t c) 
{
  if (fake_collections) return collections.create_collection(c);
  
  char fn[PATH_MAX];
  get_cdir(c, fn, sizeof(fn));
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
  get_cdir(c, fn, sizeof(fn));
  dout(15) << "_destroy_collection " << fn << dendl;
  int r = ::rmdir(fn);
  //char cmd[PATH_MAX];
  //snprintf(cmd, sizeof(cmd), "test -d %s && rm -r %s", fn, fn);
  //system(cmd);
  if (r < 0) r = -errno;
  dout(10) << "_destroy_collection " << fn << " = " << r << dendl;
  return r;
}


int FileStore::_collection_add(coll_t c, coll_t cid, const sobject_t& o) 
{
  if (fake_collections) return collections.collection_add(c, o);

  char cof[PATH_MAX];
  get_coname(c, o, cof, sizeof(cof));
  char of[PATH_MAX];
  get_coname(cid, o, of, sizeof(of));
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
  get_coname(c, o, cof, sizeof(cof));
  dout(15) << "collection_remove " << cof << dendl;
  int r = ::unlink(cof);
  if (r < 0) r = -errno;
  dout(10) << "collection_remove " << cof << " = " << r << dendl;
  return r;
}



// eof.
