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

#include "include/color.h"

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

#define COMMIT_SNAP_ITEM "snap_%lld"

#ifndef __CYGWIN__
# ifndef DARWIN
#  include "btrfs_ioctl.h"



# endif
#endif

#include "config.h"

#define DOUT_SUBSYS filestore
#undef dout_prefix
#define dout_prefix *_dout << dbeginl << "filestore(" << basedir << ") "

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

int FileStore::wipe_subvol(const char *s)
{
  struct btrfs_ioctl_vol_args volargs;
  memset(&volargs, 0, sizeof(volargs));
  strcpy(volargs.name, s);
  int fd = ::open(basedir.c_str(), O_RDONLY);
  if (fd < 0)
    return fd;
  int r = ::ioctl(fd, BTRFS_IOC_SNAP_DESTROY, &volargs);
  ::close(fd);
  if (r == 0) {
    dout(0) << "mkfs  removed old subvol " << s << dendl;
    return r;
  }

  dout(0) << "mkfs  removing old directory " << s << dendl;
  char cmd[PATH_MAX];
  snprintf(cmd, sizeof(cmd), "rm -r %s/%s", basedir.c_str(), s);
  system(cmd);
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
  DIR *dir = ::opendir(basedir.c_str());
  if (!dir)
    return -errno;
  struct dirent sde, *de;
  while (::readdir_r(dir, &sde, &de) == 0) {
    if (!de)
      break;
    if (strcmp(de->d_name, ".") == 0 ||
	strcmp(de->d_name, "..") == 0)
      continue;
    long long unsigned c;
    int r;
    if (sscanf(de->d_name, COMMIT_SNAP_ITEM, &c) == 1) {
      r = wipe_subvol(de->d_name);
    } else if (strcmp(de->d_name, "current") == 0) {
      r = wipe_subvol("current");
    } else {
      char path[PATH_MAX];
      snprintf(path, sizeof(path), "%s/%s", basedir.c_str(), de->d_name);
      r = ::unlink(path);
      dout(0) << "mkfs  removing old file " << de->d_name << dendl;
    }
    if (r < 0) {
      char buf[100];
      dout(0) << "problem wiping out " << de->d_name << ": " << strerror_r(errno, buf, sizeof(buf)) << dendl;
      return -errno;
    }
  }
  ::closedir(dir);
 
  
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
  // read fsid
  char fn[PATH_MAX];
  snprintf(fn, sizeof(fn), "%s/fsid", basedir.c_str());
  int fd = ::open(fn, O_RDONLY, 0644);
  ::read(fd, &fsid, sizeof(fsid));
  ::close(fd);

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
    derr(0) << "lock_fsid failed to lock " << basedir << "/fsid, is another cosd still running? " << strerror_r(errno, buf, sizeof(buf)) << dendl;
    return -errno;
  }
  return 0;
}

bool FileStore::test_mount_in_use()
{
  dout(5) << "test_mount basedir " << basedir << " journal " << journalpath << dendl;
  char fn[PATH_MAX];
  snprintf(fn, sizeof(fn), "%s/fsid", basedir.c_str());

  // verify fs isn't in use

  fsid_fd = ::open(fn, O_RDWR, 0644);
  if (fsid_fd < 0)
    return 0;   // no fsid, ok.
  bool inuse = lock_fsid() < 0;
  ::close(fsid_fd);
  fsid_fd = -1;
  return inuse;
}

int FileStore::_detect_fs()
{
  char buf[80];
  
  // fake collections?
  if (g_conf.filestore_fake_collections) {
    dout(0) << "faking collections (in memory)" << dendl;
    fake_collections = true;
  }

  // xattrs?
  if (g_conf.filestore_fake_attrs) {
    dout(0) << "faking xattrs (in memory)" << dendl;
    fake_attrs = true;
  } else {
    char fn[PATH_MAX];
    int x = rand();
    int y = x+1;
    snprintf(fn, sizeof(fn), "%s/fsid", basedir.c_str());
    do_setxattr(fn, "user.test", &x, sizeof(x));
    do_getxattr(fn, "user.test", &y, sizeof(y));
    /*dout(10) << "x = " << x << "   y = " << y 
	     << "  r1 = " << r1 << "  r2 = " << r2
	     << " " << strerror(errno)
	     << dendl;*/
    if (x != y) {
      derr(0) << "xattrs don't appear to work (" << strerror_r(errno, buf, sizeof(buf))
	      << ") on " << fn << ", be sure to mount underlying file system with 'user_xattr' option" << dendl;
      return -errno;
    }
  }

  // btrfs?
  int fd = ::open(basedir.c_str(), O_RDONLY);
  if (fd < 0)
    return -errno;

  struct statfs st;
  int r = ::fstatfs(fd, &st);
  if (r == 0 && st.f_type == 0x9123683E) {
    dout(0) << "mount detected btrfs" << dendl;      
    btrfs = true;

    // clone_range?
    btrfs_clone_range = true;
    int r = _do_clone_range(fsid_fd, -1, 0, 1);
    if (r == -EBADF) {
      dout(0) << "mount btrfs CLONE_RANGE ioctl is supported" << dendl;
    } else {
      btrfs_clone_range = false;
      dout(0) << "mount btrfs CLONE_RANGE ioctl is NOT supported: " << strerror_r(-r, buf, sizeof(buf)) << dendl;
    }

    // snap_create and snap_destroy?
    struct btrfs_ioctl_vol_args volargs;
    volargs.fd = fd;
    strcpy(volargs.name, "sync_snap_test");
    r = ::ioctl(fd, BTRFS_IOC_SNAP_CREATE, &volargs);
    if (r == 0 || errno == EEXIST) {
      dout(0) << "mount btrfs SNAP_CREATE is supported" << dendl;
      btrfs_snap_create = true;

      r = ::ioctl(fd, BTRFS_IOC_SNAP_DESTROY, &volargs);
      if (r == 0) {
	dout(0) << "mount btrfs SNAP_DESTROY is supported" << dendl;
	btrfs_snap_destroy = true;
      } else {
	dout(0) << "mount btrfs SNAP_DESTROY failed: " << strerror_r(-r, buf, sizeof(buf)) << dendl;
      }
    } else {
      dout(0) << "mount btrfs SNAP_CREATE failed: " << strerror_r(-r, buf, sizeof(buf)) << dendl;
    }

    if (g_conf.filestore_btrfs_snap && !btrfs_snap_destroy) {
      dout(0) << "mount btrfs snaps enabled, but no SNAP_DESTROY ioctl (from kernel 2.6.32+)" << dendl;
      cerr << TEXT_RED
	   << " ** ERROR: 'filestore btrfs snap' is enabled (for safe transactions, rollback),\n"
	   << "           but btrfs does not support the SNAP_DESTROY ioctl (added in\n"
	   << "           Linux 2.6.32).\n"
	   << TEXT_NORMAL;
      return -ENOTTY;
    }
  } else {
    dout(0) << "mount did NOT detect btrfs" << dendl;
    btrfs = false;
  }
  ::close(fd);
  return 0;
}

int FileStore::_sanity_check_fs()
{
  // sanity check(s)
  if (!btrfs) {
    if (!journal || !g_conf.filestore_journal_writeahead) {
      dout(0) << "mount WARNING: no btrfs, and no journal in writeahead mode; data may be lost" << dendl;
      cerr << TEXT_RED 
	   << " ** WARNING: no btrfs AND (no journal OR journal not in writeahead mode)\n"
	   << "             For non-btrfs volumes, a writeahead journal is required to\n"
	   << "             maintain on-disk consistency in the event of a crash.  Your conf\n"
	   << "             should include something like:\n"
	   << "        osd journal = /path/to/journal_device_or_file\n"
	   << "        filestore journal writeahead = true\n"
	   << TEXT_NORMAL;
    }

    // ext3?
    struct statfs buf;
    int r = ::statfs(basedir.c_str(), &buf);
    if (r == 0 && buf.f_type != 0xEF53 /*EXT3_SUPER_MAGIC*/) {
      dout(0) << "mount WARNING: not btrfs or ext3; data may be lost" << dendl;
      cerr << TEXT_YELLOW
	   << " ** WARNING: not btrfs or ext3.  We don't currently support file systems other\n"
	   << "             than btrfs and ext3 (data=journal or data=ordered).  Data may be\n"
	   << "             lost in the event of a crash.\n"
	   << TEXT_NORMAL;
    }    
  }

  if (!journal) {
    dout(0) << "mount WARNING: no journal" << dendl;
    cerr << TEXT_YELLOW
	 << " ** WARNING: No osd journal is configured: write latency may be high.\n"
	 << "             If you will not be using an osd journal, write latency may be\n"
	 << "             relatively high.  It can be reduced somewhat by lowering\n"
	 << "             filestore_max_sync_interval, but lower values mean lower write\n"
	 << "             throughput, especially with spinning disks.\n"
	 << TEXT_NORMAL;
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
  
  // test for btrfs, xattrs, etc.
  r = _detect_fs();
  if (r < 0)
    return r;

  // get fsid
  char fn[PATH_MAX];
  snprintf(fn, sizeof(fn), "%s/fsid", basedir.c_str());
  fsid_fd = ::open(fn, O_RDWR|O_CREAT, 0644);
  ::read(fsid_fd, &fsid, sizeof(fsid));

  if (lock_fsid() < 0)
    return -EBUSY;

  dout(10) << "mount fsid is " << fsid << dendl;

  // open some dir handles
  basedir_fd = ::open(basedir.c_str(), O_RDONLY);

  // roll back?
  if (true) {
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

    dout(0) << "mount found snaps " << snaps << dendl;
  }
  if (btrfs && g_conf.filestore_btrfs_snap) {
    if (snaps.empty()) {
      dout(0) << "mount WARNING: no consistent snaps found, store may be in inconsistent state" << dendl;
    } else if (!btrfs) {
      dout(0) << "mount WARNING: not btrfs, store may be in inconsistent state" << dendl;
    } else {
      uint64_t cp = snaps.back();
      btrfs_ioctl_vol_args snapargs;

      // drop current
      snapargs.fd = 0;
      strcpy(snapargs.name, "current");
      int r = ::ioctl(basedir_fd, BTRFS_IOC_SNAP_DESTROY, &snapargs);
      if (r) {
	char buf[80];
	dout(0) << "error removing old current subvol: " << strerror_r(errno, buf, sizeof(buf)) << dendl;
	char s[PATH_MAX];
	snprintf(s, sizeof(s), "%s/current.remove.me.%d", basedir.c_str(), rand());
	r = ::rename(current_fn, s);
	if (r) {
	  dout(0) << "error renaming old current subvol: " << strerror_r(errno, buf, sizeof(buf)) << dendl;
	  return -errno;
	}
      }
      assert(r == 0);
      
      // roll back
      char s[PATH_MAX];
      snprintf(s, sizeof(s), "%s/" COMMIT_SNAP_ITEM, basedir.c_str(), (long long unsigned)cp);
      snapargs.fd = ::open(s, O_RDONLY);
      r = ::ioctl(basedir_fd, BTRFS_IOC_SNAP_CREATE, &snapargs);
      assert(r == 0);
      ::close(snapargs.fd);
      dout(10) << "mount rolled back to consistent snap " << cp << dendl;
      snaps.pop_back();
    }
  }

  current_fd = ::open(current_fn, O_RDONLY);
  assert(current_fd >= 0);

  // init op_seq
  op_fd = ::open(current_op_seq_fn, O_CREAT|O_RDWR, 0644);
  assert(op_fd >= 0);

  uint64_t initial_op_seq = 0;
  {
    char s[40];
    int l = ::read(op_fd, s, sizeof(s));
    s[l] = 0;
    initial_op_seq = atoll(s);
  }
  dout(5) << "mount op_seq is " << initial_op_seq << dendl;

  // journal
  open_journal();
  r = journal_replay(initial_op_seq);
  if (r < 0) {
    char buf[40];
    dout(0) << "mount failed to open journal " << journalpath << ": "
	    << strerror_r(-r, buf, sizeof(buf)) << dendl;
    cerr << "mount failed to open journal " << journalpath << ": "
	 << strerror_r(-r, buf, sizeof(buf)) << std::endl;
    return r;
  }
  journal_start();
  sync_thread.create();
  op_tp.start();
  flusher_thread.create();
  op_finisher.start();
  ondisk_finisher.start();

  if (journal && g_conf.filestore_journal_writeahead)
    journal->set_wait_on_full(true);

  _sanity_check_fs();

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
  lock.Unlock();
  sync_thread.join();
  op_tp.stop();
  flusher_thread.join();

  journal_stop();

  op_finisher.stop();
  ondisk_finisher.stop();

  ::close(fsid_fd);
  ::close(op_fd);
  ::close(current_fd);
  ::close(basedir_fd);

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

void FileStore::queue_op(Sequencer *posr, uint64_t op_seq, list<Transaction*>& tls, Context *onreadable, Context *onreadable_sync)
{
  uint64_t bytes = 0, ops = 0;
  for (list<Transaction*>::iterator p = tls.begin();
       p != tls.end();
       p++) {
    bytes += (*p)->get_num_bytes();
    ops += (*p)->get_num_ops();
  }

  // initialize next_finish on first op
  if (next_finish == 0)
    next_finish = op_seq;

  Op *o = new Op;
  o->op = op_seq;
  o->tls.swap(tls);
  o->onreadable = onreadable;
  o->onreadable_sync = onreadable_sync;
  o->ops = ops;
  o->bytes = bytes;

  op_tp.lock();

  OpSequencer *osr;
  if (!posr)
    posr = &default_osr;
  if (posr->p) {
    osr = (OpSequencer *)posr->p;
    dout(10) << "queue_op existing osr " << osr << "/" << osr->parent << dendl; //<< " w/ q " << osr->q << dendl;
  } else {
    osr = new OpSequencer;
    osr->parent = posr;
    posr->p = (void *)osr;
    dout(10) << "queue_op new osr " << osr << "/" << osr->parent << dendl;
  }
  osr->q.push_back(o);

  while ((g_conf.filestore_queue_max_ops && op_queue_len >= (unsigned)g_conf.filestore_queue_max_ops) ||
	 (g_conf.filestore_queue_max_bytes && op_queue_bytes >= (unsigned)g_conf.filestore_queue_max_bytes)) {
    dout(2) << "queue_op " << o << " throttle: "
	     << op_queue_len << " > " << g_conf.filestore_queue_max_ops << " ops || "
	     << op_queue_bytes << " > " << g_conf.filestore_queue_max_bytes << dendl;
    op_tp.wait(op_throttle_cond);
  }
  op_tp.unlock();

  dout(10) << "queue_op " << o << " seq " << op_seq << " " << bytes << " bytes" << dendl;
  op_wq.queue(osr);
}

void FileStore::_do_op(OpSequencer *osr)
{
  osr->lock.Lock();
  Op *o = osr->q.front();

  dout(10) << "_do_op " << o << " " << o->op << " osr " << osr << "/" << osr->parent << " start" << dendl;
  op_apply_start(o->op);
  int r = do_transactions(o->tls, o->op);
  op_apply_finish();
  dout(10) << "_do_op " << o << " " << o->op << " r = " << r
	   << ", finisher " << o->onreadable << " " << o->onreadable_sync << dendl;
  
  /*dout(10) << "op_entry finished " << o->bytes << " bytes, queue now "
	   << op_queue_len << " ops, " << op_queue_bytes << " bytes" << dendl;
  */
}

void FileStore::_finish_op(OpSequencer *osr)
{
  Op *o = osr->q.front();
  osr->q.pop_front();
  
  if (osr->q.empty()) {
    dout(10) << "_finish_op last op " << o << " on osr " << osr << "/" << osr->parent << dendl;
    osr->parent->p = NULL;
    osr->lock.Unlock();  // locked in _do_op
    delete osr;
  } else {
    dout(10) << "_finish_op on osr " << osr << "/" << osr->parent << dendl; // << " q now " << osr->q << dendl;
    osr->lock.Unlock();  // locked in _do_op
  }

  // called with tp lock held
  op_queue_len--;
  op_queue_bytes -= o->bytes;
  op_throttle_cond.Signal();

  if (next_finish == o->op) {
    dout(10) << "_finish_op " << o->op << " next_finish " << next_finish
	     << " queueing " << o->onreadable << " doing " << o->onreadable_sync << dendl;
    next_finish++;
    if (o->onreadable_sync) {
      o->onreadable_sync->finish(0);
      delete o->onreadable_sync;
    }
    op_finisher.queue(o->onreadable);

    while (finish_queue.begin()->first == next_finish) {
      Context *c = finish_queue.begin()->second.first;
      Context *s = finish_queue.begin()->second.second;
      finish_queue.erase(finish_queue.begin());
      dout(10) << "_finish_op " << o->op << " next_finish " << next_finish
	       << " queueing delayed " << c << " doing " << s << dendl;
      if (s) {
	s->finish(0);
	delete s;
      }
      op_finisher.queue(c);
      next_finish++;
    }
  } else {
    dout(10) << "_finish_op " << o->op << " next_finish " << next_finish
	     << ", delaying " << o->onreadable << dendl;
    finish_queue[o->op] = pair<Context*,Context*>(o->onreadable, o->onreadable_sync);
  }

  delete o;
}


struct C_JournaledAhead : public Context {
  FileStore *fs;
  ObjectStore::Sequencer *osr;
  uint64_t op;
  list<ObjectStore::Transaction*> tls;
  Context *onreadable, *onreadable_sync;
  Context *ondisk;

  C_JournaledAhead(FileStore *f, ObjectStore::Sequencer *os, uint64_t o, list<ObjectStore::Transaction*>& t,
		   Context *onr, Context *ond, Context *onrsync) :
    fs(f), osr(os), op(o), tls(t), onreadable(onr), onreadable_sync(onrsync), ondisk(ond) { }
  void finish(int r) {
    fs->_journaled_ahead(osr, op, tls, onreadable, ondisk, onreadable_sync);
  }
};

int FileStore::queue_transaction(Sequencer *osr, Transaction *t)
{
  list<Transaction*> tls;
  tls.push_back(t);
  return queue_transactions(osr, tls, new C_DeleteTransaction(t));
}

int FileStore::queue_transactions(Sequencer *osr, list<Transaction*> &tls,
				  Context *onreadable, Context *ondisk,
				  Context *onreadable_sync)
{
  if (journal && journal->is_writeable()) {
    if (g_conf.filestore_journal_parallel) {

      journal->throttle();

      uint64_t op = op_journal_start(0);
      dout(10) << "queue_transactions (parallel) " << op << " " << tls << dendl;
      
      journal_transactions(tls, op, ondisk);
      
      // queue inside journal lock, to preserve ordering
      queue_op(osr, op, tls, onreadable, onreadable_sync);
      
      op_journal_finish();
      return 0;
    }
    else if (g_conf.filestore_journal_writeahead) {
      uint64_t op = op_journal_start(0);
      dout(10) << "queue_transactions (writeahead) " << op << " " << tls << dendl;
      journal_transactions(tls, op,
			   new C_JournaledAhead(this, osr, op, tls, onreadable, ondisk, onreadable_sync));
      op_journal_finish();
      return 0;
    }
  }

  uint64_t op_seq = op_apply_start(0);
  dout(10) << "queue_transactions (trailing journal) " << op_seq << " " << tls << dendl;
  int r = do_transactions(tls, op_seq);
  op_apply_finish();
    
  if (r >= 0) {
    op_journal_start(op_seq);
    journal_transactions(tls, op_seq, ondisk);
    op_journal_finish();
  } else {
    delete ondisk;
  }

  // start on_readable finisher after we queue journal item, as on_readable callback
  // is allowed to delete the Transaction
  if (onreadable_sync) {
    onreadable_sync->finish(r);
    delete onreadable_sync;
  }
  op_finisher.queue(onreadable, r);

  return r;
}

void FileStore::_journaled_ahead(Sequencer *osr, uint64_t op,
				 list<Transaction*> &tls,
				 Context *onreadable, Context *ondisk,
				 Context *onreadable_sync)
{
  dout(10) << "_journaled_ahead " << op << " " << tls << dendl;
  // this should queue in order because the journal does it's completions in order.
  queue_op(osr, op, tls, onreadable, onreadable_sync);

  // do ondisk completions async, to prevent any onreadable_sync completions
  // getting blocked behind an ondisk completion.
  if (ondisk) {
    dout(10) << " queueing ondisk " << ondisk << dendl;
    ondisk_finisher.queue(ondisk);
  }
}

int FileStore::do_transactions(list<Transaction*> &tls, uint64_t op_seq)
{
  int r = 0;

  uint64_t bytes = 0, ops = 0;
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
  
  _transaction_finish(id);
  return r;
}

unsigned FileStore::apply_transaction(Transaction &t,
				      Context *ondisk)
{
  list<Transaction*> tls;
  tls.push_back(&t);
  return apply_transactions(tls, ondisk);
}

unsigned FileStore::apply_transactions(list<Transaction*> &tls,
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
    queue_transactions(NULL, tls, onreadable, ondisk);
    
    my_lock.Lock();
    while (!done)
      my_cond.Wait(my_lock);
    my_lock.Unlock();
    dout(10) << "apply done r = " << r << dendl;
  } else {
    uint64_t op_seq = op_apply_start(0);
    r = do_transactions(tls, op_seq);
    op_apply_finish();

    if (r >= 0) {
      op_journal_start(op_seq);
      journal_transactions(tls, op_seq, ondisk);
      op_journal_finish();
    } else {
      delete ondisk;
    }
  }
  return r;
}


// btrfs transaction start/end interface

int FileStore::_transaction_start(uint64_t bytes, uint64_t ops)
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
      {
	coll_t cid = t.get_cid();
	sobject_t oid = t.get_oid();
	_touch(cid, oid);
      }
      break;
      
    case Transaction::OP_WRITE:
      {
	coll_t cid = t.get_cid();
	sobject_t oid = t.get_oid();
	uint64_t off = t.get_length();
	uint64_t len = t.get_length();
	bufferlist bl;
	t.get_bl(bl);
	_write(cid, oid, off, len, bl);
      }
      break;
      
    case Transaction::OP_ZERO:
      {
	coll_t cid = t.get_cid();
	sobject_t oid = t.get_oid();
	uint64_t off = t.get_length();
	uint64_t len = t.get_length();
	_zero(cid, oid, off, len);
      }
      break;
      
    case Transaction::OP_TRIMCACHE:
      {
	coll_t cid = t.get_cid();
	sobject_t oid = t.get_oid();
	uint64_t off = t.get_length();
	uint64_t len = t.get_length();
	trim_from_cache(cid, oid, off, len);
      }
      break;
      
    case Transaction::OP_TRUNCATE:
      {
	coll_t cid = t.get_cid();
	sobject_t oid = t.get_oid();
	uint64_t off = t.get_length();
	_truncate(cid, oid, off);
      }
      break;
      
    case Transaction::OP_REMOVE:
      {
	coll_t cid = t.get_cid();
	sobject_t oid = t.get_oid();
	_remove(cid, oid);
      }
      break;
      
    case Transaction::OP_SETATTR:
      {
	coll_t cid = t.get_cid();
	sobject_t oid = t.get_oid();
	string name = t.get_attrname();
	bufferlist bl;
	t.get_bl(bl);
	_setattr(cid, oid, name.c_str(), bl.c_str(), bl.length());
      }
      break;
      
    case Transaction::OP_SETATTRS:
      {
	coll_t cid = t.get_cid();
	sobject_t oid = t.get_oid();
	map<nstring, bufferptr> aset;
	t.get_attrset(aset);
	_setattrs(cid, oid, aset);
      }
      break;

    case Transaction::OP_RMATTR:
      {
	coll_t cid = t.get_cid();
	sobject_t oid = t.get_oid();
	string name = t.get_attrname();
	_rmattr(cid, oid, name.c_str());
      }
      break;

    case Transaction::OP_RMATTRS:
      {
	coll_t cid = t.get_cid();
	sobject_t oid = t.get_oid();
	_rmattrs(cid, oid);
      }
      break;
      
    case Transaction::OP_CLONE:
      {
	coll_t cid = t.get_cid();
	sobject_t oid = t.get_oid();
	sobject_t noid = t.get_oid();
	_clone(cid, oid, noid);
      }
      break;

    case Transaction::OP_CLONERANGE:
      {
	coll_t cid = t.get_cid();
	sobject_t oid = t.get_oid();
	sobject_t noid = t.get_oid();
 	uint64_t off = t.get_length();
	uint64_t len = t.get_length();
	_clone_range(cid, oid, noid, off, len);
      }
      break;

    case Transaction::OP_MKCOLL:
      {
	coll_t cid = t.get_cid();
	_create_collection(cid);
      }
      break;

    case Transaction::OP_RMCOLL:
      {
	coll_t cid = t.get_cid();
	_destroy_collection(cid);
      }
      break;

    case Transaction::OP_COLL_ADD:
      {
	coll_t ocid = t.get_cid();
	coll_t ncid = t.get_cid();
	sobject_t oid = t.get_oid();
	_collection_add(ocid, ncid, oid);
      }
      break;

    case Transaction::OP_COLL_REMOVE:
       {
	coll_t cid = t.get_cid();
	sobject_t oid = t.get_oid();
	_collection_remove(cid, oid);
       }
      break;

    case Transaction::OP_COLL_SETATTR:
      {
	coll_t cid = t.get_cid();
	string name = t.get_attrname();
	bufferlist bl;
	t.get_bl(bl);
	_collection_setattr(cid, name.c_str(), bl.c_str(), bl.length());
      }
      break;

    case Transaction::OP_COLL_RMATTR:
      {
	coll_t cid = t.get_cid();
	string name = t.get_attrname();
	_collection_rmattr(cid, name.c_str());
      }
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
                    uint64_t offset, size_t len,
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
    uint64_t actual = ::lseek64(fd, offset, SEEK_SET);
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

int FileStore::_truncate(coll_t cid, const sobject_t& oid, uint64_t size)
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
                     uint64_t offset, size_t len,
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
    uint64_t actual = ::lseek64(fd, offset, SEEK_SET);
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

#ifdef HAVE_SYNC_FILE_RANGE
    if (!g_conf.filestore_flusher ||
	!queue_flusher(fd, offset, len)) {
      if (g_conf.filestore_sync_flush)
	::sync_file_range(fd, offset, len, SYNC_FILE_RANGE_WRITE);
      ::close(fd);
    }
#else
    ::close(fd);
#endif
    r = did;
  }

  dout(10) << "write " << fn << " " << offset << "~" << len << " = " << r << dendl;
  return r;
}

int FileStore::_zero(coll_t cid, const sobject_t& oid, uint64_t offset, size_t len)
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

int FileStore::_do_clone_range(int from, int to, uint64_t off, uint64_t len)
{
  dout(20) << "_do_clone_range " << off << "~" << len << dendl;
  int r = 0;
  
  if (btrfs_clone_range) {
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

int FileStore::_clone_range(coll_t cid, const sobject_t& oldoid, const sobject_t& newoid, uint64_t off, uint64_t len)
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


bool FileStore::queue_flusher(int fd, uint64_t off, uint64_t len)
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
#ifdef HAVE_SYNC_FILE_RANGE
      list<uint64_t> q;
      q.swap(flusher_queue);

      int num = flusher_queue_len;  // see how many we're taking, here

      lock.Unlock();
      while (!q.empty()) {
	uint64_t ep = q.front();
	q.pop_front();
	int fd = q.front();
	q.pop_front();
	uint64_t off = q.front();
	q.pop_front();
	uint64_t len = q.front();
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
#endif
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
      uint64_t cp = op_seq;

      // make flusher stop flushing previously queued stuff
      sync_epoch++;

      dout(15) << "sync_entry committing " << cp << " sync_epoch " << sync_epoch << dendl;
      char s[30];
      sprintf(s, "%lld\n", (long long unsigned)cp);
      ::pwrite(op_fd, s, strlen(s), 0);

      bool do_snap = btrfs && g_conf.filestore_btrfs_snap;

      if (do_snap) {
	btrfs_ioctl_vol_args snapargs;
	snapargs.fd = current_fd;
	snprintf(snapargs.name, sizeof(snapargs.name), COMMIT_SNAP_ITEM, (long long unsigned)cp);
	dout(10) << "taking snap '" << snapargs.name << "'" << dendl;
	int r = ::ioctl(basedir_fd, BTRFS_IOC_SNAP_CREATE, &snapargs);
	if (r) {
	  char buf[100];
	  dout(0) << "snap create '" << snapargs.name << "' got " << r
		  << " " << strerror_r(r < 0 ? errno : 0, buf, sizeof(buf)) << dendl;
	  assert(r == 0);
	}
	snaps.push_back(cp);
      }

      commit_started();

      if (!do_snap) {
	if (btrfs) {
	  dout(15) << "sync_entry doing btrfs sync" << dendl;
	  // do a full btrfs commit
	  ::ioctl(op_fd, BTRFS_IOC_SYNC);
	} else {
	  dout(15) << "sync_entry doing fsync on " << current_op_seq_fn << dendl;
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
      if (do_snap) {
	while (snaps.size() > 2) {
	  btrfs_ioctl_vol_args snapargs;
	  snapargs.fd = 0;
	  snprintf(snapargs.name, sizeof(snapargs.name), COMMIT_SNAP_ITEM, (long long unsigned)snaps.front());
	  snaps.pop_front();
	  dout(10) << "removing snap '" << snapargs.name << "'" << dendl;
	  int r = ::ioctl(basedir_fd, BTRFS_IOC_SNAP_DESTROY, &snapargs);
	  if (r) {
	    char buf[100];
	    dout(20) << "unable to destroy snap '" << snapargs.name << "' got " << r
		     << " " << strerror_r(r < 0 ? errno : 0, buf, sizeof(buf)) << dendl;
	  }
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

void FileStore::_flush_op_queue()
{
  dout(10) << "_flush_op_queue draining op tp" << dendl;
  op_wq.drain();
  dout(10) << "_flush_op_queue waiting for apply finisher" << dendl;
  op_finisher.wait_for_empty();
}

/*
 * flush - make every queued write readable
 */
void FileStore::flush()
{
  dout(10) << "flush" << dendl;
 
  if (g_conf.filestore_journal_writeahead) {
    if (journal)
      journal->flush();
    dout(10) << "flush draining ondisk finisher" << dendl;
    ondisk_finisher.wait_for_empty();
  }

  _flush_op_queue();
  dout(10) << "flush complete" << dendl;
}

/*
 * sync_and_flush - make every queued write readable AND committed to disk
 */
void FileStore::sync_and_flush()
{
  dout(10) << "sync_and_flush" << dendl;

  if (g_conf.filestore_journal_writeahead) {
    if (journal)
      journal->flush();
    _flush_op_queue();
  } else if (g_conf.filestore_journal_parallel) {
    _flush_op_queue();
    sync();
  } else {
    _flush_op_queue();
    sync();
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
