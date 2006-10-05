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
//#include <sys/xattr.h>
//#include <sys/vfs.h>

#include "config.h"
#undef dout
#define  dout(l)    if (l<=g_conf.debug) cout << "osd" << whoami << ".fakestore "

#include "include/bufferlist.h"

#include <map>
#include <ext/hash_map>
using namespace __gnu_cxx;

// crap-a-crap hash
#define HASH_DIRS       0x80
#define HASH_MASK       0x7f
// end crap hash







int FakeStore::mount() 
{
  if (g_conf.fakestore_dev) {
    dout(0) << "mounting" << endl;
    char cmd[100];
    sprintf(cmd,"mount %s", g_conf.fakestore_dev);
    system(cmd);
  }

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

int FakeStore::umount() 
{
  dout(5) << "finalize" << endl;

  if (g_conf.fakestore_dev) {
    char cmd[100];
    dout(0) << "umounting" << endl;
    sprintf(cmd,"umount %s", g_conf.fakestore_dev);
    system(cmd);
  }

  // nothing
  return 0;
}


int FakeStore::statfs(struct statfs *buf)
{
  string mydir;
  get_dir(mydir);
  return ::statfs(mydir.c_str(), buf);
}




void FakeStore::get_dir(string& dir) {
  char s[30];
  sprintf(s, "%d", whoami);
  dir = basedir + "/" + s;
}
void FakeStore::get_oname(object_t oid, string& fn) {
  char s[100];
  static hash<object_t> H;
  sprintf(s, "%d/%02x/%016llx.%08x", whoami, H(oid) & HASH_MASK, oid.ino, oid.bno);
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
  if (g_conf.fakestore_dev) {
    dout(0) << "mounting" << endl;
    char cmd[100];
    sprintf(cmd,"mount %s", g_conf.fakestore_dev);
    system(cmd);
  }


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
  
  if (g_conf.fakestore_dev) {
    char cmd[100];
    dout(0) << "umounting" << endl;
    sprintf(cmd,"umount %s", g_conf.fakestore_dev);
    system(cmd);
  }

  dout(1) << "mkfs done in " << mydir << endl;

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
 
 

int FakeStore::remove(object_t oid, Context *onsafe) 
{
  dout(20) << "remove " << oid << endl;
  string fn;
  get_oname(oid,fn);
  int r = ::unlink(fn.c_str());
  if (onsafe) sync(onsafe);
  return r;
}

int FakeStore::truncate(object_t oid, off_t size, Context *onsafe)
{
  dout(20) << "truncate " << oid << " size " << size << endl;

  string fn;
  get_oname(oid,fn);
  int r = ::truncate(fn.c_str(), size);
  if (onsafe) sync(onsafe);
  return r;
}

int FakeStore::read(object_t oid, 
                    off_t offset, size_t len,
                    bufferlist& bl) {
  dout(20) << "read " << oid << " len " << len << " off " << offset << endl;

  string fn;
  get_oname(oid,fn);
  
  int fd = ::open(fn.c_str(), O_RDONLY);
  if (fd < 0) {
    dout(10) << "read couldn't open " << fn.c_str() << " errno " << errno << " " << strerror(errno) << endl;
    return fd;
  }
  ::flock(fd, LOCK_EX);    // lock for safety
  
  off_t actual = lseek(fd, offset, SEEK_SET);
  size_t got = 0;

  if (len == 0) {
    struct stat st;
    fstat(fd, &st);
    len = st.st_size;
  }

  if (actual == offset) {
    bufferptr bptr = new buffer(len);  // prealloc space for entire read
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
  if (onsafe) sync(onsafe);

  ::close(fd);
  
  return did;
}


class C_FakeSync : public Context {
public:
  Context *c;
  int *n;
  C_FakeSync(Context *c_, int *n_) : c(c_), n(n_) {
    ++*n;
  }
  void finish(int r) {
    c->finish(r);
    --(*n);
    //cout << "sync, " << *n << " still unsync" << endl;
  }
};

void FakeStore::sync(Context *onsafe)
{
  if (g_conf.fakestore_fake_sync) {
    g_timer.add_event_after((float)g_conf.fakestore_fake_sync,
                            new C_FakeSync(onsafe, &unsync));
    
  } else {
    assert(0); // der..no implemented anymore
  }
}



