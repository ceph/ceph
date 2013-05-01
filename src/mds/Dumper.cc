// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2010 Greg Farnum <gregf@hq.newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software 
 * Foundation.  See file COPYING.
 * 
 */

#ifndef _BACKWARD_BACKWARD_WARNING_H
#define _BACKWARD_BACKWARD_WARNING_H   // make gcc 4.3 shut up about hash_*
#endif

#include "include/compat.h"
#include "common/entity_name.h"
#include "common/errno.h"
#include "common/safe_io.h"
#include "mds/Dumper.h"
#include "mds/mdstypes.h"
#include "mon/MonClient.h"
#include "osdc/Journaler.h"

#define dout_subsys ceph_subsys_mds

Dumper::~Dumper()
{
}

bool Dumper::ms_get_authorizer(int dest_type, AuthAuthorizer **authorizer,
                         bool force_new)
{
  if (dest_type == CEPH_ENTITY_TYPE_MON)
    return true;

  if (force_new) {
    if (monc->wait_auth_rotating(10) < 0)
      return false;
  }

  *authorizer = monc->auth->build_authorizer(dest_type);
  return *authorizer != NULL;
}

void Dumper::init(int rank) 
{
  inodeno_t ino = MDS_INO_LOG_OFFSET + rank;
  unsigned pg_pool = CEPH_METADATA_RULE;
  osdmap = new OSDMap();
  objecter = new Objecter(g_ceph_context, messenger, monc, osdmap, lock, timer);
  journaler = new Journaler(ino, pg_pool, CEPH_FS_ONDISK_MAGIC,
                                       objecter, 0, 0, &timer);

  objecter->set_client_incarnation(0);

  messenger->add_dispatcher_head(this);
  messenger->start();

  monc->set_want_keys(CEPH_ENTITY_TYPE_MON|CEPH_ENTITY_TYPE_OSD|CEPH_ENTITY_TYPE_MDS);
  monc->set_messenger(messenger);
  monc->init();
  monc->authenticate();

  client_t whoami = monc->get_global_id();
  messenger->set_myname(entity_name_t::CLIENT(whoami.v));

  objecter->init_unlocked();
  lock.Lock();
  objecter->init_locked();
  lock.Unlock();
  objecter->wait_for_osd_map();
  timer.init();
}

void Dumper::shutdown()
{
  lock.Lock();
  timer.shutdown();
  objecter->shutdown_locked();
  lock.Unlock();
  objecter->shutdown_unlocked();
}

void Dumper::dump(const char *dump_file)
{
  bool done = false;
  Cond cond;
  int r = 0;
  int rank = strtol(g_conf->name.get_id().c_str(), 0, 0);
  inodeno_t ino = MDS_INO_LOG_OFFSET + rank;

  Mutex localLock("dump:lock");
  lock.Lock();
  journaler->recover(new C_SafeCond(&localLock, &cond, &done, &r));
  lock.Unlock();
  localLock.Lock();
  while (!done)
    cond.Wait(localLock);
  localLock.Unlock();

  if (r < 0) { // Error
    derr << "error on recovery: " << cpp_strerror(r) << dendl;
    messenger->shutdown();
    // wait for messenger to finish
    messenger->wait();
    shutdown();
  } else {
    dout(10) << "completed journal recovery" << dendl;
  }

  uint64_t start = journaler->get_read_pos();
  uint64_t end = journaler->get_write_pos();
  uint64_t len = end-start;
  cout << "journal is " << start << "~" << len << std::endl;

  Filer filer(objecter);
  bufferlist bl;
  lock.Lock();
  filer.read(ino, &journaler->get_layout(), CEPH_NOSNAP,
             start, len, &bl, 0, new C_SafeCond(&localLock, &cond, &done));
  lock.Unlock();
  localLock.Lock();
  while (!done)
    cond.Wait(localLock);
  localLock.Unlock();

  cout << "read " << bl.length() << " bytes at offset " << start << std::endl;

  int fd = ::open(dump_file, O_WRONLY|O_CREAT|O_TRUNC, 0644);
  if (fd >= 0) {
    // include an informative header
    char buf[200];
    memset(buf, 0, sizeof(buf));
    sprintf(buf, "Ceph mds%d journal dump\n start offset %llu (0x%llx)\n       length %llu (0x%llx)\n%c",
	    rank, 
	    (unsigned long long)start, (unsigned long long)start,
	    (unsigned long long)bl.length(), (unsigned long long)bl.length(),
	    4);
    int r = safe_write(fd, buf, sizeof(buf));
    if (r)
      ceph_abort();

    // write the data
    ::lseek64(fd, start, SEEK_SET);
    bl.write_fd(fd);
    ::close(fd);

    cout << "wrote " << bl.length() << " bytes at offset " << start << " to " << dump_file << "\n"
	 << "NOTE: this is a _sparse_ file; you can\n"
	 << "\t$ tar cSzf " << dump_file << ".tgz " << dump_file << "\n"
	 << "      to efficiently compress it while preserving sparseness." << std::endl;
  } else {
    int err = errno;
    derr << "unable to open " << dump_file << ": " << cpp_strerror(err) << dendl;
  }

  messenger->shutdown();

  // wait for messenger to finish
  messenger->wait();

  shutdown();
}

void Dumper::undump(const char *dump_file)
{
  cout << "undump " << dump_file << std::endl;
  
  int fd = ::open(dump_file, O_RDONLY);
  if (fd < 0) {
    derr << "couldn't open " << dump_file << ": " << cpp_strerror(errno) << dendl;
    return;
  }

  // Ceph mds0 journal dump
  //  start offset 232401996 (0xdda2c4c)
  //        length 1097504 (0x10bf20)

  char buf[200];
  int r = safe_read(fd, buf, sizeof(buf));
  if (r < 0) {
    TEMP_FAILURE_RETRY(::close(fd));
    return;
  }

  long long unsigned start, len;
  sscanf(strstr(buf, "start offset"), "start offset %llu", &start);
  sscanf(strstr(buf, "length"), "length %llu", &len);

  cout << "start " << start << " len " << len << std::endl;
  
  inodeno_t ino = MDS_INO_LOG_OFFSET + rank;
  unsigned pg_pool = CEPH_METADATA_RULE;

  Journaler::Header h;
  h.trimmed_pos = start;
  h.expire_pos = start;
  h.write_pos = start+len;
  h.magic = CEPH_FS_ONDISK_MAGIC;

  h.layout = g_default_file_layout;
  h.layout.fl_pg_pool = pg_pool;
  
  bufferlist hbl;
  ::encode(h, hbl);

  object_t oid = file_object_t(ino, 0);
  object_locator_t oloc(pg_pool);
  SnapContext snapc;

  bool done = false;
  Cond cond;
  
  cout << "writing header " << oid << std::endl;
  objecter->write_full(oid, oloc, snapc, hbl, ceph_clock_now(g_ceph_context), 0, 
		       NULL, 
		       new C_SafeCond(&lock, &cond, &done));

  lock.Lock();
  while (!done)
    cond.Wait(lock);
  lock.Unlock();
  
  // read
  Filer filer(objecter);
  uint64_t pos = start;
  uint64_t left = len;
  while (left > 0) {
    bufferlist j;
    lseek64(fd, pos, SEEK_SET);
    uint64_t l = MIN(left, 1024*1024);
    j.read_fd(fd, l);
    cout << " writing " << pos << "~" << l << std::endl;
    filer.write(ino, &h.layout, snapc, pos, l, j, ceph_clock_now(g_ceph_context), 0, NULL, new C_SafeCond(&lock, &cond, &done));

    lock.Lock();
    while (!done)
      cond.Wait(lock);
    lock.Unlock();
    
    pos += l;
    left -= l;
  }

  TEMP_FAILURE_RETRY(::close(fd));
  cout << "done." << std::endl;
}


