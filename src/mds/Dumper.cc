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

#include "common/entity_name.h"
#include "common/errno.h"
#include "common/safe_io.h"
#include "mds/Dumper.h"
#include "mds/mdstypes.h"
#include "mon/MonClient.h"
#include "osdc/Journaler.h"

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
  objecter = new Objecter(messenger, monc, osdmap, lock, timer);
  journaler = new Journaler(ino, pg_pool, CEPH_FS_ONDISK_MAGIC,
                                       objecter, 0, 0, &timer);

  objecter->set_client_incarnation(0);

  messenger->register_entity(entity_name_t::CLIENT());
  messenger->add_dispatcher_head(this);
  messenger->start(true);

  monc->set_want_keys(CEPH_ENTITY_TYPE_MON|CEPH_ENTITY_TYPE_OSD|CEPH_ENTITY_TYPE_MDS);
  monc->set_messenger(messenger);
  monc->init();
  monc->authenticate();

  client_t whoami = monc->get_global_id();
  messenger->set_myname(entity_name_t::CLIENT(whoami.v));

  lock.Lock();
  objecter->init();
  objecter->wait_for_osd_map();
  timer.init();
  lock.Unlock();
}

void Dumper::shutdown()
{
  lock.Lock();
  timer.shutdown();
  lock.Unlock();
}

void Dumper::dump(const char *dump_file)
{
  bool done = false;
  Cond cond;
  int rank = strtol(g_conf.name->get_id().c_str(), 0, 0);
  inodeno_t ino = MDS_INO_LOG_OFFSET + rank;

  lock.Lock();
  journaler->recover(new C_SafeCond(&lock, &cond, &done));
  while (!done)
    cond.Wait(lock);
  lock.Unlock();

  uint64_t start = journaler->get_read_pos();
  uint64_t end = journaler->get_write_pos();
  uint64_t len = end-start;
  cout << "journal is " << start << "~" << len << std::endl;

  Filer filer(objecter);
  bufferlist bl;
  filer.read(ino, &journaler->get_layout(), CEPH_NOSNAP,
             start, len, &bl, 0, new C_SafeCond(&lock, &cond, &done));
  lock.Lock();
  while (!done)
    cond.Wait(lock);
  lock.Unlock();

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
