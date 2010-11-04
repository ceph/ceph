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

#include "mds/Dumper.h"
#include "osdc/Journaler.h"
#include "mds/mdstypes.h"
#include "mon/MonClient.h"

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

void Dumper::init() {
  inodeno_t ino = MDS_INO_LOG_OFFSET + strtol(g_conf.id, 0, 0);
  unsigned pg_pool = CEPH_METADATA_RULE;
  osdmap = new OSDMap();
  objecter = new Objecter(messenger, monc, osdmap, lock);
  journaler = new Journaler(ino, pg_pool, CEPH_FS_ONDISK_MAGIC,
                                       objecter, 0, 0,  &lock);

  objecter->set_client_incarnation(0);

  messenger->register_entity(entity_name_t::CLIENT());
  messenger->add_dispatcher_head(this);
  messenger->start(true);

  monc->set_want_keys(CEPH_ENTITY_TYPE_MON|CEPH_ENTITY_TYPE_OSD|CEPH_ENTITY_TYPE_MDS);
  monc->set_messenger(messenger);
  monc->init();
  monc->authenticate();

  lock.Lock();
  objecter->init();
  objecter->wait_for_osd_map();
  lock.Unlock();
}

void Dumper::dump(const char *dump_file)
{
  bool done = false;
  Cond cond;
  inodeno_t ino = MDS_INO_LOG_OFFSET + strtol(g_conf.id, 0, 0);;

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

  cout << "read " << bl.length() << " bytes" << std::endl;
  bl.write_file(dump_file);
  messenger->shutdown();

  // wait for messenger to finish
  messenger->wait();

}
