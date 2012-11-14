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

#include "mds/Resetter.h"
#include "osdc/Journaler.h"
#include "mds/mdstypes.h"
#include "mon/MonClient.h"
#include "mds/events/EResetJournal.h"

Resetter::~Resetter()
{
}

bool Resetter::ms_get_authorizer(int dest_type, AuthAuthorizer **authorizer,
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

bool Resetter::ms_dispatch(Message *m)
{
  Mutex::Locker l(lock);
  switch (m->get_type()) {
  case CEPH_MSG_OSD_OPREPLY:
    objecter->handle_osd_op_reply((MOSDOpReply *)m);
    break;
  case CEPH_MSG_OSD_MAP:
    objecter->handle_osd_map((MOSDMap*)m);
    break;
  default:
    return false;
  }
  return true;
}


void Resetter::init(int rank) 
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
  objecter->wait_for_osd_map();
  timer.init();
  lock.Unlock();
}

void Resetter::shutdown()
{
  lock.Lock();
  timer.shutdown();
  objecter->shutdown_locked();
  lock.Unlock();
  objecter->shutdown_unlocked();
  messenger->shutdown();
  messenger->wait();
}

void Resetter::reset()
{
  Mutex mylock("Resetter::reset::lock");
  Cond cond;
  bool done;
  int r;

  lock.Lock();
  journaler->recover(new C_SafeCond(&mylock, &cond, &done, &r));
  lock.Unlock();

  mylock.Lock();
  while (!done)
    cond.Wait(mylock);
  mylock.Unlock();

  if (r != 0) {
    if (r == -ENOENT) {
      cerr << "journal does not exist on-disk. Did you set a bad rank?"
	   << std::endl;
      shutdown();
      return;
    } else {
      cerr << "got error " << r << "from Journaler, failling" << std::endl;
      shutdown();
      return;
    }
  }

  lock.Lock();
  uint64_t old_start = journaler->get_read_pos();
  uint64_t old_end = journaler->get_write_pos();
  uint64_t old_len = old_end - old_start;
  cout << "old journal was " << old_start << "~" << old_len << std::endl;

  uint64_t new_start = ROUND_UP_TO(old_end+1, journaler->get_layout_period());
  cout << "new journal start will be " << new_start
       << " (" << (new_start - old_end) << " bytes past old end)" << std::endl;

  journaler->set_read_pos(new_start);
  journaler->set_write_pos(new_start);
  journaler->set_expire_pos(new_start);
  journaler->set_trimmed_pos(new_start);
  journaler->set_writeable();

  cout << "writing journal head" << std::endl;
  journaler->write_head(new C_SafeCond(&mylock, &cond, &done, &r));
  lock.Unlock();

  mylock.Lock();
  while (!done)
    cond.Wait(mylock);
  mylock.Unlock();
    
  lock.Lock();
  assert(r == 0);

  LogEvent *le = new EResetJournal;

  bufferlist bl;
  le->encode_with_header(bl);
  
  cout << "writing EResetJournal entry" << std::endl;
  journaler->append_entry(bl);
  journaler->flush(new C_SafeCond(&mylock, &cond, &done,&r));

  lock.Unlock();

  mylock.Lock();
  while (!done)
    cond.Wait(mylock);
  mylock.Unlock();

  assert(r == 0);

  cout << "done" << std::endl;
  shutdown();
}
