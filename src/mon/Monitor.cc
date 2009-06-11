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

// TODO: missing run() method, which creates the two main timers, refreshTimer and readTimer

#include "Monitor.h"

#include "osd/OSDMap.h"

#include "MonitorStore.h"

#include "msg/Message.h"
#include "msg/Messenger.h"

#include "messages/MMonMap.h"
#include "messages/MMonGetMap.h"
#include "messages/MGenericMessage.h"
#include "messages/MMonCommand.h"
#include "messages/MMonCommandAck.h"
#include "messages/MMonObserve.h"
#include "messages/MMonObserveNotify.h"

#include "messages/MMonPaxos.h"
#include "messages/MClass.h"

#include "messages/MClientMountAck.h"

#include "common/Timer.h"
#include "common/Clock.h"

#include "OSDMonitor.h"
#include "MDSMonitor.h"
#include "ClientMonitor.h"
#include "PGMonitor.h"
#include "LogMonitor.h"
#include "ClassMonitor.h"

#include "osd/OSDMap.h"

#include "config.h"

#define DOUT_SUBSYS mon
#undef dout_prefix
#define dout_prefix _prefix(this)
static ostream& _prefix(Monitor *mon) {
  return *_dout << dbeginl
		<< "mon" << mon->whoami
		<< (mon->is_starting() ?
		    (const char*)"(starting)" : 
		    (mon->is_leader() ?
		     (const char*)"(leader)" :
		     (mon->is_peon() ? 
		      (const char*)"(peon)" : 
		      (const char*)"(?\?)")))
		<< " ";
}

Monitor::Monitor(int w, MonitorStore *s, Messenger *m, MonMap *map) :
  whoami(w), 
  messenger(m),
  lock("Monitor::lock"),
  monmap(map),
  logclient(messenger, monmap),
  timer(lock), tick_timer(0),
  store(s),
  
  state(STATE_STARTING), stopping(false),
  
  elector(this, w),
  mon_epoch(0), 
  leader(0),

  paxos(PAXOS_NUM), paxos_service(PAXOS_NUM)
{
  paxos_service[PAXOS_MDSMAP] = new MDSMonitor(this, add_paxos(PAXOS_MDSMAP));
  paxos_service[PAXOS_OSDMAP] = new OSDMonitor(this, add_paxos(PAXOS_OSDMAP));
  paxos_service[PAXOS_CLIENTMAP] = new ClientMonitor(this, add_paxos(PAXOS_CLIENTMAP));
  paxos_service[PAXOS_PGMAP] = new PGMonitor(this, add_paxos(PAXOS_PGMAP));
  paxos_service[PAXOS_LOG] = new LogMonitor(this, add_paxos(PAXOS_LOG));
  paxos_service[PAXOS_CLASS] = new ClassMonitor(this, add_paxos(PAXOS_CLASS));
}

Paxos *Monitor::add_paxos(int type)
{
  Paxos *p = new Paxos(this, whoami, type);
  paxos[type] = p;
  return p;
}

Monitor::~Monitor()
{
  for (vector<PaxosService*>::iterator p = paxos_service.begin(); p != paxos_service.end(); p++)
    delete *p;
  for (vector<Paxos*>::iterator p = paxos.begin(); p != paxos.end(); p++)
    delete *p;
  if (messenger)
    messenger->destroy();
}

void Monitor::init()
{
  lock.Lock();
  
  dout(1) << "init fsid " << monmap->fsid << dendl;
  
  // init paxos
  for (vector<Paxos*>::iterator p = paxos.begin(); p != paxos.end(); p++)
    (*p)->init();

  logclient.set_synchronous(true);
  
  // i'm ready!
  messenger->set_dispatcher(this);
  link_dispatcher(&logclient);
  
  // start ticker
  reset_tick();
  
  // call election?
  if (monmap->size() > 1) {
    call_election();
  } else {
    // we're standalone.
    set<int> q;
    q.insert(whoami);
    win_election(1, q);
  }
  
  lock.Unlock();
}

void Monitor::shutdown()
{
  dout(1) << "shutdown" << dendl;
  
  elector.shutdown();
  
  // clean up
  for (vector<PaxosService*>::iterator p = paxos_service.begin(); p != paxos_service.end(); p++)
    (*p)->shutdown();

  // cancel all events
  cancel_tick();
  timer.cancel_all();
  timer.join();  

  unlink_dispatcher(&logclient);
  
  // die.
  messenger->shutdown();
}


void Monitor::call_election(bool is_new)
{
  if (monmap->size() == 1) return;
  
  if (is_new) {
    stringstream ss;
    ss << "mon" << whoami << " calling new monitor election";
    logclient.log(LOG_INFO, ss);
  }

  dout(10) << "call_election" << dendl;
  state = STATE_STARTING;
  
  // tell paxos
  for (vector<Paxos*>::iterator p = paxos.begin(); p != paxos.end(); p++)
    (*p)->election_starting();
  for (vector<PaxosService*>::iterator p = paxos_service.begin(); p != paxos_service.end(); p++)
    (*p)->election_starting();

  // call a new election
  elector.call_election();
}

void Monitor::win_election(epoch_t epoch, set<int>& active) 
{
  state = STATE_LEADER;
  leader = whoami;
  mon_epoch = epoch;
  quorum = active;
  dout(10) << "win_election, epoch " << mon_epoch << " quorum is " << quorum << dendl;

  stringstream ss;
  ss << "mon" << whoami << " won leader election with quorum " << quorum;
  logclient.log(LOG_INFO, ss);
  
  for (vector<Paxos*>::iterator p = paxos.begin(); p != paxos.end(); p++)
    (*p)->leader_init();
  for (vector<PaxosService*>::iterator p = paxos_service.begin(); p != paxos_service.end(); p++)
    (*p)->election_finished();
} 

void Monitor::lose_election(epoch_t epoch, set<int> &q, int l) 
{
  state = STATE_PEON;
  mon_epoch = epoch;
  leader = l;
  quorum = q;
  dout(10) << "lose_election, epoch " << mon_epoch << " leader is mon" << leader
	   << " quorum is " << quorum << dendl;
  
  for (vector<Paxos*>::iterator p = paxos.begin(); p != paxos.end(); p++)
    (*p)->peon_init();
  for (vector<PaxosService*>::iterator p = paxos_service.begin(); p != paxos_service.end(); p++)
    (*p)->election_finished();
}

void Monitor::handle_command(MMonCommand *m)
{
  if (ceph_fsid_compare(&m->fsid, &monmap->fsid)) {
    dout(0) << "handle_command on fsid " << m->fsid << " != " << monmap->fsid << dendl;
    reply_command(m, -EPERM, "wrong fsid");
    return;
  }

  dout(0) << "handle_command " << *m << dendl;
  string rs;
  int r = -EINVAL;
  if (!m->cmd.empty()) {
      dout(0) << "m->cmd[0]=" << m->cmd[0] << dendl;
    if (m->cmd[0] == "mds") {
      mdsmon()->dispatch(m);
      return;
    }
    if (m->cmd[0] == "osd") {
      osdmon()->dispatch(m);
      return;
    }
    if (m->cmd[0] == "pg") {
      pgmon()->dispatch(m);
      return;
    }
    if (m->cmd[0] == "client") {
      clientmon()->dispatch(m);
      return;
    }
    if (m->cmd[0] == "stop") {
      shutdown();
      reply_command(m, 0, "stopping");
      return;
    }
    if (m->cmd[0] == "stop_cluster") {
      stop_cluster();
      reply_command(m, 0, "initiating cluster shutdown");
      return;
    }

    if (m->cmd[0] == "_injectargs") {
      parse_config_option_string(m->cmd[1]);
      return;
    } 
    if (m->cmd[0] == "class") {
      classmon()->dispatch(m);
      return;
    }
    if (m->cmd[0] == "mon") {
      if (m->cmd[1] == "injectargs" && m->cmd.size() == 4) {
	vector<string> args(2);
	args[0] = "_injectargs";
	args[1] = m->cmd[3];
	if (m->cmd[2] == "*") {
	  for (unsigned i=0; i<monmap->size(); i++)
	    inject_args(monmap->get_inst(i), args);
	  r = 0;
	  rs = "ok bcast";
	} else {
	  errno = 0;
	  int who = strtol(m->cmd[2].c_str(), 0, 10);
	  if (!errno && who >= 0) {
	    inject_args(monmap->get_inst(who), args);
	    r = 0;
	    rs = "ok";
	  } else 
	    rs = "specify mon number or *";
	}
      } else
	rs = "unrecognized mon command";
    } else 
      rs = "unrecognized subsystem";
  } else 
    rs = "no command";

  reply_command(m, r, rs);
}

void Monitor::reply_command(MMonCommand *m, int rc, const string &rs)
{
  bufferlist rdata;
  reply_command(m, rc, rs, rdata);
}

void Monitor::reply_command(MMonCommand *m, int rc, const string &rs, bufferlist& rdata)
{
  MMonCommandAck *reply = new MMonCommandAck(m->cmd, rc, rs);
  reply->set_data(rdata);
  messenger->send_message(reply, m->get_orig_source_inst());
  delete m;
}

void Monitor::handle_observe(MMonObserve *m)
{
  dout(10) << "handle_observe " << *m << " from " << m->get_source_inst() << dendl;
  if (m->machine_id >= PAXOS_NUM) {
    dout(0) << "register_observer: bad monitor id: " << m->machine_id << dendl;
  } else {
    paxos[m->machine_id]->register_observer(m->get_orig_source_inst(), m->ver);
  }
  messenger->send_message(m, m->get_orig_source_inst());
}


void Monitor::inject_args(const entity_inst_t& inst, vector<string>& args)
{
  dout(10) << "inject_args " << inst << " " << args << dendl;
  MMonCommand *c = new MMonCommand(monmap->fsid);
  c->cmd = args;
  messenger->send_message(c, inst);
}




void Monitor::stop_cluster()
{
  dout(0) << "stop_cluster -- initiating shutdown" << dendl;
  stopping = true;
  mdsmon()->do_stop();
}


bool Monitor::dispatch_impl(Message *m)
{
  // verify protocol version
  if (m->get_orig_source().is_mon() &&
      m->get_header().mon_protocol != CEPH_MON_PROTOCOL) {
    dout(0) << "mon protocol v " << (int)m->get_header().mon_protocol << " != my " << CEPH_MON_PROTOCOL
	    << " from " << m->get_orig_source_inst() << " " << *m << dendl;
    delete m;
    return true;
  }
  if (m->get_header().monc_protocol != CEPH_MONC_PROTOCOL) {
    dout(0) << "monc protocol v " << (int)m->get_header().monc_protocol << " != my " << CEPH_MONC_PROTOCOL
	    << " from " << m->get_orig_source_inst() << " " << *m << dendl;

    if (m->get_type() == CEPH_MSG_CLIENT_MOUNT) {
      stringstream ss;
      ss << "client protocol v " << (int)m->get_header().monc_protocol << " != server v " << CEPH_MONC_PROTOCOL;
      string s;
      getline(ss, s);
      messenger->send_message(new MClientMountAck(-EINVAL, s.c_str()),
			      m->get_orig_source_inst());
    }

    delete m;
    return true;
  }

  lock.Lock();
  {
    switch (m->get_type()) {
      
      // misc
    case CEPH_MSG_MON_GET_MAP:
      handle_mon_get_map((MMonGetMap*)m);
      break;

    case CEPH_MSG_SHUTDOWN:
      if (m->get_source().is_osd()) 
	osdmon()->dispatch(m);
      else
	handle_shutdown(m);
      break;
      
    case MSG_MON_COMMAND:
      handle_command((MMonCommand*)m);
      break;

      // OSDs
    case CEPH_MSG_OSD_GETMAP:
    case MSG_OSD_FAILURE:
    case MSG_OSD_BOOT:
    case MSG_OSD_IN:
    case MSG_OSD_OUT:
    case MSG_OSD_ALIVE:
    case MSG_REMOVE_SNAPS:
      paxos_service[PAXOS_OSDMAP]->dispatch(m);
      break;

      
      // MDSs
    case MSG_MDS_BEACON:
    case CEPH_MSG_MDS_GETMAP:
      paxos_service[PAXOS_MDSMAP]->dispatch(m);
      break;

      // clients
    case CEPH_MSG_CLIENT_MOUNT:
    case CEPH_MSG_CLIENT_UNMOUNT:
      paxos_service[PAXOS_CLIENTMAP]->dispatch(m);
      break;

      // pg
    case CEPH_MSG_STATFS:
    case MSG_PGSTATS:
    case MSG_GETPOOLSTATS:
      paxos_service[PAXOS_PGMAP]->dispatch(m);
      break;

      // log
    case MSG_LOG:
      paxos_service[PAXOS_LOG]->dispatch(m);
      break;

      // paxos
    case MSG_MON_PAXOS:
      {
	MMonPaxos *pm = (MMonPaxos*)m;

	// sanitize
	if (pm->epoch > mon_epoch) 
	  call_election();
	if (pm->epoch != mon_epoch) {
	  delete pm;
	  break;
	}

	// send it to the right paxos instance
	assert(pm->machine_id < PAXOS_NUM);
	Paxos *p = paxos[pm->machine_id];
	p->dispatch(m);

	// make sure service finds out about any state changes
	if (p->is_active())
	  paxos_service[p->machine_id]->update_from_paxos();
      }
      break;

    case MSG_MON_OBSERVE:
      handle_observe((MMonObserve *)m);
      break;

      // elector messages
    case MSG_MON_ELECTION:
      elector.dispatch(m);
      break;

    case MSG_CLASS:
      handle_class((MClass *)m);
      break;
      
    default:
        return false;
    }
  }
  lock.Unlock();

  return true;
}

void Monitor::handle_mon_get_map(MMonGetMap *m)
{
  dout(10) << "handle_mon_get_map" << dendl;
  bufferlist bl;
  monmap->encode(bl);
  messenger->send_message(new MMonMap(bl), m->get_orig_source_inst());
  delete m;
}


void Monitor::handle_shutdown(Message *m)
{
  assert(m->get_source().is_mon());
  if (m->get_source().num() == get_leader()) {
    dout(1) << "shutdown from leader " << m->get_source() << dendl;

    if (is_leader()) {
      // stop osds.
      set<int32_t> ls;
      osdmon()->osdmap.get_all_osds(ls);
      for (set<int32_t>::iterator it = ls.begin(); it != ls.end(); it++) {
	if (osdmon()->osdmap.is_down(*it)) continue;
	dout(10) << "sending shutdown to osd" << *it << dendl;
	messenger->send_message(new MGenericMessage(CEPH_MSG_SHUTDOWN),
				osdmon()->osdmap.get_inst(*it));
      }
      osdmon()->mark_all_down();
      
      // monitors too.
      for (unsigned i=0; i<monmap->size(); i++)
	if ((int)i != whoami)
	  messenger->send_message(new MGenericMessage(CEPH_MSG_SHUTDOWN), 
				  monmap->get_inst(i));
    }

    shutdown();
  } else {
    dout(1) << "ignoring shutdown from non-leader " << m->get_source() << dendl;
  }
  delete m;
}





/************ TICK ***************/

class C_Mon_Tick : public Context {
  Monitor *mon;
public:
  C_Mon_Tick(Monitor *m) : mon(m) {}
  void finish(int r) {
    mon->tick();
  }
};

void Monitor::cancel_tick()
{
  if (tick_timer) timer.cancel_event(tick_timer);
}

void Monitor::reset_tick()
{
  cancel_tick();
  tick_timer = new C_Mon_Tick(this);
  timer.add_event_after(g_conf.mon_tick_interval, tick_timer);
}


void Monitor::tick()
{
  tick_timer = 0;

  _dout_check_log();

  // ok go.
  dout(11) << "tick" << dendl;
  
  for (vector<PaxosService*>::iterator p = paxos_service.begin(); p != paxos_service.end(); p++)
    (*p)->tick();
  
  // next tick!
  reset_tick();
}





/*
 * this is the closest thing to a traditional 'mkfs' for ceph.
 * initialize the monitor state machines to their initial values.
 */
int Monitor::mkfs(bufferlist& osdmapbl)
{
  // create it
  int err = store->mkfs();
  if (err < 0) {
    cerr << "error " << err << " " << strerror(err) << std::endl;
    exit(1);
  }
  
  store->put_int(whoami, "whoami", 0);

  bufferlist magicbl;
  magicbl.append(CEPH_MON_ONDISK_MAGIC);
  magicbl.append("\n");
  store->put_bl_ss(magicbl, "magic", 0);

  bufferlist monmapbl;
  monmap->encode(monmapbl);
  store->put_bl_sn(monmapbl, "monmap", monmap->epoch);  
  store->put_bl_ss(monmapbl, "monmap", "latest");

  for (vector<PaxosService*>::iterator p = paxos_service.begin(); p != paxos_service.end(); p++) {
    PaxosService *svc = *p;
    bufferlist bl;
    dout(10) << "initializing " << svc->get_machine_name() << dendl;
    svc->paxos->init();
    svc->create_pending();
    if (svc->paxos->machine_id == PAXOS_OSDMAP)
      svc->create_initial(osdmapbl);
    else
      svc->create_initial(bl);
    // commit to paxos
    svc->encode_pending(bl);
    store->put_bl_sn(bl, svc->get_machine_name(), 1);
    store->put_int(1, svc->get_machine_name(), "last_committed");
  }

  return 0;
}

void Monitor::handle_class(MClass *m)
{
  switch (m->action) {
    case CLASS_SET:
    case CLASS_GET:
      classmon()->handle_request(m);
      break;
    case CLASS_RESPONSE:
      dout(0) << "got a class response (" << *m << ") ???" << dendl;
      break;
    default:
      dout(0) << "got an unknown class message (" << *m << ") ???" << dendl;
      assert(0);
      break;
  }
}

