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

#include "msg/Messenger.h"

#include "messages/PaxosServiceMessage.h"
#include "messages/MMonMap.h"
#include "messages/MMonGetMap.h"
#include "messages/MGenericMessage.h"
#include "messages/MMonCommand.h"
#include "messages/MMonCommandAck.h"
#include "messages/MMonObserve.h"
#include "messages/MMonObserveNotify.h"

#include "messages/MMonPaxos.h"
#include "messages/MClass.h"
#include "messages/MRoute.h"

#include "messages/MClientMountAck.h"

#include "messages/MMonSubscribe.h"
#include "messages/MMonSubscribeAck.h"

#include "common/Timer.h"
#include "common/Clock.h"

#include "OSDMonitor.h"
#include "MDSMonitor.h"
#include "ClientMonitor.h"
#include "PGMonitor.h"
#include "LogMonitor.h"
#include "ClassMonitor.h"
#include "AuthMonitor.h"

#include "osd/OSDMap.h"

#include "auth/AuthorizeServer.h"

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
  authorizer(m, &keys_server),
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
  paxos_service[PAXOS_AUTH] = new AuthMonitor(this, add_paxos(PAXOS_AUTH));
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

  for (vector<PaxosService*>::iterator ps = paxos_service.begin(); ps != paxos_service.end(); ps++)
    (*ps)->init();

  logclient.set_synchronous(true);
  
  // i'm ready!
  messenger->add_dispatcher_tail(this);
  messenger->add_dispatcher_head(&logclient);
  authorizer.init();
  
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
    reply_command(m, -EPERM, "wrong fsid", 0);
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
      reply_command(m, 0, "stopping", 0);
      return;
    }
    if (m->cmd[0] == "stop_cluster") {
      stop_cluster();
      reply_command(m, 0, "initiating cluster shutdown", 0);
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
    if (m->cmd[0] == "auth") {
      authmon()->dispatch(m);
      return;
    }
  if (m->cmd[0] == "mon") {
      if (m->cmd[1] == "injectargs" && m->cmd.size() == 4) {
	vector<string> args(2);
	args[0] = "_injectargs";
	args[1] = m->cmd[3];
	if (m->cmd[2] == "*") {
	  for (unsigned i=0; i<monmap->size(); i++)
	    inject_args(monmap->get_inst(i), args, 0);
	  r = 0;
	  rs = "ok bcast";
	} else {
	  errno = 0;
	  int who = strtol(m->cmd[2].c_str(), 0, 10);
	  if (!errno && who >= 0) {
	    inject_args(monmap->get_inst(who), args, 0);
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

  reply_command(m, r, rs, 0);
}

void Monitor::reply_command(MMonCommand *m, int rc, const string &rs, version_t version)
{
  bufferlist rdata;
  reply_command(m, rc, rs, rdata, version);
}

void Monitor::reply_command(MMonCommand *m, int rc, const string &rs, bufferlist& rdata, version_t version)
{
  MMonCommandAck *reply = new MMonCommandAck(m->cmd, rc, rs, version);
  reply->set_data(rdata);
  send_reply(m, reply);
  delete m;
}

void Monitor::send_reply(Message *req, Message *reply, entity_inst_t to)
{
  if (req->get_source().is_mon()) {
    messenger->send_message(new MRoute(reply, to), req->get_source_inst());
  } else {
    messenger->send_message(reply, to);
  }
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


void Monitor::inject_args(const entity_inst_t& inst, vector<string>& args, version_t version)
{
  dout(10) << "inject_args " << inst << " " << args << dendl;
  MMonCommand *c = new MMonCommand(monmap->fsid, version);
  c->cmd = args;
  messenger->send_message(c, inst);
}




void Monitor::stop_cluster()
{
  dout(0) << "stop_cluster -- initiating shutdown" << dendl;
  stopping = true;
  mdsmon()->do_stop();
}


bool Monitor::ms_dispatch(Message *m)
{
  bool ret = true;
  lock.Lock();

  Connection *connection = m->get_connection();
  Session *s = NULL;

  if (connection) {
    s = (Session *)connection->get_priv();
    if (s && s->closed) {
      s->put();
      s = NULL;
    }
    if (!s) {
      s = session_map.new_session(m->get_source_inst());
      m->get_connection()->set_priv(s->get());
      dout(10) << "ms_dispatch new session " << s << " for " << s->inst << dendl;
    } else {
      dout(20) << "ms_dispatch existing session " << s << " for " << s->inst << dendl;
    }
  }

  {
    switch (m->get_type()) {
      
    case MSG_ROUTE:
      handle_route((MRoute*)m);
      break;

      // misc
    case CEPH_MSG_MON_GET_MAP:
      handle_mon_get_map((MMonGetMap*)m);
      break;

    case CEPH_MSG_SHUTDOWN:
      handle_shutdown(m);
      break;
      
    case MSG_MON_COMMAND:
      handle_command((MMonCommand*)m);
      break;

    case CEPH_MSG_MON_SUBSCRIBE:
      handle_subscribe((MMonSubscribe*)m);
      break;

      // OSDs
    case CEPH_MSG_OSD_GETMAP:
    case MSG_OSD_FAILURE:
    case MSG_OSD_BOOT:
    case MSG_OSD_IN:
    case MSG_OSD_OUT:
    case MSG_OSD_ALIVE:
    case MSG_OSD_PGTEMP:
    case MSG_REMOVE_SNAPS:
      paxos_service[PAXOS_OSDMAP]->dispatch((PaxosServiceMessage*)m);
      break;

      
      // MDSs
    case MSG_MDS_BEACON:
    case MSG_MDS_OFFLOAD_TARGETS:
    case CEPH_MSG_MDS_GETMAP:
      paxos_service[PAXOS_MDSMAP]->dispatch((PaxosServiceMessage*)m);
      break;

      // auth
    case CEPH_MSG_AUTH:
    case MSG_AUTH_ROTATING:
    case MSG_AUTHMON:
      paxos_service[PAXOS_AUTH]->dispatch((PaxosServiceMessage*)m);
      break;

      // clients
    case CEPH_MSG_CLIENT_MOUNT:
      paxos_service[PAXOS_CLIENTMAP]->dispatch((PaxosServiceMessage*)m);
      break;

      // pg
    case CEPH_MSG_STATFS:
    case MSG_PGSTATS:
    case MSG_GETPOOLSTATS:
      paxos_service[PAXOS_PGMAP]->dispatch((PaxosServiceMessage*)m);
      break;

    case MSG_POOLOP:
     paxos_service[PAXOS_OSDMAP]->dispatch((PaxosServiceMessage*)m);
      break;

      // log
    case MSG_LOG:
      paxos_service[PAXOS_LOG]->dispatch((PaxosServiceMessage*)m);
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
	p->dispatch((PaxosServiceMessage*)m);

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
      ret = false;
    }
  }
  if (s) {
    s->put();
  }
  lock.Unlock();

  return ret;
}

void Monitor::handle_subscribe(MMonSubscribe *m)
{
  dout(10) << "handle_subscribe " << *m << dendl;
  
  bool reply = false;

  Session *s = (Session *)m->get_connection()->get_priv();

  s->until = g_clock.now();
  s->until += g_conf.mon_subscribe_interval;

  for (map<nstring,ceph_mon_subscribe_item>::iterator p = m->what.begin();
       p != m->what.end();
       p++) {
    if (!p->second.onetime)
      reply = true;
    session_map.add_update_sub(s, p->first, p->second.have, p->second.onetime);

    if (p->first == "mdsmap")
      mdsmon()->check_sub(s->sub_map["mdsmap"]);
    else if (p->first == "osdmap")
      osdmon()->check_sub(s->sub_map["osdmap"]);
    else if (p->first == "monmap")
      check_sub(s->sub_map["monmap"]);
  }

  // ???

  if (reply)
    messenger->send_message(new MMonSubscribeAck(g_conf.mon_subscribe_interval),
			    m->get_source_inst());

  s->put();
  delete m;
}

bool Monitor::ms_handle_reset(Connection *con, const entity_addr_t& peer)
{
  Session *s = (Session *)con->get_priv();
  if (!s)
    return false;

  Mutex::Locker l(lock);

  dout(10) << "reset/close on session " << s->inst << dendl;
  session_map.remove_session(s);
  s->put();
  return true;
}

void Monitor::check_subs()
{
  nstring type = "monmap";
  xlist<Subscription*>::iterator p = session_map.subs[type].begin();
  while (!p.end()) {
    Subscription *sub = *p;
    ++p;
    check_sub(sub);
  }
}

void Monitor::check_sub(Subscription *sub)
{
  if (sub->last < monmap->get_epoch()) {
    send_latest_monmap(sub->session->inst);
    if (sub->onetime)
      session_map.remove_sub(sub);
    else
      sub->last = monmap->get_epoch();
  }
}


// -----

void Monitor::send_latest_monmap(entity_inst_t i)
{
  bufferlist bl;
  monmap->encode(bl);
  messenger->send_message(new MMonMap(bl), i);
}

void Monitor::handle_mon_get_map(MMonGetMap *m)
{
  dout(10) << "handle_mon_get_map" << dendl;
  send_latest_monmap(m->get_orig_source_inst());
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
	messenger->send_message(new PaxosServiceMessage(CEPH_MSG_SHUTDOWN, 0),
				osdmon()->osdmap.get_inst(*it));
      }
      osdmon()->mark_all_down();
      
      // monitors too.
      for (unsigned i=0; i<monmap->size(); i++)
	if ((int)i != whoami)
	  messenger->send_message(new PaxosServiceMessage(CEPH_MSG_SHUTDOWN, 0), 
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
  
  // trim sessions
  utime_t now = g_clock.now();
  xlist<Session*>::iterator p = session_map.sessions.begin();
  while (!p.end()) {
    Session *s = *p;
    ++p;
    if (!s->until.is_zero() && (s->until < now)) {
      dout(10) << " trimming session " << s->inst << " (until " << s->until << " < now " << now << ")" << dendl;
      messenger->mark_down(s->inst.addr);
      session_map.remove_session(s);
    }
  }

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
    char buf[80];
    cerr << "error " << err << " " << strerror_r(err, buf, sizeof(buf)) << std::endl;
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

void Monitor::handle_route(MRoute *m)
{
  dout(10) << "handle_route " << *m->msg << " to " << m->dest << dendl;
  
  messenger->send_message(m->msg, m->dest);
  m->msg = NULL;
  delete m;
}

bool Monitor::ms_get_authorizer(int dest_type, bufferlist& authorizer, bool force_new)
{
  AuthServiceTicketInfo auth_ticket_info;

  SessionAuthInfo info;
  int ret;
  uint32_t service_id = peer_id_to_entity_type(dest_type);

  dout(0) << "ms_get_authorizer service_id=" << service_id << dendl;

  if (service_id != CEPHX_PRINCIPAL_MON) {
    ret = keys_server.build_session_auth_info(service_id, auth_ticket_info, info);
    if (ret < 0) {
      return false;
    }
  } else {
    EntityName name;
    name.entity_type = CEPHX_PRINCIPAL_MON;

    CryptoKey secret;
    map<string, bufferlist> caps;
    if (!keys_server.get_secret(name, secret, caps)) {
      dout(0) << "couldn't get secret for mon service!" << dendl;
      stringstream ss;
      keys_server.list_secrets(ss);
      dout(0) << ss.str() << dendl;
      return false;
    }
    /* mon to mon authentication uses the private monitor shared key and not the
       rotating key */
    ret = keys_server.build_session_auth_info(service_id, auth_ticket_info, info, secret, (uint64_t)-1);
    if (ret < 0) {
      return false;
    }
    dout(0) << "built session auth_info for use with mon" << dendl;

  }

  bufferlist ticket_data;
  ret = build_service_ticket(info, ticket_data);
  if (ret < 0)
    return false;

  dout(0) << "built service ticket" << dendl;
  bufferlist::iterator iter = ticket_data.begin();
  AuthTicketHandler handler;
  ::decode(handler.ticket, iter);

  handler.service_id = service_id;
  handler.session_key = info.session_key;

  AuthContext ctx;
  handler.build_authorizer(authorizer, ctx);
  
  return true;
}

bool Monitor::ms_verify_authorizer(Connection *con, int peer_type,
				    bufferlist& authorizer_data, bufferlist& authorizer_reply,
				    bool& isvalid)
{
  dout(0) << "Monitor::verify_authorizer start" << dendl;

  bufferlist::iterator iter = authorizer_data.begin();

  if (!authorizer_data.length())
    return -EPERM;

  int ret = authorizer.verify_authorizer(peer_type, iter, authorizer_reply);
  dout(0) << "Monitor::verify_authorizer returns " << ret << dendl;

  isvalid = (ret >= 0);
 
  return true;
};

