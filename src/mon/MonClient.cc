#include "msg/SimpleMessenger.h"
#include "messages/MMonGetMap.h"
#include "messages/MMonMap.h"
#include "messages/MClientMount.h"

#include "include/nstring.h"

#include "messages/MClientMountAck.h"
#include "messages/MClientUnmount.h"
#include "common/ConfUtils.h"

#include "MonClient.h"
#include "MonMap.h"

#include "config.h"


#undef dout_prefix
#define dout_prefix *_dout << dbeginl << " monclient "

Mutex monmap_lock("monmap_lock");
Cond monmap_cond;
bufferlist monmap_bl;

int MonClient::probe_mon(MonMap *pmonmap) 
{
  vector<entity_addr_t> monaddrs;

  const char *p = g_conf.mon_host;
  const char *end = p + strlen(p);
  while (p < end) {
    entity_addr_t a;
    if (parse_ip_port(p, a, &p)) {
      monaddrs.push_back(a);
    } else {
      break;
    }
  }
  if (monaddrs.empty()) {
    cerr << "couldn't parse ip:port(s) from '" << g_conf.mon_host << "'" << std::endl;
    return -1;
  }

  SimpleMessenger rank; 

  rank.bind();
  dout(1) << " connecting to monitor(s) at " << monaddrs << " ..." << dendl;
  
  Messenger *msgr = rank.register_entity(entity_name_t::CLIENT(-1));
  msgr->set_dispatcher(this);

  rank.start(true);  // do not daemonize!
  
  int attempt = 10;
  int i = 0;
  monmap_lock.Lock();
  srand(getpid());
  while (monmap_bl.length() == 0) {
    i = rand() % monaddrs.size();
    dout(10) << "querying " << monaddrs[i] << dendl;
    entity_inst_t mi;
    mi.addr = monaddrs[i];
    mi.name = entity_name_t::MON(0);  // FIXME HRM!
    msgr->send_message(new MMonGetMap, mi);
    
    if (--attempt == 0)
      break;

    utime_t interval(1, 0);
    monmap_cond.WaitInterval(monmap_lock, interval);
  }
  monmap_lock.Unlock();

  if (monmap_bl.length()) {
    pmonmap->decode(monmap_bl);
    dout(1) << "[got monmap from " << monaddrs[i] << " fsid " << pmonmap->fsid << "]" << dendl;
  }
  msgr->shutdown();
  msgr->destroy();
  rank.wait();

  if (monmap_bl.length())
    return 0;

  cerr << "unable to fetch monmap from " << monaddrs << std::endl;
  return -1; // failed
}

MonMap *MonClient::get_monmap()
{
  char *val = 0;
  char monname[10];

  if (g_conf.monmap) {
    // file?
    const char *monmap_fn = g_conf.monmap;
    int r = pmonmap->read(monmap_fn);
    if (r >= 0) {
      vector<entity_inst_t>::iterator iter = pmonmap->mon_inst.begin();
      unsigned int i;
      const sockaddr_in *ipaddr;
      entity_addr_t conf_addr;
      ConfFile a(g_conf.conf);
      ConfFile b("ceph.conf");
      ConfFile *c = 0;

      dout(1) << "[opened monmap at " << monmap_fn << " fsid " << pmonmap->fsid << "]" << dendl;

      if (a.parse())
        c = &a;
      else if (b.parse())
        c = &b;
      if (c) {
        for (i=0; i<pmonmap->mon_inst.size(); i++) {
          ipaddr = &pmonmap->mon_inst[i].addr.ipaddr;
          sprintf(monname, "mon%d", i);
          if (c->read(monname, "mon addr", &val, 0)) {
            if (parse_ip_port(val, conf_addr, NULL)) {
              if ((ipaddr->sin_addr.s_addr != conf_addr.ipaddr.sin_addr.s_addr) ||
                (ipaddr->sin_port != conf_addr.ipaddr.sin_port)) {
                   cerr << "WARNING: 'mon addr' config option (" << monname << ") does not match monmap file" << std::endl
                        << "         continuing with monmap configuration" << std::endl;
              }
	    }
          }
        }
      }

      return pmonmap;
    }

    cerr << "unable to read monmap from " << monmap_fn << ": " << strerror(errno) << std::endl;
  }

  if (!g_conf.mon_host) {
    // cluster conf?
    ConfFile a(g_conf.conf);
    ConfFile b("ceph.conf");
    ConfFile *c = 0;

    if (a.parse())
      c = &a;
    else if (b.parse())
      c = &b;
    if (c) {
      static string monstr;
      for (int i=0; i<15; i++) {
	sprintf(monname, "mon%d", i);
	c->read(monname, "mon addr", &val, 0);
	if (!val || !val[0])
	  break;
	
	if (monstr.length())
	  monstr += ",";
	monstr += val;
      }
      g_conf.mon_host = strdup(monstr.c_str());
    }
  }

  // probe?
  if (g_conf.mon_host &&
      probe_mon(pmonmap) == 0)  
    return pmonmap;

  cerr << "must specify monitor address (-m monaddr) or cluster conf (-c ceph.conf) or monmap file (-M monmap)" << std::endl;
  return NULL;
}

void MonClient::handle_monmap(MMonMap *m)
{
  dout(10) << "handle_monmap " << *m << dendl;
  monmap_lock.Lock();
  monmap_bl = m->monmapbl;
  monmap_cond.Signal();
  monmap_lock.Unlock();
}

bool MonClient::dispatch_impl(Message *m)
{
  dout(10) << "dispatch " << *m << dendl;

  switch (m->get_type()) {
  case CEPH_MSG_MON_MAP:
    handle_monmap((MMonMap*)m);
    break;
  case CEPH_MSG_OSD_MAP:
  case CEPH_MSG_MDS_MAP:
    break;
  case CEPH_MSG_CLIENT_MOUNT_ACK:
    handle_mount_ack((MClientMountAck*)m);
    dout(0) << "got mount ack" << dendl;
    break;
  default:
    return false;
  }

  delete m;
  return true;
}


// -------------------
// MOUNT

void MonClient::_try_mount(double timeout)
{
  dout(10) << "_try_mount" << dendl;
  int mon = pmonmap->pick_mon();
  dout(2) << "sending client_mount to mon" << mon << dendl;
  messenger->set_dispatcher(this);
  messenger->send_message(new MClientMount, pmonmap->get_inst(mon));

  // schedule timeout?
  assert(mount_timeout_event == 0);
  mount_timeout_event = new C_MountTimeout(this, timeout);
  timer.add_event_after(timeout, mount_timeout_event);
}

void MonClient::_mount_timeout(double timeout)
{
  dout(10) << "_mount_timeout" << dendl;
  mount_timeout_event = 0;
  _try_mount(timeout);
}

int MonClient::mount(double mount_timeout)
{
  Mutex::Locker lock(monc_lock);

  if (mounted) {
    dout(5) << "already mounted" << dendl;;
    return 0;
  }

  // only first mounter does the work
  bool itsme = false;
  if (!mounters) {
    itsme = true;
    _try_mount(mount_timeout);
  } else {
    dout(5) << "additional mounter" << dendl;
  }
  mounters++;

  while (signed_ticket.length() == 0 ||
	 (!itsme && !mounted))       // non-doers wait a little longer
    mount_cond.Wait(monc_lock);

  if (!itsme) {
    dout(5) << "additional mounter returning" << dendl;
    assert(mounted);
    return 0;
  }

  // finish.
  timer.cancel_event(mount_timeout_event);
  mount_timeout_event = 0;

  mounted = true;
  mount_cond.SignalAll(); // wake up non-doers

  return 0;
}

void MonClient::handle_mount_ack(MClientMountAck* m)
{
  dout(10) << "handle_mount_ack " << *m << dendl;

  signed_ticket = m->signed_ticket;
  bufferlist::iterator p = signed_ticket.begin();
  ::decode(ticket, p);

  messenger->reset_myname(m->get_dest());

  mount_cond.Signal();
}


// UNMOUNT
int MonClient::unmount()
{
  Mutex::Locker lock(monc_lock);

  // fixme: this should retry and time out too

  int mon = pmonmap->pick_mon();
  dout(2) << "sending client_unmount to mon" << mon << dendl;
  messenger->send_message(new MClientUnmount, pmonmap->get_inst(mon));
  
  while (mounted)
    mount_cond.Wait(monc_lock);

  dout(2) << "unmounted." << dendl;
  return 0;
}

void MonClient::handle_unmount(Message* m)
{
  Mutex::Locker lock(monc_lock);
  mounted = false;
  mount_cond.Signal();
  delete m;
}


