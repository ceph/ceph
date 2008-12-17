
#include "msg/SimpleMessenger.h"
#include "messages/MMonGetMap.h"
#include "messages/MMonMap.h"

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
  entity_addr_t monaddr;
  parse_ip_port(g_conf.mon_host, monaddr);
  
  rank.bind();
  dout(1) << " connecting to monitor at " << monaddr << " ..." << dendl;
  
  Messenger *msgr = rank.register_entity(entity_name_t::CLIENT(-1));
  msgr->set_dispatcher(this);

  rank.start(true);  // do not daemonize!
  
  int attempt = 10;
  monmap_lock.Lock();
  while (monmap_bl.length() == 0) {
    dout(10) << "querying " << monaddr << dendl;
    entity_inst_t mi;
    mi.addr = monaddr;
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
    dout(2) << "get_monmap got monmap from " << monaddr << " fsid " << pmonmap->fsid << dendl;
    cout << "[got monmap from " << monaddr << " fsid " << pmonmap->fsid << "]" << std::endl;
  }
  msgr->shutdown();
  msgr->destroy();
  rank.wait();

  if (monmap_bl.length())
    return 0;

  cerr << "unable to fetch monmap from " << monaddr
       << ": " << strerror(errno) << std::endl;
  return -1; // failed
}

int MonClient::get_monmap(MonMap *pmonmap)
{
  // probe?
  if (g_conf.mon_host &&
      probe_mon(pmonmap) == 0)  
    return 0;

  // file?
  const char *monmap_fn = ".ceph_monmap";
  int r = pmonmap->read(monmap_fn);
  if (r >= 0) {
    cout << "[opened monmap at .ceph_monmap fsid " << pmonmap->fsid << "]" << std::endl;
    return 0;
  }

  cerr << "unable to read monmap from " << monmap_fn 
       << ": " << strerror(errno) << std::endl;
  return -1;
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
  default:
    return false;
  }

  delete m;
  return true;
}
