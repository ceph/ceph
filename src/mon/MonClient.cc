
#include "msg/SimpleMessenger.h"
#include "messages/MMonGetMap.h"
#include "messages/MMonMap.h"
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
    dout(2) << "get_monmap got monmap from " << monaddrs[i] << " fsid " << pmonmap->fsid << dendl;
    cout << "[got monmap from " << monaddrs[i] << " fsid " << pmonmap->fsid << "]" << std::endl;
  }
  msgr->shutdown();
  msgr->destroy();
  rank.wait();

  if (monmap_bl.length())
    return 0;

  cerr << "unable to fetch monmap from " << monaddrs
       << ": " << strerror(errno) << std::endl;
  return -1; // failed
}

int MonClient::get_monmap(MonMap *pmonmap)
{
  static string monstr;

  if (!g_conf.mon_host) {
    // cluster conf?
    ConfFile a(g_conf.cluster_conf);
    ConfFile b("cluster.conf");
    ConfFile *c = 0;

    if (a.parse())
      c = &a;
    else if (b.parse())
      c = &b;
    if (c) {
      for (int i=0; i<15; i++) {
	char *val = 0;
	char monname[10];
	sprintf(monname, "mon%d", i);
	c->read(monname, "mon addr", &val, 0);
	if (!val || !val[0])
	  break;
	
	if (monstr.length())
	  monstr += ",";
	monstr += val;
      }
      g_conf.mon_host = monstr.c_str();
    }
  }

  // probe?
  if (g_conf.mon_host &&
      probe_mon(pmonmap) == 0)  
    return 0;

  if (g_conf.monmap_file) {
    // file?
    const char *monmap_fn = g_conf.monmap_file;
    int r = pmonmap->read(monmap_fn);
    if (r >= 0) {
      cout << "[opened monmap at " << monmap_fn << " fsid " << pmonmap->fsid << "]" << std::endl;
      return 0;
    }

    cerr << "unable to read monmap from " << monmap_fn << ": " << strerror(errno) << std::endl;
  }

  cerr << "must specify monitor address (-m monaddr) or cluster conf (-C cluster.conf) or monmap file (-M monmap)" << std::endl;
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
