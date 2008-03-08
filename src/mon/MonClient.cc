
#include "msg/SimpleMessenger.h"
#include "messages/MMonGetMap.h"
#include "messages/MMonMap.h"

#include "MonClient.h"
#include "MonMap.h"

#include "config.h"

#define dout(x)  if (x <= g_conf.debug || x <= g_conf.debug_mon) *_dout << dbeginl << g_clock.now() << " monclient "
#define derr(x)  if (x <= g_conf.debug || x <= g_conf.debug_mon) *_derr << dbeginl << g_clock.now() << " monclient "

Mutex monmap_lock;
Cond monmap_cond;
bufferlist monmap_bl;

int MonClient::get_monmap(MonMap *pmonmap, entity_addr_t monaddr)
{
  dout(1) << "get_monmap " << monaddr << dendl;

  Messenger *msgr = rank.register_entity(entity_name_t::CLIENT(-1));
  msgr->set_dispatcher(this);

  monmap_lock.Lock();
  while (monmap_bl.length() == 0) {
    dout(10) << "querying " << monaddr << dendl;
    entity_inst_t mi;
    mi.addr = monaddr;
    mi.name = entity_name_t::MON(0);  // FIXME HRM!
    msgr->send_message(new MMonGetMap, mi);
    
    utime_t interval(1, 0);
    monmap_cond.WaitInterval(monmap_lock, interval);
  }
  monmap_lock.Unlock();
  
  pmonmap->decode(monmap_bl);
  dout(1) << "get_monmap got monmap epoch " << pmonmap->epoch << " fsid " << pmonmap->fsid << dendl;

  msgr->shutdown();
  delete msgr;
  return 0;
}

void MonClient::handle_monmap(MMonMap *m)
{
  dout(10) << "handle_monmap " << *m << dendl;
  monmap_lock.Lock();
  monmap_bl = m->monmapbl;
  monmap_cond.Signal();
  monmap_lock.Unlock();
}

void MonClient::dispatch(Message *m)
{
  dout(10) << "dispatch " << *m << dendl;

  switch (m->get_type()) {
  case CEPH_MSG_MON_MAP:
    handle_monmap((MMonMap*)m);
  }

  delete m;
}
