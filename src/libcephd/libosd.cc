// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#define dout_context g_ceph_context
#include <condition_variable>
#include <mutex>
#include "ceph_osd.h"

#include "msg/Connection.h"
#include "include/types.h"

#include "Context.h"
#include "Dispatcher.h"
#include "Messengers.h"
#include "osd/OSDMap.h"
#include "Objecter.h"

#include "os/ObjectStore.h"
#include "osd/OSD.h"
#include "osd/StateObserver.h"
#include "mon/MonClient.h"

#include "msg/direct/DirectMessenger.h"
#include "msg/FastStrategy.h"

#include "common/TracepointProvider.h"
#include "common/pick_address.h"
#include "common/Finisher.h"
#include "common/common_init.h"
#include "common/errno.h"
#include "include/color.h"

#define dout_subsys ceph_subsys_osd

namespace
{
// Maintain a map to prevent multiple OSDs with the same name
// TODO: allow same name with different cluster name
std::mutex osd_lock;
typedef std::unique_lock<std::mutex> unique_osd_lock;
typedef std::lock_guard<std::mutex> osd_lock_guard;
typedef std::map<int, libosd*> osdmap;
osdmap osds;
}

namespace {

TracepointProvider::Traits osd_tracepoint_traits("libosd_tp.so",
                                                 "osd_tracing");
TracepointProvider::Traits os_tracepoint_traits("libos_tp.so",
                                                "osd_objectstore_tracing");

} // anonymous namespace

namespace ceph
{
namespace osd
{

class LibOSD : private Objecter, public libosd, private OSDStateObserver {
 public:
  CephContext *cct;
 private:
  libosd_callbacks *callbacks;
  void *user;
  Finisher *finisher; // thread to send callbacks to user

  MonClient *monc;
  ObjectStore *store;
  OSD *osd;
  Messengers *ms;
  DirectMessenger *ms_client, *ms_server; // DirectMessenger pair

  struct _osdmap {
    std::mutex mtx;
    std::condition_variable cond;
    typedef std::unique_lock<std::mutex> unique_lock;
    typedef std::lock_guard<std::mutex> lock_guard;
    int state;
    epoch_t epoch;
    bool shutdown;
  } osdmap;

  // OSDStateObserver
  void on_osd_state(int state, epoch_t epoch);

  // Objecter
  bool wait_for_active(epoch_t *epoch);

  void init_dispatcher(OSD *osd);

public:
  LibOSD(int whoami);
  ~LibOSD();

  int init(const libosd_init_args *args);

  // libosd interface
  void join();
  void shutdown();
  void signal(int signum);

  int64_t get_volume(const char *name, uint8_t id[16]);

  // read/write/truncate satisfied by Objecter
  int read(const char *object, const uint8_t volume[16],
	   uint64_t offset, uint64_t length, char *data,
	   int flags, libosd_io_completion_fn cb, void *user) {
    return Objecter::read(object, volume, offset, length,
			  data, flags, cb, user);
  }
  int write(const char *object, const uint8_t volume[16],
	    uint64_t offset, uint64_t length, char *data,
	    int flags, libosd_io_completion_fn cb, void *user) {
    return Objecter::write(object, volume, offset, length,
			   data, flags, cb, user);
  }
  int truncate(const char *object, uint8_t volume[16], uint64_t offset,
	       int flags, libosd_io_completion_fn cb, void *user) {
    char *id= (char *)volume; // FIXME
    int64_t pool_id = get_volume(id,volume);
    OSDMapRef osdmap = osd->service.get_osdmap();
    return Objecter::truncate(object, pool_id, osdmap, offset, flags, cb, user);
  }
};


LibOSD::LibOSD(int whoami)
  : libosd(whoami),
    cct(nullptr),
    callbacks(nullptr),
    user(nullptr),
    finisher(nullptr),
    monc(nullptr),
    store(nullptr),
    osd(nullptr),
    ms(nullptr),
    ms_client(nullptr),
    ms_server(nullptr)
{
  osdmap.state = 0;
  osdmap.epoch = 0;
  osdmap.shutdown = false;
}

LibOSD::~LibOSD()
{
  delete osd;
  delete monc;
  delete ms_server;
  delete ms_client;
  delete ms;
  if (finisher) {
    finisher->stop();
    delete finisher;
  }
}

int LibOSD::init(const struct libosd_init_args *args)
{
  callbacks = args->callbacks;
  user = args->user;

  // create the CephContext and parse the configuration
  int r = ceph::osd::context_create(args->id, args->config, args->cluster,
				    args->argc, args->argv, &cct);
  if (r != 0)
    return r;

  common_init_finish(cct);

  // monitor client
  monc = new MonClient(cct);
  r = monc->build_initial_monmap();
  if (r < 0)
    return r;

  const entity_name_t me(entity_name_t::OSD(whoami));
  const pid_t pid = getpid();

  g_ceph_context = cct; // FIXME
  g_conf = cct->_conf;  // FIXME

  // create and bind messengers
  ms = new Messengers();
  r = ms->create(cct, cct->_conf, me, pid);
  if (r != 0) {
    derr << TEXT_RED << " ** ERROR: messenger creation failed: "
	 << cpp_strerror(-r) << TEXT_NORMAL << dendl;
    return r;
  }
  g_ceph_context->crush_location.init_on_startup(); // FIXME

  r = ms->bind(cct, cct->_conf);
  if (r != 0) {
    derr << TEXT_RED << " ** ERROR: bind failed: " << cpp_strerror(-r)
	 << TEXT_NORMAL << dendl;
    return r;
  }

  // the store
  ObjectStore *store = ObjectStore::create(cct,
					   cct->_conf->osd_objectstore,
					   cct->_conf->osd_data,
					   cct->_conf->osd_journal);
  if (!store) {
    derr << "unable to create object store" << dendl;
    return -ENODEV;
  }

  // create osd
  osd = new OSD(cct, store, whoami,
		ms->cluster, ms->client, ms->client_hb, ms->back_hb,
		ms->front_hb, ms->back_hb, ms->client,
		monc, cct->_conf->osd_data, cct->_conf->osd_journal);

  // set up the dispatcher
  init_dispatcher(osd);

  // initialize osd
  r = osd->pre_init();
  if (r < 0) {
    derr << TEXT_RED << " ** ERROR: osd pre_init failed: " << cpp_strerror(-r)
	 << TEXT_NORMAL << dendl;
    return r;
  }

  // start callback finisher thread
  finisher = new Finisher(cct);
  finisher->start();

  // register for state change notifications
  osd->add_state_observer(this);

  // start messengers
  ms->start();
  ms_client->start();
  ms_server->start();

  TracepointProvider::initialize<osd_tracepoint_traits>(g_ceph_context);
  TracepointProvider::initialize<os_tracepoint_traits>(g_ceph_context);

  // start osd
  r = osd->init();
  if (r < 0) {
    derr << TEXT_RED << " ** ERROR: osd init failed: " << cpp_strerror(-r)
	 << TEXT_NORMAL << dendl;
    return r;
  }
  return 0;
}

void LibOSD::init_dispatcher(OSD *osd)
{
  const entity_name_t name(entity_name_t::CLIENT(whoami));

  // construct and attach the direct messenger pair
  ms_client = new DirectMessenger(cct, name, "direct osd client",
				  0, new FastStrategy());
  ms_server = new DirectMessenger(cct, name, "direct osd server",
				  0, new FastStrategy());

  ms_client->set_direct_peer(ms_server);
  ms_server->set_direct_peer(ms_client);

  ms_server->add_dispatcher_head(osd);

  // create a client connection
  ConnectionRef conn = ms_client->get_connection(ms_server->get_myinst());

  // attach a session; we bypass OSD::ms_verify_authorizer, which
  // normally takes care of this
  OSD::Session *s = new OSD::Session(cct);
  s->con = conn;
  s->entity_name.set_name(ms_client->get_myname());
  s->auid = CEPH_AUTH_UID_DEFAULT;
  s->caps.set_allow_all(); // FIXME
  conn->set_priv(s);

  // allocate the dispatcher
  dispatcher.reset(new Dispatcher(cct, ms_client, conn));

  ms_client->add_dispatcher_head(dispatcher.get());
}

struct C_StateCb : public ::Context {
  typedef void (*callback_fn)(struct libosd *osd, void *user);
  callback_fn cb;
  libosd *osd;
  void *user;
  C_StateCb(callback_fn cb, libosd *osd, void *user)
    : cb(cb), osd(osd), user(user) {}
  void finish(int r) {
    cb(osd, user);
  }
};

void LibOSD::on_osd_state(int state, epoch_t epoch)
{
  ldout(cct, 1) << "on_osd_state " << state << " epoch " << epoch << dendl;

  _osdmap::lock_guard lock(osdmap.mtx);
  if (osdmap.state != state) {
    osdmap.state = state;
    osdmap.cond.notify_all();

    if (state == OSD::STATE_ACTIVE) {
      if (callbacks && callbacks->osd_active)
	finisher->queue(new C_StateCb(callbacks->osd_active, this, user));
    } else if (state == OSD::STATE_STOPPING) {
      // make osd_shutdown calback only if we haven't called libosd_shutdown()
      if (!osdmap.shutdown && callbacks && callbacks->osd_shutdown)
	finisher->queue(new C_StateCb(callbacks->osd_shutdown, this, user));

      ms_client->shutdown();
      ms_server->shutdown();
    }
  }
  osdmap.epoch = epoch;
}

bool LibOSD::wait_for_active(epoch_t *epoch)
{
  _osdmap::unique_lock l(osdmap.mtx);
  while (osdmap.state != OSD::STATE_ACTIVE
      && osdmap.state != OSD::STATE_STOPPING)
    osdmap.cond.wait(l);

  *epoch = osdmap.epoch;
  return osdmap.state != OSD::STATE_STOPPING;
}

void LibOSD::join()
{
  // wait on messengers
  ms->wait();
  ms_client->wait();
  ms_server->wait();
}

void LibOSD::shutdown()
{
  _osdmap::unique_lock l(osdmap.mtx);
  osdmap.shutdown = true;
  l.unlock();

  osd->shutdown();
}

void LibOSD::signal(int signum)
{
  osd->handle_signal(signum);
}

int64_t LibOSD::get_volume(const char *name, uint8_t id[16])
{
  // wait for osdmap
  epoch_t epoch;
  if (!wait_for_active(&epoch))
    return -ENODEV;

  OSDMapRef osdmap = osd->service.get_osdmap();
  try {
    return osdmap->lookup_pg_pool_name("rbd"); // FIXME
  } catch (std::exception& e) {
    return -ENOENT;
  }
}

} // namespace osd
} // namespace ceph


// C interface

struct libosd* libosd_init(const struct libosd_init_args *args)
{
  if (args == nullptr)
    return nullptr;

  ceph::osd::LibOSD *osd;
  {
    // protect access to the map of osds
    osd_lock_guard lock(osd_lock);

    // existing osd with this name?
    std::pair<osdmap::iterator, bool> result =
      osds.insert(osdmap::value_type(args->id, nullptr));
    if (!result.second) {
      return nullptr;
    }

    result.first->second = osd = new ceph::osd::LibOSD(args->id);
  }

  try {
    if (osd->init(args) == 0)
      return osd;
  } catch (std::exception &e) {
  }

  // remove from the map of osds
  unique_osd_lock ol(osd_lock);
  osds.erase(args->id);
  ol.unlock();

  delete osd;
  return nullptr;
}

void libosd_join(struct libosd *osd)
{
  try {
    osd->join();
  } catch (std::exception &e) {
    CephContext *cct = static_cast<ceph::osd::LibOSD*>(osd)->cct;
    lderr(cct) << "libosd_join caught exception " << e.what() << dendl;
  }
}

void libosd_shutdown(struct libosd *osd)
{
  try {
    osd->shutdown();
  } catch (std::exception &e) {
    CephContext *cct = static_cast<ceph::osd::LibOSD*>(osd)->cct;
    lderr(cct) << "libosd_shutdown caught exception " << e.what() << dendl;
  }
}

void libosd_cleanup(struct libosd *osd)
{
  // assert(!running)
  const int id = osd->whoami;
  // delete LibOSD because base destructor is protected
  delete static_cast<ceph::osd::LibOSD*>(osd);

  // remove from the map of osds
  unique_osd_lock ol(osd_lock);
  osds.erase(id);
  ol.unlock();
}

void libosd_signal(int signum)
{
  // signal all osds under list lock
  osd_lock_guard lock(osd_lock);

  for (auto osd : osds) {
    try {
      osd.second->signal(signum);
    } catch (std::exception &e) {
      CephContext *cct = static_cast<ceph::osd::LibOSD*>(osd.second)->cct;
      lderr(cct) << "libosd_signal caught exception " << e.what() << dendl;
    }
  }
}

int libosd_get_volume(struct libosd *osd, const char *name,
		      uint8_t id[16])
{
  try {
    return osd->get_volume(name, id);
  } catch (std::exception &e) {
    CephContext *cct = static_cast<ceph::osd::LibOSD*>(osd)->cct;
    lderr(cct) << "libosd_get_volume caught exception " << e.what() << dendl;
    return -EFAULT;
  }
}

int libosd_read(struct libosd *osd, const char *object,
		const uint8_t volume[16], uint64_t offset, uint64_t length,
		char *data, int flags, libosd_io_completion_fn cb,
		void *user)
{
  try {
    return osd->read(object, volume, offset, length, data, flags, cb, user);
  } catch (std::exception &e) {
    CephContext *cct = static_cast<ceph::osd::LibOSD*>(osd)->cct;
    lderr(cct) << "libosd_read caught exception " << e.what() << dendl;
    return -EFAULT;
  }
}

int libosd_write(struct libosd *osd, const char *object, const uint8_t volume[16],
		 uint64_t offset, uint64_t length, char *data, int flags,
		 libosd_io_completion_fn cb, void *user)
{
  try {
    return osd->write(object, volume, offset, length, data, flags, cb, user);
  } catch (std::exception &e) {
    CephContext *cct = static_cast<ceph::osd::LibOSD*>(osd)->cct;
    lderr(cct) << "libosd_write caught exception " << e.what() << dendl;
    return -EFAULT;
  }
}

// FIXME - locater key? namespace?
int libosd_truncate(struct libosd *osd, const char *object,
		    uint8_t volume[16], uint64_t offset,
		    int flags, libosd_io_completion_fn cb, void *user)
{
  try {
    return osd->truncate(object, volume, offset, flags, cb, user);
  } catch (std::exception &e) {
    CephContext *cct = static_cast<ceph::osd::LibOSD*>(osd)->cct;
    lderr(cct) << "libosd_truncate caught exception " << e.what()
		    << dendl;
    return -EFAULT;
  }
}
