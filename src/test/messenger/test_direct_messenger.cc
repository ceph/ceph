// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <condition_variable>
#include <mutex>
#include "global/global_init.h"
#include "common/ceph_argparse.h"

#include "msg/direct/DirectMessenger.h"
#include "msg/FastStrategy.h"
#include "messages/MDataPing.h"


int main(int argc, const char *argv[]) {
  // command-line arguments
  vector<const char*> args;
  argv_to_vec(argc, argv, args);
  env_to_vec(args);

  global_init(nullptr, args, CEPH_ENTITY_TYPE_ANY, CODE_ENVIRONMENT_DAEMON,
	      0);
  CephContext* cct = g_ceph_context;
  common_init_finish(cct);

  /* XXX Cohort GENERIC avoided faking a specific entity when desired */
  const entity_name_t entity1 = entity_name_t::MON(1);
  const entity_name_t entity2 = entity_name_t::MON(2);

  DirectMessenger *m1 = new DirectMessenger(cct,
      entity1, "m1", 0, new FastStrategy());
  DirectMessenger *m2 = new DirectMessenger(cct,
      entity2, "m2", 0, new FastStrategy());

  m1->set_direct_peer(m2);
  m2->set_direct_peer(m1);

  // condition variable to wait on ping reply
  std::mutex mtx;
  std::condition_variable cond;
  bool done;

  class C_NotifyCond : public Context {
    std::mutex *mutex;
    std::condition_variable *cond;
    bool *done;
  public:
    C_NotifyCond(std::mutex *mutex, std::condition_variable *cond, bool *done)
      : mutex(mutex), cond(cond), done(done) {}
    void finish(int r) {
      std::lock_guard<std::mutex> lock(*mutex);
      *done = true;
      cond->notify_one();
    }
  };

  class ClientDispatcher : public Dispatcher {
    Context *c;
  public:
    ClientDispatcher(CephContext *cct, Context *c)
      : Dispatcher(cct), c(c) {}

    bool ms_handle_reset(Connection *con) { return false; }
    void ms_handle_remote_reset(Connection *con) {}

    bool ms_dispatch(Message *m) {
      std::cout << "ClientDispatcher received " << *m << std::endl;
      c->complete(0);
      return true;
    }
  };
  m1->add_dispatcher_head(new ClientDispatcher(cct,
	new C_NotifyCond(&mtx, &cond, &done)));

  class ServerDispatcher : public Dispatcher {
  public:
    ServerDispatcher(CephContext *cct) : Dispatcher(cct) {}

    bool ms_handle_reset(Connection *con) { return false; }
    void ms_handle_remote_reset(Connection *con) {}

    bool ms_dispatch(Message *m) {
      std::cout << "ServerDispatcher received " << *m
	<< ", sending reply" << std::endl;
      ConnectionRef c = m->get_connection();
      c->send_message(new MDataPing());
      return true;
    }
  };
  m2->add_dispatcher_head(new ServerDispatcher(cct));

  // send message to m2
  m1->send_message(new MDataPing(), m2->get_myinst());

  // wait for response
  std::unique_lock<std::mutex> l(mtx);
  while (!done)
    cond.wait(l);
  l.unlock();

  std::cout << "Done" << std::endl;
  return 0;
}
