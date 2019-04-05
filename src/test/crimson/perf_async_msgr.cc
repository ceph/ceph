// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-

#include <boost/program_options/variables_map.hpp>
#include <boost/program_options/parsers.hpp>

#include "auth/Auth.h"
#include "global/global_init.h"
#include "msg/Dispatcher.h"
#include "msg/Messenger.h"
#include "messages/MOSDOp.h"
#include "messages/MOSDOpReply.h"

#include "auth/DummyAuth.h"

namespace {

constexpr int CEPH_OSD_PROTOCOL = 10;

struct Server {
  Server(CephContext* cct)
    : dummy_auth(cct), dispatcher(cct)
  {
    msgr.reset(Messenger::create(cct, "async", entity_name_t::OSD(0), "server", 0, 0));
    dummy_auth.auth_registry.refresh_config();
    msgr->set_cluster_protocol(CEPH_OSD_PROTOCOL);
    msgr->set_default_policy(Messenger::Policy::stateless_server(0));
    msgr->set_auth_client(&dummy_auth);
    msgr->set_auth_server(&dummy_auth);
    dispatcher.ms_set_require_authorizer(false);
  }
  DummyAuthClientServer dummy_auth;
  unique_ptr<Messenger> msgr;
  struct ServerDispatcher : Dispatcher {
    ServerDispatcher(CephContext* cct)
      : Dispatcher(cct)
    {}
    bool ms_can_fast_dispatch_any() const override {
      return true;
    }
    bool ms_can_fast_dispatch(const Message* m) const override {
      return m->get_type() == CEPH_MSG_OSD_OP;
    }
    void ms_fast_dispatch(Message* m) override {
      ceph_assert(m->get_type() == CEPH_MSG_OSD_OP);
      MOSDOp *req = static_cast<MOSDOp*>(m);
      m->get_connection()->send_message(new MOSDOpReply(req, 0, 0, 0, false));
      m->put();
    }
    bool ms_dispatch(Message*) override {
      ceph_abort();
    }
    bool ms_handle_reset(Connection*) override {
      return true;
    }
    void ms_handle_remote_reset(Connection*) override {
    }
    bool ms_handle_refused(Connection*) override {
      return true;
    }
  } dispatcher;
};

}

static void run(CephContext* cct, entity_addr_t addr)
{
  std::cout << "async server listening at " << addr << std::endl;
  Server server{cct};
  server.msgr->bind(addr);
  server.msgr->add_dispatcher_head(&server.dispatcher);
  server.msgr->start();
  server.msgr->wait();
}

int main(int argc, char** argv)
{
  namespace po = boost::program_options;
  po::options_description desc{"Allowed options"};
  desc.add_options()
    ("help,h", "show help message")
    ("addr", po::value<std::string>()->default_value("v1:0.0.0.0:9010"),
     "server address");
  po::variables_map vm;
  std::vector<std::string> unrecognized_options;
  try {
    auto parsed = po::command_line_parser(argc, argv)
      .options(desc)
      .allow_unregistered()
      .run();
    po::store(parsed, vm);
    if (vm.count("help")) {
      std::cout << desc << std::endl;
      return 0;
    }
    po::notify(vm);
    unrecognized_options = po::collect_unrecognized(parsed.options, po::include_positional);
  } catch(const po::error& e) {
    std::cerr << "error: " << e.what() << std::endl;
    return 1;
  }

  auto addr = vm["addr"].as<std::string>();
  entity_addr_t target_addr;
  target_addr.parse(addr.c_str(), nullptr);

  std::vector<const char*> args(argv, argv + argc);
  auto cct = global_init(nullptr, args,
                         CEPH_ENTITY_TYPE_CLIENT,
                         CODE_ENVIRONMENT_UTILITY,
                         CINIT_FLAG_NO_MON_CONFIG);
  common_init_finish(cct.get());
  run(cct.get(), target_addr);
}
