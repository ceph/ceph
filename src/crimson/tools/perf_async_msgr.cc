// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-

#include <boost/program_options/variables_map.hpp>
#include <boost/program_options/parsers.hpp>

#include "auth/Auth.h"
#include "global/global_init.h"
#include "msg/Dispatcher.h"
#include "msg/Messenger.h"
#include "messages/MOSDOp.h"

#include "auth/DummyAuth.h"

namespace {

constexpr int CEPH_OSD_PROTOCOL = 10;

struct Server {
  Server(CephContext* cct, unsigned msg_len)
    : dummy_auth(cct), dispatcher(cct, msg_len)
  {
    msgr.reset(Messenger::create(cct, "async", entity_name_t::OSD(0), "server", 0));
    dummy_auth.auth_registry.refresh_config();
    msgr->set_cluster_protocol(CEPH_OSD_PROTOCOL);
    msgr->set_default_policy(Messenger::Policy::stateless_server(0));
    msgr->set_auth_client(&dummy_auth);
    msgr->set_auth_server(&dummy_auth);
  }
  DummyAuthClientServer dummy_auth;
  std::unique_ptr<Messenger> msgr;
  struct ServerDispatcher : Dispatcher {
    unsigned msg_len = 0;
    bufferlist msg_data;

    ServerDispatcher(CephContext* cct, unsigned msg_len)
      : Dispatcher(cct), msg_len(msg_len)
    {
      msg_data.append_zero(msg_len);
    }
    bool ms_can_fast_dispatch_any() const override {
      return true;
    }
    bool ms_can_fast_dispatch(const Message* m) const override {
      return m->get_type() == CEPH_MSG_OSD_OP;
    }
    void ms_fast_dispatch(Message* m) override {
      ceph_assert(m->get_type() == CEPH_MSG_OSD_OP);
      const static pg_t pgid;
      const static object_locator_t oloc;
      const static hobject_t hobj(object_t(), oloc.key, CEPH_NOSNAP, pgid.ps(),
                                  pgid.pool(), oloc.nspace);
      static spg_t spgid(pgid);
      MOSDOp *rep = new MOSDOp(0, 0, hobj, spgid, 0, 0, 0);
      bufferlist data(msg_data);
      rep->write(0, msg_len, data);
      rep->set_tid(m->get_tid());
      m->get_connection()->send_message(rep);
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

static void run(CephContext* cct, entity_addr_t addr, unsigned bs)
{
  std::cout << "async server listening at " << addr << std::endl;
  Server server{cct, bs};
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
    ("addr", po::value<std::string>()->default_value("v2:127.0.0.1:9010"),
     "server address(crimson only supports msgr v2 protocol)")
    ("bs", po::value<unsigned>()->default_value(0),
     "server block size")
    ("crc-enabled", po::value<bool>()->default_value(false),
     "enable CRC checks")
    ("threads", po::value<unsigned>()->default_value(3),
     "async messenger worker threads");
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
  ceph_assert_always(target_addr.is_msgr2());
  auto bs = vm["bs"].as<unsigned>();
  auto crc_enabled = vm["crc-enabled"].as<bool>();
  auto worker_threads = vm["threads"].as<unsigned>();

  std::vector<const char*> args(argv, argv + argc);
  auto cct = global_init(nullptr, args,
                         CEPH_ENTITY_TYPE_CLIENT,
                         CODE_ENVIRONMENT_UTILITY,
                         CINIT_FLAG_NO_MON_CONFIG);
  common_init_finish(cct.get());

  if (crc_enabled) {
    cct->_conf.set_val("ms_crc_header", "true");
    cct->_conf.set_val("ms_crc_data", "true");
  } else {
    cct->_conf.set_val("ms_crc_header", "false");
    cct->_conf.set_val("ms_crc_data", "false");
  }

  cct->_conf.set_val("ms_async_op_threads", fmt::format("{}", worker_threads));

  std::cout << "server[" << addr
            << "](bs=" << bs
            << ", crc_enabled=" << crc_enabled
            << ", worker_threads=" << worker_threads
            << std::endl;

  run(cct.get(), target_addr, bs);
}
