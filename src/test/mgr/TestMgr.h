// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <Python.h>
#include <cassert>

#include "common/async/blocked_completion.h"
#include "common/async/context_pool.h"
#include "messages/MClientReclaim.h"
#include "messages/MClientSession.h"
#include "global/global_context.h"
#include "client/MetaRequest.h"
#include "osdc/ObjectCacher.h"
#include "osdc/Objecter.h"
#include "msg/Messenger.h"
#include "mon/MonClient.h"
#include "client/Client.h"
#include "gtest/gtest.h"

#define dout_subsys ceph_subsys_client

namespace bs = boost::system;
namespace ca = ceph::async;

class TestMgr : public ::testing::Test {
  public:
    static void SetUpTestSuite()
    {
      icp.start(g_ceph_context->_conf.get_val<std::uint64_t>("client_asio_thread_count"));
    }
    static void TearDownTestSuite()
    {
      icp.stop();
    }

    void SetUp() override
    {
      messenger = Messenger::create_client_messenger(g_ceph_context, "mgrtest");
      if (messenger->start() != 0) {
        throw std::runtime_error("failed to start messenger");
      }

      mc = new MonClient(g_ceph_context, icp);
      if (mc->build_initial_monmap() < 0) {
        throw std::runtime_error("build monmap");
      }
      mc->set_messenger(messenger);
      mc->set_want_keys(CEPH_ENTITY_TYPE_MDS | CEPH_ENTITY_TYPE_OSD);
      if (mc->init() < 0) {
        throw std::runtime_error("init monclient");
      }

      objecter = new Objecter(g_ceph_context, messenger, mc, icp);
      assert(objecter != nullptr);
      objecter->set_client_incarnation(0);
      objecter->init();
      messenger->add_dispatcher_tail(objecter);
      objecter->start();
    }

    void TearDown() override
    {
      assert(objecter != nullptr);
      assert(mc != nullptr);
      assert(messenger != nullptr);

      objecter->shutdown();
      mc->shutdown();
      messenger->shutdown();
      messenger->wait();

      delete objecter;
      objecter = nullptr;
      delete mc;
      mc = nullptr;
      delete messenger;
      messenger = nullptr;
    }
  protected:
    static inline ceph::async::io_context_pool icp;
    static inline UserPerm myperm{0,0};
    MonClient* mc = nullptr;
    Messenger* messenger = nullptr;
    Objecter* objecter = nullptr;
};

struct PythonEnv : public ::testing::Environment
{
  void SetUp()    override { Py_Initialize(); }
  void TearDown() override { Py_Finalize();   }
};
