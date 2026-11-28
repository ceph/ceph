#include <gtest/gtest.h>

#include <boost/asio/io_context.hpp>

#include "cls/lock/cls_lock_client.h"
#include "cls/lock/cls_lock_ops.h"
#include "crush/CrushWrapper.h"
#include "global/global_context.h"
#include "include/rados.h"
#include "include/rados/librados.hpp"
#include "librados/AioCompletionImpl.h"
#include "librados/IoCtxImpl.h"
#include "librados/RadosClient.h"
#include "messages/MOSDOp.h"
#include "messages/MOSDOpReply.h"
#include "mon/MonClient.h"
#include "msg/Messenger.h"
#include "osd/OSDMap.h"
#include "osdc/Objecter.h"
#include "test/osdc/RecordingConnection.h"
#include "test/osd/OSDMapTestHelpers.h"

class ObjclassReadFlags : public ::testing::Test {
protected:
  static constexpr int num_osds = 3;
  static constexpr int64_t pool_id = 1;
  const pg_t pgid{0, pool_id};
  const std::vector<int> acting = {0, 1, 2};

  boost::asio::io_context ioc;
  MonClient monc{g_ceph_context, ioc};
  std::unique_ptr<Messenger> msgr{
    Messenger::create_client_messenger(g_ceph_context, "client")};
  std::unique_ptr<Objecter> objecter;
  std::unique_ptr<librados::RadosClient> rados;
  std::map<int, ceph::ref_t<RecordingConnection>> cons;

  void SetUp() override
  {
    set_conf("rados_replica_read_policy", "balance");
    set_conf("rados_replica_read_policy_on_objclass", "false");

    rados = std::make_unique<librados::RadosClient>(g_ceph_context);
    objecter =
      std::make_unique<Objecter>(g_ceph_context, msgr.get(), &monc, ioc);
    objecter->init();

    OSDMap map;
    uuid_d fsid;
    fsid.generate_random();
    ceph_assert(map.build_simple(g_ceph_context, 1, fsid, num_osds) == 0);
    for (int i = 0; i < num_osds; i++) {
      map.set_state(i, CEPH_OSD_EXISTS | CEPH_OSD_UP);
    }

    pg_pool_t pool;
    pool.type = pg_pool_t::TYPE_REPLICATED;
    pool.size = num_osds;
    pool.min_size = 2;
    pool.set_pg_num(1);
    pool.set_pgp_num(1);
    OSDMapTestHelpers::add_pool(map, pool_id, pool);
    OSDMapTestHelpers::set_pg_acting(map, pgid, acting);

    objecter->start(&map);

    for (int osd = 0; osd < num_osds; osd++) {
      auto s = new Objecter::OSDSession(g_ceph_context, osd, ioc.get_executor());
      cons[osd] = ceph::make_ref<RecordingConnection>(osd);
      s->con = cons[osd];
      s->con->set_priv(RefCountedPtr{s});
      objecter->osd_sessions[osd] = s;
    }
  }

  void TearDown() override
  {
    objecter->shutdown();
    objecter.reset();
    rados.reset();
  }

  static void set_conf(const char *key, const char *value)
  {
    ceph_assert(g_ceph_context->_conf.set_val(key, value) == 0);
    g_ceph_context->_conf.apply_changes(nullptr);
  }

  void poll()
  {
    ioc.restart();
    ioc.poll();
  }

  librados::IoCtxImpl make_ioctx()
  {
    return librados::IoCtxImpl(rados.get(), objecter.get(), pool_id,
                               CEPH_NOSNAP);
  }

  static void add_call(::ObjectOperation *op)
  {
    bufferlist in;
    op->call("cls_hello", "say_hello", in);
  }

  int sent_flags()
  {
    std::vector<int> flags;
    for (auto& [osd, con] : cons) {
      for (auto& m : con->sent) {
        ceph_assert(m->get_type() == CEPH_MSG_OSD_OP);
        flags.push_back(
          boost::static_pointer_cast<_mosdop::MOSDOp<osdc_opvec>>(m)
            ->get_flags());
      }
    }
    EXPECT_EQ(1u, flags.size()) << "expected exactly one op to be sent";
    return flags.empty() ? 0 : flags.front();
  }
};

TEST_F(ObjclassReadFlags, ExecHonoursRadosReplicaReadPolicyOnObjclass)
{
  auto io = make_ioctx();
  librados::AioCompletionImpl c;
  bufferlist in, out;
  ASSERT_EQ(0, io.aio_exec(object_t("obj"), &c, "cls_hello", "say_hello", in,
                           &out));
  poll();

  int flags = sent_flags();
  EXPECT_TRUE(flags & CEPH_OSD_FLAG_READ);
  EXPECT_FALSE(flags & CEPH_OSD_FLAG_BALANCE_READS);
}

TEST_F(ObjclassReadFlags, ReadOperationHonoursRadosReplicaReadPolicyOnObjclass)
{
  auto io = make_ioctx();
  librados::AioCompletionImpl c;
  bufferlist out;
  ::ObjectOperation op;
  add_call(&op);
  ASSERT_EQ(0, io.aio_operate_read(object_t("obj"), &op, &c, 0, &out));
  poll();

  EXPECT_FALSE(sent_flags() & CEPH_OSD_FLAG_BALANCE_READS);
}

TEST_F(ObjclassReadFlags, ObjecterReadHonoursRadosReplicaReadPolicyOnObjclass)
{
  bufferlist out;
  ::ObjectOperation op;
  add_call(&op);
  C_SaferCond onack;
  objecter->read(object_t("obj"), object_locator_t(pool_id), op, CEPH_NOSNAP,
                 &out, 0, &onack);
  poll();

  EXPECT_FALSE(sent_flags() & CEPH_OSD_FLAG_BALANCE_READS);

  objecter->op_cancel_writes(-ECANCELED);
}

class ObjclassReplicaRead : public ::testing::Test {
protected:
  static constexpr int num_osds = 3;
  static constexpr int64_t pool_id = 1;
  static constexpr int primary = 0;
  static constexpr int local_replica = 2;
  const pg_t pgid{0, pool_id};
  const std::vector<int> acting = {0, 1, 2};

  boost::asio::io_context ioc;
  MonClient monc{g_ceph_context, ioc};
  std::unique_ptr<Messenger> msgr{
    Messenger::create_client_messenger(g_ceph_context, "client")};
  std::unique_ptr<Objecter> objecter;
  std::unique_ptr<librados::RadosClient> rados;
  librados::IoCtx io;
  std::map<int, ceph::ref_t<RecordingConnection>> cons;

  void SetUp() override
  {
    ceph_assert(g_ceph_context->_conf.set_val(
      "rados_replica_read_policy", "localize") == 0);
    ceph_assert(g_ceph_context->_conf.set_val(
      "rados_replica_read_policy_on_objclass", "false") == 0);
    g_ceph_context->_conf.apply_changes(nullptr);

    rados = std::make_unique<librados::RadosClient>(g_ceph_context);
    objecter =
      std::make_unique<Objecter>(g_ceph_context, msgr.get(), &monc, ioc);
    objecter->init();

    OSDMap map;
    uuid_d fsid;
    fsid.generate_random();
    ceph_assert(map.build_simple(g_ceph_context, 1, fsid, num_osds) == 0);
    for (int i = 0; i < num_osds; i++) {
      map.set_state(i, CEPH_OSD_EXISTS | CEPH_OSD_UP);
    }

    CrushWrapper crush;
    crush.create();
    OSDMap::_build_crush_types(crush);
    int rootid = 0;
    ceph_assert(crush.add_bucket(0, CRUSH_BUCKET_STRAW2, CRUSH_HASH_DEFAULT,
                                 crush.get_type_id("root"), 0, nullptr,
                                 nullptr, &rootid) == 0);
    crush.set_item_name(rootid, "default");
    for (int osd = 0; osd < num_osds; osd++) {
      std::map<std::string, std::string> loc = {
        {"root", "default"},
        {"host", "host" + std::to_string(osd)}};
      crush.insert_item(g_ceph_context, osd, 1.0, "osd." + std::to_string(osd),
                        loc);
    }
    crush.finalize();
    OSDMap::Incremental crush_inc(map.get_epoch() + 1);
    crush_inc.fsid = map.get_fsid();
    crush.encode(crush_inc.crush, CEPH_FEATURES_SUPPORTED_DEFAULT);
    map.apply_incremental(crush_inc);

    pg_pool_t pool;
    pool.type = pg_pool_t::TYPE_REPLICATED;
    pool.size = num_osds;
    pool.min_size = 1;
    pool.set_pg_num(1);
    pool.set_pgp_num(1);
    OSDMapTestHelpers::add_pool(map, pool_id, pool);
    OSDMapTestHelpers::set_pg_acting(map, pgid, acting);

    objecter->start(&map);
    objecter->crush_location =
      {{"host", "host" + std::to_string(local_replica)}};

    monc.sub_want("osdmap", map.get_epoch() + 100, CEPH_SUBSCRIBE_ONETIME);

    for (int osd = 0; osd < num_osds; osd++) {
      auto s = new Objecter::OSDSession(g_ceph_context, osd, ioc.get_executor());
      cons[osd] = ceph::make_ref<RecordingConnection>(osd);
      s->con = cons[osd];
      s->con->set_priv(RefCountedPtr{s});
      objecter->osd_sessions[osd] = s;
    }

    librados::IoCtx::from_rados_ioctx_t(
      new librados::IoCtxImpl(rados.get(), objecter.get(), pool_id,
                              CEPH_NOSNAP),
      io);
  }

  void TearDown() override
  {
    io.close();
    objecter->shutdown();
    objecter.reset();
    rados.reset();
    ceph_assert(g_ceph_context->_conf.set_val(
      "rados_replica_read_policy", "default") == 0);
    g_ceph_context->_conf.apply_changes(nullptr);
  }

  void poll()
  {
    ioc.restart();
    ioc.poll();
  }

  std::vector<ceph::ref_t<_mosdop::MOSDOp<osdc_opvec>>> sent_to(int osd)
  {
    std::vector<ceph::ref_t<_mosdop::MOSDOp<osdc_opvec>>> ops;
    for (auto& m : cons[osd]->sent) {
      ceph_assert(m->get_type() == CEPH_MSG_OSD_OP);
      ops.push_back(
        boost::static_pointer_cast<_mosdop::MOSDOp<osdc_opvec>>(m));
    }
    return ops;
  }

  void reply(int osd, int rc)
  {
    auto op = sent_to(osd).back();
    auto r = ceph::make_message<MOSDOpReply>();
    r->set_tid(op->get_tid());
    r->set_result(rc);
    r->set_op_returns(std::vector<pg_log_op_return_item_t>(op->ops.size()));
    r->set_connection(cons[osd]);
    objecter->ms_fast_dispatch2(r);
    poll();
  }

  int submit_via_read_operation(librados::AioCompletion *c, bufferlist *out)
  {
    librados::ObjectReadOperation op;
    rados::cls::lock::get_lock_info_start(&op, "testlock");
    return io.aio_operate("obj", c, &op, out);
  }

  int submit_via_exec(librados::AioCompletion *c, bufferlist *out)
  {
    bufferlist in;
    cls_lock_get_info_op call;
    call.name = "testlock";
    encode(call, in);
    return io.aio_exec("obj", c, rados::cls::lock::method::get_info, in, out);
  }
};

TEST_F(ObjclassReplicaRead, ExecKeepsAClsCallOnThePrimary)
{
  bufferlist out;
  auto *c = librados::Rados::aio_create_completion();
  ASSERT_EQ(0, submit_via_exec(c, &out));
  poll();

  EXPECT_EQ(1u, sent_to(primary).size());
  EXPECT_EQ(0u, sent_to(local_replica).size());

  reply(primary, 0);
  ASSERT_EQ(0, c->wait_for_complete());
  c->release();
}

TEST_F(ObjclassReplicaRead, ReadOperationKeepsAClsCallOnThePrimary)
{
  bufferlist out;
  auto *c = librados::Rados::aio_create_completion();
  ASSERT_EQ(0, submit_via_read_operation(c, &out));
  poll();

  ASSERT_EQ(1u, sent_to(primary).size());
  ASSERT_EQ(0u, sent_to(local_replica).size());
  EXPECT_FALSE(sent_to(primary).back()->get_flags() &
               CEPH_OSD_FLAG_LOCALIZE_READS);

  reply(primary, 0);
  ASSERT_EQ(0, c->wait_for_complete());
  c->release();

  EXPECT_EQ(1u, sent_to(primary).size());
}
