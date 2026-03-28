// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include "test/crimson/gtest_seastar.h"

#include "common/ceph_context.h"
#include "crimson/client/io_context.h"
#include "crimson/client/rados_client.h"
#include "crimson/common/auth_handler.h"
#include "crimson/osdc/objecter.h"
#include "crimson/net/Messenger.h"
#include "crimson/mon/MonClient.h"
#include "osd/OSDMap.h"
#include "include/object.h"

using namespace crimson;

namespace {

class DummyAuthHandler : public crimson::common::AuthHandler {
public:
  void handle_authentication(const EntityName&, const AuthCapsInfo&) final {}
};

std::unique_ptr<OSDMap> build_test_osdmap()
{
  auto cct = new CephContext(CEPH_ENTITY_TYPE_CLIENT, CODE_ENVIRONMENT_UTILITY, 0);
  auto osdmap = std::make_unique<OSDMap>();
  uuid_d fsid;
  const int r = osdmap->build_simple_with_pool(cct, 1, fsid, 1, 0, 0);
  cct->put();
  if (r != 0) {
    return nullptr;
  }

  // Mark OSD 0 as up and in
  OSDMap::Incremental inc(osdmap->get_epoch() + 1);
  inc.fsid = osdmap->get_fsid();
  entity_addrvec_t addrs;
  addrs.v.push_back(entity_addr_t());
  uuid_d uuid;
  uuid.generate_random();
  addrs.v[0].nonce = 0;
  inc.new_state[0] = CEPH_OSD_EXISTS | CEPH_OSD_NEW;
  inc.new_up_client[0] = addrs;
  inc.new_up_cluster[0] = addrs;
  inc.new_hb_back_up[0] = addrs;
  inc.new_hb_front_up[0] = addrs;
  inc.new_weight[0] = CEPH_OSD_IN;
  inc.new_uuid[0] = uuid;
  osdmap->apply_incremental(inc);

  return osdmap;
}

} // namespace

struct objecter_test_t : public seastar_test_suite_t {};

#ifdef UNIT_TESTS_BUILT
TEST_F(objecter_test_t, calc_target_with_injected_osdmap)
{
  run_async([] {
    auto osdmap = build_test_osdmap();
    ASSERT_TRUE(osdmap) << "failed to build test OSDMap";

    auto msgr = crimson::net::Messenger::create(
      entity_name_t::CLIENT(0), "test", 0, true);
    DummyAuthHandler auth_handler;
    crimson::mon::Client monc(*msgr, auth_handler);
    crimson::osdc::Objecter objecter(*msgr, monc);

    objecter.inject_osdmap_for_test(std::move(osdmap)).get();

    object_t oid("test-obj");
    object_locator_t oloc(1);  // pool 1 (rbd from build_simple_with_pool)

    auto target = objecter.calc_target(oid, oloc).get();
    ASSERT_TRUE(target.has_value()) << "calc_target should succeed with valid pool";
    EXPECT_GE(target->primary_osd, 0);
    EXPECT_EQ(target->pgid.pool(), 1);
  });
}

TEST_F(objecter_test_t, calc_target_nonexistent_pool)
{
  run_async([] {
    auto osdmap = build_test_osdmap();
    ASSERT_TRUE(osdmap);

    auto msgr = crimson::net::Messenger::create(
      entity_name_t::CLIENT(0), "test", 0, true);
    DummyAuthHandler auth_handler;
    crimson::mon::Client monc(*msgr, auth_handler);
    crimson::osdc::Objecter objecter(*msgr, monc);

    objecter.inject_osdmap_for_test(std::move(osdmap)).get();

    object_t oid("test-obj");
    object_locator_t oloc(999);  // non-existent pool

    auto target = objecter.calc_target(oid, oloc).get();
    EXPECT_FALSE(target.has_value()) << "calc_target should fail for nonexistent pool";
  });
}
#endif

TEST_F(objecter_test_t, iocontext_get_pool_id)
{
  run_async([] {
    auto msgr = crimson::net::Messenger::create(
      entity_name_t::CLIENT(0), "test", 0, true);
    DummyAuthHandler auth_handler;
    crimson::mon::Client monc(*msgr, auth_handler);
    crimson::osdc::Objecter objecter(*msgr, monc);

    crimson::client::IoCtx ioctx(objecter, 42, CEPH_NOSNAP);
    EXPECT_EQ(ioctx.get_pool_id(), 42);
    EXPECT_EQ(ioctx.get_snap_seq(), CEPH_NOSNAP);
  });
}

#ifdef UNIT_TESTS_BUILT
/// Verify IoCtx has discard, write_zeroes, compare_and_write, exec APIs.
/// Full I/O testing requires a real cluster (crimson-rados-demo with vstart,
/// run_crimson_rados_demo_integration_test.sh).
TEST_F(objecter_test_t, iocontext_discard_write_zeroes_api)
{
  run_async([] {
    auto msgr = crimson::net::Messenger::create(
      entity_name_t::CLIENT(0), "test", 0, true);
    DummyAuthHandler auth_handler;
    crimson::mon::Client monc(*msgr, auth_handler);
    crimson::osdc::Objecter objecter(*msgr, monc);

    crimson::client::IoCtx ioctx(objecter, 1, CEPH_NOSNAP);

    // Verify API compiles and returns seastar::future<>
    static_assert(std::is_same_v<decltype(ioctx.discard("", 0, 0)),
                                 seastar::future<>>);
    static_assert(std::is_same_v<decltype(ioctx.write_zeroes("", 0, 0)),
                                 seastar::future<>>);

    ceph::bufferlist cmp_bl, write_bl;
    cmp_bl.append_zero(4);
    write_bl.append_zero(4);
    static_assert(std::is_same_v<decltype(ioctx.compare_and_write(
                                   "", 0, std::move(cmp_bl), 0, std::move(write_bl))),
                                 seastar::future<>>);
  });
}

/// Verify IoCtx has exec() API for cls calls.
TEST_F(objecter_test_t, iocontext_exec_api)
{
  run_async([] {
    auto msgr = crimson::net::Messenger::create(
      entity_name_t::CLIENT(0), "test", 0, true);
    DummyAuthHandler auth_handler;
    crimson::mon::Client monc(*msgr, auth_handler);
    crimson::osdc::Objecter objecter(*msgr, monc);

    crimson::client::IoCtx ioctx(objecter, 1, CEPH_NOSNAP);

    ceph::bufferlist indata;
    static_assert(std::is_same_v<decltype(ioctx.exec("", "cls", "method",
                                                    std::move(indata))),
                                 seastar::future<ceph::bufferlist>>);
  });
}
#endif
