#include "include/rados/librados.h"
#include "include/rados/librados.hpp"
#include "test/librados/test.h"
#include "test/librados/TestCase.h"
#include "include/encoding.h"
#include "tools/cephfs/DataScan.h"
#include "global/global_init.h"

#include <algorithm>
#include <errno.h>
#include "gtest/gtest.h"


using namespace librados;

typedef RadosTestPP TmapMigratePP;

TEST_F(TmapMigratePP, DataScan) {
  std::vector<const char *> args;
  global_init(NULL, args, CEPH_ENTITY_TYPE_CLIENT, CODE_ENVIRONMENT_UTILITY, 0);
  common_init_finish(g_ceph_context);

  // DataScan isn't namespace-aware, so override RadosTestPP's default
  // behaviour of putting everything into a namespace
  ioctx.set_namespace("");

  bufferlist header;
  std::map<std::string, bufferlist> kvs;
  bufferlist val;
  val.append("custard");
  kvs.insert({"rhubarb", val});

  bufferlist tmap_trans;
  ::encode(header, tmap_trans);
  ::encode(kvs, tmap_trans);

  // Create a TMAP object
  ASSERT_EQ(0, ioctx.tmap_put("10000000000.00000000", tmap_trans));

  // Create an OMAP object
  std::map<std::string, bufferlist> omap_kvs;
  bufferlist omap_val;
  omap_val.append("waffles");
  omap_kvs.insert({"tasty", omap_val});
  ASSERT_EQ(0, ioctx.omap_set("10000000001.00000000", omap_kvs));

  DataScan ds;
  ASSERT_EQ(0, ds.init());
  int r = ds.main({"tmap_upgrade", pool_name.c_str()});
  ASSERT_EQ(r, 0);
  ds.shutdown();

  // Check that the TMAP object is now an omap object
  std::map<std::string, bufferlist> read_vals;
  ASSERT_EQ(0, ioctx.omap_get_vals("10000000000.00000000", "", 1, &read_vals));
  ASSERT_EQ(read_vals.size(), 1);
  bufferlist tmap_expect_val;
  tmap_expect_val.append("custard");
  ASSERT_EQ(read_vals.at("rhubarb"), tmap_expect_val);


  // Check that the OMAP object is still readable
  read_vals.clear();
  ASSERT_EQ(0, ioctx.omap_get_vals("10000000001.00000000", "", 1, &read_vals));
  ASSERT_EQ(read_vals.size(), 1);
  bufferlist expect_omap_val;
  expect_omap_val.append("waffles");
  ASSERT_EQ(read_vals.at("tasty"), expect_omap_val);
}

