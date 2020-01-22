// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#include "gtest/gtest.h"

#include "mds/mdstypes.h"
#include "include/buffer.h"
#include "include/rbd_types.h"
#include "include/rados/librados.h"
#include "include/rados/librados.hpp"
#include "include/stringify.h"
#include "include/types.h"
#include "global/global_context.h"
#include "global/global_init.h"
#include "common/ceph_argparse.h"
#include "common/common_init.h"
#include "common/Cond.h"
#include "json_spirit/json_spirit.h"

#include <errno.h>
#include <map>
#include <sstream>
#include <string>

using namespace librados;
using std::map;
using std::ostringstream;
using std::string;

int get_primary_osd(Rados& rados, const string& pool_name,
		    const string& oid, int *pprimary)
{
  bufferlist inbl;
  string cmd = string("{\"prefix\": \"osd map\",\"pool\":\"")
    + pool_name
    + string("\",\"object\": \"")
    + oid
    + string("\",\"format\": \"json\"}");
  bufferlist outbl;
  if (int r = rados.mon_command(cmd, inbl, &outbl, nullptr);
      r < 0) {
    return r;
  }
  string outstr(outbl.c_str(), outbl.length());
  json_spirit::Value v;
  if (!json_spirit::read(outstr, v)) {
    cerr <<" unable to parse json " << outstr << std::endl;
    return -1;
  }

  json_spirit::Object& o = v.get_obj();
  for (json_spirit::Object::size_type i=0; i<o.size(); i++) {
    json_spirit::Pair& p = o[i];
    if (p.name_ == "acting_primary") {
      cout << "primary = " << p.value_.get_int() << std::endl;
      *pprimary = p.value_.get_int();
      return 0;
    }
  }
  cerr << "didn't find primary in " << outstr << std::endl;
  return -1;
}

int fence_osd(Rados& rados, int osd)
{
  bufferlist inbl, outbl;
  string cmd("{\"prefix\": \"injectargs\",\"injected_args\":["
	     "\"--ms-blackhole-osd\", "
	     "\"--ms-blackhole-mon\"]}");
  return rados.osd_command(osd, cmd, inbl, &outbl, NULL);
}

int mark_down_osd(Rados& rados, int osd)
{
  bufferlist inbl, outbl;
  string cmd("{\"prefix\": \"osd down\",\"ids\":[\"" +
	     stringify(osd) + "\"]}");
  return rados.mon_command(cmd, inbl, &outbl, NULL);
}

TEST(OSD, StaleRead) {
  // create two rados instances, one pool
  Rados rados1, rados2;
  IoCtx ioctx1, ioctx2;
  int r;

  r = rados1.init_with_context(g_ceph_context);
  ASSERT_EQ(0, r);
  r = rados1.connect();
  ASSERT_EQ(0, r);

  srand(time(0));
  string pool_name = "read-hole-test-" + stringify(rand());
  r = rados1.pool_create(pool_name.c_str());
  ASSERT_EQ(0, r);

  r = rados1.ioctx_create(pool_name.c_str(), ioctx1);
  ASSERT_EQ(0, r);

  r = rados2.init_with_context(g_ceph_context);
  ASSERT_EQ(0, r);
  r = rados2.connect();
  ASSERT_EQ(0, r);
  r = rados2.ioctx_create(pool_name.c_str(), ioctx2);
  ASSERT_EQ(0, r);

  string oid = "foo";
  bufferlist one;
  one.append("one");
  {
    cout << "client1: writing 'one'" << std::endl;
    r = ioctx1.write_full(oid, one);
    ASSERT_EQ(0, r);
  }

  // make sure 2 can read it
  {
    cout << "client2: reading 'one'" << std::endl;
    bufferlist bl;
    r = ioctx2.read(oid, bl, 3, 0);
    ASSERT_EQ(3, r);
    ASSERT_EQ('o', bl[0]);
    ASSERT_EQ('n', bl[1]);
    ASSERT_EQ('e', bl[2]);
  }

  // find the primary
  int primary;
  r = get_primary_osd(rados1, pool_name, oid, &primary);
  ASSERT_EQ(0, r);

  // fence it
  cout << "client1: fencing primary" << std::endl;
  fence_osd(rados1, primary);
  mark_down_osd(rados1, primary);
  rados1.wait_for_latest_osdmap();

  // should still be able to read the old value on 2
  {
    cout << "client2: reading 'one' again from old primary" << std::endl;
    bufferlist bl;
    r = ioctx2.read(oid, bl, 3, 0);
    ASSERT_EQ(3, r);
    ASSERT_EQ('o', bl[0]);
    ASSERT_EQ('n', bl[1]);
    ASSERT_EQ('e', bl[2]);
  }

  // update object on 1
  bufferlist two;
  two.append("two");
  {
    cout << "client1: writing 'two' to new acting set" << std::endl;
    r = ioctx1.write_full(oid, two);
    ASSERT_EQ(0, r);
  }

  // make sure we can't still read the old value on 2
  {
    cout << "client2: reading again from old primary" << std::endl;
    bufferlist bl;
    r = ioctx2.read(oid, bl, 3, 0);
    ASSERT_EQ(3, r);
    ASSERT_EQ('t', bl[0]);
    ASSERT_EQ('w', bl[1]);
    ASSERT_EQ('o', bl[2]);
  }

  rados1.shutdown();
  rados2.shutdown();
}

int main(int argc, char **argv) {
  vector<const char*> args;
  argv_to_vec(argc, (const char **)argv, args);
  auto cct = global_init(NULL, args, CEPH_ENTITY_TYPE_CLIENT,
			 CODE_ENVIRONMENT_UTILITY, 0);
  common_init_finish(g_ceph_context);
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
