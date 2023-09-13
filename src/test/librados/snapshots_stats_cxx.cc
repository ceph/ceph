#include <algorithm>
#include <errno.h>
#include <string>
#include <vector>

#include "gtest/gtest.h"

#include "include/rados.h"
#include "include/rados/librados.hpp"
#include "json_spirit/json_spirit.h"
#include "test/librados/test_cxx.h"
#include "test/librados/testcase_cxx.h"

using namespace librados;

using std::string;

class LibRadosSnapshotStatsSelfManagedPP : public RadosTestPP {
public:
  LibRadosSnapshotStatsSelfManagedPP() {};
  ~LibRadosSnapshotStatsSelfManagedPP() override {};
protected:
  void SetUp() override {
    // disable pg autoscaler for the tests
    string cmd =
      "{"
        "\"prefix\": \"config set\", "
        "\"who\": \"global\", "
        "\"name\": \"osd_pool_default_pg_autoscale_mode\", "
        "\"value\": \"off\""
      "}";
    std::cout << "Setting pg_autoscaler to 'off'" << std::endl;
    bufferlist inbl;
    bufferlist outbl;
    ASSERT_EQ(0, s_cluster.mon_command(cmd, inbl, &outbl, NULL));

    // disable scrubs for the test
    cmd = "{\"prefix\": \"osd set\",\"key\":\"noscrub\"}";
    ASSERT_EQ(0, s_cluster.mon_command(cmd, inbl, &outbl, NULL));
    cmd = "{\"prefix\": \"osd set\",\"key\":\"nodeep-scrub\"}";
    ASSERT_EQ(0, s_cluster.mon_command(cmd, inbl, &outbl, NULL));

    RadosTestPP::SetUp();
  }

  void TearDown() override {
    // re-enable pg autoscaler
    string cmd =
      "{"
        "\"prefix\": \"config set\", "
        "\"who\": \"global\", "
        "\"name\": \"osd_pool_default_pg_autoscale_mode\", "
        "\"value\": \"on\""
      "}";
    std::cout << "Setting pg_autoscaler to 'on'" << std::endl;
    bufferlist inbl;
    bufferlist outbl;
    ASSERT_EQ(0, s_cluster.mon_command(cmd, inbl, &outbl, NULL));

    // re-enable scrubs
    cmd = "{\"prefix\": \"osd unset\",\"key\":\"noscrub\"}";
    ASSERT_EQ(0, s_cluster.mon_command(cmd, inbl, &outbl, NULL));
    cmd = string("{\"prefix\": \"osd unset\",\"key\":\"nodeep-scrub\"}");
    ASSERT_EQ(0, s_cluster.mon_command(cmd, inbl, &outbl, NULL));

    RadosTestPP::TearDown();
  }
};

class LibRadosSnapshotStatsSelfManagedECPP : public RadosTestECPP {
public:
  LibRadosSnapshotStatsSelfManagedECPP() {};
  ~LibRadosSnapshotStatsSelfManagedECPP() override {};
protected:
  void SetUp() override {
    // disable pg autoscaler for the tests
    string cmd =
      "{"
        "\"prefix\": \"config set\", "
        "\"who\": \"global\", "
        "\"name\": \"osd_pool_default_pg_autoscale_mode\", "
        "\"value\": \"off\""
      "}";
    std::cout << "Setting pg_autoscaler to 'off'" << std::endl;
    bufferlist inbl;
    bufferlist outbl;
    ASSERT_EQ(0, s_cluster.mon_command(cmd, inbl, &outbl, NULL));

    // disable scrubs for the test
    cmd = string("{\"prefix\": \"osd set\",\"key\":\"noscrub\"}");
    ASSERT_EQ(0, s_cluster.mon_command(cmd, inbl, &outbl, NULL));
    cmd = string("{\"prefix\": \"osd set\",\"key\":\"nodeep-scrub\"}");
    ASSERT_EQ(0, s_cluster.mon_command(cmd, inbl, &outbl, NULL));

    RadosTestECPP::SetUp();
  }

  void TearDown() override {
    // re-enable pg autoscaler
    string cmd =
      "{"
        "\"prefix\": \"config set\", "
        "\"who\": \"global\", "
        "\"name\": \"osd_pool_default_pg_autoscale_mode\", "
        "\"value\": \"on\""
      "}";
    std::cout << "Setting pg_autoscaler to 'on'" << std::endl;
    bufferlist inbl;
    bufferlist outbl;
    ASSERT_EQ(0, s_cluster.mon_command(cmd, inbl, &outbl, NULL));

    // re-enable scrubs
    cmd = string("{\"prefix\": \"osd unset\",\"key\":\"noscrub\"}");
    ASSERT_EQ(0, s_cluster.mon_command(cmd, inbl, &outbl, NULL));
    cmd = string("{\"prefix\": \"osd unset\",\"key\":\"nodeep-scrub\"}");
    ASSERT_EQ(0, s_cluster.mon_command(cmd, inbl, &outbl, NULL));

    RadosTestECPP::TearDown();
  }
};

void get_snaptrim_stats(json_spirit::Object& pg_dump,
                        int *objs_trimmed,
                        double *trim_duration) {
  // pg_map
  json_spirit::Object pgmap;
  for (json_spirit::Object::size_type i = 0; i < pg_dump.size(); ++i) {
    json_spirit::Pair& p = pg_dump[i];
    if (p.name_ == "pg_map") {
      pgmap = p.value_.get_obj();
      break;
    }
  }

  // pg_stats array
  json_spirit::Array pgs;
  for (json_spirit::Object::size_type i = 0; i < pgmap.size(); ++i) {
    json_spirit::Pair& p = pgmap[i];
    if (p.name_ == "pg_stats") {
      pgs = p.value_.get_array();
      break;
    }
  }

  // snaptrim stats
  for (json_spirit::Object::size_type j = 0; j < pgs.size(); ++j) {
    json_spirit::Object& pg_stat = pgs[j].get_obj();
    for(json_spirit::Object::size_type k = 0; k < pg_stat.size(); ++k) {
      json_spirit::Pair& stats = pg_stat[k];
      if (stats.name_ == "objects_trimmed") {
        *objs_trimmed += stats.value_.get_int();
      }
      if (stats.name_ == "snaptrim_duration") {
        *trim_duration += stats.value_.get_real();
      }
    }
  }
}
const int bufsize = 128;

TEST_F(LibRadosSnapshotStatsSelfManagedPP, SnaptrimStatsPP) {
  int num_objs = 10;

  // create objects
  char buf[bufsize];
  memset(buf, 0xcc, sizeof(buf));
  bufferlist bl;
  bl.append(buf, sizeof(buf));
  for (int i = 0; i < num_objs; ++i) {
   string obj = string("foo") + std::to_string(i);
    ASSERT_EQ(0, ioctx.write(obj, bl, sizeof(buf), 0));
  }

  std::vector<uint64_t> my_snaps;
  char buf2[sizeof(buf)];
  memset(buf2, 0xdd, sizeof(buf2));
  bufferlist bl2;
  bl2.append(buf2, sizeof(buf2));
  for (int snap = 0; snap < 1; ++snap) {
    // create a snapshot, clone
    std::vector<uint64_t> ns(1);
    ns.insert(ns.end(), my_snaps.begin(), my_snaps.end());
    my_snaps.swap(ns);
    ASSERT_EQ(0, ioctx.selfmanaged_snap_create(&my_snaps[0]));
    ASSERT_EQ(0, ioctx.selfmanaged_snap_set_write_ctx(my_snaps[0], my_snaps));
    for (int i = 0; i < num_objs; ++i) {
      string obj = string("foo") + std::to_string(i);
      ASSERT_EQ(0, ioctx.write(obj, bl2, sizeof(buf2), 0));
    }
  }

  // wait for maps to settle
  cluster.wait_for_latest_osdmap();

  // remove snaps - should trigger snaptrim
  for (unsigned snap = 0; snap < my_snaps.size(); ++snap) {
    ioctx.selfmanaged_snap_remove(my_snaps[snap]);
  }

  // sleep for few secs for the trim stats to populate
  std::cout << "Waiting for snaptrim stats to be generated" << std::endl;
  sleep(30);

  // Dump pg stats and determine if snaptrim stats are getting set
  int objects_trimmed = 0;
  double snaptrim_duration = 0.0;
  int tries = 0;
  do {
    string cmd = string("{\"prefix\": \"pg dump\",\"format\":\"json\"}");
    bufferlist inbl;
    bufferlist outbl;
    ASSERT_EQ(0, cluster.mon_command(cmd, inbl, &outbl, NULL));
    string outstr(outbl.c_str(), outbl.length());
    json_spirit::Value v;
    ASSERT_NE(0, json_spirit::read(outstr, v)) << "unable to parse json." << '\n' << outstr;

    // pg_map
    json_spirit::Object& obj = v.get_obj();
    get_snaptrim_stats(obj, &objects_trimmed, &snaptrim_duration);
    if (objects_trimmed < num_objs) {
      tries++;
      objects_trimmed = 0;
      std::cout << "Still waiting for all objects to be trimmed... " <<std::endl;
      sleep(30);
    }
  } while(objects_trimmed < num_objs && tries < 5);

  // final check for objects trimmed
  ASSERT_EQ(objects_trimmed, num_objs);
  std::cout << "Snaptrim duration: " << snaptrim_duration << std::endl;
  ASSERT_GT(snaptrim_duration, 0.0);

  // clean-up remaining objects
  ioctx.snap_set_read(librados::SNAP_HEAD);
  for (int i = 0; i < num_objs; ++i) {
    string obj = string("foo") + std::to_string(i);
    ASSERT_EQ(0, ioctx.remove(obj));
  }
}

// EC testing
TEST_F(LibRadosSnapshotStatsSelfManagedECPP, SnaptrimStatsECPP) {
  int num_objs = 10;
  int bsize = alignment;

  // create objects
  char *buf = (char *)new char[bsize];
  memset(buf, 0xcc, bsize);
  bufferlist bl;
  bl.append(buf, bsize);
  for (int i = 0; i < num_objs; ++i) {
   string obj = string("foo") + std::to_string(i);
    ASSERT_EQ(0, ioctx.write(obj, bl, bsize, 0));
  }

  std::vector<uint64_t> my_snaps;
  char *buf2 = (char *)new char[bsize];
  memset(buf2, 0xdd, bsize);
  bufferlist bl2;
  bl2.append(buf2, bsize);
  for (int snap = 0; snap < 1; ++snap) {
    // create a snapshot, clone
    std::vector<uint64_t> ns(1);
    ns.insert(ns.end(), my_snaps.begin(), my_snaps.end());
    my_snaps.swap(ns);
    ASSERT_EQ(0, ioctx.selfmanaged_snap_create(&my_snaps[0]));
    ASSERT_EQ(0, ioctx.selfmanaged_snap_set_write_ctx(my_snaps[0], my_snaps));
    for (int i = 0; i < num_objs; ++i) {
      string obj = string("foo") + std::to_string(i);
      ASSERT_EQ(0, ioctx.write(obj, bl2, bsize, bsize));
    }
  }

  // wait for maps to settle
  cluster.wait_for_latest_osdmap();

  // remove snaps - should trigger snaptrim
  for (unsigned snap = 0; snap < my_snaps.size(); ++snap) {
    ioctx.selfmanaged_snap_remove(my_snaps[snap]);
  }

  // sleep for few secs for the trim stats to populate
  std::cout << "Waiting for snaptrim stats to be generated" << std::endl;
  sleep(30);

  // Dump pg stats and determine if snaptrim stats are getting set
  int objects_trimmed = 0;
  double snaptrim_duration = 0.0;
  int tries = 0;
  do {
    string cmd = string("{\"prefix\": \"pg dump\",\"format\":\"json\"}");
    bufferlist inbl;
    bufferlist outbl;
    ASSERT_EQ(0, cluster.mon_command(cmd, inbl, &outbl, NULL));
    string outstr(outbl.c_str(), outbl.length());
    json_spirit::Value v;
    ASSERT_NE(0, json_spirit::read(outstr, v)) << "unable to parse json." << '\n' << outstr;

    // pg_map
    json_spirit::Object& obj = v.get_obj();
    get_snaptrim_stats(obj, &objects_trimmed, &snaptrim_duration);
    if (objects_trimmed < num_objs) {
      tries++;
      objects_trimmed = 0;
      std::cout << "Still waiting for all objects to be trimmed... " <<std::endl;
      sleep(30);
    }
  } while(objects_trimmed < num_objs && tries < 5);

  // final check for objects trimmed
  ASSERT_EQ(objects_trimmed, num_objs);
  std::cout << "Snaptrim duration: " << snaptrim_duration << std::endl;
  ASSERT_GT(snaptrim_duration, 0.0);

  // clean-up remaining objects
  ioctx.snap_set_read(LIBRADOS_SNAP_HEAD);
  for (int i = 0; i < num_objs; ++i) {
    string obj = string("foo") + std::to_string(i);
    ASSERT_EQ(0, ioctx.remove(obj));
  }

  delete[] buf;
  delete[] buf2;
}
