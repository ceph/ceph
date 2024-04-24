#include "include/rados.h"
#include "json_spirit/json_spirit.h"
#include "test/librados/test.h"
#include "test/librados/TestCase.h"

#include <algorithm>
#include <errno.h>
#include "gtest/gtest.h"
#include <string>
#include <vector>

using std::string;

class LibRadosSnapshotStatsSelfManaged : public RadosTest {
public:
  LibRadosSnapshotStatsSelfManaged() {};
  ~LibRadosSnapshotStatsSelfManaged() override {};
protected:
  void SetUp() override {
    // disable pg autoscaler for the tests
    string c =
      "{"
        "\"prefix\": \"config set\", "
        "\"who\": \"global\", "
        "\"name\": \"osd_pool_default_pg_autoscale_mode\", "
        "\"value\": \"off\""
      "}";
    char *cmd[1];
    cmd[0] = (char *)c.c_str();
    std::cout << "Setting pg_autoscaler to 'off'" << std::endl;
    ASSERT_EQ(0, rados_mon_command(s_cluster, (const char **)cmd, 1, "", 0, NULL,
      0, NULL, 0));

    // disable scrubs for the test
    c = string("{\"prefix\": \"osd set\",\"key\":\"noscrub\"}");
    cmd[0] = (char *)c.c_str();
    ASSERT_EQ(0, rados_mon_command(s_cluster, (const char **)cmd, 1, "", 0, NULL, 0, NULL, 0));
    c = string("{\"prefix\": \"osd set\",\"key\":\"nodeep-scrub\"}");
    cmd[0] = (char *)c.c_str();
    ASSERT_EQ(0, rados_mon_command(s_cluster, (const char **)cmd, 1, "", 0, NULL, 0, NULL, 0));

    RadosTest::SetUp();
  }

  void TearDown() override {
    // re-enable pg autoscaler
    string c =
      "{"
        "\"prefix\": \"config set\", "
        "\"who\": \"global\", "
        "\"name\": \"osd_pool_default_pg_autoscale_mode\", "
        "\"value\": \"on\""
      "}";
    char *cmd[1];
    cmd[0] = (char *)c.c_str();
    std::cout << "Setting pg_autoscaler to 'on'" << std::endl;
    ASSERT_EQ(0, rados_mon_command(s_cluster, (const char **)cmd, 1, "", 0, NULL,
      0, NULL, 0));

    // re-enable scrubs
    c = string("{\"prefix\": \"osd unset\",\"key\":\"noscrub\"}");
    cmd[0] = (char *)c.c_str();
    ASSERT_EQ(0, rados_mon_command(s_cluster, (const char **)cmd, 1, "", 0, NULL, 0, NULL, 0));
    c = string("{\"prefix\": \"osd unset\",\"key\":\"nodeep-scrub\"}");
    cmd[0] = (char *)c.c_str();
    ASSERT_EQ(0, rados_mon_command(s_cluster, (const char **)cmd, 1, "", 0, NULL, 0, NULL, 0));

    RadosTest::TearDown();
  }
};

class LibRadosSnapshotStatsSelfManagedEC : public RadosTestEC {
public:
  LibRadosSnapshotStatsSelfManagedEC() {};
  ~LibRadosSnapshotStatsSelfManagedEC() override {};
protected:
  void SetUp() override {
    // disable pg autoscaler for the tests
    string c =
      "{"
        "\"prefix\": \"config set\", "
        "\"who\": \"global\", "
        "\"name\": \"osd_pool_default_pg_autoscale_mode\", "
        "\"value\": \"off\""
      "}";
    char *cmd[1];
    cmd[0] = (char *)c.c_str();
    std::cout << "Setting pg_autoscaler to 'off'" << std::endl;
    ASSERT_EQ(0, rados_mon_command(s_cluster, (const char **)cmd, 1, "", 0, NULL,
      0, NULL, 0));

    // disable scrubs for the test
    c = string("{\"prefix\": \"osd set\",\"key\":\"noscrub\"}");
    cmd[0] = (char *)c.c_str();
    ASSERT_EQ(0, rados_mon_command(s_cluster, (const char **)cmd, 1, "", 0, NULL, 0, NULL, 0));
    c = string("{\"prefix\": \"osd set\",\"key\":\"nodeep-scrub\"}");
    cmd[0] = (char *)c.c_str();
    ASSERT_EQ(0, rados_mon_command(s_cluster, (const char **)cmd, 1, "", 0, NULL, 0, NULL, 0));

    RadosTestEC::SetUp();
  }

  void TearDown() override {
    // re-enable pg autoscaler
    string c =
      "{"
        "\"prefix\": \"config set\", "
        "\"who\": \"global\", "
        "\"name\": \"osd_pool_default_pg_autoscale_mode\", "
        "\"value\": \"on\""
      "}";
    char *cmd[1];
    cmd[0] = (char *)c.c_str();
    std::cout << "Setting pg_autoscaler to 'on'" << std::endl;
    ASSERT_EQ(0, rados_mon_command(s_cluster, (const char **)cmd, 1, "", 0, NULL,
      0, NULL, 0));

    // re-enable scrubs
    c = string("{\"prefix\": \"osd unset\",\"key\":\"noscrub\"}");
    cmd[0] = (char *)c.c_str();
    ASSERT_EQ(0, rados_mon_command(s_cluster, (const char **)cmd, 1, "", 0, NULL, 0, NULL, 0));
    c = string("{\"prefix\": \"osd unset\",\"key\":\"nodeep-scrub\"}");
    cmd[0] = (char *)c.c_str();
    ASSERT_EQ(0, rados_mon_command(s_cluster, (const char **)cmd, 1, "", 0, NULL, 0, NULL, 0));

    RadosTestEC::TearDown();
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

TEST_F(LibRadosSnapshotStatsSelfManaged, SnaptrimStats) {
  int num_objs = 10;

  // create objects
  char buf[bufsize];
  memset(buf, 0xcc, sizeof(buf));
  for (int i = 0; i < num_objs; ++i) {
    string obj = string("foo") + std::to_string(i);
    ASSERT_EQ(0, rados_write(ioctx, obj.c_str(), buf, sizeof(buf), 0));
  }

  std::vector<uint64_t> my_snaps;
  for (int snap = 0; snap < 1; ++snap) {
    // create a snapshot, clone
    std::vector<uint64_t> ns(1);
    ns.insert(ns.end(), my_snaps.begin(), my_snaps.end());
    my_snaps.swap(ns);
    ASSERT_EQ(0, rados_ioctx_selfmanaged_snap_create(ioctx, &my_snaps[0]));
    ASSERT_EQ(0, rados_ioctx_selfmanaged_snap_set_write_ctx(ioctx, my_snaps[0],
                                        &my_snaps[0], my_snaps.size()));
    char buf2[sizeof(buf)];
    memset(buf2, 0xdd, sizeof(buf2));
    for (int i = 0; i < num_objs; ++i) {
      string obj = string("foo") + std::to_string(i);
      ASSERT_EQ(0, rados_write(ioctx, obj.c_str(), buf2, sizeof(buf2), 0));
    }
  }

  // wait for maps to settle
  ASSERT_EQ(0, rados_wait_for_latest_osdmap(cluster));

  // remove snaps - should trigger snaptrim
  rados_ioctx_snap_set_read(ioctx, LIBRADOS_SNAP_HEAD);
  for (unsigned snap = 0; snap < my_snaps.size(); ++snap) {
    ASSERT_EQ(0, rados_ioctx_selfmanaged_snap_remove(ioctx, my_snaps[snap]));
  }

  // sleep for few secs for the trim stats to populate
  std::cout << "Waiting for snaptrim stats to be generated" << std::endl;
  sleep(30);

  // Dump pg stats and determine if snaptrim stats are getting set
  int objects_trimmed = 0;
  double snaptrim_duration = 0.0;
  int tries = 0;
  do {
    char *buf, *st;
    size_t buflen, stlen;
    string c = string("{\"prefix\": \"pg dump\",\"format\":\"json\"}");
    const char *cmd = c.c_str();
    ASSERT_EQ(0, rados_mon_command(cluster, (const char **)&cmd, 1, "", 0,
      &buf, &buflen, &st, &stlen));
    string outstr(buf, buflen);
    json_spirit::Value v;
    ASSERT_NE(0, json_spirit::read(outstr, v)) << "unable to parse json."
      << '\n' << outstr;

    // pg dump object
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
  for (int i = 0; i < num_objs; ++i) {
    string obj = string("foo") + std::to_string(i);
    ASSERT_EQ(0, rados_remove(ioctx, obj.c_str()));
  }
}

// EC testing
TEST_F(LibRadosSnapshotStatsSelfManagedEC, SnaptrimStats) {
  int num_objs = 10;
  int bsize = alignment;
  char *buf = (char *)new char[bsize];
  memset(buf, 0xcc, bsize);
  // create objects
  for (int i = 0; i < num_objs; ++i) {
    string obj = string("foo") + std::to_string(i);
    ASSERT_EQ(0, rados_write(ioctx, obj.c_str(), buf, bsize, 0));
  }

  std::vector<uint64_t> my_snaps;
  for (int snap = 0; snap < 1; ++snap) {
    // create a snapshot, clone
    std::vector<uint64_t> ns(1);
    ns.insert(ns.end(), my_snaps.begin(), my_snaps.end());
    my_snaps.swap(ns);
    ASSERT_EQ(0, rados_ioctx_selfmanaged_snap_create(ioctx, &my_snaps[0]));
    ASSERT_EQ(0, rados_ioctx_selfmanaged_snap_set_write_ctx(ioctx, my_snaps[0],
                                        &my_snaps[0], my_snaps.size()));
    char *buf2 = (char *)new char[bsize];
    memset(buf2, 0xdd, bsize);
    for (int i = 0; i < num_objs; ++i) {
      string obj = string("foo") + std::to_string(i);
      ASSERT_EQ(0, rados_write(ioctx, obj.c_str(), buf2, bsize, bsize));
    }
    delete[] buf2;
  }

  // wait for maps to settle
  ASSERT_EQ(0, rados_wait_for_latest_osdmap(cluster));

  // remove snaps - should trigger snaptrim
  rados_ioctx_snap_set_read(ioctx, LIBRADOS_SNAP_HEAD);
  for (unsigned snap = 0; snap < my_snaps.size(); ++snap) {
    ASSERT_EQ(0, rados_ioctx_selfmanaged_snap_remove(ioctx, my_snaps[snap]));
  }

  // sleep for few secs for the trim stats to populate
  std::cout << "Waiting for snaptrim stats to be generated" << std::endl;
  sleep(30);

  // Dump pg stats and determine if snaptrim stats are getting set
  int objects_trimmed = 0;
  double snaptrim_duration = 0.0;
  int tries = 0;
  do {
    char *buf, *st;
    size_t buflen, stlen;
    string c = string("{\"prefix\": \"pg dump\",\"format\":\"json\"}");
    const char *cmd = c.c_str();
    ASSERT_EQ(0, rados_mon_command(cluster, (const char **)&cmd, 1, 0, 0,
      &buf, &buflen, &st, &stlen));
    string outstr(buf, buflen);
    json_spirit::Value v;
    ASSERT_NE(0, json_spirit::read(outstr, v)) << "Unable tp parse json."
      << '\n' << outstr;

    // pg dump object
    json_spirit::Object& obj = v.get_obj();
    get_snaptrim_stats(obj, &objects_trimmed, &snaptrim_duration);
    if (objects_trimmed != num_objs) {
      tries++;
      objects_trimmed = 0;
      std::cout << "Still waiting for all objects to be trimmed... " <<std::endl;
      sleep(30);
    }
  } while (objects_trimmed != num_objs && tries < 5);

  // final check for objects trimmed
  ASSERT_EQ(objects_trimmed, num_objs);
  std::cout << "Snaptrim duration: " << snaptrim_duration << std::endl;
  ASSERT_GT(snaptrim_duration, 0.0);

  // clean-up remaining objects
  for (int i = 0; i < num_objs; ++i) {
    string obj = string("foo") + std::to_string(i);
    ASSERT_EQ(0, rados_remove(ioctx, obj.c_str()));
  }

  delete[] buf;
}
