// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*
// vim: ts=8 sw=2 smarttab

#include "include/rados/librados.h"
#include "include/rados/librados.hpp"
#include "test/librados/test.h"

#include <sstream>
#include <stdlib.h>
#include <string>
#include <time.h>
#include <unistd.h>
#include "gtest/gtest.h"

using namespace librados;

std::string get_temp_pool_name()
{
  char hostname[80];
  char out[80];
  memset(hostname, 0, sizeof(hostname));
  memset(out, 0, sizeof(out));
  gethostname(hostname, sizeof(hostname)-1);
  static int num = 1;
  sprintf(out, "%s-%d-%d", hostname, getpid(), num);
  num++;
  std::string prefix("test-rados-api-");
  prefix += out;
  return prefix;
}

std::string create_one_pool(const std::string &pool_name, rados_t *cluster)
{
  std::string err = connect_cluster(cluster);
  if (err.length())
    return err;
  int ret = rados_pool_create(*cluster, pool_name.c_str());
  if (ret) {
    rados_shutdown(*cluster);
    std::ostringstream oss;
    oss << "rados_pool_create(" << pool_name << ") failed with error " << ret;
    return oss.str();
  }
  return "";
}

int destroy_ec_profile(rados_t *cluster)
{
    char *cmd[2];
    cmd[0] = (char *)"{\"prefix\": \"osd erasure-code-profile rm\", \"name\": \"testprofile\"}";
    cmd[1] = NULL;
    return rados_mon_command(*cluster, (const char **)cmd, 1, "", 0, NULL, 0, NULL, 0);
}

std::string create_one_ec_pool(const std::string &pool_name, rados_t *cluster)
{
  std::string err = connect_cluster(cluster);
  if (err.length())
    return err;

  int ret = destroy_ec_profile(cluster);
  if (ret) {
    rados_shutdown(*cluster);
    std::ostringstream oss;
    oss << "rados_mon_command erasure-code-profile rm testprofile failed with error " << ret;
    return oss.str();
  }
    
  char *cmd[2];
  cmd[1] = NULL;

  std::string profile_create = "{\"prefix\": \"osd erasure-code-profile set\", \"name\": \"testprofile\", \"profile\": [ \"k=2\", \"m=1\", \"ruleset-failure-domain=osd\"]}";
  cmd[0] = (char *)profile_create.c_str();
  ret = rados_mon_command(*cluster, (const char **)cmd, 1, "", 0, NULL, 0, NULL, 0);
  if (ret) {
    std::ostringstream oss;

    rados_shutdown(*cluster);
    oss << "rados_mon_command erasure-code-profile set name:testprofile failed with error " << ret;
    return oss.str();
  }

  std::string cmdstr = "{\"prefix\": \"osd pool create\", \"pool\": \"" +
     pool_name + "\", \"pool_type\":\"erasure\", \"pg_num\":8, \"pgp_num\":8, \"erasure_code_profile\":\"testprofile\"}";
  cmd[0] = (char *)cmdstr.c_str();
  ret = rados_mon_command(*cluster, (const char **)cmd, 1, "", 0, NULL, 0, NULL, 0);
  if (ret) {
    std::ostringstream oss;

    int ret2 = destroy_ec_profile(cluster);
    if (ret2)
      oss << "rados_mon_command osd erasure-code-profile rm name:testprofile failed with error " << ret2 << std::endl;

    rados_shutdown(*cluster);
    oss << "rados_mon_command osd pool create failed with error " << ret;
    return oss.str();
  }

  rados_wait_for_latest_osdmap(*cluster);
  return "";
}

std::string create_one_pool_pp(const std::string &pool_name, Rados &cluster)
{
  std::string err = connect_cluster_pp(cluster);
  if (err.length())
    return err;
  int ret = cluster.pool_create(pool_name.c_str());
  if (ret) {
    cluster.shutdown();
    std::ostringstream oss;
    oss << "cluster.pool_create(" << pool_name << ") failed with error " << ret;
    return oss.str();
  }
  return "";
}

int destroy_ec_profile_pp(Rados &cluster)
{
  bufferlist inbl;
  return cluster.mon_command("{\"prefix\": \"osd erasure-code-profile rm\", \"name\": \"testprofile\"}",
                             inbl, NULL, NULL);
}

std::string create_one_ec_pool_pp(const std::string &pool_name, Rados &cluster)
{
  std::string err = connect_cluster_pp(cluster);
  if (err.length())
    return err;

  int ret = destroy_ec_profile_pp(cluster);
  if (ret) {
    cluster.shutdown();
    std::ostringstream oss;
    oss << "rados_mon_command erasure-code-profile rm testprofile failed with error " << ret;
    return oss.str();
  }

  bufferlist inbl;
  ret = cluster.mon_command(
    "{\"prefix\": \"osd erasure-code-profile set\", \"name\": \"testprofile\", \"profile\": [ \"k=2\", \"m=1\", \"ruleset-failure-domain=osd\"]}",
    inbl, NULL, NULL);
  if (ret) {
    cluster.shutdown();
    std::ostringstream oss;
    oss << "mon_command erasure-code-profile set name:testprofile failed with error " << ret;
    return oss.str();
  }
    
  ret = cluster.mon_command(
    "{\"prefix\": \"osd pool create\", \"pool\": \"" + pool_name + "\", \"pool_type\":\"erasure\", \"pg_num\":8, \"pgp_num\":8, \"erasure_code_profile\":\"testprofile\"}",
    inbl, NULL, NULL);
  if (ret) {
    std::ostringstream oss;
    bufferlist inbl;
    int ret2 = destroy_ec_profile_pp(cluster);
    if (ret2)
      oss << "mon_command osd erasure-code-profile rm name:testprofile failed with error " << ret2 << std::endl;

    cluster.shutdown();
    oss << "mon_command osd pool create pool:" << pool_name << " pool_type:erasure failed with error " << ret;
    return oss.str();
  }

  cluster.wait_for_latest_osdmap();
  return "";
}

std::string connect_cluster(rados_t *cluster)
{
  char *id = getenv("CEPH_CLIENT_ID");
  if (id) std::cerr << "Client id is: " << id << std::endl;

  int ret;
  ret = rados_create(cluster, NULL);
  if (ret) {
    std::ostringstream oss;
    oss << "rados_create failed with error " << ret;
    return oss.str();
  }
  ret = rados_conf_read_file(*cluster, NULL);
  if (ret) {
    rados_shutdown(*cluster);
    std::ostringstream oss;
    oss << "rados_conf_read_file failed with error " << ret;
    return oss.str();
  }
  rados_conf_parse_env(*cluster, NULL);
  ret = rados_connect(*cluster);
  if (ret) {
    rados_shutdown(*cluster);
    std::ostringstream oss;
    oss << "rados_connect failed with error " << ret;
    return oss.str();
  }
  return "";
}

std::string connect_cluster_pp(Rados &cluster)
{
  char *id = getenv("CEPH_CLIENT_ID");
  if (id) std::cerr << "Client id is: " << id << std::endl;

  int ret;
  ret = cluster.init(id);
  if (ret) {
    std::ostringstream oss;
    oss << "cluster.init failed with error " << ret;
    return oss.str();
  }
  ret = cluster.conf_read_file(NULL);
  if (ret) {
    cluster.shutdown();
    std::ostringstream oss;
    oss << "cluster.conf_read_file failed with error " << ret;
    return oss.str();
  }
  cluster.conf_parse_env(NULL);
  ret = cluster.connect();
  if (ret) {
    cluster.shutdown();
    std::ostringstream oss;
    oss << "cluster.connect failed with error " << ret;
    return oss.str();
  }
  return "";
}

int destroy_one_pool(const std::string &pool_name, rados_t *cluster)
{
  int ret = rados_pool_delete(*cluster, pool_name.c_str());
  if (ret) {
    rados_shutdown(*cluster);
    return ret;
  }
  rados_shutdown(*cluster);
  return 0;
}

int destroy_one_ec_pool(const std::string &pool_name, rados_t *cluster)
{
  int ret = rados_pool_delete(*cluster, pool_name.c_str());
  if (ret == 0) {
    int ret2 = destroy_ec_profile(cluster);
    if (ret2) {
      rados_shutdown(*cluster);
      return ret2;
    }
    rados_wait_for_latest_osdmap(*cluster);
  }
  rados_shutdown(*cluster);
  return ret;
}

int destroy_one_pool_pp(const std::string &pool_name, Rados &cluster)
{
  int ret = cluster.pool_delete(pool_name.c_str());
  if (ret) {
    cluster.shutdown();
    return ret;
  }
  cluster.shutdown();
  return 0;
}

int destroy_one_ec_pool_pp(const std::string &pool_name, Rados &cluster)
{
  int ret = cluster.pool_delete(pool_name.c_str());
  bufferlist inbl;
  if (ret == 0) {
    int ret2 = destroy_ec_profile_pp(cluster);
    if (ret2) {
      cluster.shutdown();
      return ret2;
    }
    cluster.wait_for_latest_osdmap();
  }
  cluster.shutdown();
  return ret;
}

void assert_eq_sparse(bufferlist& expected,
                      const std::map<uint64_t, uint64_t>& extents,
                      bufferlist& actual) {
  bufferlist::iterator i = expected.begin();
  bufferlist::iterator p = actual.begin();
  uint64_t pos = 0;
  for (std::map<uint64_t, uint64_t>::const_iterator extent = extents.begin();
       extent != extents.end();
       ++extent) {
    const uint64_t start = extent->first;
    const uint64_t end = start + extent->second;
    for (; pos < end; ++i, ++pos) {
      ASSERT_FALSE(i.end());
      if (pos < start) {
        // check the hole
        ASSERT_EQ('\0', *i);
      } else {
        // then the extent
        ASSERT_EQ(*i, *p);
        ++p;
      }
    }
  }
  ASSERT_EQ(expected.length(), pos);
}
