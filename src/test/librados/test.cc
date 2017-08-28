// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*
// vim: ts=8 sw=2 smarttab

#include "include/rados/librados.h"
#include "include/rados/librados.hpp"
#include "test/librados/test.h"

#include "include/stringify.h"

#include <sstream>
#include <stdlib.h>
#include <string>
#include <time.h>
#include <unistd.h>
#include <iostream>
#include "gtest/gtest.h"

using namespace librados;

std::string get_temp_pool_name(const std::string &prefix)
{
  char hostname[80];
  char out[80];
  memset(hostname, 0, sizeof(hostname));
  memset(out, 0, sizeof(out));
  gethostname(hostname, sizeof(hostname)-1);
  static int num = 1;
  sprintf(out, "%s-%d-%d", hostname, getpid(), num);
  num++;
  return prefix + out;
}

std::string create_one_pool(
    const std::string &pool_name, rados_t *cluster, uint32_t pg_num)
{
  std::string err_str = connect_cluster(cluster);
  if (err_str.length())
    return err_str;

  int ret = rados_pool_create(*cluster, pool_name.c_str());
  if (ret) {
    rados_shutdown(*cluster);
    std::ostringstream oss;
    oss << "create_one_pool(" << pool_name << ") failed with error " << ret;
    return oss.str();
  }

  return "";
}

int destroy_ec_profile(rados_t *cluster,
		       const std::string& pool_name,
		       std::ostream &oss)
{
  char buf[1000];
  snprintf(buf, sizeof(buf),
	   "{\"prefix\": \"osd erasure-code-profile rm\", \"name\": \"testprofile-%s\"}",
	   pool_name.c_str());
  char *cmd[2];
  cmd[0] = buf;
  cmd[1] = NULL;
  int ret = rados_mon_command(*cluster, (const char **)cmd, 1, "", 0, NULL,
			      0, NULL, 0);
  if (ret)
    oss << "rados_mon_command: erasure-code-profile rm testprofile-"
	<< pool_name << " failed with error " << ret;
  return ret;
}

int destroy_ruleset(rados_t *cluster,
                    std::string ruleset,
                    std::ostream &oss)
{
  char *cmd[2];
  std::string tmp = ("{\"prefix\": \"osd crush rule rm\", \"name\":\"" +
                     ruleset + "\"}");
  cmd[0] = (char*)tmp.c_str();
  cmd[1] = NULL;
  int ret = rados_mon_command(*cluster, (const char **)cmd, 1, "", 0, NULL, 0, NULL, 0);
  if (ret)
    oss << "rados_mon_command: osd crush rule rm " + ruleset + " failed with error " << ret;
  return ret;
}

int destroy_ec_profile_and_ruleset(rados_t *cluster,
                                   std::string ruleset,
                                   std::ostream &oss)
{
  int ret;
  ret = destroy_ec_profile(cluster, ruleset, oss);
  if (ret)
    return ret;
  return destroy_ruleset(cluster, ruleset, oss);
}

std::string create_one_ec_pool(const std::string &pool_name, rados_t *cluster)
{
  std::string err = connect_cluster(cluster);
  if (err.length())
    return err;

  std::ostringstream oss;
  int ret = destroy_ec_profile_and_ruleset(cluster, pool_name, oss);
  if (ret) {
    rados_shutdown(*cluster);
    return oss.str();
  }
    
  char *cmd[2];
  cmd[1] = NULL;

  std::string profile_create = "{\"prefix\": \"osd erasure-code-profile set\", \"name\": \"testprofile-" + pool_name + "\", \"profile\": [ \"k=2\", \"m=1\", \"ruleset-failure-domain=osd\"]}";
  cmd[0] = (char *)profile_create.c_str();
  ret = rados_mon_command(*cluster, (const char **)cmd, 1, "", 0, NULL, 0, NULL, 0);
  if (ret) {
    rados_shutdown(*cluster);
    oss << "rados_mon_command erasure-code-profile set name:testprofile-" << pool_name << " failed with error " << ret;
    return oss.str();
  }

  std::string cmdstr = "{\"prefix\": \"osd pool create\", \"pool\": \"" +
     pool_name + "\", \"pool_type\":\"erasure\", \"pg_num\":8, \"pgp_num\":8, \"erasure_code_profile\":\"testprofile-" + pool_name + "\"}";
  cmd[0] = (char *)cmdstr.c_str();
  ret = rados_mon_command(*cluster, (const char **)cmd, 1, "", 0, NULL, 0, NULL, 0);
  if (ret) {
    destroy_ec_profile(cluster, pool_name, oss);
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

int destroy_ruleset_pp(Rados &cluster,
                       std::string ruleset,
                       std::ostream &oss)
{
  bufferlist inbl;
  int ret = cluster.mon_command("{\"prefix\": \"osd crush rule rm\", \"name\":\"" +
                                ruleset + "\"}", inbl, NULL, NULL);
  if (ret)
    oss << "mon_command: osd crush rule rm " + ruleset + " failed with error " << ret << std::endl;
  return ret;
}

int destroy_ec_profile_pp(Rados &cluster, const std::string& pool_name,
			  std::ostream &oss)
{
  bufferlist inbl;
  int ret = cluster.mon_command("{\"prefix\": \"osd erasure-code-profile rm\", \"name\": \"testprofile-" + pool_name + "\"}",
                                inbl, NULL, NULL);
  if (ret)
    oss << "mon_command: osd erasure-code-profile rm testprofile-" << pool_name << " failed with error " << ret << std::endl;
  return ret;
}

int destroy_ec_profile_and_ruleset_pp(Rados &cluster,
                                      std::string ruleset,
                                      std::ostream &oss)
{
  int ret;
  ret = destroy_ec_profile_pp(cluster, ruleset, oss);
  if (ret)
    return ret;
  return destroy_ruleset_pp(cluster, ruleset, oss);
}

std::string create_one_ec_pool_pp(const std::string &pool_name, Rados &cluster)
{
  std::string err = connect_cluster_pp(cluster);
  if (err.length())
    return err;

  std::ostringstream oss;
  int ret = destroy_ec_profile_and_ruleset_pp(cluster, pool_name, oss);
  if (ret) {
    cluster.shutdown();
    return oss.str();
  }

  bufferlist inbl;
  ret = cluster.mon_command(
    "{\"prefix\": \"osd erasure-code-profile set\", \"name\": \"testprofile-" + pool_name + "\", \"profile\": [ \"k=2\", \"m=1\", \"ruleset-failure-domain=osd\"]}",
    inbl, NULL, NULL);
  if (ret) {
    cluster.shutdown();
    oss << "mon_command erasure-code-profile set name:testprofile-" << pool_name << " failed with error " << ret;
    return oss.str();
  }
    
  ret = cluster.mon_command(
    "{\"prefix\": \"osd pool create\", \"pool\": \"" + pool_name + "\", \"pool_type\":\"erasure\", \"pg_num\":8, \"pgp_num\":8, \"erasure_code_profile\":\"testprofile-" + pool_name + "\"}",
    inbl, NULL, NULL);
  if (ret) {
    bufferlist inbl;
    destroy_ec_profile_pp(cluster, pool_name, oss);
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
  if (ret) {
    rados_shutdown(*cluster);
    return ret;
  }

  std::ostringstream oss;
  ret = destroy_ec_profile_and_ruleset(cluster, pool_name, oss);
  if (ret) {
    rados_shutdown(*cluster);
    return ret;
  }

  rados_wait_for_latest_osdmap(*cluster);
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
  if (ret) {
    cluster.shutdown();
    return ret;
  }

  std::ostringstream oss;
  ret = destroy_ec_profile_and_ruleset_pp(cluster, pool_name, oss);
  if (ret) {
    cluster.shutdown();
    return ret;
  }

  cluster.wait_for_latest_osdmap();
  cluster.shutdown();
  return ret;
}

void assert_eq_sparse(bufferlist& expected,
                      const std::map<uint64_t, uint64_t>& extents,
                      bufferlist& actual) {
  auto i = expected.begin();
  auto p = actual.begin();
  uint64_t pos = 0;
  for (auto extent : extents) {
    const uint64_t start = extent.first;
    const uint64_t end = start + extent.second;
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
