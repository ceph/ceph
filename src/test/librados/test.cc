// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*
// vim: ts=8 sw=2 smarttab

#include "include/rados/librados.h"
#include "include/rados/librados.hpp"
#include "test/librados/test.h"

#include "include/stringify.h"
#include "common/Formatter.h"
#include "json_spirit/json_spirit.h"
#include "errno.h"

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

int wait_for_healthy(rados_t *cluster)
{
  bool healthy = false;
  // This timeout is very long because the tests are sometimes
  // run on a thrashing cluster
  int timeout = 3600;
  int slept = 0;

  while(!healthy) {
    JSONFormatter cmd_f;
    cmd_f.open_object_section("command");
    cmd_f.dump_string("prefix", "status");
    cmd_f.dump_string("format", "json");
    cmd_f.close_section();
    std::ostringstream cmd_stream;
    cmd_f.flush(cmd_stream);
    const std::string serialized_cmd = cmd_stream.str();

    const char *cmd[2];
    cmd[1] = NULL;
    cmd[0] = serialized_cmd.c_str();

    char *outbuf = NULL;
    size_t outlen = 0;
    int ret = rados_mon_command(*cluster, (const char **)cmd, 1, "", 0,
        &outbuf, &outlen, NULL, NULL);
    if (ret) {
      return ret;
    }

    std::string out(outbuf, outlen);
    rados_buffer_free(outbuf);

    json_spirit::mValue root;
    assert(json_spirit::read(out, root));
    json_spirit::mObject root_obj = root.get_obj();
    json_spirit::mObject pgmap = root_obj["pgmap"].get_obj();
    json_spirit::mArray pgs_by_state = pgmap["pgs_by_state"].get_array();

    if (pgs_by_state.size() == 1) {
      json_spirit::mObject state = pgs_by_state[0].get_obj();
      std::string state_name = state["state_name"].get_str();
      if (state_name != std::string("active+clean")) {
        healthy = false;
      } else {
        healthy = true;
      }

    } else {
      healthy = false;
    }

    if (slept >= timeout) {
      return -ETIMEDOUT;
    };

    if (!healthy) {
      sleep(1);
      slept += 1;
    }
  }

  return 0;
}

int rados_pool_set(
    rados_t *cluster,
    const std::string &pool_name,
    const std::string &var,
    const std::string &val)
{
  JSONFormatter cmd_f;
  cmd_f.open_object_section("command");
  cmd_f.dump_string("prefix", "osd pool set"); 
  cmd_f.dump_string("pool", pool_name);
  cmd_f.dump_string("var", var);
  cmd_f.dump_string("val", val);
  cmd_f.close_section();

  std::ostringstream cmd_stream;
  cmd_f.flush(cmd_stream);

  const std::string serialized_cmd = cmd_stream.str();

  const char *cmd[2];
  cmd[1] = NULL;
  cmd[0] = serialized_cmd.c_str();
  int ret = rados_mon_command(*cluster, (const char **)cmd, 1, "", 0, NULL,
      NULL, NULL, NULL);
  return ret;
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

std::string set_pg_num(
    rados_t *cluster, const std::string &pool_name, uint32_t pg_num)
{
  // Wait for 'creating' to clear
  int r = wait_for_healthy(cluster);
  if (r != 0) {
    goto err;
  }

  // Adjust pg_num
  r = rados_pool_set(cluster, pool_name, "pg_num", stringify(pg_num));
  if (r != 0) {
    goto err;
  }

  // Wait for 'creating' to clear
  r = wait_for_healthy(cluster);
  if (r != 0) {
    goto err;
  }

  return "";

err:
  rados_shutdown(*cluster);
  std::ostringstream oss;
  oss << __func__ << "(" << pool_name << ") failed with error " << r;
  return oss.str();
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
