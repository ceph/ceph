// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*
// vim: ts=8 sw=2 sts=2 expandtab

#include "include/rados/librados.h"
#include "include/rados/librados.hpp"
#include "test/librados/test.h"

#include "include/stringify.h"
#include "common/ceph_context.h"
#include "common/config.h"

#include <errno.h>
#include <sstream>
#include <stdlib.h>
#include <string>
#include <time.h>
#include <unistd.h>
#include <iostream>
#include "gtest/gtest.h"

std::string create_one_pool(const std::string &pool_name, rados_t *cluster) {
  std::string err_str = connect_cluster(cluster);
  if (err_str.length())
    return err_str;

  return create_pool(pool_name, cluster);
}

std::string create_pool(const std::string &pool_name, rados_t *cluster) {
  int ret = rados_pool_create(*cluster, pool_name.c_str());
  if (ret) {
    rados_shutdown(*cluster);
    std::ostringstream oss;
    oss << "create_one_pool(" << pool_name << ") failed with error " << ret;
    return oss.str();
  }

  rados_ioctx_t ioctx;
  ret = rados_ioctx_create(*cluster, pool_name.c_str(), &ioctx);
  if (ret < 0) {
    rados_shutdown(*cluster);
    std::ostringstream oss;
    oss << "rados_ioctx_create(" << pool_name << ") failed with error " << ret;
    return oss.str();
  }

  rados_application_enable(ioctx, "rados", 1);
  rados_ioctx_destroy(ioctx);
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

int destroy_rule(rados_t *cluster,
		 const std::string &rule,
		 std::ostream &oss)
{
  char *cmd[2];
  std::string tmp = ("{\"prefix\": \"osd crush rule rm\", \"name\":\"" +
                     rule + "\"}");
  cmd[0] = (char*)tmp.c_str();
  cmd[1] = NULL;
  int ret = rados_mon_command(*cluster, (const char **)cmd, 1, "", 0, NULL, 0, NULL, 0);
  if (ret)
    oss << "rados_mon_command: osd crush rule rm " + rule + " failed with error " << ret;
  return ret;
}

int destroy_ec_profile_and_rule(rados_t *cluster,
				const std::string &rule,
				std::ostream &oss)
{
  int ret;
  ret = destroy_ec_profile(cluster, rule, oss);
  if (ret)
    return ret;
  return destroy_rule(cluster, rule, oss);
}

std::string set_pool_flags(const std::string &pool_name, rados_t *cluster, int64_t flags, bool set_not_unset) {
  std::ostringstream oss;

  std::string cmdstr = fmt::format(
      R"({{"prefix": "osd pool set", "pool": "{}", "var": "{}", "val": "{}", "yes_i_really_mean_it": true}})",
      pool_name,
      (set_not_unset ? "set_pool_flags" : "unset_pool_flags"),
      flags
  );

  char *cmd[2];
  cmd[0] = (char *)cmdstr.c_str();
  cmd[1] = NULL;

  int ret = rados_mon_command(*cluster, (const char **)cmd, 1, "", 0, NULL, 0, NULL, 0);
  if (ret) {
    oss << "rados_mon_command osd pool set set_pool_flags failed with error " << ret;
  }

  return oss.str();
}

std::string set_split_ops(const std::string &pool_name, rados_t *cluster, bool set_not_unset) {
  return set_pool_flags(pool_name, cluster, 1<<20, set_not_unset);
}

std::string create_one_ec_pool(const std::string &pool_name, rados_t *cluster, bool fast_ec) {
  std::string err = connect_cluster(cluster);
  if (err.length())
    return err;

  return create_ec_pool(pool_name, cluster, fast_ec);
}

std::string create_ec_pool(const std::string &pool_name, rados_t *cluster, bool fast_ec) {
  std::ostringstream oss;
  int ret = destroy_ec_profile_and_rule(cluster, pool_name, oss);
  if (ret) {
    rados_shutdown(*cluster);
    return oss.str();
  }
    
  char *cmd[2];
  cmd[1] = NULL;

  std::string profile_create = "{\"prefix\": \"osd erasure-code-profile set\", \"name\": \"testprofile-" + pool_name + "\", \"profile\": [ \"k=2\", \"m=1\", \"crush-failure-domain=osd\"]}";
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

  if (fast_ec) {
    std::string fast_ec_str = "{\"prefix\": \"osd pool set\", \"pool\": \"" + pool_name +
        "\", \"var\": \"allow_ec_optimizations\", \"val\": \"true\"}";
    cmd[0] = (char *)fast_ec_str.c_str();
    ret = rados_mon_command(*cluster, (const char **)cmd, 1, "", 0, NULL, 0, NULL, 0);
    if (ret) {
      destroy_one_pool(pool_name, cluster);
      destroy_ec_profile(cluster, pool_name, oss);
      rados_shutdown(*cluster);
      oss << "rados_mon_command osd pool create failed with error " << ret;
      return oss.str();
    }
  }

  rados_wait_for_latest_osdmap(*cluster);
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

int destroy_pool(const std::string &pool_name, rados_t *cluster) {
  return rados_pool_delete(*cluster, pool_name.c_str());
}

int destroy_ec_pool(const std::string &pool_name, rados_t *cluster) {
  int ret = destroy_pool(pool_name, cluster);
  if (ret) {
    return ret;
  }
  CephContext *cct = static_cast<CephContext*>(rados_cct(*cluster));
  if (!cct->_conf->mon_fake_pool_delete) { // hope this is in [global]
    std::ostringstream oss;
    ret = destroy_ec_profile_and_rule(cluster, pool_name, oss);
  }

  return ret;
}

int destroy_one_pool(const std::string &pool_name, rados_t *cluster)
{
  destroy_pool(pool_name, cluster);
  rados_shutdown(*cluster);
  return 0;
}

int destroy_one_ec_pool(const std::string &pool_name, rados_t *cluster)
{
  int ret = destroy_ec_pool(pool_name, cluster);
  rados_wait_for_latest_osdmap(*cluster);
  rados_shutdown(*cluster);
  return ret;
}
