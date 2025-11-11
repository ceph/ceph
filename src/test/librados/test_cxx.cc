// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*
// vim: ts=8 sw=2 sts=2 expandtab

#include "test_cxx.h"

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

#include "test.h"
#include "gtest/gtest.h"

using namespace librados;

std::string create_one_pool_pp(const std::string &pool_name, Rados &cluster)
{
    return create_one_pool_pp(pool_name, cluster, {});
}
std::string create_one_pool_pp(const std::string &pool_name, Rados &cluster,
                               const std::map<std::string, std::string> &config)
{
  std::string err = connect_cluster_pp(cluster, config);
  if (err.length())
    return err;
  int ret = cluster.pool_create(pool_name.c_str());
  if (ret) {
    cluster.shutdown();
    std::ostringstream oss;
    oss << "cluster.pool_create(" << pool_name << ") failed with error " << ret;
    return oss.str();
  }

  IoCtx ioctx;
  ret = cluster.ioctx_create(pool_name.c_str(), ioctx);
  if (ret < 0) {
    cluster.shutdown();
    std::ostringstream oss;
    oss << "cluster.ioctx_create(" << pool_name << ") failed with error "
        << ret;
    return oss.str();
  }
  ioctx.application_enable("rados", true);
  return "";
}

int destroy_rule_pp(Rados &cluster,
                       const std::string &rule,
                       std::ostream &oss)
{
  int ret = cluster.mon_command("{\"prefix\": \"osd crush rule rm\", \"name\":\"" +
                                rule + "\"}", {}, NULL, NULL);
  if (ret)
    oss << "mon_command: osd crush rule rm " + rule + " failed with error " << ret << std::endl;
  return ret;
}

int destroy_ec_profile_pp(Rados &cluster, const std::string& pool_name,
			  std::ostream &oss)
{
  int ret = cluster.mon_command("{\"prefix\": \"osd erasure-code-profile rm\", \"name\": \"testprofile-" + pool_name + "\"}",
                                {}, NULL, NULL);
  if (ret)
    oss << "mon_command: osd erasure-code-profile rm testprofile-" << pool_name << " failed with error " << ret << std::endl;
  return ret;
}

int destroy_ec_profile_and_rule_pp(Rados &cluster,
                                      const std::string &rule,
                                      std::ostream &oss)
{
  int ret;
  ret = destroy_ec_profile_pp(cluster, rule, oss);
  if (ret)
    return ret;
  return destroy_rule_pp(cluster, rule, oss);
}

std::string create_one_ec_pool_pp(const std::string &pool_name, Rados &cluster) {
  std::string err = connect_cluster_pp(cluster);
  if (err.length())
    return err;

  err = create_ec_pool_pp(pool_name, cluster, false);
  if (err.length()) {
    cluster.shutdown();
    return err;
  }

  return err;
}

std::string create_ec_pool_pp(const std::string &pool_name, Rados &cluster, bool fast_ec) {
  std::ostringstream oss;
  int ret = destroy_ec_profile_and_rule_pp(cluster, pool_name, oss);
  if (ret) {
    return oss.str();
  }

  bufferlist inbl;
  ret = cluster.mon_command(
    "{\"prefix\": \"osd erasure-code-profile set\", \"name\": \"testprofile-" + pool_name + "\", \"profile\": [ \"k=2\", \"m=1\", \"crush-failure-domain=osd\"]}",
    {}, NULL, NULL);
  if (ret) {
    cluster.shutdown();
    oss << "mon_command erasure-code-profile set name:testprofile-" << pool_name << " failed with error " << ret;
    return oss.str();
  }
    
  ret = cluster.mon_command(
    "{\"prefix\": \"osd pool create\", \"pool\": \"" + pool_name + "\", \"pool_type\":\"erasure\", \"pg_num\":8, \"pgp_num\":8, \"erasure_code_profile\":\"testprofile-" + pool_name + "\"}",
    {}, NULL, NULL);
  if (ret) {
    bufferlist inbl;
    destroy_ec_profile_pp(cluster, pool_name, oss);
    oss << "mon_command osd pool create pool:" << pool_name << " pool_type:erasure failed with error " << ret;
    return oss.str();
  }

  if (fast_ec) {
    ret = cluster.mon_command(
      "{\"prefix\": \"osd pool set\", \"pool\": \"" + pool_name +
      "\", \"var\": \"allow_ec_optimizations\", \"val\": \"true\"}",
      std::move(inbl), NULL, NULL);
    if (ret) {
      destroy_ec_pool_pp(pool_name, cluster);
      destroy_ec_profile_pp(cluster, pool_name, oss);
      oss << "rados_mon_command osd pool create failed with error " << ret;
      return oss.str();
    }
  }

  cluster.wait_for_latest_osdmap();
  return "";
}

std::string set_pool_flags_pp(const std::string &pool_name, librados::Rados &cluster, int64_t flags, bool set_not_unset) {
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

  bufferlist inbl;
  int ret = cluster.mon_command(std::move(cmdstr), std::move(inbl), NULL, NULL);
  if (ret) {
    oss << "rados_mon_command osd pool set set_pool_flags_pp failed with error " << ret;
  }

  return oss.str();
}

std::string set_allow_ec_overwrites_pp(const std::string &pool_name, Rados &cluster, bool allow)
{
  std::ostringstream oss;
  int ret = cluster.mon_command(
    "{\"prefix\": \"osd pool set\", \"pool\": \"" + pool_name + "\", \"var\": \"allow_ec_overwrites\", \"val\": \"" + (allow ? "true" : "false") + "\"}",
    {}, NULL, NULL);
  if (ret) {
    cluster.shutdown();
    oss << "mon_command osd pool set pool:" << pool_name << " pool_type:erasure allow_ec_overwrites true failed with error " << ret;
    return oss.str();
  }
  return "";
}

std::string connect_cluster_pp(librados::Rados &cluster)
{
  return connect_cluster_pp(cluster, {});
}

std::string connect_cluster_pp(librados::Rados &cluster,
                               const std::map<std::string, std::string> &config)
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

  for (auto &setting : config) {
    ret = cluster.conf_set(setting.first.c_str(), setting.second.c_str());
    if (ret) {
      std::ostringstream oss;
      oss << "failed to set config value " << setting.first << " to '"
          << setting.second << "': " << strerror(-ret);
      return oss.str();
    }
  }

  ret = cluster.connect();
  if (ret) {
    cluster.shutdown();
    std::ostringstream oss;
    oss << "cluster.connect failed with error " << ret;
    return oss.str();
  }
  return "";
}

int destroy_one_pool_pp(const std::string &pool_name, Rados &cluster)
{
  int ret = cluster.pool_delete(pool_name.c_str());
  if (ret) {
    return ret;
  }

  CephContext *cct = static_cast<CephContext*>(cluster.cct());
  if (!cct->_conf->mon_fake_pool_delete) { // hope this is in [global]
    std::ostringstream oss;
    ret = destroy_ec_profile_and_rule_pp(cluster, pool_name, oss);
    if (ret) {
      return ret;
    }
  }
  return 0;
}

int destroy_one_ec_pool_pp(const std::string &pool_name, Rados &cluster)
{
  int ret = destroy_one_pool_pp(pool_name, cluster);
  if (ret) {
    cluster.shutdown();
    return ret;
  }

  cluster.wait_for_latest_osdmap();
  cluster.shutdown();
  return ret;
}

int destroy_ec_pool_pp(const std::string &pool_name, Rados &cluster) {
  return cluster.pool_delete(pool_name.c_str());
}
