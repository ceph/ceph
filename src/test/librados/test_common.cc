// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "common/Formatter.h"
#include "include/stringify.h"
#include "json_spirit/json_spirit.h"
#include "test_common.h"

namespace {

using namespace ceph;

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
