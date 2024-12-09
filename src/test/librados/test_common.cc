// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "common/Formatter.h"
#include "include/ceph_assert.h"
#include "include/stringify.h"
#include "json_spirit/json_spirit.h"
#include "test_common.h"

using namespace std;

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
    [[maybe_unused]] bool json_parse_success = json_spirit::read(out, root);
    ceph_assert(json_parse_success);
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

struct pool_op_error : std::exception {
  string msg;
  pool_op_error(const std::string& pool_name,
		const std::string& func_name,
		int err) {
    std::ostringstream oss;
    oss << func_name << "(" << pool_name << ") failed with error " << err;
    msg = oss.str();
  }
  const char* what() const noexcept override {
    return msg.c_str();
  }
};

template<typename Func>
std::string with_healthy_cluster(rados_t* cluster,
				 const std::string& pool_name,
				 Func&& func)
{
  try {
    // Wait for 'creating/backfilling' to clear
    if (int r = wait_for_healthy(cluster); r != 0) {
      throw pool_op_error{pool_name, "wait_for_healthy", r};
    }
    func();
    // Wait for 'creating/backfilling' to clear
    if (int r = wait_for_healthy(cluster); r != 0) {
      throw pool_op_error{pool_name, "wait_for_healthy", r};
    }
  } catch (const pool_op_error& e) {
    return e.what();
  }
  return "";
}
}

std::string set_pg_num(
    rados_t *cluster, const std::string &pool_name, uint32_t pg_num)
{
  return with_healthy_cluster(cluster, pool_name, [&] {
    // Adjust pg_num
      if (int r = rados_pool_set(cluster, pool_name, "pg_num",
				 stringify(pg_num));
	  r != 0) {
	throw pool_op_error{pool_name, "set_pg_num", r};
      }
  });
}

std::string set_pgp_num(
    rados_t *cluster, const std::string &pool_name, uint32_t pgp_num)
{
  return with_healthy_cluster(cluster, pool_name, [&] {
    // Adjust pgp_num
    if (int r = rados_pool_set(cluster, pool_name, "pgp_num",
			       stringify(pgp_num));
	r != 0) {
      throw pool_op_error{pool_name, "set_pgp_num", r};
    }
  });
}
