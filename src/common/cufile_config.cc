// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 smarttab

#include "common/cufile_config.h"

#include <cstdlib>
#include <fstream>
#include <sstream>

#include "common/ceph_context.h"
#include "common/config.h"
#include "common/debug.h"
#include "common/errno.h"
#include "common/safe_io.h"
#include "json_spirit/json_spirit.h"

#define dout_subsys ceph_subsys_
#undef dout_prefix
#define dout_prefix *_dout << "cufile: "

namespace ceph::rdma {

namespace {

// where libcufile reads its settings when CUFILE_ENV_PATH_JSON is unset
constexpr const char* DEFAULT_CUFILE_JSON = "/etc/cufile.json";

std::string strip_comments(std::string_view in)
{
  std::string out;
  out.reserve(in.size());
  bool in_string = false;
  for (size_t i = 0; i < in.size(); i++) {
    const char c = in[i];
    if (in_string) {
      out += c;
      if (c == '\\' && i + 1 < in.size()) {
        out += in[++i];
      } else if (c == '"') {
        in_string = false;
      }
    } else if (c == '"') {
      in_string = true;
      out += c;
    } else if (in.substr(i, 2) == "//") {
      i = in.find('\n', i);
      if (i == std::string_view::npos) {
        break;
      }
      out += '\n';
    } else if (in.substr(i, 2) == "/*") {
      i = in.find("*/", i + 2);
      if (i == std::string_view::npos) {
        break;
      }
      i++;
    } else {
      out += c;
    }
  }
  return out;
}

} // anonymous namespace

std::string cufile_json_with_addrs(std::string_view base,
                                   const std::vector<std::string>& addrs)
{
  json_spirit::mValue v;
  if (!json_spirit::read(strip_comments(base), v) ||
      v.type() != json_spirit::obj_type) {
    return {};
  }
  auto& props = v.get_obj()["properties"];
  if (props.type() != json_spirit::obj_type) {
    props = json_spirit::mObject();
  }
  props.get_obj()["rdma_dev_addr_list"] =
    json_spirit::mArray(addrs.begin(), addrs.end());
  return json_spirit::write(v, json_spirit::pretty_print |
                               json_spirit::remove_trailing_zeros);
}

void setup_cufile_json(CephContext* cct, const std::string& config_path,
                       const std::string& addr)
{
  if (getenv("CUFILE_ENV_PATH_JSON")) {
    return;
  }
  if (!config_path.empty()) {
    setenv("CUFILE_ENV_PATH_JSON", config_path.c_str(), 0);
    return;
  }
  if (addr.empty() ||
      cct->_conf.get_val<std::string>("rdma_network").empty()) {
    return;
  }

  // start from the host's file so its other settings (DMABuf, DC key,
  // logging) still apply
  std::string base = "{}";
  if (std::ifstream in(DEFAULT_CUFILE_JSON); in) {
    std::ostringstream ss;
    ss << in.rdbuf();
    base = ss.str();
  }
  const std::string json = cufile_json_with_addrs(base, {addr});
  if (json.empty()) {
    lderr(cct) << "unable to parse " << DEFAULT_CUFILE_JSON
               << "; the cuObject client uses it unchanged" << dendl;
    return;
  }
  const std::string dir = cct->_conf.get_val<std::string>("run_dir");
  const std::string file = cct->_conf->cluster + "-" +
    cct->_conf->name.to_str() + ".cufile.json";
  if (int r = safe_write_file(dir.c_str(), file.c_str(), json.data(),
                              json.size(), 0644); r < 0) {
    lderr(cct) << "unable to write " << dir << "/" << file << ": "
               << cpp_strerror(r) << "; the cuObject client uses "
               << DEFAULT_CUFILE_JSON << dendl;
    return;
  }
  const std::string path = dir + "/" + file;
  setenv("CUFILE_ENV_PATH_JSON", path.c_str(), 0);
  ldout(cct, 1) << "rdma_dev_addr_list [" << addr << "] in " << path
                << dendl;
}

} // namespace ceph::rdma
