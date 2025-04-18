// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "include/buffer.h"
#include "include/stringify.h"
#include "common/ceph_argparse.h"
#include "common/config_proxy.h"
#include "common/errno.h"
#include "erasure-code/ErasureCodePlugin.h"
#include "global/global_context.h"
#include "global/global_init.h"
#include "osd/ECUtil.h"

#include <iostream>
#include <map>
#include <string>
#include <vector>

#include <boost/algorithm/string.hpp>

using namespace std;

std::vector<std::string> display_params = {
  "chunk_count", "data_chunk_count", "coding_chunk_count"
};

void usage(const std::string message, ostream &out) {
  if (!message.empty()) {
    out << message << std::endl;
    out << "" << std::endl;
  }
  out << "usage: ceph-erasure-code-tool test-plugin-exists <plugin>" << std::endl;
  out << "       ceph-erasure-code-tool validate-profile <profile> [<display-param> ...]" << std::endl;
  out << "       ceph-erasure-code-tool calc-chunk-size <profile> <object_size>" << std::endl;
  out << "       ceph-erasure-code-tool encode <profile> <stripe_unit> <want_to_encode> <fname>" << std::endl;
  out << "       ceph-erasure-code-tool decode <profile> <stripe_unit> <want_to_decode> <fname>" << std::endl;
  out << "" << std::endl;
  out << "  plugin          - plugin name" << std::endl;
  out << "  profile         - comma separated list of erasure-code profile settings" << std::endl;
  out << "                    example: plugin=jerasure,technique=reed_sol_van,k=3,m=2" << std::endl;
  out << "  display-param   - parameter to display (display all if empty)" << std::endl;
  out << "                    may be: " << display_params << std::endl;
  out << "  object_size     - object size" << std::endl;
  out << "  stripe_unit     - stripe unit" << std::endl;
  out << "  want_to_encode  - comma separated list of shards to encode" << std::endl;
  out << "  want_to_decode  - comma separated list of shards to decode" << std::endl;
  out << "  fname           - name for input/output files" << std::endl;
  out << "                    when encoding input is read form {fname} file," << std::endl;
  out << "                                  result is stored in {fname}.{shard} files" << std::endl;
  out << "                    when decoding input is read form {fname}.{shard} files," << std::endl;
  out << "                                  result is stored in {fname} file" << std::endl;
}

int ec_init(const std::string &profile_str,
            const std::string &stripe_unit_str,
            ceph::ErasureCodeInterfaceRef *ec_impl,
            std::unique_ptr<ECUtil::stripe_info_t> *sinfo) {
  ceph::ErasureCodeProfile profile;
  std::vector<std::string> opts;
  boost::split(opts, profile_str, boost::is_any_of(", "));
  for (auto &opt_str : opts) {
    std::vector<std::string> opt;
    boost::split(opt, opt_str, boost::is_any_of("="));
    if (opt.size() <= 1) {
      usage("invalid profile", std::cerr);
      return 1;
    }
    profile[opt[0]] = opt[1];
  }
  auto plugin = profile.find("plugin");
  if (plugin == profile.end()) {
      usage("invalid profile: plugin not specified", std::cerr);
      return 1;
  }

  stringstream ss;
  ceph::ErasureCodePluginRegistry::instance().factory(
    plugin->second, g_conf().get_val<std::string>("erasure_code_dir"),
    profile, ec_impl, &ss);
  if (!*ec_impl) {
    usage("invalid profile: " + ss.str(), std::cerr);
    return 1;
  }

  if (sinfo == nullptr) {
    return 0;
  }

  uint64_t stripe_unit = atoi(stripe_unit_str.c_str());
  if (stripe_unit <= 0) {
    usage("invalid stripe unit", std::cerr);
    return 1;
  }

  uint64_t stripe_size = atoi(profile["k"].c_str());
  ceph_assert(stripe_size > 0);
  uint64_t stripe_width = stripe_size * stripe_unit;
  sinfo->reset(new ECUtil::stripe_info_t(*ec_impl, nullptr, stripe_width));

  return 0;
}

int do_test_plugin_exists(const std::vector<const char*> &args) {
  if (args.size() < 1) {
    usage("not enought arguments", std::cerr);
    return 1;
  }

  ErasureCodePluginRegistry &instance = ErasureCodePluginRegistry::instance();
  ErasureCodePlugin *plugin;
  stringstream ss;

  std::lock_guard l{instance.lock};
  int r = instance.load(
    args[0], g_conf().get_val<std::string>("erasure_code_dir"), &plugin, &ss);
  std::cerr << ss.str() << std::endl;
  return r;
}

int do_validate_profile(const std::vector<const char*> &args) {
  if (args.size() < 1) {
    usage("not enought arguments", std::cerr);
    return 1;
  }

  ceph::ErasureCodeInterfaceRef ec_impl;
  int r = ec_init(args[0], {}, &ec_impl, nullptr);
  if (r < 0) {
    return r;
  }

  if (args.size() > 1) {
    std::set<std::string> valid_params(display_params.begin(),
                                       display_params.end());
    display_params.clear();
    for (size_t i = 1; i < args.size(); i++) {
      if (!valid_params.count(args[i])) {
        usage("invalid display param: " + std::string(args[i]), std::cerr);
        return 1;
      }
      display_params.push_back(args[i]);
    }
  }

  for (auto &param : display_params) {
    if (display_params.size() > 1) {
      std::cout << param << ": ";
    }
    if (param == "chunk_count") {
      std::cout << ec_impl->get_chunk_count() << std::endl;
    } else if (param == "data_chunk_count") {
      std::cout << ec_impl->get_data_chunk_count() << std::endl;
    } else if (param == "coding_chunk_count") {
      std::cout << ec_impl->get_coding_chunk_count() << std::endl;
    } else {
      ceph_abort_msgf("unknown display_param: %s", param.c_str());
    }
  }

  return 0;
}

int do_calc_chunk_size(const std::vector<const char*> &args) {
  if (args.size() < 2) {
    usage("not enought arguments", std::cerr);
    return 1;
  }

  ceph::ErasureCodeInterfaceRef ec_impl;
  int r = ec_init(args[0], {}, &ec_impl, nullptr);
  if (r < 0) {
    return r;
  }

  uint64_t object_size = atoi(args[1]);
  if (object_size <= 0) {
    usage("invalid object size", std::cerr);
    return 1;
  }

  std::cout << ec_impl->get_chunk_size(object_size) << std::endl;
  return 0;
}

int do_encode(const std::vector<const char*> &args) {
  if (args.size() < 4) {
    usage("not enought arguments", std::cerr);
    return 1;
  }

  ceph::ErasureCodeInterfaceRef ec_impl;
  std::unique_ptr<ECUtil::stripe_info_t> sinfo;
  int r = ec_init(args[0], args[1], &ec_impl, &sinfo);
  if (r < 0) {
    return r;
  }

  ECUtil::shard_extent_map_t encoded_data(sinfo.get());
  std::vector<std::string> shards;
  boost::split(shards, args[2], boost::is_any_of(","));
  ceph::bufferlist input_data;
  std::string fname = args[3];

  std::string error;
  r = input_data.read_file(fname.c_str(), &error);
  if (r < 0) {
    std::cerr << "failed to read " << fname << ": " << error << std::endl;
    return 1;
  }

  uint64_t stripe_width = sinfo->get_stripe_width();
  if (input_data.length() % stripe_width != 0) {
    uint64_t pad = stripe_width - input_data.length() % stripe_width;
    input_data.append_zero(pad);
  }

  sinfo->ro_range_to_shard_extent_map(0, input_data.length(), input_data, encoded_data);
  r = encoded_data.encode(ec_impl, nullptr, encoded_data.get_ro_end());
  if (r < 0) {
    std::cerr << "failed to encode: " << cpp_strerror(r) << std::endl;
    return 1;
  }

  for (auto &[shard, _] : encoded_data.get_extent_maps()) {
    std::string name = fname + "." + stringify(shard);
    bufferlist bl;
    encoded_data.get_shard_first_buffer(shard, bl);
    r = bl.write_file(name.c_str());
    if (r < 0) {
      std::cerr << "failed to write " << name << ": " << cpp_strerror(r)
                << std::endl;
      return 1;
    }
  }

  return 0;
}

int do_decode(const std::vector<const char*> &args) {
  if (args.size() < 4) {
    usage("not enought arguments", std::cerr);
    return 1;
  }

  ceph::ErasureCodeInterfaceRef ec_impl;
  std::unique_ptr<ECUtil::stripe_info_t> sinfo;
  int r = ec_init(args[0], args[1], &ec_impl, &sinfo);
  if (r) {
    return r;
  }

  ECUtil::shard_extent_map_t encoded_data(sinfo.get());
  std::vector<std::string> shards;
  boost::split(shards, args[2], boost::is_any_of(","));
  std::string fname = args[3];

  std::set<int> want_to_read;
  const auto chunk_mapping = ec_impl->get_chunk_mapping();
  for (auto &shard_str : shards) {
    std::string name = fname + "." + shard_str;
    std::string error;
    bufferlist bl;
    r = bl.read_file(name.c_str(), &error);
    if (r < 0) {
      std::cerr << "failed to read " << name << ": " << error << std::endl;
      return 1;
    }
    shard_id_t shard = sinfo->get_shard(raw_shard_id_t(atoi(shard_str.c_str())));
    encoded_data.insert_in_shard(shard, 0, bl);
  }

  ECUtil::shard_extent_set_t wanted(sinfo->get_k_plus_m());
  sinfo->ro_range_to_shard_extent_set(encoded_data.get_ro_start(),
    encoded_data.get_ro_end() - encoded_data.get_ro_start(), wanted);

  r = encoded_data.decode(ec_impl, wanted, encoded_data.get_ro_end());
  if (r < 0) {
    std::cerr << "failed to decode: " << cpp_strerror(r) << std::endl;
    return 1;
  }

  bufferlist decoded_data = encoded_data.get_ro_buffer();
  r = decoded_data.write_file(fname.c_str());
  if (r < 0) {
    std::cerr << "failed to write " << fname << ": " << cpp_strerror(r)
              << std::endl;
    return 1;
  }

  return 0;
}

int main(int argc, const char **argv) {
  auto args = argv_to_vec(argc, argv);
  auto cct = global_init(nullptr, args, CEPH_ENTITY_TYPE_CLIENT,
                         CODE_ENVIRONMENT_UTILITY,
                         CINIT_FLAG_NO_MON_CONFIG);

  if (args.empty() || args[0] == std::string("-h") ||
      args[0] == std::string("--help")) {
    usage("", std::cout);
    return 0;
  }

  if (args.size() < 1) {
    usage("not enought arguments", std::cerr);
    return 1;
  }

  std::string cmd = args[0];
  std::vector<const char*> cmd_args(args.begin() + 1, args.end());

  if (cmd == "test-plugin-exists") {
    return do_test_plugin_exists(cmd_args);
  } else if (cmd == "validate-profile") {
    return do_validate_profile(cmd_args);
  } else if (cmd == "calc-chunk-size") {
    return do_calc_chunk_size(cmd_args);
  } else if (cmd == "encode") {
    return do_encode(cmd_args);
  } else if (cmd == "decode") {
    return do_decode(cmd_args);
  }

  usage("invalid command: " + cmd, std::cerr);
  return 1;
}
