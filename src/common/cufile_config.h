// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <string>
#include <string_view>
#include <vector>

#include "include/common_fwd.h"

namespace ceph::rdma {

/**
 * The cuObject client library (libcufile) takes the NICs it registers
 * memory with only from properties.rdma_dev_addr_list in cufile.json;
 * no API or environment variable overrides it.
 *
 * Return base, a cufile.json (with // and slash-star comments, as
 * NVIDIA ships it), with that list replaced by addrs. Every other
 * setting is kept; comments are not. Empty if base is not a JSON
 * object.
 */
std::string cufile_json_with_addrs(std::string_view base,
                                   const std::vector<std::string>& addrs);

/**
 * Point this process's cuObject client at addr before its first
 * cuObjClient is built.
 *
 * A deployment that names a cufile.json keeps it: CUFILE_ENV_PATH_JSON,
 * or else config_path (the daemon's own option), which is exported.
 * Otherwise, when rdma_network is set, /etc/cufile.json with
 * rdma_dev_addr_list = [addr] is written to
 * $run_dir/$cluster-$name.cufile.json and that path exported, so no
 * host needs a hand-written file. Without rdma_network the library
 * reads /etc/cufile.json as before. Failures are logged; the library
 * then falls back to /etc/cufile.json.
 */
void setup_cufile_json(CephContext* cct, const std::string& config_path,
                       const std::string& addr);

} // namespace ceph::rdma
