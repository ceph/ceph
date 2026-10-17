// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

namespace ceph::rdma {

/// Client memory window advertised by an RDMA token.
struct token_window {
  uint64_t addr = 0;  ///< client buffer virtual address
  uint64_t size = 0;  ///< registered window size in bytes
};

/// Maximum token length accepted anywhere in ceph. The cuObject
/// descriptor is 81 characters today, and an ofi1 token at most 618;
/// this bounds untrusted input.
inline constexpr size_t RDMA_TOKEN_MAX_LEN = 640;

/// The third field of the libfabric tokens that ofi-rma writes.
inline constexpr std::string_view OFI_TOKEN_TAG = "ofi1";

/// Parse the addr and size fields of an RDMA descriptor token as
/// produced by cuObject clients: colon-separated hex fields
/// "raddr:rsize:rkey:lid:qp:has_gid:gid". Only the leading two fields
/// are interpreted; the rest of the token is opaque and must be
/// forwarded verbatim to the RDMA library. Returns std::nullopt on
/// malformed input.
std::optional<token_window> parse_rdma_token(std::string_view token);

/// Whether a token is a libfabric one: its third field is "ofi<N>",
/// the libfabric family at any version N, where a
/// cuObject descriptor has its memory key in hex. The OSD's cuObject
/// executor and the gateway's PUT take any other token for a cuObject
/// descriptor, so a version this build does not know still counts here
/// and is declined by the libfabric code. Only the shape is checked; a
/// libfabric token can still fail to parse.
bool is_ofi_token(std::string_view token);

/// Whether a token is a cuObject descriptor that names a window: not a
/// libfabric token, and parse_rdma_token() reads its addr and size.
bool is_cuobj_descriptor(std::string_view token);

/// the transport names of oob_client_transports and
/// oob_cluster_transport that are not libfabric providers
inline constexpr std::string_view TRANSPORT_CUOBJ = "cuobj";

/// Split a transport list ("verbs_ofi_rxm, cuobj") into its names, in
/// order, lower-cased, without duplicates or empty entries. A name of
/// the form <core>_ofi_<utility> is the libfabric provider
/// <core>;ofi_<utility>, as fi_info names it, since a semicolon starts a
/// comment in a configuration file; the list holds the libfabric name.
std::vector<std::string> parse_transport_list(std::string_view list);

/// a transport name as libfabric spells it: <core>_ofi_<utility> becomes
/// <core>;ofi_<utility>, verbs becomes verbs;ofi_rxm, which is what
/// libfabric layers on verbs for RDM endpoints, and any other name stays
/// as it is
std::string libfabric_name(std::string_view name);

/// whether a transport name is a libfabric provider: any name but
/// cuobj
bool is_libfabric_provider(std::string_view name);

/**
 * The out-of-band transports of a cluster, from oob_client_transports
 * and oob_cluster_transport. A daemon opens one libfabric endpoint: for
 * the cluster transport's provider when that is one, else for the first
 * provider in the client list.
 */
struct oob_transports {
  /// oob_client_transports, parsed
  std::vector<std::string> client;
  /// oob_cluster_transport: a libfabric provider, cuobj, or empty
  std::string cluster;
  /// the provider of the daemon's libfabric endpoint, or empty
  std::string ofi_provider;
  /// what the daemon ignores, and why, to log
  std::vector<std::string> warnings;

  bool client_names(std::string_view t) const;
  bool client_cuobj() const { return client_names(TRANSPORT_CUOBJ); }
  /// whether the client list names a libfabric provider
  bool client_ofi() const;
  bool cluster_cuobj() const { return cluster == TRANSPORT_CUOBJ; }
  bool cluster_ofi() const { return !cluster.empty() && !cluster_cuobj(); }
  /// whether either option names cuobj
  bool cuobj() const { return client_cuobj() || cluster_cuobj(); }
  /// whether either option names this libfabric provider
  bool names_provider(std::string_view p) const {
    return !ofi_provider.empty() && ofi_provider == p;
  }
};

/// Read the two options' values.
oob_transports parse_oob_transports(std::string_view client,
				    std::string_view cluster);

} // namespace ceph::rdma
