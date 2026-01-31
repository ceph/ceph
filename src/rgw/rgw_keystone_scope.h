// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#pragma once

#include <optional>
#include <string>
#include <vector>

#include "include/buffer.h"
#include "include/encoding.h"
#include "include/common_fwd.h"

namespace ceph {
  class Formatter;
}

namespace rgw::keystone {

// Import ceph encode/decode templates for visibility
using ceph::encode;
using ceph::decode;

// Forward declaration
class TokenEnvelope;

/**
 * Keystone authentication scope information.
 *
 * This structure captures the OpenStack Keystone authentication context
 * including project, user, and roles. It's used throughout the
 * authentication and logging pipeline.
 *
 * The structure supports:
 * - Binary encoding/decoding for RADOS backend (ops log storage)
 * - JSON formatting for file/socket backend (ops log output)
 * - Granular control via configuration flags (include_user, include_roles)
 */
struct ScopeInfo {
  /**
   * Keystone domain information.
   * Domains provide namespace isolation in Keystone.
   */
  struct domain_t {
    static constexpr uint8_t kEncV = 1;
    std::string id;
    std::string name;

    void encode(bufferlist &bl) const {
      ENCODE_START(kEncV, kEncV, bl);
      encode(id, bl);
      encode(name, bl);
      ENCODE_FINISH(bl);
    }
    void decode(bufferlist::const_iterator &p) {
      DECODE_START(kEncV, p);
      decode(id, p);
      decode(name, p);
      DECODE_FINISH(p);
    }
  };

  /**
   * Keystone project (tenant) information.
   * Projects are the primary authorization scope in Keystone.
   */
  struct project_t {
    static constexpr uint8_t kEncV = 1;
    std::string id;
    std::string name;
    domain_t domain;

    void encode(bufferlist &bl) const {
      ENCODE_START(kEncV, kEncV, bl);
      encode(id, bl);
      encode(name, bl);
      domain.encode(bl);
      ENCODE_FINISH(bl);
    }
    void decode(bufferlist::const_iterator &p) {
      DECODE_START(kEncV, p);
      decode(id, p);
      decode(name, p);
      domain.decode(p);
      DECODE_FINISH(p);
    }
  };

  /**
   * Keystone user information.
   * Optional based on rgw_keystone_scope_include_user configuration.
   */
  struct user_t {
    static constexpr uint8_t kEncV = 1;
    std::string id;
    std::string name;
    domain_t domain;

    void encode(bufferlist &bl) const {
      ENCODE_START(kEncV, kEncV, bl);
      encode(id, bl);
      encode(name, bl);
      domain.encode(bl);
      ENCODE_FINISH(bl);
    }
    void decode(bufferlist::const_iterator &p) {
      DECODE_START(kEncV, p);
      decode(id, p);
      decode(name, p);
      domain.decode(p);
      DECODE_FINISH(p);
    }
  };

  /**
   * Keystone application credential information.
   * Only present when authentication used application credentials.
   */
  struct app_cred_t {
    static constexpr uint8_t kEncV = 1;
    std::string id;
    std::string name;
    bool restricted;

    void encode(bufferlist &bl) const {
      ENCODE_START(kEncV, kEncV, bl);
      encode(id, bl);
      encode(name, bl);
      encode(restricted, bl);
      ENCODE_FINISH(bl);
    }
    void decode(bufferlist::const_iterator &p) {
      DECODE_START(kEncV, p);
      decode(id, p);
      decode(name, p);
      decode(restricted, p);
      DECODE_FINISH(p);
    }
  };

  // Fields
  project_t project;                        // Always present
  std::optional<user_t> user;               // Optional (controlled by include_user config)
  std::vector<std::string> roles;           // May be empty (controlled by include_roles config)
  std::optional<app_cred_t> app_cred;       // Optional (present only for app cred auth)

  // Serialization for RADOS backend
  static constexpr uint8_t kEncV = 1;
  void encode(bufferlist &bl) const;
  void decode(bufferlist::const_iterator &p);

  // JSON formatting for file/socket backend
  void dump(ceph::Formatter *f) const;
};

// Free function wrappers at namespace level for std::optional encoding support
inline void encode(const ScopeInfo::domain_t& v, bufferlist& bl) { v.encode(bl); }
inline void decode(ScopeInfo::domain_t& v, bufferlist::const_iterator& p) { v.decode(p); }
inline void encode(const ScopeInfo::project_t& v, bufferlist& bl) { v.encode(bl); }
inline void decode(ScopeInfo::project_t& v, bufferlist::const_iterator& p) { v.decode(p); }
inline void encode(const ScopeInfo::user_t& v, bufferlist& bl) { v.encode(bl); }
inline void decode(ScopeInfo::user_t& v, bufferlist::const_iterator& p) { v.decode(p); }
inline void encode(const ScopeInfo::app_cred_t& v, bufferlist& bl) { v.encode(bl); }
inline void decode(ScopeInfo::app_cred_t& v, bufferlist::const_iterator& p) { v.decode(p); }
inline void encode(const ScopeInfo& v, bufferlist& bl) { v.encode(bl); }
inline void decode(ScopeInfo& v, bufferlist::const_iterator& p) { v.decode(p); }

// ScopeInfo encode/decode implementations (defined after free functions for visibility)
inline void ScopeInfo::encode(bufferlist &bl) const {
  ENCODE_START(kEncV, kEncV, bl);
  encode(project, bl);
  encode(user, bl);
  encode(roles, bl);
  encode(app_cred, bl);
  ENCODE_FINISH(bl);
}

inline void ScopeInfo::decode(bufferlist::const_iterator &p) {
  DECODE_START(kEncV, p);
  decode(project, p);
  decode(user, p);
  decode(roles, p);
  decode(app_cred, p);
  DECODE_FINISH(p);
}

/**
 * Build ScopeInfo from Keystone token and configuration.
 *
 * This helper function eliminates code duplication between TokenEngine
 * and EC2Engine by centralizing the scope building logic.
 *
 * @param cct CephContext for configuration access
 * @param token TokenEnvelope containing Keystone authentication data
 * @return ScopeInfo if scope logging enabled, nullopt otherwise
 */
std::optional<ScopeInfo> build_scope_info(
    CephContext* cct,
    const TokenEnvelope& token);

} // namespace rgw::keystone
