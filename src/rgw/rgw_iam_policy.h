// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#pragma once

#include <bitset>
#include <chrono>
#include <cstdint>
#include <iostream>
#include <string>
#include <string_view>

#include <boost/algorithm/string/predicate.hpp>
#include <boost/container/flat_map.hpp>
#include <boost/container/flat_set.hpp>
#include <boost/optional.hpp>
#include <boost/thread/shared_mutex.hpp>
#include <boost/variant.hpp>

#include <fmt/format.h>

#include "common/ceph_time.h"
#include "common/iso_8601.h"

#include "rapidjson/error/error.h"
#include "rapidjson/error/en.h"

#include "rgw_acl.h"
#include "rgw_basic_types.h"
#include "rgw_iam_policy_keywords.h"
#include "rgw_string.h"
#include "rgw_arn.h"

namespace rgw {
namespace auth {
class Identity;
}
}

namespace rgw {
namespace IAM {

static constexpr std::uint64_t s3GetObject = 0;
static constexpr std::uint64_t s3GetObjectVersion = 1;
static constexpr std::uint64_t s3PutObject = 2;
static constexpr std::uint64_t s3GetObjectAcl = 3;
static constexpr std::uint64_t s3GetObjectVersionAcl = 4;
static constexpr std::uint64_t s3PutObjectAcl = 5;
static constexpr std::uint64_t s3PutObjectVersionAcl = 6;
static constexpr std::uint64_t s3DeleteObject = 7;
static constexpr std::uint64_t s3DeleteObjectVersion = 8;
static constexpr std::uint64_t s3ListMultipartUploadParts = 9;
static constexpr std::uint64_t s3AbortMultipartUpload = 10;
static constexpr std::uint64_t s3GetObjectTorrent = 11;
static constexpr std::uint64_t s3GetObjectVersionTorrent = 12;
static constexpr std::uint64_t s3RestoreObject = 13;
static constexpr std::uint64_t s3CreateBucket = 14;
static constexpr std::uint64_t s3DeleteBucket = 15;
static constexpr std::uint64_t s3ListBucket = 16;
static constexpr std::uint64_t s3ListBucketVersions = 17;
static constexpr std::uint64_t s3ListAllMyBuckets = 18;
static constexpr std::uint64_t s3ListBucketMultipartUploads = 19;
static constexpr std::uint64_t s3GetAccelerateConfiguration = 20;
static constexpr std::uint64_t s3PutAccelerateConfiguration = 21;
static constexpr std::uint64_t s3GetBucketAcl = 22;
static constexpr std::uint64_t s3PutBucketAcl = 23;
static constexpr std::uint64_t s3GetBucketCORS = 24;
static constexpr std::uint64_t s3PutBucketCORS = 25;
static constexpr std::uint64_t s3GetBucketVersioning = 26;
static constexpr std::uint64_t s3PutBucketVersioning = 27;
static constexpr std::uint64_t s3GetBucketRequestPayment = 28;
static constexpr std::uint64_t s3PutBucketRequestPayment = 29;
static constexpr std::uint64_t s3GetBucketLocation = 30;
static constexpr std::uint64_t s3GetBucketPolicy = 31;
static constexpr std::uint64_t s3DeleteBucketPolicy = 32;
static constexpr std::uint64_t s3PutBucketPolicy = 33;
static constexpr std::uint64_t s3GetBucketNotification = 34;
static constexpr std::uint64_t s3PutBucketNotification = 35;
static constexpr std::uint64_t s3GetBucketLogging = 36;
static constexpr std::uint64_t s3PutBucketLogging = 37;
static constexpr std::uint64_t s3GetBucketTagging = 38;
static constexpr std::uint64_t s3PutBucketTagging = 39;
static constexpr std::uint64_t s3GetBucketWebsite = 40;
static constexpr std::uint64_t s3PutBucketWebsite = 41;
static constexpr std::uint64_t s3DeleteBucketWebsite = 42;
static constexpr std::uint64_t s3GetLifecycleConfiguration = 43;
static constexpr std::uint64_t s3PutLifecycleConfiguration = 44;
static constexpr std::uint64_t s3PutReplicationConfiguration = 45;
static constexpr std::uint64_t s3GetReplicationConfiguration = 46;
static constexpr std::uint64_t s3DeleteReplicationConfiguration = 47;
static constexpr std::uint64_t s3GetObjectTagging = 48;
static constexpr std::uint64_t s3PutObjectTagging = 49;
static constexpr std::uint64_t s3DeleteObjectTagging = 50;
static constexpr std::uint64_t s3GetObjectVersionTagging = 51;
static constexpr std::uint64_t s3PutObjectVersionTagging = 52;
static constexpr std::uint64_t s3DeleteObjectVersionTagging = 53;
static constexpr std::uint64_t s3PutBucketObjectLockConfiguration = 54;
static constexpr std::uint64_t s3GetBucketObjectLockConfiguration = 55;
static constexpr std::uint64_t s3PutObjectRetention = 56;
static constexpr std::uint64_t s3GetObjectRetention = 57;
static constexpr std::uint64_t s3PutObjectLegalHold = 58;
static constexpr std::uint64_t s3GetObjectLegalHold = 59;
static constexpr std::uint64_t s3BypassGovernanceRetention = 60;
static constexpr std::uint64_t s3GetBucketPolicyStatus = 61;
static constexpr std::uint64_t s3PutPublicAccessBlock = 62;
static constexpr std::uint64_t s3GetPublicAccessBlock = 63;
static constexpr std::uint64_t s3DeletePublicAccessBlock = 64;
static constexpr std::uint64_t s3GetBucketPublicAccessBlock = 65;
static constexpr std::uint64_t s3PutBucketPublicAccessBlock = 66;
static constexpr std::uint64_t s3DeleteBucketPublicAccessBlock = 67;
static constexpr std::uint64_t s3GetBucketEncryption = 68;
static constexpr std::uint64_t s3PutBucketEncryption = 69;
static constexpr std::uint64_t s3All = 70;

static constexpr std::uint64_t iamPutUserPolicy = s3All + 1;
static constexpr std::uint64_t iamGetUserPolicy = s3All + 2;
static constexpr std::uint64_t iamDeleteUserPolicy = s3All + 3;
static constexpr std::uint64_t iamListUserPolicies = s3All + 4;
static constexpr std::uint64_t iamCreateRole = s3All + 5;
static constexpr std::uint64_t iamDeleteRole = s3All + 6;
static constexpr std::uint64_t iamModifyRoleTrustPolicy = s3All + 7;
static constexpr std::uint64_t iamGetRole = s3All + 8;
static constexpr std::uint64_t iamListRoles = s3All + 9;
static constexpr std::uint64_t iamPutRolePolicy = s3All + 10;
static constexpr std::uint64_t iamGetRolePolicy = s3All + 11;
static constexpr std::uint64_t iamListRolePolicies = s3All + 12;
static constexpr std::uint64_t iamDeleteRolePolicy = s3All + 13;
static constexpr std::uint64_t iamCreateOIDCProvider = s3All + 14;
static constexpr std::uint64_t iamDeleteOIDCProvider = s3All + 15;
static constexpr std::uint64_t iamGetOIDCProvider = s3All + 16;
static constexpr std::uint64_t iamListOIDCProviders = s3All + 17;
static constexpr std::uint64_t iamTagRole = s3All + 18;
static constexpr std::uint64_t iamListRoleTags = s3All + 19;
static constexpr std::uint64_t iamUntagRole = s3All + 20;
static constexpr std::uint64_t iamUpdateRole = s3All + 21;
static constexpr std::uint64_t iamAll = s3All + 22;

static constexpr std::uint64_t stsAssumeRole = iamAll + 1;
static constexpr std::uint64_t stsAssumeRoleWithWebIdentity = iamAll + 2;
static constexpr std::uint64_t stsGetSessionToken = iamAll + 3;
static constexpr std::uint64_t stsTagSession = iamAll + 4;
static constexpr std::uint64_t stsAll = iamAll + 5;

static constexpr std::uint64_t s3Count = s3All;
static constexpr std::uint64_t allCount = stsAll + 1;

using Action_t = std::bitset<allCount>;
using NotAction_t = Action_t;

template <size_t N>
constexpr std::bitset<N> make_bitmask(size_t s) {
  // unfortunately none of the shift/logic operators of std::bitset have a constexpr variation
  return s < 64 ? std::bitset<N> ((1ULL << s) - 1) :
    std::bitset<N>((1ULL << 63) - 1) | make_bitmask<N> (s - 63) << 63;
}

template <size_t N>
constexpr std::bitset<N> set_cont_bits(size_t start, size_t end)
{
  return (make_bitmask<N>(end - start)) << start;
}

static const Action_t None(0);
static const Action_t s3AllValue = set_cont_bits<allCount>(0,s3All);
static const Action_t iamAllValue = set_cont_bits<allCount>(s3All+1,iamAll);
static const Action_t stsAllValue = set_cont_bits<allCount>(iamAll+1,stsAll);
static const Action_t allValue = set_cont_bits<allCount>(0,allCount);

namespace {
// Please update the table in doc/radosgw/s3/authentication.rst if you
// modify this function.
inline int op_to_perm(std::uint64_t op) {
  switch (op) {
  case s3GetObject:
  case s3GetObjectTorrent:
  case s3GetObjectVersion:
  case s3GetObjectVersionTorrent:
  case s3GetObjectTagging:
  case s3GetObjectVersionTagging:
  case s3GetObjectRetention:
  case s3GetObjectLegalHold:
  case s3ListAllMyBuckets:
  case s3ListBucket:
  case s3ListBucketMultipartUploads:
  case s3ListBucketVersions:
  case s3ListMultipartUploadParts:
    return RGW_PERM_READ;

  case s3AbortMultipartUpload:
  case s3CreateBucket:
  case s3DeleteBucket:
  case s3DeleteObject:
  case s3DeleteObjectVersion:
  case s3PutObject:
  case s3PutObjectTagging:
  case s3PutObjectVersionTagging:
  case s3DeleteObjectTagging:
  case s3DeleteObjectVersionTagging:
  case s3RestoreObject:
  case s3PutObjectRetention:
  case s3PutObjectLegalHold:
  case s3BypassGovernanceRetention:
    return RGW_PERM_WRITE;

  case s3GetAccelerateConfiguration:
  case s3GetBucketAcl:
  case s3GetBucketCORS:
  case s3GetBucketEncryption:
  case s3GetBucketLocation:
  case s3GetBucketLogging:
  case s3GetBucketNotification:
  case s3GetBucketPolicy:
  case s3GetBucketPolicyStatus:
  case s3GetBucketRequestPayment:
  case s3GetBucketTagging:
  case s3GetBucketVersioning:
  case s3GetBucketWebsite:
  case s3GetLifecycleConfiguration:
  case s3GetObjectAcl:
  case s3GetObjectVersionAcl:
  case s3GetReplicationConfiguration:
  case s3GetBucketObjectLockConfiguration:
  case s3GetBucketPublicAccessBlock:
    return RGW_PERM_READ_ACP;

  case s3DeleteBucketPolicy:
  case s3DeleteBucketWebsite:
  case s3DeleteReplicationConfiguration:
  case s3PutAccelerateConfiguration:
  case s3PutBucketAcl:
  case s3PutBucketCORS:
  case s3PutBucketEncryption:
  case s3PutBucketLogging:
  case s3PutBucketNotification:
  case s3PutBucketPolicy:
  case s3PutBucketRequestPayment:
  case s3PutBucketTagging:
  case s3PutBucketVersioning:
  case s3PutBucketWebsite:
  case s3PutLifecycleConfiguration:
  case s3PutObjectAcl:
  case s3PutObjectVersionAcl:
  case s3PutReplicationConfiguration:
  case s3PutBucketObjectLockConfiguration:
  case s3PutBucketPublicAccessBlock:
    return RGW_PERM_WRITE_ACP;

  case s3All:
    return RGW_PERM_FULL_CONTROL;
  }
  return RGW_PERM_INVALID;
}

inline const char* action_bit_string(uint64_t action) {
  switch (action) {
  case s3GetObject:
    return "s3:GetObject";

  case s3GetObjectVersion:
    return "s3:GetObjectVersion";

  case s3PutObject:
    return "s3:PutObject";

  case s3GetObjectAcl:
    return "s3:GetObjectAcl";

  case s3GetObjectVersionAcl:
    return "s3:GetObjectVersionAcl";

  case s3PutObjectAcl:
    return "s3:PutObjectAcl";

  case s3PutObjectVersionAcl:
    return "s3:PutObjectVersionAcl";

  case s3DeleteObject:
    return "s3:DeleteObject";

  case s3DeleteObjectVersion:
    return "s3:DeleteObjectVersion";

  case s3ListMultipartUploadParts:
    return "s3:ListMultipartUploadParts";

  case s3AbortMultipartUpload:
    return "s3:AbortMultipartUpload";

  case s3GetObjectTorrent:
    return "s3:GetObjectTorrent";

  case s3GetObjectVersionTorrent:
    return "s3:GetObjectVersionTorrent";

  case s3RestoreObject:
    return "s3:RestoreObject";

  case s3CreateBucket:
    return "s3:CreateBucket";

  case s3DeleteBucket:
    return "s3:DeleteBucket";

  case s3ListBucket:
    return "s3:ListBucket";

  case s3ListBucketVersions:
    return "s3:ListBucketVersions";
  case s3ListAllMyBuckets:
    return "s3:ListAllMyBuckets";

  case s3ListBucketMultipartUploads:
    return "s3:ListBucketMultipartUploads";

  case s3GetAccelerateConfiguration:
    return "s3:GetAccelerateConfiguration";

  case s3PutAccelerateConfiguration:
    return "s3:PutAccelerateConfiguration";

  case s3GetBucketAcl:
    return "s3:GetBucketAcl";

  case s3PutBucketAcl:
    return "s3:PutBucketAcl";

  case s3GetBucketCORS:
    return "s3:GetBucketCORS";

  case s3PutBucketCORS:
    return "s3:PutBucketCORS";

  case s3GetBucketEncryption:
    return "s3:GetBucketEncryption";

  case s3PutBucketEncryption:
    return "s3:PutBucketEncryption";

  case s3GetBucketVersioning:
    return "s3:GetBucketVersioning";

  case s3PutBucketVersioning:
    return "s3:PutBucketVersioning";

  case s3GetBucketRequestPayment:
    return "s3:GetBucketRequestPayment";

  case s3PutBucketRequestPayment:
    return "s3:PutBucketRequestPayment";

  case s3GetBucketLocation:
    return "s3:GetBucketLocation";

  case s3GetBucketPolicy:
    return "s3:GetBucketPolicy";

  case s3DeleteBucketPolicy:
    return "s3:DeleteBucketPolicy";

  case s3PutBucketPolicy:
    return "s3:PutBucketPolicy";

  case s3GetBucketNotification:
    return "s3:GetBucketNotification";

  case s3PutBucketNotification:
    return "s3:PutBucketNotification";

  case s3GetBucketLogging:
    return "s3:GetBucketLogging";

  case s3PutBucketLogging:
    return "s3:PutBucketLogging";

  case s3GetBucketTagging:
    return "s3:GetBucketTagging";

  case s3PutBucketTagging:
    return "s3:PutBucketTagging";

  case s3GetBucketWebsite:
    return "s3:GetBucketWebsite";

  case s3PutBucketWebsite:
    return "s3:PutBucketWebsite";

  case s3DeleteBucketWebsite:
    return "s3:DeleteBucketWebsite";

  case s3GetLifecycleConfiguration:
    return "s3:GetLifecycleConfiguration";

  case s3PutLifecycleConfiguration:
    return "s3:PutLifecycleConfiguration";

  case s3PutReplicationConfiguration:
    return "s3:PutReplicationConfiguration";

  case s3GetReplicationConfiguration:
    return "s3:GetReplicationConfiguration";

  case s3DeleteReplicationConfiguration:
    return "s3:DeleteReplicationConfiguration";

  case s3PutObjectTagging:
    return "s3:PutObjectTagging";

  case s3PutObjectVersionTagging:
    return "s3:PutObjectVersionTagging";

  case s3GetObjectTagging:
    return "s3:GetObjectTagging";

  case s3GetObjectVersionTagging:
    return "s3:GetObjectVersionTagging";

  case s3DeleteObjectTagging:
    return "s3:DeleteObjectTagging";

  case s3DeleteObjectVersionTagging:
    return "s3:DeleteObjectVersionTagging";

  case s3PutBucketObjectLockConfiguration:
    return "s3:PutBucketObjectLockConfiguration";

  case s3GetBucketObjectLockConfiguration:
    return "s3:GetBucketObjectLockConfiguration";

  case s3PutObjectRetention:
    return "s3:PutObjectRetention";

  case s3GetObjectRetention:
    return "s3:GetObjectRetention";

  case s3PutObjectLegalHold:
    return "s3:PutObjectLegalHold";

  case s3GetObjectLegalHold:
    return "s3:GetObjectLegalHold";

  case s3BypassGovernanceRetention:
    return "s3:BypassGovernanceRetention";

  case iamPutUserPolicy:
    return "iam:PutUserPolicy";

  case iamGetUserPolicy:
    return "iam:GetUserPolicy";

  case iamListUserPolicies:
    return "iam:ListUserPolicies";

  case iamDeleteUserPolicy:
    return "iam:DeleteUserPolicy";

  case iamCreateRole:
    return "iam:CreateRole";

  case iamDeleteRole:
    return "iam:DeleteRole";

  case iamGetRole:
    return "iam:GetRole";

  case iamModifyRoleTrustPolicy:
    return "iam:ModifyRoleTrustPolicy";

  case iamListRoles:
    return "iam:ListRoles";

  case iamPutRolePolicy:
    return "iam:PutRolePolicy";

  case iamGetRolePolicy:
    return "iam:GetRolePolicy";

  case iamListRolePolicies:
    return "iam:ListRolePolicies";

  case iamDeleteRolePolicy:
    return "iam:DeleteRolePolicy";

  case iamCreateOIDCProvider:
    return "iam:CreateOIDCProvider";

  case iamDeleteOIDCProvider:
    return "iam:DeleteOIDCProvider";

  case iamGetOIDCProvider:
    return "iam:GetOIDCProvider";

  case iamListOIDCProviders:
    return "iam:ListOIDCProviders";

  case iamTagRole:
    return "iam:TagRole";

  case iamListRoleTags:
    return "iam:ListRoleTags";

  case iamUntagRole:
    return "iam:UntagRole";

  case iamUpdateRole:
    return "iam:UpdateRole";

  case stsAssumeRole:
    return "sts:AssumeRole";

  case stsAssumeRoleWithWebIdentity:
    return "sts:AssumeRoleWithWebIdentity";

  case stsGetSessionToken:
    return "sts:GetSessionToken";

  case stsTagSession:
    return "sts:TagSession";
  }
  return "s3Invalid";
}
}

enum class PolicyPrincipal {
  Role,
  Session,
  Other
};

using Environment = std::unordered_multimap<std::string, std::string>;

using Address = std::bitset<128>;
struct MaskedIP {
  bool v6;
  Address addr;
  // Since we're mapping IPv6 to IPv4 addresses, we may want to
  // consider making the prefix always be in terms of a v6 address
  // and just use the v6 bit to rewrite it as a v4 prefix for
  // output.
  unsigned int prefix;
};

std::ostream& operator <<(std::ostream& m, const MaskedIP& ip);

inline bool operator ==(const MaskedIP& l, const MaskedIP& r) {
  auto shift = std::max((l.v6 ? 128 : 32) - ((int) l.prefix),
			(r.v6 ? 128 : 32) - ((int) r.prefix));
  ceph_assert(shift >= 0);
  return (l.addr >> shift) == (r.addr >> shift);
}

struct Condition {
  TokenID op;
  // Originally I was going to use a perfect hash table, but Marcus
  // says keys are to be added at run-time not compile time.

  // In future development, use symbol internment.
  std::string key;
  bool ifexists = false;
  bool isruntime = false; //Is evaluated during run-time
  // Much to my annoyance there is no actual way to do this in a
  // typed way that is compatible with AWS. I know this because I've
  // seen examples where the same value is used as a string in one
  // context and a date in another.
  std::vector<std::string> vals;

  Condition() = default;
  Condition(TokenID op, const char* s, std::size_t len, bool ifexists)
    : op(op), key(s, len), ifexists(ifexists) {}

  bool eval(const Environment& e) const;

  static boost::optional<double> as_number(const std::string& s) {
    std::size_t p = 0;

    try {
      double d = std::stod(s, &p);
      if (p < s.length()) {
	return boost::none;
      }

      return d;
    } catch (const std::logic_error& e) {
      return boost::none;
    }
  }

  static boost::optional<ceph::real_time> as_date(const std::string& s) {
    std::size_t p = 0;

    try {
      double d = std::stod(s, &p);
      if (p == s.length()) {
	return ceph::real_time(
	  std::chrono::seconds(static_cast<uint64_t>(d)) +
	  std::chrono::nanoseconds(
	    static_cast<uint64_t>((d - static_cast<uint64_t>(d))
				  * 1000000000)));
      }

      return from_iso_8601(std::string_view(s), false);
    } catch (const std::logic_error& e) {
      return boost::none;
    }
  }

  static boost::optional<bool> as_bool(const std::string& s) {
    std::size_t p = 0;

    if (s.empty() || boost::iequals(s, "false")) {
      return false;
    }

    try {
      double d = std::stod(s, &p);
      if (p == s.length()) {
	return !((d == +0.0) || (d == -0.0) || std::isnan(d));
      }
    } catch (const std::logic_error& e) {
      // Fallthrough
    }

    return true;
  }

  static boost::optional<ceph::bufferlist> as_binary(const std::string& s) {
    // In a just world
    ceph::bufferlist base64;
    // I could populate a bufferlist
    base64.push_back(buffer::create_static(
		       s.length(),
		       const_cast<char*>(s.data()))); // Yuck
    // From a base64 encoded std::string.
    ceph::bufferlist bin;

    try {
      bin.decode_base64(base64);
    } catch (const ceph::buffer::malformed_input& e) {
      return boost::none;
    }
    return bin;
  }

  static boost::optional<MaskedIP> as_network(const std::string& s);


  struct ci_equal_to {
    bool operator ()(const std::string& s1,
		     const std::string& s2) const {
      return boost::iequals(s1, s2);
    }
  };

  struct string_like {
    bool operator ()(const std::string& input,
                     const std::string& pattern) const {
      return match_wildcards(pattern, input, 0);
    }
  };

  struct ci_starts_with {
    bool operator()(const std::string& s1,
		    const std::string& s2) const {
      return boost::istarts_with(s1, s2);
    }
  };

  using unordered_multimap_it_pair = std::pair <std::unordered_multimap<std::string,std::string>::const_iterator, std::unordered_multimap<std::string,std::string>::const_iterator>;

  template<typename F>
  static bool andible(F&& f, const unordered_multimap_it_pair& it,
		      const std::vector<std::string>& v) {
    for (auto itr = it.first; itr != it.second; itr++) {
      bool matched = false;
      for (const auto& d : v) {
        if (f(itr->second, d)) {
	        matched = true;
      }
     }
     if (!matched)
      return false;
    }
    return true;
  }

  template<typename F>
  static bool orrible(F&& f, const unordered_multimap_it_pair& it,
		      const std::vector<std::string>& v) {
    for (auto itr = it.first; itr != it.second; itr++) {
      for (const auto& d : v) {
        if (f(itr->second, d)) {
	        return true;
      }
     }
    }
    return false;
  }

  template<typename F, typename X>
  static bool shortible(F&& f, X& x, const std::string& c,
			const std::vector<std::string>& v) {
    auto xc = std::forward<X>(x)(c);
    if (!xc) {
      return false;
    }

    for (const auto& d : v) {
      auto xd = x(d);
      if (!xd) {
        continue;
      }

      if (f(*xc, *xd)) {
        return true;
      }
    }
    return false;
  }

  template <typename F>
  bool has_key_p(const std::string& _key, F p) const {
    return p(key, _key);
  }

  template <typename F>
  bool has_val_p(const std::string& _val, F p) const {
    for (auto val : vals) {
      if (p(val, _val))
        return true;
    }
    return false;
  }
};

std::ostream& operator <<(std::ostream& m, const Condition& c);

struct Statement {
  boost::optional<std::string> sid = boost::none;

  boost::container::flat_set<rgw::auth::Principal> princ;
  boost::container::flat_set<rgw::auth::Principal> noprinc;

  // Every statement MUST provide an effect. I just initialize it to
  // deny as defensive programming.
  Effect effect = Effect::Deny;

  Action_t action = 0;
  NotAction_t notaction = 0;

  boost::container::flat_set<ARN> resource;
  boost::container::flat_set<ARN> notresource;

  std::vector<Condition> conditions;

  Effect eval(const Environment& e,
	      boost::optional<const rgw::auth::Identity&> ida,
	      std::uint64_t action, boost::optional<const ARN&> resource, boost::optional<PolicyPrincipal&> princ_type=boost::none) const;

  Effect eval_principal(const Environment& e,
		       boost::optional<const rgw::auth::Identity&> ida, boost::optional<PolicyPrincipal&> princ_type=boost::none) const;

  Effect eval_conditions(const Environment& e) const;
};

std::ostream& operator <<(std::ostream& m, const Statement& s);

struct PolicyParseException : public std::exception {
  rapidjson::ParseResult pr;
  std::string msg;

  explicit PolicyParseException(const rapidjson::ParseResult pr,
				const std::string& annotation)
    : pr(pr),
      msg(fmt::format("At character offset {}, {}",
		      pr.Offset(),
		      (pr.Code() == rapidjson::kParseErrorTermination ?
		       annotation :
		       rapidjson::GetParseError_En(pr.Code())))) {}

  const char* what() const noexcept override {
    return msg.c_str();
  }
};

struct Policy {
  std::string text;
  Version version = Version::v2008_10_17;
  boost::optional<std::string> id = boost::none;

  std::vector<Statement> statements;

  // reject_invalid_principals should be set to
  // `cct->_conf.get_val<bool>("rgw_policy_reject_invalid_principals")`
  // when executing operations that *set* a bucket policy, but should
  // be false when reading a stored bucket policy so as not to break
  // backwards configuration.
  Policy(CephContext* cct, const std::string& tenant,
	 const bufferlist& text,
	 bool reject_invalid_principals);

  Effect eval(const Environment& e,
	      boost::optional<const rgw::auth::Identity&> ida,
	      std::uint64_t action, boost::optional<const ARN&> resource, boost::optional<PolicyPrincipal&> princ_type=boost::none) const;

  Effect eval_principal(const Environment& e,
	      boost::optional<const rgw::auth::Identity&> ida, boost::optional<PolicyPrincipal&> princ_type=boost::none) const;

  Effect eval_conditions(const Environment& e) const;

  template <typename F>
  bool has_conditional(const std::string& conditional, F p) const {
    for (const auto&s: statements){
      if (std::any_of(s.conditions.begin(), s.conditions.end(),
		      [&](const Condition& c) { return c.has_key_p(conditional, p);}))
	return true;
    }
    return false;
  }

  template <typename F>
  bool has_conditional_value(const std::string& conditional, F p) const {
    for (const auto&s: statements){
      if (std::any_of(s.conditions.begin(), s.conditions.end(),
		      [&](const Condition& c) { return c.has_val_p(conditional, p);}))
	    return true;
    }
    return false;
  }

  bool has_conditional(const std::string& c) const {
    return has_conditional(c, Condition::ci_equal_to());
  }

  bool has_partial_conditional(const std::string& c) const {
    return has_conditional(c, Condition::ci_starts_with());
  }

  // Example: ${s3:ResourceTag}
  bool has_partial_conditional_value(const std::string& c) const {
    return has_conditional_value(c, Condition::ci_starts_with());
  }
};

std::ostream& operator <<(std::ostream& m, const Policy& p);
bool is_public(const Policy& p);

}
}
