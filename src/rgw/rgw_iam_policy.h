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

enum {
  s3GetObject,
  s3GetObjectVersion,
  s3PutObject,
  s3GetObjectAcl,
  s3GetObjectVersionAcl,
  s3PutObjectAcl,
  s3PutObjectVersionAcl,
  s3DeleteObject,
  s3DeleteObjectVersion,
  s3ListMultipartUploadParts,
  s3AbortMultipartUpload,
  s3GetObjectTorrent,
  s3GetObjectVersionTorrent,
  s3RestoreObject,
  s3CreateBucket,
  s3DeleteBucket,
  s3ListBucket,
  s3ListBucketVersions,
  s3ListAllMyBuckets,
  s3ListBucketMultipartUploads,
  s3GetAccelerateConfiguration,
  s3PutAccelerateConfiguration,
  s3GetBucketAcl,
  s3PutBucketAcl,
  s3GetBucketOwnershipControls,
  s3PutBucketOwnershipControls,
  s3GetBucketCORS,
  s3PutBucketCORS,
  s3GetBucketVersioning,
  s3PutBucketVersioning,
  s3GetBucketRequestPayment,
  s3PutBucketRequestPayment,
  s3GetBucketLocation,
  s3GetBucketPolicy,
  s3DeleteBucketPolicy,
  s3PutBucketPolicy,
  s3GetBucketNotification,
  s3PutBucketNotification,
  s3GetBucketLogging,
  s3PutBucketLogging,
  s3GetBucketTagging,
  s3PutBucketTagging,
  s3GetBucketWebsite,
  s3PutBucketWebsite,
  s3DeleteBucketWebsite,
  s3GetLifecycleConfiguration,
  s3PutLifecycleConfiguration,
  s3PutReplicationConfiguration,
  s3GetReplicationConfiguration,
  s3DeleteReplicationConfiguration,
  s3GetObjectTagging,
  s3PutObjectTagging,
  s3DeleteObjectTagging,
  s3GetObjectVersionTagging,
  s3PutObjectVersionTagging,
  s3DeleteObjectVersionTagging,
  s3PutBucketObjectLockConfiguration,
  s3GetBucketObjectLockConfiguration,
  s3PutObjectRetention,
  s3GetObjectRetention,
  s3PutObjectLegalHold,
  s3GetObjectLegalHold,
  s3BypassGovernanceRetention,
  s3GetBucketPolicyStatus,
  s3PutPublicAccessBlock,
  s3GetPublicAccessBlock,
  s3DeletePublicAccessBlock,
  s3GetBucketPublicAccessBlock,
  s3PutBucketPublicAccessBlock,
  s3DeleteBucketPublicAccessBlock,
  s3GetBucketEncryption,
  s3PutBucketEncryption,
  s3DescribeJob,
  s3All,

  s3objectlambdaGetObject,
  s3objectlambdaListBucket,
  s3objectlambdaAll,

  iamPutUserPolicy,
  iamGetUserPolicy,
  iamDeleteUserPolicy,
  iamListUserPolicies,
  iamAttachUserPolicy,
  iamDetachUserPolicy,
  iamListAttachedUserPolicies,
  iamCreateRole,
  iamDeleteRole,
  iamModifyRoleTrustPolicy,
  iamGetRole,
  iamListRoles,
  iamPutRolePolicy,
  iamGetRolePolicy,
  iamListRolePolicies,
  iamDeleteRolePolicy,
  iamAttachRolePolicy,
  iamDetachRolePolicy,
  iamListAttachedRolePolicies,
  iamCreateOIDCProvider,
  iamDeleteOIDCProvider,
  iamGetOIDCProvider,
  iamListOIDCProviders,
  iamTagRole,
  iamListRoleTags,
  iamUntagRole,
  iamUpdateRole,
  iamCreateUser,
  iamGetUser,
  iamUpdateUser,
  iamDeleteUser,
  iamListUsers,
  iamCreateAccessKey,
  iamUpdateAccessKey,
  iamDeleteAccessKey,
  iamListAccessKeys,
  iamCreateGroup,
  iamGetGroup,
  iamUpdateGroup,
  iamDeleteGroup,
  iamListGroups,
  iamAddUserToGroup,
  iamRemoveUserFromGroup,
  iamListGroupsForUser,
  iamPutGroupPolicy,
  iamGetGroupPolicy,
  iamListGroupPolicies,
  iamDeleteGroupPolicy,
  iamAttachGroupPolicy,
  iamDetachGroupPolicy,
  iamListAttachedGroupPolicies,
  iamGenerateCredentialReport,
  iamGenerateServiceLastAccessedDetails,
  iamSimulateCustomPolicy,
  iamSimulatePrincipalPolicy,
  iamAll,

  stsAssumeRole,
  stsAssumeRoleWithWebIdentity,
  stsGetSessionToken,
  stsTagSession,
  stsAll,

  snsGetTopicAttributes,
  snsDeleteTopic,
  snsPublish,
  snsSetTopicAttributes,
  snsCreateTopic,
  snsListTopics,
  snsAll,

  organizationsDescribeAccount,
  organizationsDescribeOrganization,
  organizationsDescribeOrganizationalUnit,
  organizationsDescribePolicy,
  organizationsListChildren,
  organizationsListParents,
  organizationsListPoliciesForTarget,
  organizationsListRoots,
  organizationsListPolicies,
  organizationsListTargetsForPolicy,
  organizationsAll,

  allCount
};

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
static const Action_t s3objectlambdaAllValue = set_cont_bits<allCount>(s3All+1,s3objectlambdaAll);
static const Action_t iamAllValue = set_cont_bits<allCount>(s3objectlambdaAll+1,iamAll);
static const Action_t stsAllValue = set_cont_bits<allCount>(iamAll+1,stsAll);
static const Action_t snsAllValue = set_cont_bits<allCount>(stsAll+1, snsAll);
static const Action_t organizationsAllValue = set_cont_bits<allCount>(snsAll+1,organizationsAll);
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
  Policy(CephContext* cct, const std::string* tenant,
	 std::string text,
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
