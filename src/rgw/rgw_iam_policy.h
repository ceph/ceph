// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_RGW_IAM_POLICY_H
#define CEPH_RGW_IAM_POLICY_H

#include <bitset>
#include <chrono>
#include <cstdint>
#include <iostream>
#include <string>
#include <bitset>

#include <boost/algorithm/string/predicate.hpp>
#include <boost/container/flat_map.hpp>
#include <boost/container/flat_set.hpp>
#include <boost/optional.hpp>
#include <boost/thread/shared_mutex.hpp>
#include <boost/utility/string_ref.hpp>
#include <boost/variant.hpp>

#include "common/ceph_time.h"
#include "common/iso_8601.h"

#include "rapidjson/error/error.h"
#include "rapidjson/error/en.h"

#include "rgw_acl.h"
#include "rgw_basic_types.h"
#include "rgw_iam_policy_keywords.h"
#include "rgw_string.h"

class RGWRados;
namespace rgw {
namespace auth {
class Identity;
}
}
struct rgw_obj;
struct rgw_bucket;

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
static constexpr std::uint64_t s3All = 54;

static constexpr std::uint64_t iamPutUserPolicy = 55;
static constexpr std::uint64_t iamGetUserPolicy = 56;
static constexpr std::uint64_t iamDeleteUserPolicy = 57;
static constexpr std::uint64_t iamListUserPolicies = 58;
static constexpr std::uint64_t iamCreateRole = 59;
static constexpr std::uint64_t iamDeleteRole = 60;
static constexpr std::uint64_t iamModifyRole = 61;
static constexpr std::uint64_t iamGetRole = 62;
static constexpr std::uint64_t iamListRoles = 63;
static constexpr std::uint64_t iamPutRolePolicy = 64;
static constexpr std::uint64_t iamGetRolePolicy = 65;
static constexpr std::uint64_t iamListRolePolicies = 66;
static constexpr std::uint64_t iamDeleteRolePolicy = 67;
static constexpr std::uint64_t iamAll = 68;
static constexpr std::uint64_t stsAssumeRole = 69;
static constexpr std::uint64_t stsAssumeRoleWithWebIdentity = 70;
static constexpr std::uint64_t stsGetSessionToken = 71;
static constexpr std::uint64_t stsAll = 72;

static constexpr std::uint64_t s3Count = s3DeleteObjectVersionTagging + 1;
static constexpr std::uint64_t allCount = stsAll + 1;

using Action_t = bitset<allCount>;
using NotAction_t = Action_t;

static const Action_t None(0);
static const Action_t s3AllValue("111111111111111111111111111111111111111111111111111111");
static const Action_t iamAllValue("11111111111110000000000000000000000000000000000000000000000000000000");
static const Action_t stsAllValue("111000000000000000000000000000000000000000000000000000000000000000000000");
//Modify allValue if more Actions are added
static const Action_t allValue("1111111111111111111111111111111111111111111111111111111111111111111111111");

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
    return RGW_PERM_WRITE;

  case s3GetAccelerateConfiguration:
  case s3GetBucketAcl:
  case s3GetBucketCORS:
  case s3GetBucketLocation:
  case s3GetBucketLogging:
  case s3GetBucketNotification:
  case s3GetBucketPolicy:
  case s3GetBucketRequestPayment:
  case s3GetBucketTagging:
  case s3GetBucketVersioning:
  case s3GetBucketWebsite:
  case s3GetLifecycleConfiguration:
  case s3GetObjectAcl:
  case s3GetObjectVersionAcl:
  case s3GetReplicationConfiguration:
    return RGW_PERM_READ_ACP;

  case s3DeleteBucketPolicy:
  case s3DeleteBucketWebsite:
  case s3DeleteReplicationConfiguration:
  case s3PutAccelerateConfiguration:
  case s3PutBucketAcl:
  case s3PutBucketCORS:
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
    return RGW_PERM_WRITE_ACP;

  case s3All:
    return RGW_PERM_FULL_CONTROL;
  }
  return RGW_PERM_INVALID;
}
}

using Environment = boost::container::flat_map<std::string, std::string>;

enum struct Partition {
  aws, aws_cn, aws_us_gov, wildcard
  // If we wanted our own ARNs for principal type unique to us
  // (maybe to integrate better with Swift) or for anything else we
  // provide that doesn't map onto S3, we could add an 'rgw'
  // partition type.
};

enum struct Service {
  apigateway, appstream, artifact, autoscaling, aws_portal, acm,
  cloudformation, cloudfront, cloudhsm, cloudsearch, cloudtrail,
  cloudwatch, events, logs, codebuild, codecommit, codedeploy,
  codepipeline, cognito_idp, cognito_identity, cognito_sync,
  config, datapipeline, dms, devicefarm, directconnect,
  ds, dynamodb, ec2, ecr, ecs, ssm, elasticbeanstalk, elasticfilesystem,
  elasticloadbalancing, elasticmapreduce, elastictranscoder, elasticache,
  es, gamelift, glacier, health, iam, importexport, inspector, iot,
  kms, kinesisanalytics, firehose, kinesis, lambda, lightsail,
  machinelearning, aws_marketplace, aws_marketplace_management,
  mobileanalytics, mobilehub, opsworks, opsworks_cm, polly,
  redshift, rds, route53, route53domains, sts, servicecatalog,
  ses, sns, sqs, s3, swf, sdb, states, storagegateway, support,
  trustedadvisor, waf, workmail, workspaces, wildcard
};

struct ARN {
  Partition partition;
  Service service;
  std::string region;
  // Once we refit tenant, we should probably use that instead of a
  // string.
  std::string account;
  std::string resource;

  ARN()
    : partition(Partition::wildcard), service(Service::wildcard) {}
  ARN(Partition partition, Service service, std::string region,
      std::string account, std::string resource)
    : partition(partition), service(service), region(std::move(region)),
      account(std::move(account)), resource(std::move(resource)) {}
  ARN(const rgw_obj& o);
  ARN(const rgw_bucket& b);
  ARN(const rgw_bucket& b, const std::string& o);
  ARN(const string& resource_name, const string& type, const string& tenant, bool has_path=false);

  static boost::optional<ARN> parse(const std::string& s,
				    bool wildcard = false);
  std::string to_string() const;

  // `this` is the pattern
  bool match(const ARN& candidate) const;
};

inline std::string to_string(const ARN& a) {
  return a.to_string();
}

inline std::ostream& operator <<(std::ostream& m, const ARN& a) {
  return m << to_string(a);
}

bool operator ==(const ARN& l, const ARN& r);
bool operator <(const ARN& l, const ARN& r);

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

      return from_iso_8601(boost::string_ref(s), false);
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
      base64.decode_base64(bin);
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

  template<typename F>
  static bool orrible(F&& f, const std::string& c,
		      const std::vector<std::string>& v) {
    for (const auto& d : v) {
      if (std::forward<F>(f)(c, d)) {
	return true;
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
      auto xd = std::forward<X>(x)(d);
      if (!xd) {
	continue;
      }

      if (std::forward<F>(f)(*xc, *xd)) {
	return true;
      }
    }
    return false;
  }

  template <typename F>
  bool has_key_p(const std::string& _key, F p) const {
    return p(key, _key);
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
	      std::uint64_t action, const ARN& resource) const;

  Effect eval_principal(const Environment& e,
		       boost::optional<const rgw::auth::Identity&> ida) const;

  Effect eval_conditions(const Environment& e) const;
};

std::ostream& operator <<(ostream& m, const Statement& s);

struct PolicyParseException : public std::exception {
  rapidjson::ParseResult pr;

  explicit PolicyParseException(rapidjson::ParseResult&& pr)
    : pr(pr) { }
  const char* what() const noexcept override {
    return rapidjson::GetParseError_En(pr.Code());
  }
};

struct Policy {
  std::string text;
  Version version = Version::v2008_10_17;
  boost::optional<std::string> id = boost::none;

  std::vector<Statement> statements;

  Policy(CephContext* cct, const std::string& tenant,
	 const bufferlist& text);

  Effect eval(const Environment& e,
	      boost::optional<const rgw::auth::Identity&> ida,
	      std::uint64_t action, const ARN& resource) const;

  Effect eval_principal(const Environment& e,
	      boost::optional<const rgw::auth::Identity&> ida) const;

  Effect eval_conditions(const Environment& e) const;

  template <typename F>
  bool has_conditional(const string& conditional, F p) const {
    for (const auto&s: statements){
      if (std::any_of(s.conditions.begin(), s.conditions.end(),
		      [&](const Condition& c) { return c.has_key_p(conditional, p);}))
	return true;
    }
    return false;
  }

  bool has_conditional(const string& c) const {
    return has_conditional(c, Condition::ci_equal_to());
  }

  bool has_partial_conditional(const string& c) const {
    return has_conditional(c, Condition::ci_starts_with());
  }
};

std::ostream& operator <<(ostream& m, const Policy& p);
}
}

namespace std {
template<>
struct hash<::rgw::IAM::Service> {
  size_t operator()(const ::rgw::IAM::Service& s) const noexcept {
    // Invoke a default-constructed hash object for int.
    return hash<int>()(static_cast<int>(s));
  }
};
}

#endif
