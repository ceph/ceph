// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_RGW_IAM_POLICY_H
#define CEPH_RGW_IAM_POLICY_H

#include <bitset>
#include <chrono>
#include <cstdint>
#include <iostream>
#include <string>

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

#include "fnmatch.h"

#include "rgw_basic_types.h"
#include "rgw_iam_policy_keywords.h"

#include "include/assert.h" // razzin' frazzin' ...grrr.

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
static constexpr std::uint64_t s3None = 0;
static constexpr std::uint64_t s3GetObject = 1ULL << 0;
static constexpr std::uint64_t s3GetObjectVersion = 1ULL << 1;
static constexpr std::uint64_t s3PutObject = 1ULL << 2;
static constexpr std::uint64_t s3GetObjectAcl = 1ULL << 3;
static constexpr std::uint64_t s3GetObjectVersionAcl = 1ULL << 4;
static constexpr std::uint64_t s3PutObjectAcl = 1ULL << 5;
static constexpr std::uint64_t s3PutObjectVersionAcl = 1ULL << 6;
static constexpr std::uint64_t s3DeleteObject = 1ULL << 7;
static constexpr std::uint64_t s3DeleteObjectVersion = 1ULL << 8;
static constexpr std::uint64_t s3ListMultipartUploadParts = 1ULL << 9;
static constexpr std::uint64_t s3AbortMultipartUpload = 1ULL << 10;
static constexpr std::uint64_t s3GetObjectTorrent = 1ULL << 11;
static constexpr std::uint64_t s3GetObjectVersionTorrent = 1ULL << 12;
static constexpr std::uint64_t s3RestoreObject = 1ULL << 13;
static constexpr std::uint64_t s3CreateBucket = 1ULL << 14;
static constexpr std::uint64_t s3DeleteBucket = 1ULL << 15;
static constexpr std::uint64_t s3ListBucket = 1ULL << 16;
static constexpr std::uint64_t s3ListBucketVersions = 1ULL << 17;
static constexpr std::uint64_t s3ListAllMyBuckets = 1ULL << 18;
static constexpr std::uint64_t s3ListBucketMultiPartUploads = 1ULL << 19;
static constexpr std::uint64_t s3GetAccelerateConfiguration = 1ULL << 20;
static constexpr std::uint64_t s3PutAccelerateConfiguration = 1ULL << 21;
static constexpr std::uint64_t s3GetBucketAcl = 1ULL << 22;
static constexpr std::uint64_t s3PutBucketAcl = 1ULL << 23;
static constexpr std::uint64_t s3GetBucketCORS = 1ULL << 24;
static constexpr std::uint64_t s3PutBucketCORS = 1ULL << 25;
static constexpr std::uint64_t s3GetBucketVersioning = 1ULL << 26;
static constexpr std::uint64_t s3PutBucketVersioning = 1ULL << 27;
static constexpr std::uint64_t s3GetBucketRequestPayment = 1ULL << 28;
static constexpr std::uint64_t s3PutBucketRequestPayment = 1ULL << 29;
static constexpr std::uint64_t s3GetBucketLocation = 1ULL << 30;
static constexpr std::uint64_t s3GetBucketPolicy = 1ULL << 31;
static constexpr std::uint64_t s3DeleteBucketPolicy = 1ULL << 32;
static constexpr std::uint64_t s3PutBucketPolicy = 1ULL << 33;
static constexpr std::uint64_t s3GetBucketNotification = 1ULL << 34;
static constexpr std::uint64_t s3PutBucketNotification = 1ULL << 35;
static constexpr std::uint64_t s3GetBucketLogging = 1ULL << 36;
static constexpr std::uint64_t s3PutBucketLogging = 1ULL << 37;
static constexpr std::uint64_t s3GetBucketTagging = 1ULL << 38;
static constexpr std::uint64_t s3PutBucketTagging = 1ULL << 39;
static constexpr std::uint64_t s3GetBucketWebsite = 1ULL << 40;
static constexpr std::uint64_t s3PutBucketWebsite = 1ULL << 41;
static constexpr std::uint64_t s3DeleteBucketWebsite = 1ULL << 42;
static constexpr std::uint64_t s3GetLifecycleConfiguration = 1ULL << 43;
static constexpr std::uint64_t s3PutLifecycleConfiguration = 1ULL << 44;
static constexpr std::uint64_t s3PutReplicationConfiguration = 1ULL << 45;
static constexpr std::uint64_t s3GetReplicationConfiguration = 1ULL << 46;
static constexpr std::uint64_t s3DeleteReplicationConfiguration = 1ULL << 47;
static constexpr std::uint64_t s3GetObjectTagging = 1ULL << 48;
static constexpr std::uint64_t s3PutObjectTagging = 1ULL << 49;
static constexpr std::uint64_t s3DeleteObjectTagging = 1ULL << 50;
static constexpr std::uint64_t s3GetObjectVersionTagging = 1ULL << 51;
static constexpr std::uint64_t s3PutObjectVersionTagging = 1ULL << 52;
static constexpr std::uint64_t s3DeleteObjectVersionTagging = 1ULL << 53;
static constexpr std::uint64_t s3Count = 54;
static constexpr std::uint64_t s3All = (1ULL << s3Count) - 1;

namespace {
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
  case s3ListBucketMultiPartUploads:
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
  // Once we refity tenant, we should probably use that instead of a
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
string to_string(const MaskedIP& m);

inline bool operator ==(const MaskedIP& l, const MaskedIP& r) {
  auto shift = std::max((l.v6 ? 128 : 32) - l.prefix,
			(r.v6 ? 128 : 32) - r.prefix);
  ceph_assert(shift > 0);
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
  Condition(TokenID op, const char* s, std::size_t len) : op(op) {
    static constexpr char ifexistr[] = "IfExists";
    auto l = static_cast<const char*>(memmem(static_cast<const void*>(s), len,
					     static_cast<const void*>(ifexistr),
					     sizeof(ifexistr) -1));
    if (l && ((l + sizeof(ifexistr) - 1 == (s + len)))) {
      ifexists = true;
      key.assign(s, static_cast<const char*>(l) - s);
    } else {
      key.assign(s, len);
    }
  }

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
	return !((d == +0.0) || (d = -0.0) || std::isnan(d));
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


  struct ci_equal_to : public std::binary_function<const std::string,
						   const std::string,
						   bool> {
    bool operator ()(const std::string& s1,
		     const std::string& s2) const {
      return boost::iequals(s1, s2);
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
};

std::ostream& operator <<(std::ostream& m, const Condition& c);

std::string to_string(const Condition& c);

struct Statement {
  boost::optional<std::string> sid = boost::none;

  boost::container::flat_set<rgw::auth::Principal> princ;
  boost::container::flat_set<rgw::auth::Principal> noprinc;

  // Every statement MUST provide an effect. I just initialize it to
  // deny as defensive programming.
  Effect effect = Effect::Deny;

  std::uint64_t action = 0;
  std::uint64_t notaction = 0;

  boost::container::flat_set<ARN> resource;
  boost::container::flat_set<ARN> notresource;

  std::vector<Condition> conditions;

  Effect eval(const Environment& e,
	      boost::optional<const rgw::auth::Identity&> ida,
	      std::uint64_t action, const ARN& resource) const;
};

std::ostream& operator <<(ostream& m, const Statement& s);
std::string to_string(const Statement& s);

struct PolicyParseException : public std::exception {
  rapidjson::ParseResult pr;

  PolicyParseException(rapidjson::ParseResult&& pr)
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
};

std::ostream& operator <<(ostream& m, const Policy& p);
std::string to_string(const Policy& p);
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
