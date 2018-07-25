// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab


#include <cstring>
#include <iostream>
#include <regex>
#include <sstream>
#include <stack>
#include <utility>

#include <experimental/iterator>

#include "rapidjson/reader.h"

#include "rgw_auth.h"
#include <arpa/inet.h>
#include "rgw_iam_policy.h"

namespace {
constexpr int dout_subsys = ceph_subsys_rgw;
}

using std::bitset;
using std::find;
using std::int64_t;
using std::move;
using std::pair;
using std::size_t;
using std::string;
using std::stringstream;
using std::ostream;
using std::uint16_t;
using std::uint64_t;
using std::unordered_map;

using boost::container::flat_set;
using std::regex;
using std::regex_constants::ECMAScript;
using std::regex_constants::optimize;
using std::regex_match;
using std::smatch;

using rapidjson::BaseReaderHandler;
using rapidjson::UTF8;
using rapidjson::SizeType;
using rapidjson::Reader;
using rapidjson::kParseCommentsFlag;
using rapidjson::kParseNumbersAsStringsFlag;
using rapidjson::StringStream;
using rapidjson::ParseResult;

using rgw::auth::Principal;

namespace rgw {
namespace IAM {
#include "rgw_iam_policy_keywords.frag.cc"

struct actpair {
  const char* name;
  const uint64_t bit;
};

namespace {
boost::optional<Partition> to_partition(const smatch::value_type& p,
					bool wildcards) {
  if (p == "aws") {
    return Partition::aws;
  } else if (p == "aws-cn") {
    return Partition::aws_cn;
  } else if (p == "aws-us-gov") {
    return Partition::aws_us_gov;
  } else if (p == "*" && wildcards) {
    return Partition::wildcard;
  } else {
    return boost::none;
  }

  ceph_abort();
}

boost::optional<Service> to_service(const smatch::value_type& s,
				    bool wildcards) {
  static const unordered_map<string, Service> services = {
    { "acm", Service::acm },
    { "apigateway", Service::apigateway },
    { "appstream", Service::appstream },
    { "artifact", Service::artifact },
    { "autoscaling", Service::autoscaling },
    { "aws-marketplace", Service::aws_marketplace },
    { "aws-marketplace-management",
      Service::aws_marketplace_management },
    { "aws-portal", Service::aws_portal },
    { "cloudformation", Service::cloudformation },
    { "cloudfront", Service::cloudfront },
    { "cloudhsm", Service::cloudhsm },
    { "cloudsearch", Service::cloudsearch },
    { "cloudtrail", Service::cloudtrail },
    { "cloudwatch", Service::cloudwatch },
    { "codebuild", Service::codebuild },
    { "codecommit", Service::codecommit },
    { "codedeploy", Service::codedeploy },
    { "codepipeline", Service::codepipeline },
    { "cognito-identity", Service::cognito_identity },
    { "cognito-idp", Service::cognito_idp },
    { "cognito-sync", Service::cognito_sync },
    { "config", Service::config },
    { "datapipeline", Service::datapipeline },
    { "devicefarm", Service::devicefarm },
    { "directconnect", Service::directconnect },
    { "dms", Service::dms },
    { "ds", Service::ds },
    { "dynamodb", Service::dynamodb },
    { "ec2", Service::ec2 },
    { "ecr", Service::ecr },
    { "ecs", Service::ecs },
    { "elasticache", Service::elasticache },
    { "elasticbeanstalk", Service::elasticbeanstalk },
    { "elasticfilesystem", Service::elasticfilesystem },
    { "elasticloadbalancing", Service::elasticloadbalancing },
    { "elasticmapreduce", Service::elasticmapreduce },
    { "elastictranscoder", Service::elastictranscoder },
    { "es", Service::es },
    { "events", Service::events },
    { "firehose", Service::firehose },
    { "gamelift", Service::gamelift },
    { "glacier", Service::glacier },
    { "health", Service::health },
    { "iam", Service::iam },
    { "importexport", Service::importexport },
    { "inspector", Service::inspector },
    { "iot", Service::iot },
    { "kinesis", Service::kinesis },
    { "kinesisanalytics", Service::kinesisanalytics },
    { "kms", Service::kms },
    { "lambda", Service::lambda },
    { "lightsail", Service::lightsail },
    { "logs", Service::logs },
    { "machinelearning", Service::machinelearning },
    { "mobileanalytics", Service::mobileanalytics },
    { "mobilehub", Service::mobilehub },
    { "opsworks", Service::opsworks },
    { "opsworks-cm", Service::opsworks_cm },
    { "polly", Service::polly },
    { "rds", Service::rds },
    { "redshift", Service::redshift },
    { "route53", Service::route53 },
    { "route53domains", Service::route53domains },
    { "s3", Service::s3 },
    { "sdb", Service::sdb },
    { "servicecatalog", Service::servicecatalog },
    { "ses", Service::ses },
    { "sns", Service::sns },
    { "sqs", Service::sqs },
    { "ssm", Service::ssm },
    { "states", Service::states },
    { "storagegateway", Service::storagegateway },
    { "sts", Service::sts },
    { "support", Service::support },
    { "swf", Service::swf },
    { "trustedadvisor", Service::trustedadvisor },
    { "waf", Service::waf },
    { "workmail", Service::workmail },
    { "workspaces", Service::workspaces }};

  if (wildcards && s == "*") {
    return Service::wildcard;
  }

  auto i = services.find(s);
  if (i == services.end()) {
    return boost::none;
  } else {
    return i->second;
  }
}
}

ARN::ARN(const rgw_obj& o)
  : partition(Partition::aws),
    service(Service::s3),
    region(),
    account(o.bucket.tenant),
    resource(o.bucket.name)
{
  resource.push_back('/');
  resource.append(o.key.name);
}

ARN::ARN(const rgw_bucket& b)
  : partition(Partition::aws),
    service(Service::s3),
    region(),
    account(b.tenant),
    resource(b.name) { }

ARN::ARN(const rgw_bucket& b, const string& o)
  : partition(Partition::aws),
    service(Service::s3),
    region(),
    account(b.tenant),
    resource(b.name) {
  resource.push_back('/');
  resource.append(o);
}

ARN::ARN(const string& resource_name, const string& type, const string& tenant, bool has_path)
  : partition(Partition::aws),
    service(Service::iam),
    region(),
    account(tenant),
    resource(type) {
  if (! has_path)
    resource.push_back('/');
  resource.append(resource_name);
}

boost::optional<ARN> ARN::parse(const string& s, bool wildcards) {
  static const regex rx_wild("arn:([^:]*):([^:]*):([^:]*):([^:]*):([^:]*)",
			     std::regex_constants::ECMAScript |
			     std::regex_constants::optimize);
  static const regex rx_no_wild(
    "arn:([^:*]*):([^:*]*):([^:*]*):([^:*]*):(.*)",
    std::regex_constants::ECMAScript |
    std::regex_constants::optimize);

  smatch match;

  if ((s == "*") && wildcards) {
    return ARN(Partition::wildcard, Service::wildcard, "*", "*", "*");
  } else if (regex_match(s, match, wildcards ? rx_wild : rx_no_wild) &&
	     match.size() == 6) {
    if (auto p = to_partition(match[1], wildcards)) {
      if (auto s = to_service(match[2], wildcards)) {
	return ARN(*p, *s, match[3], match[4], match[5]);
      }
    }
  }
  return boost::none;
}

string ARN::to_string() const {
  string s;

  if (partition == Partition::aws) {
    s.append("aws:");
  } else if (partition == Partition::aws_cn) {
    s.append("aws-cn:");
  } else if (partition == Partition::aws_us_gov) {
    s.append("aws-us-gov:");
  } else {
    s.append("*:");
  }

  static const unordered_map<Service, string> services = {
    { Service::acm, "acm" },
    { Service::apigateway, "apigateway" },
    { Service::appstream, "appstream" },
    { Service::artifact, "artifact" },
    { Service::autoscaling, "autoscaling" },
    { Service::aws_marketplace, "aws-marketplace" },
    { Service::aws_marketplace_management, "aws-marketplace-management" },
    { Service::aws_portal, "aws-portal" },
    { Service::cloudformation, "cloudformation" },
    { Service::cloudfront, "cloudfront" },
    { Service::cloudhsm, "cloudhsm" },
    { Service::cloudsearch, "cloudsearch" },
    { Service::cloudtrail, "cloudtrail" },
    { Service::cloudwatch, "cloudwatch" },
    { Service::codebuild, "codebuild" },
    { Service::codecommit, "codecommit" },
    { Service::codedeploy, "codedeploy" },
    { Service::codepipeline, "codepipeline" },
    { Service::cognito_identity, "cognito-identity" },
    { Service::cognito_idp, "cognito-idp" },
    { Service::cognito_sync, "cognito-sync" },
    { Service::config, "config" },
    { Service::datapipeline, "datapipeline" },
    { Service::devicefarm, "devicefarm" },
    { Service::directconnect, "directconnect" },
    { Service::dms, "dms" },
    { Service::ds, "ds" },
    { Service::dynamodb, "dynamodb" },
    { Service::ec2, "ec2" },
    { Service::ecr, "ecr" },
    { Service::ecs, "ecs" },
    { Service::elasticache, "elasticache" },
    { Service::elasticbeanstalk, "elasticbeanstalk" },
    { Service::elasticfilesystem, "elasticfilesystem" },
    { Service::elasticloadbalancing, "elasticloadbalancing" },
    { Service::elasticmapreduce, "elasticmapreduce" },
    { Service::elastictranscoder, "elastictranscoder" },
    { Service::es, "es" },
    { Service::events, "events" },
    { Service::firehose, "firehose" },
    { Service::gamelift, "gamelift" },
    { Service::glacier, "glacier" },
    { Service::health, "health" },
    { Service::iam, "iam" },
    { Service::importexport, "importexport" },
    { Service::inspector, "inspector" },
    { Service::iot, "iot" },
    { Service::kinesis, "kinesis" },
    { Service::kinesisanalytics, "kinesisanalytics" },
    { Service::kms, "kms" },
    { Service::lambda, "lambda" },
    { Service::lightsail, "lightsail" },
    { Service::logs, "logs" },
    { Service::machinelearning, "machinelearning" },
    { Service::mobileanalytics, "mobileanalytics" },
    { Service::mobilehub, "mobilehub" },
    { Service::opsworks, "opsworks" },
    { Service::opsworks_cm, "opsworks-cm" },
    { Service::polly, "polly" },
    { Service::rds, "rds" },
    { Service::redshift, "redshift" },
    { Service::route53, "route53" },
    { Service::route53domains, "route53domains" },
    { Service::s3, "s3" },
    { Service::sdb, "sdb" },
    { Service::servicecatalog, "servicecatalog" },
    { Service::ses, "ses" },
    { Service::sns, "sns" },
    { Service::sqs, "sqs" },
    { Service::ssm, "ssm" },
    { Service::states, "states" },
    { Service::storagegateway, "storagegateway" },
    { Service::sts, "sts" },
    { Service::support, "support" },
    { Service::swf, "swf" },
    { Service::trustedadvisor, "trustedadvisor" },
    { Service::waf, "waf" },
    { Service::workmail, "workmail" },
    { Service::workspaces, "workspaces" }};

  auto i = services.find(service);
  if (i != services.end()) {
    s.append(i->second);
  } else {
    s.push_back('*');
  }
  s.push_back(':');

  s.append(region);
  s.push_back(':');

  s.append(account);
  s.push_back(':');

  s.append(resource);

  return s;
}

bool operator ==(const ARN& l, const ARN& r) {
  return ((l.partition == r.partition) &&
	  (l.service == r.service) &&
	  (l.region == r.region) &&
	  (l.account == r.account) &&
	  (l.resource == r.resource));
}
bool operator <(const ARN& l, const ARN& r) {
  return ((l.partition < r.partition) ||
	  (l.service < r.service) ||
	  (l.region < r.region) ||
	  (l.account < r.account) ||
	  (l.resource < r.resource));
}

// The candidate is not allowed to have wildcards. The only way to
// do that sanely would be to use unification rather than matching.
bool ARN::match(const ARN& candidate) const {
  if ((candidate.partition == Partition::wildcard) ||
      (partition != candidate.partition && partition
       != Partition::wildcard)) {
    return false;
  }

  if ((candidate.service == Service::wildcard) ||
      (service != candidate.service && service != Service::wildcard)) {
    return false;
  }

  if (!match_policy(region, candidate.region, MATCH_POLICY_ARN)) {
    return false;
  }

  if (!match_policy(account, candidate.account, MATCH_POLICY_ARN)) {
    return false;
  }

  if (!match_policy(resource, candidate.resource, MATCH_POLICY_RESOURCE)) {
    return false;
  }

  return true;
}

static const actpair actpairs[] =
{{ "s3:AbortMultipartUpload", s3AbortMultipartUpload },
 { "s3:CreateBucket", s3CreateBucket },
 { "s3:DeleteBucketPolicy", s3DeleteBucketPolicy },
 { "s3:DeleteBucket", s3DeleteBucket },
 { "s3:DeleteBucketWebsite", s3DeleteBucketWebsite },
 { "s3:DeleteObject", s3DeleteObject },
 { "s3:DeleteObjectVersion", s3DeleteObjectVersion },
 { "s3:DeleteObjectTagging", s3DeleteObjectTagging },
 { "s3:DeleteObjectVersionTagging", s3DeleteObjectVersionTagging },
 { "s3:DeleteReplicationConfiguration", s3DeleteReplicationConfiguration },
 { "s3:GetAccelerateConfiguration", s3GetAccelerateConfiguration },
 { "s3:GetBucketAcl", s3GetBucketAcl },
 { "s3:GetBucketCORS", s3GetBucketCORS },
 { "s3:GetBucketLocation", s3GetBucketLocation },
 { "s3:GetBucketLogging", s3GetBucketLogging },
 { "s3:GetBucketNotification", s3GetBucketNotification },
 { "s3:GetBucketPolicy", s3GetBucketPolicy },
 { "s3:GetBucketRequestPayment", s3GetBucketRequestPayment },
 { "s3:GetBucketTagging", s3GetBucketTagging },
 { "s3:GetBucketVersioning", s3GetBucketVersioning },
 { "s3:GetBucketWebsite", s3GetBucketWebsite },
 { "s3:GetLifecycleConfiguration", s3GetLifecycleConfiguration },
 { "s3:GetObjectAcl", s3GetObjectAcl },
 { "s3:GetObject", s3GetObject },
 { "s3:GetObjectTorrent", s3GetObjectTorrent },
 { "s3:GetObjectVersionAcl", s3GetObjectVersionAcl },
 { "s3:GetObjectVersion", s3GetObjectVersion },
 { "s3:GetObjectVersionTorrent", s3GetObjectVersionTorrent },
 { "s3:GetObjectTagging", s3GetObjectTagging },
 { "s3:GetObjectVersionTagging", s3GetObjectVersionTagging},
 { "s3:GetReplicationConfiguration", s3GetReplicationConfiguration },
 { "s3:ListAllMyBuckets", s3ListAllMyBuckets },
 { "s3:ListBucketMultipartUploads", s3ListBucketMultipartUploads },
 { "s3:ListBucket", s3ListBucket },
 { "s3:ListBucketVersions", s3ListBucketVersions },
 { "s3:ListMultipartUploadParts", s3ListMultipartUploadParts },
 { "s3:PutAccelerateConfiguration", s3PutAccelerateConfiguration },
 { "s3:PutBucketAcl", s3PutBucketAcl },
 { "s3:PutBucketCORS", s3PutBucketCORS },
 { "s3:PutBucketLogging", s3PutBucketLogging },
 { "s3:PutBucketNotification", s3PutBucketNotification },
 { "s3:PutBucketPolicy", s3PutBucketPolicy },
 { "s3:PutBucketRequestPayment", s3PutBucketRequestPayment },
 { "s3:PutBucketTagging", s3PutBucketTagging },
 { "s3:PutBucketVersioning", s3PutBucketVersioning },
 { "s3:PutBucketWebsite", s3PutBucketWebsite },
 { "s3:PutLifecycleConfiguration", s3PutLifecycleConfiguration },
 { "s3:PutObjectAcl",  s3PutObjectAcl },
 { "s3:PutObject", s3PutObject },
 { "s3:PutObjectVersionAcl", s3PutObjectVersionAcl },
 { "s3:PutObjectTagging", s3PutObjectTagging },
 { "s3:PutObjectVersionTagging", s3PutObjectVersionTagging },
 { "s3:PutReplicationConfiguration", s3PutReplicationConfiguration },
 { "s3:RestoreObject", s3RestoreObject },
 { "iam:PutUserPolicy", iamPutUserPolicy },
 { "iam:GetUserPolicy", iamGetUserPolicy },
 { "iam:DeleteUserPolicy", iamDeleteUserPolicy },
 { "iam:ListUserPolicies", iamListUserPolicies },
 { "iam:CreateRole", iamCreateRole},
 { "iam:DeleteRole", iamDeleteRole},
 { "iam:GetRole", iamGetRole},
 { "iam:ModifyRole", iamModifyRole},
 { "iam:ListRoles", iamListRoles},
 { "iam:PutRolePolicy", iamPutRolePolicy},
 { "iam:GetRolePolicy", iamGetRolePolicy},
 { "iam:ListRolePolicies", iamListRolePolicies},
 { "iam:DeleteRolePolicy", iamDeleteRolePolicy},
 { "sts:AssumeRole", stsAssumeRole},
 { "sts:AssumeRoleWithWebIdentity", stsAssumeRoleWithWebIdentity},
 { "sts:GetSessionToken", stsGetSessionToken},
};

struct PolicyParser;

const Keyword top[1]{"<Top>", TokenKind::pseudo, TokenID::Top, 0, false,
    false};
const Keyword cond_key[1]{"<Condition Key>", TokenKind::cond_key,
    TokenID::CondKey, 0, true, false};

struct ParseState {
  PolicyParser* pp;
  const Keyword* w;

  bool arraying = false;
  bool objecting = false;
  bool cond_ifexists = false;

  void reset();

  ParseState(PolicyParser* pp, const Keyword* w)
    : pp(pp), w(w) {}

  bool obj_start();

  bool obj_end();

  bool array_start() {
    if (w->arrayable && !arraying) {
      arraying = true;
      return true;
    }
    return false;
  }

  bool array_end();

  bool key(const char* s, size_t l);
  bool do_string(CephContext* cct, const char* s, size_t l);
  bool number(const char* str, size_t l);
};

// If this confuses you, look up the Curiously Recurring Template Pattern
struct PolicyParser : public BaseReaderHandler<UTF8<>, PolicyParser> {
  keyword_hash tokens;
  std::vector<ParseState> s;
  CephContext* cct;
  const string& tenant;
  Policy& policy;
  uint32_t v = 0;

  uint32_t seen = 0;

  uint32_t dex(TokenID in) const {
    switch (in) {
    case TokenID::Version:
      return 0x1;
    case TokenID::Id:
      return 0x2;
    case TokenID::Statement:
      return 0x4;
    case TokenID::Sid:
      return 0x8;
    case TokenID::Effect:
      return 0x10;
    case TokenID::Principal:
      return 0x20;
    case TokenID::NotPrincipal:
      return 0x40;
    case TokenID::Action:
      return 0x80;
    case TokenID::NotAction:
      return 0x100;
    case TokenID::Resource:
      return 0x200;
    case TokenID::NotResource:
      return 0x400;
    case TokenID::Condition:
      return 0x800;
    case TokenID::AWS:
      return 0x1000;
    case TokenID::Federated:
      return 0x2000;
    case TokenID::Service:
      return 0x4000;
    case TokenID::CanonicalUser:
      return 0x8000;
    default:
      ceph_abort();
    }
  }
  bool test(TokenID in) {
    return seen & dex(in);
  }
  void set(TokenID in) {
    seen |= dex(in);
    if (dex(in) & (dex(TokenID::Sid) | dex(TokenID::Effect) |
		   dex(TokenID::Principal) | dex(TokenID::NotPrincipal) |
		   dex(TokenID::Action) | dex(TokenID::NotAction) |
		   dex(TokenID::Resource) | dex(TokenID::NotResource) |
		   dex(TokenID::Condition) | dex(TokenID::AWS) |
		   dex(TokenID::Federated) | dex(TokenID::Service) |
		   dex(TokenID::CanonicalUser))) {
      v |= dex(in);
    }
  }
  void set(std::initializer_list<TokenID> l) {
    for (auto in : l) {
      seen |= dex(in);
      if (dex(in) & (dex(TokenID::Sid) | dex(TokenID::Effect) |
		     dex(TokenID::Principal) | dex(TokenID::NotPrincipal) |
		     dex(TokenID::Action) | dex(TokenID::NotAction) |
		     dex(TokenID::Resource) | dex(TokenID::NotResource) |
		     dex(TokenID::Condition) | dex(TokenID::AWS) |
		     dex(TokenID::Federated) | dex(TokenID::Service) |
		     dex(TokenID::CanonicalUser))) {
	v |= dex(in);
      }
    }
  }
  void reset(TokenID in) {
    seen &= ~dex(in);
    if (dex(in) & (dex(TokenID::Sid) | dex(TokenID::Effect) |
		   dex(TokenID::Principal) | dex(TokenID::NotPrincipal) |
		   dex(TokenID::Action) | dex(TokenID::NotAction) |
		   dex(TokenID::Resource) | dex(TokenID::NotResource) |
		   dex(TokenID::Condition) | dex(TokenID::AWS) |
		   dex(TokenID::Federated) | dex(TokenID::Service) |
		   dex(TokenID::CanonicalUser))) {
      v &= ~dex(in);
    }
  }
  void reset(std::initializer_list<TokenID> l) {
    for (auto in : l) {
      seen &= ~dex(in);
      if (dex(in) & (dex(TokenID::Sid) | dex(TokenID::Effect) |
		     dex(TokenID::Principal) | dex(TokenID::NotPrincipal) |
		     dex(TokenID::Action) | dex(TokenID::NotAction) |
		     dex(TokenID::Resource) | dex(TokenID::NotResource) |
		     dex(TokenID::Condition) | dex(TokenID::AWS) |
		     dex(TokenID::Federated) | dex(TokenID::Service) |
		     dex(TokenID::CanonicalUser))) {
	v &= ~dex(in);
      }
    }
  }
  void reset(uint32_t& v) {
    seen &= ~v;
    v = 0;
  }

  PolicyParser(CephContext* cct, const string& tenant, Policy& policy)
    : cct(cct), tenant(tenant), policy(policy) {}
  PolicyParser(const PolicyParser& policy) = delete;

  bool StartObject() {
    if (s.empty()) {
      s.push_back({this, top});
      s.back().objecting = true;
      return true;
    }

    return s.back().obj_start();
  }
  bool EndObject(SizeType memberCount) {
    if (s.empty()) {
      return false;
    }
    return s.back().obj_end();
  }
  bool Key(const char* str, SizeType length, bool copy) {
    if (s.empty()) {
      return false;
    }
    return s.back().key(str, length);
  }

  bool String(const char* str, SizeType length, bool copy) {
    if (s.empty()) {
      return false;
    }
    return s.back().do_string(cct, str, length);
  }
  bool RawNumber(const char* str, SizeType length, bool copy) {
    if (s.empty()) {
      return false;
    }

    return s.back().number(str, length);
  }
  bool StartArray() {
    if (s.empty()) {
      return false;
    }

    return s.back().array_start();
  }
  bool EndArray(SizeType) {
    if (s.empty()) {
      return false;
    }

    return s.back().array_end();
  }

  bool Default() {
    return false;
  }
};


// I really despise this misfeature of C++.
//
bool ParseState::obj_end() {
  if (objecting) {
    objecting = false;
    if (!arraying) {
      pp->s.pop_back();
    } else {
      reset();
    }
    return true;
  }
  return false;
}

bool ParseState::key(const char* s, size_t l) {
  auto token_len = l;
  bool ifexists = false;
  if (w->id == TokenID::Condition && w->kind == TokenKind::statement) {
    static constexpr char IfExists[] = "IfExists";
    if (boost::algorithm::ends_with(boost::string_view{s, l}, IfExists)) {
      ifexists = true;
      token_len -= sizeof(IfExists)-1;
    }
  }
  auto k = pp->tokens.lookup(s, token_len);

  if (!k) {
    if (w->kind == TokenKind::cond_op) {
      auto id = w->id;
      auto& t = pp->policy.statements.back();
      auto c_ife =  cond_ifexists;
      pp->s.emplace_back(pp, cond_key);
      t.conditions.emplace_back(id, s, l, c_ife);
      return true;
    } else {
      return false;
    }
  }

  // If the token we're going with belongs within the condition at the
  // top of the stack and we haven't already encountered it, push it
  // on the stack
  // Top
  if ((((w->id == TokenID::Top) && (k->kind == TokenKind::top)) ||
       // Statement
       ((w->id == TokenID::Statement) && (k->kind == TokenKind::statement)) ||

       /// Principal
       ((w->id == TokenID::Principal || w->id == TokenID::NotPrincipal) &&
	(k->kind == TokenKind::princ_type))) &&

      // Check that it hasn't been encountered. Note that this
      // conjoins with the run of disjunctions above.
      !pp->test(k->id)) {
    pp->set(k->id);
    pp->s.emplace_back(pp, k);
    return true;
  } else if ((w->id == TokenID::Condition) &&
	     (k->kind == TokenKind::cond_op)) {
    pp->s.emplace_back(pp, k);
    pp->s.back().cond_ifexists = ifexists;
    return true;
  }
  return false;
}

// I should just rewrite a few helper functions to use iterators,
// which will make all of this ever so much nicer.
static boost::optional<Principal> parse_principal(CephContext* cct, TokenID t,
						  string&& s) {
  // Wildcard!
  if ((t == TokenID::AWS) && (s == "*")) {
    return Principal::wildcard();

    // Do nothing for now.
  } else if (t == TokenID::CanonicalUser) {

  }  // AWS and Federated ARNs
   else if (t == TokenID::AWS || t == TokenID::Federated) {
    if (auto a = ARN::parse(s)) {
      if (a->resource == "root") {
	return Principal::tenant(std::move(a->account));
      }

      static const char rx_str[] = "([^/]*)/(.*)";
      static const regex rx(rx_str, sizeof(rx_str) - 1,
			    std::regex_constants::ECMAScript |
			    std::regex_constants::optimize);
      smatch match;
      if (regex_match(a->resource, match, rx) && match.size() == 3) {
	if (match[1] == "user") {
	  return Principal::user(std::move(a->account),
				 match[2]);
	}

	if (match[1] == "role") {
	  return Principal::role(std::move(a->account),
				 match[2]);
	}

        if (match[1] == "oidc-provider") {
                return Principal::oidc_provider(std::move(match[2]));
        }
      }
    } else {
      if (std::none_of(s.begin(), s.end(),
		       [](const char& c) {
			 return (c == ':') || (c == '/');
		       })) {
	// Since tenants are simply prefixes, there's no really good
	// way to see if one exists or not. So we return the thing and
	// let them try to match against it.
	return Principal::tenant(std::move(s));
      }
    }
  }

  ldout(cct, 0) << "Supplied principal is discarded: " << s << dendl;
  return boost::none;
}

bool ParseState::do_string(CephContext* cct, const char* s, size_t l) {
  auto k = pp->tokens.lookup(s, l);
  Policy& p = pp->policy;
  bool is_action = false;
  bool is_validaction = false;
  Statement* t = p.statements.empty() ? nullptr : &(p.statements.back());

  // Top level!
  if ((w->id == TokenID::Version) && k &&
      k->kind == TokenKind::version_key) {
    p.version = static_cast<Version>(k->specific);
  } else if (w->id == TokenID::Id) {
    p.id = string(s, l);

    // Statement

  } else if (w->id == TokenID::Sid) {
    t->sid.emplace(s, l);
  } else if ((w->id == TokenID::Effect) && k &&
	     k->kind == TokenKind::effect_key) {
    t->effect = static_cast<Effect>(k->specific);
  } else if (w->id == TokenID::Principal && s && *s == '*') {
    t->princ.emplace(Principal::wildcard());
  } else if (w->id == TokenID::NotPrincipal && s && *s == '*') {
    t->noprinc.emplace(Principal::wildcard());
  } else if ((w->id == TokenID::Action) ||
	     (w->id == TokenID::NotAction)) {
    is_action = true;
    if (*s == '*') {
      is_validaction = true;
      (w->id == TokenID::Action ?
        t->action = allValue : t->notaction = allValue);
    } else {
      for (auto& p : actpairs) {
        if (match_policy({s, l}, p.name, MATCH_POLICY_ACTION)) {
          is_validaction = true;
          (w->id == TokenID::Action ? t->action[p.bit] = 1 : t->notaction[p.bit] = 1);
        }
        if ((t->action & s3AllValue) == s3AllValue) {
          t->action[s3All] = 1;
        }
        if ((t->notaction & s3AllValue) == s3AllValue) {
          t->notaction[s3All] = 1;
        }
        if ((t->action & iamAllValue) == iamAllValue) {
          t->action[iamAll] = 1;
        }
        if ((t->notaction & iamAllValue) == iamAllValue) {
          t->notaction[iamAll] = 1;
        }
        if ((t->action & stsAllValue) == stsAllValue) {
          t->action[stsAll] = 1;
        }
        if ((t->notaction & stsAllValue) == stsAllValue) {
          t->notaction[stsAll] = 1;
        }
      }
    }
  } else if (w->id == TokenID::Resource || w->id == TokenID::NotResource) {
    auto a = ARN::parse({s, l}, true);
    // You can't specify resources for someone ELSE'S account.
    if (a && (a->account.empty() || a->account == pp->tenant ||
	      a->account == "*")) {
      if (a->account.empty() || a->account == "*")
	a->account = pp->tenant;
      (w->id == TokenID::Resource ? t->resource : t->notresource)
	.emplace(std::move(*a));
    }
    else
      ldout(cct, 0) << "Supplied resource is discarded: " << string(s, l)
		    << dendl;
  } else if (w->kind == TokenKind::cond_key) {
    auto& t = pp->policy.statements.back();
    t.conditions.back().vals.emplace_back(s, l);

    // Principals

  } else if (w->kind == TokenKind::princ_type) {
    if (pp->s.size() <= 1) {
      return false;
    }
    auto& pri = pp->s[pp->s.size() - 2].w->id == TokenID::Principal ?
      t->princ : t->noprinc;


    if (auto o = parse_principal(pp->cct, w->id, string(s, l))) {
      pri.emplace(std::move(*o));
    }

    // Failure

  } else {
    return false;
  }

  if (!arraying) {
    pp->s.pop_back();
  }

  if (is_action && !is_validaction){
    return false;
  }

  return true;
}

bool ParseState::number(const char* s, size_t l) {
  // Top level!
  if (w->kind == TokenKind::cond_key) {
    auto& t = pp->policy.statements.back();
    t.conditions.back().vals.emplace_back(s, l);

    // Failure

  } else {
    return false;
  }

  if (!arraying) {
    pp->s.pop_back();
  }

  return true;
}

void ParseState::reset() {
  pp->reset(pp->v);
}

bool ParseState::obj_start() {
  if (w->objectable && !objecting) {
    objecting = true;
    if (w->id == TokenID::Statement) {
      pp->policy.statements.emplace_back();
    }

    return true;
  }

  return false;
}


bool ParseState::array_end() {
  if (arraying && !objecting) {
    pp->s.pop_back();
    return true;
  }

  return false;
}

ostream& operator <<(ostream& m, const MaskedIP& ip) {
  // I have a theory about why std::bitset is the way it is.
  if (ip.v6) {
    for (int i = 7; i >= 0; --i) {
      uint16_t hextet = 0;
      for (int j = 15; j >= 0; --j) {
	hextet |= (ip.addr[(i * 16) + j] << j);
      }
      m << hex << (unsigned int) hextet;
      if (i != 0) {
	m << ":";
      }
    }
  } else {
    // It involves Satan.
    for (int i = 3; i >= 0; --i) {
      uint8_t b = 0;
      for (int j = 7; j >= 0; --j) {
	b |= (ip.addr[(i * 8) + j] << j);
      }
      m << (unsigned int) b;
      if (i != 0) {
	m << ".";
      }
    }
  }
  m << "/" << dec << ip.prefix;
  // It would explain a lot
  return m;
}

bool Condition::eval(const Environment& env) const {
  auto i = env.find(key);
  if (op == TokenID::Null) {
    return i == env.end() ? true : false;
  }

  if (i == env.end()) {
    return ifexists;
  }
  const auto& s = i->second;

  switch (op) {
    // String!
  case TokenID::StringEquals:
    return orrible(std::equal_to<std::string>(), s, vals);

  case TokenID::StringNotEquals:
    return orrible(std::not_fn(std::equal_to<std::string>()),
		   s, vals);

  case TokenID::StringEqualsIgnoreCase:
    return orrible(ci_equal_to(), s, vals);

  case TokenID::StringNotEqualsIgnoreCase:
    return orrible(std::not_fn(ci_equal_to()), s, vals);

  case TokenID::StringLike:
    return orrible(string_like(), s, vals);

  case TokenID::StringNotLike:
    return orrible(std::not_fn(string_like()), s, vals);

    // Numeric
  case TokenID::NumericEquals:
    return shortible(std::equal_to<double>(), as_number, s, vals);

  case TokenID::NumericNotEquals:
    return shortible(std::not_fn(std::equal_to<double>()),
		     as_number, s, vals);


  case TokenID::NumericLessThan:
    return shortible(std::less<double>(), as_number, s, vals);


  case TokenID::NumericLessThanEquals:
    return shortible(std::less_equal<double>(), as_number, s, vals);

  case TokenID::NumericGreaterThan:
    return shortible(std::greater<double>(), as_number, s, vals);

  case TokenID::NumericGreaterThanEquals:
    return shortible(std::greater_equal<double>(), as_number, s, vals);

    // Date!
  case TokenID::DateEquals:
    return shortible(std::equal_to<ceph::real_time>(), as_date, s, vals);

  case TokenID::DateNotEquals:
    return shortible(std::not_fn(std::equal_to<ceph::real_time>()),
		     as_date, s, vals);

  case TokenID::DateLessThan:
    return shortible(std::less<ceph::real_time>(), as_date, s, vals);


  case TokenID::DateLessThanEquals:
    return shortible(std::less_equal<ceph::real_time>(), as_date, s, vals);

  case TokenID::DateGreaterThan:
    return shortible(std::greater<ceph::real_time>(), as_date, s, vals);

  case TokenID::DateGreaterThanEquals:
    return shortible(std::greater_equal<ceph::real_time>(), as_date, s,
		     vals);

    // Bool!
  case TokenID::Bool:
    return shortible(std::equal_to<bool>(), as_bool, s, vals);

    // Binary!
  case TokenID::BinaryEquals:
    return shortible(std::equal_to<ceph::bufferlist>(), as_binary, s,
		     vals);

    // IP Address!
  case TokenID::IpAddress:
    return shortible(std::equal_to<MaskedIP>(), as_network, s, vals);

  case TokenID::NotIpAddress:
    {
      auto xc = as_network(s);
      if (!xc) {
	return false;
      }

      for (const string& d : vals) {
	auto xd = as_network(d);
	if (!xd) {
	  continue;
	}

	if (xc == xd) {
	  return false;
	}
      }
      return true;
    }

#if 0
    // Amazon Resource Names! (Does S3 need this?)
    TokenID::ArnEquals, TokenID::ArnNotEquals, TokenID::ArnLike,
      TokenID::ArnNotLike,
#endif

  default:
    return false;
  }
}

boost::optional<MaskedIP> Condition::as_network(const string& s) {
  MaskedIP m;
  if (s.empty()) {
    return boost::none;
  }

  m.v6 = (s.find(':') == string::npos) ? false : true;

  auto slash = s.find('/');
  if (slash == string::npos) {
    m.prefix = m.v6 ? 128 : 32;
  } else {
    char* end = 0;
    m.prefix = strtoul(s.data() + slash + 1, &end, 10);
    if (*end != 0 || (m.v6 && m.prefix > 128) ||
	(!m.v6 && m.prefix > 32)) {
      return boost::none;
    }
  }

  string t;
  auto p = &s;

  if (slash != string::npos) {
    t.assign(s, 0, slash);
    p = &t;
  }

  if (m.v6) {
    struct in6_addr a;
    if (inet_pton(AF_INET6, p->c_str(), static_cast<void*>(&a)) != 1) {
      return boost::none;
    }

    m.addr |= Address(a.s6_addr[15]) << 0;
    m.addr |= Address(a.s6_addr[14]) << 8;
    m.addr |= Address(a.s6_addr[13]) << 16;
    m.addr |= Address(a.s6_addr[12]) << 24;
    m.addr |= Address(a.s6_addr[11]) << 32;
    m.addr |= Address(a.s6_addr[10]) << 40;
    m.addr |= Address(a.s6_addr[9]) << 48;
    m.addr |= Address(a.s6_addr[8]) << 56;
    m.addr |= Address(a.s6_addr[7]) << 64;
    m.addr |= Address(a.s6_addr[6]) << 72;
    m.addr |= Address(a.s6_addr[5]) << 80;
    m.addr |= Address(a.s6_addr[4]) << 88;
    m.addr |= Address(a.s6_addr[3]) << 96;
    m.addr |= Address(a.s6_addr[2]) << 104;
    m.addr |= Address(a.s6_addr[1]) << 112;
    m.addr |= Address(a.s6_addr[0]) << 120;
  } else {
    struct in_addr a;
    if (inet_pton(AF_INET, p->c_str(), static_cast<void*>(&a)) != 1) {
      return boost::none;
    }

    m.addr = ntohl(a.s_addr);
  }

  return m;
}

namespace {
const char* condop_string(const TokenID t) {
  switch (t) {
  case TokenID::StringEquals:
    return "StringEquals";

  case TokenID::StringNotEquals:
    return "StringNotEquals";

  case TokenID::StringEqualsIgnoreCase:
    return "StringEqualsIgnoreCase";

  case TokenID::StringNotEqualsIgnoreCase:
    return "StringNotEqualsIgnoreCase";

  case TokenID::StringLike:
    return "StringLike";

  case TokenID::StringNotLike:
    return "StringNotLike";

  // Numeric!
  case TokenID::NumericEquals:
    return "NumericEquals";

  case TokenID::NumericNotEquals:
    return "NumericNotEquals";

  case TokenID::NumericLessThan:
    return "NumericLessThan";

  case TokenID::NumericLessThanEquals:
    return "NumericLessThanEquals";

  case TokenID::NumericGreaterThan:
    return "NumericGreaterThan";

  case TokenID::NumericGreaterThanEquals:
    return "NumericGreaterThanEquals";

  case TokenID::DateEquals:
    return "DateEquals";

  case TokenID::DateNotEquals:
    return "DateNotEquals";

  case TokenID::DateLessThan:
    return "DateLessThan";

  case TokenID::DateLessThanEquals:
    return "DateLessThanEquals";

  case TokenID::DateGreaterThan:
    return "DateGreaterThan";

  case TokenID::DateGreaterThanEquals:
    return "DateGreaterThanEquals";

  case TokenID::Bool:
    return "Bool";

  case TokenID::BinaryEquals:
    return "BinaryEquals";

  case TokenID::IpAddress:
    return "case TokenID::IpAddress";

  case TokenID::NotIpAddress:
    return "NotIpAddress";

  case TokenID::ArnEquals:
    return "ArnEquals";

  case TokenID::ArnNotEquals:
    return "ArnNotEquals";

  case TokenID::ArnLike:
    return "ArnLike";

  case TokenID::ArnNotLike:
    return "ArnNotLike";

  case TokenID::Null:
    return "Null";

  default:
    return "InvalidConditionOperator";
  }
}

template<typename Iterator>
ostream& print_array(ostream& m, Iterator begin, Iterator end) {
  if (begin == end) {
    m << "[]";
  } else {
    m << "[ ";
    std::copy(begin, end, std::experimental::make_ostream_joiner(m, ", "));
    m << " ]";
  }
  return m;
}

template<typename Iterator>
ostream& print_dict(ostream& m, Iterator begin, Iterator end) {
  m << "{ ";
  std::copy(begin, end, std::experimental::make_ostream_joiner(m, ", "));
  m << " }";
  return m;
}

}

ostream& operator <<(ostream& m, const Condition& c) {
  m << condop_string(c.op);
  if (c.ifexists) {
    m << "IfExists";
  }
  m << ": { " << c.key;
  print_array(m, c.vals.cbegin(), c.vals.cend());
  return m << " }";
}

Effect Statement::eval(const Environment& e,
		       boost::optional<const rgw::auth::Identity&> ida,
		       uint64_t act, const ARN& res) const {
  if (ida) {
    if (!princ.empty() && !ida->is_identity(princ)) {
      return Effect::Pass;
    } else if (!noprinc.empty() && ida->is_identity(noprinc)) {
      return Effect::Pass;
    }
  }

  if (!resource.empty()) {
    if (!std::any_of(resource.begin(), resource.end(),
          [&res](const ARN& pattern) {
            return pattern.match(res);
          })) {
      return Effect::Pass;
    }
  } else if (!notresource.empty()) {
    if (std::any_of(notresource.begin(), notresource.end(),
          [&res](const ARN& pattern) {
            return pattern.match(res);
          })) {
      return Effect::Pass;
    }
  }

  if (!(action[act] == 1) || (notaction[act] == 1)) {
    return Effect::Pass;
  }

  if (std::all_of(conditions.begin(),
		  conditions.end(),
		  [&e](const Condition& c) { return c.eval(e);})) {
    return effect;
  }

  return Effect::Pass;
}

Effect Statement::eval_principal(const Environment& e,
		       boost::optional<const rgw::auth::Identity&> ida) const {
  if (ida) {
    if (princ.empty() && noprinc.empty()) {
      return Effect::Deny;
    }
    if (!princ.empty() && !ida->is_identity(princ)) {
      return Effect::Deny;
    } else if (!noprinc.empty() && ida->is_identity(noprinc)) {
      return Effect::Deny;
    }
  }
  return Effect::Allow;
}

Effect Statement::eval_conditions(const Environment& e) const {
  if (std::all_of(conditions.begin(),
		  conditions.end(),
		  [&e](const Condition& c) { return c.eval(e);})) {
    return Effect::Allow;
  }
  return Effect::Deny;
}

namespace {
const char* action_bit_string(uint64_t action) {
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

  case iamModifyRole:
    return "iam:ModifyRole";

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

  case stsAssumeRole:
    return "sts:AssumeRole";

  case stsAssumeRoleWithWebIdentity:
    return "sts:AssumeRoleWithWebIdentity";

  case stsGetSessionToken:
    return "sts:GetSessionToken";
  }
  return "s3Invalid";
}

ostream& print_actions(ostream& m, const Action_t a) {
  bool begun = false;
  m << "[ ";
  for (auto i = 0U; i < allCount; ++i) {
    if (a[i] == 1) {
      if (begun) {
        m << ", ";
      } else {
        begun = true;
      }
      m << action_bit_string(i);
    }
  }
  if (begun) {
    m << " ]";
  } else {
    m << "]";
  }
  return m;
}
}

ostream& operator <<(ostream& m, const Statement& s) {
  m << "{ ";
  if (s.sid) {
    m << "Sid: " << *s.sid << ", ";
  }
  if (!s.princ.empty()) {
    m << "Principal: ";
    print_dict(m, s.princ.cbegin(), s.princ.cend());
    m << ", ";
  }
  if (!s.noprinc.empty()) {
    m << "NotPrincipal: ";
    print_dict(m, s.noprinc.cbegin(), s.noprinc.cend());
    m << ", ";
  }

  m << "Effect: " <<
    (s.effect == Effect::Allow ?
     (const char*) "Allow" :
     (const char*) "Deny");

  if (s.action.any() || s.notaction.any() || !s.resource.empty() ||
      !s.notresource.empty() || !s.conditions.empty()) {
    m << ", ";
  }

  if (s.action.any()) {
    m << "Action: ";
    print_actions(m, s.action);

    if (s.notaction.any() || !s.resource.empty() ||
	!s.notresource.empty() || !s.conditions.empty()) {
      m << ", ";
    }
  }

  if (s.notaction.any()) {
    m << "NotAction: ";
    print_actions(m, s.notaction);

    if (!s.resource.empty() || !s.notresource.empty() ||
	!s.conditions.empty()) {
      m << ", ";
    }
  }

  if (!s.resource.empty()) {
    m << "Resource: ";
    print_array(m, s.resource.cbegin(), s.resource.cend());

    if (!s.notresource.empty() || !s.conditions.empty()) {
      m << ", ";
    }
  }

  if (!s.notresource.empty()) {
    m << "NotResource: ";
    print_array(m, s.notresource.cbegin(), s.notresource.cend());

    if (!s.conditions.empty()) {
      m << ", ";
    }
  }

  if (!s.conditions.empty()) {
    m << "Condition: ";
    print_dict(m, s.conditions.cbegin(), s.conditions.cend());
  }

  return m << " }";
}

Policy::Policy(CephContext* cct, const string& tenant,
	       const bufferlist& _text)
  : text(_text.to_str()) {
  StringStream ss(text.data());
  PolicyParser pp(cct, tenant, *this);
  auto pr = Reader{}.Parse<kParseNumbersAsStringsFlag |
			   kParseCommentsFlag>(ss, pp);
  if (!pr) {
    throw PolicyParseException(std::move(pr));
  }
}

Effect Policy::eval(const Environment& e,
		    boost::optional<const rgw::auth::Identity&> ida,
		    std::uint64_t action, const ARN& resource) const {
  auto allowed = false;
  for (auto& s : statements) {
    auto g = s.eval(e, ida, action, resource);
    if (g == Effect::Deny) {
      return g;
    } else if (g == Effect::Allow) {
      allowed = true;
    }
  }
  return allowed ? Effect::Allow : Effect::Pass;
}

Effect Policy::eval_principal(const Environment& e,
		    boost::optional<const rgw::auth::Identity&> ida) const {
  auto allowed = false;
  for (auto& s : statements) {
    auto g = s.eval_principal(e, ida);
    if (g == Effect::Deny) {
      return g;
    } else if (g == Effect::Allow) {
      allowed = true;
    }
  }
  return allowed ? Effect::Allow : Effect::Deny;
}

Effect Policy::eval_conditions(const Environment& e) const {
  auto allowed = false;
  for (auto& s : statements) {
    auto g = s.eval_conditions(e);
    if (g == Effect::Deny) {
      return g;
    } else if (g == Effect::Allow) {
      allowed = true;
    }
  }
  return allowed ? Effect::Allow : Effect::Deny;
}

ostream& operator <<(ostream& m, const Policy& p) {
  m << "{ Version: "
    << (p.version == Version::v2008_10_17 ? "2008-10-17" : "2012-10-17");

  if (p.id || !p.statements.empty()) {
    m << ", ";
  }

  if (p.id) {
    m << "Id: " << *p.id;
    if (!p.statements.empty()) {
      m << ", ";
    }
  }

  if (!p.statements.empty()) {
    m << "Statements: ";
    print_array(m, p.statements.cbegin(), p.statements.cend());
    m << ", ";
  }
  return m << " }";
}

}
}
