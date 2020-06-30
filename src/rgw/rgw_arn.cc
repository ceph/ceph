// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include "rgw_arn.h"
#include "rgw_common.h"
#include <regex>

namespace rgw {

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

ARN::ARN(const rgw_bucket& b, const std::string& o)
  : partition(Partition::aws),
    service(Service::s3),
    region(),
    account(b.tenant),
    resource(b.name) {
  resource.push_back('/');
  resource.append(o);
}

ARN::ARN(const std::string& resource_name, const std::string& type, const std::string& tenant, bool has_path)
  : partition(Partition::aws),
    service(Service::iam),
    region(),
    account(tenant),
    resource(type) {
  if (! has_path)
    resource.push_back('/');
  resource.append(resource_name);
}

boost::optional<ARN> ARN::parse(const std::string& s, bool wildcards) {
  static const std::regex rx_wild("arn:([^:]*):([^:]*):([^:]*):([^:]*):([^:]*)",
			     std::regex_constants::ECMAScript |
			     std::regex_constants::optimize);
  static const std::regex rx_no_wild(
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

std::string ARN::to_string() const {
  std::string s{"arn:"};

  if (partition == Partition::aws) {
    s.append("aws:");
  } else if (partition == Partition::aws_cn) {
    s.append("aws-cn:");
  } else if (partition == Partition::aws_us_gov) {
    s.append("aws-us-gov:");
  } else {
    s.append("*:");
  }

  static const std::unordered_map<Service, string> services = {
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

boost::optional<ARNResource> ARNResource::parse(const std::string& s) {
  static const std::regex rx("^([^:/]*)[:/]?([^:/]*)?[:/]?(.*)$",
			     std::regex_constants::ECMAScript |
			     std::regex_constants::optimize);
  std::smatch match;
  if (!regex_match(s, match, rx)) {
    return boost::none;
  }
  if (match[2].str().empty() && match[3].str().empty()) {
    // only resource exist
    return rgw::ARNResource("", match[1], "");
  }

  // resource type also exist, and cannot be wildcard
  if (match[1] != std::string(wildcard)) {
    // resource type cannot be wildcard
    return rgw::ARNResource(match[1], match[2], match[3]);
  }

  return boost::none;
}

std::string ARNResource::to_string() const {
  std::string s;

  if (!resource_type.empty()) {
    s.append(resource_type);
    s.push_back(':');

    s.append(resource);
    s.push_back(':');

    s.append(qualifier);
  } else {
    s.append(resource);
  }

  return s;
}

}

