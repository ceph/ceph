// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#pragma once
#include <string>
#include <boost/optional.hpp>

class rgw_obj;
class rgw_bucket;

namespace rgw {

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

/* valid format:
 * 'arn:partition:service:region:account-id:resource'
 * The 'resource' part can be further broken down via ARNResource
*/
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
  ARN(const std::string& resource_name, const std::string& type, const std::string& tenant, bool has_path=false);

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

/* valid formats (only resource part):
 * 'resource'
 * 'resourcetype/resource'
 * 'resourcetype/resource/qualifier'
 * 'resourcetype/resource:qualifier'
 * 'resourcetype:resource'
 * 'resourcetype:resource:qualifier'
 * Note that 'resourceType' cannot be wildcard
*/
struct ARNResource {
  constexpr static const char* const wildcard = "*";
  std::string resource_type;
  std::string resource;
  std::string qualifier;

  ARNResource() : resource_type(""), resource(wildcard), qualifier("") {}
  
  ARNResource(const std::string& _resource_type, const std::string& _resource, const std::string& _qualifier) : 
    resource_type(std::move(_resource_type)), resource(std::move(_resource)), qualifier(std::move(_qualifier)) {}

  static boost::optional<ARNResource> parse(const std::string& s);
  
  std::string to_string() const;
};

inline std::string to_string(const ARNResource& r) {
  return r.to_string();
}

} // namespace rgw

namespace std {
template<>
struct hash<::rgw::Service> {
  size_t operator()(const ::rgw::Service& s) const noexcept {
    // Invoke a default-constructed hash object for int.
    return hash<int>()(static_cast<int>(s));
  }
};
} // namespace std

