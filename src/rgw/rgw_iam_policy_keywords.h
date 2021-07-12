// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#ifndef CEPH_RGW_POLICY_S3V2_KEYWORDS_H
#define CEPH_RGW_POLICY_S3V2_KEYWORDS_H

namespace rgw {
namespace IAM {

enum class TokenKind {
  pseudo, top, statement, cond_op, cond_key, version_key, effect_key,
  princ_type
};

enum class TokenID {
  /// Pseudo-token
  Top,

  /// Top-level tokens
  Version, Id, Statement,

  /// Statement level tokens
  Sid, Effect, Principal, NotPrincipal, Action, NotAction,
  Resource, NotResource, Condition,

  /// Condition Operators!
  /// Any of these, except Null, can have an IfExists variant.

  // String!
  StringEquals, StringNotEquals, StringEqualsIgnoreCase,
  StringNotEqualsIgnoreCase, StringLike, StringNotLike,
  ForAllValuesStringEquals, ForAnyValueStringEquals,
  ForAllValuesStringLike, ForAnyValueStringLike,
  ForAllValuesStringEqualsIgnoreCase, ForAnyValueStringEqualsIgnoreCase,

  // Numeric!
  NumericEquals, NumericNotEquals, NumericLessThan, NumericLessThanEquals,
  NumericGreaterThan, NumericGreaterThanEquals,

  // Date!
  DateEquals, DateNotEquals, DateLessThan, DateLessThanEquals,
  DateGreaterThan, DateGreaterThanEquals,

  // Bool!
  Bool,

  // Binary!
  BinaryEquals,

  // IP Address!
  IpAddress, NotIpAddress,

  // Amazon Resource Names! (Does S3 need this?)
  ArnEquals, ArnNotEquals, ArnLike, ArnNotLike,

  // Null!
  Null,

#if 0 // Keys are done at runtime now

      /// Condition Keys!
  awsCurrentTime,
  awsEpochTime,
  awsTokenIssueTime,
  awsMultiFactorAuthPresent,
  awsMultiFactorAuthAge,
  awsPrincipalType,
  awsReferer,
  awsSecureTransport,
  awsSourceArn,
  awsSourceIp,
  awsSourceVpc,
  awsSourceVpce,
  awsUserAgent,
  awsuserid,
  awsusername,
  s3x_amz_acl,
  s3x_amz_grant_permission,
  s3x_amz_copy_source,
  s3x_amz_server_side_encryption,
  s3x_amz_server_side_encryption_aws_kms_key_id,
  s3x_amz_metadata_directive,
  s3x_amz_storage_class,
  s3VersionId,
  s3LocationConstraint,
  s3prefix,
  s3delimiter,
  s3max_keys,
  s3signatureversion,
  s3authType,
  s3signatureAge,
  s3x_amz_content_sha256,
#else
  CondKey,
#endif

  ///
  /// Versions!
  ///
  v2008_10_17,
  v2012_10_17,

  ///
  /// Effects!
  ///
  Allow,
  Deny,

  /// Principal Types!
  AWS,
  Federated,
  Service,
  CanonicalUser
};


enum class Version {
  v2008_10_17,
  v2012_10_17
};


enum class Effect {
  Allow,
  Deny,
  Pass
};

enum class Type {
  string,
  number,
  date,
  boolean,
  binary,
  ipaddr,
  arn,
  null
};
}
}

#endif // CEPH_RGW_POLICY_S3V2_KEYWORDS_H
