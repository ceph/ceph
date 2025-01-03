// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp


#include <cstring>
#include <iostream>
#include <regex>
#include <sstream>
#include <stack>
#include <utility>

#include <arpa/inet.h>

#include <experimental/iterator>

#include "rapidjson/reader.h"

#include "include/expected.hpp"

#include "rgw_auth.h"
#include "rgw_iam_policy.h"


namespace {
constexpr int dout_subsys = ceph_subsys_rgw;
}

using std::dec;
using std::hex;
using std::int64_t;
using std::size_t;
using std::string;
using std::stringstream;
using std::ostream;
using std::uint16_t;
using std::uint64_t;

using boost::container::flat_set;
using std::regex;
using std::regex_match;
using std::smatch;

using rapidjson::BaseReaderHandler;
using rapidjson::UTF8;
using rapidjson::SizeType;
using rapidjson::Reader;
using rapidjson::kParseCommentsFlag;
using rapidjson::kParseNumbersAsStringsFlag;
using rapidjson::StringStream;

using rgw::auth::Principal;

namespace rgw {
namespace IAM {
#include "rgw_iam_policy_keywords.frag.cc"

struct actpair {
  const char* name;
  const uint64_t bit;
};



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
 { "s3:DeleteBucketPublicAccessBlock", s3DeleteBucketPublicAccessBlock},
 { "s3:DeletePublicAccessBlock", s3DeletePublicAccessBlock},
 { "s3:DeleteReplicationConfiguration", s3DeleteReplicationConfiguration },
 { "s3:GetAccelerateConfiguration", s3GetAccelerateConfiguration },
 { "s3:GetBucketAcl", s3GetBucketAcl },
 { "s3:GetBucketCORS", s3GetBucketCORS },
 { "s3:GetBucketEncryption", s3GetBucketEncryption },
 { "s3:GetBucketLocation", s3GetBucketLocation },
 { "s3:GetBucketLogging", s3GetBucketLogging },
 { "s3:GetBucketNotification", s3GetBucketNotification },
 { "s3:GetBucketOwnershipControls", s3GetBucketOwnershipControls },
 { "s3:GetBucketPolicy", s3GetBucketPolicy },
 { "s3:GetBucketPolicyStatus", s3GetBucketPolicyStatus },
 { "s3:GetBucketPublicAccessBlock", s3GetBucketPublicAccessBlock },
 { "s3:GetBucketRequestPayment", s3GetBucketRequestPayment },
 { "s3:GetBucketTagging", s3GetBucketTagging },
 { "s3:GetBucketVersioning", s3GetBucketVersioning },
 { "s3:GetBucketWebsite", s3GetBucketWebsite },
 { "s3:GetLifecycleConfiguration", s3GetLifecycleConfiguration },
 { "s3:GetBucketObjectLockConfiguration", s3GetBucketObjectLockConfiguration },
 { "s3:GetPublicAccessBlock", s3GetPublicAccessBlock },
 { "s3:GetObjectAcl", s3GetObjectAcl },
 { "s3:GetObject", s3GetObject },
 { "s3:GetObjectTorrent", s3GetObjectTorrent },
 { "s3:GetObjectVersionAcl", s3GetObjectVersionAcl },
 { "s3:GetObjectVersion", s3GetObjectVersion },
 { "s3:GetObjectVersionTorrent", s3GetObjectVersionTorrent },
 { "s3:GetObjectTagging", s3GetObjectTagging },
 { "s3:GetObjectVersionTagging", s3GetObjectVersionTagging},
 { "s3:GetObjectRetention", s3GetObjectRetention},
 { "s3:GetObjectLegalHold", s3GetObjectLegalHold},
 { "s3:GetReplicationConfiguration", s3GetReplicationConfiguration },
 { "s3:ListAllMyBuckets", s3ListAllMyBuckets },
 { "s3:ListBucketMultipartUploads", s3ListBucketMultipartUploads },
 { "s3:ListBucket", s3ListBucket },
 { "s3:ListBucketVersions", s3ListBucketVersions },
 { "s3:ListMultipartUploadParts", s3ListMultipartUploadParts },
 { "s3:PutAccelerateConfiguration", s3PutAccelerateConfiguration },
 { "s3:PutBucketAcl", s3PutBucketAcl },
 { "s3:PutBucketCORS", s3PutBucketCORS },
 { "s3:PutBucketEncryption", s3PutBucketEncryption },
 { "s3:PutBucketLogging", s3PutBucketLogging },
 { "s3:PostBucketLogging", s3PostBucketLogging },
 { "s3:PutBucketNotification", s3PutBucketNotification },
 { "s3:PutBucketOwnershipControls", s3PutBucketOwnershipControls },
 { "s3:PutBucketPolicy", s3PutBucketPolicy },
 { "s3:PutBucketRequestPayment", s3PutBucketRequestPayment },
 { "s3:PutBucketTagging", s3PutBucketTagging },
 { "s3:PutBucketVersioning", s3PutBucketVersioning },
 { "s3:PutBucketWebsite", s3PutBucketWebsite },
 { "s3:PutLifecycleConfiguration", s3PutLifecycleConfiguration },
 { "s3:PutBucketObjectLockConfiguration", s3PutBucketObjectLockConfiguration },
 { "s3:PutObjectAcl",  s3PutObjectAcl },
 { "s3:PutObject", s3PutObject },
 { "s3:PutObjectVersionAcl", s3PutObjectVersionAcl },
 { "s3:PutObjectTagging", s3PutObjectTagging },
 { "s3:PutObjectVersionTagging", s3PutObjectVersionTagging },
 { "s3:PutObjectRetention", s3PutObjectRetention },
 { "s3:PutObjectLegalHold", s3PutObjectLegalHold },
 { "s3:BypassGovernanceRetention", s3BypassGovernanceRetention },
 { "s3:PutBucketPublicAccessBlock", s3PutBucketPublicAccessBlock },
 { "s3:PutPublicAccessBlock", s3PutPublicAccessBlock },
 { "s3:PutReplicationConfiguration", s3PutReplicationConfiguration },
 { "s3:RestoreObject", s3RestoreObject },
 { "s3:DescribeJob", s3DescribeJob },
 { "s3-object-lambda:GetObject", s3objectlambdaGetObject },
 { "s3-object-lambda:ListBucket", s3objectlambdaListBucket },
 { "iam:PutUserPolicy", iamPutUserPolicy },
 { "iam:GetUserPolicy", iamGetUserPolicy },
 { "iam:DeleteUserPolicy", iamDeleteUserPolicy },
 { "iam:ListUserPolicies", iamListUserPolicies },
 { "iam:AttachUserPolicy", iamAttachUserPolicy },
 { "iam:DetachUserPolicy", iamDetachUserPolicy },
 { "iam:ListAttachedUserPolicies", iamListAttachedUserPolicies },
 { "iam:CreateRole", iamCreateRole},
 { "iam:DeleteRole", iamDeleteRole},
 { "iam:GetRole", iamGetRole},
 { "iam:ModifyRoleTrustPolicy", iamModifyRoleTrustPolicy},
 { "iam:ListRoles", iamListRoles},
 { "iam:PutRolePolicy", iamPutRolePolicy},
 { "iam:GetRolePolicy", iamGetRolePolicy},
 { "iam:ListRolePolicies", iamListRolePolicies},
 { "iam:DeleteRolePolicy", iamDeleteRolePolicy},
 { "iam:AttachRolePolicy", iamAttachRolePolicy },
 { "iam:DetachRolePolicy", iamDetachRolePolicy },
 { "iam:ListAttachedRolePolicies", iamListAttachedRolePolicies },
 { "iam:CreateOIDCProvider", iamCreateOIDCProvider},
 { "iam:DeleteOIDCProvider", iamDeleteOIDCProvider},
 { "iam:GetOIDCProvider", iamGetOIDCProvider},
 { "iam:ListOIDCProviders", iamListOIDCProviders},
 { "iam:AddClientIdToOIDCProvider", iamAddClientIdToOIDCProvider},
 { "iam:UpdateOIDCProviderThumbprint", iamUpdateOIDCProviderThumbprint},
 { "iam:TagRole", iamTagRole},
 { "iam:ListRoleTags", iamListRoleTags},
 { "iam:UntagRole", iamUntagRole},
 { "iam:UpdateRole", iamUpdateRole},
 { "iam:CreateUser", iamCreateUser},
 { "iam:GetUser", iamGetUser},
 { "iam:UpdateUser", iamUpdateUser},
 { "iam:DeleteUser", iamDeleteUser},
 { "iam:ListUsers", iamListUsers},
 { "iam:CreateAccessKey", iamCreateAccessKey},
 { "iam:UpdateAccessKey", iamUpdateAccessKey},
 { "iam:DeleteAccessKey", iamDeleteAccessKey},
 { "iam:ListAccessKeys", iamListAccessKeys},
 { "iam:CreateGroup", iamCreateGroup},
 { "iam:GetGroup", iamGetGroup},
 { "iam:UpdateGroup", iamUpdateGroup},
 { "iam:DeleteGroup", iamDeleteGroup},
 { "iam:ListGroups", iamListGroups},
 { "iam:AddUserToGroup", iamAddUserToGroup},
 { "iam:RemoveUserFromGroup", iamRemoveUserFromGroup},
 { "iam:ListGroupsForUser", iamListGroupsForUser},
 { "iam:PutGroupPolicy", iamPutGroupPolicy },
 { "iam:GetGroupPolicy", iamGetGroupPolicy },
 { "iam:ListGroupPolicies", iamListGroupPolicies },
 { "iam:DeleteGroupPolicy", iamDeleteGroupPolicy },
 { "iam:AttachGroupPolicy", iamAttachGroupPolicy },
 { "iam:DetachGroupPolicy", iamDetachGroupPolicy },
 { "iam:ListAttachedGroupPolicies", iamListAttachedGroupPolicies },
 { "iam:GenerateCredentialReport", iamGenerateCredentialReport},
 { "iam:GenerateServiceLastAccessedDetails", iamGenerateServiceLastAccessedDetails},
 { "iam:SimulateCustomPolicy", iamSimulateCustomPolicy},
 { "iam:SimulatePrincipalPolicy", iamSimulatePrincipalPolicy},
 { "sts:AssumeRole", stsAssumeRole},
 { "sts:AssumeRoleWithWebIdentity", stsAssumeRoleWithWebIdentity},
 { "sts:GetSessionToken", stsGetSessionToken},
 { "sts:TagSession", stsTagSession},
 { "sns:GetTopicAttributes", snsGetTopicAttributes},
 { "sns:DeleteTopic", snsDeleteTopic},
 { "sns:Publish", snsPublish},
 { "sns:SetTopicAttributes", snsSetTopicAttributes},
 { "sns:CreateTopic", snsCreateTopic},
 { "sns:ListTopics", snsListTopics},
 { "organizations:DescribeAccount", organizationsDescribeAccount},
 { "organizations:DescribeOrganization", organizationsDescribeOrganization},
 { "organizations:DescribeOrganizationalUnit", organizationsDescribeOrganizationalUnit},
 { "organizations:DescribePolicy", organizationsDescribePolicy},
 { "organizations:ListChildren", organizationsListChildren},
 { "organizations:ListParents", organizationsListParents},
 { "organizations:ListPoliciesForTarget", organizationsListPoliciesForTarget},
 { "organizations:ListRoots", organizationsListRoots},
 { "organizations:ListPolicies", organizationsListPolicies},
 { "organizations:ListTargetsForPolicy", organizationsListTargetsForPolicy},
};

struct PolicyParser;

const Keyword top[1]{{"<Top>", TokenKind::pseudo, TokenID::Top, 0, false,
			false}};
const Keyword cond_key[1]{{"<Condition Key>", TokenKind::cond_key,
			     TokenID::CondKey, 0, true, false}};

struct ParseState {
  PolicyParser* pp;
  const Keyword* w;

  bool arraying = false;
  bool objecting = false;
  bool cond_ifexists = false;

  void reset();

  void annotate(std::string&& a);

  boost::optional<Principal> parse_principal(string&& s, string* errmsg);

  ParseState(PolicyParser* pp, const Keyword* w)
    : pp(pp), w(w) {}

  bool obj_start();

  bool obj_end();

  bool array_start() {
    if (w->arrayable && !arraying) {
      arraying = true;
      return true;
    }
    annotate(fmt::format("`{}` does not take array.",
			 w->name));
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
  const string* tenant = nullptr;
  Policy& policy;
  uint32_t v = 0;

  const bool reject_invalid_principals;

  uint32_t seen = 0;

  std::string annotation{"No error?"};

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

  PolicyParser(CephContext* cct, const string* tenant, Policy& policy,
	       bool reject_invalid_principals)
    : cct(cct), tenant(tenant), policy(policy),
      reject_invalid_principals(reject_invalid_principals) {}
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
      annotation = "Attempt to end unopened object at top level.";
      return false;
    }
    return s.back().obj_end();
  }
  bool Key(const char* str, SizeType length, bool copy) {
    if (s.empty()) {
      annotation = "Key not allowed at top level.";
      return false;
    }
    return s.back().key(str, length);
  }

  bool String(const char* str, SizeType length, bool copy) {
    if (s.empty()) {
      annotation = "String not allowed at top level.";
      return false;
    }
    return s.back().do_string(cct, str, length);
  }
  bool RawNumber(const char* str, SizeType length, bool copy) {
    if (s.empty()) {
      annotation = "Number not allowed at top level.";
      return false;
    }

    return s.back().number(str, length);
  }
  bool StartArray() {
    if (s.empty()) {
      annotation = "Array not allowed at top level.";
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
void ParseState::annotate(std::string&& a) {
  pp->annotation = std::move(a);
}

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
  annotate(
    fmt::format("Attempt to end unopened object on keyword `{}`.",
		w->name));
  return false;
}

bool ParseState::key(const char* s, size_t l) {
  auto token_len = l;
  bool ifexists = false;
  if (w->id == TokenID::Condition && w->kind == TokenKind::statement) {
    static constexpr char IfExists[] = "IfExists";
    if (boost::algorithm::ends_with(std::string_view{s, l}, IfExists)) {
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
      annotate(fmt::format("Unknown key `{}`.", std::string_view{s, token_len}));
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
  annotate(fmt::format("Token `{}` is not allowed in the context of `{}`.",
		       k->name, w->name));
  return false;
}

// I should just rewrite a few helper functions to use iterators,
// which will make all of this ever so much nicer.
boost::optional<Principal> ParseState::parse_principal(string&& s,
						       string* errmsg) {
  if ((w->id == TokenID::AWS) && (s == "*")) {
    // Wildcard!
    return Principal::wildcard();
  } else if (w->id == TokenID::CanonicalUser) {
    // Do nothing for now.
    if (errmsg)
      *errmsg = "RGW does not support canonical users.";
    return boost::none;
  } else if (w->id == TokenID::AWS || w->id == TokenID::Federated) {
    // AWS and Federated ARNs
    if (auto a = ARN::parse(s)) {
      if (a->resource == "root") {
	return Principal::account(std::move(a->account));
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
	if (match[1] == "assumed-role") {
	  return Principal::assumed_role(std::move(a->account), match[2]);
	}
      }
    } else if (std::none_of(s.begin(), s.end(),
		       [](const char& c) {
			 return (c == ':') || (c == '/');
		       })) {
      // Since tenants are simply prefixes, there's no really good
      // way to see if one exists or not. So we return the thing and
      // let them try to match against it.
      return Principal::account(std::move(s));
    }
    if (errmsg)
      *errmsg =
	fmt::format(
	  "`{}` is not a supported AWS or Federated ARN. Supported ARNs are "
	  "forms like: "
	  "`arn:aws:iam::tenant:root` or a bare tenant name for a tenant, "
	  "`arn:aws:iam::tenant:role/role-name` for a role, "
	  "`arn:aws:sts::tenant:assumed-role/role-name/role-session-name` "
	  "for an assumed role, "
	  "`arn:aws:iam::tenant:user/user-name` for a user, "
	  "`arn:aws:iam::tenant:oidc-provider/idp-url` for OIDC.", s);
  }

  if (errmsg)
    *errmsg = fmt::format("RGW does not support principals of type `{}`.",
			  w->name);
  return boost::none;
}

bool ParseState::do_string(CephContext* cct, const char* s, size_t l) {
  assert(s);

  auto k = pp->tokens.lookup(s, l);
  Policy& p = pp->policy;
  bool is_action = false;
  bool is_valid_action = false;
  Statement* t = p.statements.empty() ? nullptr : &(p.statements.back());
  ceph_assert(t || w->id == TokenID::Version || w->id == TokenID::Id);

  // Top level!
  if (w->id == TokenID::Version) {
    if (k && k->kind == TokenKind::version_key) {
      p.version = static_cast<Version>(k->specific);
    } else {
      annotate(
	fmt::format("`{}` is not a valid version. Valid versions are "
		    "`2008-10-17` and `2012-10-17`.",
		    std::string_view{s, l}));

      return false;
    }
  } else if (w->id == TokenID::Id) {
    p.id = string(s, l);

    // Statement

  } else if (w->id == TokenID::Sid) {
    t->sid.emplace(s, l);
  } else if (w->id == TokenID::Effect) {
    if (k && k->kind == TokenKind::effect_key) {
      t->effect = static_cast<Effect>(k->specific);
    } else {
      annotate(fmt::format("`{}` is not a valid effect.",
			   std::string_view{s, l}));
      return false;
    }
  } else if (w->id == TokenID::Principal && *s == '*') {
    t->princ.emplace(Principal::wildcard());
  } else if (w->id == TokenID::NotPrincipal && *s == '*') {
    t->noprinc.emplace(Principal::wildcard());
  } else if ((w->id == TokenID::Action) ||
	     (w->id == TokenID::NotAction)) {
    is_action = true;
    if (*s == '*') {
      is_valid_action = true;
      (w->id == TokenID::Action ?
        t->action = allValue : t->notaction = allValue);
    } else {
      for (auto& p : actpairs) {
        if (match_policy(string(s, l), p.name, MATCH_POLICY_ACTION)) {
          is_valid_action = true;
          (w->id == TokenID::Action ? t->action[p.bit] = 1 : t->notaction[p.bit] = 1);
        }
        if ((t->action & s3AllValue) == s3AllValue) {
          t->action[s3All] = 1;
        }
        if ((t->notaction & s3AllValue) == s3AllValue) {
          t->notaction[s3All] = 1;
        }
        if ((t->action & s3objectlambdaAllValue) == s3objectlambdaAllValue) {
          t->action[s3objectlambdaAll] = 1;
        }
        if ((t->notaction & s3objectlambdaAllValue) == s3objectlambdaAllValue) {
          t->notaction[s3objectlambdaAll] = 1;
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
        if ((t->action & snsAllValue) == snsAllValue) {
          t->action[snsAll] = 1;
        }
        if ((t->notaction & snsAllValue) == snsAllValue) {
          t->notaction[snsAll] = 1;
        }
        if ((t->action & organizationsAllValue) == organizationsAllValue) {
          t->action[organizationsAll] = 1;
        }
        if ((t->notaction & organizationsAllValue) == organizationsAllValue) {
          t->notaction[organizationsAll] = 1;
        }
      }
    }
  } else if (w->id == TokenID::Resource || w->id == TokenID::NotResource) {
    auto a = ARN::parse({s, l}, true);
    if (!a) {
      annotate(
	fmt::format("`{}` is not a valid ARN. Resource ARNs should have a "
		    "format like `arn:aws:s3::tenant:resource' or "
		    "`arn:aws:s3:::resource`.",
		    std::string_view{s, l}));
      return false;
    }
    // You can't specify resources for someone ELSE'S account.
    if (a->account.empty() || pp->tenant == nullptr ||
	a->account == *pp->tenant || a->account == "*") {
      if (pp->tenant && (a->account.empty() || a->account == "*"))
	a->account = *pp->tenant;
      (w->id == TokenID::Resource ? t->resource : t->notresource)
	.emplace(std::move(*a));
    } else {
      annotate(fmt::format("Policy owned by tenant `{}` cannot grant access to "
			   "resource owned by tenant `{}`.",
			   *pp->tenant, a->account));
      return false;
    }
  } else if (w->kind == TokenKind::cond_key) {
    if (l > 0 && *s == '$') {
      if (l >= 2 && *(s+1) == '{') {
        if (l > 0 && *(s+l-1) == '}') {
          t->conditions.back().isruntime = true;
        } else {
	  annotate(fmt::format("Invalid interpolation `{}`.",
			       std::string_view{s, l}));
          return false;
        }
      } else {
	annotate(fmt::format("Invalid interpolation `{}`.",
			     std::string_view{s, l}));
        return false;
      }
    }
    t->conditions.back().vals.emplace_back(s, l);

    // Principals

  } else if (w->kind == TokenKind::princ_type) {
    if (pp->s.size() <= 1) {
      annotate(fmt::format("Principle isn't allowed at top level."));
      return false;
    }
    auto& pri = pp->s[pp->s.size() - 2].w->id == TokenID::Principal ?
      t->princ : t->noprinc;

    string errmsg;
    if (auto o = parse_principal({s, l}, &errmsg)) {
      pri.emplace(std::move(*o));
    } else if (pp->reject_invalid_principals) {
      annotate(std::move(errmsg));
      return false;
    } else {
      ldout(cct, 0) << "Ignored principle `" << std::string_view{s, l} << "`: "
		    << errmsg << dendl;
    }
  } else {
    // Failure
    annotate(fmt::format("`{}` is not valid in the context of `{}`.",
			 std::string_view{s, l}, w->name));
    return false;
  }

  if (!arraying) {
    pp->s.pop_back();
  }

  if (is_action && !is_valid_action) {
    annotate(fmt::format("`{}` is not a valid action.",
			 std::string_view{s, l}));
    return false;
  }

  // NotPrincipal must be used with "Effect":"Deny". Using it with "Effect":"Allow" is not supported.
  // cf. https://docs.aws.amazon.com/IAM/latest/UserGuide/reference_policies_elements_notprincipal.html
  if (t && t->effect == Effect::Allow && !t->noprinc.empty()) {
    annotate("Allow with NotPrincipal is not allowed.");
    return false;
  }

  return true;
}

bool ParseState::number(const char* s, size_t l) {
  // Top level!
  if (w->kind == TokenKind::cond_key) {
    auto& t = pp->policy.statements.back();
    t.conditions.back().vals.emplace_back(s, l);
  } else {
    // Failure
    annotate("Numbers are not allowed outside condition arguments.");
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

  annotate(fmt::format("The {} keyword cannot introduce an object.",
		       w->name));

  return false;
}


bool ParseState::array_end() {
  if (arraying && !objecting) {
    pp->s.pop_back();
    return true;
  }

  annotate("Attempt to close unopened array.");
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
  std::vector<std::string> runtime_vals;
  auto i = env.find(key);
  if (op == TokenID::Null) {
    return i == env.end() ? true : false;
  }

  if (i == env.end()) {
    if (op == TokenID::ForAllValuesStringEquals ||
        op == TokenID::ForAllValuesStringEqualsIgnoreCase ||
        op == TokenID::ForAllValuesStringLike) {
      return true;
    } else {
      return ifexists;
    }
  }

  if (isruntime) {
    string k = vals.back();
    k.erase(0,2); //erase $, {
    k.erase(k.length() - 1, 1); //erase }
    const auto& it = env.equal_range(k);
    for (auto itr = it.first; itr != it.second; itr++) {
      runtime_vals.emplace_back(itr->second);
    }
  }
  const auto& s = i->second;

  const auto& itr = env.equal_range(key);

  switch (op) {
    // String!
  case TokenID::ForAnyValueStringEquals:
  case TokenID::StringEquals:
    return orrible(std::equal_to<std::string>(), itr, isruntime? runtime_vals : vals);

  case TokenID::StringNotEquals:
    return orrible(std::not_fn(std::equal_to<std::string>()),
		   itr, isruntime? runtime_vals : vals);

  case TokenID::ForAnyValueStringEqualsIgnoreCase:
  case TokenID::StringEqualsIgnoreCase:
    return orrible(ci_equal_to(), itr, isruntime? runtime_vals : vals);

  case TokenID::StringNotEqualsIgnoreCase:
    return orrible(std::not_fn(ci_equal_to()), itr, isruntime? runtime_vals : vals);

  case TokenID::ForAnyValueStringLike:
  case TokenID::StringLike:
    return orrible(string_like(), itr, isruntime? runtime_vals : vals);

  case TokenID::StringNotLike:
    return orrible(std::not_fn(string_like()), itr, isruntime? runtime_vals : vals);

  case TokenID::ForAllValuesStringEquals:
    return andible(std::equal_to<std::string>(), itr, isruntime? runtime_vals : vals);

  case TokenID::ForAllValuesStringLike:
    return andible(string_like(), itr, isruntime? runtime_vals : vals);

  case TokenID::ForAllValuesStringEqualsIgnoreCase:
    return andible(ci_equal_to(), itr, isruntime? runtime_vals : vals);

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
		       uint64_t act, boost::optional<const ARN&> res, boost::optional<PolicyPrincipal&> princ_type) const {

  if (eval_principal(e, ida, princ_type) == Effect::Deny) {
    return Effect::Pass;
  }

  if (res && resource.empty() && notresource.empty()) {
    return Effect::Pass;
  }
  if (!res && (!resource.empty() || !notresource.empty())) {
    return Effect::Pass;
  }
  if (!resource.empty() && res) {
    if (!std::any_of(resource.begin(), resource.end(),
          [&res](const ARN& pattern) {
            return pattern.match(*res);
          })) {
      return Effect::Pass;
    }
  } else if (!notresource.empty() && res) {
    if (std::any_of(notresource.begin(), notresource.end(),
          [&res](const ARN& pattern) {
            return pattern.match(*res);
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

static bool is_identity(const auth::Identity& ida,
                        const flat_set<auth::Principal>& princ)
{
  return std::any_of(princ.begin(), princ.end(),
      [&ida] (const auth::Principal& p) {
        return ida.is_identity(p);
      });
}

Effect Statement::eval_principal(const Environment& e,
		       boost::optional<const rgw::auth::Identity&> ida, boost::optional<PolicyPrincipal&> princ_type) const {
  if (princ_type) {
    *princ_type = PolicyPrincipal::Other;
  }
  if (ida) {
    if (princ.empty() && noprinc.empty()) {
      return Effect::Deny;
    }
    if (ida->get_identity_type() != TYPE_ROLE && !princ.empty() && !is_identity(*ida, princ)) {
      return Effect::Deny;
    }
    if (ida->get_identity_type() == TYPE_ROLE && !princ.empty()) {
      bool princ_matched = false;
      for (auto p : princ) { // Check each principal to determine the type of the one that has matched
        if (ida->is_identity(p)) {
          if (p.is_assumed_role() || p.is_user()) {
            if (princ_type) *princ_type = PolicyPrincipal::Session;
          } else {
            if (princ_type) *princ_type = PolicyPrincipal::Role;
          }
          princ_matched = true;
        }
      }
      if (!princ_matched) {
        return Effect::Deny;
      }
    } else if (!noprinc.empty() && is_identity(*ida, noprinc)) {
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

  case s3GetBucketOwnershipControls:
    return "s3:GetBucketOwnershipControls";

  case s3PutBucketOwnershipControls:
    return "s3:PutBucketOwnershipControls";

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

    case s3PostBucketLogging:
      return "s3:PostBucketLogging";

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

  case s3DescribeJob:
    return "s3:DescribeJob";

  case s3objectlambdaGetObject:
    return "s3-object-lambda:GetObject";

  case s3objectlambdaListBucket:
    return "s3-object-lambda:ListBucket";

  case iamPutUserPolicy:
    return "iam:PutUserPolicy";

  case iamGetUserPolicy:
    return "iam:GetUserPolicy";

  case iamListUserPolicies:
    return "iam:ListUserPolicies";

  case iamDeleteUserPolicy:
    return "iam:DeleteUserPolicy";

  case iamAttachUserPolicy:
    return "iam:AttachUserPolicy";

  case iamDetachUserPolicy:
    return "iam:DetachUserPolicy";

  case iamListAttachedUserPolicies:
    return "iam:ListAttachedUserPolicies";

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

  case iamAttachRolePolicy:
    return "iam:AttachRolePolicy";

  case iamDetachRolePolicy:
    return "iam:DetachRolePolicy";

  case iamListAttachedRolePolicies:
    return "iam:ListAttachedRolePolicies";

  case iamCreateOIDCProvider:
    return "iam:CreateOIDCProvider";

  case iamDeleteOIDCProvider:
    return "iam:DeleteOIDCProvider";

  case iamGetOIDCProvider:
    return "iam:GetOIDCProvider";

  case iamListOIDCProviders:
    return "iam:ListOIDCProviders";

  case iamAddClientIdToOIDCProvider:
    return "iam:AddClientIdToOIDCProvider";

  case iamUpdateOIDCProviderThumbprint:
    return "iam:UpdateOIDCProviderThumbprint";

  case iamTagRole:
    return "iam:TagRole";

  case iamListRoleTags:
    return "iam:ListRoleTags";

  case iamUntagRole:
    return "iam:UntagRole";

  case iamUpdateRole:
    return "iam:UpdateRole";

  case iamCreateUser:
    return "iam:CreateUser";

  case iamGetUser:
    return "iam:GetUser";

  case iamUpdateUser:
    return "iam:UpdateUser";

  case iamDeleteUser:
    return "iam:DeleteUser";

  case iamListUsers:
    return "iam:ListUsers";

  case iamCreateAccessKey:
    return "iam:CreateAccessKey";

  case iamUpdateAccessKey:
    return "iam:UpdateAccessKey";

  case iamDeleteAccessKey:
    return "iam:DeleteAccessKey";

  case iamListAccessKeys:
    return "iam:ListAccessKeys";

  case iamCreateGroup:
    return "iam:CreateGroup";

  case iamGetGroup:
    return "iam:GetGroup";

  case iamUpdateGroup:
    return "iam:UpdateGroup";

  case iamDeleteGroup:
    return "iam:DeleteGroup";

  case iamListGroups:
    return "iam:ListGroups";

  case iamAddUserToGroup:
    return "iam:AddUserToGroup";

  case iamRemoveUserFromGroup:
    return "iam:RemoveUserFromGroup";

  case iamListGroupsForUser:
    return "iam:ListGroupsForUser";

  case iamPutGroupPolicy:
    return "iam:PutGroupPolicy";

  case iamGetGroupPolicy:
    return "iam:GetGroupPolicy";

  case iamListGroupPolicies:
    return "iam:ListGroupPolicies";

  case iamDeleteGroupPolicy:
    return "iam:DeleteGroupPolicy";

  case iamAttachGroupPolicy:
    return "iam:AttachGroupPolicy";

  case iamDetachGroupPolicy:
    return "iam:DetachGroupPolicy";

  case iamListAttachedGroupPolicies:
    return "iam:ListAttachedGroupPolicies";

  case iamGenerateCredentialReport:
    return "iam:GenerateCredentialReport";

  case iamGenerateServiceLastAccessedDetails:
    return "iam:GenerateServiceLastAccessedDetails";

  case iamSimulateCustomPolicy:
    return "iam:SimulateCustomPolicy";

  case iamSimulatePrincipalPolicy:
    return "iam:SimulatePrincipalPolicy";

  case stsAssumeRole:
    return "sts:AssumeRole";

  case stsAssumeRoleWithWebIdentity:
    return "sts:AssumeRoleWithWebIdentity";

  case stsGetSessionToken:
    return "sts:GetSessionToken";

  case stsTagSession:
    return "sts:TagSession";

  case snsSetTopicAttributes:
    return "sns:SetTopicAttributes";

  case snsGetTopicAttributes:
    return "sns:GetTopicAttributes";

  case snsDeleteTopic:
    return "sns:DeleteTopic";

  case snsPublish:
    return "sns:Publish";

  case snsCreateTopic:
    return "sns:CreateTopic";

  case snsListTopics:
    return "sns:ListTopics";

  case organizationsDescribeAccount:
    return "organizations:DescribeAccount";

  case organizationsDescribeOrganization:
    return "organizations:DescribeOrganization";

  case organizationsDescribeOrganizationalUnit:
    return "organizations:DescribeOrganizationalUnit";

  case organizationsDescribePolicy:
    return "organizations:DescribePolicy";

  case organizationsListChildren:
    return "organizations:ListChildren";

  case organizationsListParents:
    return "organizations:ListParents";

  case organizationsListPoliciesForTarget:
    return "organizations:ListPoliciesForTarget";

  case organizationsListRoots:
    return "organizations:ListRoots";

  case organizationsListPolicies:
    return "organizations:ListPolicies";

  case organizationsListTargetsForPolicy:
    return "organizations:ListTargetsForPolicy";
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

Policy::Policy(CephContext* cct, const string* tenant,
	       std::string _text,
	       bool reject_invalid_principals)
  : text(std::move(_text)) {
  StringStream ss(text.data());
  PolicyParser pp(cct, tenant, *this, reject_invalid_principals);
  auto pr = Reader{}.Parse<kParseNumbersAsStringsFlag |
			   kParseCommentsFlag>(ss, pp);
  if (!pr) {
    throw PolicyParseException(pr, pp.annotation);
  }
}

Effect Policy::eval(const Environment& e,
		    boost::optional<const rgw::auth::Identity&> ida,
		    std::uint64_t action, boost::optional<const ARN&> resource,
        boost::optional<PolicyPrincipal&> princ_type) const {
  auto allowed = false;
  for (auto& s : statements) {
    auto g = s.eval(e, ida, action, resource, princ_type);
    if (g == Effect::Deny) {
      return g;
    } else if (g == Effect::Allow) {
      allowed = true;
    }
  }
  return allowed ? Effect::Allow : Effect::Pass;
}

Effect Policy::eval_principal(const Environment& e,
		    boost::optional<const rgw::auth::Identity&> ida, boost::optional<PolicyPrincipal&> princ_type) const {
  auto allowed = false;
  for (auto& s : statements) {
    auto g = s.eval_principal(e, ida, princ_type);
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

static const Environment iam_all_env = {
					{"aws:SourceIp","1.1.1.1"},
					{"aws:UserId","anonymous"},
					{"s3:x-amz-server-side-encryption-aws-kms-key-id","secret"}
};

struct IsPublicStatement
{
  bool operator() (const Statement &s) const {
    if (s.effect == Effect::Allow) {
      for (const auto& p : s.princ) {
        if (p.is_wildcard()) {
          return s.eval_conditions(iam_all_env) == Effect::Allow;
        }
      }
    }
    return false;
  }
};


bool is_public(const Policy& p)
{
  return std::any_of(p.statements.begin(), p.statements.end(), IsPublicStatement());
}

} // namespace IAM
} // namespace rgw
