// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#pragma once

#include <map>
#include <string>
#include <iosfwd>
#include <include/types.h>

#include "include/str_list.h"
#include "rgw_xml.h"
#include "rgw_acl.h"
#include "rgw_sal_fwd.h"

class RGWUserCtl;

class ACLPermission_S3 : public ACLPermission, public XMLObj
{
public:
  ACLPermission_S3() {}
  virtual ~ACLPermission_S3() override {}

  bool xml_end(const char *el) override;
};

class ACLGrantee_S3 : public XMLObj
{
public:
  ACLGrantee_S3() {}
  virtual ~ACLGrantee_S3() override {}

  bool xml_start(const char *el, const char **attr);
};


class ACLGrant_S3 : public ACLGrant, public XMLObj
{
public:
  ACLGrant_S3() {}
  virtual ~ACLGrant_S3() override {}

  bool xml_end(const char *el) override;
  bool xml_start(const char *el, const char **attr);

  static ACLGroupTypeEnum uri_to_group(std::string& uri);
  static bool group_to_uri(ACLGroupTypeEnum group, std::string& uri);
};

class RGWAccessControlList_S3 : public RGWAccessControlList, public XMLObj
{
public:
  bool xml_end(const char *el) override;
};

class ACLOwner_S3 : public ACLOwner, public XMLObj
{
public:
  ACLOwner_S3() {}
  virtual ~ACLOwner_S3() override {}

  bool xml_end(const char *el) override;
};

class RGWEnv;

class RGWAccessControlPolicy_S3 : public RGWAccessControlPolicy, public XMLObj
{
public:
  bool xml_end(const char *el) override;

  int rebuild(const DoutPrefixProvider *dpp, rgw::sal::Driver* driver, ACLOwner *owner,
	      RGWAccessControlPolicy& dest, std::string &err_msg);
};

/**
 * Interfaces with the webserver's XML handling code
 * to parse it in a way that makes sense for the rgw.
 */
class RGWACLXMLParser_S3 : public RGWXMLParser
{
  CephContext *cct;

  XMLObj *alloc_obj(const char *el) override;
public:
  explicit RGWACLXMLParser_S3(CephContext *_cct) : cct(_cct) {}
};

namespace rgw::s3 {

/// Write an AccessControlPolicy xml document for the given policy.
void write_policy_xml(const RGWAccessControlPolicy& policy,
                      std::ostream& out);

/// Construct a policy from a s3 canned acl string.
int create_canned_acl(const ACLOwner& owner,
                      const ACLOwner& bucket_owner,
                      const std::string& canned_acl,
                      RGWAccessControlPolicy& policy);

/// Construct a policy from x-amz-grant-* request headers.
int create_policy_from_headers(const DoutPrefixProvider* dpp,
                               rgw::sal::Driver* driver,
                               const ACLOwner& owner,
                               const RGWEnv& env,
                               RGWAccessControlPolicy& policy);

} // namespace rgw::s3
