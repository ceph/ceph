// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#ifndef CEPH_RGW_ACL_S3_H
#define CEPH_RGW_ACL_S3_H

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
  void to_xml(std::ostream& out);
};

class ACLGrantee_S3 : public ACLGrantee, public XMLObj
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

  void to_xml(CephContext *cct, std::ostream& out);
  bool xml_end(const char *el) override;
  bool xml_start(const char *el, const char **attr);

  static ACLGroupTypeEnum uri_to_group(std::string& uri);
  static bool group_to_uri(ACLGroupTypeEnum group, std::string& uri);
};

class RGWAccessControlList_S3 : public RGWAccessControlList, public XMLObj
{
public:
  explicit RGWAccessControlList_S3(CephContext *_cct) : RGWAccessControlList(_cct) {}
  virtual ~RGWAccessControlList_S3() override {}

  bool xml_end(const char *el) override;
  void to_xml(std::ostream& out);

  int create_canned(ACLOwner& owner, ACLOwner& bucket_owner, const std::string& canned_acl);
  int create_from_grants(std::list<ACLGrant>& grants);
};

class ACLOwner_S3 : public ACLOwner, public XMLObj
{
public:
  ACLOwner_S3() {}
  virtual ~ACLOwner_S3() override {}

  bool xml_end(const char *el) override;
  void to_xml(std::ostream& out);
};

class RGWEnv;

class RGWAccessControlPolicy_S3 : public RGWAccessControlPolicy, public XMLObj
{
public:
  explicit RGWAccessControlPolicy_S3(CephContext *_cct) : RGWAccessControlPolicy(_cct) {}
  virtual ~RGWAccessControlPolicy_S3() override {}

  bool xml_end(const char *el) override;

  void to_xml(std::ostream& out);
  int rebuild(const DoutPrefixProvider *dpp, rgw::sal::Store* store, ACLOwner *owner,
	      RGWAccessControlPolicy& dest, std::string &err_msg);
  bool compare_group_name(std::string& id, ACLGroupTypeEnum group) override;

  virtual int create_canned(ACLOwner& _owner, ACLOwner& bucket_owner, const std::string& canned_acl) {
    RGWAccessControlList_S3& _acl = static_cast<RGWAccessControlList_S3 &>(acl);
    if (_owner.get_id() == rgw_user("anonymous")) {
      owner = bucket_owner;
    } else {
      owner = _owner;
    }
    int ret = _acl.create_canned(owner, bucket_owner, canned_acl);
    return ret;
  }
  int create_from_headers(const DoutPrefixProvider *dpp, rgw::sal::Store* store,
			  const RGWEnv *env, ACLOwner& _owner);
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

#endif
