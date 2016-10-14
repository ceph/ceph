// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_RGW_ACL_S3_H
#define CEPH_RGW_ACL_S3_H

#include <map>
#include <string>
#include <iosfwd>
#include <include/types.h>

#include "include/str_list.h"
#include "rgw_xml.h"
#include "rgw_acl.h"


using namespace std;

class RGWRados;

class ACLPermission_S3 : public ACLPermission, public XMLObj
{
public:
  ACLPermission_S3() {}
  ~ACLPermission_S3() {}

  bool xml_end(const char *el);
  void to_xml(ostream& out);
};

class ACLGrantee_S3 : public ACLGrantee, public XMLObj
{
public:
  ACLGrantee_S3() {}
  ~ACLGrantee_S3() {}

  bool xml_start(const char *el, const char **attr);
};


class ACLGrant_S3 : public ACLGrant, public XMLObj
{
public:
  ACLGrant_S3() {}
  ~ACLGrant_S3() {}

  void to_xml(CephContext *cct, ostream& out);
  bool xml_end(const char *el);
  bool xml_start(const char *el, const char **attr);

  static ACLGroupTypeEnum uri_to_group(string& uri);
  static bool group_to_uri(ACLGroupTypeEnum group, string& uri);
};

class RGWAccessControlList_S3 : public RGWAccessControlList, public XMLObj
{
public:
  explicit RGWAccessControlList_S3(CephContext *_cct) : RGWAccessControlList(_cct) {}
  ~RGWAccessControlList_S3() {}

  bool xml_end(const char *el);
  void to_xml(ostream& out);

  int create_canned(ACLOwner& owner, ACLOwner& bucket_owner, const string& canned_acl);
  int create_from_grants(std::list<ACLGrant>& grants);
};

class ACLOwner_S3 : public ACLOwner, public XMLObj
{
public:
  ACLOwner_S3() {}
  ~ACLOwner_S3() {}

  bool xml_end(const char *el);
  void to_xml(ostream& out);
};

class RGWEnv;

class RGWAccessControlPolicy_S3 : public RGWAccessControlPolicy, public XMLObj
{
public:
  explicit RGWAccessControlPolicy_S3(CephContext *_cct) : RGWAccessControlPolicy(_cct) {}
  ~RGWAccessControlPolicy_S3() {}

  bool xml_end(const char *el);

  void to_xml(ostream& out);
  int rebuild(RGWRados *store, ACLOwner *owner, RGWAccessControlPolicy& dest);
  bool compare_group_name(string& id, ACLGroupTypeEnum group);

  virtual int create_canned(ACLOwner& _owner, ACLOwner& bucket_owner, string canned_acl) {
    RGWAccessControlList_S3& _acl = static_cast<RGWAccessControlList_S3 &>(acl);
    int ret = _acl.create_canned(_owner, bucket_owner, canned_acl);
    owner = _owner;
    return ret;
  }
  int create_from_headers(RGWRados *store, RGWEnv *env, ACLOwner& _owner);
};

/**
 * Interfaces with the webserver's XML handling code
 * to parse it in a way that makes sense for the rgw.
 */
class RGWACLXMLParser_S3 : public RGWXMLParser
{
  CephContext *cct;

  XMLObj *alloc_obj(const char *el);
public:
  explicit RGWACLXMLParser_S3(CephContext *_cct) : cct(_cct) {}
};

#endif
