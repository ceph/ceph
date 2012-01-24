#ifndef CEPH_RGW_ACL_S3_H
#define CEPH_RGW_ACL_S3_H

#include <map>
#include <string>
#include <iostream>
#include <include/types.h>

#include <expat.h>

#include "rgw_xml.h"
#include "rgw_acl.h"

using namespace std;

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

  void to_xml(ostream& out);
  bool xml_end(const char *el);
  bool xml_start(const char *el, const char **attr);
};

class RGWAccessControlList_S3 : public RGWAccessControlList, public XMLObj
{
public:
  RGWAccessControlList_S3();
  ~RGWAccessControlList_S3();

  bool xml_end(const char *el);
  void to_xml(ostream& out) {
    multimap<string, ACLGrant>::iterator iter;
    out << "<AccessControlList>";
    for (iter = grant_map.begin(); iter != grant_map.end(); ++iter) {
      ACLGrant_S3& grant = static_cast<ACLGrant_S3 &>(iter->second);
      grant.to_xml(out);
    }
    out << "</AccessControlList>";
  }

  bool create_canned(string id, string name, string canned_acl);
};

class ACLOwner_S3 : public ACLOwner, public XMLObj
{
public:
  ACLOwner_S3() {}
  ~ACLOwner_S3() {}

  bool xml_end(const char *el);
  void to_xml(ostream& out) {
    if (id.empty())
      return;
    out << "<Owner>" << "<ID>" << id << "</ID>";
    if (!display_name.empty())
      out << "<DisplayName>" << display_name << "</DisplayName>";
    out << "</Owner>";
  }
};

class RGWAccessControlPolicy_S3 : public RGWAccessControlPolicy, public XMLObj
{
public:
  RGWAccessControlPolicy_S3() {}
  ~RGWAccessControlPolicy_S3() {}

  bool xml_end(const char *el);

  void to_xml(ostream& out) {
    out << "<AccessControlPolicy xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">";
    ACLOwner_S3& _owner = static_cast<ACLOwner_S3 &>(owner);
    RGWAccessControlList_S3& _acl = static_cast<RGWAccessControlList_S3 &>(acl);
    _owner.to_xml(out);
    _acl.to_xml(out);
    out << "</AccessControlPolicy>";
  }
  bool compare_group_name(string& id, ACLGroupTypeEnum group);
};

/**
 * Interfaces with the webserver's XML handling code
 * to parse it in a way that makes sense for the rgw.
 */
class RGWACLXMLParser_S3 : public RGWXMLParser
{
  XMLObj *alloc_obj(const char *el);
public:
  RGWACLXMLParser_S3() {}
};

#endif
