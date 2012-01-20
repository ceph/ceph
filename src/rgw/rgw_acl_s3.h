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
  ACLPermission *alloc_permission() {
    return new ACLPermission_S3;
  }
  void free_permission(ACLPermission *perm) {
    delete perm;
  }
public:
  ACLGrant_S3() {}
  ~ACLGrant_S3() {}

  bool xml_end(const char *el);
  void to_xml(ostream& out) {
    out << "<Grant>" <<
            "<Grantee xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:type=\"" << type.to_string() << "\">";
    switch (type.get_type()) {
    case ACL_TYPE_CANON_USER:
      out << "<ID>" << id << "</ID>" <<
             "<DisplayName>" << name << "</DisplayName>";
      break;
    case ACL_TYPE_EMAIL_USER:
      out << "<EmailAddress>" << email << "</EmailAddress>";
      break;
    case ACL_TYPE_GROUP:
       out << "<URI>" << uri << "</URI>";
      break;
    default:
      break;
    }
    out << "</Grantee>";
    ((ACLPermission_S3 *)permission)->to_xml(out);
    out << "</Grant>";
  }
  bool xml_start(const char *el, const char **attr);
};

class RGWAccessControlList_S3 : public RGWAccessControlList, public XMLObj
{
public:
  RGWAccessControlList_S3();
  ~RGWAccessControlList_S3();

  bool xml_end(const char *el);
  void to_xml(ostream& out) {
    map<string, ACLGrant>::iterator iter;
    out << "<AccessControlList>";
    for (iter = grant_map.begin(); iter != grant_map.end(); ++iter) {
      ACLGrant& grant = iter->second;
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
    owner.to_xml(out);
    acl.to_xml(out);
    out << "</AccessControlPolicy>";
  }
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
