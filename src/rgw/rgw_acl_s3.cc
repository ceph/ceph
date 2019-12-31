// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include <string.h>

#include <iostream>
#include <map>

#include "include/types.h"

#include "rgw_acl_s3.h"
#include "rgw_user.h"

#define dout_subsys ceph_subsys_rgw



#define RGW_URI_ALL_USERS	"http://acs.amazonaws.com/groups/global/AllUsers"
#define RGW_URI_AUTH_USERS	"http://acs.amazonaws.com/groups/global/AuthenticatedUsers"

static string rgw_uri_all_users = RGW_URI_ALL_USERS;
static string rgw_uri_auth_users = RGW_URI_AUTH_USERS;

void ACLPermission_S3::to_xml(ostream& out)
{
  if ((flags & RGW_PERM_FULL_CONTROL) == RGW_PERM_FULL_CONTROL) {
   out << "<Permission>FULL_CONTROL</Permission>";
  } else {
    if (flags & RGW_PERM_READ)
      out << "<Permission>READ</Permission>";
    if (flags & RGW_PERM_WRITE)
      out << "<Permission>WRITE</Permission>";
    if (flags & RGW_PERM_READ_ACP)
      out << "<Permission>READ_ACP</Permission>";
    if (flags & RGW_PERM_WRITE_ACP)
      out << "<Permission>WRITE_ACP</Permission>";
  }
}

bool ACLPermission_S3::
xml_end(const char *el)
{
  const char *s = data.c_str();
  if (strcasecmp(s, "READ") == 0) {
    flags |= RGW_PERM_READ;
    return true;
  } else if (strcasecmp(s, "WRITE") == 0) {
    flags |= RGW_PERM_WRITE;
    return true;
  } else if (strcasecmp(s, "READ_ACP") == 0) {
    flags |= RGW_PERM_READ_ACP;
    return true;
  } else if (strcasecmp(s, "WRITE_ACP") == 0) {
    flags |= RGW_PERM_WRITE_ACP;
    return true;
  } else if (strcasecmp(s, "FULL_CONTROL") == 0) {
    flags |= RGW_PERM_FULL_CONTROL;
    return true;
  }
  return false;
}


class ACLGranteeType_S3 {
public:
  static const char *to_string(ACLGranteeType& type) {
    switch (type.get_type()) {
    case ACL_TYPE_CANON_USER:
      return "CanonicalUser";
    case ACL_TYPE_EMAIL_USER:
      return "AmazonCustomerByEmail";
    case ACL_TYPE_GROUP:
      return "Group";
     default:
      return "unknown";
    }
  }

  static void set(const char *s, ACLGranteeType& type) {
    if (!s) {
      type.set(ACL_TYPE_UNKNOWN);
      return;
    }
    if (strcmp(s, "CanonicalUser") == 0)
      type.set(ACL_TYPE_CANON_USER);
    else if (strcmp(s, "AmazonCustomerByEmail") == 0)
      type.set(ACL_TYPE_EMAIL_USER);
    else if (strcmp(s, "Group") == 0)
      type.set(ACL_TYPE_GROUP);
    else
      type.set(ACL_TYPE_UNKNOWN);
  }
};

class ACLID_S3 : public XMLObj
{
public:
  ACLID_S3() {}
  ~ACLID_S3() override {}
  string& to_str() { return data; }
};

class ACLURI_S3 : public XMLObj
{
public:
  ACLURI_S3() {}
  ~ACLURI_S3() override {}
};

class ACLEmail_S3 : public XMLObj
{
public:
  ACLEmail_S3() {}
  ~ACLEmail_S3() override {}
};

class ACLDisplayName_S3 : public XMLObj
{
public:
 ACLDisplayName_S3() {}
 ~ACLDisplayName_S3() override {}
};

bool ACLOwner_S3::xml_end(const char *el) {
  ACLID_S3 *acl_id = static_cast<ACLID_S3 *>(find_first("ID"));
  ACLID_S3 *acl_name = static_cast<ACLID_S3 *>(find_first("DisplayName"));

  // ID is mandatory
  if (!acl_id)
    return false;
  id = acl_id->get_data();

  // DisplayName is optional
  if (acl_name)
    display_name = acl_name->get_data();
  else
    display_name = "";

  return true;
}

void  ACLOwner_S3::to_xml(ostream& out) {
  string s;
  id.to_str(s);
  if (s.empty())
    return;
  out << "<Owner>" << "<ID>" << s << "</ID>";
  if (!display_name.empty())
    out << "<DisplayName>" << display_name << "</DisplayName>";
  out << "</Owner>";
}

bool ACLGrant_S3::xml_end(const char *el) {
  ACLGrantee_S3 *acl_grantee;
  ACLID_S3 *acl_id;
  ACLURI_S3 *acl_uri;
  ACLEmail_S3 *acl_email;
  ACLPermission_S3 *acl_permission;
  ACLDisplayName_S3 *acl_name;
  string uri;

  acl_grantee = static_cast<ACLGrantee_S3 *>(find_first("Grantee"));
  if (!acl_grantee)
    return false;
  string type_str;
  if (!acl_grantee->get_attr("xsi:type", type_str))
    return false;
  ACLGranteeType_S3::set(type_str.c_str(), type);
  
  acl_permission = static_cast<ACLPermission_S3 *>(find_first("Permission"));
  if (!acl_permission)
    return false;

  permission = *acl_permission;

  id.clear();
  name.clear();
  email.clear();

  switch (type.get_type()) {
  case ACL_TYPE_CANON_USER:
    acl_id = static_cast<ACLID_S3 *>(acl_grantee->find_first("ID"));
    if (!acl_id)
      return false;
    id = acl_id->to_str();
    acl_name = static_cast<ACLDisplayName_S3 *>(acl_grantee->find_first("DisplayName"));
    if (acl_name)
      name = acl_name->get_data();
    break;
  case ACL_TYPE_GROUP:
    acl_uri = static_cast<ACLURI_S3 *>(acl_grantee->find_first("URI"));
    if (!acl_uri)
      return false;
    uri = acl_uri->get_data();
    group = uri_to_group(uri);
    break;
  case ACL_TYPE_EMAIL_USER:
    acl_email = static_cast<ACLEmail_S3 *>(acl_grantee->find_first("EmailAddress"));
    if (!acl_email)
      return false;
    email = acl_email->get_data();
    break;
  default:
    // unknown user type
    return false;
  };
  return true;
}

void ACLGrant_S3::to_xml(CephContext *cct, ostream& out) {
  ACLPermission_S3& perm = static_cast<ACLPermission_S3 &>(permission);

  /* only show s3 compatible permissions */
  if (!(perm.get_permissions() & RGW_PERM_ALL_S3))
    return;

  string uri;

  out << "<Grant>" <<
         "<Grantee xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:type=\"" << ACLGranteeType_S3::to_string(type) << "\">";
  switch (type.get_type()) {
  case ACL_TYPE_CANON_USER:
    out << "<ID>" << id << "</ID>";
    if (name.size()) {
      out << "<DisplayName>" << name << "</DisplayName>";
    }
    break;
  case ACL_TYPE_EMAIL_USER:
    out << "<EmailAddress>" << email << "</EmailAddress>";
    break;
  case ACL_TYPE_GROUP:
    if (!group_to_uri(group, uri)) {
      ldout(cct, 0) << "ERROR: group_to_uri failed with group=" << (int)group << dendl;
      break;
    }
    out << "<URI>" << uri << "</URI>";
    break;
  default:
    break;
  }
  out << "</Grantee>";
  perm.to_xml(out);
  out << "</Grant>";
}

bool ACLGrant_S3::group_to_uri(ACLGroupTypeEnum group, string& uri)
{
  switch (group) {
  case ACL_GROUP_ALL_USERS:
    uri = rgw_uri_all_users;
    return true;
  case ACL_GROUP_AUTHENTICATED_USERS:
    uri = rgw_uri_auth_users;
    return true;
  default:
    return false;
  }
}

bool RGWAccessControlList_S3::xml_end(const char *el) {
  XMLObjIter iter = find("Grant");
  ACLGrant_S3 *grant = static_cast<ACLGrant_S3 *>(iter.get_next());
  while (grant) {
    add_grant(grant);
    grant = static_cast<ACLGrant_S3 *>(iter.get_next());
  }
  return true;
}

void  RGWAccessControlList_S3::to_xml(ostream& out) {
  multimap<string, ACLGrant>::iterator iter;
  out << "<AccessControlList>";
  for (iter = grant_map.begin(); iter != grant_map.end(); ++iter) {
    ACLGrant_S3& grant = static_cast<ACLGrant_S3 &>(iter->second);
    grant.to_xml(cct, out);
  }
  out << "</AccessControlList>";
}

struct s3_acl_header {
  int rgw_perm;
  const char *http_header;
};

static const char *get_acl_header(const RGWEnv *env,
        const struct s3_acl_header *perm)
{
  const char *header = perm->http_header;

  return env->get(header, NULL);
}

static int parse_grantee_str(RGWUserCtl *user_ctl, string& grantee_str,
        const struct s3_acl_header *perm, ACLGrant& grant)
{
  string id_type, id_val_quoted;
  int rgw_perm = perm->rgw_perm;
  int ret;

  RGWUserInfo info;

  ret = parse_key_value(grantee_str, id_type, id_val_quoted);
  if (ret < 0)
    return ret;

  string id_val = rgw_trim_quotes(id_val_quoted);

  if (strcasecmp(id_type.c_str(), "emailAddress") == 0) {
    ret = user_ctl->get_info_by_email(id_val, &info, null_yield);
    if (ret < 0)
      return ret;

    grant.set_canon(info.user_id, info.display_name, rgw_perm);
  } else if (strcasecmp(id_type.c_str(), "id") == 0) {
    rgw_user user(id_val);
    ret = user_ctl->get_info_by_uid(user, &info, null_yield);
    if (ret < 0)
      return ret;

    grant.set_canon(info.user_id, info.display_name, rgw_perm);
  } else if (strcasecmp(id_type.c_str(), "uri") == 0) {
    ACLGroupTypeEnum gid = grant.uri_to_group(id_val);
    if (gid == ACL_GROUP_NONE)
      return -EINVAL;

    grant.set_group(gid, rgw_perm);
  } else {
    return -EINVAL;
  }

  return 0;
}

static int parse_acl_header(RGWUserCtl *user_ctl, const RGWEnv *env,
         const struct s3_acl_header *perm, std::list<ACLGrant>& _grants)
{
  std::list<string> grantees;
  std::string hacl_str;

  const char *hacl = get_acl_header(env, perm);
  if (hacl == NULL)
    return 0;

  hacl_str = hacl;
  get_str_list(hacl_str, ",", grantees);

  for (list<string>::iterator it = grantees.begin(); it != grantees.end(); ++it) {
    ACLGrant grant;
    int ret = parse_grantee_str(user_ctl, *it, perm, grant);
    if (ret < 0)
      return ret;

    _grants.push_back(grant);
  }

  return 0;
}

int RGWAccessControlList_S3::create_canned(ACLOwner& owner, ACLOwner& bucket_owner, const string& canned_acl)
{
  acl_user_map.clear();
  grant_map.clear();

  ACLGrant owner_grant;

  rgw_user bid = bucket_owner.get_id();
  string bname = bucket_owner.get_display_name();

  /* owner gets full control */
  owner_grant.set_canon(owner.get_id(), owner.get_display_name(), RGW_PERM_FULL_CONTROL);
  add_grant(&owner_grant);

  if (canned_acl.size() == 0 || canned_acl.compare("private") == 0) {
    return 0;
  }

  ACLGrant bucket_owner_grant;
  ACLGrant group_grant;
  if (canned_acl.compare("public-read") == 0) {
    group_grant.set_group(ACL_GROUP_ALL_USERS, RGW_PERM_READ);
    add_grant(&group_grant);
  } else if (canned_acl.compare("public-read-write") == 0) {
    group_grant.set_group(ACL_GROUP_ALL_USERS, RGW_PERM_READ);
    add_grant(&group_grant);
    group_grant.set_group(ACL_GROUP_ALL_USERS, RGW_PERM_WRITE);
    add_grant(&group_grant);
  } else if (canned_acl.compare("authenticated-read") == 0) {
    group_grant.set_group(ACL_GROUP_AUTHENTICATED_USERS, RGW_PERM_READ);
    add_grant(&group_grant);
  } else if (canned_acl.compare("bucket-owner-read") == 0) {
    bucket_owner_grant.set_canon(bid, bname, RGW_PERM_READ);
    if (bid.compare(owner.get_id()) != 0)
      add_grant(&bucket_owner_grant);
  } else if (canned_acl.compare("bucket-owner-full-control") == 0) {
    bucket_owner_grant.set_canon(bid, bname, RGW_PERM_FULL_CONTROL);
    if (bid.compare(owner.get_id()) != 0)
      add_grant(&bucket_owner_grant);
  } else {
    return -EINVAL;
  }

  return 0;
}

int RGWAccessControlList_S3::create_from_grants(std::list<ACLGrant>& grants)
{
  if (grants.empty())
    return -EINVAL;

  acl_user_map.clear();
  grant_map.clear();

  for (std::list<ACLGrant>::iterator it = grants.begin(); it != grants.end(); ++it) {
    ACLGrant g = *it;
    add_grant(&g);
  }

  return 0;
}

bool RGWAccessControlPolicy_S3::xml_end(const char *el) {
  RGWAccessControlList_S3 *s3acl =
      static_cast<RGWAccessControlList_S3 *>(find_first("AccessControlList"));
  if (!s3acl)
    return false;

  acl = *s3acl;

  ACLOwner *owner_p = static_cast<ACLOwner_S3 *>(find_first("Owner"));
  if (!owner_p)
    return false;
  owner = *owner_p;
  return true;
}

void  RGWAccessControlPolicy_S3::to_xml(ostream& out) {
  out << "<AccessControlPolicy xmlns=\"" << XMLNS_AWS_S3 << "\">";
  ACLOwner_S3& _owner = static_cast<ACLOwner_S3 &>(owner);
  RGWAccessControlList_S3& _acl = static_cast<RGWAccessControlList_S3 &>(acl);
  _owner.to_xml(out);
  _acl.to_xml(out);
  out << "</AccessControlPolicy>";
}

static const s3_acl_header acl_header_perms[] = {
  {RGW_PERM_READ, "HTTP_X_AMZ_GRANT_READ"},
  {RGW_PERM_WRITE, "HTTP_X_AMZ_GRANT_WRITE"},
  {RGW_PERM_READ_ACP,"HTTP_X_AMZ_GRANT_READ_ACP"},
  {RGW_PERM_WRITE_ACP, "HTTP_X_AMZ_GRANT_WRITE_ACP"},
  {RGW_PERM_FULL_CONTROL, "HTTP_X_AMZ_GRANT_FULL_CONTROL"},
  {0, NULL}
};

int RGWAccessControlPolicy_S3::create_from_headers(RGWUserCtl *user_ctl, const RGWEnv *env, ACLOwner& _owner)
{
  std::list<ACLGrant> grants;
  int r = 0;

  for (const struct s3_acl_header *p = acl_header_perms; p->rgw_perm; p++) {
    r = parse_acl_header(user_ctl, env, p, grants);
    if (r < 0) {
      return r;
    }
  }

  RGWAccessControlList_S3& _acl = static_cast<RGWAccessControlList_S3 &>(acl);
  r = _acl.create_from_grants(grants);

  owner = _owner;

  return r;
}

/*
  can only be called on object that was parsed
 */
int RGWAccessControlPolicy_S3::rebuild(RGWUserCtl *user_ctl, ACLOwner *owner, RGWAccessControlPolicy& dest,
                                       std::string &err_msg)
{
  if (!owner)
    return -EINVAL;

  ACLOwner *requested_owner = static_cast<ACLOwner_S3 *>(find_first("Owner"));
  if (requested_owner) {
    rgw_user& requested_id = requested_owner->get_id();
    if (!requested_id.empty() && requested_id.compare(owner->get_id()) != 0)
      return -EPERM;
  }

  RGWUserInfo owner_info;
  if (user_ctl->get_info_by_uid(owner->get_id(), &owner_info, null_yield) < 0) {
    ldout(cct, 10) << "owner info does not exist" << dendl;
    err_msg = "Invalid id";
    return -EINVAL;
  }
  ACLOwner& dest_owner = dest.get_owner();
  dest_owner.set_id(owner->get_id());
  dest_owner.set_name(owner_info.display_name);

  ldout(cct, 20) << "owner id=" << owner->get_id() << dendl;
  ldout(cct, 20) << "dest owner id=" << dest.get_owner().get_id() << dendl;

  RGWAccessControlList& dst_acl = dest.get_acl();

  multimap<string, ACLGrant>& grant_map = acl.get_grant_map();
  multimap<string, ACLGrant>::iterator iter;
  for (iter = grant_map.begin(); iter != grant_map.end(); ++iter) {
    ACLGrant& src_grant = iter->second;
    ACLGranteeType& type = src_grant.get_type();
    ACLGrant new_grant;
    bool grant_ok = false;
    rgw_user uid;
    RGWUserInfo grant_user;
    switch (type.get_type()) {
    case ACL_TYPE_EMAIL_USER:
      {
        string email;
        rgw_user u;
        if (!src_grant.get_id(u)) {
          ldout(cct, 0) << "ERROR: src_grant.get_id() failed" << dendl;
          return -EINVAL;
        }
        email = u.id;
        ldout(cct, 10) << "grant user email=" << email << dendl;
        if (user_ctl->get_info_by_email(email, &grant_user, null_yield) < 0) {
          ldout(cct, 10) << "grant user email not found or other error" << dendl;
          err_msg = "The e-mail address you provided does not match any account on record.";
          return -ERR_UNRESOLVABLE_EMAIL;
        }
        uid = grant_user.user_id;
      }
    case ACL_TYPE_CANON_USER:
      {
        if (type.get_type() == ACL_TYPE_CANON_USER) {
          if (!src_grant.get_id(uid)) {
            ldout(cct, 0) << "ERROR: src_grant.get_id() failed" << dendl;
            err_msg = "Invalid id";
            return -EINVAL;
          }
        }
    
        if (grant_user.user_id.empty() && user_ctl->get_info_by_uid(uid, &grant_user, null_yield) < 0) {
          ldout(cct, 10) << "grant user does not exist:" << uid << dendl;
          err_msg = "Invalid id";
          return -EINVAL;
        } else {
          ACLPermission& perm = src_grant.get_permission();
          new_grant.set_canon(uid, grant_user.display_name, perm.get_permissions());
          grant_ok = true;
          rgw_user new_id;
          new_grant.get_id(new_id);
          ldout(cct, 10) << "new grant: " << new_id << ":" << grant_user.display_name << dendl;
        }
      }
      break;
    case ACL_TYPE_GROUP:
      {
        string uri;
        if (ACLGrant_S3::group_to_uri(src_grant.get_group(), uri)) {
          new_grant = src_grant;
          grant_ok = true;
          ldout(cct, 10) << "new grant: " << uri << dendl;
        } else {
          ldout(cct, 10) << "bad grant group:" << (int)src_grant.get_group() << dendl;
          err_msg = "Invalid group uri";
          return -EINVAL;
        }
      }
    default:
      break;
    }
    if (grant_ok) {
      dst_acl.add_grant(&new_grant);
    }
  }

  return 0; 
}

bool RGWAccessControlPolicy_S3::compare_group_name(string& id, ACLGroupTypeEnum group)
{
  switch (group) {
  case ACL_GROUP_ALL_USERS:
    return (id.compare(RGW_USER_ANON_ID) == 0);
  case ACL_GROUP_AUTHENTICATED_USERS:
    return (id.compare(rgw_uri_auth_users) == 0);
  default:
    return id.empty();
  }

  // shouldn't get here
  return false;
}

XMLObj *RGWACLXMLParser_S3::alloc_obj(const char *el)
{
  XMLObj * obj = NULL;
  if (strcmp(el, "AccessControlPolicy") == 0) {
    obj = new RGWAccessControlPolicy_S3(cct);
  } else if (strcmp(el, "Owner") == 0) {
    obj = new ACLOwner_S3();
  } else if (strcmp(el, "AccessControlList") == 0) {
    obj = new RGWAccessControlList_S3(cct);
  } else if (strcmp(el, "ID") == 0) {
    obj = new ACLID_S3();
  } else if (strcmp(el, "DisplayName") == 0) {
    obj = new ACLDisplayName_S3();
  } else if (strcmp(el, "Grant") == 0) {
    obj = new ACLGrant_S3();
  } else if (strcmp(el, "Grantee") == 0) {
    obj = new ACLGrantee_S3();
  } else if (strcmp(el, "Permission") == 0) {
    obj = new ACLPermission_S3();
  } else if (strcmp(el, "URI") == 0) {
    obj = new ACLURI_S3();
  } else if (strcmp(el, "EmailAddress") == 0) {
    obj = new ACLEmail_S3();
  }

  return obj;
}

