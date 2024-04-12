// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include <string.h>

#include <iostream>
#include <map>

#include "include/types.h"
#include "common/split.h"

#include "rgw_acl_s3.h"
#include "rgw_user.h"
#include "rgw_sal.h"

#define dout_subsys ceph_subsys_rgw



#define RGW_URI_ALL_USERS	"http://acs.amazonaws.com/groups/global/AllUsers"
#define RGW_URI_AUTH_USERS	"http://acs.amazonaws.com/groups/global/AuthenticatedUsers"

using namespace std;

static string rgw_uri_all_users = RGW_URI_ALL_USERS;
static string rgw_uri_auth_users = RGW_URI_AUTH_USERS;

class ACLPermission_S3 : public XMLObj
{
public:
  uint32_t flags = 0;

  bool xml_end(const char *el) override;
};

void to_xml(ACLPermission perm, std::ostream& out)
{
  const uint32_t flags = perm.get_permissions();
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

bool ACLPermission_S3::xml_end(const char *el)
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
  static const char *to_string(ACLGranteeType type) {
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

class ACLGrantee_S3 : public XMLObj
{
public:
  ACLGrantee_S3() {}
  virtual ~ACLGrantee_S3() override {}

  bool xml_start(const char *el, const char **attr);
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

class ACLOwner_S3 : public XMLObj
{
public:
  std::string id;
  std::string display_name;

  bool xml_end(const char *el) override;
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

void to_xml(const ACLOwner& o, std::ostream& out)
{
  const std::string s = to_string(o.id);
  if (s.empty())
    return;
  out << "<Owner>" << "<ID>" << s << "</ID>";
  if (!o.display_name.empty())
    out << "<DisplayName>" << o.display_name << "</DisplayName>";
  out << "</Owner>";
}

class ACLGrant_S3 : public XMLObj
{
public:
  ACLGranteeType type;
  std::string id;
  std::string name;
  std::string uri;
  std::string email;
  ACLPermission_S3* permission = nullptr;

  bool xml_end(const char *el) override;
  bool xml_start(const char *el, const char **attr);
};

bool ACLGrant_S3::xml_end(const char *el) {
  ACLGrantee_S3 *acl_grantee;
  ACLID_S3 *acl_id;
  ACLURI_S3 *acl_uri;
  ACLEmail_S3 *acl_email;
  ACLDisplayName_S3 *acl_name;
  string uri;

  acl_grantee = static_cast<ACLGrantee_S3 *>(find_first("Grantee"));
  if (!acl_grantee)
    return false;
  string type_str;
  if (!acl_grantee->get_attr("xsi:type", type_str))
    return false;

  ACLGranteeType_S3::set(type_str.c_str(), type);

  permission = static_cast<ACLPermission_S3*>(find_first("Permission"));
  if (!permission)
    return false;

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

void to_xml(const ACLGrant& grant, ostream& out)
{
  const ACLPermission perm = grant.get_permission();

  /* only show s3 compatible permissions */
  if (!(perm.get_permissions() & RGW_PERM_ALL_S3))
    return;

  const std::string type = ACLGranteeType_S3::to_string(grant.get_type());

  out << "<Grant>" <<
         "<Grantee xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:type=\"" << type << "\">";

  if (const auto* user = grant.get_user(); user) {
    out << "<ID>" << user->id << "</ID>";
    if (user->name.size()) {
      out << "<DisplayName>" << user->name << "</DisplayName>";
    }
  } else if (const auto* email = grant.get_email(); email) {
    out << "<EmailAddress>" << email->address << "</EmailAddress>";
  } else if (const auto* group = grant.get_group(); group) {
    std::string uri;
    rgw::s3::acl_group_to_uri(group->type, uri);
    out << "<URI>" << uri << "</URI>";
  }
  out << "</Grantee>";
  to_xml(perm, out);
  out << "</Grant>";
}

class RGWAccessControlList_S3 : public XMLObj
{
public:
  bool xml_end(const char *el) override;
};

bool RGWAccessControlList_S3::xml_end(const char *el) {
  return true;
}

void to_xml(const RGWAccessControlList& acl, std::ostream& out)
{
  out << "<AccessControlList>";
  for (const auto& p : acl.get_grant_map()) {
    to_xml(p.second, out);
  }
  out << "</AccessControlList>";
}

struct s3_acl_header {
  int rgw_perm;
  const char *http_header;
};

static int read_owner_display_name(const DoutPrefixProvider* dpp,
                                   optional_yield y, rgw::sal::Driver* driver,
                                   const rgw_owner& owner, std::string& name)
{
  return std::visit(fu2::overload(
      [&] (const rgw_user& uid) {
        auto user = driver->get_user(uid);
        int r = user->load_user(dpp, y);
        if (r >= 0) {
          name = user->get_display_name();
        }
        return r;
      },
      [&] (const rgw_account_id& account_id) {
        RGWAccountInfo info;
        rgw::sal::Attrs attrs;
        RGWObjVersionTracker objv;
        int r = driver->load_account_by_id(dpp, y, account_id, info, attrs, objv);
        if (r >= 0) {
          name = info.name;
        }
        return r;
      }), owner);
}

static int read_aclowner_by_email(const DoutPrefixProvider* dpp,
                                  optional_yield y,
                                  rgw::sal::Driver* driver,
                                  std::string_view email,
                                  ACLOwner& aclowner)
{
  int ret = driver->load_owner_by_email(dpp, y, email, aclowner.id);
  if (ret < 0) {
    return ret;
  }
  return read_owner_display_name(dpp, y, driver, aclowner.id,
                                 aclowner.display_name);
}

static int parse_grantee_str(const DoutPrefixProvider* dpp,
                             optional_yield y,
                             rgw::sal::Driver* driver,
                             const std::string& grantee_str,
                             const s3_acl_header* perm,
                             ACLGrant& grant)
{
  string id_type, id_val_quoted;
  int rgw_perm = perm->rgw_perm;
  int ret;

  ret = parse_key_value(grantee_str, id_type, id_val_quoted);
  if (ret < 0)
    return ret;

  string id_val = rgw_trim_quotes(id_val_quoted);

  if (strcasecmp(id_type.c_str(), "emailAddress") == 0) {
    ACLOwner owner;
    ret = read_aclowner_by_email(dpp, y, driver, id_val, owner);
    if (ret < 0)
      return ret;

    grant.set_canon(owner.id, owner.display_name, rgw_perm);
  } else if (strcasecmp(id_type.c_str(), "id") == 0) {
    ACLOwner owner;
    owner.id = parse_owner(id_val);
    ret = read_owner_display_name(dpp, y, driver,
                                  owner.id, owner.display_name);
    if (ret < 0)
      return ret;

    grant.set_canon(owner.id, owner.display_name, rgw_perm);
  } else if (strcasecmp(id_type.c_str(), "uri") == 0) {
    ACLGroupTypeEnum gid = rgw::s3::acl_uri_to_group(id_val);
    if (gid == ACL_GROUP_NONE)
      return -EINVAL;

    grant.set_group(gid, rgw_perm);
  } else {
    return -EINVAL;
  }

  return 0;
}

static int parse_acl_header(const DoutPrefixProvider* dpp,
                            optional_yield y, rgw::sal::Driver* driver,
                            const RGWEnv& env, const s3_acl_header* perm,
                            RGWAccessControlList& acl)
{
  const char* hacl = env.get(perm->http_header, nullptr);
  if (hacl == nullptr) {
    return 0;
  }

  for (std::string_view grantee : ceph::split(hacl, ",")) {
    ACLGrant grant;
    int ret = parse_grantee_str(dpp, y, driver, std::string{grantee}, perm, grant);
    if (ret < 0)
      return ret;

    acl.add_grant(grant);
  }

  return 0;
}

static int create_canned(const ACLOwner& owner, const ACLOwner& bucket_owner,
                         const string& canned_acl, RGWAccessControlList& acl)
{
  const rgw_owner& bid = bucket_owner.id;
  const std::string& bname = bucket_owner.display_name;

  /* owner gets full control */
  {
    ACLGrant grant;
    grant.set_canon(owner.id, owner.display_name, RGW_PERM_FULL_CONTROL);
    acl.add_grant(grant);
  }

  if (canned_acl.size() == 0 || canned_acl.compare("private") == 0) {
    return 0;
  }

  if (canned_acl == "public-read") {
    ACLGrant grant;
    grant.set_group(ACL_GROUP_ALL_USERS, RGW_PERM_READ);
    acl.add_grant(grant);
  } else if (canned_acl == "public-read-write") {
    ACLGrant grant;
    grant.set_group(ACL_GROUP_ALL_USERS, RGW_PERM_READ);
    acl.add_grant(grant);
    grant.set_group(ACL_GROUP_ALL_USERS, RGW_PERM_WRITE);
    acl.add_grant(grant);
  } else if (canned_acl == "authenticated-read") {
    ACLGrant grant;
    grant.set_group(ACL_GROUP_AUTHENTICATED_USERS, RGW_PERM_READ);
    acl.add_grant(grant);
  } else if (canned_acl == "bucket-owner-read") {
    if (bid != owner.id) {
      ACLGrant grant;
      grant.set_canon(bid, bname, RGW_PERM_READ);
      acl.add_grant(grant);
    }
  } else if (canned_acl == "bucket-owner-full-control") {
    if (bid != owner.id) {
      ACLGrant grant;
      grant.set_canon(bid, bname, RGW_PERM_FULL_CONTROL);
      acl.add_grant(grant);
    }
  } else {
    return -EINVAL;
  }

  return 0;
}

class RGWAccessControlPolicy_S3 : public XMLObj
{
public:
  bool xml_end(const char *el) override;
};

bool RGWAccessControlPolicy_S3::xml_end(const char *el) {
  RGWAccessControlList_S3 *s3acl =
      static_cast<RGWAccessControlList_S3 *>(find_first("AccessControlList"));
  if (!s3acl)
    return false;

  ACLOwner_S3 *owner_p = static_cast<ACLOwner_S3 *>(find_first("Owner"));
  if (!owner_p)
    return false;
  return true;
}

void to_xml(const RGWAccessControlPolicy& p, std::ostream& out)
{
  out << "<AccessControlPolicy xmlns=\"" << XMLNS_AWS_S3 << "\">";
  to_xml(p.get_owner(), out);
  to_xml(p.get_acl(), out);
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

static int resolve_grant(const DoutPrefixProvider* dpp, optional_yield y,
                         rgw::sal::Driver* driver, ACLGrant_S3& xml_grant,
                         ACLGrant& grant, std::string& err_msg)
{
  const uint32_t perm = xml_grant.permission->flags;

  ACLOwner owner;
  switch (xml_grant.type.get_type()) {
  case ACL_TYPE_EMAIL_USER:
    if (xml_grant.email.empty()) {
      return -EINVAL;
    }
    if (read_aclowner_by_email(dpp, y, driver, xml_grant.email, owner) < 0) {
      ldpp_dout(dpp, 10) << "grant user email not found or other error" << dendl;
      err_msg = "The e-mail address you provided does not match any account on record.";
      return -ERR_UNRESOLVABLE_EMAIL;
    }
    grant.set_canon(owner.id, owner.display_name, perm);
    return 0;

  case ACL_TYPE_CANON_USER:
    owner.id = parse_owner(xml_grant.id);
    if (read_owner_display_name(dpp, y, driver, owner.id,
                                owner.display_name) < 0) {
      ldpp_dout(dpp, 10) << "grant user does not exist: " << xml_grant.id << dendl;
      err_msg = "Invalid CanonicalUser id";
      return -EINVAL;
    }
    grant.set_canon(owner.id, owner.display_name, perm);
    return 0;

  case ACL_TYPE_GROUP:
    if (const auto group = rgw::s3::acl_uri_to_group(xml_grant.uri);
        group != ACL_GROUP_NONE) {
      grant.set_group(group, perm);
      return 0;
    } else {
      ldpp_dout(dpp, 10) << "bad grant group: " << xml_grant.uri << dendl;
      err_msg = "Invalid group uri";
      return -EINVAL;
    }

  case ACL_TYPE_REFERER:
  case ACL_TYPE_UNKNOWN:
  default:
    err_msg = "Invalid Grantee type";
    return -EINVAL;
  }
}

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

XMLObj *RGWACLXMLParser_S3::alloc_obj(const char *el)
{
  XMLObj * obj = NULL;
  if (strcmp(el, "AccessControlPolicy") == 0) {
    obj = new RGWAccessControlPolicy_S3();
  } else if (strcmp(el, "Owner") == 0) {
    obj = new ACLOwner_S3();
  } else if (strcmp(el, "AccessControlList") == 0) {
    obj = new RGWAccessControlList_S3();
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

namespace rgw::s3 {

ACLGroupTypeEnum acl_uri_to_group(std::string_view uri)
{
  if (uri == rgw_uri_all_users)
    return ACL_GROUP_ALL_USERS;
  else if (uri == rgw_uri_auth_users)
    return ACL_GROUP_AUTHENTICATED_USERS;

  return ACL_GROUP_NONE;
}

bool acl_group_to_uri(ACLGroupTypeEnum group, std::string& uri)
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

int parse_policy(const DoutPrefixProvider* dpp, optional_yield y,
                 rgw::sal::Driver* driver, std::string_view document,
                 RGWAccessControlPolicy& policy, std::string& err_msg)
{
  RGWACLXMLParser_S3 parser(dpp->get_cct());
  if (!parser.init()) {
    return -EINVAL;
  }
  if (!parser.parse(document.data(), document.size(), 1)) {
    return -EINVAL;
  }

  const auto xml_root = static_cast<RGWAccessControlPolicy_S3*>(
      parser.find_first("AccessControlPolicy"));
  if (!xml_root) {
    err_msg = "Missing element AccessControlPolicy";
    return -EINVAL;
  }

  const auto xml_owner = static_cast<ACLOwner_S3*>(
      xml_root->find_first("Owner"));
  if (!xml_owner) {
    err_msg = "Missing element Owner";
    return -EINVAL;
  }

  ACLOwner& owner = policy.get_owner();
  owner.id = parse_owner(xml_owner->id);

  // owner must exist
  int r = read_owner_display_name(dpp, y, driver, owner.id, owner.display_name);
  if (r < 0) {
    ldpp_dout(dpp, 10) << "acl owner " << owner.id << " does not exist" << dendl;
    err_msg = "Invalid Owner ID";
    return -EINVAL;
  }
  if (!xml_owner->display_name.empty()) {
    owner.display_name = xml_owner->display_name;
  }

  const auto xml_acl = static_cast<ACLOwner_S3*>(
      xml_root->find_first("AccessControlList"));
  if (!xml_acl) {
    err_msg = "Missing element AccessControlList";
    return -EINVAL;
  }

  // iterate parsed grants
  XMLObjIter iter = xml_acl->find("Grant");
  ACLGrant_S3* xml_grant = static_cast<ACLGrant_S3*>(iter.get_next());
  while (xml_grant) {
    ACLGrant grant;
    r = resolve_grant(dpp, y, driver, *xml_grant, grant, err_msg);
    if (r < 0) {
      return r;
    }
    policy.get_acl().add_grant(grant);
    xml_grant = static_cast<ACLGrant_S3*>(iter.get_next());
  }

  return 0;
}

void write_policy_xml(const RGWAccessControlPolicy& policy,
                      std::ostream& out)
{
  to_xml(policy, out);
}

int create_canned_acl(const ACLOwner& owner,
                      const ACLOwner& bucket_owner,
                      const std::string& canned_acl,
                      RGWAccessControlPolicy& policy)
{
  if (owner.id == parse_owner("anonymous")) {
    policy.set_owner(bucket_owner);
  } else {
    policy.set_owner(owner);
  }
  return create_canned(owner, bucket_owner, canned_acl, policy.get_acl());
}

int create_policy_from_headers(const DoutPrefixProvider* dpp,
                               optional_yield y,
                               rgw::sal::Driver* driver,
                               const ACLOwner& owner,
                               const RGWEnv& env,
                               RGWAccessControlPolicy& policy)
{
  policy.set_owner(owner);
  auto& acl = policy.get_acl();

  for (const s3_acl_header* p = acl_header_perms; p->rgw_perm; p++) {
    int r = parse_acl_header(dpp, y, driver, env, p, acl);
    if (r < 0) {
      return r;
    }
  }

  return 0;
}

} // namespace rgw::s3
