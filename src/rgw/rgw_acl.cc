#include <string.h>

#include <iostream>
#include <map>

#include "include/types.h"

#include "rgw_acl.h"
#include "rgw_user.h"

using namespace std;

static string rgw_uri_all_users = RGW_URI_ALL_USERS;
static string rgw_uri_auth_users = RGW_URI_AUTH_USERS;

XMLObjIter::
XMLObjIter()
{
}

XMLObjIter::
~XMLObjIter()
{
}

void XMLObjIter::
set(const XMLObjIter::map_iter_t &_cur, const XMLObjIter::map_iter_t &_end)
{
  cur = _cur;
  end = _end;
}

XMLObj *XMLObjIter::
get_next()
{
  XMLObj *obj = NULL;
  if (cur != end) {
    obj = cur->second;
    ++cur;
  }
  return obj;
};

ostream& operator<<(ostream& out, XMLObj& obj) {
   out << obj.type << ": " << obj.data;
   return out;
}

XMLObj::
~XMLObj()
{
}

bool XMLObj::
xml_start(XMLObj *parent, const char *el, const char **attr)
{
  this->parent = parent;
  type = el;
  for (int i = 0; attr[i]; i += 2) {
    attr_map[attr[i]] = string(attr[i + 1]);
  }
  return true;
}

bool XMLObj::
xml_end(const char *el)
{
  return true;
}

void XMLObj::
xml_handle_data(const char *s, int len)
{
  data = string(s, len);
}

string& XMLObj::
XMLObj::get_data()
{
  return data;
}

XMLObj *XMLObj::
XMLObj::get_parent()
{
  return parent;
}

void XMLObj::
add_child(string el, XMLObj *obj)
{
  children.insert(pair<string, XMLObj *>(el, obj));
}

bool XMLObj::
get_attr(string name, string& attr)
{
  map<string, string>::iterator iter = attr_map.find(name);
  if (iter == attr_map.end())
    return false;
  attr = iter->second;
  return true;
}

XMLObjIter XMLObj::
find(string name)
{
  XMLObjIter iter;
  map<string, XMLObj *>::iterator first;
  map<string, XMLObj *>::iterator last;
  first = children.find(name);
  last = children.upper_bound(name);
  iter.set(first, last);
  return iter;
}

XMLObj *XMLObj::
find_first(string name)
{
  XMLObjIter iter;
  map<string, XMLObj *>::iterator first;
  first = children.find(name);
  if (first != children.end())
    return first->second;
  return NULL;
}

ACLPermission::
ACLPermission() : flags(0)
{
}

ACLPermission::~ACLPermission()
{
}

int ACLPermission::
get_permissions()
{
  return flags;
}

void ACLPermission::
set_permissions(int perm)
{
  flags = perm;
}

void ACLPermission::
to_xml(ostream& out)
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

bool ACLPermission::
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

ACLGranteeType::
ACLGranteeType() : type(ACL_TYPE_UNKNOWN)
{
}

ACLGranteeType::
~ACLGranteeType()
{
}

const char *ACLGranteeType::
to_string()
{
  switch (type) {
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

ACLGranteeTypeEnum ACLGranteeType::
get_type()
{
  return (ACLGranteeTypeEnum)type;
};

void ACLGranteeType::
set(ACLGranteeTypeEnum t)
{
  type = t;
}

void ACLGranteeType::
set(const char *s)
{
  if (!s) {
    type = ACL_TYPE_UNKNOWN;
    return;
  }
  if (strcmp(s, "CanonicalUser") == 0)
    type = ACL_TYPE_CANON_USER;
  else if (strcmp(s, "AmazonCustomerByEmail") == 0)
    type = ACL_TYPE_EMAIL_USER;
  else if (strcmp(s, "Group") == 0)
    type = ACL_TYPE_GROUP;
  else
    type = ACL_TYPE_UNKNOWN;
}

ACLGrantee::
ACLGrantee()
{
}

ACLGrantee::
~ACLGrantee()
{
}

string& ACLGrantee::
get_type()
{
  return type;
}

class ACLID : public XMLObj
{
public:
  ACLID() {}
  ~ACLID() {}
  string& to_str() { return data; }
};

class ACLURI : public XMLObj
{
public:
  ACLURI() {}
  ~ACLURI() {}
};

class ACLEmail : public XMLObj
{
public:
  ACLEmail() {}
  ~ACLEmail() {}
};

class ACLDisplayName : public XMLObj
{
public:
 ACLDisplayName() {}
 ~ACLDisplayName() {}
};

ACLOwner::
ACLOwner()
{
}

ACLOwner::
~ACLOwner()
{
}

bool ACLOwner::xml_end(const char *el) {
  ACLID *acl_id = (ACLID *)find_first("ID");
  ACLID *acl_name = (ACLID *)find_first("DisplayName");

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

ACLGrant::
ACLGrant()
{
}

ACLGrant::
~ACLGrant()
{
}

bool ACLGrant::xml_end(const char *el) {
  ACLGrantee *acl_grantee;
  ACLID *acl_id;
  ACLURI *acl_uri;
  ACLEmail *acl_email;
  ACLPermission *acl_permission;
  ACLDisplayName *acl_name;

  acl_grantee = (ACLGrantee *)find_first("Grantee");
  if (!acl_grantee)
    return false;
  string type_str;
  if (!acl_grantee->get_attr("xsi:type", type_str))
    return false;
  type.set(type_str.c_str());
  acl_permission = (ACLPermission *)find_first("Permission");
  if (!acl_permission)
    return false;
  permission = *acl_permission;

  id.clear();
  name.clear();
  uri.clear();
  email.clear();

  switch (type.get_type()) {
  case ACL_TYPE_CANON_USER:
    acl_id = (ACLID *)acl_grantee->find_first("ID");
    if (!acl_id)
      return false;
    id = acl_id->to_str();
    acl_name = (ACLDisplayName *)acl_grantee->find_first("DisplayName");
    if (acl_name)
      name = acl_name->get_data();
    RGW_LOG(15) << "[" << *acl_grantee << ", " << permission << ", " << id << ", " << "]" << dendl;
    break;
  case ACL_TYPE_GROUP:
    acl_uri = (ACLURI *)acl_grantee->find_first("URI");
    if (!acl_uri)
      return false;
    uri = acl_uri->get_data();
    RGW_LOG(15) << "[" << *acl_grantee << ", " << permission << ", " << uri << "]" << dendl;
    break;
  case ACL_TYPE_EMAIL_USER:
    acl_email = (ACLEmail *)acl_grantee->find_first("EmailAddress");
    if (!acl_email)
      return false;
    email = acl_email->get_data();
    RGW_LOG(15) << "[" << *acl_grantee << ", " << permission << ", " << email << "]" << dendl;
    break;
  default:
    // unknown user type
    return false;
  };
  return true;
}

RGWAccessControlList::
RGWAccessControlList() : user_map_initialized(false)
{
}

RGWAccessControlList::
~RGWAccessControlList()
{
}

void RGWAccessControlList::init_user_map()
{
  multimap<string, ACLGrant>::iterator iter;
  acl_user_map.clear();
  for (iter = grant_map.begin(); iter != grant_map.end(); ++iter) {
    ACLGrant& grant = iter->second;
    ACLPermission& perm = grant.get_permission();
    acl_user_map[grant.get_id()] |= perm.get_permissions();
  }
  user_map_initialized = true;
}

void RGWAccessControlList::add_grant(ACLGrant *grant)
{
  string id = grant->get_id();
  if (id.size() > 0) {
    grant_map.insert(pair<string, ACLGrant>(id, *grant));
  }
}

bool RGWAccessControlList::xml_end(const char *el) {
  XMLObjIter iter = find("Grant");
  ACLGrant *grant = (ACLGrant *)iter.get_next();
  while (grant) {
    add_grant(grant);
    grant = (ACLGrant *)iter.get_next();
  }
  init_user_map();
  return true;
}

int RGWAccessControlList::get_perm(string& id, int perm_mask) {
  RGW_LOG(5) << "Searching permissions for uid=" << id << " mask=" << perm_mask << dendl;
  if (!user_map_initialized)
    init_user_map();
  map<string, int>::iterator iter = acl_user_map.find(id);
  if (iter != acl_user_map.end()) {
    RGW_LOG(5) << "Found permission: " << iter->second << dendl;
    return iter->second & perm_mask;
  }
  RGW_LOG(5) << "Permissions for user not found" << dendl;
  return 0;
}

bool RGWAccessControlList::create_canned(string id, string name, string canned_acl)
{
  acl_user_map.clear();
  grant_map.clear();

  /* owner gets full control */
  ACLGrant grant;
  grant.set_canon(id, name, RGW_PERM_FULL_CONTROL);
  add_grant(&grant);

  if (canned_acl.size() == 0 || canned_acl.compare("private") == 0) {
    return true;
  }

  ACLGrant group_grant;
  if (canned_acl.compare("public-read") == 0) {
    group_grant.set_group(rgw_uri_all_users, RGW_PERM_READ);
    add_grant(&group_grant);
  } else if (canned_acl.compare("public-read-write") == 0) {
    group_grant.set_group(rgw_uri_all_users, RGW_PERM_READ);
    add_grant(&group_grant);
    group_grant.set_group(rgw_uri_all_users, RGW_PERM_WRITE);
    add_grant(&group_grant);
  } else if (canned_acl.compare("authenticated-read") == 0) {
    group_grant.set_group(rgw_uri_auth_users, RGW_PERM_READ);
    add_grant(&group_grant);
  } else {
    return false;
  }

  return true;

}

RGWAccessControlPolicy::
RGWAccessControlPolicy()
{
}

RGWAccessControlPolicy::
~RGWAccessControlPolicy()
{
}

bool RGWAccessControlPolicy::xml_end(const char *el) {
  RGWAccessControlList *acl_p =
      (RGWAccessControlList *)find_first("AccessControlList");
  if (!acl_p)
    return false;
  acl = *acl_p;

  ACLOwner *owner_p = (ACLOwner*)find_first("Owner");
  if (!owner_p)
    return false;
  owner = *owner_p;
  return true;
}

int RGWAccessControlPolicy::get_perm(string& id, int perm_mask) {
  int perm = acl.get_perm(id, perm_mask);

  if (perm == perm_mask)
    return perm;

  if (perm_mask & (RGW_PERM_READ_ACP | RGW_PERM_WRITE_ACP)) {
    /* this is the owner, it has implicit permissions */
    if (id.compare(owner.get_id()) == 0) {
      perm |= RGW_PERM_READ_ACP | RGW_PERM_WRITE_ACP;
      perm &= perm_mask;
    }
  }

  /* should we continue looking up? */
  if ((perm & perm_mask) != perm_mask) {
    perm |= acl.get_perm(rgw_uri_all_users, perm_mask);

    if (id.compare(RGW_USER_ANON_ID)) {
      /* this is not the anonymous user */
      perm |= acl.get_perm(rgw_uri_auth_users, perm_mask);
    }
  }

  RGW_LOG(5) << "Getting permissions id=" << id << " owner=" << owner << dendl;

  return perm;
}

void xml_start(void *data, const char *el, const char **attr) {
  RGWXMLParser *handler = (RGWXMLParser *)data;

  if (!handler->xml_start(el, attr))
    handler->set_failure();
}

RGWXMLParser::
RGWXMLParser() : buf(NULL), buf_len(0), cur_obj(NULL), success(true)
{
}

RGWXMLParser::
~RGWXMLParser()
{
  free(buf);
  vector<XMLObj *>::iterator iter;
  for (iter = objs.begin(); iter != objs.end(); ++iter) {
    XMLObj *obj = *iter;
    delete obj;
  }
}

bool RGWXMLParser::xml_start(const char *el, const char **attr) {
  XMLObj * obj;
  if (strcmp(el, "AccessControlPolicy") == 0) {
    obj = new RGWAccessControlPolicy();
  } else if (strcmp(el, "Owner") == 0) {
    obj = new ACLOwner();
  } else if (strcmp(el, "AccessControlList") == 0) {
    obj = new RGWAccessControlList();
  } else if (strcmp(el, "ID") == 0) {
    obj = new ACLID();
  } else if (strcmp(el, "DisplayName") == 0) {
    obj = new ACLDisplayName();
  } else if (strcmp(el, "Grant") == 0) {
    obj = new ACLGrant();
  } else if (strcmp(el, "Grantee") == 0) {
    obj = new ACLGrantee();
  } else if (strcmp(el, "Permission") == 0) {
    obj = new ACLPermission();
  } else if (strcmp(el, "URI") == 0) {
    obj = new ACLURI();
  } else if (strcmp(el, "EmailAddress") == 0) {
    obj = new ACLEmail();
  } else {
    obj = new XMLObj();
  }
  if (!obj->xml_start(cur_obj, el, attr))
    return false;
  if (cur_obj) {
    cur_obj->add_child(el, obj);
  } else {
    children.insert(pair<string, XMLObj *>(el, obj));
  }
  cur_obj = obj;

  objs.push_back(obj);
  return true;
}

void xml_end(void *data, const char *el) {
  RGWXMLParser *handler = (RGWXMLParser *)data;

  if (!handler->xml_end(el))
    handler->set_failure();
}

bool RGWXMLParser::xml_end(const char *el) {
  XMLObj *parent_obj = cur_obj->get_parent();
  if (!cur_obj->xml_end(el))
    return false;
  cur_obj = parent_obj;
  return true;
}

void handle_data(void *data, const char *s, int len)
{
  RGWXMLParser *handler = (RGWXMLParser *)data;

  handler->handle_data(s, len);
}

void RGWXMLParser::handle_data(const char *s, int len)
{
  cur_obj->xml_handle_data(s, len);
}


bool RGWXMLParser::init()
{
  p = XML_ParserCreate(NULL);
  if (!p) {
    RGW_LOG(10) << "RGWXMLParser::init(): ERROR allocating memory" << dendl;
    return false;
  }
  XML_SetElementHandler(p, ::xml_start, ::xml_end);
  XML_SetCharacterDataHandler(p, ::handle_data);
  XML_SetUserData(p, (void *)this);
  return true;
}

bool RGWXMLParser::parse(const char *_buf, int len, int done)
{
  int pos = buf_len;
  buf = (char *)realloc(buf, buf_len + len);
  memcpy(&buf[buf_len], _buf, len);
  buf_len += len;

  success = true;
  if (!XML_Parse(p, &buf[pos], len, done)) {
    fprintf(stderr, "Parse error at line %d:\n%s\n",
	      (int)XML_GetCurrentLineNumber(p),
	      XML_ErrorString(XML_GetErrorCode(p)));
    return false;
  }
  return success;
}
