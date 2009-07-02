#ifndef __S3_ACL_H
#define __S3_ACL_H

#include <map>
#include <string>
#include <iostream>
#include <include/types.h>

#include <expat.h>

using namespace std;


#define S3_URI_ALL_USERS	"http://acs.amazonaws.com/groups/global/AllUsers"
#define S3_URI_AUTH_USERS	"http://acs.amazonaws.com/groups/global/AuthenticatedUsers"

#define S3_PERM_READ            0x01
#define S3_PERM_WRITE           0x02
#define S3_PERM_READ_ACP        0x04
#define S3_PERM_WRITE_ACP       0x08
#define S3_PERM_FULL_CONTROL    ( S3_PERM_READ | S3_PERM_WRITE | \
                                  S3_PERM_READ_ACP | S3_PERM_WRITE_ACP )
#define S3_PERM_ALL             S3_PERM_FULL_CONTROL 

class XMLObj;

class XMLObjIter {
  map<string, XMLObj *>::iterator cur;
  map<string, XMLObj *>::iterator end;
public:
  XMLObjIter() {}
  void set(map<string, XMLObj *>::iterator& _cur, map<string, XMLObj *>::iterator& _end) { cur = _cur; end = _end; }
  XMLObj *get_next() { 
    XMLObj *obj = NULL;
    if (cur != end) {
      obj = cur->second;
      ++cur;
    }
    return obj;
  };
};

class XMLObj
{
  int refcount;
  XMLObj *parent;
  string type;
protected:
  string data;
  multimap<string, XMLObj *> children;
  map<string, string> attr_map;
public:
  XMLObj() : refcount(0) {}
  virtual ~XMLObj() { }
  void xml_start(XMLObj *parent, const char *el, const char **attr) {
    this->parent = parent;
    type = el;
    for (int i = 0; attr[i]; i += 2) {
      attr_map[attr[i]] = string(attr[i + 1]);
    }
  }
  virtual void xml_end(const char *el) {}
  virtual void xml_handle_data(const char *s, int len) { data = string(s, len); }
  string& get_data() { return data; }
  XMLObj *get_parent() { return parent; }
  void add_child(string el, XMLObj *obj) {
    children.insert(pair<string, XMLObj *>(el, obj));
  }

  bool get_attr(string name, string& attr) {
    map<string, string>::iterator iter = attr_map.find(name);
    if (iter == attr_map.end())
      return false;
    attr = iter->second;
    return true;
  }

  XMLObjIter find(string name) {
    XMLObjIter iter;
    map<string, XMLObj *>::iterator first;
    map<string, XMLObj *>::iterator last;
    first = children.find(name);
    last = children.upper_bound(name);
    iter.set(first, last);
    return iter;
  }

  XMLObj *find_first(string name) {
    XMLObjIter iter;
    map<string, XMLObj *>::iterator first;
    first = children.find(name);
    if (first != children.end())
      return first->second;
    return NULL;
  }

  friend ostream& operator<<(ostream& out, XMLObj& obj);
};

class ACLPermission : public XMLObj
{
  int flags;
public:
  ACLPermission() : flags(0) {}
  ~ACLPermission() {}
  void xml_end(const char *el);
  int get_permissions() { return flags; }
  void set_permissions(int perm) { flags = perm; }

  void encode(bufferlist& bl) const {
     ::encode(flags, bl);
  }
  void decode(bufferlist::iterator& bl) {
    ::decode(flags, bl);
  }
  void to_xml(ostream& out) {
    if ((flags & S3_PERM_FULL_CONTROL) == S3_PERM_FULL_CONTROL) {
     out << "<Permission>FULL_CONTROL</Permission>";
    } else {
      if (flags & S3_PERM_READ)
        out << "<Permission>READ</Permission>";
      if (flags & S3_PERM_WRITE)
        out << "<Permission>WRITE</Permission>";
      if (flags & S3_PERM_READ_ACP)
        out << "<Permission>READ_ACP</Permission>";
      if (flags & S3_PERM_WRITE_ACP)
        out << "<Permission>WRITE_ACP</Permission>";
    }
  }
};
WRITE_CLASS_ENCODER(ACLPermission)

enum ACLGranteeTypeEnum {
  ACL_TYPE_CANON_USER,
  ACL_TYPE_EMAIL_USER,
  ACL_TYPE_GROUP,
  ACL_TYPE_UNKNOWN,
};
class ACLGranteeType
{
  __u32 type;
public:
  ACLGranteeType() : type(ACL_TYPE_UNKNOWN) {}
  const char *to_string() {
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
  ACLGranteeTypeEnum get_type() { return (ACLGranteeTypeEnum)type; };
  void set(ACLGranteeTypeEnum t) { type = t; }

  void set(const char *s) {
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

  void encode(bufferlist& bl) const {
     ::encode(type, bl);
  }
  void decode(bufferlist::iterator& bl) {
     ::decode(type, bl);
  }
};
WRITE_CLASS_ENCODER(ACLGranteeType)

class ACLGrantee : public XMLObj
{
  string type;
public:
  ACLGrantee() {} 
  ~ACLGrantee() {}

  void xml_start(const char *el, const char **attr);

  string& get_type() { return type; }
};


class ACLGrant : public XMLObj
{
  ACLGranteeType type;
  
  string id;
  string uri;
  string email;
  ACLPermission permission;
  string name;
public:
  ACLGrant() {}
  ~ACLGrant() {}

  void xml_end(const char *el);

  /* there's an assumption here that email/uri/id encodings are
     different and there can't be any overlap */
  string& get_id() {
    switch(type.get_type()) {
    case ACL_TYPE_EMAIL_USER:
      return email;
    case ACL_TYPE_GROUP:
      return uri;
    default:
      return id;
    }
  }
  ACLGranteeType& get_type() { return type; }
  ACLPermission& get_permission() { return permission; }

  void encode(bufferlist& bl) const {
     ::encode(type, bl);
     ::encode(id, bl);
     ::encode(uri, bl);
     ::encode(email, bl);
     ::encode(permission, bl);
     ::encode(name, bl);
  }
  void decode(bufferlist::iterator& bl) {
    ::decode(type, bl);
    ::decode(id, bl);
    ::decode(uri, bl);
    ::decode(email, bl);
    ::decode(permission, bl);
    ::decode(name, bl);
  }
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
    permission.to_xml(out);
    out << "</Grant>";
  }
  void set(string& _id, string& _name, int perm) {
    type.set(ACL_TYPE_CANON_USER);
    id = _id;
    name = _name;
    permission.set_permissions(perm);
  }
  void xml_start(const char *el, const char **attr);
};
WRITE_CLASS_ENCODER(ACLGrant)

class S3AccessControlList : public XMLObj
{
  map<string, int> acl_user_map;
  multimap<string, ACLGrant> grant_map;
  bool user_map_initialized;

  void init_user_map();
public:
  S3AccessControlList() : user_map_initialized(false) {}
  ~S3AccessControlList() {}

  void xml_end(const char *el);
  int get_perm(string& id, int perm_mask);
  void encode(bufferlist& bl) const {
     ::encode(user_map_initialized, bl);
     ::encode(acl_user_map, bl);
     ::encode(grant_map, bl);
  }
  void decode(bufferlist::iterator& bl) {
    ::decode(user_map_initialized, bl);
    ::decode(acl_user_map, bl);
    ::decode(grant_map, bl);
  }
  void to_xml(ostream& out) {
    map<string, ACLGrant>::iterator iter;
    out << "<AccessControlList>";
    for (iter = grant_map.begin(); iter != grant_map.end(); ++iter) {
      ACLGrant& grant = iter->second;
      grant.to_xml(out);
    }
    out << "</AccessControlList>";
  }
  void add_grant(ACLGrant *grant);

  void create_default(string name, string id) {
    acl_user_map.clear();
    grant_map.clear();

    ACLGrant grant;
    grant.set(name, id, S3_PERM_FULL_CONTROL);
    add_grant(&grant);
  }
};
WRITE_CLASS_ENCODER(S3AccessControlList)

class ACLOwner : public XMLObj
{
  string id;
  string display_name;
public:
  ACLOwner() {}
  ~ACLOwner() {}

  void xml_end(const char *el);
  void encode(bufferlist& bl) const {
     ::encode(id, bl);
     ::encode(display_name, bl);
  }
  void decode(bufferlist::iterator& bl) {
    ::decode(id, bl);
    ::decode(display_name, bl);
  }
  void to_xml(ostream& out) {
    out << "<Owner>" <<
                   "<ID>" << id << "</ID>" <<
                   "<DisplayName>" << display_name << "</DisplayName>" <<
                  "</Owner>";
  }
  void set_id(string& _id) { id = _id; }
  void set_name(string& name) { display_name = name; }

  string& get_id() { return id; }
};
WRITE_CLASS_ENCODER(ACLOwner)

class S3AccessControlPolicy : public XMLObj
{
  S3AccessControlList acl;
  ACLOwner owner;

public:
  S3AccessControlPolicy() {}
  ~S3AccessControlPolicy() {}

  void xml_end(const char *el);

  int get_perm(string& id, int perm_mask);

  void encode(bufferlist& bl) const {
    ::encode(owner, bl);
    ::encode(acl, bl);
  }
  void decode(bufferlist::iterator& bl) {
     ::decode(owner, bl);
     ::decode(acl, bl);
   }
  void to_xml(ostream& out) {
    out << "<AccessControlPolicy>";
    owner.to_xml(out);
    acl.to_xml(out);
    out << "</AccessControlPolicy>";
  }

  void set_owner(ACLOwner& o) { owner = o; }
  ACLOwner& get_owner() {
    return owner;
  }

  void create_default(string& id, string& name) {
    acl.create_default(id, name);
    owner.set_id(id);
    owner.set_name(name);
  }

  S3AccessControlList& get_acl() {
    return acl;
  }
};
WRITE_CLASS_ENCODER(S3AccessControlPolicy)

class S3XMLParser : public XMLObj
{
  XML_Parser p;
  char *buf;
  int buf_len;
  XMLObj *cur_obj;
  vector<XMLObj *> objs;
public:
  S3XMLParser() : buf(NULL), buf_len(0), cur_obj(NULL) {}
  ~S3XMLParser() {
    free(buf);
    vector<XMLObj *>::iterator iter;
    for (iter = objs.begin(); iter != objs.end(); ++iter) {
      XMLObj *obj = *iter;
      delete obj;
    }
  }
  bool init();
  void xml_start(const char *el, const char **attr);
  void xml_end(const char *el);
  void handle_data(const char *s, int len);

  bool parse(const char *buf, int len, int done);
  const char *get_xml() { return buf; }
};

#endif
