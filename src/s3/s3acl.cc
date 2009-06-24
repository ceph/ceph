#include <string.h>

#include <iostream>
#include <vector>
#include <map>

#include "include/types.h"

#include "expat.h"

using namespace std;

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
  XMLObj *parent;
  string type;
  map<string, string> attr_map;
protected:
  string data;
  multimap<string, XMLObj *> children;
public:
  XMLObj() {}
  virtual ~XMLObj() {
    multimap<string, XMLObj *>::iterator iter;
    for (iter = children.begin(); iter != children.end(); ++iter) {
      delete iter->second;
    }
  }
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
ostream& operator<<(ostream& out, XMLObj& obj) {
   out << obj.type << ": " << obj.data;
   return out;
}

class ACLOwner : public XMLObj
{
public:
  ACLOwner() {}
  ~ACLOwner() {}
};

class ACLGrantee : public XMLObj
{
public:
 ACLGrantee() {} 
 ~ACLGrantee() {}
};

class ACLPermission : public XMLObj
{
  int flags;
public:
  ACLPermission() : flags(0) {}
  ~ACLPermission() {}
  void xml_end(const char *el) {
    const char *s = data.c_str();
    if (strcasecmp(s, "READ") == 0) {
      flags |= S3_PERM_READ;
    } else if (strcasecmp(s, "WRITE") == 0) {
      flags |= S3_PERM_WRITE;
    } else if (strcasecmp(s, "READ_ACP") == 0) {
      flags |= S3_PERM_READ_ACP;
    } else if (strcasecmp(s, "WRITE_ACP") == 0) {
      flags |= S3_PERM_WRITE_ACP;
    } else if (strcasecmp(s, "FULL_CONTROL") == 0) {
      flags |= S3_PERM_FULL_CONTROL;
    }
  }
  int get_permissions() { return flags; }
};

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

class ACLDisplayName : public XMLObj
{
public:
 ACLDisplayName() {} 
 ~ACLDisplayName() {}
};

class ACLGrant : public XMLObj
{
  ACLGrantee *grantee;
  ACLID *id;
  ACLURI *uri;
  ACLPermission *permission;
  ACLDisplayName *name;
public:
  ACLGrant() : grantee(NULL), id(NULL), uri(NULL), permission(NULL) {}
  ~ACLGrant() {}

  void xml_end(const char *el) {
    grantee = (ACLGrantee *)find_first("Grantee");
    if (!grantee)
      return;
    permission = (ACLPermission *)find_first("Permission");
    if (!permission)
      return;
    name = (ACLDisplayName *)grantee->find_first("DisplayName");
    id = (ACLID *)grantee->find_first("ID");
    if (id) {
      cout << "[" << *grantee << ", " << *permission << ", " << *id << ", " << "]" << std::endl;
    } else {
      uri = (ACLURI *)grantee->find_first("URI");
      if (uri)
        cout << "[" << *grantee << ", " << *permission << ", " << *uri << "]" << std::endl;
    }
  }
  ACLID *get_id() { return id; }
  ACLPermission *get_permission() { return permission; }
};

class AccessControlList : public XMLObj
{
  map<string, int> acl_user_map;
public:
  AccessControlList() {}
  ~AccessControlList() {}

  void xml_end(const char *el) {
    XMLObjIter iter = find("Grant");
    ACLGrant *grant = (ACLGrant *)iter.get_next();
    while (grant) {
      ACLID *id = (ACLID *)grant->get_id();
      ACLPermission *perm = (ACLPermission *)grant->get_permission();
      if (id && perm) {
        acl_user_map[id->to_str()] |= perm->get_permissions();
      }
      grant = (ACLGrant *)iter.get_next();
    }
  }
  int get_perm(string& id, int perm_mask) {
    map<string, int>::iterator iter = acl_user_map.find(id);
    if (iter != acl_user_map.end())
      return iter->second;
    return 0;
  }
  void encode(bufferlist& bl) const {
     ::encode(acl_user_map, bl);
  }
  void decode(bufferlist::iterator& bl) {
    ::decode(acl_user_map, bl);
  }
};
WRITE_CLASS_ENCODER(AccessControlList)


class AccessControlPolicy : public XMLObj
{
  AccessControlList acl;
public:
  AccessControlPolicy() {}
  ~AccessControlPolicy() {}

  void xml_end(const char *el) {
    acl = *(AccessControlList *)find_first("AccessControlList");
  }

  int get_perm(string& id, int perm_mask) {
    return acl.get_perm(id, perm_mask);
  }

  void encode(bufferlist& bl) const {
    ::encode(acl, bl);
  }
  void decode(bufferlist::iterator& bl) {
     ::decode(acl, bl);
   }
};
WRITE_CLASS_ENCODER(AccessControlPolicy)

class S3XMLParser : public XMLObj
{
  XML_Parser p;
  char *buf;
  int buf_len;
  XMLObj *cur_obj;
public:
  S3XMLParser() : buf(NULL), buf_len(0), cur_obj(NULL) {}
  ~S3XMLParser() {
    free(buf);
  }
  bool init();
  void xml_start(const char *el, const char **attr);
  void xml_end(const char *el);
  void handle_data(const char *s, int len);

  bool parse(const char *buf, int len, int done);
  const char *get_xml() { return buf; }
};

void xml_start(void *data, const char *el, const char **attr) {
  S3XMLParser *handler = (S3XMLParser *)data;

  handler->xml_start(el, attr);
}

void S3XMLParser::xml_start(const char *el, const char **attr) {
  XMLObj * obj;
  if (strcmp(el, "AccessControlPolicy") == 0) {
    obj = new AccessControlPolicy();    
  } else if (strcmp(el, "AccessControlList") == 0) {
    obj = new AccessControlList();    
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
  } else {
    obj = new XMLObj(); 
  }
  obj->xml_start(cur_obj, el, attr);
  if (cur_obj) {
    cur_obj->add_child(el, obj);
  } else {
    children.insert(pair<string, XMLObj *>(el, obj));
  }
  cur_obj = obj;

}

void xml_end(void *data, const char *el) {
  S3XMLParser *handler = (S3XMLParser *)data;

  handler->xml_end(el);
}

void S3XMLParser::xml_end(const char *el) {
  XMLObj *parent_obj = cur_obj->get_parent();
  cur_obj->xml_end(el);
  cur_obj = parent_obj;
}

void handle_data(void *data, const char *s, int len)
{
  S3XMLParser *handler = (S3XMLParser *)data;

  handler->handle_data(s, len);
}

void S3XMLParser::handle_data(const char *s, int len)
{
  cur_obj->xml_handle_data(s, len);
}


bool S3XMLParser::init()
{
  p = XML_ParserCreate(NULL);
  if (!p) {
    cerr << "S3XMLParser::init(): ERROR allocating memory" << std::endl;
    return false;
  }
  XML_SetElementHandler(p, ::xml_start, ::xml_end);
  XML_SetCharacterDataHandler(p, ::handle_data);
  XML_SetUserData(p, (void *)this);

  return true;
}

bool S3XMLParser::parse(const char *_buf, int len, int done)
{
  buf = (char *)realloc(buf, buf_len + len);
  memcpy(&buf[buf_len], _buf, len);
  buf_len += len;

  if (!XML_Parse(p, _buf, len, done)) {
    fprintf(stderr, "Parse error at line %d:\n%s\n",
	      (int)XML_GetCurrentLineNumber(p),
	      XML_ErrorString(XML_GetErrorCode(p)));
    return false;
  }
  return true; 
}


#if 0
int main(int argc, char **argv) {
  S3XMLParser parser;

  if (!parser.init())
    exit(1);

  char buf[1024];

  for (;;) {
    int done;
    int len;

    len = fread(buf, 1, sizeof(buf), stdin);
    if (ferror(stdin)) {
      fprintf(stderr, "Read error\n");
      exit(-1);
    }
    done = feof(stdin);

    parser.parse(buf, len, done);

    if (done)
      break;
  }

  AccessControlPolicy *policy = (AccessControlPolicy *)parser.find_first("AccessControlPolicy");

  if (policy) {
    string id="79a59df900b949e55d96a1e698fbacedfd6e09d98eacf8f8d5218e7cd47ef2be";
    cout << hex << policy->get_perm(id, S3_PERM_ALL) << dec << endl;
  }
  cout << parser.get_xml() << endl;

  exit(0);
}
#endif

