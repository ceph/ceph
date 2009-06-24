#include <string.h>

#include <iostream>
#include <map>

#include "include/types.h"

#include "s3acl.h"

using namespace std;
                                  
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

void S3AccessControlList::xml_end(const char *el) {
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

int S3AccessControlList::get_perm(string& id, int perm_mask) {
  map<string, int>::iterator iter = acl_user_map.find(id);
  if (iter != acl_user_map.end())
    return iter->second;
  return 0;
}

void S3AccessControlPolicy::xml_end(const char *el) {
  acl = *(S3AccessControlList *)find_first("AccessControlList");
}

int S3AccessControlPolicy::get_perm(string& id, int perm_mask) {
  return acl.get_perm(id, perm_mask);
}

void xml_start(void *data, const char *el, const char **attr) {
  S3XMLParser *handler = (S3XMLParser *)data;

  handler->xml_start(el, attr);
}

void S3XMLParser::xml_start(const char *el, const char **attr) {
  XMLObj * obj;
  if (strcmp(el, "AccessControlPolicy") == 0) {
    obj = new S3AccessControlPolicy();    
  } else if (strcmp(el, "AccessControlList") == 0) {
    obj = new S3AccessControlList();    
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

  S3AccessControlPolicy *policy = (S3AccessControlPolicy *)parser.find_first("S3AccessControlPolicy");

  if (policy) {
    string id="79a59df900b949e55d96a1e698fbacedfd6e09d98eacf8f8d5218e7cd47ef2be";
    cout << hex << policy->get_perm(id, S3_PERM_ALL) << dec << endl;
  }
  cout << parser.get_xml() << endl;

  exit(0);
}
#endif

