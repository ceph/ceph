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


class ACLGrantee : public XMLObj
{
public:
 ACLGrantee() {} 
 ~ACLGrantee() {}
};

void ACLPermission::xml_end(const char *el) {
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

void ACLOwner::xml_end(const char *el) {
  ACLID *acl_id = (ACLID *)find_first("ID");  
  ACLID *acl_name = (ACLID *)find_first("DisplayName");

  if (acl_id)
    id = acl_id->get_data();
  if (acl_name)
    display_name = acl_name->get_data();
}


void ACLGrant::xml_end(const char *el) {
  ACLGrantee *acl_grantee;
  ACLID *acl_id;
  ACLURI *acl_uri;
  ACLPermission *acl_permission;
  ACLDisplayName *acl_name;

  acl_grantee = (ACLGrantee *)find_first("Grantee");
  if (!acl_grantee)
    return;
  acl_permission = (ACLPermission *)find_first("Permission");
  if (!acl_permission)
    return;
  permission = *acl_permission;

  acl_name = (ACLDisplayName *)acl_grantee->find_first("DisplayName");
  if (acl_name)
    name = acl_name->get_data();

  acl_id = (ACLID *)acl_grantee->find_first("ID");
  if (acl_id) {
    id = acl_id->to_str();
    cout << "[" << *acl_grantee << ", " << permission << ", " << id << ", " << "]" << std::endl;
  } else {
    acl_uri = (ACLURI *)acl_grantee->find_first("URI");
    if (acl_uri) {
      cout << "[" << *acl_grantee << ", " << permission << ", " << uri << "]" << std::endl;
      uri = acl_uri->get_data();
    }
  }
}

void S3AccessControlList::init_user_map()
{
  map<string, ACLGrant>::iterator iter;
  acl_user_map.clear();
  for (iter = grant_map.begin(); iter != grant_map.end(); ++iter) {
    ACLGrant& grant = iter->second;
    ACLPermission& perm = grant.get_permission();
    acl_user_map[grant.get_id()] |= perm.get_permissions();
  }
  user_map_initialized = true;
}

void S3AccessControlList::add_grant(ACLGrant *grant)
{
  string id = grant->get_id();
  if (id.size() > 0) {
    grant_map[id] = *grant;
  }
}

void S3AccessControlList::xml_end(const char *el) {
  XMLObjIter iter = find("Grant");
  ACLGrant *grant = (ACLGrant *)iter.get_next();
  while (grant) {
    add_grant(grant);
    grant = (ACLGrant *)iter.get_next();
  }
  init_user_map();
}

int S3AccessControlList::get_perm(string& id, int perm_mask) {
  cerr << "searching permissions for uid=" << id << " mask=" << perm_mask << std::endl;
  if (!user_map_initialized)
    init_user_map();
  map<string, int>::iterator iter = acl_user_map.find(id);
  if (iter != acl_user_map.end()) {
    cerr << "found permission: " << iter->second << std::endl;
    return iter->second & perm_mask;
  }
  cerr << "permissions for user not found" << std::endl;
  return 0;
}

void S3AccessControlPolicy::xml_end(const char *el) {
  acl = *(S3AccessControlList *)find_first("AccessControlList");
  owner = *(ACLOwner *)find_first("Owner");
}

int S3AccessControlPolicy::get_perm(string& id, int perm_mask) {
  int perm = acl.get_perm(id, perm_mask);

  if (perm == perm_mask)
    return perm;

  if (perm_mask & (S3_PERM_READ_ACP | S3_PERM_WRITE_ACP)) {
    /* this is the owner, it has implicit permissions */
    if (id.compare(owner.get_id()) == 0) {
      perm |= S3_PERM_READ_ACP | S3_PERM_WRITE_ACP;
      perm &= perm_mask; 
    }
  }

  cerr << "id=" << id << " owner=" << owner << std::endl;

  return perm;
}

void xml_start(void *data, const char *el, const char **attr) {
  S3XMLParser *handler = (S3XMLParser *)data;

  handler->xml_start(el, attr);
}

void S3XMLParser::xml_start(const char *el, const char **attr) {
  XMLObj * obj;
  if (strcmp(el, "AccessControlPolicy") == 0) {
    obj = new S3AccessControlPolicy();    
  } else if (strcmp(el, "Owner") == 0) {
    obj = new ACLOwner();    
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

  objs.push_back(obj);
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
  int pos = buf_len;
  buf = (char *)realloc(buf, buf_len + len);
  memcpy(&buf[buf_len], _buf, len);
  buf_len += len;

  if (!XML_Parse(p, &buf[pos], len, done)) {
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

  S3AccessControlPolicy *policy = (S3AccessControlPolicy *)parser.find_first("AccessControlPolicy");

  if (policy) {
    string id="79a59df900b949e55d96a1e698fbacedfd6e09d98eacf8f8d5218e7cd47ef2be";
    cout << hex << policy->get_perm(id, S3_PERM_ALL) << dec << endl;
    policy->to_xml(cout);
  }

  cout << parser.get_xml() << endl;

  S3AccessControlPolicy def;
  string id="thisistheid!";
  string name="foobar";
  def.create_default(id, name);

  def.to_xml(cout);

  exit(0);
}
#endif

