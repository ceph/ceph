#include <string.h>

#include <iostream>
#include <vector>
#include <map>

#include "expat.h"

using namespace std;

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
  const char *el_type;
  map<string, string> attr_map;
  string data;
protected:
  multimap<string, XMLObj *> children;
public:
  XMLObj() {}
  virtual ~XMLObj() {}
  void xml_start(XMLObj *parent, const char *el, const char **attr) {
    this->parent = parent;
    el_type = el;
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
};

class ACLOwner : public XMLObj
{
public:
  ACLOwner() {}
  ~ACLOwner() {}
};

class ACLGrant : public XMLObj
{
public:
  ACLGrant() {}
  ~ACLGrant() {}
};

class ACLGrantee : public XMLObj
{
public:
 ACLGrantee() {} 
 ~ACLGrantee() {}
};

class ACLDisplayName : public XMLObj
{
public:
 ACLDisplayName() {} 
 ~ACLDisplayName() {}
};

class ACLPermission : public XMLObj
{
public:
  ACLPermission() {}
  ~ACLPermission() {}
};

class ACLID : public XMLObj
{
public:
  ACLID() {}
  ~ACLID() {}
};

class ACLURI : public XMLObj
{
public:
  ACLURI() {}
  ~ACLURI() {}
};

class AccessControlPolicy : public XMLObj
{
public:
  AccessControlPolicy() {}
  ~AccessControlPolicy() {}
};

class S3XMLParser : public XMLObj
{
  XML_Parser p;
  const char *buf;
  XMLObj *cur_obj;
  int pos;
public:
  S3XMLParser() {}
  bool init();
  void xml_start(const char *el, const char **attr);
  void xml_end(const char *el);
  void handle_data(const char *s, int len);
  

  bool parse(const char *buf, int len, int done);
};

void xml_start(void *data, const char *el, const char **attr) {
  S3XMLParser *handler = (S3XMLParser *)data;

  handler->xml_start(el, attr);
}

void S3XMLParser::xml_start(const char *el, const char **attr) {
  XMLObj * obj;
  if (strcmp(el, "AccessControlPolicy") == 0) {
    obj = new AccessControlPolicy();    
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
  printf(">%.*s\n", len, s);
}


bool S3XMLParser::init()
{
  pos = -1;
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
  buf = _buf;
  if (!XML_Parse(p, buf, len, done)) {
    fprintf(stderr, "Parse error at line %d:\n%s\n",
	      XML_GetCurrentLineNumber(p),
	      XML_ErrorString(XML_GetErrorCode(p)));
    return false;
  }
  return true; 
}


#if 0
int main(int argc, char **argv) {
  S3XMLParser s3acl;

  if (!s3acl.init())
    exit(1);

  char buf[2048];

  for (;;) {
    int done;
    int len;

    len = fread(buf, 1, 2048, stdin);
    if (ferror(stdin)) {
      fprintf(stderr, "Read error\n");
      exit(-1);
    }
    done = feof(stdin);

    s3acl.parse(buf, len, done);

    if (done)
      break;
  }

  XMLObjIter iter = s3acl.find("AccessControlPolicy");
  AccessControlPolicy *acp = (AccessControlPolicy *)iter.get_next();
  cout << (void *)acp << std::endl;

  exit(0);
}

#endif
