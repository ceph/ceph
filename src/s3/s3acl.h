#ifndef __S3_ACL_H
#define __S3_ACL_H

#include <map>
#include <string>
#include <include/types.h>

#include <expat.h>

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

class S3AccessControlList : public XMLObj
{
  map<string, int> acl_user_map;
public:
  S3AccessControlList() {}
  ~S3AccessControlList() {}

  void xml_end(const char *el);
  int get_perm(string& id, int perm_mask);
  void encode(bufferlist& bl) const {
     ::encode(acl_user_map, bl);
  }
  void decode(bufferlist::iterator& bl) {
    ::decode(acl_user_map, bl);
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
    ::encode(acl, bl);
  }
  void decode(bufferlist::iterator& bl) {
     ::decode(acl, bl);
   }
};
WRITE_CLASS_ENCODER(S3AccessControlPolicy)

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

#endif
