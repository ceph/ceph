// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_RGW_XML_H
#define CEPH_RGW_XML_H

#include <map>
#include <string>
#include <iosfwd>
#include <include/types.h>
#include <common/Formatter.h>

class XMLObj;

class XMLObjIter {
  typedef map<string, XMLObj *>::iterator map_iter_t;
  map_iter_t cur;
  map_iter_t end;
public:
  XMLObjIter();
  ~XMLObjIter();
  void set(const XMLObjIter::map_iter_t &_cur, const XMLObjIter::map_iter_t &_end);
  XMLObj *get_next();
};

/**
 * Represents a block of XML.
 * Give the class an XML blob, and it will parse the blob into
 * an attr_name->value map.
 * This really ought to be an abstract class or something; it
 * shouldn't be the startpoint for any parsing. Look at RGWXMLParser for that.
 */
class XMLObj
{
  XMLObj *parent;
  string obj_type;
protected:
  string data;
  multimap<string, XMLObj *> children;
  map<string, string> attr_map;
public:

  XMLObj() : parent(NULL) {}

  virtual ~XMLObj();
  bool xml_start(XMLObj *parent, const char *el, const char **attr);
  virtual bool xml_end(const char *el);
  virtual void xml_handle_data(const char *s, int len);
  string& get_data();
  XMLObj *get_parent();
  void add_child(string el, XMLObj *obj);
  bool get_attr(string name, string& attr);
  XMLObjIter find(string name);
  XMLObjIter find_first();
  XMLObj *find_first(string name);

  friend ostream& operator<<(ostream &out, const XMLObj &obj);
};

struct XML_ParserStruct;
class RGWXMLParser : public XMLObj
{
  XML_ParserStruct *p;
  char *buf;
  int buf_len;
  XMLObj *cur_obj;
  vector<XMLObj *> objs;
  list<XMLObj *> allocated_objs;
  list<XMLObj> unallocated_objs;
protected:
  virtual XMLObj *alloc_obj(const char *el) {
    return NULL;
  }
public:
  RGWXMLParser();
  ~RGWXMLParser() override;
  bool init();
  bool xml_start(const char *el, const char **attr);
  bool xml_end(const char *el) override;
  void handle_data(const char *s, int len);

  bool parse(const char *buf, int len, int done);
  const char *get_xml() { return buf; }
  void set_failure() { success = false; }

private:
  bool success;
};

class RGWXMLDecoder {
public:
  struct err {
    string message;

    explicit err(const string& m) : message(m) {}
  };

  class XMLParser : public RGWXMLParser {
  public:
    XMLParser() {}
    ~XMLParser() override {}
  } parser;

  explicit RGWXMLDecoder(bufferlist& bl) {
    if (!parser.parse(bl.c_str(), bl.length(), 1)) {
      cout << "RGWXMLDecoder::err()" << std::endl;
      throw RGWXMLDecoder::err("failed to parse XML input");
    }
  }

  template<class T>
  static bool decode_xml(const char *name, T& val, XMLObj *obj, bool mandatory = false);

  template<class C>
  static bool decode_xml(const char *name, C& container, void (*cb)(C&, XMLObj *obj), XMLObj *obj, bool mandatory = false);

  template<class T>
  static void decode_xml(const char *name, T& val, T& default_val, XMLObj *obj);
};

static inline ostream& operator<<(ostream &out, RGWXMLDecoder::err& err)
{
  return out << err.message;
}

template<class T>
void decode_xml_obj(T& val, XMLObj *obj)
{
  val.decode_xml(obj);
}

static inline void decode_xml_obj(string& val, XMLObj *obj)
{
  val = obj->get_data();
}

void decode_xml_obj(unsigned long long& val, XMLObj *obj);
void decode_xml_obj(long long& val, XMLObj *obj);
void decode_xml_obj(unsigned long& val, XMLObj *obj);
void decode_xml_obj(long& val, XMLObj *obj);
void decode_xml_obj(unsigned& val, XMLObj *obj);
void decode_xml_obj(int& val, XMLObj *obj);
void decode_xml_obj(bool& val, XMLObj *obj);
void decode_xml_obj(bufferlist& val, XMLObj *obj);
class utime_t;
void decode_xml_obj(utime_t& val, XMLObj *obj);

template<class T>
void do_decode_xml_obj(list<T>& l, const string& name, XMLObj *obj)
{
  l.clear();

  XMLObjIter iter = obj->find(name);
  XMLObj *o;

  while ((o = iter.get_next())) {
    T val;
    decode_xml_obj(val, o);
    l.push_back(val);
  }
}

template<class T>
void decode_xml_obj(std::vector<T>& v, XMLObj *obj)
{
  v.clear();

  XMLObjIter iter = obj->find_first();
  XMLObj *o;

  while ((o = iter.get_next())) {
    T val;
    decode_xml_obj(val, o);
    v.push_back(val);
  }
}

template<class T>
bool RGWXMLDecoder::decode_xml(const char *name, T& val, XMLObj *obj, bool mandatory)
{
  XMLObjIter iter = obj->find(name);
  XMLObj *o = iter.get_next();
  if (!o) {
    if (mandatory) {
      string s = "missing mandatory field " + string(name);
      throw err(s);
    }
    val = T();
    return false;
  }

  try {
    decode_xml_obj(val, o);
  } catch (err& e) {
    string s = string(name) + ": ";
    s.append(e.message);
    throw err(s);
  }

  return true;
}

template<class C>
bool RGWXMLDecoder::decode_xml(const char *name, C& container, void (*cb)(C&, XMLObj *), XMLObj *obj, bool mandatory)
{
  container.clear();

  XMLObjIter iter = obj->find(name);
  XMLObj *o = iter.get_next();
  if (!o) {
    if (mandatory) {
      string s = "missing mandatory field " + string(name);
      throw err(s);
    }
    return false;
  }

  try {
    decode_xml_obj(container, cb, o);
  } catch (err& e) {
    string s = string(name) + ": ";
    s.append(e.message);
    throw err(s);
  }

  return true;
}

template<class T>
void RGWXMLDecoder::decode_xml(const char *name, T& val, T& default_val, XMLObj *obj)
{
  XMLObjIter iter = obj->find(name);
  XMLObj *o = iter.get_next();
  if (!o) {
    val = default_val;
    return;
  }

  try {
    decode_xml_obj(val, o);
  } catch (err& e) {
    val = default_val;
    string s = string(name) + ": ";
    s.append(e.message);
    throw err(s);
  }
}

template<class T>
static void encode_xml(const char *name, const T& val, ceph::Formatter *f)
{
  f->open_object_section(name);
  val.dump_xml(f);
  f->close_section();
}

void encode_xml(const char *name, const string& val, ceph::Formatter *f);
void encode_xml(const char *name, const char *val, ceph::Formatter *f);
void encode_xml(const char *name, bool val, ceph::Formatter *f);
void encode_xml(const char *name, int val, ceph::Formatter *f);
void encode_xml(const char *name, unsigned val, ceph::Formatter *f);
void encode_xml(const char *name, long val, ceph::Formatter *f);
void encode_xml(const char *name, unsigned long val, ceph::Formatter *f);
void encode_xml(const char *name, long long val, ceph::Formatter *f);
void encode_xml(const char *name, const utime_t& val, ceph::Formatter *f);
void encode_xml(const char *name, const bufferlist& bl, ceph::Formatter *f);
void encode_xml(const char *name, long long unsigned val, ceph::Formatter *f);

template<class T>
static void do_encode_xml(const char *name, const std::list<T>& l, const char *entry_name, ceph::Formatter *f)
{
  f->open_array_section(name);
  for (typename std::list<T>::const_iterator iter = l.begin(); iter != l.end(); ++iter) {
    encode_xml(entry_name, *iter, f);
  }
  f->close_section();
}



#endif
