// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#pragma once

#include <map>
#include <stdexcept>
#include <string>
#include <iosfwd>
#include <include/types.h>
#include <common/Formatter.h>
#include "common/ceph_time.h"

class XMLObj;
class RGWXMLParser;

class XMLObjIter {
public:
  typedef std::map<std::string, XMLObj *>::iterator map_iter_t;
  typedef std::map<std::string, XMLObj *>::iterator const_map_iter_t;

  XMLObjIter();
  virtual ~XMLObjIter();
  void set(const XMLObjIter::const_map_iter_t &_cur, const XMLObjIter::const_map_iter_t &_end);
  XMLObj *get_next();
  bool get_name(std::string& name) const;

private:
  map_iter_t cur;
  map_iter_t end;
};

/**
 * Represents a block of XML.
 * Give the class an XML blob, and it will parse the blob into
 * an attr_name->value map.
 * It shouldn't be the start point for any parsing. Look at RGWXMLParser for that.
 */
class XMLObj
{
private:
  XMLObj *parent;
  std::string obj_type;

protected:
  std::string data;
  std::multimap<std::string, XMLObj *> children;
  std::map<std::string, std::string> attr_map;

  // invoked at the beginning of the XML tag, and populate any attributes
  bool xml_start(XMLObj *parent, const char *el, const char **attr);
  // callback invoked at the end of the XML tag
  // if objects are created while parsing, this should be overwritten in the drived class
  virtual bool xml_end(const char *el);
  // callback invoked for storing the data of the XML tag
  // if data manipulation is needed this could be overwritten in the drived class
  virtual void xml_handle_data(const char *s, int len);
  // get the parent object
  XMLObj *get_parent();
  // add a child XML object
  void add_child(const std::string& el, XMLObj *obj);

public:
  XMLObj() : parent(nullptr) {}
  virtual ~XMLObj();

  // get the data (as string)
  const std::string& get_data() const;
  // get the type of the object (as string)
  const std::string& get_obj_type() const;
  bool get_attr(const std::string& name, std::string& attr) const;
  // return a list of sub-tags matching the name
  XMLObjIter find(const std::string& name);
  // return the first sub-tag
  XMLObjIter find_first();
  // return the first sub-tags matching the name
  XMLObj *find_first(const std::string& name);

  friend std::ostream& operator<<(std::ostream &out, const XMLObj &obj);
  friend RGWXMLParser;
};

struct XML_ParserStruct;

// an XML parser is an XML object without a parent (root of the tree)
// the parser could be used in 2 ways:
//
// (1) lazy object creation/intrusive API: usually used within the RGWXMLDecode namespace (as RGWXMLDecode::XMLParser)
// the parser will parse the input and store info, but will not generate the target object. The object can be allocated outside
// of the parser (stack or heap), and require to implement the decode_xml() API for the values to be populated.
// note that the decode_xml() calls may throw exceptions if parsing fails
//
// (2) object creation while parsing: a new class needs to be derived from RGWXMLParser and implement alloc_obj()
// API that should create a set of classes derived from XMLObj implementing xml_end() to create the actual target objects
//
// There could be a mix-and-match of the 2 types, control over that is in the alloc_obj() call
// deciding for which tags objects are allocate during parsing and for which tags object allocation is external

class RGWXMLParser : public XMLObj
{
private:
  XML_ParserStruct *p;
  char *buf;
  int buf_len;
  XMLObj *cur_obj;
  std::vector<XMLObj *> objs;
  std::list<XMLObj *> allocated_objs;
  std::list<XMLObj> unallocated_objs;
  bool success;
  bool init_called;

  // calls xml_start() on each parsed object
  // passed as static callback to actual parser, passes itself as user_data
  static void call_xml_start(void* user_data, const char *el, const char **attr);
  // calls xml_end() on each parsed object
  // passed as static callback to actual parser, passes itself as user_data
  static void call_xml_end(void* user_data, const char *el);
  // calls xml_handle_data() on each parsed object
  // passed as static callback to actual parser, passes itself as user_data
  static void call_xml_handle_data(void* user_data, const char *s, int len);

protected:
  // if objects are created while parsing, this should be implemented in the derived class
  // and be a factory for creating the classes derived from XMLObj
  // note that not all sub-tags has to be constructed here, any such tag which is not
  // constructed will be lazily created when decode_xml() is invoked on it
  //
  // note that in case of different tags sharing the same name at different levels
  // this method should not be used
  virtual XMLObj *alloc_obj(const char *el) {
    return nullptr;
  }

public:
  RGWXMLParser();
  virtual ~RGWXMLParser() override;

  // initialize the parser, must be called before parsing
  bool init();
  // parse the XML buffer (can be invoked multiple times for incremental parsing)
  // receives the buffer to parse, its length, and boolean indication (0,1)
  // whether this is the final chunk of the buffer
  bool parse(const char *buf, int len, int done);
  // get the XML blob being parsed
  const char *get_xml() const { return buf; }
};

namespace RGWXMLDecoder {
  struct err : std::runtime_error {
    using runtime_error::runtime_error;
  };

  typedef RGWXMLParser XMLParser;

  template<class T>
  bool decode_xml(const char *name, T& val, XMLObj* obj, bool mandatory = false);

  template<class T>
  bool decode_xml(const char *name, std::vector<T>& v, XMLObj* obj, bool mandatory = false);

  template<class C>
  bool decode_xml(const char *name, C& container, void (*cb)(C&, XMLObj *obj), XMLObj *obj, bool mandatory = false);

  template<class T>
  void decode_xml(const char *name, T& val, T& default_val, XMLObj* obj);
}

static inline std::ostream& operator<<(std::ostream &out, RGWXMLDecoder::err& err)
{
  return out << err.what();
}

template<class T>
void decode_xml_obj(T& val, XMLObj *obj)
{
  val.decode_xml(obj);
}

static inline void decode_xml_obj(std::string& val, XMLObj *obj)
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
void decode_xml_obj(ceph::real_time& val, XMLObj *obj);

template<class T>
void decode_xml_obj(std::optional<T>& val, XMLObj *obj)
{
  val.emplace();
  decode_xml_obj(*val, obj);
}

template<class T>
void do_decode_xml_obj(std::list<T>& l, const std::string& name, XMLObj *obj)
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
bool RGWXMLDecoder::decode_xml(const char *name, T& val, XMLObj *obj, bool mandatory)
{
  XMLObjIter iter = obj->find(name);
  XMLObj *o = iter.get_next();
  if (!o) {
    if (mandatory) {
      std::string s = "missing mandatory field " + std::string(name);
      throw err(s);
    }
    val = T();
    return false;
  }

  try {
    decode_xml_obj(val, o);
  } catch (const err& e) {
    std::string s = std::string(name) + ": ";
    s.append(e.what());
    throw err(s);
  }

  return true;
}

template<class T>
bool RGWXMLDecoder::decode_xml(const char *name, std::vector<T>& v, XMLObj *obj, bool mandatory)
{
  XMLObjIter iter = obj->find(name);
  XMLObj *o = iter.get_next();

  v.clear();

  if (!o) {
    if (mandatory) {
      std::string s = "missing mandatory field " + std::string(name);
      throw err(s);
    }
    return false;
  }

  do {
    T val;
    try {
      decode_xml_obj(val, o);
    } catch (const err& e) {
      std::string s = std::string(name) + ": ";
      s.append(e.what());
      throw err(s);
    }
    v.push_back(val);
  } while ((o = iter.get_next()));
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
      std::string s = "missing mandatory field " + std::string(name);
      throw err(s);
    }
    return false;
  }

  try {
    decode_xml_obj(container, cb, o);
  } catch (const err& e) {
    std::string s = std::string(name) + ": ";
    s.append(e.what());
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
  } catch (const err& e) {
    val = default_val;
    std::string s = std::string(name) + ": ";
    s.append(e.what());
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

template<class T>
static void encode_xml(const char *name, const char *ns, const T& val, ceph::Formatter *f)
{
  f->open_object_section_in_ns(name, ns);
  val.dump_xml(f);
  f->close_section();
}

void encode_xml(const char *name, const std::string& val, ceph::Formatter *f);
void encode_xml(const char *name, const std::string_view& val, ceph::Formatter *f);
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

template<class T>
static void encode_xml(const char *name, const std::vector<T>& l, ceph::Formatter *f)
{
  for (typename std::vector<T>::const_iterator iter = l.begin(); iter != l.end(); ++iter) {
    encode_xml(name, *iter, f);
  }
}

template<class T>
static void encode_xml(const char *name, const std::optional<T>& o, ceph::Formatter *f)
{
  if (!o) {
    return;
  }

  encode_xml(name, *o, f);
}
