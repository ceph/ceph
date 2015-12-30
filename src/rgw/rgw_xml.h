// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_RGW_XML_H
#define CEPH_RGW_XML_H

#include <map>
#include <string>
#include <iosfwd>
#include <include/types.h>

#include <expat.h>

using namespace std;


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
  XMLObj *find_first(string name);

  friend ostream& operator<<(ostream& out, XMLObj& obj);
};

class RGWXMLParser : public XMLObj
{
  XML_Parser p;
  char *buf;
  int buf_len;
  XMLObj *cur_obj;
  vector<XMLObj *> objs;
protected:
  virtual XMLObj *alloc_obj(const char *el) = 0;
public:
  RGWXMLParser();
  virtual ~RGWXMLParser();
  bool init();
  bool xml_start(const char *el, const char **attr);
  bool xml_end(const char *el);
  void handle_data(const char *s, int len);

  bool parse(const char *buf, int len, int done);
  const char *get_xml() { return buf; }
  void set_failure() { success = false; }

private:
  bool success;
};

#endif
