// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <string.h>

#include <iostream>
#include <map>

#include <expat.h>

#include "include/types.h"

#include "rgw_common.h"
#include "rgw_xml.h"

#define dout_subsys ceph_subsys_rgw

using namespace std;

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
}

ostream& operator<<(ostream &out, const XMLObj &obj) {
   out << obj.obj_type << ": " << obj.data;
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
  obj_type = el;
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
  data.append(s, len);
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
  if (first != children.end()) {
    last = children.upper_bound(name);
  }else
    last = children.end();
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
static void xml_start(void *data, const char *el, const char **attr) {
  RGWXMLParser *handler = static_cast<RGWXMLParser *>(data);

  if (!handler->xml_start(el, attr))
    handler->set_failure();
}

RGWXMLParser::
RGWXMLParser() : buf(NULL), buf_len(0), cur_obj(NULL), success(true)
{
  p = XML_ParserCreate(NULL);
}

RGWXMLParser::
~RGWXMLParser()
{
  XML_ParserFree(p);

  free(buf);
  list<XMLObj *>::iterator iter;
  for (iter = allocated_objs.begin(); iter != allocated_objs.end(); ++iter) {
    XMLObj *obj = *iter;
    delete obj;
  }
}


bool RGWXMLParser::xml_start(const char *el, const char **attr) {
  XMLObj * obj = alloc_obj(el);
  if (!obj) {
    unallocated_objs.push_back(XMLObj());
    obj = &unallocated_objs.back();
  } else {
    allocated_objs.push_back(obj);
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

static void xml_end(void *data, const char *el) {
  RGWXMLParser *handler = static_cast<RGWXMLParser *>(data);

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

static void handle_data(void *data, const char *s, int len)
{
  RGWXMLParser *handler = static_cast<RGWXMLParser *>(data);

  handler->handle_data(s, len);
}

void RGWXMLParser::handle_data(const char *s, int len)
{
  cur_obj->xml_handle_data(s, len);
}


bool RGWXMLParser::init()
{
  if (!p) {
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
  char *tmp_buf;
  tmp_buf = (char *)realloc(buf, buf_len + len);
  if (tmp_buf == NULL){
    free(buf);
    buf = NULL;
    return false;
  } else {
    buf = tmp_buf;
  }

  memcpy(&buf[buf_len], _buf, len);
  buf_len += len;

  success = true;
  if (!XML_Parse(p, &buf[pos], len, done)) {
    fprintf(stderr, "Parse error at line %d:\n%s\n",
	      (int)XML_GetCurrentLineNumber(p),
	      XML_ErrorString(XML_GetErrorCode(p)));
    success = false;
  }

  return success;
}

void decode_xml_obj(unsigned long& val, XMLObj *obj)
{
  string& s = obj->get_data();
  const char *start = s.c_str();
  char *p;

  errno = 0;
  val = strtoul(start, &p, 10);

  /* Check for various possible errors */

 if ((errno == ERANGE && val == ULONG_MAX) ||
     (errno != 0 && val == 0)) {
   throw RGWXMLDecoder::err("failed to number");
 }

 if (p == start) {
   throw RGWXMLDecoder::err("failed to parse number");
 }

 while (*p != '\0') {
   if (!isspace(*p)) {
     throw RGWXMLDecoder::err("failed to parse number");
   }
   p++;
 }
}


void decode_xml_obj(long& val, XMLObj *obj)
{
  string s = obj->get_data();
  const char *start = s.c_str();
  char *p;

  errno = 0;
  val = strtol(start, &p, 10);

  /* Check for various possible errors */

 if ((errno == ERANGE && (val == LONG_MAX || val == LONG_MIN)) ||
     (errno != 0 && val == 0)) {
   throw RGWXMLDecoder::err("failed to parse number");
 }

 if (p == start) {
   throw RGWXMLDecoder::err("failed to parse number");
 }

 while (*p != '\0') {
   if (!isspace(*p)) {
     throw RGWXMLDecoder::err("failed to parse number");
   }
   p++;
 }
}

void decode_xml_obj(long long& val, XMLObj *obj)
{
  string s = obj->get_data();
  const char *start = s.c_str();
  char *p;

  errno = 0;
  val = strtoll(start, &p, 10);

  /* Check for various possible errors */

 if ((errno == ERANGE && (val == LLONG_MAX || val == LLONG_MIN)) ||
     (errno != 0 && val == 0)) {
   throw RGWXMLDecoder::err("failed to parse number");
 }

 if (p == start) {
   throw RGWXMLDecoder::err("failed to parse number");
 }

 while (*p != '\0') {
   if (!isspace(*p)) {
     throw RGWXMLDecoder::err("failed to parse number");
   }
   p++;
 }
}

void decode_xml_obj(unsigned long long& val, XMLObj *obj)
{
  string s = obj->get_data();
  const char *start = s.c_str();
  char *p;

  errno = 0;
  val = strtoull(start, &p, 10);

  /* Check for various possible errors */

 if ((errno == ERANGE && val == ULLONG_MAX) ||
     (errno != 0 && val == 0)) {
   throw RGWXMLDecoder::err("failed to number");
 }

 if (p == start) {
   throw RGWXMLDecoder::err("failed to parse number");
 }

 while (*p != '\0') {
   if (!isspace(*p)) {
     throw RGWXMLDecoder::err("failed to parse number");
   }
   p++;
 }
}

void decode_xml_obj(int& val, XMLObj *obj)
{
  long l;
  decode_xml_obj(l, obj);
#if LONG_MAX > INT_MAX
  if (l > INT_MAX || l < INT_MIN) {
    throw RGWXMLDecoder::err("integer out of range");
  }
#endif

  val = (int)l;
}

void decode_xml_obj(unsigned& val, XMLObj *obj)
{
  unsigned long l;
  decode_xml_obj(l, obj);
#if ULONG_MAX > UINT_MAX
  if (l > UINT_MAX) {
    throw RGWXMLDecoder::err("unsigned integer out of range");
  }
#endif

  val = (unsigned)l;
}

void decode_xml_obj(bool& val, XMLObj *obj)
{
  string s = obj->get_data();
  if (strcasecmp(s.c_str(), "true") == 0) {
    val = true;
    return;
  }
  if (strcasecmp(s.c_str(), "false") == 0) {
    val = false;
    return;
  }
  int i;
  decode_xml_obj(i, obj);
  val = (bool)i;
}

void decode_xml_obj(bufferlist& val, XMLObj *obj)
{
  string s = obj->get_data();

  bufferlist bl;
  bl.append(s.c_str(), s.size());
  try {
    val.decode_base64(bl);
  } catch (buffer::error& err) {
   throw RGWXMLDecoder::err("failed to decode base64");
  }
}

void decode_xml_obj(utime_t& val, XMLObj *obj)
{
  string s = obj->get_data();
  uint64_t epoch;
  uint64_t nsec;
  int r = utime_t::parse_date(s, &epoch, &nsec);
  if (r == 0) {
    val = utime_t(epoch, nsec);
  } else {
    throw RGWXMLDecoder::err("failed to decode utime_t");
  }
}

void encode_xml(const char *name, const string& val, Formatter *f)
{
  f->dump_string(name, val);
}

void encode_xml(const char *name, const char *val, Formatter *f)
{
  f->dump_string(name, val);
}

void encode_xml(const char *name, bool val, Formatter *f)
{
  string s;
  if (val)
    s = "True";
  else
    s = "False";

  f->dump_string(name, s);
}

void encode_xml(const char *name, int val, Formatter *f)
{
  f->dump_int(name, val);
}

void encode_xml(const char *name, long val, Formatter *f)
{
  f->dump_int(name, val);
}

void encode_xml(const char *name, unsigned val, Formatter *f)
{
  f->dump_unsigned(name, val);
}

void encode_xml(const char *name, unsigned long val, Formatter *f)
{
  f->dump_unsigned(name, val);
}

void encode_xml(const char *name, unsigned long long val, Formatter *f)
{
  f->dump_unsigned(name, val);
}

void encode_xml(const char *name, long long val, Formatter *f)
{
  f->dump_int(name, val);
}

void encode_xml(const char *name, const utime_t& val, Formatter *f)
{
  val.gmtime(f->dump_stream(name));
}

void encode_xml(const char *name, const bufferlist& bl, Formatter *f)
{
  /* need to copy data from bl, as it is const bufferlist */
  bufferlist src = bl;

  bufferlist b64;
  src.encode_base64(b64);

  string s(b64.c_str(), b64.length());

  encode_xml(name, s, f);
}

