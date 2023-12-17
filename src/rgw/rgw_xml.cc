// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include <string.h>

#include <iostream>
#include <map>

#include <expat.h>

#include "include/types.h"
#include "include/utime.h"

#include "rgw_xml.h"

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

bool XMLObjIter::get_name(std::string& name) const
{
  if (cur == end) {
    return false;
  }

  name = cur->first;
  return true;
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
    attr_map[attr[i]] = std::string(attr[i + 1]);
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

const std::string& XMLObj::
XMLObj::get_data() const
{
  return data;
}

const std::string& XMLObj::
XMLObj::get_obj_type() const
{
  return obj_type;
}

XMLObj *XMLObj::
XMLObj::get_parent()
{
  return parent;
}

void XMLObj::
add_child(const std::string& el, XMLObj *obj)
{
  children.insert(std::pair<std::string, XMLObj *>(el, obj));
}

bool XMLObj::
get_attr(const std::string& name, std::string& attr) const
{
  const std::map<std::string, std::string>::const_iterator iter = attr_map.find(name);
  if (iter == attr_map.end())
    return false;
  attr = iter->second;
  return true;
}

XMLObjIter XMLObj::
find(const std::string& name)
{
  XMLObjIter iter;
  const XMLObjIter::const_map_iter_t first = children.find(name);
  XMLObjIter::const_map_iter_t last;
  if (first != children.end()) {
    last = children.upper_bound(name);
  }else
    last = children.end();
  iter.set(first, last);
  return iter;
}

XMLObjIter XMLObj::find_first()
{
  XMLObjIter iter;
  const XMLObjIter::const_map_iter_t first = children.begin();
  const XMLObjIter::const_map_iter_t last = children.end();
  iter.set(first, last);
  return iter;
}

XMLObj *XMLObj::
find_first(const std::string& name)
{
  const XMLObjIter::const_map_iter_t first = children.find(name);
  if (first != children.end())
    return first->second;
  return nullptr;
}

RGWXMLParser::
RGWXMLParser() : buf(nullptr), buf_len(0), cur_obj(nullptr), success(true), init_called(false)
{
  p = XML_ParserCreate(nullptr);
}

RGWXMLParser::
~RGWXMLParser()
{
  XML_ParserFree(p);

  free(buf);
  std::list<XMLObj *>::const_iterator iter;
  for (iter = allocated_objs.begin(); iter != allocated_objs.end(); ++iter) {
    XMLObj *obj = *iter;
    delete obj;
  }
}

void RGWXMLParser::call_xml_start(void* user_data, const char *el, const char **attr) {
  RGWXMLParser *handler = static_cast<RGWXMLParser *>(user_data);
  XMLObj * obj = handler->alloc_obj(el);
  if (!obj) {
    handler->unallocated_objs.push_back(XMLObj());
    obj = &handler->unallocated_objs.back();
  } else {
    handler->allocated_objs.push_back(obj);
  }
  if (!obj->xml_start(handler->cur_obj, el, attr)) {
    handler->success = false;
    return;
  }
  if (handler->cur_obj) {
    handler->cur_obj->add_child(el, obj);
  } else {
    handler->children.insert(std::pair<std::string, XMLObj *>(el, obj));
  }
  handler->cur_obj = obj;

  handler->objs.push_back(obj);
}

void RGWXMLParser::call_xml_end(void* user_data, const char *el) {
  RGWXMLParser *handler = static_cast<RGWXMLParser *>(user_data);
  XMLObj *parent_obj = handler->cur_obj->get_parent();
  if (!handler->cur_obj->xml_end(el)) {
    handler->success = false;
    return;
  }
  handler->cur_obj = parent_obj;
}

void RGWXMLParser::call_xml_handle_data(void* user_data, const char *s, int len)
{
  RGWXMLParser *handler = static_cast<RGWXMLParser *>(user_data);
  handler->cur_obj->xml_handle_data(s, len);
}

bool RGWXMLParser::init()
{
  if (!p) {
    return false;
  }
  init_called = true;
  XML_SetElementHandler(p, RGWXMLParser::call_xml_start, RGWXMLParser::call_xml_end);
  XML_SetCharacterDataHandler(p, RGWXMLParser::call_xml_handle_data);
  XML_SetUserData(p, (void *)this);
  return true;
}

bool RGWXMLParser::parse(const char *_buf, int len, int done)
{
  ceph_assert(init_called);
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
  auto& s = obj->get_data();
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
  const std::string s = obj->get_data();
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
  const std::string s = obj->get_data();
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
  const std::string s = obj->get_data();
  const char *start = s.c_str();
  char *p;

  errno = 0;
  val = strtoull(start, &p, 10);

  /* Check for various possible errors */

 if ((errno == ERANGE && val == ULLONG_MAX) ||
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
  const std::string s = obj->get_data();
  if (strncasecmp(s.c_str(), "true", 8) == 0) {
    val = true;
    return;
  }
  if (strncasecmp(s.c_str(), "false", 8) == 0) {
    val = false;
    return;
  }
  int i;
  decode_xml_obj(i, obj);
  val = (bool)i;
}

void decode_xml_obj(bufferlist& val, XMLObj *obj)
{
  const std::string s = obj->get_data();

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
  const std::string s = obj->get_data();
  uint64_t epoch;
  uint64_t nsec;
  int r = utime_t::parse_date(s, &epoch, &nsec);
  if (r == 0) {
    val = utime_t(epoch, nsec);
  } else {
    throw RGWXMLDecoder::err("failed to decode utime_t");
  }
}

void decode_xml_obj(ceph::real_time& val, XMLObj *obj)
{
  const std::string s = obj->get_data();
  uint64_t epoch;
  uint64_t nsec;
  int r = utime_t::parse_date(s, &epoch, &nsec);
  if (r == 0) {
    using namespace std::chrono;
    val = real_time{seconds(epoch) + nanoseconds(nsec)};
  } else {
    throw RGWXMLDecoder::err("failed to decode real_time");
  }
}

void encode_xml(const char *name, const string& val, Formatter *f)
{
  f->dump_string(name, val);
}

void encode_xml(const char *name, const string_view & val, Formatter *f)
{
  f->dump_string(name, val);
}

void encode_xml(const char *name, const char *val, Formatter *f)
{
  f->dump_string(name, val);
}

void encode_xml(const char *name, bool val, Formatter *f)
{
  std::string s;
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

  const std::string s(b64.c_str(), b64.length());

  encode_xml(name, s, f);
}

