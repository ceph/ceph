#include "common/ceph_json.h"
#include "include/utime.h"

// for testing DELETE ME
#include <fstream>
#include <include/types.h>

#include <boost/algorithm/string.hpp>
#include <boost/tokenizer.hpp>
#include <boost/lexical_cast.hpp>

#include "json_spirit/json_spirit_writer_template.h"

using namespace json_spirit;

#define dout_subsys ceph_subsys_rgw


static JSONFormattable default_formattable;


void encode_json(const char *name, const JSONObj::data_val& v, Formatter *f)
{
  if (v.quoted) {
    encode_json(name, v.str, f);
  } else {
    f->dump_format_unquoted(name, "%s", v.str.c_str());
  }
}

JSONObjIter::JSONObjIter()
{
}

JSONObjIter::~JSONObjIter()
{
}

void JSONObjIter::set(const JSONObjIter::map_iter_t &_cur, const JSONObjIter::map_iter_t &_last)
{
  cur = _cur;
  last = _last;
}

void JSONObjIter::operator++()
{
  if (cur != last)
    ++cur;
}

JSONObj *JSONObjIter::operator*()
{
  return cur->second;
}

// does not work, FIXME
ostream& operator<<(ostream &out, const JSONObj &obj) {
   out << obj.name << ": " << obj.val;
   return out;
}

JSONObj::~JSONObj()
{
  multimap<string, JSONObj *>::iterator iter;
  for (iter = children.begin(); iter != children.end(); ++iter) {
    JSONObj *obj = iter->second;
    delete obj;
  }
}


void JSONObj::add_child(string el, JSONObj *obj)
{
  children.insert(pair<string, JSONObj *>(el, obj));
}

bool JSONObj::get_attr(string name, data_val& attr)
{
  auto iter = attr_map.find(name);
  if (iter == attr_map.end())
    return false;
  attr = iter->second;
  return true;
}

JSONObjIter JSONObj::find(const string& name)
{
  JSONObjIter iter;
  map<string, JSONObj *>::iterator first;
  map<string, JSONObj *>::iterator last;
  first = children.find(name);
  if (first != children.end()) {
    last = children.upper_bound(name);
    iter.set(first, last);
  }
  return iter;
}

JSONObjIter JSONObj::find_first()
{
  JSONObjIter iter;
  iter.set(children.begin(), children.end());
  return iter;
}

JSONObjIter JSONObj::find_first(const string& name)
{
  JSONObjIter iter;
  map<string, JSONObj *>::iterator first;
  first = children.find(name);
  iter.set(first, children.end());
  return iter;
}

JSONObj *JSONObj::find_obj(const string& name)
{
  JSONObjIter iter = find(name);
  if (iter.end())
    return NULL;

  return *iter;
}

bool JSONObj::get_data(const string& key, data_val *dest)
{
  JSONObj *obj = find_obj(key);
  if (!obj)
    return false;

  *dest = obj->get_data_val();

  return true;
}

/* accepts a JSON Array or JSON Object contained in
 * a JSON Spirit Value, v,  and creates a JSONObj for each
 * child contained in v
 */
void JSONObj::handle_value(Value v)
{
  if (v.type() == obj_type) {
    Object temp_obj = v.get_obj();
    for (Object::size_type i = 0; i < temp_obj.size(); i++) {
      Pair temp_pair = temp_obj[i];
      string temp_name = temp_pair.name_;
      Value temp_value = temp_pair.value_;
      JSONObj *child = new JSONObj;
      child->init(this, temp_value, temp_name);
      add_child(temp_name, child);
    }
  } else if (v.type() == array_type) {
    Array temp_array = v.get_array();
    Value value;

    for (unsigned j = 0; j < temp_array.size(); j++) {
      Value cur = temp_array[j];
      string temp_name;

      JSONObj *child = new JSONObj;
      child->init(this, cur, temp_name);
      add_child(child->get_name(), child);
    }
  }
}

void JSONObj::init(JSONObj *p, Value v, string n)
{
  name = n;
  parent = p;
  data = v;

  handle_value(v);
  if (v.type() == str_type) {
    val.set(v.get_str(), true);
  } else {
    val.set(json_spirit::write_string(v), false);
  }
  attr_map.insert(pair<string,data_val>(name, val));
}

JSONObj *JSONObj::get_parent()
{
  return parent;
}

bool JSONObj::is_object()
{
  return (data.type() == obj_type);
}

bool JSONObj::is_array()
{
  return (data.type() == array_type);
}

vector<string> JSONObj::get_array_elements()
{
  vector<string> elements;
  Array temp_array;

  if (data.type() == array_type)
    temp_array = data.get_array();

  int array_size = temp_array.size();
  if (array_size > 0)
    for (int i = 0; i < array_size; i++) {
      Value temp_value = temp_array[i];
      string temp_string;
      temp_string = write(temp_value, raw_utf8);
      elements.push_back(temp_string);
    }

  return elements;
}

JSONParser::JSONParser() : buf_len(0), success(true)
{
}

JSONParser::~JSONParser()
{
}



void JSONParser::handle_data(const char *s, int len)
{
  json_buffer.append(s, len); // check for problems with null termination FIXME
  buf_len += len;
}

// parse a supplied JSON fragment
bool JSONParser::parse(const char *buf_, int len)
{
  if (!buf_) {
    set_failure();
    return false;
  }

  string json_string(buf_, len);
  success = read(json_string, data);
  if (success) {
    handle_value(data);
    if (data.type() != obj_type &&
        data.type() != array_type) {
      if (data.type() == str_type) {
        val.set(data.get_str(), true);
      } else {
        val.set(json_spirit::write_string(data), false);
      }
    }
  } else {
    set_failure();
  }

  return success;
}

// parse the internal json_buffer up to len
bool JSONParser::parse(int len)
{
  string json_string = json_buffer.substr(0, len);
  success = read(json_string, data);
  if (success)
    handle_value(data);
  else
    set_failure();

  return success;
}

// parse the complete internal json_buffer
bool JSONParser::parse()
{
  success = read(json_buffer, data);
  if (success)
    handle_value(data);
  else
    set_failure();

  return success;
}

// parse a supplied ifstream, for testing. DELETE ME
bool JSONParser::parse(const char *file_name)
{
  ifstream is(file_name);
  success = read(is, data);
  if (success)
    handle_value(data);
  else
    set_failure();

  return success;
}


void decode_json_obj(long& val, JSONObj *obj)
{
  string s = obj->get_data();
  const char *start = s.c_str();
  char *p;

  errno = 0;
  val = strtol(start, &p, 10);

  /* Check for various possible errors */

 if ((errno == ERANGE && (val == LONG_MAX || val == LONG_MIN)) ||
     (errno != 0 && val == 0)) {
   throw JSONDecoder::err("failed to parse number");
 }

 if (p == start) {
   throw JSONDecoder::err("failed to parse number");
 }

 while (*p != '\0') {
   if (!isspace(*p)) {
     throw JSONDecoder::err("failed to parse number");
   }
   p++;
 }
}

void decode_json_obj(unsigned long& val, JSONObj *obj)
{
  string s = obj->get_data();
  const char *start = s.c_str();
  char *p;

  errno = 0;
  val = strtoul(start, &p, 10);

  /* Check for various possible errors */

 if ((errno == ERANGE && val == ULONG_MAX) ||
     (errno != 0 && val == 0)) {
   throw JSONDecoder::err("failed to number");
 }

 if (p == start) {
   throw JSONDecoder::err("failed to parse number");
 }

 while (*p != '\0') {
   if (!isspace(*p)) {
     throw JSONDecoder::err("failed to parse number");
   }
   p++;
 }
}

void decode_json_obj(long long& val, JSONObj *obj)
{
  string s = obj->get_data();
  const char *start = s.c_str();
  char *p;

  errno = 0;
  val = strtoll(start, &p, 10);

  /* Check for various possible errors */

 if ((errno == ERANGE && (val == LLONG_MAX || val == LLONG_MIN)) ||
     (errno != 0 && val == 0)) {
   throw JSONDecoder::err("failed to parse number");
 }

 if (p == start) {
   throw JSONDecoder::err("failed to parse number");
 }

 while (*p != '\0') {
   if (!isspace(*p)) {
     throw JSONDecoder::err("failed to parse number");
   }
   p++;
 }
}

void decode_json_obj(unsigned long long& val, JSONObj *obj)
{
  string s = obj->get_data();
  const char *start = s.c_str();
  char *p;

  errno = 0;
  val = strtoull(start, &p, 10);

  /* Check for various possible errors */

 if ((errno == ERANGE && val == ULLONG_MAX) ||
     (errno != 0 && val == 0)) {
   throw JSONDecoder::err("failed to number");
 }

 if (p == start) {
   throw JSONDecoder::err("failed to parse number");
 }

 while (*p != '\0') {
   if (!isspace(*p)) {
     throw JSONDecoder::err("failed to parse number");
   }
   p++;
 }
}

void decode_json_obj(int& val, JSONObj *obj)
{
  long l;
  decode_json_obj(l, obj);
#if LONG_MAX > INT_MAX
  if (l > INT_MAX || l < INT_MIN) {
    throw JSONDecoder::err("integer out of range");
  }
#endif

  val = (int)l;
}

void decode_json_obj(unsigned& val, JSONObj *obj)
{
  unsigned long l;
  decode_json_obj(l, obj);
#if ULONG_MAX > UINT_MAX
  if (l > UINT_MAX) {
    throw JSONDecoder::err("unsigned integer out of range");
  }
#endif

  val = (unsigned)l;
}

void decode_json_obj(bool& val, JSONObj *obj)
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
  decode_json_obj(i, obj);
  val = (bool)i;
}

void decode_json_obj(bufferlist& val, JSONObj *obj)
{
  string s = obj->get_data();

  bufferlist bl;
  bl.append(s.c_str(), s.size());
  try {
    val.decode_base64(bl);
  } catch (buffer::error& err) {
   throw JSONDecoder::err("failed to decode base64");
  }
}

void decode_json_obj(utime_t& val, JSONObj *obj)
{
  string s = obj->get_data();
  uint64_t epoch;
  uint64_t nsec;
  int r = utime_t::parse_date(s, &epoch, &nsec);
  if (r == 0) {
    val = utime_t(epoch, nsec);
  } else {
    throw JSONDecoder::err("failed to decode utime_t");
  }
}

void encode_json(const char *name, const string& val, Formatter *f)
{
  f->dump_string(name, val);
}

void encode_json(const char *name, const char *val, Formatter *f)
{
  f->dump_string(name, val);
}

void encode_json(const char *name, bool val, Formatter *f)
{
  string s;
  if (val)
    s = "true";
  else
    s = "false";

  f->dump_string(name, s);
}

void encode_json(const char *name, int val, Formatter *f)
{
  f->dump_int(name, val);
}

void encode_json(const char *name, long val, Formatter *f)
{
  f->dump_int(name, val);
}

void encode_json(const char *name, unsigned val, Formatter *f)
{
  f->dump_unsigned(name, val);
}

void encode_json(const char *name, unsigned long val, Formatter *f)
{
  f->dump_unsigned(name, val);
}

void encode_json(const char *name, unsigned long long val, Formatter *f)
{
  f->dump_unsigned(name, val);
}

void encode_json(const char *name, long long val, Formatter *f)
{
  f->dump_int(name, val);
}

void encode_json(const char *name, const utime_t& val, Formatter *f)
{
  val.gmtime(f->dump_stream(name));
}

void encode_json(const char *name, const bufferlist& bl, Formatter *f)
{
  /* need to copy data from bl, as it is const bufferlist */
  bufferlist src = bl;

  bufferlist b64;
  src.encode_base64(b64);

  string s(b64.c_str(), b64.length());

  encode_json(name, s, f);
}



/* JSONFormattable */

const JSONFormattable& JSONFormattable::operator[](const string& name) const
{
  auto i = obj.find(name);
  if (i == obj.end()) {
    return default_formattable;
  }
  return i->second;
}

const JSONFormattable& JSONFormattable::operator[](size_t index) const
{
  if (index >= arr.size()) {
    return default_formattable;
  }
  return arr[index];
}

JSONFormattable& JSONFormattable::operator[](const string& name)
{
  auto i = obj.find(name);
  if (i == obj.end()) {
    return default_formattable;
  }
  return i->second;
}

JSONFormattable& JSONFormattable::operator[](size_t index)
{
  if (index >= arr.size()) {
    return default_formattable;
  }
  return arr[index];
}

bool JSONFormattable::exists(const string& name) const
{
  auto i = obj.find(name);
  return (i != obj.end());
}

bool JSONFormattable::exists(size_t index) const
{
  return (index < arr.size());
}

bool JSONFormattable::find(const string& name, string *val) const
{
  auto i = obj.find(name);
  if (i == obj.end()) {
    return false;
  }
  *val = i->second.val();
  return true;
}

int JSONFormattable::val_int() const {
  return atoi(value.str.c_str());
}

long JSONFormattable::val_long() const {
  return atol(value.str.c_str());
}

long long JSONFormattable::val_long_long() const {
  return atoll(value.str.c_str());
}

bool JSONFormattable::val_bool() const {
  return (boost::iequals(value.str, "true") ||
          boost::iequals(value.str, "on") ||
          boost::iequals(value.str, "yes") ||
          boost::iequals(value.str, "1"));
}

string JSONFormattable::def(const string& def_val) const {
  if (type == FMT_NONE) {
    return def_val;
  }
  return val();
}

int JSONFormattable::def(int def_val) const {
  if (type == FMT_NONE) {
    return def_val;
  }
  return val_int();
}

bool JSONFormattable::def(bool def_val) const {
  if (type == FMT_NONE) {
    return def_val;
  }
  return val_bool();
}

string JSONFormattable::get(const string& name, const string& def_val) const
{
  return (*this)[name].def(def_val);
}

int JSONFormattable::get_int(const string& name, int def_val) const
{
  return (*this)[name].def(def_val);
}

bool JSONFormattable::get_bool(const string& name, bool def_val) const
{
  return (*this)[name].def(def_val);
}

struct field_entity {
  bool is_obj{false}; /* either obj field or array entity */
  string name; /* if obj */
  int index{0}; /* if array */
  bool append{false};

  field_entity() {}
  explicit field_entity(const string& n) : is_obj(true), name(n) {}
  explicit field_entity(int i) : is_obj(false), index(i) {}
};

static int parse_entity(const string& s, vector<field_entity> *result)
{
  size_t ofs = 0;

  while (ofs < s.size()) {
    size_t next_arr = s.find('[', ofs);
    if (next_arr == string::npos) {
      if (ofs != 0) {
        return -EINVAL;
      }
      result->push_back(field_entity(s));
      return 0;
    }
    if (next_arr > ofs) {
      string field = s.substr(ofs, next_arr - ofs);
      result->push_back(field_entity(field));
      ofs = next_arr;
    }
    size_t end_arr = s.find(']', next_arr + 1);
    if (end_arr == string::npos) {
      return -EINVAL;
    }

    string index_str = s.substr(next_arr + 1, end_arr - next_arr - 1);

    ofs = end_arr + 1;

    if (!index_str.empty()) {
      result->push_back(field_entity(atoi(index_str.c_str())));
    } else {
      field_entity f;
      f.append = true;
      result->push_back(f);
    }
  }
  return 0;
}

static bool is_numeric(const string& val)
{
  try {
    boost::lexical_cast<double>(val);
  } catch (const boost::bad_lexical_cast& e) {
    return false;
  }
  return true;
}

int JSONFormattable::set(const string& name, const string& val)
{
  boost::escaped_list_separator<char> els('\\', '.', '"');
  boost::tokenizer<boost::escaped_list_separator<char> > tok(name, els);

  JSONFormattable *f = this;

  JSONParser jp;

  bool is_valid_json = jp.parse(val.c_str(), val.size());

  for (const auto& i : tok) {
    vector<field_entity> v;
    int ret = parse_entity(i, &v);
    if (ret < 0) {
      return ret;
    }
    for (const auto& vi : v) {
      if (f->type == FMT_NONE) {
        if (vi.is_obj) {
          f->type = FMT_OBJ;
        } else {
          f->type = FMT_ARRAY;
        }
      }

      if (f->type == FMT_OBJ) {
        if (!vi.is_obj) {
          return -EINVAL;
        }
        f = &f->obj[vi.name];
      } else if (f->type == FMT_ARRAY) {
        if (vi.is_obj) {
          return -EINVAL;
        }
        int index = vi.index;
        if (vi.append) {
          index = f->arr.size();
        } else if (index < 0) {
          index = f->arr.size() + index;
          if (index < 0) {
            return -EINVAL; /* out of bounds */
          }
        }
        if ((size_t)index >= f->arr.size()) {
          f->arr.resize(index + 1);
        }
        f = &f->arr[index];
      }
    }
  }

  if (is_valid_json) {
    f->decode_json(&jp);
  } else {
    f->type = FMT_VALUE;
    f->value.set(val, !is_numeric(val));
  }

  return 0;
}

int JSONFormattable::erase(const string& name)
{
  boost::escaped_list_separator<char> els('\\', '.', '"');
  boost::tokenizer<boost::escaped_list_separator<char> > tok(name, els);

  JSONFormattable *f = this;
  JSONFormattable *parent = nullptr;
  field_entity last_entity;

  for (auto& i : tok) {
    vector<field_entity> v;
    int ret = parse_entity(i, &v);
    if (ret < 0) {
      return ret;
    }
    for (const auto& vi : v) {
      if (f->type == FMT_NONE ||
          f->type == FMT_VALUE) {
        if (vi.is_obj) {
          f->type = FMT_OBJ;
        } else {
          f->type = FMT_ARRAY;
        }
      }

      parent = f;

      if (f->type == FMT_OBJ) {
        if (!vi.is_obj) {
          return -EINVAL;
        }
        auto iter = f->obj.find(vi.name);
        if (iter == f->obj.end()) {
          return 0; /* nothing to erase */
        }
        f = &iter->second;
      } else if (f->type == FMT_ARRAY) {
        if (vi.is_obj) {
          return -EINVAL;
        }
        int index = vi.index;
        if (index < 0) {
          index = f->arr.size() + index;
          if (index < 0) { /* out of bounds, nothing to remove */
            return 0;
          }
        }
        if ((size_t)index >= f->arr.size()) {
          return 0; /* index beyond array boundaries */
        }
        f = &f->arr[index];
      }
      last_entity = vi;
    }
  }

  if (!parent) {
    *this = JSONFormattable(); /* erase everything */
  } else {
    if (last_entity.is_obj) {
      parent->obj.erase(last_entity.name);
    } else {
      int index = (last_entity.index >= 0 ? last_entity.index : parent->arr.size() + last_entity.index);
      if (index < 0 || (size_t)index >= parent->arr.size()) {
        return 0;
      }
      parent->arr.erase(parent->arr.begin() + index);
    }
  }

  return 0;
}

void JSONFormattable::derive_from(const JSONFormattable& parent)
{
  for (auto& o : parent.obj) {
    if (obj.find(o.first) == obj.end()) {
      obj[o.first] = o.second;
    }
  }
}

void encode_json(const char *name, const JSONFormattable& v, Formatter *f)
{
  v.encode_json(name, f);
}

void JSONFormattable::encode_json(const char *name, Formatter *f) const
{
  switch (type) {
    case JSONFormattable::FMT_VALUE:
      ::encode_json(name, value, f);
      break;
    case JSONFormattable::FMT_ARRAY:
      ::encode_json(name, arr, f);
      break;
    case JSONFormattable::FMT_OBJ:
      f->open_object_section(name);
      for (auto iter : obj) {
        ::encode_json(iter.first.c_str(), iter.second, f);
      }
      f->close_section();
      break;
    case JSONFormattable::FMT_NONE:
      break;
  }
}

bool JSONFormattable::handle_value(const char *name, std::string_view s, bool quoted) {
  JSONFormattable *new_val;
  if (cur_enc->is_array()) {
    cur_enc->arr.push_back(JSONFormattable());
    new_val = &cur_enc->arr.back();
  } else {
    cur_enc->set_type(JSONFormattable::FMT_OBJ);
    new_val  = &cur_enc->obj[name];
  }
  new_val->set_type(JSONFormattable::FMT_VALUE);
  new_val->value.set(s, quoted);

  return false;
}
bool JSONFormattable::handle_open_section(const char *name, const char *ns, bool section_is_array) {
  if (cur_enc->is_array()) {
    cur_enc->arr.push_back(JSONFormattable());
    cur_enc = &cur_enc->arr.back();
  } else if (enc_stack.size() > 1) {
      /* only open a new section if already nested,
       * otherwise root is the container
       */
      cur_enc = &cur_enc->obj[name];
  }
  enc_stack.push_back(cur_enc);

  if (section_is_array) {
    cur_enc->set_type(JSONFormattable::FMT_ARRAY);
  } else {
    cur_enc->set_type(JSONFormattable::FMT_OBJ);
  }

  return false; /* continue processing */
}

bool JSONFormattable::handle_close_section() {
  if (enc_stack.size() <= 1) {
    return false;
  }

  enc_stack.pop_back();
  cur_enc = enc_stack.back();
  return false; /* continue processing */
}

