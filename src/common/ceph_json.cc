#include "common/ceph_json.h"
#include "include/utime.h"

#include <boost/algorithm/string.hpp>
#include <boost/tokenizer.hpp>
#include <boost/lexical_cast.hpp>
#include <include/types.h>

// Enable boost.json's header-only mode ("https://github.com/boostorg/json?tab=readme-ov-file#header-only"):
#include <boost/json/src.hpp>

#include <memory>
#include <fstream>

using std::ifstream;
using std::pair;
using std::ostream;
using std::string;
using std::vector;

using ceph::bufferlist;
using ceph::Formatter;

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

// IMPORTANT: The returned pointer is intended as NON-OWNING (i.e. the JSONObjIter is responsible for it):
JSONObj *JSONObjIter::operator*()
{
  return cur->second.get();
}

// does not work, FIXME
ostream& operator<<(ostream &out, const JSONObj &obj) {
   out << obj.name << ": " << obj.val;
   return out;
}

void JSONObj::add_child(string el, JSONObj *obj)
{
  auto p = std::unique_ptr<JSONObj>(obj);

  children.insert({el, std::move(p)});
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
  auto first = children.find(name);
  if (first != children.end()) {
    auto last = children.upper_bound(name);
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
  auto first = children.find(name);
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
 * a Boost.JSON value, v, and creates a Ceph JSONObj for each
 * child contained in v
 */
void JSONObj::handle_value(boost::json::value v)
{
  if (auto op = v.if_object()) {

	for (const auto& kvp : *op)
	 {
		JSONObj *child = new JSONObj;

		child->init(this, kvp.value(), kvp.key());

		add_child(kvp.key(), child);
	 }

	return;
  }

 if (auto ap = v.if_array()) {
	for (const auto& kvp : *ap)
	 {
		JSONObj *child = new JSONObj;
		child->init(this, kvp, ""); 

		add_child(child->get_name(), child);
	 }
 }

 // unknown type is not-an-error
}

void JSONObj::init(JSONObj *parent_node, boost::json::value data_in, std::string name_in)
{
  parent = parent_node;
  data = data_in;
  name = name_in;

  handle_value(data_in);

  if (auto vp = data_in.if_string())
   val.set(*vp, true); 
  else {
   val.set(boost::json::serialize(data_in), false);
  }

  attr_map.insert({name, val});
}

JSONObj *JSONObj::get_parent()
{
  return parent;
}

bool JSONObj::is_object()
{
  return data.is_object();
}

bool JSONObj::is_array()
{
  return data.is_array();
}

vector<string> JSONObj::get_array_elements()
{
 vector<string> elements;

 boost::json::array temp_array;

 if (auto ap = data.if_array())
  temp_array = *ap;

 auto array_size = temp_array.size();

 if (array_size > 0) {
	for (boost::json::array::size_type i = 0; i < array_size; i++) {
	      boost::json::value temp_value = temp_array[i];
	      string temp_string;
	      temp_string = boost::json::serialize(temp_value);
	      elements.push_back(temp_string);
	}
  }

  return elements;
}

void JSONParser::handle_data(const char *s, int len)
{
  json_buffer.append(s, len); // check for problems with null termination FIXME
  buf_len += len;
}

// parse a supplied JSON fragment
bool JSONParser::parse(const char *buf_, int len)
{
  if (!buf_ || 0 >= len) {
    return false;
  }

  std::string_view json_string_view(buf_, len);

  std::error_code ec; 
  data = boost::json::parse(json_string_view, ec);
  
  if(ec)
   return false;

  // recursively evaluate the result:
  handle_value(data);

  if (data.is_object() or data.is_array()) 
   return true;

  if (data.is_string()) {
   val.set(data.as_string(), true);
   return true;
  } 

  // For any other kind of value:
  std::string s = boost::json::serialize(data);

  if (s.size() == static_cast<uint64_t>(len)) { // entire string was read
    val.set(s, false);
    return true;
   }

  // Could not parse and convert:
  return false; 
}

// parse the internal json_buffer up to len
bool JSONParser::parse(int len)
{
  std::string_view json_string(std::begin(json_buffer), len + std::begin(json_buffer));

  std::error_code ec;
  if(data = boost::json::parse(json_string, ec); ec)
   return false;

  handle_value(data);

  return true;
}

// parse the complete internal json_buffer
bool JSONParser::parse()
{
  std::error_code ec;
 
  data = boost::json::parse(json_buffer, ec);

  if(ec)
   return false;

  handle_value(data);

  return true;
}

// parse a supplied ifstream:
bool JSONParser::parse(const char *file_name)
{
 ifstream is(file_name);

 std::error_code ec;
 data = boost::json::parse(is, ec);

 if(ec)
  return false;

 handle_value(data);

 return true;
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
  } catch (ceph::buffer::error& err) {
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

void decode_json_obj(ceph::real_time& val, JSONObj *obj)
{
  const std::string& s = obj->get_data();
  uint64_t epoch;
  uint64_t nsec;
  int r = utime_t::parse_date(s, &epoch, &nsec);
  if (r == 0) {
    using namespace std::chrono;
    val = real_time{seconds(epoch) + nanoseconds(nsec)};
  } else {
    throw JSONDecoder::err("failed to decode real_time");
  }
}

void decode_json_obj(ceph::coarse_real_time& val, JSONObj *obj)
{
  const std::string& s = obj->get_data();
  uint64_t epoch;
  uint64_t nsec;
  int r = utime_t::parse_date(s, &epoch, &nsec);
  if (r == 0) {
    using namespace std::chrono;
    val = coarse_real_time{seconds(epoch) + nanoseconds(nsec)};
  } else {
    throw JSONDecoder::err("failed to decode coarse_real_time");
  }
}

void decode_json_obj(ceph_dir_layout& i, JSONObj *obj){

    unsigned tmp;
    JSONDecoder::decode_json("dir_hash", tmp, obj, true);
    i.dl_dir_hash = tmp;
    JSONDecoder::decode_json("unused1", tmp, obj, true);
    i.dl_unused1 = tmp;
    JSONDecoder::decode_json("unused2", tmp, obj, true);
    i.dl_unused2 = tmp;
    JSONDecoder::decode_json("unused3", tmp, obj, true);
    i.dl_unused3 = tmp;
}

void encode_json(const char *name, std::string_view val, Formatter *f)
{
  f->dump_string(name, val);
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
  f->dump_bool(name, val);
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

void encode_json(const char *name, const ceph::real_time& val, Formatter *f)
{
  encode_json(name, utime_t{val}, f);
}

void encode_json(const char *name, const ceph::coarse_real_time& val, Formatter *f)
{
  encode_json(name, utime_t{val}, f);
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

bool JSONFormattable::handle_value(std::string_view name, std::string_view s, bool quoted) {
  JSONFormattable *new_val;
  if (cur_enc->is_array()) {
    cur_enc->arr.push_back(JSONFormattable());
    new_val = &cur_enc->arr.back();
  } else {
    cur_enc->set_type(JSONFormattable::FMT_OBJ);
    new_val  = &cur_enc->obj[string{name}];
  }
  new_val->set_type(JSONFormattable::FMT_VALUE);
  new_val->value.set(s, quoted);

  return false;
}

bool JSONFormattable::handle_open_section(std::string_view name,
                                          const char *ns,
                                          bool section_is_array) {
  if (cur_enc->is_array()) {
    cur_enc->arr.push_back(JSONFormattable());
    cur_enc = &cur_enc->arr.back();
  } else if (enc_stack.size() > 1) {
      /* only open a new section if already nested,
       * otherwise root is the container
       */
    cur_enc = &cur_enc->obj[string{name}];
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

