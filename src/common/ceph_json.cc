#include "common/ceph_json.h"
#include "include/utime.h"

#include <boost/algorithm/string.hpp>
#include <boost/tokenizer.hpp>
#include <boost/lexical_cast.hpp>
#include <include/types.h>

/* Enable boost.json's header-only mode:
	(see: "https://github.com/boostorg/json?tab=readme-ov-file#header-only"): */
#include <boost/json/src.hpp>

#include <memory>
#include <string>
#include <fstream>

using std::ifstream;
using std::ostream;

using std::begin;
using std::end;

using std::pair;
using std::vector;

using std::string;
using std::string_view;

using ceph::bufferlist;
using ceph::Formatter;

#define dout_subsys ceph_subsys_rgw

static JSONFormattable default_formattable;

// does not work, FIXME
ostream& operator<<(ostream &out, const JSONObj &obj) {
   out << obj.name << ": " << obj.val;
   return out;
}

bool JSONObj::get_attr(const string name, data_val& attr)
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
    return nullptr;

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
	for (const auto& kvp : *op) {
		auto child = std::make_unique<JSONObj>(this, kvp.key(), kvp.value());
		children.insert(std::pair { kvp.key(), std::move(child) });
	 }

	return;
  }

 if (auto ap = v.if_array()) {
	for (const auto& kvp : *ap) {
		auto child = std::make_unique<JSONObj>(this, "", kvp);
		children.insert(std::pair { child->get_name(), std::move(child) });
	 }
 }

 // unknown type is not-an-error
}

vector<string> JSONObj::get_array_elements()
{
 vector<string> elements;

 if(!data.is_array())
  return elements;

 std::ranges::for_each(data.as_array(), [&elements](const auto& i) {
 	elements.emplace_back(boost::json::serialize(i));
 });

  return elements;
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

// parse a supplied JSON fragment:
bool JSONParser::parse(std::string_view json_string_view) 
{
  if(json_string_view.empty())
   return false;

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

  // Was the entire string read?
  if (s.size() == static_cast<uint64_t>(json_string_view.length())) { 
    val.set(s, false);
    return true;
   }

  // Could not parse and convert:
  return false; 
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
	  boost::iequals(value.str, "1") ||
          boost::iequals(value.str, "on") ||
          boost::iequals(value.str, "yes"));
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

  field_entity() = default;
  explicit field_entity(std::string_view sv) : is_obj(true), name(sv) {}
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
      result->emplace_back(field_entity(s));
      return 0;
    }
    if (next_arr > ofs) {

      auto field = string_view(ofs + begin(s), next_arr - ofs + begin(s)); 

      result->emplace_back(field_entity(field));
      ofs = next_arr;
    }
    size_t end_arr = s.find(']', next_arr + 1);
    if (end_arr == string::npos) {
      return -EINVAL;
    }

    auto index_str = string_view(s).substr(1 + next_arr, end_arr - next_arr - 1);

    ofs = end_arr + 1;

    if (!index_str.empty()) {

	int x;
	if(auto [_, ec] = std::from_chars(begin(index_str), end(index_str), x); std::errc() == ec) 
         result->emplace_back(field_entity(x));
        else
	 throw std::invalid_argument(fmt::format("{}", index_str));

    } else {
      field_entity f;
      f.append = true;
      result->emplace_back(f);
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

bool JSONFormattable::handle_value(std::string_view name, std::string_view s, bool quoted) {
  JSONFormattable *new_val;
  if (cur_enc->is_array()) {
    cur_enc->arr.emplace_back(JSONFormattable());
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
    cur_enc->arr.emplace_back(JSONFormattable());
    cur_enc = &cur_enc->arr.back();
  } else if (enc_stack.size() > 1) {
      /* only open a new section if already nested,
       * otherwise root is the container
       */
    cur_enc = &cur_enc->obj[string{name}];
  }
  enc_stack.emplace_back(cur_enc);

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

