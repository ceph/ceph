// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2025-2026 International Business Machines Corp.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
*/

#include "common/ceph_json.h"
#include "include/utime.h"

#include <include/types.h>

#include <boost/tokenizer.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/algorithm/string.hpp>

#include "glaze/glaze.hpp"

#include <memory>
#include <string>
#include <fstream>
#include <exception>

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

// does not work, FIXME
ostream& operator<<(ostream &out, const JSONObj &obj) {
   out << obj.name << ": " << obj.val;
   return out;
}

JSONObj::JSONObj(JSONObj *parent_node, std::string_view name_in,
                 const ceph_json::value& data_in)
  : parent { parent_node },
    name { name_in },
    data(data_in)
{
  handle_value(data);
}

void JSONObj::cache_data_value(std::string_view s, bool quoted) const
{
  val.set(s, quoted);
  data_value_ready = true;
}

void JSONObj::ensure_data_value() const
{
  if (data_value_ready)
    return;

  if (data.is_string()) {
    cache_data_value(data.as<std::string>(), true);
    return;
  }

  std::string json_data;
  if (auto ec = glz::write_json(data, json_data); ec) {
    throw JSONDecoderErr(fmt::format("failed to parse JSON input: {}",
                                     glz::format_error(ec, name)));
  }

  cache_data_value(json_data, false);
}

/* accepts a JSON Array or JSON Object contained in
 * a Glaze value, v, and creates a Ceph JSONObj for each
 * child contained in v
 */
void JSONObj::handle_value(const ceph_json::value& v)
{
  children.clear();

  if (auto *o = v.get_if<ceph_json::value::object_t>()) {
    children.reserve(o->size());

    for (const auto& [k, v] : *o) {
      children.emplace_back(std::make_unique<JSONObj>(this, k, v));
    }

    std::ranges::sort(children, std::less<>{}, child_name);

    return;
  }

  if (auto *ap = v.get_if<ceph_json::value::array_t>()) {
    children.reserve(ap->size());

    for (const auto& kv : *ap) {
      children.emplace_back(std::make_unique<JSONObj>(this, "", kv));
    }
  }

  // unknown type is not-an-error (as per the original implementation)
}

vector<string> JSONObj::get_array_elements()
{
  if (!data.is_array())
    return {};

  vector<string> decoded_elements;
  decoded_elements.reserve(children.size());

  std::ranges::transform(children, std::back_inserter(decoded_elements), [](const auto& child_ptr) {
    const auto& child = *child_ptr;
    if (!child.native_handle().is_string())
      return child.get_data();

    auto json_str = glz::write_json(child.native_handle());

    if (!json_str) {
      throw JSONDecoder::err(glz::format_error(json_str));
    }

    return std::move(*json_str);
  });

  return decoded_elements;
}

// parse the complete internal json_buffer
bool JSONParser::parse()
{
  if (!parse_json(json_buffer, data))
   return false;

  handle_value(data);
  data_value_ready = false;

  return true;
}

// parse the internal json_buffer up to len
bool JSONParser::parse(int len)
{
  const auto json = std::string_view { json_buffer }.substr(0, len);

  if (!parse_json(json, data))
   return false;

  handle_value(data);
  data_value_ready = false;

  return true;
}

// parse a supplied JSON fragment:
bool JSONParser::parse(std::string_view json_string_view) 
{
  // The original implementation checked this, I'm not sure we have to but we're doing it for now:
  if (json_string_view.empty())
   return false;

  if (!parse_json(json_string_view, data))
   return false; 

  // recursively evaluate the result:
  handle_value(data);
  data_value_ready = false;

  if (data.is_object() or data.is_array()) 
   return true;

  if (data.is_string()) {
    cache_data_value(data.get<std::string>(), true);
    return true;
  } 

  // For any other kind of value:
  std::string s;
  if(auto ec = glz::write_json(data, s); ec) {
      throw JSONDecoder::err(glz::format_error(ec));
  }

  // Was the entire string read?
  if (s.size() == static_cast<uint64_t>(json_string_view.length())) { 
    cache_data_value(s, false);
    return true;
  }

  // Could not parse and convert:
  return false; 
}

bool JSONParser::parse_file(const std::filesystem::path file_name)
{
 if (auto ec = glz::read_file_json(data, file_name.string(), std::string()); ec) {
  return false;
 }

 handle_value(data);
 data_value_ready = false;

 return true;
}

/* JSONFormattable */

bool JSONFormattable::val_bool() const {
  return (boost::iequals(value.str, "true") ||
          boost::iequals(value.str, "1") ||
          boost::iequals(value.str, "on") ||
          boost::iequals(value.str, "yes"));
}

std::list<JSONFormattable> JSONFormattable::generate_test_instances() {
    std::list<JSONFormattable> o;
    o.emplace_back();
    o.emplace_back();
    o.back().set_type(FMT_VALUE);
    o.back().value.str = "foo";
    o.back().value.quoted = true;
    o.emplace_back();
    o.back().set_type(FMT_VALUE);
    o.back().value.str = "foo";
    o.back().value.quoted = false;
    o.emplace_back();
    o.back().set_type(FMT_ARRAY);
    o.back().arr.push_back(JSONFormattable());
    o.back().arr.back().set_type(FMT_VALUE);
    o.back().arr.back().value.str = "foo";
    o.back().arr.back().value.quoted = true;
    o.back().arr.push_back(JSONFormattable());
    o.back().arr.back().set_type(FMT_VALUE);
    o.back().arr.back().value.str = "bar";
    o.back().arr.back().value.quoted = true;
    o.emplace_back();
    o.back().set_type(FMT_OBJ);
    o.back().obj["foo"] = JSONFormattable();
    o.back().obj["foo"].set_type(FMT_VALUE);
    o.back().obj["foo"].value.str = "bar";
    o.back().obj["foo"].value.quoted = true;
    return o;
  }

void JSONFormattable::generate_test_instances(std::list<JSONFormattable*>& o) {
    o.push_back(new JSONFormattable);
    o.push_back(new JSONFormattable);
    o.back()->set_type(FMT_VALUE);
    o.back()->value.str = "foo";
    o.back()->value.quoted = true;
    o.push_back(new JSONFormattable);
    o.back()->set_type(FMT_VALUE);
    o.back()->value.str = "foo";
    o.back()->value.quoted = false;
    o.push_back(new JSONFormattable);
    o.back()->set_type(FMT_ARRAY);
    o.back()->arr.push_back(JSONFormattable());
    o.back()->arr.back().set_type(FMT_VALUE);
    o.back()->arr.back().value.str = "foo";
    o.back()->arr.back().value.quoted = true;
    o.back()->arr.push_back(JSONFormattable());
    o.back()->arr.back().set_type(FMT_VALUE);
    o.back()->arr.back().value.str = "bar";
    o.back()->arr.back().value.quoted = true;
    o.push_back(new JSONFormattable);
    o.back()->set_type(FMT_OBJ);
    o.back()->obj["foo"] = JSONFormattable();
    o.back()->obj["foo"].set_type(FMT_VALUE);
    o.back()->obj["foo"].value.str = "bar";
    o.back()->obj["foo"].value.quoted = true;
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
        if (auto [_, ec] = std::from_chars(begin(index_str), end(index_str), x); std::errc() == ec) 
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

  bool is_valid_json = jp.parse(val);

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

void JSONFormattable::encode(ceph::buffer::list& bl) const {
  ENCODE_START(2, 1, bl);
  encode((uint8_t)type, bl);
  encode(value.str, bl);
  encode(arr, bl);
  encode(obj, bl);
  encode(value.quoted, bl);
  ENCODE_FINISH(bl);
}

void JSONFormattable::decode(ceph::buffer::list::const_iterator& bl) {
  DECODE_START(2, bl);
  uint8_t t;
  decode(t, bl);
  type = (Type)t;
  decode(value.str, bl);
  decode(arr, bl);
  decode(obj, bl);
  if (struct_v >= 2) {
    decode(value.quoted, bl);
  } else {
    value.quoted = true;
  }
  DECODE_FINISH(bl);
}
