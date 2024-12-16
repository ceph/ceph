// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2025 International Business Machines Corp.
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
#include <boost/container/flat_map.hpp>

/* Enable boost.json's header-only mode:
        (see: "https://github.com/boostorg/json?tab=readme-ov-file#header-only"): */
#include <boost/json/src.hpp>

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

/* accepts a JSON Array or JSON Object contained in
 * a Boost.JSON value, v, and creates a Ceph JSONObj for each
 * child contained in v
 */
void JSONObj::handle_value(boost::json::value v)
{
  if (auto op = v.if_object()) {
      for (const auto& kvp : *op) {
        auto child = std::make_unique<JSONObj>(this, kvp.key(), kvp.value());
        children.emplace(std::pair { kvp.key(), std::move(child) });
      }

      return;
  }

 if (auto ap = v.if_array()) {
      for (const auto& kvp : *ap) {
        auto child = std::make_unique<JSONObj>(this, "", kvp);
        children.emplace(std::pair { child->get_name(), std::move(child) });
      }
 }

 // unknown type is not-an-error
}

vector<string> JSONObj::get_array_elements()
{
 vector<string> elements;

 if (!data.is_array())
  return elements;

 std::ranges::for_each(data.as_array(), [&elements](const auto& i) {
   elements.emplace_back(boost::json::serialize(i));
 });

  return elements;
}

// parse the complete internal json_buffer
bool JSONParser::parse()
{
  if (!parse_json(json_buffer, data))
   return false;

  handle_value(data);

  return true;
}

// parse the internal json_buffer up to len
bool JSONParser::parse(int len)
{
  if (!parse_json(std::string_view { std::begin(json_buffer), len + std::begin(json_buffer) }, data))
   return false;

  handle_value(data);

  return true;
}

// parse a supplied JSON fragment:
bool JSONParser::parse(std::string_view json_string_view) 
{
  // The original implementation checked this, I'm not sure we have to but we're doing it for now:
  if (json_string_view.empty())
   return false;

  if(json_string_view.ends_with('\0')) 
   json_string_view.remove_suffix(1);

  if (!parse_json(json_string_view, data))
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

bool JSONParser::parse_file(const std::filesystem::path file_name)
{
 ifstream is(file_name);

 if (!is.is_open()) {
  throw std::runtime_error(fmt::format("unable to open \"{}\"", file_name.string()));
 }

 std::error_code ec;
 data = boost::json::parse(is, ec);

 if (ec)
  return false;

 handle_value(data);

 return true;
}

/* JSONFormattable */

bool JSONFormattable::val_bool() const {
  return (boost::iequals(value.str, "true") ||
          boost::iequals(value.str, "1") ||
          boost::iequals(value.str, "on") ||
          boost::iequals(value.str, "yes"));
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

