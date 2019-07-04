// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
#pragma once

#include <string>
// boost notes:
// std::optional does not support reference
// TODO: replace boost::string_view with std::string_view
#include <boost/utility/string_view.hpp>
#include <boost/optional.hpp>
#include <map>

#define RGW_SYS_PARAM_PREFIX "rgwx-"

/** Convert an input URL into a sane object name
 * by converting %-escaped strings into characters, etc*/
void rgw_uri_escape_char(char c, std::string& dst);

std::string url_decode(const boost::string_view& src_str,
                              bool in_query = false);

void url_encode(const std::string& src, std::string& dst,
                       bool encode_slash = true);

std::string url_encode(const std::string& src, bool encode_slash = true);

// parses the HTTP arguments associated with the HTTP request
class HTTPArgs {
private:
  boost::optional<std::string> cached_str;
  std::map<std::string, std::string> val_map;
  static const std::string empty_str;
public:
  HTTPArgs(const boost::string_view& str, bool expect_question_mark);
  const std::string& get(const std::string& name, bool* exists = nullptr) const;
  boost::optional<const std::string&> get_optional(const std::string& name) const;
  bool exists(const char *name) const {
    return (val_map.find(name) != std::end(val_map));
  }
  const std::string& get_str();
};

// parses the HTTP arguments associated with the HTTP request
// with handling for RGW specific args
class RGWHTTPArgs {
  std::string str;
  static const std::string empty_str;
  std::map<std::string, std::string> val_map;
  std::map<std::string, std::string> sys_val_map;
  std::map<std::string, std::string> sub_resources;
  bool has_resp_modifier;
  bool admin_subresource_added;

 public:
  RGWHTTPArgs() : has_resp_modifier(false), admin_subresource_added(false) {}

  /** Set the arguments; as received */
  void set(const std::string& s) {
    has_resp_modifier = false;
    val_map.clear();
    sub_resources.clear();
    str = s;
  }
  /** parse the received arguments */
  int parse();
  void append(const std::string& name, const std::string& val);
  /** Get the value for a specific argument parameter */
  const std::string& get(const std::string& name, bool *exists = nullptr) const;
  boost::optional<const std::string&> get_optional(const std::string& name) const;
  int get_bool(const std::string& name, bool *val, bool *exists) const;
  int get_bool(const char *name, bool *val, bool *exists) const;
  void get_bool(const char *name, bool *val, bool def_val) const;
  int get_int(const char *name, int *val, int def_val) const;

  /** Get the value for specific system argument parameter */
  std::string sys_get(const std::string& name, bool *exists = nullptr) const;

  /** see if a parameter is contained in this RGWHTTPArgs */
  bool exists(const char *name) const {
    return (val_map.find(name) != std::end(val_map));
  }
  bool sub_resource_exists(const char *name) const {
    return (sub_resources.find(name) != std::end(sub_resources));
  }
  std::map<std::string, std::string>& get_params() {
    return val_map;
  }
  const std::map<std::string, std::string>& get_sub_resources() const {
    return sub_resources;
  }
  unsigned get_num_params() const {
    return val_map.size();
  }
  bool has_response_modifier() const {
    return has_resp_modifier;
  }
  void set_system() { /* make all system params visible */
    for (auto iter = sys_val_map.begin(); iter != sys_val_map.end(); ++iter) {
      val_map[iter->first] = iter->second;
    }
  }
  const std::string& get_str() const {
    return str;
  }
};

