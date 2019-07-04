// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab

#include "rgw_url.h"
#include "common/strtol.h"
#include <stdio.h>
#include <strings.h>

class HexTable {
  char table[256]={-1};

public:
  HexTable() {
    int i;
    for (i = '0'; i <= '9'; ++i)
      table[i] = i - '0';
    for (i = 'A'; i <= 'F'; ++i)
      table[i] = i - 'A' + 0xa;
    for (i = 'a'; i <= 'f'; ++i)
      table[i] = i - 'a' + 0xa;
  }

  char to_num(char c) const {
    return table[(int)c];
  }
};

static char hex_to_num(char c) {
  static const HexTable hex_table;
  return hex_table.to_num(c);
}

std::string url_decode(const boost::string_view& src_str, bool in_query) {
  std::string dest_str;
  dest_str.reserve(src_str.length() + 1);

  for (auto src = std::begin(src_str); src != std::end(src_str); ++src) {
    if (*src != '%') {
      if (!in_query || *src != '+') {
        if (*src == '?') {
          in_query = true;
        }
        dest_str.push_back(*src);
      } else {
        dest_str.push_back(' ');
      }
    } else {
      /* 3 == strlen("%%XX") */
      if (std::distance(src, std::end(src_str)) < 3) {
        break;
      }

      src++;
      const char c1 = hex_to_num(*src++);
      const char c2 = hex_to_num(*src);
      if (c1 < 0 || c2 < 0) {
        return std::string();
      } else {
        dest_str.push_back(c1 << 4 | c2);
      }
    }
  }

  return dest_str;
}

void rgw_uri_escape_char(char c, std::string& dst) {
  char buf[16];
  snprintf(buf, sizeof(buf), "%%%.2X", (int)(unsigned char)c);
  dst.append(buf);
}

static bool char_needs_url_encoding(char c) {
  if (c <= 0x20 || c >= 0x7f)
    return true;

  switch (c) {
    case 0x22:
    case 0x23:
    case 0x25:
    case 0x26:
    case 0x2B:
    case 0x2C:
    case 0x2F:
    case 0x3A:
    case 0x3B:
    case 0x3C:
    case 0x3E:
    case 0x3D:
    case 0x3F:
    case 0x40:
    case 0x5B:
    case 0x5D:
    case 0x5C:
    case 0x5E:
    case 0x60:
    case 0x7B:
    case 0x7D:
      return true;
  }
  return false;
}

void url_encode(const std::string& src, std::string& dst, bool encode_slash)
{
  const char *p = src.data();
  for (unsigned i = 0; i < src.size(); i++, p++) {
    if ((!encode_slash && *p == 0x2F) || !char_needs_url_encoding(*p)) {
      dst.append(p, 1);
    }else {
      rgw_uri_escape_char(*p, dst);
    }
  }
}

std::string url_encode(const std::string& src, bool encode_slash)
{
  std::string dst;
  url_encode(src, dst, encode_slash);

  return dst;
}

// class that parses "name=value" string into two different strings
// in the case that there is only "name" value is an empty string (equivalent to "name=")
class NameVal {
private:
   std::string name;
   std::string val;
 public:
    explicit NameVal(const std::string& nv);
    const std::string& get_name() const { return name; }
    const std::string& get_val() const { return val; }
};

NameVal::NameVal(const std::string& nv) {
  const auto delim_pos = nv.find('=');
  if (delim_pos == std::string::npos) {
    name = nv;
  } else {
    name = nv.substr(0, delim_pos);
    val = nv.substr(delim_pos + 1);
  }
}

const std::string HTTPArgs::empty_str;

HTTPArgs::HTTPArgs(const boost::string_view& str, bool expect_question_mark) {
  int pos = 0;
  bool end = false;

  if (str.empty()) {
    return;
  }

  if (expect_question_mark && str[pos] == '?') {
    ++pos;
  }

  while (!end) {
    int fpos = str.find('&', pos);
    if (fpos  < pos) {
       end = true;
       fpos = str.size(); 
    }
    const NameVal nv(url_decode(str.substr(pos, fpos - pos), true));
    val_map[std::string(nv.get_name())] = std::string(nv.get_val());
    pos = fpos + 1;  
  }
}

const std::string& HTTPArgs::get(const std::string& name, bool* exists) const {
  const auto iter = val_map.find(name);
  const bool e = (iter != std::end(val_map));
  if (exists)
    *exists = e;
  if (e)
    return iter->second;
  return empty_str;
}

boost::optional<const std::string&>
HTTPArgs::get_optional(const std::string& name) const
{
  bool exists;
  const std::string& value = get(name, &exists);
  if (exists) {
    return value;
  } else {
    return boost::none;
  }
}

const std::string& HTTPArgs::get_str() {
    if (cached_str) {
        return cached_str.get();
    }
    cached_str.emplace("");
    for (const auto arg : val_map) {
        const std::string nameval(arg.first+"="+arg.second);
        url_encode(nameval, cached_str.get(), false);
    }
    return cached_str.get();
}

int RGWHTTPArgs::parse()
{
  int pos = 0;
  bool end = false;

  if (str.empty())
    return 0;

  if (str[pos] == '?')
    pos++;

  while (!end) {
    int fpos = str.find('&', pos);
    if (fpos  < pos) {
       end = true;
       fpos = str.size(); 
    }
    const NameVal nv(url_decode(str.substr(pos, fpos - pos), true));
    append(std::string(nv.get_name()), std::string(nv.get_val()));
    pos = fpos + 1;  
  }

  return 0;
}


const std::string RGWHTTPArgs::empty_str;

void RGWHTTPArgs::append(const std::string& name, const std::string& val)
{
  if (name.compare(0, sizeof(RGW_SYS_PARAM_PREFIX) - 1, RGW_SYS_PARAM_PREFIX) == 0) {
    sys_val_map[name] = val;
  } else {
    val_map[name] = val;
  }

  if ((name.compare("acl") == 0) ||
      (name.compare("cors") == 0) ||
      (name.compare("notification") == 0) ||
      (name.compare("location") == 0) ||
      (name.compare("logging") == 0) ||
      (name.compare("usage") == 0) ||
      (name.compare("lifecycle") == 0) ||
      (name.compare("delete") == 0) ||
      (name.compare("uploads") == 0) ||
      (name.compare("partNumber") == 0) ||
      (name.compare("uploadId") == 0) ||
      (name.compare("versionId") == 0) ||
      (name.compare("start-date") == 0) ||
      (name.compare("end-date") == 0) ||
      (name.compare("versions") == 0) ||
      (name.compare("versioning") == 0) ||
      (name.compare("website") == 0) ||
      (name.compare("requestPayment") == 0) ||
      (name.compare("torrent") == 0) ||
      (name.compare("tagging") == 0) ||
      (name.compare("append") == 0) ||
      (name.compare("position") == 0)) {
    sub_resources[name] = val;
  } else if (name[0] == 'r') { // root of all evil
    if ((name.compare("response-content-type") == 0) ||
        (name.compare("response-content-language") == 0) ||
        (name.compare("response-expires") == 0) ||
        (name.compare("response-cache-control") == 0) ||
        (name.compare("response-content-disposition") == 0) ||
        (name.compare("response-content-encoding") == 0)) {
      sub_resources[name] = val;
      has_resp_modifier = true;
    }
  } else if  ((name.compare("subuser") == 0) ||
              (name.compare("key") == 0) ||
              (name.compare("caps") == 0) ||
              (name.compare("index") == 0) ||
              (name.compare("policy") == 0) ||
              (name.compare("quota") == 0) ||
              (name.compare("list") == 0) ||
              (name.compare("object") == 0)) {

    if (!admin_subresource_added) {
      sub_resources[name] = "";
      admin_subresource_added = true;
    }
  }
}

const std::string& RGWHTTPArgs::get(const std::string& name, bool *exists) const
{
  const auto iter = val_map.find(name);
  const bool e = (iter != std::end(val_map));
  if (exists)
    *exists = e;
  if (e)
    return iter->second;
  return empty_str;
}

boost::optional<const std::string&>
RGWHTTPArgs::get_optional(const std::string& name) const
{
  bool exists;
  const std::string& value = get(name, &exists);
  if (exists) {
    return value;
  } else {
    return boost::none;
  }
}

int RGWHTTPArgs::get_bool(const std::string& name, bool *val, bool *exists) const
{
  const auto iter = val_map.find(name);
  bool e = (iter != val_map.end());
  if (exists)
    *exists = e;

  if (e) {
    const char *s = iter->second.c_str();

    if (strcasecmp(s, "false") == 0) {
      *val = false;
    } else if (strcasecmp(s, "true") == 0) {
      *val = true;
    } else {
      return -EINVAL;
    }
  }

  return 0;
}

int RGWHTTPArgs::get_bool(const char *name, bool *val, bool *exists) const
{
  return get_bool(std::string(name), val, exists);
}

void RGWHTTPArgs::get_bool(const char *name, bool *val, bool def_val) const
{
  bool exists = false;
  if ((get_bool(name, val, &exists) < 0) ||
      !exists) {
    *val = def_val;
  }
}

int RGWHTTPArgs::get_int(const char *name, int *val, int def_val) const
{
  bool exists = false;
  std::string val_str;
  val_str = get(name, &exists);
  if (!exists) {
    *val = def_val;
    return 0;
  }

  std::string err;

  *val = (int)strict_strtol(val_str.c_str(), 10, &err);
  if (!err.empty()) {
    *val = def_val;
    return -EINVAL;
  }
  return 0;
}

std::string RGWHTTPArgs::sys_get(const std::string& name, bool * const exists) const
{
  const auto iter = sys_val_map.find(name);
  const bool e = (iter != sys_val_map.end());

  if (exists) {
    *exists = e;
  }

  return e ? iter->second : std::string();
}

