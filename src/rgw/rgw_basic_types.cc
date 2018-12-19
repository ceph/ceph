// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <iostream>
#include <sstream>
#include <string>

#include "rgw_basic_types.h"
#include "common/ceph_json.h"

using std::string;
using std::stringstream;

void decode_json_obj(rgw_user& val, JSONObj *obj)
{
  string s = obj->get_data();
  val.from_str(s);
}

void encode_json(const char *name, const rgw_user& val, Formatter *f)
{
  string s = val.to_str();
  f->dump_string(name, s);
}

namespace rgw {
namespace auth {
ostream& operator <<(ostream& m, const Principal& p) {
  if (p.is_wildcard()) {
    return m << "*";
  }

  m << "arn:aws:iam:" << p.get_tenant() << ":";
  if (p.is_tenant()) {
    return m << "root";
  }
  return m << (p.is_user() ? "user/" : "role/") << p.get_id();
}
}
}
