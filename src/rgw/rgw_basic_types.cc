// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include <iostream>
#include <sstream>
#include <string>

#include "rgw_basic_types.h"
#include "rgw_xml.h"
#include "common/ceph_json.h"

using std::string;
using std::stringstream;

void decode_json_obj(rgw_user& val, JSONObj *obj)
{
  val.from_str(obj->get_data());
}

void encode_json(const char *name, const rgw_user& val, Formatter *f)
{
  f->dump_string(name, val.to_str());
}

void encode_xml(const char *name, const rgw_user& val, Formatter *f)
{
  encode_xml(name, val.to_str(), f);
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
