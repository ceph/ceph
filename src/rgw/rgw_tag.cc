// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include <map>
#include <string>

#include <common/errno.h>
#include <boost/algorithm/string.hpp>

#include "rgw_tag.h"
#include "rgw_common.h"

using namespace std;

void RGWObjTags::add_tag(const string& key, const string& val){
  tag_map.emplace(std::make_pair(key,val));
}

void RGWObjTags::emplace_tag(std::string&& key, std::string&& val){
  tag_map.emplace(std::move(key), std::move(val));
}

int RGWObjTags::check_and_add_tag(const string&key, const string& val){
  if (tag_map.size() == max_obj_tags ||
      key.size() > max_tag_key_size ||
      val.size() > max_tag_val_size ||
      key.size() == 0){
    return -ERR_INVALID_TAG;
  }

  add_tag(key,val);

  return 0;
}

int RGWObjTags::set_from_string(const string& input){
  if (input.empty()) {
    return 0;
  }
  int ret=0;
  vector <string> kvs;
  boost::split(kvs, input, boost::is_any_of("&"));
  for (const auto& kv: kvs){
    auto p = kv.find("=");
    string key,val;
    if (p != string::npos) {
      ret = check_and_add_tag(url_decode(kv.substr(0,p)),
                              url_decode(kv.substr(p+1)));
    } else {
      ret = check_and_add_tag(url_decode(kv));
    }

    if (ret < 0)
      return ret;
  }
  return ret;
}

void RGWObjTags::dump(Formatter *f) const
{
  f->open_object_section("tagset");
  for (auto& tag: tag_map){
    f->dump_string(tag.first.c_str(), tag.second);
  }
  f->close_section();
}

void RGWObjTags::generate_test_instances(std::list<RGWObjTags*>& o)
{
  RGWObjTags *r = new RGWObjTags;
  r->add_tag("key1","val1");
  r->add_tag("key2","val2");
  o.push_back(r);
  o.push_back(new RGWObjTags);
}

