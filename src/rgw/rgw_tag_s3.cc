// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include <map>
#include <string>
#include <iostream>

#include "include/types.h"

#include "rgw_tag_s3.h"

void RGWObjTagEntry_S3::decode_xml(XMLObj *obj) {
  RGWXMLDecoder::decode_xml("Key", key, obj, true);
  RGWXMLDecoder::decode_xml("Value", val, obj, true);
}

void RGWObjTagEntry_S3::dump_xml(Formatter *f) const {
  encode_xml("Key", key, f);
  encode_xml("Value", val, f);

  if (key.empty()) {
    throw RGWXMLDecoder::err("empty key");
  }

  if (val.empty()) {
    throw RGWXMLDecoder::err("empty val");
  }
}

void RGWObjTagSet_S3::decode_xml(XMLObj *obj) {
  vector<RGWObjTagEntry_S3> entries;

  RGWXMLDecoder::decode_xml("Tag", entries, obj, true);

  for (auto& entry : entries) {
    const std::string& key = entry.get_key();
    const std::string& val = entry.get_val();
    add_tag(key,val);
  }
}

int RGWObjTagSet_S3::rebuild(RGWObjTags& dest) {
  int ret;
  for (const auto &it : tag_map){
    ret = dest.check_and_add_tag(it.first, it.second);
    if (ret < 0)
      return ret;
  }
  return 0;
}

void RGWObjTagging_S3::decode_xml(XMLObj *obj) {
  RGWXMLDecoder::decode_xml("TagSet", tagset, obj, true);
}

void RGWObjTagSet_S3::dump_xml(Formatter *f) const {
  for (const auto& tag : tag_map){
    Formatter::ObjectSection os(*f, "Tag");
    encode_xml("Key", tag.first, f);
    encode_xml("Value", tag.second, f);
  }
}

