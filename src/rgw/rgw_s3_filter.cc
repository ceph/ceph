// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include "rgw_pubsub.h"
#include "rgw_tools.h"
#include "rgw_xml.h"
#include "rgw_s3_filter.h"
#include "common/errno.h"
#include "rgw_sal.h"
#include <regex>
#include <algorithm>

void rgw_s3_key_filter::dump(Formatter *f) const {
  if (!has_content()) {
    return;
  }
  f->open_array_section("FilterRules");
  if (!prefix_rule.empty()) {
    f->open_object_section("");
    ::encode_json("Name", "prefix", f);
    ::encode_json("Value", prefix_rule, f);
    ::encode_json("Type", negative_filter_map.find("prefix")->second == true ? "OUT" : "IN", f);
    f->close_section();
  }
  if (!suffix_rule.empty()) {
    f->open_object_section("");
    ::encode_json("Name", "suffix", f);
    ::encode_json("Value", suffix_rule, f);
    ::encode_json("Type", negative_filter_map.find("suffix")->second == true ? "OUT" : "IN", f);
    f->close_section();
  }
  if (!regex_rule.empty()) {
    f->open_object_section("");
    ::encode_json("Name", "regex", f);
    ::encode_json("Value", regex_rule, f);
    ::encode_json("Type", negative_filter_map.find("regex")->second == true ? "OUT" : "IN", f);
    f->close_section();
  }
  f->close_section();
}

bool rgw_s3_key_filter::decode_xml(XMLObj* obj) {
  XMLObjIter iter = obj->find("FilterRule");
  XMLObj *o;

  const auto throw_if_missing = true;
  auto prefix_not_set = true;
  auto suffix_not_set = true;
  auto regex_not_set = true;
  std::string name;
  std::string type; 

  while ((o = iter.get_next())) {
    RGWXMLDecoder::decode_xml("Name", name, o, throw_if_missing);
    
    if(RGWXMLDecoder::decode_xml("Type", type, o, !throw_if_missing))
      negative_filter_map[name] = type == "IN" ? false : true;
    else 
      negative_filter_map[name] = false; // default to IN if not specified
    
    if (name == "prefix" && prefix_not_set) {
      prefix_not_set = false;
      RGWXMLDecoder::decode_xml("Value", prefix_rule, o, throw_if_missing);
    } else if (name == "suffix" && suffix_not_set) {
      suffix_not_set = false;
      RGWXMLDecoder::decode_xml("Value", suffix_rule, o, throw_if_missing);
    } else if (name == "regex" && regex_not_set) {
      regex_not_set = false;
      RGWXMLDecoder::decode_xml("Value", regex_rule, o, throw_if_missing);
    } else {
      throw RGWXMLDecoder::err("invalid/duplicate S3Key filter rule name: '" + name + "'");
    }
  }
  return true;
}

void rgw_s3_key_filter::dump_xml(Formatter *f) const {
  if (!prefix_rule.empty()) {
    f->open_object_section("FilterRule");
    ::encode_xml("Name", "prefix", f);
    ::encode_xml("Value", prefix_rule, f);
    ::encode_xml("Type",  negative_filter_map.find("prefix")->second == true ? "OUT" : "IN", f); 
    f->close_section();
  }
  if (!suffix_rule.empty()) {
    f->open_object_section("FilterRule");
    ::encode_xml("Name", "suffix", f);
    ::encode_xml("Value", suffix_rule, f);
    ::encode_xml("Type", negative_filter_map.find("suffix")->second == true ? "OUT" : "IN", f); 
    f->close_section();
  }
  if (!regex_rule.empty()) {
    f->open_object_section("FilterRule");
    ::encode_xml("Name", "regex", f);
    ::encode_xml("Value", regex_rule, f);
    ::encode_xml("Type", negative_filter_map.find("regex")->second == true ? "OUT" : "IN", f); 
    f->close_section();
  }
}

bool rgw_s3_key_filter::has_content() const {
  return !(prefix_rule.empty() && suffix_rule.empty() && regex_rule.empty());
}

void rgw_s3_key_value_filter::dump(Formatter *f) const {
  if (!has_content()) {
    return;
  }
  f->open_array_section("FilterRules");
  for (const auto& key_value : kv) {
    f->open_object_section("");
    ::encode_json("Name", key_value.first, f);
    ::encode_json("Value", key_value.second, f);
    ::encode_json("Type", negative_filter_map.find(key_value.first)->second == true ? "OUT" : "IN", f);
    f->close_section();
  }
  f->close_section();
}

bool rgw_s3_key_value_filter::decode_xml(XMLObj* obj) {
  kv.clear();
  XMLObjIter iter = obj->find("FilterRule");
  XMLObj *o;

  const auto throw_if_missing = true;

  std::string key;
  std::string value;
  std::string type;

  while ((o = iter.get_next())) {
    RGWXMLDecoder::decode_xml("Name", key, o, throw_if_missing);
    RGWXMLDecoder::decode_xml("Value", value, o, throw_if_missing);
    kv.emplace(key, value);
    if(RGWXMLDecoder::decode_xml("Type", type, o, !throw_if_missing))
      negative_filter_map[key] = type == "IN" ? false : true;
    else {
      negative_filter_map[key] = false; // default to IN if not specified
    }
  }
  return true;
}

void rgw_s3_key_value_filter::dump_xml(Formatter *f) const {
  for (const auto& key_value : kv) {
    f->open_object_section("FilterRule");
    ::encode_xml("Name", key_value.first, f);
    ::encode_xml("Value", key_value.second, f);
    ::encode_xml("Type", negative_filter_map.find(key_value.first)->second == true ? "OUT" : "IN", f);
    f->close_section();
  }
}

bool rgw_s3_key_value_filter::has_content() const {
  return !kv.empty();
}

void rgw_s3_zone_filter::dump(Formatter *f) const {
  if(!has_content())
    return; 
  f->open_array_section("FilterRules");
  for (const auto& filter : negative_filter_map) {
    f->open_object_section("");
    ::encode_json("Name", filter.first, f);
    ::encode_json("Type", filter.second == true ? "OUT" : "IN", f);
    f->close_section();
  }
  f->close_section();
}

bool rgw_s3_zone_filter::decode_xml(XMLObj* obj) {
  XMLObjIter iter = obj->find("FilterRule");
  XMLObj *o;

  const auto throw_if_missing = true;
  std::string name;
  std::string type;

  while ((o = iter.get_next())) {
    RGWXMLDecoder::decode_xml("Name", name, o, throw_if_missing);
    if(RGWXMLDecoder::decode_xml("Type", type, o, !throw_if_missing))
      negative_filter_map[name] = type == "IN" ? false : true;
    else 
      negative_filter_map[name] = false;
  }

  return true;
}

void rgw_s3_zone_filter::dump_xml(Formatter *f) const {
  for (const auto& filter : negative_filter_map) {
    f->open_object_section("FilterRule");
    ::encode_xml("Name", filter.first, f);
    ::encode_xml("Type", filter.second == true ? "OUT" : "IN", f);
    f->close_section();
  }
}

bool rgw_s3_zone_filter::has_content() const {
  return !negative_filter_map.empty();
}

void rgw_s3_filter::dump(Formatter *f) const {
  encode_json("S3Key", key_filter, f);
  encode_json("S3Metadata", metadata_filter, f);
  encode_json("S3Tags", tag_filter, f);
  encode_json("Zones", zone_filter, f);
}

bool rgw_s3_filter::decode_xml(XMLObj* obj) {
  RGWXMLDecoder::decode_xml("S3Key", key_filter, obj);
  RGWXMLDecoder::decode_xml("S3Metadata", metadata_filter, obj);
  RGWXMLDecoder::decode_xml("S3Tags", tag_filter, obj);
  RGWXMLDecoder::decode_xml("Zones", zone_filter, obj);
  return true;
}

void rgw_s3_filter::dump_xml(Formatter *f) const {
  if (key_filter.has_content()) {
    ::encode_xml("S3Key", key_filter, f);
  }
  if (metadata_filter.has_content()) {
    ::encode_xml("S3Metadata", metadata_filter, f);
  }
  if (tag_filter.has_content()) {
    ::encode_xml("S3Tags", tag_filter, f);
  }
  if (zone_filter.has_content()) {
    ::encode_xml("Zones", zone_filter, f);
  }
}

bool rgw_s3_filter::has_content() const {
  return key_filter.has_content()  ||
         metadata_filter.has_content() ||
         tag_filter.has_content() ||
         zone_filter.has_content();
}

bool match(const rgw_s3_key_filter& filter, const std::string& key) {
  const auto key_size = key.size();
  const auto prefix_size = filter.prefix_rule.size();
  if (prefix_size != 0) {
    // prefix rule exists
    bool is_negative = filter.negative_filter_map.find("prefix")->second == true;
    if (prefix_size > key_size) {
      // if prefix is longer than key, we fail if filter is positive, else continue
      if(!is_negative)
        return false; 
    }
    // if prefix does not match, and if filter is positive, we fail or
    // if prefix matches but filter is negative, we fail
    // else continue
    bool does_prefix_match = std::equal(filter.prefix_rule.begin(), filter.prefix_rule.end(), key.begin()); 
    if (!(is_negative ^ does_prefix_match)) {
      return false; 
    }
  }
  const auto suffix_size = filter.suffix_rule.size();
  if (suffix_size != 0) {
    // suffix rule exists
    bool is_negative = filter.negative_filter_map.find("suffix")->second == true;
    if (suffix_size > key_size) {
      // if suffix is longer than key, we fail if filter is positive, else continue
      if(!is_negative)
        return false; 
    }
    // if suffix does not match, and if filter is positive, we fail or
    // if suffix matches but filter is negative, we fail
    // else continue
    bool does_suffix_match = std::equal(filter.suffix_rule.begin(), filter.suffix_rule.end(), (key.end() - suffix_size));
    if (!(is_negative ^ does_suffix_match)) {
      return false;
    }
  }
  if (!filter.regex_rule.empty()) {
    // TODO add regex caching in the filter
    const std::regex base_regex(filter.regex_rule);
    // if regex does not match, and if filter is positive, we fail or
    // if regex matches but filter is negative, we fail
    // else continue
    bool is_negative = filter.negative_filter_map.find("regex")->second == true; 
    bool does_regex_match = std::regex_match(key, base_regex); 
    if (!(is_negative ^ does_regex_match)) {
      return false;
    }
  }
  return true;
}

bool match(const rgw_s3_key_value_filter& filter, const KeyValueMap& kv) {
  // all filter pairs must exist with the same value in the object's metadata/tags
  // object metadata/tags may include items not in the filter 
  return std::all_of(filter.kv.begin(), filter.kv.end(), [&](const auto& p){ 
    bool is_negative = filter.negative_filter_map.find(p.first)->second; 
    auto it = kv.find(p.first);
    bool match_negative = it == kv.end() || it->second != p.second; 
    bool match_positive = it != kv.end() && it->second == p.second;
    return is_negative ? match_negative : match_positive;
  });
}

bool match(const rgw_s3_key_value_filter& filter, const KeyMultiValueMap& kv) {
  // all filter pairs must exist with the same value in the object's metadata/tags
  // object metadata/tags may include items not in the filter
  const auto &negative_filter = filter.negative_filter_map;
  for (auto& filter : filter.kv) {
    auto result = kv.equal_range(filter.first);
    bool is_negative = negative_filter.find(filter.first)->second;
    // For negative filters, all values must NOT match filter.second
    // For positive filters, at least one value must match filter.second
    bool match_negative = std::all_of(result.first, result.second, [&filter](const std::pair<std::string, std::string>&p){ return p.second != filter.second;}); 
    bool match_positive = std::any_of(result.first, result.second, [&filter](const std::pair<std::string, std::string>&p){ return p.second == filter.second;});
    if(is_negative && !match_negative || !is_negative && !match_positive)
      return false;
  }
  return true;
}

bool match(const rgw_s3_zone_filter& filter, const std::string& zone) {
  //if zone is set to OUT, we fail else we return true
  auto it = filter.negative_filter_map.find(zone); 
  if(it != filter.negative_filter_map.end() && it->second == true)
    return false; 
  return true; 
}

bool match(const rgw_s3_filter& s3_filter, const rgw::sal::Object* obj) {
  if (obj == nullptr) {
    return false;
  }

  if (match(s3_filter.key_filter, obj->get_name())) {
    return true;
  }

  const auto &attrs = obj->get_attrs();
  if (!s3_filter.metadata_filter.kv.empty()) {
    KeyValueMap attrs_map;
    for (auto& attr : attrs) {
      if (boost::algorithm::starts_with(attr.first, RGW_ATTR_META_PREFIX)) {
        std::string_view key(attr.first);
        key.remove_prefix(sizeof(RGW_ATTR_PREFIX)-1);
        // we want to pass a null terminated version
        // of the bufferlist, hence "to_str().c_str()"
        attrs_map.emplace(key, attr.second.to_str().c_str());
      }
    }
    if (match(s3_filter.metadata_filter, attrs_map)) {
      return true;
    }
  }

  if (!s3_filter.tag_filter.kv.empty()) {
    // tag filter exists
    // try to fetch tags from the attributes
    KeyMultiValueMap tags;
    const auto attr_iter = attrs.find(RGW_ATTR_TAGS);
    if (attr_iter != attrs.end()) {
      auto bliter = attr_iter->second.cbegin();
      RGWObjTags obj_tags;
      try {
        ::decode(obj_tags, bliter);
      } catch (buffer::error &) {
        // not able to decode tags
        return false;
      }
      tags = std::move(obj_tags.get_tags());
    }
    if (match(s3_filter.tag_filter, tags)) {
      return true;
    }
  }

  
  return false;
}
