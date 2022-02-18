// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#ifndef RGW_TAG_H
#define RGW_TAG_H

#include <string>
#include <include/types.h>
#include <map>

class RGWObjTags
{
public:
  using tag_map_t = std::multimap <std::string, std::string>;

protected:
  tag_map_t tag_map;

  uint32_t max_obj_tags{10};
  static constexpr uint32_t max_tag_key_size{128};
  static constexpr uint32_t max_tag_val_size{256};

 public:
  RGWObjTags() = default;
  RGWObjTags(uint32_t max_obj_tags):max_obj_tags(max_obj_tags) {}

  void encode(bufferlist& bl) const {
    ENCODE_START(1,1,bl);
    encode(tag_map, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator &bl) {
    DECODE_START_LEGACY_COMPAT_LEN(1, 1, 1, bl);
    decode(tag_map,bl);
    DECODE_FINISH(bl);
  }

  void dump(Formatter *f) const;
  void add_tag(const std::string& key, const std::string& val="");
  void emplace_tag(std::string&& key, std::string&& val);
  int check_and_add_tag(const std::string& key, const std::string& val="");
  size_t count() const {return tag_map.size();}
  int set_from_string(const std::string& input);
  void clear() { tag_map.clear(); }
  bool empty() const noexcept { return tag_map.empty(); }
  const tag_map_t& get_tags() const {return tag_map;}
  tag_map_t& get_tags() {return tag_map;}
};
WRITE_CLASS_ENCODER(RGWObjTags)

#endif /* RGW_TAG_H */
