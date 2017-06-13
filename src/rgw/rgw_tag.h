#ifndef RGW_TAG_H
#define RGW_TAG_H

#include <string>
#include <include/types.h>
#include <boost/container/flat_map.hpp>

#include "rgw_common.h"

class RGWObjTags
{
 protected:
  using tag_map_t = boost::container::flat_map <std::string, std::string>;
  tag_map_t tag_map;
 public:
  RGWObjTags() {}
  ~RGWObjTags() {}

  void encode(bufferlist& bl) const {
    ENCODE_START(1,1,bl);
    ::encode(tag_map, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::iterator &bl) {
    DECODE_START_LEGACY_COMPAT_LEN(1, 1, 1, bl);
    ::decode(tag_map,bl);
    DECODE_FINISH(bl);
  }

  void dump(Formatter *f) const;
  bool add_tag(const std::string& key, const std::string& val="");
  int check_and_add_tag(const std::string& key, const std::string& val="");
  size_t count() const {return tag_map.size();}
  int set_from_string(const std::string& input);
  const tag_map_t& get_tags() const {return tag_map;}
};
WRITE_CLASS_ENCODER(RGWObjTags)

#endif /* RGW_TAG_H */
