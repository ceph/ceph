#ifndef RGW_TAG_H
#define RGW_TAG_H

#include <map>
#include <string>
#include <include/types.h>

#include "rgw_common.h"

class RGWObjTags
{
 protected:
  std::map <std::string, std::string> tags;
 public:
  RGWObjTags() {}
  ~RGWObjTags() {}

  void encode(bufferlist& bl) const {
    ENCODE_START(1,1,bl);
    ::encode(tags, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::iterator &bl) {
    DECODE_START_LEGACY_COMPAT_LEN(1, 1, 1, bl);
    ::decode(tags,bl);
    DECODE_FINISH(bl);
  }

  void dump(Formatter *f) const;
  bool add_tag(const std::string& key, const std::string& val="");
  int check_and_add_tag(const std::string& key, const std::string& val="");
  size_t count() const {return tags.size();}
  int set_from_string(const std::string& input);
  const map <std::string,std::string>& get_tags() const {return tags;}
};
WRITE_CLASS_ENCODER(RGWObjTags)

#endif /* RGW_TAG_H */
