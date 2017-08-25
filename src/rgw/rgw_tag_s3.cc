#include <map>
#include <string>
#include <iostream>

#include "include/types.h"

#include "rgw_tag_s3.h"

bool RGWObjTagEntry_S3::xml_end(const char*){
  RGWObjTagKey_S3 *key_obj = static_cast<RGWObjTagKey_S3 *>(find_first("Key"));
  RGWObjTagValue_S3 *val_obj = static_cast<RGWObjTagValue_S3 *>(find_first("Value"));

  if (!key_obj)
    return false;

  string s = key_obj->get_data();
  if (s.empty()){
    return false;
  }

  key = s;
  if (val_obj) {
    val = val_obj->get_data();
  }

  return true;
}

bool RGWObjTagSet_S3::xml_end(const char*){
  XMLObjIter iter = find("Tag");
  RGWObjTagEntry_S3 *tagentry = static_cast<RGWObjTagEntry_S3 *>(iter.get_next());
  while (tagentry) {
    const std::string& key = tagentry->get_key();
    const std::string& val = tagentry->get_val();
    if (!add_tag(key,val))
      return false;

    tagentry = static_cast<RGWObjTagEntry_S3 *>(iter.get_next());
  }
  return true;
}

int RGWObjTagSet_S3::rebuild(RGWObjTags& dest){
  int ret;
  for (const auto &it: tag_map){
    ret = dest.check_and_add_tag(it.first, it.second);
    if (ret < 0)
      return ret;
  }
  return 0;
}

bool RGWObjTagging_S3::xml_end(const char*){
  RGWObjTagSet_S3 *tagset = static_cast<RGWObjTagSet_S3 *> (find_first("TagSet"));
  return tagset != nullptr;

}

void RGWObjTagSet_S3::dump_xml(Formatter *f) const {
  for (const auto& tag: tag_map){
    f->open_object_section("Tag");
    f->dump_string("Key", tag.first);
    f->dump_string("Value", tag.second);
    f->close_section();
  }
}

XMLObj *RGWObjTagsXMLParser::alloc_obj(const char *el){
  XMLObj* obj = nullptr;
  if(strcmp(el,"Tagging") == 0) {
    obj = new RGWObjTagging_S3();
  } else if (strcmp(el,"TagSet") == 0) {
    obj = new RGWObjTagSet_S3();
  } else if (strcmp(el,"Tag") == 0) {
    obj = new RGWObjTagEntry_S3();
  } else if (strcmp(el,"Key") == 0) {
    obj = new RGWObjTagKey_S3();
  } else if (strcmp(el,"Value") == 0) {
    obj = new RGWObjTagValue_S3();
  }

  return obj;
}
