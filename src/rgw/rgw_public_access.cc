#include "rgw_public_access.h"
#include "rgw_xml.h"

void PublicAccessBlockConfiguration::decode_xml(XMLObj *obj) {
  // a misspelled flag would otherwise read as false, a repeated one as its first value
  XMLObjIter iter = obj->find_first();
  std::string_view prev; // children are sorted by name, so a repeat follows the first
  while (const XMLObj *child = iter.get_next()) {
    const std::string& name = child->get_obj_type();
    if (name != "BlockPublicAcls" && name != "IgnorePublicAcls" &&
        name != "BlockPublicPolicy" && name != "RestrictPublicBuckets") {
      throw RGWXMLDecoder::err("unknown element " + name);
    }
    if (name == prev) {
      throw RGWXMLDecoder::err("duplicate element " + name);
    }
    prev = name;
  }
  RGWXMLDecoder::decode_xml("BlockPublicAcls", BlockPublicAcls, obj);
  RGWXMLDecoder::decode_xml("IgnorePublicAcls", IgnorePublicAcls, obj);
  RGWXMLDecoder::decode_xml("BlockPublicPolicy", BlockPublicPolicy, obj);
  RGWXMLDecoder::decode_xml("RestrictPublicBuckets", RestrictPublicBuckets, obj);
}

void PublicAccessBlockConfiguration::dump_xml(Formatter *f) const {
  Formatter::ObjectSection os(*f, "PublicAccessBlockConfiguration");
  // Note: AWS spec mentions the values to be ALL CAPs, but clients seem to
  // require all small letters, and S3 itself doesn't seem to follow the API
  // spec here
  f->dump_bool("BlockPublicAcls", BlockPublicAcls);
  f->dump_bool("IgnorePublicAcls", IgnorePublicAcls);
  f->dump_bool("BlockPublicPolicy", BlockPublicPolicy);
  f->dump_bool("RestrictPublicBuckets", RestrictPublicBuckets);
}


std::ostream& operator<< (std::ostream& os, const PublicAccessBlockConfiguration& access_conf)
{
    std::ios oldState(nullptr);
    oldState.copyfmt(os);

    os << std::boolalpha
       << "BlockPublicAcls: " << access_conf.BlockPublicAcls << std::endl
       << "IgnorePublicAcls: " << access_conf.IgnorePublicAcls << std::endl
       << "BlockPublicPolicy" << access_conf.BlockPublicPolicy << std::endl
       << "RestrictPublicBuckets" << access_conf.RestrictPublicBuckets << std::endl;

    os.copyfmt(oldState);
    return os;
}

auto config_union(const PublicAccessBlockConfiguration& lhs,
                  const PublicAccessBlockConfiguration& rhs)
  -> PublicAccessBlockConfiguration
{
  return {
    .BlockPublicAcls = lhs.BlockPublicAcls || rhs.BlockPublicAcls,
    .IgnorePublicAcls = lhs.IgnorePublicAcls || rhs.IgnorePublicAcls,
    .BlockPublicPolicy = lhs.BlockPublicPolicy || rhs.BlockPublicPolicy,
    .RestrictPublicBuckets = lhs.RestrictPublicBuckets || rhs.RestrictPublicBuckets,
  };
}
