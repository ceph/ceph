#include "rgw_public_access.h"
#include "rgw_xml.h"

void PublicAccessBlockConfiguration::decode_xml(XMLObj *obj) {
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
       << "BlockPublicAcls: " << access_conf.block_public_acls() << std::endl
       << "IgnorePublicAcls: " << access_conf.ignore_public_acls() << std::endl
       << "BlockPublicPolicy" << access_conf.block_public_policy() << std::endl
       << "RestrictPublicBuckets" << access_conf.restrict_public_buckets() << std::endl;

    os.copyfmt(oldState);
    return os;
}

