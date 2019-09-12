#include "rgw_public_access.h"
#include "rgw_xml.h"

namespace rgw::IAM {

void PublicAccessConfiguration::decode_xml(XMLObj *obj) {
  RGWXMLDecoder::decode_xml("BlockPublicAcls", BlockPublicAcls, obj);
  RGWXMLDecoder::decode_xml("IgnorePublicAcls", IgnorePublicAcls, obj);
  RGWXMLDecoder::decode_xml("BlockPublicPolicy", BlockPublicPolicy, obj);
  RGWXMLDecoder::decode_xml("RestrictPublicBuckets", RestrictPublicBuckets, obj);
  //  RGWXMLDecoder::decode_xml("BlockPublicAccessConfiguration", this, obj);
}

void PublicAccessConfiguration::dump_xml(Formatter *f) const {
  Formatter::ObjectSection os(*f, "BlockPublicAccessConfiguration");
  // AWS spec mentions the values to be ALL CAPs, but clients will not
  // understand this or a mixed case like it is supposed to, hence the need to
  // manually encode here
  auto bool_val = [](bool b) -> auto { return b ? "true": "false"; };

  f->dump_string("BlockPublicAcls", bool_val(BlockPublicAcls));
  f->dump_string("IgnorePublicAcls", bool_val(IgnorePublicAcls));
  f->dump_string("BlockPublicPolicy", bool_val(BlockPublicPolicy));
  f->dump_string("RestrictPublicBuckets", bool_val(RestrictPublicBuckets));
}


ostream& operator<< (ostream& os, const PublicAccessConfiguration& access_conf)
{
    os << std::boolalpha
       << "BlockPublicAcls: " << access_conf.block_public_acls() << std::endl
       << "IgnorePublicAcls: " << access_conf.ignore_public_acls() << std::endl
       << "BlockPublicPolicy" << access_conf.block_public_policy() << std::endl
       << "RestrictPublicBuckets" << access_conf.restrict_public_buckets() << std::endl;

    return os;
}

} // namespace rgw::IAM
