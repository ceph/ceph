#include "rgw_public_acess.h"
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
  encode_xml("BlockPublicAcls", BlockPublicAcls, f);
  encode_xml("IgnorePublicAcls", IgnorePublicAcls, f);
  encode_xml("BlockPublicPolicy", BlockPublicPolicy, f);
  encode_xml("RestrictPublicBuckets", RestrictPublicBuckets, f);
}

} // namespace rgw::IAM
