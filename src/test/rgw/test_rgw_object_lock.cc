// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include "rgw_object_lock.h"
#include "rgw_xml.h"
#include "common/ceph_json.h"
#include "common/Formatter.h"
#include <gtest/gtest.h>
#include <chrono>
#include <cstring>
#include <sstream>
#include <string>

using namespace std::chrono_literals;

namespace {

// parse an XML document and decode its root element into val
template <typename T>
void decode_xml_doc(const char* name, const std::string& xml, T& val)
{
  RGWXMLDecoder::XMLParser parser;
  ASSERT_TRUE(parser.init());
  ASSERT_TRUE(parser.parse(xml.c_str(), xml.size(), 1));
  RGWXMLDecoder::decode_xml(name, val, &parser, true);
}

template <typename T>
bool xml_doc_rejected(const char* name, const std::string& xml)
{
  RGWXMLDecoder::XMLParser parser;
  if (!parser.init() || !parser.parse(xml.c_str(), xml.size(), 1)) {
    return true;
  }
  T val;
  try {
    RGWXMLDecoder::decode_xml(name, val, &parser, true);
  } catch (const RGWXMLDecoder::err&) {
    return true;
  }
  return false;
}

std::string lock_config_xml(const std::string& enabled, const std::string& rule)
{
  return "<ObjectLockConfiguration><ObjectLockEnabled>" + enabled +
         "</ObjectLockEnabled>" + rule + "</ObjectLockConfiguration>";
}

std::string rule_xml(const std::string& retention)
{
  return "<Rule><DefaultRetention>" + retention + "</DefaultRetention></Rule>";
}

RGWObjectLock lock_from_rule(const std::string& retention)
{
  RGWObjectLock lock;
  decode_xml_doc("ObjectLockConfiguration",
                 lock_config_xml("Enabled", rule_xml(retention)), lock);
  return lock;
}

const ceph::real_time mtime = ceph::real_clock::from_time_t(1700000000); // 2023-11-14T22:13:20Z

} // anonymous namespace

TEST(ObjectLockConfiguration, DecodeDays)
{
  auto lock = lock_from_rule("<Mode>COMPLIANCE</Mode><Days>720</Days>");
  ASSERT_TRUE(lock.has_rule());
  EXPECT_EQ("COMPLIANCE", lock.get_mode());
  EXPECT_EQ(720, lock.get_days());
  EXPECT_EQ(0, lock.get_years());
  EXPECT_TRUE(lock.retention_period_valid());
}

TEST(ObjectLockConfiguration, DecodeYears)
{
  auto lock = lock_from_rule("<Mode>GOVERNANCE</Mode><Years>2</Years>");
  ASSERT_TRUE(lock.has_rule());
  EXPECT_EQ("GOVERNANCE", lock.get_mode());
  EXPECT_EQ(0, lock.get_days());
  EXPECT_EQ(2, lock.get_years());
  EXPECT_TRUE(lock.retention_period_valid());
}

TEST(ObjectLockConfiguration, DecodeWithoutRule)
{
  RGWObjectLock lock;
  decode_xml_doc("ObjectLockConfiguration", lock_config_xml("Enabled", ""), lock);
  EXPECT_FALSE(lock.has_rule());
  EXPECT_EQ(ceph::real_time(), lock.get_lock_until_date(mtime));
}

TEST(ObjectLockConfiguration, RejectInvalid)
{
  // object lock cannot be disabled
  EXPECT_TRUE(xml_doc_rejected<RGWObjectLock>("ObjectLockConfiguration",
      lock_config_xml("Disabled", "")));
  EXPECT_TRUE(xml_doc_rejected<RGWObjectLock>("ObjectLockConfiguration",
      lock_config_xml("", "")));
  // Days and Years are mutually exclusive, and one is required
  EXPECT_TRUE(xml_doc_rejected<RGWObjectLock>("ObjectLockConfiguration",
      lock_config_xml("Enabled", rule_xml("<Mode>GOVERNANCE</Mode><Days>1</Days><Years>1</Years>"))));
  EXPECT_TRUE(xml_doc_rejected<RGWObjectLock>("ObjectLockConfiguration",
      lock_config_xml("Enabled", rule_xml("<Mode>GOVERNANCE</Mode>"))));
  // mode is case sensitive and required
  EXPECT_TRUE(xml_doc_rejected<RGWObjectLock>("ObjectLockConfiguration",
      lock_config_xml("Enabled", rule_xml("<Mode>governance</Mode><Days>1</Days>"))));
  EXPECT_TRUE(xml_doc_rejected<RGWObjectLock>("ObjectLockConfiguration",
      lock_config_xml("Enabled", rule_xml("<Days>1</Days>"))));
  // a Rule must contain DefaultRetention
  EXPECT_TRUE(xml_doc_rejected<RGWObjectLock>("ObjectLockConfiguration",
      lock_config_xml("Enabled", "<Rule></Rule>")));
}

TEST(ObjectLockConfiguration, RetentionPeriodValid)
{
  // these parse, but must be rejected by retention_period_valid()
  EXPECT_FALSE(lock_from_rule("<Mode>GOVERNANCE</Mode><Days>0</Days>").retention_period_valid());
  EXPECT_FALSE(lock_from_rule("<Mode>GOVERNANCE</Mode><Days>-1</Days>").retention_period_valid());
  EXPECT_FALSE(lock_from_rule("<Mode>GOVERNANCE</Mode><Years>0</Years>").retention_period_valid());
  EXPECT_FALSE(lock_from_rule("<Mode>GOVERNANCE</Mode><Years>-1</Years>").retention_period_valid());
}

TEST(ObjectLockConfiguration, LockUntilDateDays)
{
  auto lock = lock_from_rule("<Mode>COMPLIANCE</Mode><Days>720</Days>");
  const ceph::real_time expected = mtime + std::chrono::days(720);
  EXPECT_EQ(expected, lock.get_lock_until_date(mtime));
  // compare counts: gtest cannot print std::chrono::duration with every libstdc++
  EXPECT_EQ(std::chrono::seconds(720 * 24h).count(),
            std::chrono::duration_cast<std::chrono::seconds>(
                lock.get_lock_until_date(mtime) - mtime).count());
}

TEST(ObjectLockConfiguration, LockUntilDateYears)
{
  auto lock = lock_from_rule("<Mode>COMPLIANCE</Mode><Years>1</Years>");
  // one year is the average Gregorian year of 365.2425 days
  EXPECT_EQ(31556952,
            std::chrono::duration_cast<std::chrono::seconds>(
                lock.get_lock_until_date(mtime) - mtime).count());
}

TEST(ObjectLockConfiguration, EncodeDecode)
{
  auto in = lock_from_rule("<Mode>COMPLIANCE</Mode><Days>720</Days>");
  bufferlist bl;
  encode(in, bl);
  RGWObjectLock out;
  auto p = bl.cbegin();
  decode(out, p);
  EXPECT_TRUE(out.has_rule());
  EXPECT_EQ(in.get_mode(), out.get_mode());
  EXPECT_EQ(in.get_days(), out.get_days());
  EXPECT_EQ(in.get_years(), out.get_years());
}

// bucket instance metadata sync carries RGWObjectLock as JSON
TEST(ObjectLockConfiguration, JsonRoundTrip)
{
  for (const auto& retention : {"<Mode>COMPLIANCE</Mode><Days>720</Days>",
                                "<Mode>GOVERNANCE</Mode><Years>3</Years>"}) {
    auto in = lock_from_rule(retention);

    ceph::JSONFormatter f;
    f.open_object_section("bucket_info");
    encode_json("obj_lock", in, &f);
    f.close_section();
    std::stringstream ss;
    f.flush(ss);
    const std::string json = ss.str();

    JSONParser parser;
    ASSERT_TRUE(parser.parse(json.c_str(), json.size()));
    RGWObjectLock out;
    JSONDecoder::decode_json("obj_lock", out, &parser);
    EXPECT_TRUE(out.has_rule()) << json;
    EXPECT_EQ(in.get_mode(), out.get_mode()) << json;
    EXPECT_EQ(in.get_days(), out.get_days()) << json;
    EXPECT_EQ(in.get_years(), out.get_years()) << json;
    EXPECT_EQ(in.get_lock_until_date(mtime), out.get_lock_until_date(mtime)) << json;
  }
}

TEST(ObjectRetention, DecodeXml)
{
  RGWObjectRetention retention;
  decode_xml_doc("Retention",
      "<Retention><Mode>COMPLIANCE</Mode>"
      "<RetainUntilDate>2030-01-01T00:00:00Z</RetainUntilDate></Retention>",
      retention);
  EXPECT_EQ("COMPLIANCE", retention.get_mode());
  // 1893456000 is 2030-01-01T00:00:00Z
  EXPECT_EQ(ceph::real_clock::from_time_t(1893456000), retention.get_retain_until_date());
}

TEST(ObjectRetention, RejectInvalid)
{
  // retention cannot be removed with an empty element
  EXPECT_TRUE(xml_doc_rejected<RGWObjectRetention>("Retention", "<Retention></Retention>"));
  EXPECT_TRUE(xml_doc_rejected<RGWObjectRetention>("Retention",
      "<Retention><Mode>COMPLIANCE</Mode></Retention>"));
  EXPECT_TRUE(xml_doc_rejected<RGWObjectRetention>("Retention",
      "<Retention><RetainUntilDate>2030-01-01T00:00:00Z</RetainUntilDate></Retention>"));
  EXPECT_TRUE(xml_doc_rejected<RGWObjectRetention>("Retention",
      "<Retention><Mode>compliance</Mode>"
      "<RetainUntilDate>2030-01-01T00:00:00Z</RetainUntilDate></Retention>"));
  EXPECT_TRUE(xml_doc_rejected<RGWObjectRetention>("Retention",
      "<Retention><Mode>COMPLIANCE</Mode>"
      "<RetainUntilDate>not-a-date</RetainUntilDate></Retention>"));
}

TEST(ObjectRetention, EncodeDecode)
{
  // 2120-01-01T00:00:00Z is beyond 2106 and needs the 64-bit round-trip encoding
  const ceph::real_time date = ceph::real_clock::from_time_t(4733510400) + 123456789ns;
  RGWObjectRetention in("COMPLIANCE", date);
  bufferlist bl;
  encode(in, bl);
  RGWObjectRetention out;
  auto p = bl.cbegin();
  decode(out, p);
  EXPECT_EQ("COMPLIANCE", out.get_mode());
  EXPECT_EQ(date, out.get_retain_until_date());
}

TEST(ObjectRetention, DecodeV1)
{
  // struct_v 1 carried only the 32-bit encoded date
  const ceph::real_time date = ceph::real_clock::from_time_t(1893456000); // 2030-01-01T00:00:00Z
  bufferlist bl;
  {
    ENCODE_START(1, 1, bl);
    encode(std::string("GOVERNANCE"), bl);
    encode(date, bl);
    ENCODE_FINISH(bl);
  }
  RGWObjectRetention out;
  auto p = bl.cbegin();
  decode(out, p);
  EXPECT_EQ("GOVERNANCE", out.get_mode());
  EXPECT_EQ(date, out.get_retain_until_date());
}

TEST(ObjectLegalHold, DecodeXml)
{
  RGWObjectLegalHold on;
  decode_xml_doc("LegalHold", "<LegalHold><Status>ON</Status></LegalHold>", on);
  EXPECT_TRUE(on.is_enabled());

  RGWObjectLegalHold off;
  decode_xml_doc("LegalHold", "<LegalHold><Status>OFF</Status></LegalHold>", off);
  EXPECT_FALSE(off.is_enabled());

  EXPECT_TRUE(xml_doc_rejected<RGWObjectLegalHold>("LegalHold",
      "<LegalHold><Status>on</Status></LegalHold>"));
  EXPECT_TRUE(xml_doc_rejected<RGWObjectLegalHold>("LegalHold", "<LegalHold></LegalHold>"));
}

TEST(ObjectLegalHold, EncodeDecode)
{
  RGWObjectLegalHold in("ON");
  bufferlist bl;
  encode(in, bl);
  RGWObjectLegalHold out;
  auto p = bl.cbegin();
  decode(out, p);
  EXPECT_TRUE(out.is_enabled());
}
