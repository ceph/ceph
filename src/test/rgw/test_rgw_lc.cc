// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "rgw_xml.h"
#include "rgw_lc.h"
#include "rgw_lc_s3.h"
#include <gtest/gtest.h>
//#include <spawn/spawn.hpp>
#include <string>
#include <vector>
#include <stdexcept>

static const char* xmldoc_1 =
R"(<Filter>
   <And>
      <Prefix>tax/</Prefix>
      <Tag>
         <Key>key1</Key>
         <Value>value1</Value>
      </Tag>
      <Tag>
         <Key>key2</Key>
         <Value>value2</Value>
      </Tag>
    </And>
</Filter>
)";

TEST(TestLCFilterDecoder, XMLDoc1)
{
  RGWXMLDecoder::XMLParser parser;
  ASSERT_TRUE(parser.init());
  ASSERT_TRUE(parser.parse(xmldoc_1, strlen(xmldoc_1), 1));
  LCFilter_S3 filter;
  auto result = RGWXMLDecoder::decode_xml("Filter", filter, &parser, true);
  ASSERT_TRUE(result);
  /* check repeated Tag element */
  auto tag_map = filter.get_tags().get_tags();
  auto val1 = tag_map.find("key1");
  ASSERT_EQ(val1->second, "value1");
  auto val2 = tag_map.find("key2");
  ASSERT_EQ(val2->second, "value2");
  /* check our flags */
  ASSERT_EQ(filter.get_flags(), 0);
}

static const char* xmldoc_2 =
R"(<Filter>
   <And>
      <Flag>
         ArchiveZone
      </Flag>
      <Flag>
         ArchiveZone
      </Flag>
      <Tag>
         <Key>spongebob</Key>
         <Value>squarepants</Value>
      </Tag>
    </And>
</Filter>
)";

TEST(TestLCFilterDecoder, XMLDoc2)
{
  RGWXMLDecoder::XMLParser parser;
  ASSERT_TRUE(parser.init());
  ASSERT_TRUE(parser.parse(xmldoc_2, strlen(xmldoc_2), 1));
  LCFilter_S3 filter;
  auto result = RGWXMLDecoder::decode_xml("Filter", filter, &parser, true);
  ASSERT_TRUE(result);
  /* check repeated Tag element */
  auto tag_map = filter.get_tags().get_tags();
  auto val1 = tag_map.find("spongebob");
  ASSERT_EQ(val1->second, "squarepants");
  /* check our flags */
  ASSERT_EQ(filter.get_flags(), uint32_t(LCFlagType::ArchiveZone));
}

// invalid And element placement
static const char* xmldoc_3 =
R"(<Filter>
    <And>
      <Tag>
         <Key>miles</Key>
         <Value>davis</Value>
      </Tag>
    </And>
      <Tag>
         <Key>spongebob</Key>
         <Value>squarepants</Value>
      </Tag>
</Filter>
)";

TEST(TestLCFilterInvalidAnd, XMLDoc3)
{
  RGWXMLDecoder::XMLParser parser;
  ASSERT_TRUE(parser.init());
  ASSERT_TRUE(parser.parse(xmldoc_3, strlen(xmldoc_3), 1));
  LCFilter_S3 filter;
  auto result = RGWXMLDecoder::decode_xml("Filter", filter, &parser, true);
  ASSERT_TRUE(result);
  /* check repeated Tag element */
  auto tag_map = filter.get_tags().get_tags();
  auto val1 = tag_map.find("spongebob");
  ASSERT_TRUE(val1 == tag_map.end());
  /* because the invalid 2nd tag element was not recognized,
   * we cannot access it:
  ASSERT_EQ(val1->second, "squarepants");
  */
  /* check our flags */
  ASSERT_EQ(filter.get_flags(), uint32_t(LCFlagType::none));
}
