// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "rgw_xml.h"
#include "rgw_lc.h"
#include "rgw_lc_s3.h"
#include <gtest/gtest.h>
#include <string>
#include <vector>
#include <chrono>
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
      <ArchiveZone />
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
  /* check tags */
  auto tag_map = filter.get_tags().get_tags();
  auto val1 = tag_map.find("spongebob");
  ASSERT_EQ(val1->second, "squarepants");
  /* check our flags */
  ASSERT_EQ(filter.get_flags(), LCFilter::make_flag(LCFlagType::ArchiveZone));
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

TEST(ExpHdr, ReplaceStrftime)
{
  using namespace std::chrono;

  constexpr auto dec21 = year(2012)/12/21;
  auto exp = sys_days(dec21) + 9h + 13min + 7s ;
  auto exp_str = fmt::format("{:%a, %d %b %Y %T %Z}", fmt::gmtime(exp));
  std::cout << "exp_str: " << exp_str << std::endl;
  ASSERT_EQ(exp_str, "Fri, 21 Dec 2012 09:13:07 GMT");

}

static const char *xmldoc_4 =
R"(<Rule>
        <ID>noncur-cleanup-rule</ID>
        <Filter>
           <Prefix></Prefix>
        </Filter>
        <Status>Enabled</Status>
       <NoncurrentVersionExpiration>
            <NewerNoncurrentVersions>5</NewerNoncurrentVersions>
            <NoncurrentDays>365</NoncurrentDays>
       </NoncurrentVersionExpiration>
    </Rule>
)";

TEST(TestLCConfigurationDecoder, XMLDoc4)
{
  RGWXMLDecoder::XMLParser parser;
  ASSERT_TRUE(parser.init());
  ASSERT_TRUE(parser.parse(xmldoc_4, strlen(xmldoc_4), 1));
  LCRule_S3 rule;
  auto result = RGWXMLDecoder::decode_xml("Rule", rule, &parser, true);
  ASSERT_TRUE(result);
  /* check results */
  ASSERT_TRUE(rule.is_enabled());
  const auto& noncur_expiration = rule.get_noncur_expiration();
  ASSERT_EQ(noncur_expiration.get_days(), 365);
  ASSERT_EQ(noncur_expiration.get_newer(), 5);
}

static const char *xmldoc_5 =
R"(<Rule>
        <ID>expire-gt</ID>
        <Expiration>
            <Days>365</Days>
        </Expiration>
        <Filter>
           <And>
           <Prefix></Prefix>
           <ObjectSizeGreaterThan>1024</ObjectSizeGreaterThan>
           <ObjectSizeLessThan>65536</ObjectSizeLessThan>
           </And>
        </Filter>
        <Status>Enabled</Status>
    </Rule>
)";

TEST(TestLCConfigurationDecoder, XMLDoc5)
{
  RGWXMLDecoder::XMLParser parser;
  ASSERT_TRUE(parser.init());
  auto result1 = parser.parse(xmldoc_5, strlen(xmldoc_5), 1);
  ASSERT_TRUE(result1);
  LCRule_S3 rule;
  auto result2 = RGWXMLDecoder::decode_xml("Rule", rule, &parser, true);
  ASSERT_TRUE(result2);
  /* check results */
  ASSERT_TRUE(rule.is_enabled());
  const auto& expiration = rule.get_expiration();
  ASSERT_EQ(expiration.get_days(), 365);
  const auto& filter = rule.get_filter();
  ASSERT_EQ(filter.get_size_gt(), 1024);
  ASSERT_EQ(filter.get_size_lt(), 65536);
}

struct LCWorkTimeTests : ::testing::Test
{
   boost::intrusive_ptr<CephContext> cct;
   std::unique_ptr<RGWLC::LCWorker> worker;

   // expects input in the form of "%m/%d/%y %H:%M:%S"; e.g., "01/15/23 23:59:01"
   utime_t get_utime_by_date_time_string(const std::string& date_time_str)
   {
      struct tm tm{};
      struct timespec ts = {0};

      strptime(date_time_str.c_str(), "%m/%d/%y %H:%M:%S", &tm);
      ts.tv_sec = mktime(&tm);

      return utime_t(ts);
   }

   // expects a map from input value (date & time string) to expected result (boolean)
   void run_should_work_test(const auto& test_values_to_expectations_map) {
      for (const auto& [date_time_str, expected_value] : test_values_to_expectations_map) {
         auto ut = get_utime_by_date_time_string(date_time_str);
         auto should_work = worker->should_work(ut);

         ASSERT_EQ(should_work, expected_value)
            << "input time: " << ut
            << " expected: " << expected_value
            << " should_work: " << should_work
            << " work-time-window: " << cct->_conf->rgw_lifecycle_work_time << std::endl;
      }
   }

   // expects a map from input value (a tuple of date & time strings) to expected result (seconds)
   void run_schedule_next_start_time_test(const auto& test_values_to_expectations_map) {
      for (const auto& [date_time_str_tuple, expected_value] : test_values_to_expectations_map) {
         auto work_started_at = get_utime_by_date_time_string(std::get<0>(date_time_str_tuple));
         auto work_completed_at = get_utime_by_date_time_string(std::get<1>(date_time_str_tuple));
         auto wait_secs_till_next_start = worker->schedule_next_start_time(work_started_at, work_completed_at);

         ASSERT_EQ(wait_secs_till_next_start, expected_value)
            << "work_started_at: " << work_started_at
            << " work_completed_at: " << work_completed_at
            << " expected: " << expected_value
            << " wait_secs_till_next_start: " << wait_secs_till_next_start
            << " work-time-window: " << cct->_conf->rgw_lifecycle_work_time << std::endl;
      }
   }

protected:

   void SetUp() override {
      cct.reset(new CephContext(CEPH_ENTITY_TYPE_ANY), false);

      cct->_conf->set_value("rgw_lc_max_wp_worker", 0, 0); // no need to create a real workpool
      worker = std::make_unique<RGWLC::LCWorker>(nullptr, cct.get(), nullptr, 0);
   }

   void TearDown() override {
      worker.reset();
      cct.reset();
   }
};

TEST_F(LCWorkTimeTests, ShouldWorkDefaultWorkTime)
{
   std::unordered_map<std::string, bool> test_values_to_expectations = {
      {"01/01/23 00:00:00", true},
      {"01/01/24 00:00:00", true}, // date is not relevant, but only the time-window
      {"01/01/23 00:00:01", true},
      {"01/01/23 03:00:00", true},
      {"01/01/23 05:59:59", true},
      {"01/01/23 06:00:00", true},
      {"01/01/23 06:00:59", true}, // seconds don't matter, but only hours and minutes
      {"01/01/23 06:01:00", false},
      {"01/01/23 23:59:59", false},
      {"01/02/23 23:59:59", false},
      {"01/01/23 12:00:00", false},
      {"01/01/23 14:00:00", false}
   };

   run_should_work_test(test_values_to_expectations);
}

TEST_F(LCWorkTimeTests, ShouldWorkCustomWorkTimeEndTimeInTheSameDay)
{
   cct->_conf->rgw_lifecycle_work_time = "14:00-16:00";

   std::unordered_map<std::string, bool> test_values_to_expectations = {
      {"01/01/23 00:00:00", false},
      {"01/01/23 12:00:00", false},
      {"01/01/24 13:59:59", false},
      {"01/01/23 14:00:00", true},
      {"01/01/23 16:00:00", true},
      {"01/01/23 16:00:59", true},
      {"01/01/23 16:01:00", false},
      {"01/01/23 17:00:00", false},
      {"01/01/23 23:59:59", false},
   };

   run_should_work_test(test_values_to_expectations);
}

TEST_F(LCWorkTimeTests, ShouldWorkCustomWorkTimeEndTimeInTheSameDay24Hours)
{
   cct->_conf->rgw_lifecycle_work_time = "00:00-23:59";

   std::unordered_map<std::string, bool> test_values_to_expectations = {
      {"01/01/23 23:59:00", true},
      {"01/01/23 23:59:59", true},
      {"01/01/23 00:00:00", true},
      {"01/01/23 00:00:01", true},
      {"01/01/23 00:01:00", true},
      {"01/01/23 01:00:00", true},
      {"01/01/23 12:00:00", true},
      {"01/01/23 17:00:00", true},
      {"01/01/23 23:00:00", true}
   };

   run_should_work_test(test_values_to_expectations);
}


TEST_F(LCWorkTimeTests, ShouldWorkCustomWorkTimeEndTimeInTheNextDay)
{
   cct->_conf->rgw_lifecycle_work_time = "14:00-01:00";

   std::unordered_map<std::string, bool> test_values_to_expectations = {
      {"01/01/23 13:59:00", false},
      {"01/01/23 13:59:59", false},
      {"01/01/24 14:00:00", true}, // used-to-fail
      {"01/01/24 17:00:00", true}, // used-to-fail
      {"01/01/24 23:59:59", true}, // used-to-fail
      {"01/01/23 00:00:00", true}, // used-to-fail
      {"01/01/23 00:59:59", true}, // used-to-fail
      {"01/01/23 01:00:00", true}, // used-to-fail
      {"01/01/23 01:00:59", true}, // used-to-fail
      {"01/01/23 01:01:00", false},
      {"01/01/23 05:00:00", false},
      {"01/01/23 12:00:00", false},
      {"01/01/23 13:00:00", false}
   };

   run_should_work_test(test_values_to_expectations);
}

TEST_F(LCWorkTimeTests, ShouldWorkCustomWorkTimeEndTimeInTheNextDay24Hours)
{
   cct->_conf->rgw_lifecycle_work_time = "14:00-13:59";

   // all of the below cases used-to-fail
   std::unordered_map<std::string, bool> test_values_to_expectations = {
      {"01/01/23 00:00:00", true},
      {"01/01/23 00:00:01", true},
      {"01/01/23 00:01:00", true},
      {"01/01/24 01:00:00", true},
      {"01/01/24 12:00:00", true},
      {"01/01/24 13:00:00", true},
      {"01/01/24 13:59:00", true},
      {"01/01/24 13:59:59", true},
      {"01/01/23 14:00:00", true},
      {"01/01/23 14:00:01", true},
      {"01/01/23 14:01:00", true},
      {"01/01/23 16:00:00", true},
      {"01/01/23 23:59:00", true},
      {"01/01/23 23:59:59", true},
   };

   run_should_work_test(test_values_to_expectations);
}

TEST_F(LCWorkTimeTests, ShouldWorkCustomWorkTimeEndTimeInTheNextDayIrregularMins)
{
   cct->_conf->rgw_lifecycle_work_time = "22:15-03:33";

   std::unordered_map<std::string, bool> test_values_to_expectations = {
      {"01/01/23 22:14:59", false},
      {"01/01/23 22:15:00", true}, // used-to-fail
      {"01/01/24 00:00:00", true}, // used-to-fail
      {"01/01/24 01:00:00", true}, // used-to-fail
      {"01/01/24 02:00:00", true}, // used-to-fail
      {"01/01/23 03:33:00", true}, // used-to-fail
      {"01/01/23 03:33:59", true}, // used-to-fail
      {"01/01/23 03:34:00", false},
      {"01/01/23 04:00:00", false},
      {"01/01/23 12:00:00", false},
      {"01/01/23 22:00:00", false},
   };

   run_should_work_test(test_values_to_expectations);
}

TEST_F(LCWorkTimeTests, ShouldWorkCustomWorkTimeStartEndSameHour)
{
   cct->_conf->rgw_lifecycle_work_time = "22:15-22:45";

   std::unordered_map<std::string, bool> test_values_to_expectations = {
      {"01/01/23 22:14:59", false},
      {"01/01/23 22:15:00", true},
      {"01/01/24 22:44:59", true},
      {"01/01/24 22:45:59", true},
      {"01/01/24 22:46:00", false},
      {"01/01/23 23:00:00", false},
      {"01/01/23 00:00:00", false},
      {"01/01/23 12:00:00", false},
      {"01/01/23 21:00:00", false},
   };

   run_should_work_test(test_values_to_expectations);
}

TEST_F(LCWorkTimeTests, ScheduleNextStartTime)
{
   cct->_conf->rgw_lifecycle_work_time = "22:15-03:33";

   // items of the map: [ (work_started_time, work_completed_time), expected_value (seconds) ]
   //
   // expected_value is the difference between configured start time (i.e, 22:15:00) and
   // the second item of the tuple (i.e., work_completed_time).
   //
   // Note that "seconds" of work completion time is taken into account but date is not relevant.
   // e.g., the first testcase: 75713 == 01:13:07 - 22:15:00 (https://tinyurl.com/ydm86752)
   std::map<std::tuple<std::string, std::string>, int> test_values_to_expectations = {
      {{"01/01/23 22:15:05", "01/01/23 01:13:07"}, 75713},
      {{"01/01/23 22:15:05", "01/02/23 01:13:07"}, 75713},
      {{"01/01/23 22:15:05", "01/01/23 22:17:07"}, 86273},
      {{"01/01/23 22:15:05", "01/02/23 22:17:07"}, 86273},
      {{"01/01/23 22:15:05", "01/01/23 22:14:00"}, 60},
      {{"01/01/23 22:15:05", "01/02/23 22:14:00"}, 60},
      {{"01/01/23 22:15:05", "01/01/23 22:15:00"}, 24 * 60 * 60},
      {{"01/01/23 22:15:05", "01/02/23 22:15:00"}, 24 * 60 * 60},
      {{"01/01/23 22:15:05", "01/01/23 22:15:01"}, 24 * 60 * 60 - 1},
      {{"01/01/23 22:15:05", "01/02/23 22:15:01"}, 24 * 60 * 60 - 1},
   };

   run_schedule_next_start_time_test(test_values_to_expectations);
}
