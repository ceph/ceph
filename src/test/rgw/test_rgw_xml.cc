// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "rgw_xml.h"
#include <gtest/gtest.h>
#include <list>
#include <stdexcept>

struct NameAndStatus {
  // these are sub-tags
  std::string name;
  bool status;
  
  // intrusive XML decoding API
  bool decode_xml(XMLObj *obj) {
    if (!RGWXMLDecoder::decode_xml("Name", name, obj, true)) {
      // name is mandatory
      return false;
    }
    if (!RGWXMLDecoder::decode_xml("Status", status, obj, false)) {
      // status is optional and defaults to True
      status = true;
    }
    return true;
  }
};

struct Item {
  // these are sub-tags
  NameAndStatus name_and_status;
  int value;
  int extra_value;

  // these are attributes
  std::string date;
  std::string comment;
 
  // intrusive XML decoding API
  bool decode_xml(XMLObj *obj) {
    if (!RGWXMLDecoder::decode_xml("NameAndStatus", name_and_status, obj, true)) {
      // name amd status are mandatory
      return false;
    }
    if (!RGWXMLDecoder::decode_xml("Value", value, obj, true)) {
      // value is mandatory
      return false;
    }
    if (!RGWXMLDecoder::decode_xml("ExtraValue", extra_value, obj, false)) {
      // extra value is optional and defaults to zero
      extra_value = 0;
    }

    // date attribute is optional
    if (!obj->get_attr("Date", date)) {
      date = "no date";
    }
    // comment attribute is optional
    if (!obj->get_attr("Comment", comment)) {
      comment = "no comment";
    }

    return true;
  }
};

struct Items {
  // these are sub-tags
  std::list<Item> item_list;
  
  // intrusive XML decoding API
  bool decode_xml(XMLObj *obj) {
    do_decode_xml_obj(item_list, "Item", obj);
    return true;
  }
};

// in case of non-intrusive decoding class
// hierarchy should reflect the XML hierarchy

class NameXMLObj: public XMLObj {
protected:
  void xml_handle_data(const char *s, int len) override {
    // no need to set "data", setting "name" directly
    value.append(s, len);
  }

public:
  std::string value;
  ~NameXMLObj() override = default;
};

class StatusXMLObj: public XMLObj {
protected:
  void xml_handle_data(const char *s, int len) override {
    std::istringstream is(std::string(s, len));
    is >> std::boolalpha >> value;
  }

public:
  bool value;
  ~StatusXMLObj() override = default;
};

class NameAndStatusXMLObj: public NameAndStatus, public XMLObj {
public:
  ~NameAndStatusXMLObj() override = default;

  bool xml_end(const char *el) override {
    XMLObjIter iter = find("Name");
    NameXMLObj* _name = static_cast<NameXMLObj*>(iter.get_next());
    if (!_name) {
      // name is mandatory
      return false;
    }
    name = _name->value;
    iter = find("Status");
    StatusXMLObj* _status = static_cast<StatusXMLObj*>(iter.get_next());
    if (!_status) {
      // status is optional and defaults to True
      status = true;
    } else {
      status = _status->value;
    }
    return true;
  }
};

class ItemXMLObj: public Item, public XMLObj {
public:
  ~ItemXMLObj() override = default;
  
  bool xml_end(const char *el) override {
    XMLObjIter iter = find("NameAndStatus");
    NameAndStatusXMLObj* _name_and_status = static_cast<NameAndStatusXMLObj*>(iter.get_next());
    if (!_name_and_status) {
      // name and status are mandatory
      return false;
    }
    name_and_status = *static_cast<NameAndStatus*>(_name_and_status);
    iter = find("Value");
    XMLObj* _value = iter.get_next();
    if (!_value) {
      // value is mandatory
      return false;
    }
    try {
      value = std::stoi(_value->get_data());
    } catch (const std::exception& e) {
      return false;
    }
    iter = find("ExtraValue");
    XMLObj* _extra_value = iter.get_next();
    if (_extra_value) {
      // extra value is optional but cannot contain garbage
      try {
        extra_value = std::stoi(_extra_value->get_data());
      } catch (const std::exception& e) {
        return false;
      }
    } else {
      // if not set, it defaults to zero
      extra_value = 0;
    }

    // date attribute is optional
    if (!get_attr("Date", date)) {
      date = "no date";
    }
    // comment attribute is optional
    if (!get_attr("Comment", comment)) {
      comment = "no comment";
    }

    return true;
  }
};

class ItemsXMLObj: public Items, public XMLObj {
public:
  ~ItemsXMLObj() override = default;

  bool xml_end(const char *el) override {
    XMLObjIter iter = find("Item");
    ItemXMLObj* item_ptr = static_cast<ItemXMLObj*>(iter.get_next());
    // mandatory to have at least one item
    bool item_found = false;
    while (item_ptr) {
      item_list.push_back(*static_cast<Item*>(item_ptr));
      item_ptr = static_cast<ItemXMLObj*>(iter.get_next());
      item_found = true;
    }
    return item_found;
  }
};

class ItemsXMLParser: public RGWXMLParser {
  static const int MAX_NAME_LEN = 16;
public:
  XMLObj *alloc_obj(const char *el) override {
    if (strncmp(el, "Items", MAX_NAME_LEN) == 0) {
      items = new ItemsXMLObj;
      return items;
    } else if (strncmp(el, "Item", MAX_NAME_LEN) == 0) {
      return new ItemXMLObj;
    } else if (strncmp(el, "NameAndStatus", MAX_NAME_LEN) == 0) {
      return new NameAndStatusXMLObj;
    } else if (strncmp(el, "Name", MAX_NAME_LEN) == 0) {
      return new NameXMLObj;
    } else if (strncmp(el, "Status", MAX_NAME_LEN) == 0) {
      return new StatusXMLObj;
    }
    return nullptr;
  }
  // this is a pointer to the parsed results
  ItemsXMLObj* items;
};

static const char* good_input = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
                           "<Items>"
                             "<Item><NameAndStatus><Name>hello</Name></NameAndStatus><Value>1</Value></Item>"
                             "<Item><ExtraValue>99</ExtraValue><NameAndStatus><Name>world</Name></NameAndStatus><Value>2</Value></Item>"
                             "<Item><Value>3</Value><NameAndStatus><Name>foo</Name></NameAndStatus></Item>"
                             "<Item><Value>4</Value><ExtraValue>42</ExtraValue><NameAndStatus><Name>bar</Name><Status>False</Status></NameAndStatus></Item>"
                           "</Items>";

static const char* expected_output = "((hello,1),1,0),((world,1),2,99),((foo,1),3,0),((bar,0),4,42),";

std::string to_string(const Items& items) {
  std::stringstream ss;
  for (const auto& item : items.item_list) {
    ss << "((" << item.name_and_status.name << "," << item.name_and_status.status << ")," << item.value << "," << item.extra_value << ")" << ",";
  }
  return ss.str();
}

std::string to_string_with_attributes(const Items& items) {
  std::stringstream ss;
  for (const auto& item : items.item_list) {
    ss << "(" << item.date << "," << item.comment << ",(" << item.name_and_status.name << "," << item.name_and_status.status << ")," 
      << item.value << "," << item.extra_value << ")" << ",";
  }
  return ss.str();
}

TEST(TestParser, BasicParsing)
{  
  ItemsXMLParser parser;
  ASSERT_TRUE(parser.init());
  ASSERT_TRUE(parser.parse(good_input, strlen(good_input), 1));
  ASSERT_EQ(parser.items->item_list.size(), 4U);
  ASSERT_STREQ(to_string(*parser.items).c_str(), expected_output);
}

static const char* malformed_input = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
                           "<Items>"
                             "<Item><NameAndStatus><Name>hello</Name></NameAndStatus><Value>1</Value><Item>"
                             "<Item><ExtraValue>99</ExtraValue><NameAndStatus><Name>world</Name></NameAndStatus><Value>2</Value></Item>"
                             "<Item><Value>3</Value><NameAndStatus><Name>foo</Name></NameAndStatus></Item>"
                             "<Item><Value>4</Value><ExtraValue>42</ExtraValue><NameAndStatus><Name>bar</Name><Status>False</Status></NameAndStatus></Item>"
                           "</Items>";

TEST(TestParser, MalformedInput)
{  
  ItemsXMLParser parser;
  ASSERT_TRUE(parser.init());
  ASSERT_FALSE(parser.parse(good_input, strlen(malformed_input), 1));
}

static const char* missing_value_input = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
                           "<Items>"
                             "<Item><NameAndStatus><Name>hello</Name></NameAndStatus><Value>1</Value></Item>"
                             "<Item><ExtraValue>99</ExtraValue><NameAndStatus><Name>world</Name></NameAndStatus><Value>2</Value></Item>"
                             "<Item><Value>3</Value><NameAndStatus><Name>foo</Name></NameAndStatus></Item>"
                             "<Item><ExtraValue>42</ExtraValue><NameAndStatus><Name>bar</Name><Status>False</Status></NameAndStatus></Item>"
                           "</Items>";

TEST(TestParser, MissingMandatoryTag)
{  
  ItemsXMLParser parser;
  ASSERT_TRUE(parser.init());
  ASSERT_FALSE(parser.parse(missing_value_input, strlen(missing_value_input), 1));
}

static const char* unknown_tag_input = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
                           "<Items>"
                             "<Item><NameAndStatus><Name>hello</Name></NameAndStatus><Value>1</Value></Item>"
                             "<Item><ExtraValue>99</ExtraValue><NameAndStatus><Name>world</Name></NameAndStatus><Value>2</Value></Item>"
                             "<Item><Value>3</Value><NameAndStatus><Name>foo</Name></NameAndStatus><Kaboom>0</Kaboom></Item>"
                             "<Item><Value>4</Value><ExtraValue>42</ExtraValue><NameAndStatus><Name>bar</Name><Status>False</Status></NameAndStatus></Item>"
                             "<Kaboom>0</Kaboom>"
                           "</Items>";

TEST(TestParser, UnknownTag)
{  
  ItemsXMLParser parser;
  ASSERT_TRUE(parser.init());
  ASSERT_TRUE(parser.parse(unknown_tag_input, strlen(unknown_tag_input), 1));
  ASSERT_EQ(parser.items->item_list.size(), 4U);
  ASSERT_STREQ(to_string(*parser.items).c_str(), expected_output);
}

static const char* invalid_value_input = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
                           "<Items>"
                             "<Item><NameAndStatus><Name>hello</Name></NameAndStatus><Value>1</Value></Item>"
                             "<Item><ExtraValue>kaboom</ExtraValue><NameAndStatus><Name>world</Name></NameAndStatus><Value>2</Value></Item>"
                             "<Item><Value>3</Value><NameAndStatus><Name>foo</Name></NameAndStatus></Item>"
                             "<Item><Value>4</Value><ExtraValue>42</ExtraValue><NameAndStatus><Name>bar</Name><Status>False</Status></NameAndStatus></Item>"
                           "</Items>";

TEST(TestParser, InvalidValue)
{  
  ItemsXMLParser parser;
  ASSERT_TRUE(parser.init());
  ASSERT_FALSE(parser.parse(invalid_value_input, strlen(invalid_value_input), 1));
}

static const char* good_input1 = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
                           "<Items>"
                             "<Item><NameAndStatus><Name>hello</Name></NameAndStatus><Value>1</Value></Item>"
                             "<Item><ExtraValue>99</ExtraValue><NameAndStatus><Name>world</Name>";

static const char* good_input2 = "</NameAndStatus><Value>2</Value></Item>"
                             "<Item><Value>3</Value><NameAndStatus><Name>foo</Name></NameAndStatus></Item>"
                             "<Item><Value>4</Value><ExtraValue>42</ExtraValue><NameAndStatus><Name>bar</Name><Status>False</Status></NameAndStatus></Item>"
                           "</Items>";

TEST(TestParser, MultipleChunks)
{  
  ItemsXMLParser parser;
  ASSERT_TRUE(parser.init());
  ASSERT_TRUE(parser.parse(good_input1, strlen(good_input1), 0));
  ASSERT_TRUE(parser.parse(good_input2, strlen(good_input2), 1));
  ASSERT_EQ(parser.items->item_list.size(), 4U);
  ASSERT_STREQ(to_string(*parser.items).c_str(), expected_output);
}

static const char* input_with_attributes = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
                           "<Items>"
                             "<Item Date=\"Tue Dec 27 17:21:29 2011\" Kaboom=\"just ignore\">"
                               "<NameAndStatus><Name>hello</Name></NameAndStatus><Value>1</Value>"
                             "</Item>"
                             "<Item Comment=\"hello world\">"
                               "<ExtraValue>99</ExtraValue><NameAndStatus><Name>world</Name></NameAndStatus><Value>2</Value>"
                             "</Item>"
                             "<Item><Value>3</Value><NameAndStatus><Name>foo</Name></NameAndStatus></Item>"
                             "<Item Comment=\"goodbye\" Date=\"Thu Feb 28 10:00:18 UTC 2019 \">"
                               "<Value>4</Value><ExtraValue>42</ExtraValue><NameAndStatus><Name>bar</Name><Status>False</Status></NameAndStatus>"
                             "</Item>"
                           "</Items>";

static const char* expected_output_with_attributes = "(Tue Dec 27 17:21:29 2011,no comment,(hello,1),1,0),"
                                                     "(no date,hello world,(world,1),2,99),"
                                                     "(no date,no comment,(foo,1),3,0),"
                                                     "(Thu Feb 28 10:00:18 UTC 2019 ,goodbye,(bar,0),4,42),";

TEST(TestParser, Attributes)
{  
  ItemsXMLParser parser;
  ASSERT_TRUE(parser.init());
  ASSERT_TRUE(parser.parse(input_with_attributes, strlen(input_with_attributes), 1));
  ASSERT_EQ(parser.items->item_list.size(), 4U);
  ASSERT_STREQ(to_string_with_attributes(*parser.items).c_str(), 
      expected_output_with_attributes);
}

TEST(TestDecoder, BasicParsing)
{
  RGWXMLDecoder::XMLParser parser;
  ASSERT_TRUE(parser.init());
  ASSERT_TRUE(parser.parse(good_input, strlen(good_input), 1));
  Items result;
  ASSERT_NO_THROW({
    ASSERT_TRUE(RGWXMLDecoder::decode_xml("Items", result, &parser, true));
  });
  ASSERT_EQ(result.item_list.size(), 4U);
  ASSERT_STREQ(to_string(result).c_str(), expected_output);
}

TEST(TestDecoder, MalformedInput)
{
  RGWXMLDecoder::XMLParser parser;
  ASSERT_TRUE(parser.init());
  ASSERT_FALSE(parser.parse(good_input, strlen(malformed_input), 1));
}

TEST(TestDecoder, MissingMandatoryTag)
{
  RGWXMLDecoder::XMLParser parser;
  ASSERT_TRUE(parser.init());
  ASSERT_TRUE(parser.parse(missing_value_input, strlen(missing_value_input), 1));
  Items result;
  ASSERT_ANY_THROW({
    ASSERT_TRUE(RGWXMLDecoder::decode_xml("Items", result, &parser, true));
  });
}

TEST(TestDecoder, InvalidValue)
{
  RGWXMLDecoder::XMLParser parser;
  ASSERT_TRUE(parser.init());
  ASSERT_TRUE(parser.parse(invalid_value_input, strlen(invalid_value_input), 1));
  Items result;
  ASSERT_ANY_THROW({
    ASSERT_TRUE(RGWXMLDecoder::decode_xml("Items", result, &parser, true));
  });
}

TEST(TestDecoder, MultipleChunks)
{  
  RGWXMLDecoder::XMLParser parser;
  ASSERT_TRUE(parser.init());
  ASSERT_TRUE(parser.parse(good_input1, strlen(good_input1), 0));
  ASSERT_TRUE(parser.parse(good_input2, strlen(good_input2), 1));
  Items result;
  ASSERT_NO_THROW({
    ASSERT_TRUE(RGWXMLDecoder::decode_xml("Items", result, &parser, true));
  });
  ASSERT_EQ(result.item_list.size(), 4U);
  ASSERT_STREQ(to_string(result).c_str(), expected_output);
}

TEST(TestDecoder, Attributes)
{  
  RGWXMLDecoder::XMLParser parser;
  ASSERT_TRUE(parser.init());
  ASSERT_TRUE(parser.parse(input_with_attributes, strlen(input_with_attributes), 1));
  Items result;
  ASSERT_NO_THROW({
    ASSERT_TRUE(RGWXMLDecoder::decode_xml("Items", result, &parser, true));
  });
  ASSERT_EQ(result.item_list.size(), 4U);
  ASSERT_STREQ(to_string_with_attributes(result).c_str(), 
      expected_output_with_attributes);
}

static const char* expected_xml_output = "<Items xmlns=\"https://www.ceph.com/doc/\">"
                             "<Item Order=\"0\"><NameAndStatus><Name>hello</Name><Status>True</Status></NameAndStatus><Value>0</Value></Item>"
                             "<Item Order=\"1\"><NameAndStatus><Name>hello</Name><Status>False</Status></NameAndStatus><Value>1</Value></Item>"
                             "<Item Order=\"2\"><NameAndStatus><Name>hello</Name><Status>True</Status></NameAndStatus><Value>2</Value></Item>"
                             "<Item Order=\"3\"><NameAndStatus><Name>hello</Name><Status>False</Status></NameAndStatus><Value>3</Value></Item>"
                             "<Item Order=\"4\"><NameAndStatus><Name>hello</Name><Status>True</Status></NameAndStatus><Value>4</Value></Item>"
                           "</Items>";
TEST(TestEncoder, ListWithAttrsAndNS)
{
  XMLFormatter f;
  const auto array_size = 5;
  f.open_array_section_in_ns("Items", "https://www.ceph.com/doc/");
  for (auto i = 0; i < array_size; ++i) {
    FormatterAttrs item_attrs("Order", std::to_string(i).c_str(), NULL);
    f.open_object_section_with_attrs("Item", item_attrs);
    f.open_object_section("NameAndStatus");
    encode_xml("Name", "hello", &f);
    encode_xml("Status", (i%2 == 0), &f);
    f.close_section();
    encode_xml("Value", i, &f);
    f.close_section();
  }
  f.close_section();
  std::stringstream ss;
  f.flush(ss);
  ASSERT_STREQ(ss.str().c_str(), expected_xml_output);
}

