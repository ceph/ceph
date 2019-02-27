// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "rgw/rgw_xml.h"
#include <gtest/gtest.h>
#include <list>
#include <stdexcept>

struct NameAndStatus {
  NameAndStatus() = default;
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
  Item() = default;
  Item(const NameAndStatus& _name, int _value, int _extra_value) {
    name = _name;
    value = _value;
    extra_value = _extra_value;
  }

  NameAndStatus name;
  int value;
  int extra_value;
 
  // intrusive XML decoding API
  bool decode_xml(XMLObj *obj) {
    if (!RGWXMLDecoder::decode_xml("NameAndStatus", name, obj, true)) {
      // name is mandatory
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
    return true;
  }
};

struct Items {
  std::list<Item> item_list;
  
  // intrusive XML decoding API
  bool decode_xml(XMLObj *obj) {
    do_decode_xml_obj(item_list, "Item", obj);
    return true;
  }
};

// in case of non-intrusive decoding class
// hierarchy should reflect the XML hierarchy

class NameAndStatusXMLObj: public NameAndStatus, public XMLObj {
public:
  NameAndStatusXMLObj() = default;
  virtual ~NameAndStatusXMLObj() = default;
  
  bool xml_end(const char *el) override {
    // mixing the 2 types
    return decode_xml(this);
  }
};

class ItemXMLObj: public Item, public XMLObj {
public:
  ItemXMLObj() = default;
  virtual ~ItemXMLObj() = default;
  
  bool xml_end(const char *el) override {
    XMLObjIter iter = find("NameAndStatus");
    NameAndStatusXMLObj* _name = static_cast<NameAndStatusXMLObj*>(iter.get_next());
    if (!_name) {
      // name is mandatory
      return false;
    }
    name.name = _name->name;
    name.status = _name->status;
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

    return true;
  }
};

class ItemsXMLObj: public Items, public XMLObj {
public:
  ItemsXMLObj() = default;
  virtual ~ItemsXMLObj() = default;

  bool xml_end(const char *el) override {
    XMLObjIter iter = find("Item");
    ItemXMLObj* item = static_cast<ItemXMLObj*>(iter.get_next());
    // mandatory to have at least one item
    bool item_found = false;
    while (item) {
      item_list.emplace_back(item->name, item->value, item->extra_value);
      item = static_cast<ItemXMLObj*>(iter.get_next());
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
    ss << "((" << item.name.name << "," << item.name.status << ")," << item.value << "," << item.extra_value << ")" << ",";
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

TEST(TestDecoder, MalfomedInput)
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
