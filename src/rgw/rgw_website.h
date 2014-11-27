#ifndef RGW_WEBSITE_H
#define RGW_WEBSITE_H

struct RGWRedirectInfo
{
  string protocol;
  string hostname;
  uint16_t http_redirect_code;

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    ::encode(protocol, bl);
    ::encode(hostname, bl);
    ::encode(http_redirect_code, bl);
    ENCODE_FINISH(bl);
  }
  void decode(bufferlist::iterator& bl) {
    DECODE_START(1, bl);
    ::decode(protocol, bl);
    ::decode(hostname, bl);
    ::decode(http_redirect_code, bl);
    DECODE_FINISH(bl);
  }

  void dump(Formatter *f) const;
  void decode_json(JSONObj *obj);
};
WRITE_CLASS_ENCODER(RGWRedirectInfo)


struct RGWBWRedirectInfo
{
  RGWRedirectInfo redirect;
  string replace_key_prefix_with;
  string replace_key_with;

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    ::encode(redirect, bl);
    ::encode(replace_key_prefix_with, bl);
    ::encode(replace_key_with, bl);
    ENCODE_FINISH(bl);
  }
  void decode(bufferlist::iterator& bl) {
    DECODE_START(1, bl);
    ::decode(redirect, bl);
    ::decode(replace_key_prefix_with, bl);
    ::decode(replace_key_with, bl);
    DECODE_FINISH(bl);
  }

  void dump(Formatter *f) const;
  void decode_json(JSONObj *obj);
};
WRITE_CLASS_ENCODER(RGWBWRedirectInfo)

struct RGWBWRoutingRuleCondition
{
  string key_prefix_equals;
  uint16_t http_error_code_returned_equals;

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    ::encode(key_prefix_equals, bl);
    ::encode(http_error_code_returned_equals, bl);
    ENCODE_FINISH(bl);
  }
  void decode(bufferlist::iterator& bl) {
    DECODE_START(1, bl);
    ::decode(key_prefix_equals, bl);
    ::decode(http_error_code_returned_equals, bl);
    DECODE_FINISH(bl);
  }

  void dump(Formatter *f) const;
  void decode_json(JSONObj *obj);

  bool check_key_condition(const string& key);
  bool check_error_code_condition(int error_code) {
    return (uint16_t)error_code == http_error_code_returned_equals;
  }
};
WRITE_CLASS_ENCODER(RGWBWRoutingRuleCondition)

struct RGWBWRoutingRule
{
  RGWBWRoutingRuleCondition condition;
  RGWBWRedirectInfo redirect_info;

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    ::encode(condition, bl);
    ::encode(redirect_info, bl);
    ENCODE_FINISH(bl);
  }
  void decode(bufferlist::iterator& bl) {
    DECODE_START(1, bl);
    ::decode(condition, bl);
    ::decode(redirect_info, bl);
    DECODE_FINISH(bl);
  }

  void dump(Formatter *f) const;
  void decode_json(JSONObj *obj);

  bool check_key_condition(const string& key) {
    return condition.check_key_condition(key);
  }
  bool check_error_code_condition(int error_code) {
    return condition.check_error_code_condition(error_code);
  }
};
WRITE_CLASS_ENCODER(RGWBWRoutingRule)

struct RGWBWRoutingRules
{
  list<RGWBWRoutingRule> rules;

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    ::encode(rules, bl);
    ENCODE_FINISH(bl);
  }
  void decode(bufferlist::iterator& bl) {
    DECODE_START(1, bl);
    ::decode(rules, bl);
    DECODE_FINISH(bl);
  }

  void dump(Formatter *f) const;
  void decode_json(JSONObj *obj);

  bool check_key_condition(const string& key, RGWBWRoutingRule **rule);
  bool check_error_code_condition(int error_code, RGWBWRoutingRule **rule);
};
WRITE_CLASS_ENCODER(RGWBWRoutingRules)

struct RGWBucketWebsiteConf
{
  RGWRedirectInfo redirect_all;
  string index_doc_suffix;
  string error_doc;
  RGWBWRoutingRules routing_rules;

  RGWBucketWebsiteConf() {}

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    ::encode(index_doc_suffix, bl);
    ::encode(error_doc, bl);
    ::encode(routing_rules, bl);
    ::encode(redirect_all, bl);
    ENCODE_FINISH(bl);
  }
  void decode(bufferlist::iterator& bl) {
    DECODE_START(1, bl);
    ::decode(index_doc_suffix, bl);
    ::decode(error_doc, bl);
    ::decode(routing_rules, bl);
    ::decode(redirect_all, bl);
    DECODE_FINISH(bl);
  }

  void dump(Formatter *f) const;
  void decode_json(JSONObj *obj);

  void get_effective_target(const string& key, string *effective_key, RGWRedirectInfo *redirect);
};
WRITE_CLASS_ENCODER(RGWBucketWebsiteConf)

#endif
