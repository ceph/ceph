// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#pragma once

#include <map>
#include <set>
#include <string>
#include <vector>
#include <utility>

#include "rgw_string.h"

class ESInfixQueryParser {
  std::string query;
  int size;
  const char *str;
  int pos{0};
  std::vector<std::string> args;

  void skip_whitespace(const char *str, int size, int& pos);
  bool get_next_token(bool (*filter)(char));

  bool parse_condition();
  bool parse_and_or();
  bool parse_specific_char(const char *pchar);
  bool parse_open_bracket();
  bool parse_close_bracket();

public:
  explicit ESInfixQueryParser(const std::string& _query) : query(_query), size(query.size()), str(query.c_str()) {}
  bool parse(std::vector<std::string>& result);
};

class ESQueryNode;

struct ESEntityTypeMap {
  enum EntityType {
    ES_ENTITY_NONE = 0,
    ES_ENTITY_STR  = 1,
    ES_ENTITY_INT  = 2,
    ES_ENTITY_DATE = 3
  };

  std::map<std::string, EntityType> m;

  explicit ESEntityTypeMap(std::map<std::string, EntityType>& _m) : m(_m) {}

  bool find(const std::string& entity, EntityType *ptype) {
    auto i = m.find(entity);
    if (i != m.end()) {
      *ptype = i->second;
      return true;
    }

    *ptype = ES_ENTITY_NONE;
    return false;
  }
};

class ESQueryCompiler {
  ESInfixQueryParser parser;
  ESQueryNode *query_root{nullptr};

  std::string custom_prefix;
  std::vector<std::pair<std::string, std::string>> eq_conds;

  ESEntityTypeMap *generic_type_map{nullptr};
  ESEntityTypeMap *custom_type_map{nullptr};

  std::map<std::string, std::string, ltstr_nocase> *field_aliases = nullptr;
  std::set<std::string> *restricted_fields = nullptr;

  bool convert(std::vector<std::string> infix, std::string *perr);

public:
  ESQueryCompiler(
    const std::string& query,
    std::vector<std::pair<std::string, std::string>> prepend_eq_conds,
    const std::string& _custom_prefix)
    : parser(query),
      custom_prefix(_custom_prefix),
      eq_conds(std::move(prepend_eq_conds)) {}

  ~ESQueryCompiler();

  bool compile(std::string *perr);
  void dump(Formatter *f) const;
  
  void set_generic_type_map(ESEntityTypeMap *entity_map) {
    generic_type_map = entity_map;
  }

  ESEntityTypeMap *get_generic_type_map() {
    return generic_type_map;
  }
  const std::string& get_custom_prefix() { return custom_prefix; }

  void set_custom_type_map(ESEntityTypeMap *entity_map) {
    custom_type_map = entity_map;
  }

  ESEntityTypeMap *get_custom_type_map() {
    return custom_type_map;
  }

  void set_field_aliases(std::map<std::string, std::string, ltstr_nocase> *fa) {
    field_aliases = fa;
  }

  std::string unalias_field(const std::string& field) {
    if (!field_aliases) {
      return field;
    }
    auto i = field_aliases->find(field);
    if (i == field_aliases->end()) {
      return field;
    }

    return i->second;
  }

  void set_restricted_fields(std::set<std::string> *rf) {
    restricted_fields = rf;
  }

  bool is_restricted(const std::string& f) {
    return (restricted_fields && restricted_fields->find(f) != restricted_fields->end());
  }
};
