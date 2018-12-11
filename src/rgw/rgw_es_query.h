// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_RGW_ES_QUERY_H
#define CEPH_RGW_ES_QUERY_H

#include "rgw_string.h"

class ESQueryStack {
  list<string> l;
  list<string>::iterator iter;

public:
  explicit ESQueryStack(list<string>& src) {
    assign(src);
  }

  ESQueryStack() {}

  void assign(list<string>& src) {
    l.swap(src);
    iter = l.begin();
  }

  bool peek(string *dest) {
    if (done()) {
      return false;
    }
    *dest = *iter;
    return true;
  }

  bool pop(string *dest) {
    bool valid = peek(dest);
    if (!valid) {
      return false;
    }
    ++iter;
    return true;
  }

  bool done() {
    return (iter == l.end());
  }
};

class ESInfixQueryParser {
  string query;
  int size;
  const char *str;
  int pos{0};
  list<string> args;

  void skip_whitespace(const char *str, int size, int& pos);
  bool get_next_token(bool (*filter)(char));

  bool parse_condition();
  bool parse_and_or();
  bool parse_specific_char(const char *pchar);
  bool parse_open_bracket();
  bool parse_close_bracket();

public:
  explicit ESInfixQueryParser(const string& _query) : query(_query), size(query.size()), str(query.c_str()) {}
  bool parse(list<string> *result);
};

class ESQueryNode;

struct ESEntityTypeMap {
  enum EntityType {
    ES_ENTITY_NONE = 0,
    ES_ENTITY_STR  = 1,
    ES_ENTITY_INT  = 2,
    ES_ENTITY_DATE = 3,
  };

  map<string, EntityType> m;

  explicit ESEntityTypeMap(map<string, EntityType>& _m) : m(_m) {}

  bool find(const string& entity, EntityType *ptype) {
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
  ESQueryStack stack;
  ESQueryNode *query_root{nullptr};

  string custom_prefix;

  bool convert(list<string>& infix, string *perr);

  list<pair<string, string> > eq_conds;

  ESEntityTypeMap *generic_type_map{nullptr};
  ESEntityTypeMap *custom_type_map{nullptr};

  map<string, string, ltstr_nocase> *field_aliases = nullptr;
  set<string> *restricted_fields = nullptr;

public:
  ESQueryCompiler(const string& query, list<pair<string, string> > *prepend_eq_conds, const string& _custom_prefix) : parser(query), custom_prefix(_custom_prefix) {
    if (prepend_eq_conds) {
      eq_conds = std::move(*prepend_eq_conds);
    }
  }
  ~ESQueryCompiler();

  bool compile(string *perr);
  void dump(Formatter *f) const;
  
  void set_generic_type_map(ESEntityTypeMap *entity_map) {
    generic_type_map = entity_map;
  }

  ESEntityTypeMap *get_generic_type_map() {
    return generic_type_map;
  }
  const string& get_custom_prefix() { return custom_prefix; }

  void set_custom_type_map(ESEntityTypeMap *entity_map) {
    custom_type_map = entity_map;
  }

  ESEntityTypeMap *get_custom_type_map() {
    return custom_type_map;
  }

  void set_field_aliases(map<string, string, ltstr_nocase> *fa) {
    field_aliases = fa;
  }

  string unalias_field(const string& field) {
    if (!field_aliases) {
      return field;
    }
    auto i = field_aliases->find(field);
    if (i == field_aliases->end()) {
      return field;
    }

    return i->second;
  }

  void set_restricted_fields(set<string> *rf) {
    restricted_fields = rf;
  }

  bool is_restricted(const string& f) {
    return (restricted_fields && restricted_fields->find(f) != restricted_fields->end());
  }
};


#endif
