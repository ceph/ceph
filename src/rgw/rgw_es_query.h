#ifndef CEPH_RGW_ES_QUERY_H
#define CEPH_RGW_ES_QUERY_H

class ESQueryStack {
  list<string> l;
  list<string>::iterator iter;

public:
  ESQueryStack(list<string>& src) {
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
  ESInfixQueryParser(const string& _query) : query(_query), size(query.size()), str(query.c_str()) {}
  bool parse(list<string> *result);
};

class ESQueryNode;

struct ESEntityTypeMap {
  enum EntityType {
    ES_ENTITY_NONE,
    ES_ENTITY_STR,
    ES_ENTITY_INT,
    ES_ENTITY_DATE,
  };

  map<string, EntityType> m;

  ESEntityTypeMap(map<string, EntityType>& _m) : m(_m) {}

  EntityType find(const string& entity) {
    auto i = m.find(entity);
    if (i != m.end()) {
      return i->second;
    }

    return ES_ENTITY_NONE;
  }
};

class ESQueryCompiler {
  ESInfixQueryParser parser;
  ESQueryStack stack;
  ESQueryNode *query_root{nullptr};

  string custom_prefix;

  bool convert(list<string>& infix);

  list<pair<string, string> > eq_conds;

  ESEntityTypeMap *custom_type_map{nullptr};

public:
  ESQueryCompiler(const string& query, list<pair<string, string> > *prepend_eq_conds, const string& _custom_prefix) : parser(query), custom_prefix(_custom_prefix) {
    if (prepend_eq_conds) {
      eq_conds = std::move(*prepend_eq_conds);
    }
  }
  ~ESQueryCompiler();

  bool compile();
  void dump(Formatter *f) const;
  
  const string& get_custom_prefix() { return custom_prefix; }

  void set_custom_type_map(ESEntityTypeMap *entity_map) {
    custom_type_map = entity_map;
  }

  ESEntityTypeMap *get_custom_type_map() {
    return custom_type_map;
  }
};


#endif
