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

class ESQueryCompiler {
  ESInfixQueryParser parser;
  ESQueryStack stack;
  ESQueryNode *query_root{nullptr};

  bool convert(list<string>& infix);

  list<pair<string, string> > eq_conds;

public:
  ESQueryCompiler(const string& query, list<pair<string, string> > *prepend_eq_conds) : parser(query) {
    if (prepend_eq_conds) {
      eq_conds = std::move(*prepend_eq_conds);
    }
  }
  ~ESQueryCompiler();

  bool compile();
  void dump(Formatter *f) const;
};


#endif
