#include <list>
#include <map>
#include <string>
#include <iostream>

#include "common/ceph_json.h"
#include "rgw_common.h"

using namespace std;

bool pop_front(list<string>& l, string *s)
{
  if (l.empty()) {
    return false;
  }
  *s = l.front();
  l.pop_front();
  return true;
}

map<string, int> operator_map = {
  { "or",  1 },
  { "and", 2 },
  { "<",   3 },
  { "<=",  3 },
  { "==",  3 },
  { ">=",  3 },
  { ">",   3 },
};

bool is_operator(const string& s)
{
  return (operator_map.find(s) != operator_map.end());
}

int operand_value(const string& op)
{
  auto i = operator_map.find(op);
  if (i == operator_map.end()) {
    return 0;
  }

  return i->second;
}

int check_precedence(const string& op1, const string& op2)
{
  return operand_value(op1) - operand_value(op2);
}

static bool infix_to_prefix(list<string>& source, list<string> *out)
{
  list<string> operator_stack;
  list<string> operand_stack;

  operator_stack.push_front("(");
  source.push_back(")");

  for (string& entity : source) {
    if (entity == "(") {
      operator_stack.push_front(entity);
    } else if (entity == ")") {
      string popped_operator;
      if (!pop_front(operator_stack, &popped_operator)) {
        return false;
      }

      while (popped_operator != "(") {
        operand_stack.push_front(popped_operator);
        if (!pop_front(operator_stack, &popped_operator)) {
          return false;
        }
      }

    } else if (is_operator(entity)) {
      string popped_operator;
      if (!pop_front(operator_stack, &popped_operator)) {
        return false;
      }

      int precedence = check_precedence(popped_operator, entity);

      while (precedence >= 0) {
        operand_stack.push_front(popped_operator);
        if (!pop_front(operator_stack, &popped_operator)) {
          return false;
        }
        precedence = check_precedence(popped_operator, entity);
      }

      operator_stack.push_front(popped_operator);
      operator_stack.push_front(entity);
    } else {
      operand_stack.push_front(entity);
    }

  }

  if (!operator_stack.empty()) {
    return false;
  }

  out->swap(operand_stack);
  return true;
}

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

class ESQueryNode {
public:
  ESQueryNode() {}
  virtual ~ESQueryNode() {}

  virtual bool init(ESQueryStack *s) = 0;

  virtual void dump(Formatter *f) const = 0;
};

static bool alloc_node(ESQueryStack *s, ESQueryNode **pnode);

class ESQueryNode_Bool : public ESQueryNode {
  string op;
  ESQueryNode *first{nullptr};
  ESQueryNode *second{nullptr};
public:
  ESQueryNode_Bool() {}
  bool init(ESQueryStack *s) {
    bool valid = s->pop(&op);
    if (!valid) {
      return false;
    }
    valid = alloc_node(s, &first) &&
      alloc_node(s, &second);
    if (!valid) {
      return false;
    }
    return true;
  }
  virtual ~ESQueryNode_Bool() {
    delete first;
    delete second;
  }

  void dump(Formatter *f) const {
    f->open_object_section("bool");
    const char *section = (op == "and" ? "must" : "should");
    f->open_object_section(section);
    first->dump(f);
    second->dump(f);
    f->close_section();
    f->close_section();
  }
};

class ESQueryNode_Op : public ESQueryNode {
protected:
  string op;
  string field;
  string val;
public:
  ESQueryNode_Op() {}
  bool init(ESQueryStack *s) {
    bool valid = s->pop(&op) &&
      s->pop(&val) &&
      s->pop(&field);
    if (!valid) {
      return false;
    }
    return true;
  }

  virtual void dump(Formatter *f) const = 0;
};

class ESQueryNode_Op_Equal : public ESQueryNode_Op {
public:
  ESQueryNode_Op_Equal() {}

  virtual void dump(Formatter *f) const {
    f->open_object_section("term");
    encode_json(field.c_str(), val.c_str(), f);
    f->close_section();
  }
};

class ESQueryNode_Op_Range : public ESQueryNode_Op {
  string range_str;
public:
  ESQueryNode_Op_Range(const string& rs) : range_str(rs) {}

  virtual void dump(Formatter *f) const {
    f->open_object_section("range");
    f->open_object_section(field.c_str());
    encode_json(range_str.c_str(), val.c_str(), f);
    f->close_section();
    f->close_section();
  }
};

static bool is_bool_op(const string& str)
{
  return (str == "or" || str == "and");
}

static bool alloc_node(ESQueryStack *s, ESQueryNode **pnode)
{
  string op;
  bool valid = s->peek(&op);
  if (!valid) {
    return false;
  }

  ESQueryNode *node;

  if (is_bool_op(op)) {
    node = new ESQueryNode_Bool();
  } else if (op == "==") {
    node = new ESQueryNode_Op_Equal();
  } else {
    static map<string, string> range_op_map = {
      { "<", "lt"},
      { "<=", "lte"},
      { ">=", "gte"},
      { ">", "gt"},
    };

    auto iter = range_op_map.find(op);
    if (iter == range_op_map.end()) {
      return false;
    }

    node = new ESQueryNode_Op_Range(iter->second);
  }

  if (!node->init(s)) {
    delete node;
    return false;
  }
  *pnode = node;
  return true;
}

class ESQuery {
  ESQueryStack stack;
  ESQueryNode *query_root{nullptr};
public:
  ESQuery() {}
  ~ESQuery() {
    delete query_root;
  }
  
  bool init(list<string>& infix) {
    list<string> prefix;
    if (!infix_to_prefix(infix, &prefix)) {
      return false;
    }
    stack.assign(prefix);
    if (!alloc_node(&stack, &query_root)) {
      return false;
    }
    if (!stack.done()) {
      return false;
    }
    return true;
  }

  void dump(Formatter *f) const {
    encode_json("query", *query_root, f);
  }
};

void skip_whitespace(const char *str, int size, int& pos)
{
  while (pos < size && isspace(str[pos])) {
    ++pos;
  }
}

static bool is_key_char(char c)
{
  switch (c) {
    case '(':
    case ')':
    case '<':
    case '>':
    case '@':
    case ',':
    case ';':
    case ':':
    case '\\':
    case '"':
    case '/':
    case '[':
    case ']':
    case '?':
    case '=':
    case '{':
    case '}':
    case ' ':
    case '\t':
      return false;
  };
  return (isascii(c) > 0);
}

static bool is_op_char(char c)
{
  switch (c) {
    case '<':
    case '=':
    case '>':
      return true;
  };
  return false;
}

static bool is_val_char(char c)
{
  if (isspace(c)) {
    return false;
  }
  return (c != ')');
}

static bool get_next_token(const char *str, int size, int& pos, list<string> *args, bool (*filter)(char))
{
  skip_whitespace(str, size, pos);
  int token_start = pos;
  while (pos < size && filter(str[pos])) {
    ++pos;
  }
  if (pos == token_start) {
    return false;
  }
  string token = string(str + token_start, pos - token_start);
  args->push_back(token);
  return true;
}

static bool parse_condition(const char *str, int size, int& pos, list<string> *args)
{
  /*
   * condition: <key> <operator> <val>
   *
   * whereas key: needs to conform to http header field restrictions
   *         operator: one of the following: < <= == >= >
   *         val: ascii, terminated by either space or ')' (or end of string)
   */

  /* parse key */
  bool valid = get_next_token(str, size, pos, args, is_key_char) &&
    get_next_token(str, size, pos, args, is_op_char) &&
    get_next_token(str, size, pos, args, is_val_char);

  if (!valid) {
    return false;
  }

  return true;
}

static bool parse_and_or(const char *str, int size, int& pos, list<string> *args)
{
  skip_whitespace(str, size, pos);
  if (pos + 3 <= size && strncmp(str + pos, "and", 3) == 0) {
    pos += 3;
    args->push_back("and");
    return true;
  }

  if (pos + 2 <= size && strncmp(str + pos, "or", 2) == 0) {
    pos += 2;
    args->push_back("or");
    return true;
  }

  return false;
}

static bool parse_specific_char(const char *str, int size, int& pos, list<string> *args, const char *pchar)
{
  skip_whitespace(str, size, pos);
  if (pos >= size) {
    return false;
  }
  if (str[pos] != *pchar) {
    return false;
  }

  args->push_back(pchar);
  ++pos;
  return true;
}

static bool parse_open_bracket(const char *str, int size, int& pos, list<string> *args)
{
  return parse_specific_char(str, size, pos, args, "(");
}

static bool parse_close_bracket(const char *str, int size, int& pos, list<string> *args)
{
  return parse_specific_char(str, size, pos, args, ")");
}

static bool parse_expression(const string& s, list<string> *args)
{
  /*
   * expression: [(]<condition>[[and/or]<condition>][)][and/or]...
   */

  int pos = 0;
  int size = s.size();
  const char *str = s.c_str();

  while (pos < size) {
    parse_open_bracket(str, size, pos, args);
    if (!parse_condition(str, size, pos, args)) {
      return false;
    }
    parse_close_bracket(str, size, pos, args);
    parse_and_or(str, size, pos, args);
  }

  return true;
}


int main(int argc, char *argv[])
{
  list<string> infix;

  string expr;

  if (argc > 1) {
    expr = argv[1];
  } else {
    expr = "age >= 30";
  }

  if (!parse_expression(expr, &infix)) {
    cout << "ERROR: failed to parse : " << expr << std::endl;
    return EINVAL;
  }

  ESQuery es_query;
  
  bool valid = es_query.init(infix);
  if (!valid) {
    cout << "invalid query, failed generating request json" << std::endl;
    return EINVAL;
  }

  JSONFormatter f;
  encode_json("root", es_query, &f);

  f.flush(cout);

  return 0;
}

