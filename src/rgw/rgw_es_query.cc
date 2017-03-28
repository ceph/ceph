#include <list>
#include <map>
#include <string>
#include <iostream>
#include <boost/algorithm/string.hpp>

#include "common/ceph_json.h"
#include "rgw_common.h"
#include "rgw_es_query.h"

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

class ESQueryNode {
protected:
  ESQueryCompiler *compiler;
public:
  ESQueryNode(ESQueryCompiler *_compiler) : compiler(_compiler) {}
  virtual ~ESQueryNode() {}

  virtual bool init(ESQueryStack *s) = 0;

  virtual void dump(Formatter *f) const = 0;

  virtual bool leaf_field_name(string *name) {
    return false;
  }

  virtual void leaf_field_rename(const string& new_name) {}
};

static bool alloc_node(ESQueryCompiler *compiler, ESQueryStack *s, ESQueryNode **pnode);

class ESQueryNode_Bool : public ESQueryNode {
  string op;
  ESQueryNode *first{nullptr};
  ESQueryNode *second{nullptr};
public:
  ESQueryNode_Bool(ESQueryCompiler *compiler) : ESQueryNode(compiler) {}
  ESQueryNode_Bool(ESQueryCompiler *compiler, const string& _op, ESQueryNode *_first, ESQueryNode *_second) :ESQueryNode(compiler), op(_op), first(_first), second(_second) {}
  bool init(ESQueryStack *s) {
    bool valid = s->pop(&op);
    if (!valid) {
      return false;
    }
    valid = alloc_node(compiler, s, &first) &&
      alloc_node(compiler, s, &second);
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
    f->open_array_section(section);
    encode_json("entry", *first, f);
    encode_json("entry", *second, f);
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
  ESQueryNode_Op(ESQueryCompiler *compiler) : ESQueryNode(compiler) {}
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
  bool leaf_field_name(string *name) override {
    *name = field;
    return true;
  }

  void leaf_field_rename(const string& new_name) override {
    field = new_name;
  }
};

class ESQueryNode_Op_Equal : public ESQueryNode_Op {
public:
  ESQueryNode_Op_Equal(ESQueryCompiler *compiler) : ESQueryNode_Op(compiler) {}
  ESQueryNode_Op_Equal(ESQueryCompiler *compiler, const string& f, const string& v) : ESQueryNode_Op(compiler) {
    op = "==";
    field = f;
    val = v;
  }

  virtual void dump(Formatter *f) const {
    f->open_object_section("term");
    encode_json(field.c_str(), val.c_str(), f);
    f->close_section();
  }
};

class ESQueryNode_Op_Range : public ESQueryNode_Op {
  string range_str;
public:
  ESQueryNode_Op_Range(ESQueryCompiler *compiler, const string& rs) : ESQueryNode_Op(compiler), range_str(rs) {}

  virtual void dump(Formatter *f) const {
    f->open_object_section("range");
    f->open_object_section(field.c_str());
    encode_json(range_str.c_str(), val.c_str(), f);
    f->close_section();
    f->close_section();
  }
};

class ESQueryNode_Op_Nested_Parent : public ESQueryNode_Op {
public:
  ESQueryNode_Op_Nested_Parent(ESQueryCompiler *compiler) : ESQueryNode_Op(compiler) {}

  virtual string get_custom_leaf_field_name() = 0;
};

template <class T>
class ESQueryNode_Op_Nested : public ESQueryNode_Op_Nested_Parent {
  string name;
  ESQueryNode *next;
public:
  ESQueryNode_Op_Nested(ESQueryCompiler *compiler, const string& _name, ESQueryNode *_next) : ESQueryNode_Op_Nested_Parent(compiler),
                                                                                              name(_name), next(_next) {}
  ~ESQueryNode_Op_Nested() {
    delete next;
  }

  virtual void dump(Formatter *f) const {
    f->open_object_section("nested");
    string s = string("custom-") + type_str();
    encode_json("path", s.c_str(), f);
    f->open_object_section("query");
    f->open_object_section("bool");
    f->open_array_section("must");
    f->open_object_section("entry");
    f->open_object_section("match");
    string n = s + ".name";
    encode_json(n.c_str(), name.c_str(), f);
    f->close_section();
    f->close_section();
    encode_json("entry", *next, f);
    f->close_section();
    f->close_section();
    f->close_section();
    f->close_section();
  }

  string type_str() const;
  string get_custom_leaf_field_name() {
    return string("meta.custom-") + type_str() + ".value";
  }
};

template<>
string ESQueryNode_Op_Nested<string>::type_str() const {
  return "string";
}

template<>
string ESQueryNode_Op_Nested<int64_t>::type_str() const {
  return "int";
}

template<>
string ESQueryNode_Op_Nested<ceph::real_time>::type_str() const {
  return "date";
}

static bool is_bool_op(const string& str)
{
  return (str == "or" || str == "and");
}

static bool alloc_node(ESQueryCompiler *compiler, ESQueryStack *s, ESQueryNode **pnode)
{
  string op;
  bool valid = s->peek(&op);
  if (!valid) {
    return false;
  }

  ESQueryNode *node;

  if (is_bool_op(op)) {
    node = new ESQueryNode_Bool(compiler);
  } else if (op == "==") {
    node = new ESQueryNode_Op_Equal(compiler);
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

    node = new ESQueryNode_Op_Range(compiler, iter->second);
  }

  if (!node->init(s)) {
    delete node;
    return false;
  }
  string field_name;
  string custom_prefix = compiler->get_custom_prefix();
  if (node->leaf_field_name(&field_name) &&
      boost::algorithm::starts_with(field_name, custom_prefix)) {
    ESQueryNode *leaf_node = node;
    field_name = field_name.substr(custom_prefix.size());
    ESEntityTypeMap::EntityType entity_type = ESEntityTypeMap::ES_ENTITY_NONE;
    auto m = compiler->get_custom_type_map();
    if (m) {
      entity_type = m->find(field_name);
    }

    ESQueryNode_Op_Nested_Parent *new_node;
    switch (entity_type) {
      case ESEntityTypeMap::ES_ENTITY_INT:
        new_node = new ESQueryNode_Op_Nested<int64_t>(compiler, field_name, node);
        break;
      case ESEntityTypeMap::ES_ENTITY_DATE:
        new_node = new ESQueryNode_Op_Nested<ceph::real_time>(compiler, field_name, node);
        break;
      default:
        new_node = new ESQueryNode_Op_Nested<string>(compiler, field_name, node);
    }
    
    leaf_node->leaf_field_rename(new_node->get_custom_leaf_field_name());

    node = new_node;
  }
  *pnode = node;
  return true;
}


bool is_key_char(char c)
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

void ESInfixQueryParser::skip_whitespace(const char *str, int size, int& pos) {
  while (pos < size && isspace(str[pos])) {
    ++pos;
  }
}

bool ESInfixQueryParser::get_next_token(bool (*filter)(char)) {
  skip_whitespace(str, size, pos);
  int token_start = pos;
  while (pos < size && filter(str[pos])) {
    ++pos;
  }
  if (pos == token_start) {
    return false;
  }
  string token = string(str + token_start, pos - token_start);
  args.push_back(token);
  return true;
}

bool ESInfixQueryParser::parse_condition() {
  /*
   * condition: <key> <operator> <val>
   *
   * whereas key: needs to conform to http header field restrictions
   *         operator: one of the following: < <= == >= >
   *         val: ascii, terminated by either space or ')' (or end of string)
   */

  /* parse key */
  bool valid = get_next_token(is_key_char) &&
    get_next_token(is_op_char) &&
    get_next_token(is_val_char);

  if (!valid) {
    return false;
  }

  return true;
}

bool ESInfixQueryParser::parse_and_or() {
  skip_whitespace(str, size, pos);
  if (pos + 3 <= size && strncmp(str + pos, "and", 3) == 0) {
    pos += 3;
    args.push_back("and");
    return true;
  }

  if (pos + 2 <= size && strncmp(str + pos, "or", 2) == 0) {
    pos += 2;
    args.push_back("or");
    return true;
  }

  return false;
}

bool ESInfixQueryParser::parse_specific_char(const char *pchar) {
  skip_whitespace(str, size, pos);
  if (pos >= size) {
    return false;
  }
  if (str[pos] != *pchar) {
    return false;
  }

  args.push_back(pchar);
  ++pos;
  return true;
}

bool ESInfixQueryParser::parse_open_bracket() {
  return parse_specific_char("(");
}

bool ESInfixQueryParser::parse_close_bracket() {
  return parse_specific_char(")");
}

bool ESInfixQueryParser::parse(list<string> *result) {
  /*
   * expression: [(]<condition>[[and/or]<condition>][)][and/or]...
   */

  while (pos < size) {
    parse_open_bracket();
    if (!parse_condition()) {
      return false;
    }
    parse_close_bracket();
    parse_and_or();
  }

  result->swap(args);

  return true;
}

bool ESQueryCompiler::convert(list<string>& infix) {
  list<string> prefix;
  if (!infix_to_prefix(infix, &prefix)) {
    return false;
  }
  stack.assign(prefix);
  if (!alloc_node(this, &stack, &query_root)) {
    return false;
  }
  if (!stack.done()) {
    return false;
  }
  return true;
}

ESQueryCompiler::~ESQueryCompiler() {
  delete query_root;
}

bool ESQueryCompiler::compile() {
  list<string> infix;
  if (!parser.parse(&infix)) {
    return false;
  }

  if (!convert(infix)) {
    return false;
  }

  for (auto& c : eq_conds) {
    ESQueryNode_Op_Equal *eq_node = new ESQueryNode_Op_Equal(this, c.first, c.second);
    query_root = new ESQueryNode_Bool(this, "and", eq_node, query_root);
  }

  return true;
}

void ESQueryCompiler::dump(Formatter *f) const {
  encode_json("query", *query_root, f);
}

