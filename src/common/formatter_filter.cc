// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <boost/program_options/variables_map.hpp>
#include <boost/program_options/parsers.hpp>

#include <stdio.h>
#include <string.h>
#include <iostream>
#include <time.h>
#include <fcntl.h>
#include <unistd.h>
#include "global/global_init.h"
#include "common/ceph_argparse.h"
#include "include/stringify.h"
#include "common/errno.h"
#include "common/safe_io.h"

#include "os/bluestore/BlueFS.h"
#include "os/bluestore/BlueStore.h"
#include "common/admin_socket.h"
#include "common/url_escape.h"

#include "formatter_filter.h"
/*
  expression:= literal ||
               number ||
	       variable ||
	       ( expression ) ||
	       LOP expression ||
               expression OP expression ||
	       expression ROP
term


OP term OP OP term OP expression
3             4  3             2
reduce
              shift
                 shift         reduce
                 reduce
              reduce

literal -> expression
number -> expression
variable -> expression
( expression ) -> expression
LOP expression -> expression
expression OP expression -> expression

! == != > < >= <=
&& ||
has
+ - * / %

Variables are set when encountered in formatter, and never unset.
Condition is checked every time a variable is set.
*/


namespace ceph {
namespace formatter_filter {

  operator_spec_t operators[op_last] =
    {
      {"==", 2, true, false},
      {"!=", 2, true, false},
      {">=", 2, true, false},
      {"<=", 2, true, false},
      {">", 2, true, false},
      {"<", 2, true, false},
      {"&&", 3, true, false},
      {"||", 3, true, false},
      {"has", 0, true, false},
      {"exists", 0, false, true},
      {"+", 1, true, false},
      {"-", 1, true, true},
      {"*", 0, true, false},
      {"/", 0, true, false},
      {"%", 0, true, false},
      {"!", 0, false, true}
    };

  /* splits input into stream of tokens */
  void TokenSplitter::parse_one() {
    skip_whitespace();
    if (left.empty()) {
      token = eEnd;
      return;
    }
    peeked = left;
    switch (peeked.front()) {
    case '"':
      {
	//literal
	token_literal.clear();
	peeked = peeked.substr(1);
	while (!peeked.empty()) {
	  switch (peeked.front()) {
	  case '"':
	    token = eLiteral;
	    peeked = peeked.substr(1);
	    return;
	  case '\\':
	    peeked = peeked.substr(1);
	    if (peeked.empty()) {
	      token = eError;
	      return;
	    }
	    token_literal.push_back(peeked.front());
	    peeked = peeked.substr(1);
	    break;
	  default:
	    token_literal.push_back(peeked.front());
	    peeked = peeked.substr(1);
	    break;
	  }
	}
	token = eError;
	break;
      }
    case '0' ... '9':
      {
	//number
	char* endptr;
	int64_t v = strtoll(&peeked[0],  &endptr, 0);
	if (endptr == &peeked.front()) {
	  /* this is actually possible, if input is "0x" */
	  token = eError;
	  break;
	}
	token = eNumber;
	token_number = v;
	peeked = left.substr(endptr - &peeked.front());
	break;
      }
    case '(':
      {
	token = eLeftBracket;
	peeked = left.substr(1);
	break;
      }
    case ')':
      {
	token = eRightBracket;
	peeked = left.substr(1);
	break;
      }
    case '.':
      {
	//variable
	size_t cnt = 0;
	while (cnt < peeked.size()) {
	  switch (peeked[cnt]) {
	  case 'A' ... 'Z':
	  case 'a' ... 'z':
	  case '0' ... '9':
	  case '_':
	  case '.':
	    {
	      cnt++;
	      continue;
	    }
	  default:
	    break;
	  }
	  break;
	}
	token_variable = peeked.substr(0, cnt);
	peeked = peeked.substr(cnt);
	token = eVariable;
	break;
      }
    default:
      {
	//this must be operator
	std::string_view cand;
	for (size_t i = 1; i <= std::min<size_t>(6, left.size()); i++) {
	  std::string_view cand_n = left.substr(0, i);
	  if (possibly_operator(cand_n) < 1) {
	    //longer will not be operator
	    break;
	  };
	  cand = cand_n;
	}
	auto [op, prio] = get_operator(cand);
	if (op != op_last) {
	  token_op = op;
	  token_op_prio = prio;
	  token = eOperator;
	  peeked = peeked.substr(cand.size());
	} else {
	  token = eError;
	}
      }
    }
  }

  token_t TokenSplitter::peek() {
    parse_one();
    return token;
  }
  void TokenSplitter::consume() {
    ceph_assert(left != peeked);
    left = peeked;
  }
  std::string TokenSplitter::get_variable() {
    ceph_assert(token == eVariable);
    return token_variable;
  }
  uint64_t TokenSplitter::get_number() {
    ceph_assert(token == eNumber);
    return token_number;
  }
  std::string TokenSplitter::get_literal() {
    ceph_assert(token == eLiteral);
    return token_literal;
  }
  operator_t TokenSplitter::get_operator() {
    ceph_assert(token == eOperator);
    return token_op;
  }
  int8_t TokenSplitter::get_operator_prio() {
    ceph_assert(token == eOperator);
    return token_op_prio;
  }
  void TokenSplitter::skip_whitespace() {
    while (!left.empty() &&
	   (left.front() == ' ' ||
	    left.front() == '\n' ||
	    left.front() == '\t')) {
      left = left.substr(1);
    }
  }
  std::tuple<operator_t, uint8_t> TokenSplitter::get_operator(std::string_view cand) {
    for (size_t i = 0; i < op_last; i++) {
      if (cand == operators[i].name) {
	operator_t op = static_cast<operator_t>(i);
	return std::make_tuple(op, operators[i].prio);
      }
    }
    return std::make_tuple(op_last, 255);
  }
  size_t TokenSplitter::possibly_operator(std::string_view cand) {
    size_t cnt = 0;
    for (size_t i = 0; i < op_last; i++) {
      if (cand == operators[i].name.substr(0, cand.size())) {
	cnt++;
      }
    }
    return cnt;
  }
  void TokenSplitter::set_input(std::string_view str) {
    input = str;
    left = str;
    token = eError;
  }

  bool TokenSplitter::show_error(std::ostream& out,
				 const std::string& error)
  {
    size_t consumed = input.size() - left.size();
    std::string_view s = input.substr(0, consumed + 20);
    out << s << std::endl;
    out << std::string(consumed, ' ') << "^ " << error << std::endl;
    return true;
  }


  Exp::Value Exp::cast_to(variant_t target, const Value& right) {
    if (right.index() == einv) {
      return right;
    }
    switch (target) {
    case einv:
      return inv();
    case eint:
      switch (right.index()) {
      case eint:
	return right;
      case estring:
	{
	  const std::string& str = *std::get<pstring>(right);
	  char* endptr;
	  int64_t v = strtoll(str.c_str(),  &endptr, 0);
	  if (*endptr != '\0') {
	    return Value{inv()};
	  }
	  return Value{v};
	}
      case ebool:
	return Value{int64_t(std::get<bool>(right) ? 1 : 0) };
      }
    case estring:
      switch (right.index()) {
      case eint:
	{
	  //std::string* p = new std::string;
	  std::string p = std::to_string(std::get<int64_t>(right));
	  pstring pp = std::make_shared<std::string>(p);
	  return Value{pp};
	}
      case estring:
	return right;
      case ebool:
	{
	  //	  std::string* p = new std::string;
	  std::string p = std::get<bool>(right) ? "1"s : "0"s;
	  pstring pp = std::make_shared<std::string>(p);
	  return Value{pp};
	}
      }
    case ebool:
      switch (right.index()) {
      case eint:
	return Value{std::get<int64_t>(right) != 0 ? true : false};
      case estring:
	{
	  std::string& s = *std::get<pstring>(right);
	  if (s == "0" || s == "false")
	    return Value{false};
	  if (s == "1" || s == "true")
	    return Value{true};
	  return Value{inv()};
	}
      case ebool:
	return right;
      }
    }
    return {inv()};
  }


  class ExpLiteral : public Exp {
  public:
    ExpLiteral(const std::string& literal)
      : literal(std::make_shared<std::string>(literal)) {};
    Value get_value() override {
      return {literal};
    }
  private:
    pstring literal;
  };

  class ExpNumber : public Exp {
  public:
    ExpNumber(uint64_t value)
      : value(value) {};
    Value get_value() override {
      return {(int64_t)value};
    }
  private:
    uint64_t value;
  };

  class ExpVariable : public Exp {
  public:
    ExpVariable(std::shared_ptr<std::string>& value)
      : value(value) {};
    Value get_value() override {
      if (!value) {
	return {inv()};
      } else {
	return {value};
      }
    }
  private:
    std::shared_ptr<std::string>& value;
  };



  class BinaryExp : public Exp {
    ExpRef left;
    ExpRef right;
    operator_t op;
  public:
    BinaryExp(ExpRef left, operator_t op, ExpRef right)
      :left(left), right(right), op(op) {}

    Value get_value() override
    {
      switch (op) {
      case eq:
      case neq:
      case gt:
      case le:
      case lt:
      case ge:
	{
	  Value l = left->get_value();
	  Value r = cast_to(static_cast<variant_t>(l.index()), right->get_value());
	  switch (op) {
	  case eq:
	  case neq:
	    {
	      bool b = false;
	      switch (r.index()) {
	      case einv:
		return r;
	      case eint:
		b = std::get<int64_t>(l) == std::get<int64_t>(r);
		break;
	      case estring:
		b = *std::get<pstring>(l) == *std::get<pstring>(r);
		break;
	      case ebool:
		b = std::get<bool>(l) == std::get<bool>(r);
		break;
	      }
	      if (op == neq) b = !b;
	      return Value{b};
	    }
	  case gt:
	  case le:
	    {
	      bool b = false;
	      switch (r.index()) {
	      case einv:
		return r;
	      case eint:
		b = std::get<int64_t>(l) > std::get<int64_t>(r);
		break;
	      case estring:
		b = *std::get<pstring>(l) > *std::get<pstring>(r);
		break;
	      case ebool:
		b = std::get<bool>(l) > std::get<bool>(r);
		break;
	      }
	      if (op == le) b = !b;
	      return Value{b};
	    }
	  case lt:
	  case ge:
	    {
	      bool b = false;
	      switch (r.index()) {
	      case einv:
		return r;
	      case eint:
		b = std::get<int64_t>(l) < std::get<int64_t>(r);
		break;
	      case estring:
		b = *std::get<pstring>(l) < *std::get<pstring>(r);
		break;
	      case ebool:
		b = std::get<bool>(l) < std::get<bool>(r);
		break;
	      }
	      if (op == ge) b = !b;
	      return Value{b};
	    }
	  default:
	    ceph_assert(false);
	  }
	}
      case _or:
      case _and:
	{
	  Value l = cast_to(ebool, left->get_value());
	  Value r = cast_to(ebool, right->get_value());
	  if (l.index() == einv || r.index() == einv) {
	    return inv();
	  }
	  ceph_assert(l.index() == ebool);
	  ceph_assert(r.index() == ebool);
	  switch (op) {
	  case _or:
	    return std::get<bool>(l) || std::get<bool>(r);
	  case _and:
	    return std::get<bool>(l) && std::get<bool>(r);
	  default:
	    ceph_assert(false);
	  }
	}
      case plus:
      case minus:
      case mult:
      case div:
      case mod:
	{
	  Value L = cast_to(eint, left->get_value());
	  Value R = cast_to(eint, right->get_value());
	  if (L.index() == einv || R.index() == einv) {
	    return inv();
	  }
	  ceph_assert(L.index() == eint);
	  ceph_assert(R.index() == eint);
	  int64_t l = std::get<int64_t>(L);
	  int64_t r = std::get<int64_t>(R);
	  switch (op) {
	  case plus:
	    return l + r;
	  case minus:
	    return l - r;
	  case mult:
	    return l * r;
	  case div:
	    if (r == 0) {
	      return inv();
	    }
	    return l / r;
	  case mod:
	    if (r == 0) {
	      return inv();
	    }
	    return l % r;
	  default:
	    ceph_assert(false);
	  }
	  break;
	}
      case has:
	{
	  Value l = cast_to(estring, left->get_value());
	  Value r = cast_to(estring, right->get_value());
	  if (l.index() == einv || r.index() == einv) {
	    return false;
	  }
	  ceph_assert(l.index() == estring);
	  ceph_assert(r.index() == estring);
	  const std::string& L = *std::get<pstring>(l);
	  const std::string& R = *std::get<pstring>(r);
	  bool b = L.find(R) != std::string::npos;
	  return b;
	}
      default:
	{
	  ceph_assert(false);
	}
      }
      return inv();
    }
  };

  class UnaryExp : public Exp {
    operator_t op;
    ExpRef right;
  public:
    UnaryExp(operator_t op, ExpRef right)
      :op(op), right(right) {}

    Value get_value() override
    {
      switch (op) {
      case exists:
	{
	  return right->get_value().index() != einv;
	}
      case neg:
	{
	  Value r = cast_to(ebool, right->get_value());
	  if (r.index() == einv) {
	    return inv();
	  }
	  return !std::get<bool>(r);
	}
      default:
	{
	  ceph_assert(false);
	}
      }
    }
  };

  bool isUnary(operator_t op) {
    return operators[op].is_unary;
  }

  bool isBinary(operator_t op) {
    return operators[op].is_binary;
  }


  //must read at least one token and form expression
  //return true if reading expression succeded
  bool parse_expression(TokenSplitter& token_src,
			VariableMap& vars,
			std::string& error,
			ExpRef *exp_result,
			uint8_t left_op_prio = 255)
  {
    bool res = false;
    ExpRef left;
    error = "dupa";
    //mandatory consumption of token into expression
    token_t t = token_src.peek();
    switch (t) {
    case eLiteral:
      left.reset(new ExpLiteral(token_src.get_literal()));
      token_src.consume();
      res = true;
      break;
    case eNumber:
      left.reset(new ExpNumber(token_src.get_number()));
      token_src.consume();
      res = true;
      break;
    case eVariable:
      left.reset(new ExpVariable(vars[token_src.get_variable()]));
      token_src.consume();
      res = true;
      break;
    case eLeftBracket:
      {
	token_src.consume();
	//"shift"
	if (!parse_expression(token_src, vars, error, &left) ) {
	  //pass error from inside
	  break;
	}
	token_t t1 = token_src.peek();
	if (t1 != eRightBracket) {
	  error = ") missing";
	  break;
	}
	token_src.consume();
	//"reduce"
	res = true;
	break;
      }
    case eRightBracket:
      error = ") unexpected";
      break;
    case eOperator:
      {
	//must be unary operator
	operator_t op = token_src.get_operator();
	if (!isUnary(op)) {
	  error = "expected unary operator";
	  break;
	}
	uint8_t prio = token_src.get_operator_prio();
	token_src.consume();
	ExpRef exp;
	//shift
	res = parse_expression(token_src, vars, error, &exp, prio);
	//reduce
	if (res)
	  left = std::make_shared<UnaryExp>(op, exp);
	break;
      }
    case eEnd:
      error = "unexpected end of input";
      return false;
    case eError:
      error = "unrecognized input";
      return false;
    }

    //greedy part
    if (res) {
      while (true) {
	t = token_src.peek();
	if (t == eEnd) {
	  error = "";
	  break;
	}
	if (t == eOperator) {
	  operator_t op = token_src.get_operator();
	  if (!isBinary(op)) {
	    error = "binary operator expected";
	    //error
	    break;
	  }
	  uint8_t prio = token_src.get_operator_prio();
	  if (prio < left_op_prio) {
	    token_src.consume();
	    ExpRef right;
	    //shift
	    res = parse_expression(token_src, vars, error, &right, prio);
	    if (!res) {
	      //if fail, inherit error from nested
	      break;
	    }
	    left = std::make_shared<BinaryExp>(left, op, right);
	    //reduce
	    continue;
	  } else {
	    break;
	  }
	} else {
	  //this is not operator, so we exit greedy part
	  //if this is problem, outer expression will handle it
	  ceph_assert(res == true);
	  break;
	}
      }
    }
    if (res) {
      *exp_result = left;
    }
    return res;
  }

bool parse(TokenSplitter& token_src,
	   VariableMap& vars,
	   std::string& error,
	   ExpRef *exp_result)
{
  bool res = parse_expression(token_src, vars, error, exp_result);
  if (!res) {
    return false;
  }
  token_t t = token_src.peek();
  if (t != eEnd) {
    error = "extra token at end of input";
    return false;
  }
  error = "";
  return true;
}


class BufferImpl final: public Buffer, public Formatter {
  Formatter* sink;
  std::vector<std::function<void()>> stack;
  std::string dump_stream_name;
  std::stringstream dump_stream_stream;
  bool dump_stream_is = false;
public:
  virtual ~BufferImpl() {};
  Formatter* attach(Formatter* sink) override {
    detach();
    this->sink = sink;
    return this;
  }
  void detach() override {
    reset_buffer();
    sink = nullptr;
  }
  /* function to dump all buffered formats */
  void unbuffer() override {
    for (auto f: stack) {
      f();
    }
    stack.clear();
  }

private:
  void reset_buffer() {
    dump_stream_is = false;
    dump_stream_stream.str("");
    dump_stream_name.clear();
    stack.clear();
  }
  static void list_helper(Formatter* f,
			  const char *name, const char *ns, bool quoted, const char *fmt, ...) {
    va_list ap;
    va_start(ap, fmt);
    f->dump_format_va(name, ns, quoted, fmt, ap);
    va_end(ap);
  }
  void push(std::function<void()> f) {
    maybe_dump_stream();
    stack.push_back(std::move(f));
  }
  void maybe_dump_stream() {
    if (unlikely(dump_stream_is)) {
      Formatter* f = sink;
      std::string name = dump_stream_name;
      std::string data = dump_stream_stream.str();
      push([f, name, data](){f->dump_stream(name.c_str()) << data;});
      dump_stream_is = false;
      dump_stream_stream.str("");
    }
  }

  //formatter functions
  void enable_line_break() override {
    Formatter* f = sink;
    push([f](){f->enable_line_break();});
  }
  void flush(std::ostream& os) override {
    maybe_dump_stream();
    sink->flush(os);
  }
  void reset() override {
    maybe_dump_stream();
    sink->reset();
    }
  void set_status(int status, const char* status_name) override {
    sink->set_status(status, status_name);
  }
  void output_header() override {
    Formatter* f = sink;
    push([f](){f->output_header();});
  }
  void output_footer() override {
    Formatter* f = sink;
    push([f](){f->output_footer();});
  }
  void open_array_section(const char *name) override {
    Formatter* f = sink;
    std::string _name(name);
    push([f, _name](){f->open_array_section(_name.c_str());});
  }
  void open_array_section_in_ns(const char *name, const char *ns) override {
    Formatter* f = sink;
    std::string _name(name);
    std::string _ns(ns);
    push([f, _name, _ns](){f->open_array_section_in_ns(_name.c_str(), _ns.c_str());});    
  }
  void open_object_section(const char *name) override {
    Formatter* f = sink;
    std::string _name(name);
    push([f, _name](){f->open_object_section(_name.c_str());});
  }
  void open_object_section_in_ns(const char *name, const char *ns) override {
    Formatter* f = sink;
    std::string _name(name);
    std::string _ns(ns);
    push([f, _name, _ns](){f->open_object_section_in_ns(_name.c_str(), _ns.c_str());});    
  }
  void close_section() override {
    Formatter* f = sink;
    push([f](){f->close_section();});
  }
  void dump_unsigned(const char *name, uint64_t u) override {
    std::string _name(name);
    Formatter* f = sink;
    push([f, _name, u](){f->dump_unsigned(_name.c_str(), u);});
  }
  void dump_int(const char *name, int64_t s) override {
    std::string _name(name);
    Formatter* f = sink;
    push([f, _name, s](){f->dump_int(_name.c_str(), s);});
  }
  void dump_float(const char *name, double d) override {
    std::string _name(name);
    Formatter* f = sink;
    push([f, _name, d](){f->dump_float(_name.c_str(), d);});
  }
  void dump_string(const char *name, std::string_view s) override {
    std::string _name(name);
    std::string _s(s);
    Formatter* f = sink;
    push([f, _name, _s](){f->dump_string(_name.c_str(), _s);});
  }
  std::ostream& dump_stream(const char *name) override {
    dump_stream_name = name;
    dump_stream_is = true;
    return dump_stream_stream;
  }
  void dump_format_va(const char *name, const char *ns, bool quoted, const char *fmt, va_list ap) override {
    Formatter* f = sink;
    char* _buf;
    int r = vasprintf(&_buf, fmt, ap);
    ceph_assert(r >= 0);
    std::string _name(name);
    if (ns != nullptr) {
      std::string _ns(ns);
      push([f, _name, _ns, quoted, _buf]() {
	  list_helper(f, _name.c_str(), _ns.c_str(), quoted, "%s", _buf);
	  free(_buf);
	});
    } else {
      push([f, _name, quoted, _buf]() {
	  list_helper(f, _name.c_str(), nullptr, quoted, "%s", _buf);
	  free(_buf);
	});
    }
  }
  int get_len() const override {
    //can't suspend getting len
    return sink->get_len();
  }
  void write_raw_data(const char *data) override {
    Formatter* f = sink;
    std::string _data(data);
    push([f, _data](){f->write_raw_data(_data.c_str());});
  }
  void open_array_section_with_attrs(const char *name, const FormatterAttrs& attrs) override {
    Formatter* f = sink;
    std::string _name(name);
    FormatterAttrs _attrs(attrs);
    push([f, _name, _attrs](){f->open_array_section_with_attrs(_name.c_str(), _attrs);});    
  }
  void open_object_section_with_attrs(const char *name, const FormatterAttrs& attrs) override {
    Formatter* f = sink;
    std::string _name(name);
    FormatterAttrs _attrs(attrs);
    push([f, _name, _attrs](){f->open_object_section_with_attrs(_name.c_str(), _attrs);});        
  }
  void dump_string_with_attrs(const char *name, std::string_view s, const FormatterAttrs& attrs) override {
    Formatter* f = sink;
    std::string _name(name);
    std::string _s(s);
    FormatterAttrs _attrs(attrs);
    push([f, _name, _s, _attrs](){f->dump_string_with_attrs(_name.c_str(), _s, _attrs);});
  }
};



class FilterImpl final: public Filter, public Formatter {
  ExpRef exp;
  VariableMap vars;
  std::function<void()> on_match;
  Formatter* sink = nullptr;
  std::string path;
  std::vector<size_t> path_sizes;
  std::string dump_stream_name;
  std::stringstream dump_stream_stream;
  bool dump_stream_is = false;

public:
  virtual ~FilterImpl() {
  };
  bool setup(const std::string& condition,
	     std::string& error) override {
    TokenSplitter token_source;
    token_source.set_input(condition);
    bool res = parse(token_source, vars, error, &exp);
    if (!res) {
      stringstream ss;
      token_source.show_error(ss, error);
      error = ss.str();
    } else {
      error = "";
    }
    return res;
  }
  Formatter* attach(Formatter* sink, std::function<void()> on_match) override {
    reset_filter();
    this->sink = sink;
    this->on_match = on_match;
    return this;
  }
  void detach() override {
    reset_filter();
    sink = nullptr;
  }
  void reset_filter() {
    for (auto& v : vars) {
      v.second.reset();
    }
    path.clear();
    path_sizes.clear();
    dump_stream_is = false;
    dump_stream_name.clear();
    dump_stream_stream.str("");
  }

private:
  void enter(const char* name) {
    maybe_dump_stream();
    size_t l = path.length();
    path_sizes.push_back(l);
    path.push_back('.');
    path.append(name);
    auto it = vars.find(path);
    if (it != vars.end()) {
      it->second.reset(new std::string(""));
      check();
    }
  }
  void leave() {
    maybe_dump_stream();
    if (path_sizes.size() > 0) {
      size_t l = path_sizes.back();
      path_sizes.pop_back();
      path.resize(l);
    }
  }
  void check() {
    Exp::Value v = Exp::cast_to(Exp::ebool, exp->get_value());
    if (v.index() == Exp::ebool) {
      if (std::get<bool>(v)) {
	on_match();
      }
    }
  }
  VariableMap::iterator var_it(const char* name) {
    maybe_dump_stream();
    size_t l = path.length();
    path.push_back('.');
    path.append(name);
    auto it = vars.find(path);
    path.resize(l);
    return it;
  }
  void maybe_dump_stream() {
    if (unlikely(dump_stream_is)) {
      sink->dump_stream(dump_stream_name.c_str()) << dump_stream_stream.str();
      dump_stream_is = false;
      dump_stream_stream.str("");
    }
  }

  //Formatter methods
  void enable_line_break() override {
    sink->enable_line_break();
  }
  void flush(std::ostream& os) override {
    sink->flush(os);
  }
  void reset() override {
    sink->reset();
  }
  void set_status(int status, const char* status_name) override {
    sink->set_status(status, status_name);
  }
  void output_header() override {
    sink->output_header();
  }
  void output_footer() override {
    sink->output_footer();
  }
  void open_array_section(const char *name) override {
    enter(name);
    sink->open_array_section(name);
  }
  void open_array_section_in_ns(const char *name, const char *ns) override {
    enter(name);
    sink->open_array_section_in_ns(name, ns);
  }
  void open_object_section(const char *name) override {
    enter(name);
    sink->open_object_section(name);
  }
  void open_object_section_in_ns(const char *name, const char *ns) override {
    enter(name);
    sink->open_object_section_in_ns(name, ns);
  }
  void close_section() override {
    leave();
    sink->close_section();
  }
  void dump_unsigned(const char *name, uint64_t u) override {
    auto it = var_it(name);
    if (it != vars.end()) {
      it->second.reset(new std::string(to_string(u)));
      check();
    }
    sink->dump_unsigned(name, u);
  }
  void dump_int(const char *name, int64_t s) override {
    auto it = var_it(name);
    if (it != vars.end()) {
      it->second.reset(new std::string(to_string(s)));
      check();
    }
    sink->dump_int(name, s);
  }
  void dump_float(const char *name, double d) override {
    auto it = var_it(name);
    if (it != vars.end()) {
      it->second.reset(new std::string(to_string(d)));
      check();
    }
    sink->dump_float(name, d);
  }
  void dump_string(const char *name, std::string_view s) override {
    auto it = var_it(name);
    if (it != vars.end()) {
      it->second.reset(new std::string(s));
      check();
    }
    sink->dump_string(name, s);
  }
  std::ostream& dump_stream(const char *name) override {
    dump_stream_name = name;
    dump_stream_is = true;
    return dump_stream_stream;
  }
  void dump_format_va(const char *name, const char *ns, bool quoted, const char *fmt, va_list ap) override {
    auto it = var_it(name);
    if (it != vars.end()) {
      char* buf;
      int r = vasprintf(&buf, fmt, ap);
      ceph_assert(r >= 0);
      it->second.reset(new std::string(buf));
      check();
    }
    sink->dump_format_va(name, ns, quoted, fmt, ap);
  }
  int get_len() const override {
    return sink->get_len();
  }
  void write_raw_data(const char *data) override {
    maybe_dump_stream();
    //cannot be filtered
    sink->write_raw_data(data);
  }
  void open_array_section_with_attrs(const char *name, const FormatterAttrs& attrs) override {
    enter(name);
    sink->open_array_section_with_attrs(name, attrs);
  }
  void open_object_section_with_attrs(const char *name, const FormatterAttrs& attrs) override {
    enter(name);
    sink->open_object_section_with_attrs(name, attrs);
  }
  void dump_string_with_attrs(const char *name, std::string_view s, const FormatterAttrs& attrs) override {
    auto it = var_it(name);
    if (it != vars.end()) {
      it->second.reset(new std::string(s));
      check();
    }
    sink->dump_string_with_attrs(name, s, attrs);
  }
};



/*
Rule: "-a,+a.b.c,+a.d,-a.d.e.f.g,-a.h.i"
gives tree:
  -a -> ^b -> +c
     -> +d -> ^e -> ^f -> -g
     -> ^h -> -i
Legend:
  + in
  - out
  ^ maybe

When going through (^maybe) down, scope opening action is pushed for delayed resolve.
At some point it might be flushed on (+in).
When going through (^maybe) up, either scope close action is ignored (and corresponding opening action popped),
or scope close action is executed.
This translates to the following:
- if close action at (^maybe) can pop opening action, then ignore close action,
- otherwise emit close action
*/

class TrimmerImpl : public ceph::Trimmer, public Formatter {
  typedef enum {
    in,   //when this branch/element and all below are to be passed to output
    out,  //when this branch/element and all below are to be erased from output
    maybe //when this branch/element is undecided
          //if some element deeper will be IN, then it also will be IN
          //if no element deeper will be IN, then this is deleted
  } shape;
  struct stencil {
    shape action = in;
    std::map<std::string, stencil> pattern;
    stencil* up = nullptr;
    void dump(ostringstream& ss, const std::string& prev) {
      ss << prev;
      switch (action) {
      case in:
	ss << " in";
	break;
      case out:
	ss << " out";
	break;
      case maybe:
	ss << " maybe";
	break;
      }
      ss << std::endl;

      for (auto& x: pattern) {
	x.second.dump(ss, prev + " " + x.first);
      }
    }
  };
  stencil* root; /// < root for stencil
  stencil* current; /// < current location in stencil pattern
  Formatter* sink;
  std::vector<std::function<void()>> maybe_stack;
  size_t extra_depth = 0; /// < branch depth after last pattern shape was selected
  std::stringstream stream_devnull; /// < null stringstream to dump to if not selected

public:
  virtual ~TrimmerImpl() {
    if (root) {
      delete root;
    }
  };
  bool setup(std::string_view def, std::string& error) override {
    //split stencil by ","
    if (root) {
      delete root;
    }
    root = new stencil;
    std::string_view path;

    root->action = in;
    stencil* curr = root;

    while (!def.empty()) {
      size_t pos = def.find(",");
      if (pos != std::string_view::npos) {
	path = def.substr(0,pos);
	def = def.substr(pos+1);
      } else {
	path = def;
	def = std::string_view();
      }
      if (path.empty()) {
	continue;
      }
      curr = root;
      shape sh = in;
      if (path.front() == '-') {
	sh = out;
	path = path.substr(1);
      } else if (path.front() == '+') {
	path = path.substr(1);
      }
      if (!path.empty()) {
	if (path.front() == '.') {
	  //ignore leading '.'
	  path = path.substr(1);
	}
      }

      //1. When added rule is longer then existing tree,
      //types of branches bridging the gap between last branch and rule branch:
      //rule  last   action
      // +in  +in    do nothing, there is no reason to extend
      // +in  -out   extend with +in
      //-out  +in    extend with ^maybe
      //-out  -out   do nothing, there is no reason to extend
      //2. Delete any branches that go deeper then just defined branch.

      if (sh == in && root->action == out) {
	root->action = maybe;
      }
      while (!path.empty()) {
	std::string level;
	pos = path.find('.');
	if (pos != std::string_view::npos) {
	  level = std::string(path.substr(0, pos));
	  path = path.substr(pos+1);
	} else {
	  level = std::string(path);
	  path = std::string_view();
	}

	if (!level.empty()) {
	  auto it = curr->pattern.find(level);
	  if (it == curr->pattern.end()) {
	    //when we make deeper tree, we inherit action
	    auto& n = curr->pattern[level];
	    ceph_assert(! (sh == in && curr->action == out));
	    if (curr->action == maybe && sh == in) {
	      n.action = maybe;
	    } else {
	      n.action = curr->action;
	    }
	    n.up = curr;
	  } else {
	    //found this branch
	    //if it is -out and we are +in then we need to change to ^maybe
	    if (it->second.action == out && sh == in) {
	      it->second.action = maybe;
	    }
	  }
	  curr = &curr->pattern[level];
	}
      }
      curr->action = sh;
      //if there was some specialization of this path, it does not matter now
      curr->pattern.clear();
    }
    return true;
  }
  Formatter* attach(Formatter* sink) override {
    detach();
    this->sink = sink;
    return this;
  }
  virtual void detach() override {
    reset_trimmer();
    sink = nullptr;
  }

  void reset_trimmer() {
    current = root;
    extra_depth = 0;
  }

  void push(std::function<void()> f) {
    maybe_stack.push_back(std::move(f));
  }

  void flush_maybes() {
    for (auto f: maybe_stack) {
      f();
    }
    maybe_stack.clear();
  }

  bool pop_maybe() {
    if (maybe_stack.empty()) {
      return false;
    } else {
      maybe_stack.pop_back();
      return true;
    }
  }

  static char* capture(const char* name) {
    if (name == nullptr) {
      return nullptr;
    } else {
      return strdup(name);
    }
  }

  bool qualify(const char* name) {
    if (extra_depth > 0) {
      return current->action == in;
    }
    auto it = current->pattern.find(name);
    if (it == current->pattern.end()) {
      //no specialization for that name
      return current->action == in;
    } else {
      //there is specialization for that name
      if (it->second.action == in) {
	flush_maybes();
      }
      return it->second.action == in;
    }
  }

  shape enter(const char* name) {
    if (extra_depth > 0) {
      extra_depth++;
      //if we are in ^maybe mode, it means ^out
      if (current->action == in) {
	return in;
      } else {
	return out;
      }
    }
    auto it = current->pattern.find(name);
    if (it == current->pattern.end()) {
      //no specialization for that name
      extra_depth++;
      //if we are in ^maybe mode, it means ^out
      if (current->action == in) {
	return in;
      } else {
	return out;
      }
    } else {
      //there is specialization for that name
      if (current->action == maybe) {
	//if we just left ^maybe land we need to flush stacked opening actions
	current = &it->second;
	if (current->action == in) {
	  flush_maybes();
	}
	return current->action;
      } else {
	current = &it->second;
	return current->action;
      }
    }
  }
  /* true - emit, false - ignore */
  bool leave() {
    if (extra_depth > 0) {
      extra_depth--;
      //if we are in ^maybe mode, it means ^out
      return current->action == in;
    }
    bool result = false;
    //if we are at ^maybe we have to choose
    switch (current->action) {
    case maybe:
      result = !pop_maybe();
      break;
    case in:
      result = true;
      break;
    case out:
      result = false;
      break;
    }
    if (current->up != nullptr) {
      current = current->up;
    } else {
      //this basically means someone went below root
      //we ignore it, but we can self-check
      ceph_assert(current == root);
    }
    return result;
  }

  void enable_line_break() override {
    sink->enable_line_break();
  }
  void flush(std::ostream& os) override {
    sink->flush(os);
  }
  void reset() override {
    sink->reset();
  }

  void set_status(int status, const char* status_name) override {
    sink->set_status(status, status_name);
  }
  void output_header() override {
    sink->output_header();
  }
  void output_footer() override {
    sink->output_footer();
  }

  void open_array_section(const char *name) override {
    switch (enter(name)) {
    case maybe:
      {
	Formatter* f = sink;
	char* _name = capture(name);
	push([f, _name](){f->open_array_section(_name); free(_name);});
      }
      break;
    case in:
      sink->open_array_section(name);
      break;
    case out:
      break;
    }
  }
  void open_array_section_in_ns(const char *name, const char *ns) override {
    switch (enter(name)) {
    case maybe:
      {
	Formatter* f = sink;
	char* _name = capture(name);
	char* _ns = capture(ns);
	push([f, _name, _ns](){f->open_array_section_in_ns(_name, _ns); free(_name); free(_ns);});
      }
      break;
    case in:
      sink->open_array_section_in_ns(name, ns);
      break;
    case out:
      break;
    }
  }
  void open_object_section(const char *name) override {
    switch (enter(name)) {
    case maybe:
      {
	Formatter* f = sink;
	char* _name = capture(name);
	push([f, _name](){f->open_object_section(_name); free(_name); });
      }
      break;
    case in:
      sink->open_object_section(name);
      break;
    case out:
      break;
    }
  }
  void open_object_section_in_ns(const char *name, const char *ns) override {
    switch (enter(name)) {
    case maybe:
      {
	Formatter* f = sink;
	char* _name = capture(name);
	char* _ns = capture(ns);
	push([f, _name, _ns](){f->open_object_section_in_ns(_name, _ns); free(_name); free(_ns);});
      }
      break;
    case in:
      sink->open_object_section_in_ns(name, ns);
      break;
    case out:
      break;
    }
  }
  void close_section() override {
    if (leave()) {
      sink->close_section();
    }
  }
  void dump_unsigned(const char *name, uint64_t u) override {
    if (qualify(name)) {
      sink->dump_unsigned(name, u);
    }
  }
  void dump_int(const char *name, int64_t s) override {
    if (qualify(name)) {
      sink->dump_int(name, s);
    }
  }
  void dump_float(const char *name, double d) override {
    if (qualify(name)) {
      sink->dump_float(name, d);
    }
  }
  void dump_string(const char *name, std::string_view s) override {
    if (qualify(name)) {
      sink->dump_string(name, s);
    }
  }
  std::ostream& dump_stream(const char *name) override {
    if (qualify(name)) {
      return sink->dump_stream(name);
    } else {
      //provide null stream to dump data
      stream_devnull.str("");
      return stream_devnull;
    }
  }
  void dump_format_va(const char *name, const char *ns, bool quoted, const char *fmt, va_list ap) override {
    if (qualify(name)) {
      sink->dump_format_va(name, ns, quoted, fmt, ap);
    }
  };
  int get_len() const override {
    return sink->get_len();
  }
  void write_raw_data(const char *data) override {
    if (current->action == in || true) {
      sink->write_raw_data(data);
    }
  }
  void open_array_section_with_attrs(const char *name, const FormatterAttrs& attrs) override {
    switch (enter(name)) {
    case maybe:
      {
	Formatter* f = sink;
	char* _name = capture(name);
	FormatterAttrs _attrs(attrs);
	push([f, _name, _attrs](){f->open_array_section_with_attrs(_name, _attrs); free(_name);});
      }
      break;
    case in:
      sink->open_array_section_with_attrs(name, attrs);
      break;
    case out:
      break;
    }
  }
  void open_object_section_with_attrs(const char *name, const FormatterAttrs& attrs) override {
    switch (enter(name)) {
    case maybe:
      {
	Formatter* f = sink;
	char* _name = capture(name);
	FormatterAttrs _attrs(attrs);
	push([f, _name, _attrs](){f->open_object_section_with_attrs(_name, _attrs); free(_name);});
      }
      break;
    case in:
      sink->open_object_section_with_attrs(name, attrs);
      break;
    case out:
      break;
    }
  }
  void dump_string_with_attrs(const char *name, std::string_view s, const FormatterAttrs& attrs) override {
    if (qualify(name)) {
      sink->dump_string_with_attrs(name, s, attrs);
    }
  }
};

} /*namespace formatter_filter*/

Filter* Filter::create() {
  return new formatter_filter::FilterImpl();
}

Buffer* Buffer::create() {
  return new formatter_filter::BufferImpl();
}

Trimmer* Trimmer::create() {
  return new formatter_filter::TrimmerImpl();
}

} /*namespace ceph*/
