// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#ifndef FORMATTER_FILTER_H
#define FORMATTER_FILTER_H

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
#include "common/formatter_filter.h"

namespace ceph {
namespace formatter_filter {

struct filter_branch {
  typedef enum {branch_in, branch_out} branch_handling;
  std::map<std::string, filter_branch> branches;
  branch_handling action;
  bool is_leaf() {
    return branches.size() == 0;
  }
  bool is_in() {
    return action == branch_in;
  }
};

using VariableMap = std::map<std::string, std::shared_ptr<std::string>>;

typedef enum {
  op_first = 0,
  eq = op_first,  //==
  neq, //!=
  ge,  //>=
  le,  //<=
  gt,  //>
  lt,  //<
  _and, // &&
  _or,  // ||
  has, // "has"
  exists, // "exists"
  plus, // +
  minus, // -
  mult, // *
  div, // /
  mod, // %
  neg, // !
  op_last
} operator_t;

typedef enum {
  eLeft,
  eRight,
  eBi
} assoc_t;

struct operator_spec_t {
  std::string name;
  uint8_t prio;
  bool is_binary;
  bool is_unary;
};

typedef enum {
  eLiteral,
  eNumber,
  eVariable,
  eOperator,
  eLeftBracket,
  eRightBracket,
  eEnd,
  eError
} token_t;

class Exp {
public:
  Exp() {}
  virtual ~Exp() {}
  class inv {};
  using pstring = std::shared_ptr<std::string>;
  using Value = std::variant<inv,
			     int64_t,
			     pstring,
			     bool>;
  typedef enum {
    einv,
    eint,
    estring,
    ebool
  } variant_t;
  static_assert(std::is_same_v<inv, std::variant_alternative_t<einv, Value>>);
  static_assert(std::is_same_v<int64_t, std::variant_alternative_t<eint, Value>>);
  static_assert(std::is_same_v<pstring, std::variant_alternative_t<estring, Value>>);
  static_assert(std::is_same_v<bool, std::variant_alternative_t<ebool, Value>>);

  virtual Value get_value() = 0;
  static Value cast_to(variant_t target, const Value& right);
};
using ExpRef = std::shared_ptr<Exp>;



class TokenSplitter {
public:
  void set_input(std::string_view str);
  token_t peek();
  void consume();
  std::string get_variable();
  std::string get_literal();
  uint64_t get_number();
  operator_t get_operator();
  int8_t get_operator_prio();

  bool show_error(std::ostream&out,
		  const std::string& error);
private:
  std::string_view input; //whole passed input
  std::string_view left; //input not consumed yet
  std::string_view peeked; //input after consuming peeked token

  token_t token;
  operator_t token_op;
  uint8_t token_op_prio;
  std::string token_literal;
  uint64_t token_number;
  std::string token_variable;

  std::string error;
  void parse_one();
  void skip_whitespace();
  std::tuple<operator_t, uint8_t> get_operator(std::string_view cand);
  size_t possibly_operator(std::string_view cand);
};

bool parse(TokenSplitter& token_src,
	   VariableMap& vars,
	   std::string& error,
	   ExpRef *exp_result);

} /*namespace formatting_filter*/
} /*namespace ceph*/

#endif
