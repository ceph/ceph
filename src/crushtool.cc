// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software 
 * Foundation.  See file COPYING.
 * 
 */

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <errno.h>

#include <sys/stat.h>

#include "config.h"

#include "crush/CrushWrapper.h"
#include "crush/grammar.h"

#include <iostream>
#include <stack>
#include <functional>
#include <string>
#include <cassert>
#include <map>
using namespace std;

/*typedef char const*         iterator_t;
typedef tree_match<iterator_t> parse_tree_match_t;
typedef parse_tree_match_t::tree_iterator iter_t;
*/

//
/*
long evaluate(parse_tree_match_t hit);
long eval_expression(iter_t const& i);

long evaluate(tree_parse_info<> info)
{
  return eval_expression(info.trees.begin());
}

long eval_expression(iter_t const& i)
{
  cout << "In eval_expression. i->value = "
       << string(i->value.begin(), i->value.end())
       << " i->children.size() = " << i->children.size() << std::endl;
  
  if (i->value.id() == crush_grammar::integerID)
    {
      assert(i->children.size() == 0);
      
      // extract integer (not always delimited by '\0')
      string integer(i->value.begin(), i->value.end());
      
      return strtol(integer.c_str(), 0, 10);
    }
  else if (i->value.id() == crush_grammar::factorID)
    {
      // factor can only be unary minus
      assert(*i->value.begin() == '-');
      return - eval_expression(i->children.begin());
    }
  else if (i->value.id() == crush_grammar::termID)
    {
      if (*i->value.begin() == '*')
        {
	  assert(i->children.size() == 2);
	  return eval_expression(i->children.begin()) *
	    eval_expression(i->children.begin()+1);
        }
      else if (*i->value.begin() == '/')
        {
	  assert(i->children.size() == 2);
	  return eval_expression(i->children.begin()) /
	    eval_expression(i->children.begin()+1);
        }
      else
	assert(0);
    }
  else if (i->value.id() == crush_grammar::expressionID)
    {
      if (*i->value.begin() == '+')
        {
	  assert(i->children.size() == 2);
	  return eval_expression(i->children.begin()) +
	    eval_expression(i->children.begin()+1);
        }
      else if (*i->value.begin() == '-')
        {
	  assert(i->children.size() == 2);
	  return eval_expression(i->children.begin()) -
	    eval_expression(i->children.begin()+1);
        }
      else
	assert(0);
    }
  else
    {
      assert(0); // error
    }
  
  return 0;
}
*/


////////////////////////////////////////////////////////////////////////////
int main(int argc, char **argv)
{
  
  cout << "go" << std::endl;
  string big;
  string str;
  int line = 1;
  while (getline(cin, str)) {
    // fixme: strip out comments
    int l = str.length();
    if (l && str[l] == '\n')
      str.erase(l-1, 1);
    int n = str.find("#");
    if (n >= 0)
      str.erase(n, str.length()-n);
    cout << line++ << ": " << str << std::endl;
    if (big.length()) big += " ";
    big += str;
  }

  cout << "whole file is: \"" << big << "\"" << std::endl;

  crush_grammar crushg;
  //bool parsed = parse(big.c_str(), crushg, space_p).full;
  tree_parse_info<> info = ast_parse(big.c_str(), crushg, space_p);
  bool parsed = info.full;

  if (parsed) {
    // dump parse tree as XML
    std::map<parser_id, std::string> rule_names;
    rule_names[crush_grammar::_int] = "int";
    rule_names[crush_grammar::_posint] = "posint";
    rule_names[crush_grammar::_name] = "name";
    rule_names[crush_grammar::_device] = "device";
    rule_names[crush_grammar::_bucket_type] = "bucket_type";
    rule_names[crush_grammar::_bucket_id] = "bucket_id";
    rule_names[crush_grammar::_bucket_alg] = "bucket_alg";
    rule_names[crush_grammar::_bucket_item] = "bucket_item";
    rule_names[crush_grammar::_bucket] = "bucket";
    rule_names[crush_grammar::_step_take] = "step_take";
    rule_names[crush_grammar::_step_choose_indep] = "step_choose_indep";
    rule_names[crush_grammar::_step_choose_firstn] = "step_choose_firstn";
    rule_names[crush_grammar::_step_emit] = "step_emit";
    rule_names[crush_grammar::_crushrule] = "rule";
    rule_names[crush_grammar::_crushmap] = "map";
    tree_to_xml(cout, info.trees, big.c_str(), rule_names);

    // print the result
    cout << "parsing succeeded\n";
    //cout << "result = " << evaluate(info) << "\n\n";
  } else {
    cout << "did not parse" << std::endl;
  }

  return 0;
}

