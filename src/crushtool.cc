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

#include <boost/spirit/core.hpp>
#include <boost/spirit/tree/ast.hpp>
#include <boost/spirit/tree/tree_to_xml.hpp>

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


using namespace boost::spirit;

typedef char const*         iterator_t;
typedef tree_match<iterator_t> parse_tree_match_t;
typedef parse_tree_match_t::tree_iterator iter_t;


//
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

////////////////////////////////////////////////////////////////////////////
int main(int argc, char **argv)
{
  // look in tree_calc_grammar for the definition of crush_grammar
  crush_grammar calc;
  
  cout << "/////////////////////////////////////////////////////////\n\n";
  cout << "\t\tThe simplest working crush_grammar...\n\n";
  cout << "/////////////////////////////////////////////////////////\n\n";
  cout << "Type an expression...or [q or Q] to quit\n\n";
  
  string str;
  while (getline(cin, str))
    {
      if (str.empty() || str[0] == 'q' || str[0] == 'Q')
	break;
      
      tree_parse_info<> info = ast_parse(str.c_str(), calc);
      
      if (info.full)
        {
#if defined(BOOST_SPIRIT_DUMP_PARSETREE_AS_XML)
	  // dump parse tree as XML
	  std::map<parser_id, std::string> rule_names;
	  rule_names[crush_grammar::integerID] = "integer";
	  rule_names[crush_grammar::factorID] = "factor";
	  rule_names[crush_grammar::termID] = "term";
	  rule_names[crush_grammar::expressionID] = "expression";
	  tree_to_xml(cout, info.trees, str.c_str(), rule_names);
#endif
	  
	  // print the result
	  cout << "parsing succeeded\n";
	  cout << "result = " << evaluate(info) << "\n\n";
        }
      else
        {
	  cout << "parsing failed\n";
        }
    }
  
  cout << "Bye... :-) \n\n";
  return 0;
}

