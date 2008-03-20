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
#include <fstream>
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



const char *infn = "stdin";


////////////////////////////////////////////////////////////////////////////

int compile_crush_file(const char *infn, CrushWrapper &crush)
{ 
  // read the file
  ifstream in(infn);
  string big;
  string str;
  int line = 1;
  map<int,int> line_pos;  // pos -> line
  map<int,string> line_val;
  while (getline(cin, str)) {
    // remove newline
    int l = str.length();
    if (l && str[l] == '\n')
      str.erase(l-1, 1);

    line_val[line] = str;

    // strip comment
    int n = str.find("#");
    if (n >= 0)
      str.erase(n, str.length()-n);
    cout << line << ": " << str << std::endl;

    if (big.length()) big += " ";
    line_pos[big.length()] = line;
    line++;
    big += str;
  }

  cout << "whole file is: \"" << big << "\"" << std::endl;
  
  crush_grammar crushg;
  const char *start = big.c_str();
  tree_parse_info<> info = ast_parse(start, crushg, space_p);

  // parse error?
  if (!info.full) {
    int cpos = info.stop - start;
    //cout << "cpos " << cpos << std::endl;
    //cout << " linemap " << line_pos << std::endl;
    assert(!line_pos.empty());
    map<int,int>::iterator p = line_pos.upper_bound(cpos);
    if (p != line_pos.begin()) p--;
    int line = p->second;
    int pos = cpos - p->first;
    cerr << infn << ":" << line //<< ":" << (pos+1)
	 << " error: parse error at '" << line_val[line].substr(pos) << "'" << std::endl;
    return 1;
  }
  {
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
  }

  return 0;
}

void print_type_name(ostream& out, int t, CrushWrapper &crush)
{
  const char *name = crush.get_type_name(t);
  if (name)
    out << name;
  else if (t == 0)
    out << "device";
  else
    out << "type" << t;
}

void print_item_name(ostream& out, int t, CrushWrapper &crush)
{
  const char *name = crush.get_item_name(t);
  if (name)
    out << name;
  else if (t >= 0)
    out << "device" << t;
  else
    out << "bucket" << (-1-t);
}

void print_rule_name(ostream& out, int t, CrushWrapper &crush)
{
  const char *name = crush.get_rule_name(t);
  if (name)
    out << name;
  else
    out << "rule" << t;
}

void print_fixedpoint(ostream& out, int i)
{
  out << (i / 0x10000);
}

int decompile_crush(CrushWrapper &crush, ostream &out)
{
  out << "# begin crush map\n\n";

  out << "# devices\n";
  for (int i=0; i<crush.get_max_devices(); i++) {
    out << "device " << i << " ";
    print_item_name(out, i, crush);
    if (crush.get_device_offload(i)) {
      out << " offload ";
      print_fixedpoint(out, crush.get_device_offload(i));
    }
    out << "\n";
  }
  
  out << "\n# types\n";
  int n = crush.get_num_type_names();
  for (int i=0; n; i++) {
    const char *name = crush.get_type_name(i);
    if (!name) {
      if (i == 0) out << "type 0 device\n";
      continue;
    }
    n--;
    out << "type " << i << " " << name << "\n";
  }

  out << "\n# buckets\n";
  for (int i=-1; i > -1-crush.get_max_buckets(); i--) {
    if (!crush.bucket_exists(i)) continue;
    int type = crush.get_bucket_type(i);
    print_type_name(out, type, crush);
    out << " ";
    print_item_name(out, i, crush);
    out << " {\n";
    out << "\tid " << i << "\n";
    out << "\talg " << crush.get_bucket_alg(i) << "\n";
    int n = crush.get_bucket_size(i);
    bool dopos = false;
    for (int j=0; j<n; j++) {
      int item = crush.get_bucket_item(i, j);
      int w = crush.get_bucket_item_weight(i, j);
      if (!w) {
	dopos = true;
	continue;
      }
      out << "\titem ";
      print_item_name(out, item, crush);
      out << " weight ";
      print_fixedpoint(out, w);
      if (dopos)
	out << " pos " << j;
      out << "\n";
    }
    out << "}\n";
  }

  out << "\n# rules\n";
  for (int i=0; i<crush.get_max_rules(); i++) {
    if (!crush.rule_exists(i)) continue;
    out << "rule ";
    print_rule_name(out, i, crush);
    out << " {\n";
    out << "\tpool " << crush.get_rule_mask_pool(i) << "\n";
    switch (crush.get_rule_mask_type(i)) {
    case CEPH_PG_TYPE_REP: out << "\ttype replicated\n"; break;
    case CEPH_PG_TYPE_RAID4: out << "\ttype raid4\n"; break;
    default: out << "\ttype " << crush.get_rule_mask_type(i) << "\n";
    }
    out << "\tmin_size " << crush.get_rule_mask_min_size(i) << "\n";
    out << "\tmax_size " << crush.get_rule_mask_max_size(i) << "\n";
    for (int j=0; j<crush.get_rule_len(i); j++) {
      switch (crush.get_rule_op(i, j)) {
      case CRUSH_RULE_NOOP:
	out << "\tstep noop\n";
	break;
      case CRUSH_RULE_TAKE:
	out << "\tstep take ";
	print_item_name(out, crush.get_rule_arg1(i, j), crush);
	out << "\n";
	break;
      case CRUSH_RULE_EMIT:
	out << "\tstep emit\n";
	break;
      case CRUSH_RULE_CHOOSE_FIRSTN:
	out << "\tstep choose firstn "
	    << crush.get_rule_arg1(i, j) 
	    << " type ";
	print_type_name(out, crush.get_rule_arg2(i, j), crush);
	out << "\n";
	break;
      case CRUSH_RULE_CHOOSE_INDEP:
	out << "\tstep choose indep "
	    << crush.get_rule_arg1(i, j) 
	    << " type ";
	print_type_name(out, crush.get_rule_arg2(i, j), crush);
	out << "\n";
	break;
      }
    }
    out << "}\n";
  }
  out << "\n# end crush map" << std::endl;
  return 0;
}


int usage(const char *me)
{
  cout << me << ": usage: crushtool [-i infile] [-c infile.txt] [-o outfile] [-x outfile.txt]" << std::endl;
  exit(1);
}

int main(int argc, const char **argv)
{

  vector<const char*> args;
  argv_to_vec(argc, argv, args);
  parse_config_options(args);

  const char *me = argv[0];

  const char *infn = 0;
  const char *outfn = 0;
  const char *cinfn = 0;
  const char *doutfn = 0;
  bool print = false;
  bool createsimple = false;
  int size = 0;
  bool clobber = false;
  list<entity_addr_t> add, rm;

  for (unsigned i=0; i<args.size(); i++) {
    if (strcmp(args[i], "--print") == 0)
      print = true;
    else if (strcmp(args[i], "--createsimple") == 0) {
      createsimple = true;
      size = atoi(args[++i]);
    } else if (strcmp(args[i], "--clobber") == 0) 
      clobber = true;
    else if (strcmp(args[i], "-i") == 0)
      infn = args[++i];
    else if (strcmp(args[i], "-o") == 0)
      outfn = args[++i];
    else if (strcmp(args[i], "-c") == 0)
      cinfn = args[++i];
    else if (strcmp(args[i], "-x") == 0)
      doutfn = args[++i];
    else 
      usage(me);
  }

  CrushWrapper crush;
  
  if (infn) {
    bufferlist bl;
    int r = bl.read_file(infn);
    if (r < 0) {
      cerr << me << ": error reading '" << infn << "': " << strerror(-r) << std::endl;
      exit(1);
    }
    bufferlist::iterator p = bl.begin();
    crush._decode(p);
  }

  if (cinfn) {
    compile_crush_file(cinfn, crush);

    if (outfn) {
      bufferlist bl;
      crush._encode(bl);
      int r = bl.write_file(outfn);
      if (r < 0) {
	cerr << me << ": error writing '" << outfn << "': " << strerror(-r) << std::endl;
	exit(1);
      }
    }
  }

  if (doutfn) {
    decompile_crush(crush, cout);
  }

  return 0;
}
