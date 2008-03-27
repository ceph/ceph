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

#include <typeinfo>

using namespace std;

typedef char const*         iterator_t;                                                                              
typedef tree_match<iterator_t> parse_tree_match_t;                                                                   
typedef parse_tree_match_t::tree_iterator iter_t;

long eval_term(iter_t const& i);                                                                                                                   
long evaluate(parse_tree_match_t hit);                                                                               
long eval_expression(iter_t const& i);                                                                               
long evaluate(tree_parse_info<> info)                                                                                
{                                                                                                                    

  std::cout << "t_id(info.trees.begin()).name(): " << typeid(info.trees.begin()).name() << '\n';
  std::cout << "info.trees.size(): " <<  info.trees.size() <<std::endl;

  for (unsigned int i=0; i<info.trees.size();i++) {
    std::cout << "i: " << i << std::endl;
    std::cout << "DOWN A LEVEL\n";
    std::cout << "info.trees[index_i].children.size() :" <<  info.trees[i].children.size() <<std::endl;
    //boost::spirit::parser_id id = info.trees[i].value.id();
    
    std::cout << "In eval_expression. info.trees[index_i].value = " << string(info.trees[i].value.begin(), info.trees[i].value.end()) << std::endl 
	      << "info.trees[index_i].children.size() = " << info.trees[i].children.size() << std::endl;      
    std::cout << "t_id(info.trees[index_ii].value.begin()).name(): " << typeid(info.trees[i].value.begin()).name() << '\n';
    std::cout << "t_id(info.trees[index_i].value).name: " << typeid(info.trees[i].value).name() << '\n';
    
    if (info.trees[i].value.is_root()) {
      printf("is root!\n");
    }
    
    std::cout << "typeid( (info.trees[index_i].value).value() ).name(): " << typeid( info.trees[i].value.value() ).name() << std::endl;
    
    string blah(info.trees[i].value.begin(), info.trees[i].value.end());
    //string integerB(info.trees[i].value.value.begin(), info.trees[i].value.value.end());

    std::cout << "value: " << blah << std::endl;
    //printf("IB: %s\n", integerB);
    
    for (unsigned int j = 0; j < info.trees[i].children.size(); j++) {                        
      std::cout<< "index_j: " << j << std::endl;
      std::cout << "typeid(info.trees[index_i].children[index_j]).name(): " << typeid(info.trees[i].children[j]).name() << '\n';
      std::cout << "typeid(info.trees[index_i].children[index_j].value).name(): " << typeid(info.trees[i].children[j].value).name() << '\n';
      
      //string temp = typeid(info.trees[i].children[j]).name() 

      //string blah(info.trees[i].children[j].value.begin(), info.trees[i].children[j].value.end());
      //std::cout << "value string: " << blah << std::endl;

      //string blah2(info.trees[i].children[j].begin(), info.trees[i].children[j].end());
      //std::cout << "child string: " << blah2 << std::endl;

      // eval_expression( info.trees[i].children[j].begin );
      
      
      //      const type_info tp = typeid(info.trees[i].children[j]);
      //const char *nm = tp.name();
      //std::cout << nm<<std::endl;
      // next node points to the operator.  The text of the operator is                                                     
      // stored in value (a vector<char>) 
      //for (iter_t j = chi->value.begin(); j != chi->value.end(); ij+) { 
      //std::cout << "here: " << *j <<std::endl;
      //char op = *(chi->value.begin());
      //std::cout << chi->value <<std::endl;
      //printf("op: %c\n", op);
      //}
    }                                                                                                                         
  
    cout << "evalutaing info..." << std::endl;                                                                         
    //printf("%d\n", (int)info.trees[i].value.id());
    
    if (info.trees[i].value.id() == crush_grammar::_int) {                                                                        
      return 1;                                                                                                        
    } else if (info.trees[i].value.id() == crush_grammar::_posint) {                                                              
      return 2;                                                                                                        
    } else if (info.trees[i].value.id() == crush_grammar::_name) {                                                                
      return 3;                                                                                                        
    } else if (info.trees[i].value.id() == crush_grammar::_device) {                                                              
      return 4;                                                                                                        
    } else if (info.trees[i].value.id() == crush_grammar::_bucket_type) {                                                         
      return 5;                                                                                                        
    } else if (info.trees[i].value.id() == crush_grammar::_bucket_id) {                                                           
      return 6;                                                                                                        
    } else if (info.trees[i].value.id() == crush_grammar::_bucket_alg) {                                                          
      return 7;                                                                                                        
    } else {                                                                                                           
      return 0;                                                                                                        
    }                                                                                                                  
  }
  cout << "should never get here." << std::endl;                                                                     
  return -1;                                                                                                         
}                                                                                                                    


const char *infn = "stdin";


////////////////////////////////////////////////////////////////////////////

int verbose = 0;

int compile_crush_file(const char *infn, CrushWrapper &crush)
{ 
  // read the file
  ifstream in(infn);
  if (!in.is_open()) {
    cerr << "input file " << infn << " not found" << std::endl;
    return -ENOENT;
  }

  string big;
  string str;
  int line = 1;
  map<int,int> line_pos;  // pos -> line
  map<int,string> line_val;
  while (getline(in, str)) {
    // remove newline
    int l = str.length();
    if (l && str[l] == '\n')
      str.erase(l-1, 1);

    line_val[line] = str;

    // strip comment
    int n = str.find("#");
    if (n >= 0)
      str.erase(n, str.length()-n);
    
    if (verbose) cout << line << ": " << str << std::endl;

    if (big.length()) big += " ";
    line_pos[big.length()] = line;
    line++;
    big += str;
  }

  if (verbose >= 2) cout << "whole file is: \"" << big << "\"" << std::endl;
  
  crush_grammar crushg;
  const char *start = big.c_str();
  //tree_parse_info<const char *> info = ast_parse(start, crushg, space_p);
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
    //tree_to_xml(cout, info.trees, big.c_str(), rule_names);


    // print the result
    cout << "parsing succeeded\n";
    cout << "result = " << evaluate(info) << "\n\n";
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
  cout << me << ": usage: crushtool [-d map] [-c map.txt] [-o outfile [--clobber]]" << std::endl;
  exit(1);
}

int main(int argc, const char **argv)
{

  vector<const char*> args;
  argv_to_vec(argc, argv, args);

  const char *me = argv[0];
  const char *cinfn = 0;
  const char *dinfn = 0;
  const char *outfn = 0;
  bool clobber = false;

  for (unsigned i=0; i<args.size(); i++) {
    if (strcmp(args[i], "--clobber") == 0) 
      clobber = true;
    else if (strcmp(args[i], "-d") == 0)
      dinfn = args[++i];
    else if (strcmp(args[i], "-o") == 0)
      outfn = args[++i];
    else if (strcmp(args[i], "-c") == 0)
      cinfn = args[++i];
    else if (strcmp(args[i], "-v") == 0)
      verbose++;
    else 
      usage(me);
  }
  if (cinfn && dinfn)
    usage(me);
  if (!cinfn && !dinfn)
    usage(me);

  /*
  if (outfn) cout << "outfn " << outfn << std::endl;
  if (cinfn) cout << "cinfn " << cinfn << std::endl;
  if (dinfn) cout << "dinfn " << dinfn << std::endl;
  */

  CrushWrapper crush;

  if (dinfn) {
    bufferlist bl;
    int r = bl.read_file(dinfn);
    if (r < 0) {
      cerr << me << ": error reading '" << dinfn << "': " << strerror(-r) << std::endl;
      exit(1);
    }
    bufferlist::iterator p = bl.begin();
    crush._decode(p);

    if (outfn) {
      ofstream o;
      o.open(outfn, ios::out | ios::binary | ios::trunc);
      if (!o.is_open()) {
	cerr << me << ": error writing '" << outfn << "'" << std::endl;
	exit(1);
      }
      decompile_crush(crush, o);
      o.close();
    } else 
      decompile_crush(crush, cout);
  }

  if (cinfn) {
    int r = compile_crush_file(cinfn, crush);
    if (r < 0) 
      exit(1);

    if (outfn) {
      bufferlist bl;
      crush._encode(bl);
      int r = bl.write_file(outfn);
      if (r < 0) {
	cerr << me << ": error writing '" << outfn << "': " << strerror(-r) << std::endl;
	exit(1);
      }
    } else {
      cout << me << " successfully compiled '" << cinfn << "'.  Use -o file to write it out." << std::endl;
    }
  }

  return 0;
}
