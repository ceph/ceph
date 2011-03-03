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

#include "common/config.h"

#include "common/ceph_argparse.h"
#include "common/common_init.h"
#include "crush/CrushWrapper.h"
#include "crush/grammar.h"


#include <iostream>
#include <fstream>
#include <stack>
#include <functional>
#include <string>
#include <stdexcept>
#include <map>

#include <typeinfo>

using namespace std;

typedef char const*         iterator_t;                                                                              
typedef tree_match<iterator_t> parse_tree_match_t;                                                                   
typedef parse_tree_match_t::tree_iterator iter_t;
typedef parse_tree_match_t::node_t node_t;

int verbose = 0;

map<string, int> item_id;
map<int, string> id_item;
map<int, float> item_weight;

map<string, int> type_id;

map<string, int> rule_id;

string string_node(node_t &node)
{
  return string(node.value.begin(), node.value.end());
}

int int_node(node_t &node) 
{
  string str = string_node(node);
  return strtol(str.c_str(), 0, 10);
}

float float_node(node_t &node)
{
  string s = string_node(node);
  return strtof(s.c_str(), 0);
}

void parse_device(iter_t const& i, CrushWrapper &crush)
{
  int id = int_node(i->children[1]);

  string name = string_node(i->children[2]);
  crush.set_item_name(id, name.c_str());
  if (item_id.count(name)) {
    cerr << "item " << name << " defined twice" << std::endl;
    exit(1);
  }    
  item_id[name] = id;
  id_item[id] = name;

  if (verbose) cout << "device " << id << " " << name << std::endl;

  if (id >= crush.get_max_devices())
    crush.set_max_devices(id+1);
}

void parse_bucket_type(iter_t const& i, CrushWrapper &crush)
{
  int id = int_node(i->children[1]);
  string name = string_node(i->children[2]);
  if (verbose) cout << "type " << id << " " << name << std::endl;
  type_id[name] = id;
  crush.set_type_name(id, name.c_str());
}

void parse_bucket(iter_t const& i, CrushWrapper &crush)
{
  string tname = string_node(i->children[0]);
  if (!type_id.count(tname)) {
    cerr << "bucket type '" << tname << "' is not defined" << std::endl;
    exit(1);
  }
  int type = type_id[tname];

  string name = string_node(i->children[1]);
  if (item_id.count(name)) {
    cerr << "bucket or device '" << name << "' is already defined" << std::endl;
    exit(1);
  }

  int id = 0;  // none, yet!
  int alg = -1;
  int hash = -1;
  set<int> used_items;
  int size = 0;
  
  for (unsigned p=3; p<i->children.size()-1; p++) {
    iter_t sub = i->children.begin() + p;
    string tag = string_node(sub->children[0]);
    //cout << "tag " << tag << std::endl;
    if (tag == "id") 
      id = int_node(sub->children[1]);
    else if (tag == "alg") {
      string a = string_node(sub->children[1]);
      if (a == "uniform")
	alg = CRUSH_BUCKET_UNIFORM;
      else if (a == "list")
	alg = CRUSH_BUCKET_LIST;
      else if (a == "tree")
	alg = CRUSH_BUCKET_TREE;
      else if (a == "straw")
	alg = CRUSH_BUCKET_STRAW;
      else {
	cerr << "unknown bucket alg '" << a << "'" << std::endl;
	exit(1);
      }
    }
    else if (tag == "hash") {
      string a = string_node(sub->children[1]);
      if (a == "rjenkins1")
	hash = CRUSH_HASH_RJENKINS1;
      else
	hash = atoi(a.c_str());
    }
    else if (tag == "item") {
      // first, just determine which item pos's are already used
      size++;
      for (unsigned q = 2; q < sub->children.size(); q++) {
	string tag = string_node(sub->children[q++]);
	if (tag == "pos") {
	  int pos = int_node(sub->children[q]);
	  if (used_items.count(pos)) {
	    cerr << "item '" << string_node(sub->children[1]) << "' in bucket '" << name << "' has explicit pos " << pos << ", which is occupied" << std::endl;
	    exit(1);
	  }
	  used_items.insert(pos);
	}
      }
    }
    else assert(0);
  }

  // now do the items.
  if (!used_items.empty())
    size = MAX(size, *used_items.rbegin());
  vector<int> items(size);
  vector<int> weights(size);

  int curpos = 0;
  float bucketweight = 0;
  for (unsigned p=3; p<i->children.size()-1; p++) {
    iter_t sub = i->children.begin() + p;
    string tag = string_node(sub->children[0]);
    if (tag == "item") {

      string iname = string_node(sub->children[1]);
      if (!item_id.count(iname)) {
	cerr << "item '" << iname << "' in bucket '" << name << "' is not defined" << std::endl;
	exit(1);
      }
      int itemid = item_id[iname];

      float weight = 1.0;
      if (item_weight.count(itemid))
	weight = item_weight[itemid];

      int pos = -1;
      for (unsigned q = 2; q < sub->children.size(); q++) {
	string tag = string_node(sub->children[q++]);
	if (tag == "weight")
	  weight = float_node(sub->children[q]);
	else if (tag == "pos") 
	  pos = int_node(sub->children[q]);
	else
	  assert(0);
      }
      if (pos >= size) {
	cerr << "item '" << iname << "' in bucket '" << name << "' has pos " << pos << " >= size " << size << std::endl;
	exit(1);
      }
      if (pos < 0) {
	while (used_items.count(curpos)) curpos++;
	pos = curpos++;
      }
      //cout << " item " << iname << " (" << itemid << ") pos " << pos << " weight " << weight << std::endl;
      items[pos] = itemid;
      weights[pos] = (unsigned)(weight * 0x10000);
      bucketweight += weight;
    }
  }

  if (id == 0) {
    for (id=-1; id_item.count(id); id--) ;
    //cout << "assigned id " << id << std::endl;
  }

  if (verbose) cout << "bucket " << name << " (" << id << ") " << size << " items and weight " << bucketweight << std::endl;
  id_item[id] = name;
  item_id[name] = id;
  item_weight[id] = bucketweight;
  
  crush.add_bucket(id, alg, hash, type, size, &items[0], &weights[0]);
  crush.set_item_name(id, name.c_str());
}

void parse_rule(iter_t const& i, CrushWrapper &crush)
{
  int start;  // rule name is optional!
 
  string rname = string_node(i->children[1]);
  if (rname != "{") {
    if (rule_id.count(rname)) {
      cerr << "rule name '" << rname << "' already defined\n" << std::endl;
      exit(1);
    }
    start = 4;
  } else {
    rname = string();
    start = 3;
  }

  int ruleset = int_node(i->children[start]);

  string tname = string_node(i->children[start+2]);
  int type;
  if (tname == "replicated")
    type = CEPH_PG_TYPE_REP;
  else if (tname == "raid4") 
    type = CEPH_PG_TYPE_RAID4;
  else 
    assert(0);    

  int minsize = int_node(i->children[start+4]);
  int maxsize = int_node(i->children[start+6]);
  
  int steps = i->children.size() - start - 8;
  //cout << "num steps " << steps << std::endl;
  
  int ruleno = crush.add_rule(steps, ruleset, type, minsize, maxsize, -1);
  if (rname.length()) {
    crush.set_rule_name(ruleno, rname.c_str());
    rule_id[rname] = ruleno;
  }

  int step = 0;
  for (iter_t p = i->children.begin() + start + 7; step < steps; p++) {
    iter_t s = p->children.begin() + 1;
    int stepid = s->value.id().to_long();
    switch (stepid) {
    case crush_grammar::_step_take: 
      {
	string item = string_node(s->children[1]);
	if (!item_id.count(item)) {
	  cerr << "in rule '" << rname << "' item '" << item << "' not defined" << std::endl;
	  exit(1);
	}
	crush.set_rule_step_take(ruleno, step++, item_id[item]);
      }
      break;

    case crush_grammar::_step_choose:
    case crush_grammar::_step_chooseleaf:
      {
	string type = string_node(s->children[4]);
	if (!type_id.count(type)) {
	  cerr << "in rule '" << rname << "' type '" << type << "' not defined" << std::endl;
	  exit(1);
	}
	string choose = string_node(s->children[0]);
	string mode = string_node(s->children[1]);
	if (choose == "choose") {
	  if (mode == "firstn")
	    crush.set_rule_step_choose_firstn(ruleno, step++, int_node(s->children[2]), type_id[type]);
	  else if (mode == "indep")
	    crush.set_rule_step_choose_indep(ruleno, step++, int_node(s->children[2]), type_id[type]);
	  else assert(0);
	} else if (choose == "chooseleaf") {
	  if (mode == "firstn") 
	    crush.set_rule_step_choose_leaf_firstn(ruleno, step++, int_node(s->children[2]), type_id[type]);
	  else if (mode == "indep")
	    crush.set_rule_step_choose_leaf_indep(ruleno, step++, int_node(s->children[2]), type_id[type]);
	  else assert(0);
	} else assert(0);
      }
      break;

    case crush_grammar::_step_emit:
      crush.set_rule_step_emit(ruleno, step++);
      break;

    default:
      cerr << "bad crush step " << stepid << std::endl;
      assert(0);
    }
  }
  assert(step == steps);
}

void dump(iter_t const& i, int ind=1) 
{
  cout << "dump"; 
  for (int j=0; j<ind; j++) cout << "\t"; 
  long id = i->value.id().to_long();
  cout << id << "\t"; 
  cout << "'" << string(i->value.begin(), i->value.end())  
       << "' " << i->children.size() << " children" << std::endl; 
  for (unsigned int j = 0; j < i->children.size(); j++)  
    dump(i->children.begin() + j, ind+1); 
}

void find_used_bucket_ids(iter_t const& i)
{
  for (iter_t p = i->children.begin(); p != i->children.end(); p++) {
    if ((int)p->value.id().to_long() == crush_grammar::_bucket) {
      iter_t firstline = p->children.begin() + 3;
      string tag = string_node(firstline->children[0]);
      if (tag == "id") {
	int id = int_node(firstline->children[1]);
	//cout << "saw bucket id " << id << std::endl;
	id_item[id] = string();
      }
    }
  }
}

void parse_crush(iter_t const& i, CrushWrapper &crush) 
{ 
  find_used_bucket_ids(i);

  for (iter_t p = i->children.begin(); p != i->children.end(); p++) {
    switch (p->value.id().to_long()) {
    case crush_grammar::_device: 
      parse_device(p, crush);
      break;
    case crush_grammar::_bucket_type: 
      parse_bucket_type(p, crush);
      break;
    case crush_grammar::_bucket: 
      parse_bucket(p, crush);
      break;
    case crush_grammar::_crushrule: 
      parse_rule(p, crush);
      break;
    default:
      assert(0);
    }
  }

  //cout << "max_devices " << crush.get_max_devices() << std::endl;
  crush.finalize();
  
} 

const char *infn = "stdin";


////////////////////////////////////////////////////////////////////////////

string consolidate_whitespace(string in)
{
  string out;

  bool white = false;
  for (unsigned p=0; p<in.length(); p++) {
    if (in[p] == ' ' || in[p] == '\t') {
      if (white)
	continue;
      white = true;
    } else {
      if (white) {
	if (out.length()) out += " ";
	white = false;
      }
      out += in[p];
    }
  }
  if (verbose > 3)
    cout << " \"" << in << "\" -> \"" << out << "\"" << std::endl;
  return out;
}

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
    
    if (verbose>1) cout << line << ": " << str << std::endl;

    // work around spirit crankiness by removing extraneous
    // whitespace.  there is probably a more elegant solution, but
    // this only broke with the latest spirit (with the switchover to
    // "classic"), i don't want to spend too much time figuring it
    // out.
    string stripped = consolidate_whitespace(str);
    if (stripped.length() && big.length() && big[big.length()-1] != ' ') big += " ";

    line_pos[big.length()] = line;
    line++;
    big += stripped;
  }
  
  if (verbose > 2) cout << "whole file is: \"" << big << "\"" << std::endl;
  
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
    return -1;
  }

  //cout << "parsing succeeded\n";
  //dump(info.trees.begin());
  parse_crush(info.trees.begin(), crush);
  
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
  char s[20];
  snprintf(s, sizeof(s), "%.3f", (float)i / (float)0x10000);
  out << s;
}

enum dcb_state_t {
  DCB_STATE_IN_PROGRESS = 0,
  DCB_STATE_DONE
};

static int decompile_crush_bucket_impl(int i,
			    CrushWrapper &crush, ostream &out)
{
  int type = crush.get_bucket_type(i);
  print_type_name(out, type, crush);
  out << " ";
  print_item_name(out, i, crush);
  out << " {\n";
  out << "\tid " << i << "\t\t# do not change unnecessarily\n";

  int n = crush.get_bucket_size(i);

  int alg = crush.get_bucket_alg(i);
  out << "\talg " << crush_bucket_alg_name(alg);

  // notate based on alg type
  bool dopos = false;
  switch (alg) {
  case CRUSH_BUCKET_UNIFORM:
    out << "\t# do not change bucket size (" << n << ") unnecessarily";
    dopos = true;
    break;
  case CRUSH_BUCKET_LIST:
    out << "\t# add new items at the end; do not change order unnecessarily";
    break;
  case CRUSH_BUCKET_TREE:
    out << "\t# do not change pos for existing items unnecessarily";
    dopos = true;
    break;
  }
  out << "\n";

  int hash = crush.get_bucket_hash(i);
  out << "\thash " << hash << "\t# " << crush_hash_name(hash) << "\n";

  for (int j=0; j<n; j++) {
    int item = crush.get_bucket_item(i, j);
    int w = crush.get_bucket_item_weight(i, j);
    out << "\titem ";
    print_item_name(out, item, crush);
    out << " weight ";
    print_fixedpoint(out, w);
    if (dopos) {
      if (alg == CRUSH_BUCKET_TREE)
	out << " pos " << (j-1)/2;
      else
	out << " pos " << j;
    }
    out << "\n";
  }
  out << "}\n";
  return 0;
}

/* Basically, we just descend recursively into all of the buckets,
 * executing a depth-first traversal of the graph. Since the buckets form a
 * directed acyclic graph, this should work just fine. The graph isn't
 * necessarily a tree, so we have to keep track of what buckets we already
 * outputted. We don't want to output anything twice. We also keep track of
 * what buckets are in progress so that we can detect cycles. These can
 * arise through user error.
 */
static int decompile_crush_bucket(int cur,
		    std::map<int, dcb_state_t> &dcb_states,
		    CrushWrapper &crush, ostream &out)
{
  if ((cur == 0) || (!crush.bucket_exists(cur)))
    return 0;

  std::map<int, dcb_state_t>::iterator c = dcb_states.find(cur);
  if (c == dcb_states.end()) {
    // Mark this bucket as "in progress."
    std::map<int, dcb_state_t>::value_type val(cur, DCB_STATE_IN_PROGRESS);
    std::pair <std::map<int, dcb_state_t>::iterator, bool> rval
      (dcb_states.insert(val));
    assert(rval.second);
    c = rval.first;
  }
  else if (c->second == DCB_STATE_DONE) {
    // We already did this bucket.
    return 0;
  }
  else if (c->second == DCB_STATE_IN_PROGRESS) {
    cout << "decompile_crush_bucket: logic error: tried to decompile "
	"a bucket that is already being decompiled" << std::endl;
    return -EBADE;
  }
  else {
    cout << "decompile_crush_bucket: logic error: illegal bucket state! "
	 << c->second << std::endl;
    return -EBADE;
  }

  int bsize = crush.get_bucket_size(cur);
  for (int i = 0; i < bsize; ++i) {
    int item = crush.get_bucket_item(cur, i);
    std::map<int, dcb_state_t>::iterator d = dcb_states.find(item);
    if (d == dcb_states.end()) {
      int ret = decompile_crush_bucket(item, dcb_states, crush, out);
      if (ret)
	return ret;
    }
    else if (d->second == DCB_STATE_IN_PROGRESS) {
      cout << "decompile_crush_bucket: error: while trying to output bucket "
	   << cur << ", we found out that it contains one of the buckets that "
	   << "contain it. This is not allowed. The buckets must form a "
	   <<  "directed acyclic graph." << std::endl;
      return -EINVAL;
    }
    else if (d->second != DCB_STATE_DONE) {
      cout << "decompile_crush_bucket: logic error: illegal bucket state "
	   << d->second << std::endl;
      return -EBADE;
    }
  }
  decompile_crush_bucket_impl(cur, crush, out);
  c->second = DCB_STATE_DONE;
  return 0;
}

int decompile_crush(CrushWrapper &crush, ostream &out)
{
  out << "# begin crush map\n\n";

  out << "# devices\n";
  for (int i=0; i<crush.get_max_devices(); i++) {
    //if (crush.get_item_name(i) == 0)
    //continue;
    out << "device " << i << " ";
    print_item_name(out, i, crush);
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
  std::map<int, dcb_state_t> dcb_states;
  for (int bucket = -1; bucket > -1-crush.get_max_buckets(); --bucket) {
    int ret = decompile_crush_bucket(bucket, dcb_states, crush, out);
    if (ret)
      return ret;
  }

  out << "\n# rules\n";
  for (int i=0; i<crush.get_max_rules(); i++) {
    if (!crush.rule_exists(i)) continue;
    out << "rule ";
    if (crush.get_rule_name(i))
      print_rule_name(out, i, crush);
    out << " {\n";
    out << "\truleset " << crush.get_rule_mask_ruleset(i) << "\n";
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
      case CRUSH_RULE_CHOOSE_LEAF_FIRSTN:
	out << "\tstep chooseleaf firstn "
	    << crush.get_rule_arg1(i, j) 
	    << " type ";
	print_type_name(out, crush.get_rule_arg2(i, j), crush);
	out << "\n";
	break;
      case CRUSH_RULE_CHOOSE_LEAF_INDEP:
	out << "\tstep chooseleaf indep "
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


void usage()
{
  cout << "usage: crushtool ...\n";
  cout << "   --decompile|-d map    decompile a crush map to source\n";
  cout << "   --compile|-c map.txt  compile a map from source\n";
  cout << "   [-o outfile [--clobber]]\n";
  cout << "                         specify output for for (de)compilation\n";
  cout << "   --build --num_osd N layer1 ...\n";
  cout << "                         build a new map, where each 'layer' is\n";
  cout << "                           'name (uniform|straw|list|tree) size'\n";
  cout << "   --test mapfn          test a range of inputs on the map\n";
  cout << "      [--min-x x] [--max-x x] [--x x]\n";
  cout << "      [--min-rule r] [--max-rule r] [--rule r]\n";
  cout << "      [--num-rep n]\n";
  cout << "      [--weight|-w devno weight]\n";
  cout << "                         where weight is 0 to 1.0\n";
  exit(1);
}

struct bucket_types_t {
  const char *name;
  int type;
} bucket_types[] = {
  { "uniform", CRUSH_BUCKET_UNIFORM },
  { "list", CRUSH_BUCKET_LIST },
  { "straw", CRUSH_BUCKET_STRAW },
  { "tree", CRUSH_BUCKET_TREE },
  { 0, 0 },
};

struct layer_t {
  const char *name;
  const char *buckettype;
  int size;
};

int main(int argc, const char **argv)
{
  vector<const char*> args;
  argv_to_vec(argc, argv, args);

  const char *me = argv[0];
  const char *infn = 0;
  const char *srcfn = 0;
  bool compile = false;
  bool decompile = false;
  bool test = false;
  const char *outfn = 0;
  bool clobber = false;

  int build = 0;
  int num_osds =0;
  vector<layer_t> layers;
  int num_rep = 2;
  int min_x = 0, max_x = 10000-1;
  int min_rule = 0, max_rule = 1000;
  map<int, int> device_weight;
  DEFINE_CONF_VARS(usage);

  FOR_EACH_ARG(args) {
    if (CONF_ARG_EQ("clobber", '\0')) {
      clobber = true;
    } else if (CONF_ARG_EQ("decompile", 'd')) {
      CONF_SAFE_SET_ARG_VAL(&infn, OPT_STR);
      decompile = true;
    } else if (CONF_ARG_EQ("outfn", 'o')) {
      CONF_SAFE_SET_ARG_VAL(&outfn, OPT_STR);
    } else if (CONF_ARG_EQ("compile", 'c')) {
      CONF_SAFE_SET_ARG_VAL(&srcfn, OPT_STR);
      compile = true;
    } else if (CONF_ARG_EQ("test", 't')) {
      CONF_SAFE_SET_ARG_VAL(&infn, OPT_STR);
      test = true;
    } else if (CONF_ARG_EQ("verbose", 'v')) {
      verbose++;
    } else if (CONF_ARG_EQ("build", '\0')) {
      CONF_SAFE_SET_ARG_VAL(&build, OPT_BOOL);
    } else if (CONF_ARG_EQ("num_osds", '\0')) {
      CONF_SAFE_SET_ARG_VAL(&num_osds, OPT_INT);
    } else if (CONF_ARG_EQ("num_rep", '\0')) {
      CONF_SAFE_SET_ARG_VAL(&num_rep, OPT_INT);
    } else if (CONF_ARG_EQ("max_x", '\0')) {
      CONF_SAFE_SET_ARG_VAL(&max_x, OPT_INT);
    } else if (CONF_ARG_EQ("min_x", '\0')) {
      CONF_SAFE_SET_ARG_VAL(&min_x, OPT_INT);
    } else if (CONF_ARG_EQ("x", '\0')) {
      CONF_SAFE_SET_ARG_VAL(&min_x, OPT_INT);
      max_x = min_x;
    } else if (CONF_ARG_EQ("max_rule", '\0')) {
      CONF_SAFE_SET_ARG_VAL(&max_rule, OPT_INT);
    } else if (CONF_ARG_EQ("min_rule", '\0')) {
      CONF_SAFE_SET_ARG_VAL(&min_rule, OPT_INT);
    } else if (CONF_ARG_EQ("rule", '\0')) {
      CONF_SAFE_SET_ARG_VAL(&min_rule, OPT_INT);
      max_rule = min_rule;
    } else if (CONF_ARG_EQ("weight", 'w')) {
      int dev;
      CONF_SAFE_SET_ARG_VAL(&dev, OPT_INT);
      float f;
      CONF_SAFE_SET_ARG_VAL(&f, OPT_FLOAT);
      int w = f * (float)0x10000;
      if (w < 0)
	w = 0;
      if (w > 0x10000)
	w = 0x10000;
      device_weight[dev] = w;
    } else if (!build)
      usage();
    else if (i + 3 <= args.size()) {
      layer_t l;
      l.name = args[i++];
      l.buckettype = args[i++];
      l.size = atoi(args[i]);
      layers.push_back(l);
    }      
  }
  if (decompile + compile + build > 1)
    usage();
  if (!compile && !decompile && !build && !test)
    usage();

  /*
  if (outfn) cout << "outfn " << outfn << std::endl;
  if (cinfn) cout << "cinfn " << cinfn << std::endl;
  if (dinfn) cout << "dinfn " << dinfn << std::endl;
  */

  CrushWrapper crush;

  if (infn) {
    bufferlist bl;
    int r = bl.read_file(infn);
    if (r < 0) {
      char buf[80];
      cerr << me << ": error reading '" << infn << "': " << strerror_r(-r, buf, sizeof(buf)) << std::endl;
      exit(1);
    }
    bufferlist::iterator p = bl.begin();
    crush.decode(p);
  }

  if (decompile) {
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

  if (compile) {
    crush.create();
    int r = compile_crush_file(srcfn, crush);
    crush.finalize();
    if (r < 0) 
      exit(1);
    if (!outfn)
      cout << me << " successfully compiled '" << srcfn << "'.  Use -o file to write it out." << std::endl;
  }

  if (build) {
    if (layers.empty()) {
      cerr << me << ": must specify at least one layer" << std::endl;
      exit(1);
    }

    crush.create();

    vector<int> lower_items;
    vector<int> lower_weights;

    for (int i=0; i<num_osds; i++) {
      lower_items.push_back(i);
      lower_weights.push_back(0x10000);
    }

    int type = 1;
    int rootid = 0;
    for (vector<layer_t>::iterator p = layers.begin(); p != layers.end(); p++, type++) {
      layer_t &l = *p;

      dout(0) << "layer " << type
	      << "  " << l.name
	      << "  bucket type " << l.buckettype
	      << "  " << l.size 
	      << dendl;

      crush.set_type_name(type, l.name);

      int buckettype = -1;
      for (int i = 0; i < (int)(sizeof(bucket_types)/sizeof(bucket_types[0])); i++)
	if (strcmp(l.buckettype, bucket_types[i].name) == 0) {
	  buckettype = bucket_types[i].type;
	  break;
	}
      if (buckettype < 0) {
	cerr << "unknown bucket type '" << l.buckettype << "'" << std::endl;
	exit(1);
      }

      // build items
      vector<int> cur_items;
      vector<int> cur_weights;
      unsigned lower_pos = 0;  // lower pos

      dout(0) << "lower_items " << lower_items << dendl;
      dout(0) << "lower_weights " << lower_weights << dendl;

      int i = 0;
      while (1) {
	if (lower_pos == lower_items.size())
	  break;

	int items[num_osds];
	int weights[num_osds];

	int weight = 0;
	int j;
	for (j=0; j<l.size || l.size==0; j++) {
	  if (lower_pos == lower_items.size())
	    break;
	  items[j] = lower_items[lower_pos];
	  weights[j] = lower_weights[lower_pos];
	  weight += weights[j];
	  lower_pos++;
	  dout(0) << "  item " << items[j] << " weight " << weights[j] << dendl;
	}

	crush_bucket *b = crush_make_bucket(buckettype, CRUSH_HASH_DEFAULT, type, j, items, weights);
	int id = crush_add_bucket(crush.crush, 0, b);
	rootid = id;

	char format[20];
	if (l.size)
	  snprintf(format, sizeof(format), "%s%%d", l.name);
	else
	  strcpy(format, l.name);
	char name[20];
	snprintf(name, sizeof(name), format, i);
	crush.set_item_name(id, name);

	dout(0) << " in bucket " << id << " '" << name << "' size " << j << " weight " << weight << dendl;

	cur_items.push_back(id);
	cur_weights.push_back(weight);
	i++;
      }

      lower_items.swap(cur_items);
      lower_weights.swap(cur_weights);
    }
    
    // make a generic rules
    int ruleset=1;
    crush_rule *rule = crush_make_rule(3, ruleset, CEPH_PG_TYPE_REP, 2, 2);
    crush_rule_set_step(rule, 0, CRUSH_RULE_TAKE, rootid, 0);
    crush_rule_set_step(rule, 1, CRUSH_RULE_CHOOSE_LEAF_FIRSTN, CRUSH_CHOOSE_N, 1);
    crush_rule_set_step(rule, 2, CRUSH_RULE_EMIT, 0, 0);
    int rno = crush_add_rule(crush.crush, rule, -1);
    crush.set_rule_name(rno, "data");

    crush.finalize();
    dout(0) << "crush max_devices " << crush.crush->max_devices << dendl;

    if (!outfn)
      cout << me << " successfully built map.  Use -o file to write it out." << std::endl;
  }
  if (compile || build) {
    if (outfn) {
      bufferlist bl;
      crush.encode(bl);
      int r = bl.write_file(outfn);
      if (r < 0) {
	char buf[80];
	cerr << me << ": error writing '" << outfn << "': " << strerror_r(-r, buf, sizeof(buf)) << std::endl;
	exit(1);
      }
      if (verbose)
	cout << "wrote crush map to " << outfn << std::endl;
    }
  }

  if (test) {

    // all osds in
    vector<__u32> weight;
    for (int o = 0; o < crush.get_max_devices(); o++)
      if (device_weight.count(o))
	weight.push_back(device_weight[o]);
      else
	weight.push_back(0x10000);
    cout << "devices weights (hex): " << hex << weight << dec << std::endl;

    for (int r = min_rule; r < crush.get_max_rules() && r <= max_rule; r++) {
      if (!crush.rule_exists(r)) {
	cout << "rule " << r << " dne" << std::endl;
	continue;
      }
      cout << "rule " << r << " (" << crush.get_rule_name(r) << "), x = " << min_x << ".." << max_x << std::endl;
      vector<int> per(crush.get_max_devices());
      map<int,int> sizes;
      for (int x = min_x; x <= max_x; x++) {
	vector<int> out;
	crush.do_rule(r, x, out, num_rep, -1, weight);
	//cout << "rule " << r << " x " << x << " " << out << std::endl;
	for (unsigned i = 0; i < out.size(); i++)
	  per[out[i]]++;
	sizes[out.size()]++;
      }
      for (unsigned i = 0; i < per.size(); i++)
	cout << " device " << i << ":\t" << per[i] << std::endl;
      for (map<int,int>::iterator p = sizes.begin(); p != sizes.end(); p++)
	cout << " num results " << p->first << ":\t" << p->second << std::endl;
    }
  }

  return 0;
}
