// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_CRUSH_COMPILER_H
#define CEPH_CRUSH_COMPILER_H

#include "crush/CrushWrapper.h"
#include "crush/grammar.h"

#include <map>
#include <ostream>
#include <functional>

class CrushCompiler {
  CrushWrapper& crush;
  ostream& err;
  int verbose;
  bool unsafe_tunables;

  // decompile
  enum dcb_state_t {
    DCB_STATE_IN_PROGRESS = 0,
    DCB_STATE_DONE
  };

  int decompile_bucket_impl(int i, ostream &out);
  int decompile_bucket(int cur,
		       std::map<int, dcb_state_t>& dcb_states,
		       ostream &out);

  // compile
  typedef char const*         iterator_t;
  typedef tree_match<iterator_t> parse_tree_match_t;
  typedef parse_tree_match_t::tree_iterator iter_t;
  typedef parse_tree_match_t::node_t node_t;

  map<string, int> item_id;
  map<int, string> id_item;
  map<int, unsigned> item_weight;
  map<string, int> type_id;
  map<string, int> rule_id;
  map<int, string> ruleset_name;
  string string_node(node_t &node);
  int int_node(node_t &node); 
  float float_node(node_t &node);

  int parse_tunable(iter_t const& i);
  int parse_device(iter_t const& i);
  int parse_bucket_type(iter_t const& i);
  int parse_bucket(iter_t const& i);
  int parse_rule(iter_t const& i);
  void find_used_bucket_ids(iter_t const& i);
  int parse_crush(iter_t const& i);  
  void dump(iter_t const& i, int ind=1);

  string consolidate_whitespace(string in);

public:
  CrushCompiler(CrushWrapper& c, ostream& eo, int verbosity=0)
    : crush(c), err(eo), verbose(verbosity),
      unsafe_tunables(false) {}
  ~CrushCompiler() {}

  void enable_unsafe_tunables() {
    unsafe_tunables = true;
  }

  int decompile(ostream& out);
  int compile(istream& in, const char *infn=0);
};

#endif
