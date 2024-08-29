// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_CRUSH_COMPILER_H
#define CEPH_CRUSH_COMPILER_H

#include "crush/CrushWrapper.h"
#include "crush/grammar.h"

#include <map>
#include <iostream>

class CrushCompiler {
  CrushWrapper& crush;
  std::ostream& err;
  int verbose;
  bool unsafe_tunables;

  // decompile
  enum dcb_state_t {
    DCB_STATE_IN_PROGRESS = 0,
    DCB_STATE_DONE
  };

  int decompile_weight_set_weights(crush_weight_set weight_set,
				   std::ostream &out);
  int decompile_weight_set(crush_weight_set *weight_set,
			   __u32 size,
			   std::ostream &out);
  int decompile_choose_arg(crush_choose_arg *arg,
			   int bucket_id,
			   std::ostream &out);
  int decompile_ids(int *ids,
		    __u32 size,
		    std::ostream &out);
  int decompile_choose_arg_map(crush_choose_arg_map arg_map,
			       std::ostream &out);
  int decompile_choose_args(const std::pair<const long unsigned int, crush_choose_arg_map> &i,
			    std::ostream &out);
  int decompile_bucket_impl(int i, std::ostream &out);
  int decompile_bucket(int cur,
		       std::map<int, dcb_state_t>& dcb_states,
		       std::ostream &out);

  // compile
  typedef char const*         iterator_t;
  typedef boost::spirit::tree_match<iterator_t> parse_tree_match_t;
  typedef parse_tree_match_t::tree_iterator iter_t;
  typedef parse_tree_match_t::node_t node_t;

  std::map<std::string, int> item_id;
  std::map<int, std::string> id_item;
  std::map<int, unsigned> item_weight;
  std::map<std::string, int> type_id;
  std::map<std::string, int> rule_id;
  std::map<int32_t, std::map<int32_t, int32_t> > class_bucket; // bucket id -> class id -> shadow bucket id

  std::string string_node(node_t &node);
  int int_node(node_t &node); 
  float float_node(node_t &node);

  int parse_tunable(iter_t const& i);
  int parse_device(iter_t const& i);
  int parse_bucket_type(iter_t const& i);
  int parse_bucket(iter_t const& i);
  int parse_rule(iter_t const& i);
  int parse_weight_set_weights(iter_t const& i, int bucket_id, crush_weight_set *weight_set);
  int parse_weight_set(iter_t const& i, int bucket_id, crush_choose_arg *arg);
  int parse_choose_arg_ids(iter_t const& i, int bucket_id, crush_choose_arg *args);
  int parse_choose_arg(iter_t const& i, crush_choose_arg *args);
  int parse_choose_args(iter_t const& i);
  void find_used_bucket_ids(iter_t const& i);
  int parse_crush(iter_t const& i);  
  void dump(iter_t const& i, int ind=1);
  std::string consolidate_whitespace(std::string in);
  int adjust_bucket_item_place(iter_t const &i);

public:
  CrushCompiler(CrushWrapper& c, std::ostream& eo, int verbosity=0)
    : crush(c), err(eo), verbose(verbosity),
      unsafe_tunables(false) {}
  ~CrushCompiler() {}

  void enable_unsafe_tunables() {
    unsafe_tunables = true;
  }

  int decompile(std::ostream& out);
  int compile(std::istream& in, const char *infn=0);
};

#endif
