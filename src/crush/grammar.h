// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2008 Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef CEPH_CRUSH_GRAMMAR_H
#define CEPH_CRUSH_GRAMMAR_H

//#define BOOST_SPIRIT_DEBUG

#ifdef USE_BOOST_SPIRIT_OLD_HDR
#include <boost/spirit/core.hpp>
#include <boost/spirit/tree/ast.hpp>
#include <boost/spirit/tree/tree_to_xml.hpp>
#else
#define BOOST_SPIRIT_USE_OLD_NAMESPACE
#include <boost/spirit/include/classic_core.hpp>
#include <boost/spirit/include/classic_ast.hpp>
#include <boost/spirit/include/classic_tree_to_xml.hpp>
#endif
using namespace boost::spirit;

struct crush_grammar : public boost::spirit::grammar<crush_grammar>
{
  enum {
    _int = 1,
    _posint,
    _negint,
    _name,
    _device,
    _bucket_type,
    _bucket_id,
    _bucket_alg,
    _bucket_hash,
    _bucket_item,
    _bucket,
    _step_take,
    _step_set_chooseleaf_tries,
    _step_set_chooseleaf_vary_r,
    _step_set_chooseleaf_stable,
    _step_set_choose_tries,
    _step_set_choose_local_tries,
    _step_set_choose_local_fallback_tries,
    _step_set_msr_descents,
    _step_set_msr_collision_tries,
    _step_choose,
    _step_chooseleaf,
    _step_emit,
    _step,
    _crushrule,
    _weight_set_weights,
    _weight_set,
    _choose_arg_ids,
    _choose_arg,
    _choose_args,
    _crushmap,
    _tunable,
  };

  template <typename ScannerT>
  struct definition
  {
    boost::spirit::rule<ScannerT, boost::spirit::parser_context<>,boost::spirit::parser_tag<_int> >      integer;
    boost::spirit::rule<ScannerT, boost::spirit::parser_context<>, boost::spirit::parser_tag<_posint> >      posint;
    boost::spirit::rule<ScannerT, boost::spirit::parser_context<>, boost::spirit::parser_tag<_negint> >      negint;
    boost::spirit::rule<ScannerT, boost::spirit::parser_context<>, boost::spirit::parser_tag<_name> >      name;

    boost::spirit::rule<ScannerT, boost::spirit::parser_context<>, boost::spirit::parser_tag<_tunable> >      tunable;

    boost::spirit::rule<ScannerT, boost::spirit::parser_context<>, boost::spirit::parser_tag<_device> >      device;

    boost::spirit::rule<ScannerT, boost::spirit::parser_context<>, boost::spirit::parser_tag<_bucket_type> >    bucket_type;

    boost::spirit::rule<ScannerT, boost::spirit::parser_context<>, boost::spirit::parser_tag<_bucket_id> >      bucket_id;
    boost::spirit::rule<ScannerT, boost::spirit::parser_context<>, boost::spirit::parser_tag<_bucket_alg> >     bucket_alg;
    boost::spirit::rule<ScannerT, boost::spirit::parser_context<>, boost::spirit::parser_tag<_bucket_hash> >    bucket_hash;
    boost::spirit::rule<ScannerT, boost::spirit::parser_context<>, boost::spirit::parser_tag<_bucket_item> >    bucket_item;
    boost::spirit::rule<ScannerT, boost::spirit::parser_context<>, boost::spirit::parser_tag<_bucket> >      bucket;

    boost::spirit::rule<ScannerT, boost::spirit::parser_context<>, boost::spirit::parser_tag<_step_take> >      step_take;
    boost::spirit::rule<ScannerT, boost::spirit::parser_context<>, boost::spirit::parser_tag<_step_set_choose_tries> >    step_set_choose_tries;
    boost::spirit::rule<ScannerT, boost::spirit::parser_context<>, boost::spirit::parser_tag<_step_set_choose_local_tries> >    step_set_choose_local_tries;
    boost::spirit::rule<ScannerT, boost::spirit::parser_context<>, boost::spirit::parser_tag<_step_set_choose_local_fallback_tries> >    step_set_choose_local_fallback_tries;
    boost::spirit::rule<ScannerT, boost::spirit::parser_context<>, boost::spirit::parser_tag<_step_set_chooseleaf_tries> >    step_set_chooseleaf_tries;
    boost::spirit::rule<ScannerT, boost::spirit::parser_context<>, boost::spirit::parser_tag<_step_set_chooseleaf_vary_r> >    step_set_chooseleaf_vary_r;
    boost::spirit::rule<ScannerT, boost::spirit::parser_context<>, boost::spirit::parser_tag<_step_set_chooseleaf_stable> >    step_set_chooseleaf_stable;
    boost::spirit::rule<ScannerT, boost::spirit::parser_context<>, boost::spirit::parser_tag<_step_set_msr_descents> >    step_set_msr_descents;
    boost::spirit::rule<ScannerT, boost::spirit::parser_context<>, boost::spirit::parser_tag<_step_set_msr_collision_tries> >    step_set_msr_collision_tries;
    boost::spirit::rule<ScannerT, boost::spirit::parser_context<>, boost::spirit::parser_tag<_step_choose> >    step_choose;
    boost::spirit::rule<ScannerT, boost::spirit::parser_context<>, boost::spirit::parser_tag<_step_chooseleaf> >      step_chooseleaf;
    boost::spirit::rule<ScannerT, boost::spirit::parser_context<>, boost::spirit::parser_tag<_step_emit> >      step_emit;
    boost::spirit::rule<ScannerT, boost::spirit::parser_context<>, boost::spirit::parser_tag<_step> >      step;
    boost::spirit::rule<ScannerT, boost::spirit::parser_context<>, boost::spirit::parser_tag<_crushrule> >      crushrule;
    boost::spirit::rule<ScannerT, boost::spirit::parser_context<>, boost::spirit::parser_tag<_weight_set_weights> >     weight_set_weights;
    boost::spirit::rule<ScannerT, boost::spirit::parser_context<>, boost::spirit::parser_tag<_weight_set> >     weight_set;
    boost::spirit::rule<ScannerT, boost::spirit::parser_context<>, boost::spirit::parser_tag<_choose_arg_ids> >     choose_arg_ids;
    boost::spirit::rule<ScannerT, boost::spirit::parser_context<>, boost::spirit::parser_tag<_choose_arg> >     choose_arg;
    boost::spirit::rule<ScannerT, boost::spirit::parser_context<>, boost::spirit::parser_tag<_choose_args> >     choose_args;

    boost::spirit::rule<ScannerT, boost::spirit::parser_context<>, boost::spirit::parser_tag<_crushmap> >      crushmap;

    definition(crush_grammar const& /*self*/)
    {
      using boost::spirit::leaf_node_d;
      using boost::spirit::lexeme_d;
      using boost::spirit::str_p;
      using boost::spirit::ch_p;
      using boost::spirit::digit_p;
      using boost::spirit::alnum_p;
      using boost::spirit::real_p;

      // base types
      integer     =   leaf_node_d[ lexeme_d[
					    (!ch_p('-') >> +digit_p)
					    ] ];
      posint     =   leaf_node_d[ lexeme_d[ +digit_p ] ];
      negint     =   leaf_node_d[ lexeme_d[ ch_p('-') >> +digit_p ] ];
      name = leaf_node_d[ lexeme_d[ +( alnum_p || ch_p('-') || ch_p('_') || ch_p('.')) ] ];

      // tunables
      tunable = str_p("tunable") >> name >> posint;

      // devices
      device = str_p("device") >> posint >> name >> !( str_p("class") >> name );

      // bucket types
      bucket_type = str_p("type") >> posint >> name;

      // buckets
      bucket_id = str_p("id") >> negint >> !( str_p("class") >> name );
      bucket_alg = str_p("alg") >> name;
      bucket_hash = str_p("hash") >> ( integer |
				       str_p("rjenkins1") );
      bucket_item = str_p("item") >> name
				  >> !( str_p("weight") >> real_p )
				  >> !( str_p("pos") >> posint );
      bucket = name >> name >> '{' >> *bucket_id >> bucket_alg >> *bucket_hash >> *bucket_item >> '}';

      // rules
      step_take = str_p("take") >> name >> !( str_p("class") >> name );
      step_set_choose_tries = str_p("set_choose_tries") >> posint;
      step_set_choose_local_tries = str_p("set_choose_local_tries") >> posint;
      step_set_choose_local_fallback_tries = str_p("set_choose_local_fallback_tries") >> posint;
      step_set_chooseleaf_tries = str_p("set_chooseleaf_tries") >> posint;
      step_set_chooseleaf_vary_r = str_p("set_chooseleaf_vary_r") >> posint;
      step_set_chooseleaf_stable = str_p("set_chooseleaf_stable") >> posint;
      step_set_msr_descents = str_p("set_msr_descents") >> posint;
      step_set_msr_collision_tries = str_p("set_msr_collision_tries") >> posint;
      step_choose = str_p("choose")
	>> ( str_p("indep") | str_p("firstn") )
	>> integer
	>> str_p("type") >> name;
      step_chooseleaf = str_p("chooseleaf")
	>> ( str_p("indep") | str_p("firstn") )
	>> integer
	>> str_p("type") >> name;
      step_emit = str_p("emit");
      step = str_p("step") >> ( step_take |
				step_set_choose_tries |
				step_set_choose_local_tries |
				step_set_choose_local_fallback_tries |
				step_set_chooseleaf_tries |
				step_set_chooseleaf_vary_r |
				step_set_chooseleaf_stable |
				step_set_msr_descents |
				step_set_msr_collision_tries |
				step_choose |
				step_chooseleaf |
				step_emit );
      crushrule = str_p("rule") >> !name >> '{'
				>> (str_p("id") | str_p("ruleset")) >> posint
				>> str_p("type") >> ( str_p("replicated") | str_p("erasure") | str_p("msr_firstn") | str_p("msr_indep") )
				>> !(str_p("min_size") >> posint)
				>> !(str_p("max_size") >> posint)
			   >> +step
			   >> '}';

      weight_set_weights = str_p("[") >> *real_p >> str_p("]");
      weight_set = str_p("weight_set") >> str_p("[")
				       >> *weight_set_weights
				       >> str_p("]");
      choose_arg_ids = str_p("ids") >> str_p("[") >> *integer >> str_p("]");
      choose_arg = str_p("{") >> str_p("bucket_id") >> negint
			      >> !weight_set
			      >> !choose_arg_ids
			      >> str_p("}");
      choose_args = str_p("choose_args") >> posint >> str_p("{") >> *choose_arg >> str_p("}");

      // the whole crush map
      crushmap = *(tunable | device | bucket_type) >> *(bucket | crushrule) >> *choose_args;
    }

    boost::spirit::rule<ScannerT, boost::spirit::parser_context<>,
			boost::spirit::parser_tag<_crushmap> > const&
    start() const { return crushmap; }
  };
};

#endif
