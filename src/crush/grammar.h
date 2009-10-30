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

#ifndef __CRUSH_GRAMMAR_H
#define __CRUSH_GRAMMAR_H

//#define BOOST_SPIRIT_DEBUG

#include <boost/spirit/core.hpp>
#include <boost/spirit/tree/ast.hpp>
#include <boost/spirit/tree/tree_to_xml.hpp>
using namespace boost::spirit;

struct crush_grammar : public grammar<crush_grammar>
{
  static const int _int = 1;
  static const int _posint = 2;
  static const int _negint = 3;
  static const int _name = 4;

  static const int _device = 12;
  static const int _bucket_type = 13;
  static const int _bucket_id = 14;
  static const int _bucket_alg = 15;
  static const int _bucket_item = 16;
  static const int _bucket = 17;

  static const int _step_take = 18;
  static const int _step_choose = 19;
  static const int _step_chooseleaf = 20;
  static const int _step_emit = 21;
  static const int _step = 22;
  static const int _crushrule = 23;

  static const int _crushmap = 24;

  template <typename ScannerT>
  struct definition
  {
    rule<ScannerT, parser_context<>, parser_tag<_int> >      integer;
    rule<ScannerT, parser_context<>, parser_tag<_posint> >      posint;
    rule<ScannerT, parser_context<>, parser_tag<_negint> >      negint;
    rule<ScannerT, parser_context<>, parser_tag<_name> >      name;

    rule<ScannerT, parser_context<>, parser_tag<_device> >      device;

    rule<ScannerT, parser_context<>, parser_tag<_bucket_type> >      bucket_type;

    rule<ScannerT, parser_context<>, parser_tag<_bucket_id> >      bucket_id;
    rule<ScannerT, parser_context<>, parser_tag<_bucket_alg> >      bucket_alg;
    rule<ScannerT, parser_context<>, parser_tag<_bucket_item> >      bucket_item;
    rule<ScannerT, parser_context<>, parser_tag<_bucket> >      bucket;

    rule<ScannerT, parser_context<>, parser_tag<_step_take> >      step_take;
    rule<ScannerT, parser_context<>, parser_tag<_step_choose> >      step_choose;
    rule<ScannerT, parser_context<>, parser_tag<_step_chooseleaf> >      step_chooseleaf;
    rule<ScannerT, parser_context<>, parser_tag<_step_emit> >      step_emit;
    rule<ScannerT, parser_context<>, parser_tag<_step> >      step;
    rule<ScannerT, parser_context<>, parser_tag<_crushrule> >      crushrule;

    rule<ScannerT, parser_context<>, parser_tag<_crushmap> >      crushmap;

    definition(crush_grammar const& /*self*/)
    {
      // base types
      integer     =   leaf_node_d[ lexeme_d[
					    (!ch_p('-') >> +digit_p)
					    ] ];
      posint     =   leaf_node_d[ lexeme_d[ +digit_p ] ];
      negint     =   leaf_node_d[ lexeme_d[ ch_p('-') >> +digit_p ] ];
      name = leaf_node_d[ lexeme_d[ +alnum_p ] ];

      // devices
      device = str_p("device") >> posint >> name;

      // bucket types
      bucket_type = str_p("type") >> posint >> name;

      // buckets
      bucket_id = str_p("id") >> negint;
      bucket_alg = str_p("alg") >> ( str_p("uniform") |
				     str_p("list") |
				     str_p("tree") |
				     str_p("straw") );
      bucket_item = str_p("item") >> name
				  >> !( str_p("weight") >> real_p )
				  >> !( str_p("pos") >> posint );
      bucket = name >> name >> '{' >> !bucket_id >> bucket_alg >> *bucket_item >> '}';

      // rules
      step_take = str_p("take") >> name;
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
				step_choose |
				step_chooseleaf |
				step_emit );
      crushrule = str_p("rule") >> !name >> '{'
			   >> str_p("ruleset") >> posint
			   >> str_p("type") >> ( str_p("replicated") | str_p("raid4") )
			   >> str_p("min_size") >> posint
			   >> str_p("max_size") >> posint
			   >> +step
			   >> '}';

      // the whole crush map
      crushmap = *(device | bucket_type) >> *bucket >> *crushrule;
    }

    rule<ScannerT, parser_context<>, parser_tag<_crushmap> > const&
    start() const { return crushmap; }
  };
};

#endif
