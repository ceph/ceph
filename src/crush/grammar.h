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

#ifndef __CRUSH_GRAMMAR
#define __CRUSH_GRAMMAR

using namespace boost::spirit;

struct crush_grammar : public grammar<crush_grammar>
{
  static const int integerID = 1;
  static const int factorID = 2;
  static const int termID = 3;
  static const int expressionID = 4;
  
  static const int _posint = 10;
  static const int _name = 11;
  static const int _device = 12;
  static const int _bucket_type = 13;
  static const int _bucket_id = 14;
  static const int _bucket_alg = 15;
  static const int _bucket_item = 16;
  static const int _bucket = 17;

  static const int _step_take = 18;
  static const int _step_choose_firstn = 19;
  static const int _step_choose_indep = 20;
  static const int _step_emit = 21;
  static const int _step = 22;
  static const int _crushrule = 23;

  static const int _crushmap = 23;

  template <typename ScannerT>
  struct definition
  {
    rule<ScannerT, parser_context<>, parser_tag<expressionID> >   expression;
    rule<ScannerT, parser_context<>, parser_tag<termID> >         term;
    rule<ScannerT, parser_context<>, parser_tag<factorID> >       factor;
    rule<ScannerT, parser_context<>, parser_tag<integerID> >      integer;

    rule<ScannerT, parser_context<>, parser_tag<_posint> >      posint;
    rule<ScannerT, parser_context<>, parser_tag<_name> >      name;

    rule<ScannerT, parser_context<>, parser_tag<_device> >      device;

    rule<ScannerT, parser_context<>, parser_tag<_bucket_type> >      bucket_type;

    rule<ScannerT, parser_context<>, parser_tag<_bucket_id> >      bucket_id;
    rule<ScannerT, parser_context<>, parser_tag<_bucket_alg> >      bucket_alg;
    rule<ScannerT, parser_context<>, parser_tag<_bucket_item> >      bucket_item;
    rule<ScannerT, parser_context<>, parser_tag<_bucket> >      bucket;

    rule<ScannerT, parser_context<>, parser_tag<_step_take> >      step_take;
    rule<ScannerT, parser_context<>, parser_tag<_step_choose_firstn> >      step_choose_firstn;
    rule<ScannerT, parser_context<>, parser_tag<_step_choose_indep> >      step_choose_indep;
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
      name = +alnum_p;

      // devices
      device = str_p("device") >> posint >> name >> *(str_p("overload") >> real_p);
      
      // bucket types
      bucket_type = str_p("buckettype") >> posint >> name;

      // buckets
      bucket_id = str_p("id") >> ch_p('-') >> posint;
      bucket_alg = str_p("alg") >> ( str_p("uniform") | str_p("list") | str_p("tree") | str_p("straw") );
      bucket_item = str_p("item") >> name
				  >> !( str_p("weight") >> real_p )
				  >> !( str_p("pos") >> posint );
      bucket = name >> name >> '{' >> !bucket_id >> bucket_alg >> *bucket_item >> '}';
      
      // rules
      step_take = str_p("take") >> str_p("root");
      step_choose_indep = str_p("choose_indep") >> integer >> name;
      step_choose_firstn = str_p("choose_firstn") >> integer >> name;
      step_emit = str_p("emit");
      step = str_p("step") >> ( step_take | step_choose_indep | step_choose_firstn | step_emit );
      crushrule = str_p("rule") >> name >> '{' 
			   >> str_p("pool") >> posint
			   >> str_p("type") >> ( str_p("replicated") | str_p("raid4") )
			   >> str_p("min_size") >> posint
			   >> str_p("max_size") >> posint
			   >> +step
			   >> '}';

      // the whole crush map
      crushmap = *(device | bucket_type) >> *bucket >> *crushrule;
      	


      //  Start grammar definition
      factor      =   integer
	|   inner_node_d[ch_p('(') >> expression >> ch_p(')')]
	|   (root_node_d[ch_p('-')] >> factor);
      
      term        =   factor >>
	*(  (root_node_d[ch_p('*')] >> factor)
	    | (root_node_d[ch_p('/')] >> factor)
	    );
      
      expression  =   term >>
	*(  (root_node_d[ch_p('+')] >> term)
	    | (root_node_d[ch_p('-')] >> term)
	    );
      //  End grammar definition
      
      // turn on the debugging info.
      BOOST_SPIRIT_DEBUG_RULE(integer);
      BOOST_SPIRIT_DEBUG_RULE(factor);
      BOOST_SPIRIT_DEBUG_RULE(term);
      BOOST_SPIRIT_DEBUG_RULE(expression);
    }
    
    rule<ScannerT, parser_context<>, parser_tag<expressionID> > const& 
    start() const { return expression; }
  };
};

#endif
