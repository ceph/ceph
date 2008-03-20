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
  
  template <typename ScannerT>
  struct definition
  {
    definition(crush_grammar const& /*self*/)
    {
      //  Start grammar definition
      integer     =   leaf_node_d[ lexeme_d[
					    (!ch_p('-') >> +digit_p)
					    ] ];
      
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
    
    rule<ScannerT, parser_context<>, parser_tag<expressionID> >   expression;
    rule<ScannerT, parser_context<>, parser_tag<termID> >         term;
    rule<ScannerT, parser_context<>, parser_tag<factorID> >       factor;
    rule<ScannerT, parser_context<>, parser_tag<integerID> >      integer;
    
    rule<ScannerT, parser_context<>, parser_tag<expressionID> > const&
    start() const { return expression; }
  };
};

#endif
