// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2020 Red Hat, Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 */

#pragma once

#include <boost/spirit/include/qi.hpp>

#include <map>
#include <string>

// parse a map of keys/values.
namespace qi = boost::spirit::qi;

template <typename Iterator>
struct keys_and_values
  : qi::grammar<Iterator, std::map<std::string, std::string>()>
{
    keys_and_values()
      : keys_and_values::base_type(query)
    {
      query =  pair >> *(qi::lit(' ') >> pair);
      pair  =  key >> '=' >> value;
      key   =  qi::char_("a-zA-Z_") >> *qi::char_("a-zA-Z_0-9");
      value = +qi::char_("a-zA-Z0-9-_.");
    }
  qi::rule<Iterator, std::map<std::string, std::string>()> query;
  qi::rule<Iterator, std::pair<std::string, std::string>()> pair;
  qi::rule<Iterator, std::string()> key, value;
};
